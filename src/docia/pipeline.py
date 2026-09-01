"""Pipeline `run` : sélection → blocs DocFuse → envoi asynchrone → persistance.

Idempotent et reprenable : chaque bloc et chaque fichier a un statut en base ;
relancer ne renvoie que les blocs `built`/`sent` sans résultat et les fichiers
`pending` sans analyse pour leur version de contenu courante.
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from pathlib import Path

from docia.blocks.builder import build_blocks
from docia.config import Config, LLMConfig
from docia.db import Database
from docia.llm.aggregate import aggregate_segments
from docia.llm.client import BlockTooLongError, LLMClient, LLMError
from docia.llm.parse import ParseError, parse_block_response
from docia.llm.schema import load_system_prompt, prompt_hash
from docia.models import BlockFile, BlockSpec, FileStatus

logger = logging.getLogger(__name__)

MAX_FILE_ATTEMPTS = 2
SYSTEM_PROMPT_RESERVE_TOKENS = 2_000
"""Tokens gardés libres pour le prompt système et le gabarit de conversation."""
SEGMENT_SAFETY = {"approx": 0.6, "openai": 0.85, "mistral": 0.85}
"""Part du contexte utilisable par un segment, selon le moteur de comptage : `approx`
(octets/4) sous-estime le tokenizer Qwen de ~30 % sur du texte français chiffré (banc
du 30/08 : 202 388 estimés → 266 402 réels) ; o200k/tekken restent à ±15 %. Le comptage
exact (`/tokenize`) avant envoi et la seconde passe corrigent le reste."""
RESPLIT_SAFETY = 0.9
"""Après un `BlockTooLongError`, budget de re-découpage = place réelle / ratio × 0.9."""


def segment_budget(cfg: Config) -> int:
    """Budget de tokens (avec marge) d'un segment de gros fichier."""
    room = cfg.llm.max_context_tokens - output_reserve_tokens(cfg.llm)
    safety = SEGMENT_SAFETY.get(cfg.blocks.tokenizer_engine, SEGMENT_SAFETY["approx"])
    return max(2_000, int(room * safety))


def output_reserve_tokens(llm: LLMConfig) -> int:
    """Tokens à garder libres sous `llm.max_context_tokens` pour qu'un segment qui
    occupe presque tout le contexte laisse la place au prompt système et à la
    réponse — raisonnement compris, et même après le renvoi avec budget doublé
    (`LLMClient.analyze_block`). Sans cela vLLM refuse la requête (400)."""
    single = llm.max_tokens_floor + llm.max_tokens_per_file
    if llm.enable_thinking:
        single += llm.thinking_budget_tokens
    return SYSTEM_PROMPT_RESERVE_TOKENS + 2 * min(
        single, llm.max_tokens_cap + llm.thinking_budget_tokens
    )


"""Un fichier absent de la réponse ou dans un bloc en erreur est retenté une
fois (dans un autre bloc), puis passe en `error` — jamais de boucle infinie.
Les tentatives se comptent **par segment** pour un fichier découpé : sinon un
fichier en K parties épuisait ses essais dès le premier run (voir
`Database.file_attempts`)."""


@dataclass
class RunReport:
    run_id: int
    files_resplit: int = 0
    """Fichiers re-découpés en seconde passe après un comptage exact trop long."""
    files_selected: int = 0
    files_done: int = 0
    files_error: int = 0
    blocks_built: int = 0
    blocks_resumed: int = 0
    blocks_skipped: int = 0
    """Blocs non envoyés : hors plafond du modèle, ou segments déjà analysés repris."""
    files_segmented: int = 0
    files_duplicates: int = 0
    blocks_done: int = 0
    blocks_error: int = 0
    prompt_tokens: int = 0
    completion_tokens: int = 0
    errors: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


ProgressCallback = Callable[[str], None]
"""Journal texte destiné à l'humain (CLI, GUI)."""

ProgressEventCallback = Callable[[dict[str, object]], None]
"""Progression structurée : un dictionnaire par étape (voir `_run`), pour la
couche service (`service.run_campaign`) et, demain, l'API REST."""


def resolve_system_prompt(db: Database, cfg: Config) -> str:
    """Prompt système effectif : fichier `prompt_path` > profil actif en base > prompt embarqué."""
    if cfg.prompt_path:
        return load_system_prompt(Path(cfg.prompt_path))
    active = db.active_prompt()
    if active is not None:
        return active[1]
    return load_system_prompt(None)


def run_pipeline(
    db: Database,
    cfg: Config,
    *,
    limit: int | None = None,
    dry_run: bool = False,
    progress: ProgressCallback | None = None,
    on_progress: ProgressEventCallback | None = None,
    cancel: threading.Event | None = None,
) -> RunReport:
    """Exécute un run complet (synchrone pour l'appelant ; asyncio à l'intérieur).

    `cancel` (GUI) : positionné, il arrête la construction des blocs et n'envoie
    plus de nouveau bloc ; les blocs déjà construits restent `built` (repris au
    run suivant) et les fichiers en vol sont remis `pending` par `requeue_stale`.
    """
    return asyncio.run(
        _run(
            db,
            cfg,
            limit=limit,
            dry_run=dry_run,
            progress=progress,
            on_progress=on_progress,
            cancel=cancel,
        )
    )


async def _run(  # noqa: C901, PLR0915
    db: Database,
    cfg: Config,
    *,
    limit: int | None,
    dry_run: bool,
    progress: ProgressCallback | None,
    on_progress: ProgressEventCallback | None = None,
    cancel: threading.Event | None = None,
) -> RunReport:
    say = progress or (lambda _m: None)
    started_at = time.monotonic()
    cancelled = cancel.is_set if cancel is not None else (lambda: False)
    system_prompt = resolve_system_prompt(db, cfg)
    phash = prompt_hash(system_prompt, cfg.llm.model)
    run_id = db.start_run(
        model=cfg.llm.model,
        prompt_hash=phash,
        config_json=json.dumps(asdict(cfg), ensure_ascii=False),
    )
    report = RunReport(run_id=run_id)
    work_dir = cfg.work_dir() / f"run_{run_id:04d}"

    clos: list[str] = []

    def close(status: str) -> None:
        """Clôt la ligne `runs`, une seule fois. Un run doit **toujours** être clos :
        sans cela une exception inattendue laisse `runs.status='running'` pour
        toujours et le tableau de bord annonce une campagne en cours qui n'existe plus."""
        if clos:
            return
        clos.append(status)
        db.finish_run(run_id, status)

    try:
        return await _execute(
            db,
            cfg,
            report=report,
            phash=phash,
            system_prompt=system_prompt,
            work_dir=work_dir,
            limit=limit,
            dry_run=dry_run,
            say=say,
            on_progress=on_progress,
            cancelled=cancelled,
            close=close,
            started_at=started_at,
        )
    finally:
        close("error")


async def _execute(  # noqa: C901, PLR0912, PLR0915
    db: Database,
    cfg: Config,
    *,
    report: RunReport,
    phash: str,
    system_prompt: str,
    work_dir: Path,
    limit: int | None,
    dry_run: bool,
    say: ProgressCallback,
    on_progress: ProgressEventCallback | None,
    cancelled: Callable[[], bool],
    close: Callable[[str], None],
    started_at: float,
) -> RunReport:
    """Corps du run. Toujours appelé sous le `try/finally` de `_run`, qui garantit
    la clôture de la ligne `runs` quoi qu'il arrive."""
    run_id = report.run_id

    # 1. Reprise : blocs d'un run précédent restés sans résultat, fichiers `queued` orphelins.
    requeued = db.requeue_stale()
    if requeued:
        say(f"reprise : {requeued} fichier(s) remis à analyser")
    leftover = db.pending_blocks(prompt_hash=phash, model=cfg.llm.model)
    if leftover:
        report.blocks_resumed = len(leftover)
        say(f"reprise : {len(leftover)} bloc(s) à renvoyer")

    specs: list[BlockSpec] = list(leftover)
    duplicates: list[tuple[int, int]] = []
    too_long: dict[int, tuple[float, int]] = {}
    """`file_id` → (pire ratio réel/estimé mesuré par `/tokenize`, plus petite place
    réellement disponible). Ces fichiers sont re-découpés plus finement dans une
    seconde passe du même run, sur la **place** et non sur le seul ratio."""
    encore_trop_longs: set[int] = set()
    """Fichiers dont un bloc est ENCORE refusé au comptage exact après re-découpage :
    plus rien ne peut les faire passer, ils partent en `error` avec la raison."""
    seconde_passe = [False]
    engages: set[int] = {bf.file_id for spec in leftover for bf in spec.files}
    """Fichiers embarqués dans un bloc de ce run (repris ou construit) : ils doivent
    tous finir `done` ou `error`, sinon le run le dit (voir la vérification finale)."""
    faits: set[int] = set()
    rates: set[int] = set()
    blocs_termines: set[int] = set()
    """Blocs `done`/`error` : seuls ceux-là voient leur `.md` effacé si
    `keep_blocks = false` — effacer un bloc resté `built` bloque la campagne."""

    def emit(kind: str, message: str = "", **extra: object) -> None:
        """Événement structuré vers `on_progress` (compteurs vivants du rapport)."""
        if on_progress is None:
            return
        payload: dict[str, object] = {
            "event": kind,
            "message": message,
            "files_total": report.files_selected,
            "files_done": report.files_done,
            "files_error": report.files_error,
            "blocks_total": len(specs),
            "blocks_done": report.blocks_done,
            "blocks_error": report.blocks_error,
            "elapsed_s": round(time.monotonic() - started_at, 3),
            "finished": False,
            "cancelled": False,
        }
        payload.update(extra)
        on_progress(payload)

    def compte_fait(file_id: int) -> None:
        """`files_done` compte des **fichiers**, pas des segments ni des blocs."""
        if file_id not in faits:
            faits.add(file_id)
            report.files_done += 1

    def compte_rate(file_id: int) -> None:
        if file_id not in rates:
            rates.add(file_id)
            report.files_error += 1

    def agrege_si_complet(bf: BlockFile, block_id: int) -> None:
        """Agrège les K segments d'un gros fichier — **uniquement** si les K segments
        du découpage courant sont tous en base.

        `segment_analyses` rend toutes les lignes du couple (fichier, version, prompt,
        modèle) : sans filtrer sur `segment_count` et sans exiger l'ensemble {1..K}
        exactement, des segments d'un découpage plus fin laissés par un run précédent
        suffisaient à déclarer `done` un fichier analysé à 20 %.
        """
        connus = db.segment_analyses(
            bf.file_id,
            bf.content_version,
            prompt_hash=phash,
            model=cfg.llm.model,
            segment_count=bf.segment_count,
        )
        if {index for index, _count, _raw in connus} != set(range(1, bf.segment_count + 1)):
            return  # on attend les autres segments (même run ou suivant)
        merged = aggregate_segments(
            bf.file_ref.rsplit(" [partie", 1)[0], [raw for _i, _c, raw in connus]
        )
        db.store_analysis(
            bf.file_id,
            block_id,
            bf.content_version,
            prompt_hash=phash,
            model=cfg.llm.model,
            analysis=merged,
            segments=bf.segment_count,
        )
        db.set_block_file_outcome(block_id, bf.file_id, "done (segments agrégés)")
        compte_fait(bf.file_id)

    def segments_deja_analyses(spec: BlockSpec) -> bool:
        """Vrai si tous les fichiers du bloc sont des segments déjà en base pour le
        découpage courant : reprise d'un gros fichier dont un seul segment a échoué —
        on ne repaie pas les K−1 autres (600 segments pour un fichier de 2 M tokens)."""
        if not spec.files or not all(bf.is_segment for bf in spec.files):
            return False
        for bf in spec.files:
            connus = db.segment_analyses(
                bf.file_id,
                bf.content_version,
                prompt_hash=phash,
                model=cfg.llm.model,
                segment_count=bf.segment_count,
            )
            if bf.segment_index not in {index for index, _c, _raw in connus}:
                return False
        return True

    def retry_or_fail(bf: BlockFile, block_id: int, reason: str) -> None:
        """Un fichier sans résultat repart `pending` pour un autre bloc, jusqu'à
        `MAX_FILE_ATTEMPTS` tentatives ; ensuite `error` avec la raison."""
        db.set_block_file_outcome(block_id, bf.file_id, f"failed: {reason[:200]}")
        attempts = db.file_attempts(
            bf.file_id,
            segment_index=bf.segment_index if bf.is_segment else None,
            segment_count=bf.segment_count if bf.is_segment else None,
        )
        if attempts >= MAX_FILE_ATTEMPTS:
            db.set_file_status(bf.file_id, FileStatus.ERROR, reason[:500])
            compte_rate(bf.file_id)
        else:
            db.set_file_status(bf.file_id, FileStatus.PENDING, None)

    def fail_block(spec: BlockSpec, error: str) -> None:
        assert spec.block_id is not None
        logger.error("bloc %s en erreur : %s", spec.block_id, error)
        db.mark_block_error(spec.block_id, error)
        blocs_termines.add(spec.block_id)
        report.blocks_error += 1
        report.errors.append(f"bloc {spec.block_id} : {error}")
        for bf in spec.files:
            retry_or_fail(bf, spec.block_id, error)

    def perte_du_bloc(spec: BlockSpec, raison: str) -> None:
        """Le `.md` du bloc a disparu (annulation + `keep_blocks = false`, ménage
        disque, antivirus). Le bloc est mort : le clore en `error` — sinon il est
        repris et replante à chaque run — et remettre ses fichiers à analyser, qui
        seront reconstruits dans un bloc neuf."""
        assert spec.block_id is not None
        logger.error("bloc %s : %s", spec.block_id, raison)
        db.mark_block_error(spec.block_id, raison)
        blocs_termines.add(spec.block_id)
        report.blocks_error += 1
        report.errors.append(f"bloc {spec.block_id} : {raison}")
        for bf in spec.files:
            db.set_block_file_outcome(spec.block_id, bf.file_id, "bloc perdu : à reconstruire")
            db.set_file_status(bf.file_id, FileStatus.PENDING, None)
        say(f"bloc {spec.block_id} : {raison} — fichiers remis à analyser")
        emit("block_error", f"bloc {spec.block_id} : {raison}")

    async with LLMClient(cfg.llm, system_prompt) as client:
        # 2. Le contexte réellement servi fait foi, et il doit être connu AVANT de
        #    découper : le budget d'un segment (`max_file_tokens`) en dérive.
        if not dry_run:
            served = await client.server_max_model_len()
            if served is not None and served != cfg.llm.max_context_tokens:
                say(
                    f"attention : llm.max_context_tokens={cfg.llm.max_context_tokens} mais le "
                    f"serveur sert {served} (--max-model-len) — la valeur du serveur fait foi"
                )
                if served < cfg.llm.max_context_tokens:
                    cfg.llm.max_context_tokens = served
                    # Le serveur sert moins que prévu : les blocs doivent être bâtis
                    # à sa mesure, sinon TOUS dépassent le plafond et toute la
                    # campagne part en erreur sans avoir envoyé une seule requête.
                    budget = segment_budget(cfg)
                    cfg.blocks.max_file_tokens = min(cfg.blocks.max_file_tokens or budget, budget)
                    if cfg.blocks.block_tokens > cfg.blocks.max_file_tokens:
                        cfg.blocks.block_tokens = cfg.blocks.max_file_tokens
                        say(
                            f"blocs ramenés à {cfg.blocks.block_tokens} tokens "
                            "pour tenir dans le contexte servi"
                        )
        if cfg.blocks.max_file_tokens <= 0:
            # Réserve pour le prompt système et la réponse JSON du fichier découpé.
            cfg.blocks.max_file_tokens = segment_budget(cfg)
        if not dry_run and not await client.health():
            close("error")
            report.errors.append(f"serveur LLM injoignable : {cfg.llm.base_url}")
            emit("finished", f"serveur LLM injoignable : {cfg.llm.base_url}", finished=True)
            return report

        # 3. Sélection + construction des blocs, par lots.
        #    Seuls les **identifiants** sont retenus pour toute la durée du run : la liste
        #    complète des `FileRow` pesait 1 722 Mo pour 700 797 fichiers et restait en
        #    mémoire des heures durant, sur un serveur qui n'a que 8 à 16 Go. Chaque lot
        #    est relu juste avant d'être traité (`files_by_ids`). La liste d'identifiants
        #    est un instantané : elle ne bouge pas quand le run change les statuts,
        #    contrairement à un curseur ouvert sur la connexion qui écrit.
        selected = db.select_pending_ids(limit or 10**9, prompt_hash=phash, model=cfg.llm.model)
        report.files_selected = len(engages | set(selected))
        say(f"{len(selected)} fichier(s) à analyser")

        for start in range(0, len(selected), cfg.blocks.batch_files):
            if cancelled():
                say("annulation demandée : construction des blocs interrompue")
                break
            batch = db.files_by_ids(selected[start : start + cfg.blocks.batch_files])
            label = f"b{start // cfg.blocks.batch_files + 1:04d}"
            built = build_blocks(batch, cfg.blocks, work_dir, batch_label=label)
            for failed in built.failed:
                db.set_file_status(failed.file_id, FileStatus.ERROR, failed.reason)
                compte_rate(failed.file_id)
                emit("file_error", f"extraction impossible : {failed.reason}")
            duplicates.extend(built.duplicates)
            for spec in built.blocks:
                if spec.tokens_with_margin > cfg.llm.max_context_tokens:
                    # Ne devrait plus arriver : le builder tronque au-delà de
                    # `max_file_tokens`. Garde-fou explicite, jamais silencieux.
                    reason = (
                        f"hors plafond du modèle malgré troncature : {spec.tokens_with_margin} "
                        f"tokens > {cfg.llm.max_context_tokens} (llm.max_context_tokens)"
                    )
                    for bf in spec.files:
                        db.set_file_status(bf.file_id, FileStatus.ERROR, reason)
                        compte_rate(bf.file_id)
                    report.blocks_skipped += 1
                    report.errors.append(reason)
                    continue
                db.create_block(run_id, spec, prompt_hash=phash, model=cfg.llm.model)
                specs.append(spec)
                engages.update(bf.file_id for bf in spec.files)
                report.blocks_built += 1
                report.files_segmented += sum(1 for bf in spec.files if bf.segment_index == 1)
            say(
                f"lot {label} : {len(built.blocks)} bloc(s), "
                f"{len(built.failed)} échec(s) d'extraction"
            )

        emit("start", f"{len(selected)} fichier(s) à analyser, {len(specs)} bloc(s) construit(s)")

        if dry_run:
            say("dry-run : blocs construits, aucun envoi")
            for dup_id, _orig in duplicates:
                db.set_file_status(dup_id, FileStatus.PENDING)
            close("dry-run")
            emit("finished", "dry-run : blocs construits, aucun envoi", finished=True)
            return report

        # 4. Envoi asynchrone, N en vol.
        async def one(spec: BlockSpec) -> None:  # noqa: C901, PLR0912
            assert spec.block_id is not None
            if cancelled():
                return  # reste `built`, repris au prochain run
            if segments_deja_analyses(spec):
                db.mark_block_done(spec.block_id, None)
                blocs_termines.add(spec.block_id)
                report.blocks_skipped += 1
                for bf in spec.files:
                    db.set_block_file_outcome(spec.block_id, bf.file_id, "segment déjà analysé")
                    agrege_si_complet(bf, spec.block_id)
                say(f"bloc {spec.block_id} : segments déjà analysés, non renvoyés")
                return
            if not spec.path.is_file():
                perte_du_bloc(spec, f"bloc introuvable sur le disque : {spec.path}")
                return
            db.mark_block_sent(spec.block_id)
            try:
                result = await client.analyze_block(spec)
            except BlockTooLongError as exc:
                # Refusé AVANT envoi : ce n'est pas une tentative du modèle. Le fichier
                # repart `pending` sans consommer d'essai (un gros fichier a K segments :
                # compter K échecs le mettrait en erreur avant même la seconde passe).
                db.mark_block_error(spec.block_id, f"comptage exact : {exc}")
                blocs_termines.add(spec.block_id)
                report.blocks_error += 1
                for bf in spec.files:
                    ratio, place = too_long.get(bf.file_id, (0.0, exc.room))
                    too_long[bf.file_id] = (max(ratio, exc.ratio), min(place, exc.room))
                    if seconde_passe[0]:
                        encore_trop_longs.add(bf.file_id)
                    db.set_block_file_outcome(spec.block_id, bf.file_id, "too long: re-découpage")
                    db.set_file_status(bf.file_id, FileStatus.PENDING, None)
                emit("block_error", f"bloc {spec.block_id} : {exc} — re-découpage automatique")
                return
            except OSError as exc:
                if spec.path.is_file():
                    raise  # ce n'est pas la perte du bloc : au traitement générique
                perte_du_bloc(spec, f"bloc introuvable sur le disque : {exc}")
                return
            except LLMError as exc:
                fail_block(spec, f"LLM : {exc}")
                emit("block_error", f"bloc {spec.block_id} : LLM : {exc}")
                return
            try:
                parsed = parse_block_response(result.content, spec.files)
            except ParseError as exc:
                fail_block(spec, f"réponse illisible : {exc}")
                emit("block_error", f"bloc {spec.block_id} : réponse illisible : {exc}")
                return
            for bf in spec.files:
                analysis = parsed.analyses.get(bf.file_id)
                if analysis is None:
                    reason = next((r for ref, r in parsed.invalid if ref == bf.file_ref), None)
                    retry_or_fail(bf, spec.block_id, reason or "absent de la réponse")
                    continue
                if bf.is_segment:
                    db.store_segment_analysis(
                        bf.file_id,
                        spec.block_id,
                        bf.content_version,
                        prompt_hash=phash,
                        model=cfg.llm.model,
                        segment_index=bf.segment_index,
                        segment_count=bf.segment_count,
                        raw=analysis.raw,
                    )
                    db.set_block_file_outcome(spec.block_id, bf.file_id, "segment done")
                    agrege_si_complet(bf, spec.block_id)
                    continue
                db.store_analysis(
                    bf.file_id,
                    spec.block_id,
                    bf.content_version,
                    prompt_hash=phash,
                    model=cfg.llm.model,
                    analysis=analysis,
                )
                db.set_block_file_outcome(spec.block_id, bf.file_id, "done")
                compte_fait(bf.file_id)
            if parsed.unknown_refs:
                logger.warning(
                    "bloc %s : file_ref inconnus ignorés : %s",
                    spec.block_id,
                    parsed.unknown_refs[:5],
                )
            db.mark_block_done(spec.block_id, result.usage)
            blocs_termines.add(spec.block_id)
            report.blocks_done += 1
            report.prompt_tokens += result.usage.prompt_tokens
            report.completion_tokens += result.usage.completion_tokens
            message = (
                f"bloc {spec.block_id} : {len(parsed.analyses)}/{len(spec.files)} fichiers, "
                f"{result.usage.prompt_tokens} tok prompt, {result.usage.latency_ms} ms"
            )
            say(message)
            emit("block_done", message)

        async def envoyer(a_envoyer: list[BlockSpec]) -> None:
            """Envoie les blocs, au plus `max_in_flight` en vol, alimentés **à la demande**.

            Un `gather` sur la totalité des blocs ordonnance toutes les coroutines
            d'emblée : le test d'annulation de `one` était évalué pour tous les blocs à
            l'instant zéro (donc jamais après le premier envoi), et un million de blocs
            créait un million de coroutines et de transactions SQLite. Ici chaque
            travailleur prend le bloc suivant seulement quand il est libre, et vérifie
            l'annulation à ce moment-là : « n'envoie plus de nouveau bloc » est tenu.

            Aucune exception ne doit emporter le run : une coupure de flux
            (`httpx.RemoteProtocolError`, hors `LLMError`) tuait tous les blocs en vol,
            sautait la clôture du run et laissait les fichiers `queued` pour toujours.
            """
            file_attente = iter(a_envoyer)

            async def travailleur() -> None:
                for spec in file_attente:
                    if cancelled():
                        return
                    try:
                        await one(spec)
                    except Exception as exc:  # noqa: BLE001
                        fail_block(spec, f"erreur inattendue : {type(exc).__name__} : {exc}")
                        emit("block_error", f"bloc {spec.block_id} : {type(exc).__name__} : {exc}")

            nb = max(1, min(cfg.llm.max_in_flight, len(a_envoyer)))
            issues = await asyncio.gather(
                *(travailleur() for _ in range(nb)), return_exceptions=True
            )
            for issue in issues:
                if isinstance(issue, BaseException):
                    logger.error("envoi interrompu : %s", issue)
                    report.errors.append(f"envoi interrompu : {type(issue).__name__} : {issue}")

        await envoyer(specs)

        # 5. Seconde passe : fichiers dont un bloc était trop long au comptage exact.
        #    Re-découpage sur la place réellement disponible (`BlockTooLongError.room`)
        #    et non sur le seul ratio réel/estimé : quand le serveur sert moins que
        #    prévu, le ratio vaut ~1 et le budget ne bougeait pas — le fichier n'était
        #    alors jamais analysé, jamais en erreur, et chaque relance recommençait.
        if too_long and not cancelled():
            retry_rows = [
                row
                for fid in sorted(too_long)
                if (row := db.get_file(fid)) is not None and row.status == FileStatus.PENDING
            ]
            if retry_rows:
                pire_ratio = max(too_long[row.id][0] for row in retry_rows)
                place = min(too_long[row.id][1] for row in retry_rows)
                from dataclasses import replace

                budget = max(2_000, int(place / max(pire_ratio, 0.01) * RESPLIT_SAFETY))
                retry_cfg = replace(
                    cfg.blocks,
                    max_file_tokens=budget,
                    block_tokens=min(cfg.blocks.block_tokens, budget),
                )
                say(
                    f"seconde passe : {len(retry_rows)} fichier(s) re-découpé(s) "
                    f"(ratio réel/estimé {pire_ratio:.2f}, place {place} tokens, "
                    f"budget {budget} tokens)"
                )
                db.set_files_status([row.id for row in retry_rows], FileStatus.QUEUED)
                retry_specs: list[BlockSpec] = []
                built = build_blocks(retry_rows, retry_cfg, work_dir, batch_label="r0001")
                for failed in built.failed:
                    db.set_file_status(failed.file_id, FileStatus.ERROR, failed.reason)
                    compte_rate(failed.file_id)
                duplicates.extend(built.duplicates)
                for spec in built.blocks:
                    db.create_block(run_id, spec, prompt_hash=phash, model=cfg.llm.model)
                    specs.append(spec)
                    retry_specs.append(spec)
                    engages.update(bf.file_id for bf in spec.files)
                    report.blocks_built += 1
                    report.files_resplit += sum(1 for bf in spec.files if bf.segment_index == 1)
                seconde_passe[0] = True
                await envoyer(retry_specs)

            # Toujours trop long après re-découpage : le dire, et le mettre en erreur.
            # Sans cela le fichier repart `pending` à chaque run, indéfiniment, et le
            # run sort « done » avec le code de retour 0.
            for fid in sorted(encore_trop_longs):
                row = db.get_file(fid)
                if row is None or row.status != FileStatus.PENDING:
                    continue
                raison = (
                    "bloc encore trop long après re-découpage pour le contexte servi "
                    f"({cfg.llm.max_context_tokens} tokens) : réduisez blocks.block_tokens / "
                    "llm.max_tokens_cap, ou servez un --max-model-len plus grand"
                )
                db.set_file_status(fid, FileStatus.ERROR, raison)
                compte_rate(fid)
                report.errors.append(f"fichier {fid} ({row.name}) : {raison}")

    # Doublons exacts : héritent de l'analyse de leur original (même contenu).
    for dup_id, orig_id in duplicates:
        dup = db.get_file(dup_id)
        if dup is None:
            continue
        if db.copy_analysis(
            orig_id, dup_id, dup.content_version, prompt_hash=phash, model=cfg.llm.model
        ):
            compte_fait(dup_id)
            report.files_duplicates += 1
        else:
            # Original pas (encore) analysé : le doublon sera analysé pour
            # lui-même au prochain run (l'original ne sera plus dans le lot).
            db.set_file_status(dup_id, FileStatus.PENDING)
    if report.files_duplicates:
        say(f"{report.files_duplicates} doublon(s) exact(s) : analyse héritée de l'original")

    if not cfg.blocks.keep_blocks:
        # Uniquement les blocs `done`/`error` : effacer le `.md` d'un bloc resté
        # `built` (annulation) le rend illisible à la reprise et bloque la campagne.
        for spec in specs:
            if spec.block_id in blocs_termines:
                spec.path.unlink(missing_ok=True)

    if cancelled():
        close("cancelled")
        say("run annulé — relancer pour reprendre")
        emit("cancelled", "run annulé — relancer pour reprendre", cancelled=True)
        emit("finished", "run annulé", finished=True, cancelled=True)
        return report

    # Aucun fichier embarqué dans un bloc ne doit rester en plan sans que le run le
    # dise : un run qui n'a rien analysé ne sort jamais « done » avec le code 0.
    restants, exemples = db.unfinished_files(sorted(engages))
    if restants:
        suite = "…" if restants > len(exemples) else ""
        report.errors.append(
            f"{restants} fichier(s) engagé(s) dans un bloc n'ont été ni analysés ni mis en "
            f"erreur (à reprendre) : {', '.join(exemples)}{suite}"
        )
        say(report.errors[-1])
    close("done" if not report.errors else "error")
    emit("finished", f"run {run_id} terminé", finished=True)
    return report
