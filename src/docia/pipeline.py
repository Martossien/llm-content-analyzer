"""Pipeline `run` : sélection → blocs DocFuse → envoi asynchrone → persistance.

Idempotent et reprenable : chaque bloc et chaque fichier a un statut en base ;
relancer ne renvoie que les blocs `built`/`sent` sans résultat et les fichiers
`pending` sans analyse pour leur version de contenu courante.

Le run est une machine à étapes portée par `_Run` : reprise → contexte servi →
construction des blocs → envoi (N en vol) → seconde passe des blocs trop longs →
doublons → ménage → clôture. Chaque étape est une méthode courte ; l'état partagé
(compteurs, ensembles de fichiers engagés/faits/ratés) vit sur l'objet, pas dans
des fermetures imbriquées.
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path

from docia.blocks.builder import BuildResult, build_blocks
from docia.blocks.policy import SegmentPolicy
from docia.config import Config, LLMConfig
from docia.db import Database
from docia.llm.aggregate import aggregate_segments
from docia.llm.client import BlockTooLongError, LLMClient, LLMError
from docia.llm.parse import ParsedBlock, ParseError, parse_block_response
from docia.llm.schema import load_system_prompt, prompt_hash
from docia.llm.tokenize import ServerTokenCounter
from docia.models import BlockFile, BlockSpec, FileStatus

logger = logging.getLogger(__name__)

MAX_FILE_ATTEMPTS = 2
"""Un fichier absent de la réponse ou dans un bloc en erreur est retenté une
fois (dans un autre bloc), puis passe en `error` — jamais de boucle infinie.
Les tentatives se comptent **par segment** pour un fichier découpé : sinon un
fichier en K parties épuisait ses essais dès le premier run (voir
`Database.file_attempts`)."""
SYSTEM_PROMPT_RESERVE_TOKENS = 2_000
"""Tokens gardés libres pour le prompt système et le gabarit de conversation."""
SEGMENT_SAFETY = {"approx": 0.6, "openai": 0.85, "mistral": 0.85}
"""Part du contexte utilisable par un segment, selon le moteur de comptage : `approx`
(octets/4) sous-estime le tokenizer Qwen de ~30 % sur du texte français chiffré (banc
du 30/08 : 202 388 estimés → 266 402 réels) ; o200k/tekken restent à ±15 %. Le comptage
exact (`/tokenize`) avant envoi et la seconde passe corrigent le reste."""
RESPLIT_SAFETY = 0.9
"""Après un `BlockTooLongError`, budget de re-découpage = place réelle / ratio × 0.9."""


def file_cap(cfg: Config, prompt_tokens: int = SYSTEM_PROMPT_RESERVE_TOKENS) -> int:
    """Plafond **exact** (tokens réels du serveur) d'un fichier seul : la part
    `blocks.max_file_share` du contexte, une fois réservés le prompt système et la
    réponse. Au-dessus, le fichier est découpé en segments (`blocks/policy.py`).

    `prompt_tokens` : réserve pour le prompt système (mesurée par le client quand le
    serveur sait compter, voir `LLMClient.prompt_reserve`)."""
    room = cfg.llm.max_context_tokens - output_reserve_tokens(cfg.llm, prompt_tokens)
    return max(2_000, int(room * cfg.blocks.max_file_share))


def segment_budget(cfg: Config, prompt_tokens: int = SYSTEM_PROMPT_RESERVE_TOKENS) -> int:
    """Le même plafond en tokens **estimés** (avec marge, moteur local), dévalué par
    le facteur de sécurité du moteur : budget d'un segment quand le serveur ne sait
    pas compter, seuil de candidature au découpage sinon."""
    safety = SEGMENT_SAFETY.get(cfg.blocks.tokenizer_engine, SEGMENT_SAFETY["approx"])
    return max(2_000, int(file_cap(cfg, prompt_tokens) * safety))


def output_reserve_tokens(llm: LLMConfig, prompt_tokens: int = SYSTEM_PROMPT_RESERVE_TOKENS) -> int:
    """Tokens à garder libres sous `llm.max_context_tokens` pour qu'un segment qui
    occupe presque tout le contexte laisse la place au prompt système et à la
    réponse — raisonnement compris, et même après le renvoi avec budget doublé
    (`LLMClient.analyze_block`). Sans cela vLLM refuse la requête (400)."""
    single = llm.max_tokens_floor + llm.max_tokens_per_file
    if llm.enable_thinking:
        single += llm.thinking_budget_tokens
    return prompt_tokens + 2 * min(single, llm.max_tokens_cap + llm.thinking_budget_tokens)


@dataclass
class RunReport:
    """Bilan d'un run : fichiers et blocs comptés, tokens consommés, erreurs."""

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
        """Bilan sous forme de dictionnaire (sortie `--json`, événements)."""
        return asdict(self)


ProgressCallback = Callable[[str], None]
"""Journal texte destiné à l'humain (CLI, GUI)."""

ProgressEventCallback = Callable[[dict[str, object]], None]
"""Progression structurée : un dictionnaire par étape (voir `_Run.emit`), pour la
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


async def _run(
    db: Database,
    cfg: Config,
    *,
    limit: int | None,
    dry_run: bool,
    progress: ProgressCallback | None,
    on_progress: ProgressEventCallback | None = None,
    cancel: threading.Event | None = None,
) -> RunReport:
    system_prompt = resolve_system_prompt(db, cfg)
    phash = prompt_hash(system_prompt, cfg.llm.model)
    run_id = db.start_run(
        model=cfg.llm.model,
        prompt_hash=phash,
        config_json=json.dumps(asdict(cfg), ensure_ascii=False),
    )
    run = _Run(
        db,
        cfg,
        report=RunReport(run_id=run_id),
        prompt_hash=phash,
        system_prompt=system_prompt,
        work_dir=cfg.work_dir() / f"run_{run_id:04d}",
        limit=limit,
        dry_run=dry_run,
        say=progress or (lambda _m: None),
        on_progress=on_progress,
        cancelled=cancel.is_set if cancel is not None else (lambda: False),
    )
    try:
        return await run.execute()
    finally:
        # Un run doit **toujours** être clos : sans cela une exception inattendue
        # laisse `runs.status='running'` pour toujours et le tableau de bord annonce
        # une campagne en cours qui n'existe plus. `close` est sans effet si l'étape
        # de clôture a déjà posé le vrai statut.
        run.close("error")


class _Run:
    """Un run en cours : son état partagé et ses étapes (voir le module)."""

    def __init__(
        self,
        db: Database,
        cfg: Config,
        *,
        report: RunReport,
        prompt_hash: str,
        system_prompt: str,
        work_dir: Path,
        limit: int | None,
        dry_run: bool,
        say: ProgressCallback,
        on_progress: ProgressEventCallback | None,
        cancelled: Callable[[], bool],
    ) -> None:
        self.db = db
        self.cfg = cfg
        self.report = report
        self.prompt_hash = prompt_hash
        self.system_prompt = system_prompt
        self.work_dir = work_dir
        self.limit = limit
        self.dry_run = dry_run
        self.say = say
        self.on_progress = on_progress
        self.cancelled = cancelled
        self.started_at = time.monotonic()
        self.closed_status: str | None = None
        self.client: LLMClient | None = None

        self.specs: list[BlockSpec] = []
        """Tous les blocs de ce run (repris, construits, re-découpés)."""
        self.duplicates: list[tuple[int, int]] = []
        """(doublon, original) : contenu identique, l'analyse est héritée en fin de run."""
        self.too_long: dict[int, tuple[float, int]] = {}
        """`file_id` → (pire ratio réel/estimé mesuré par `/tokenize`, plus petite place
        réellement disponible). Ces fichiers sont re-découpés plus finement dans une
        seconde passe du même run, sur la **place** et non sur le seul ratio."""
        self.still_too_long: set[int] = set()
        """Fichiers dont un bloc est ENCORE refusé au comptage exact après re-découpage :
        plus rien ne peut les faire passer, ils partent en `error` avec la raison."""
        self.second_pass = False
        self.file_cap = 0
        """Plafond exact par fichier (`file_cap`), fixé une fois le contexte servi connu."""
        self.engaged: set[int] = set()
        """Fichiers embarqués dans un bloc de ce run (repris ou construit) : ils doivent
        tous finir `done` ou `error`, sinon le run le dit (voir `_finish`)."""
        self.done_files: set[int] = set()
        self.failed_files: set[int] = set()
        self.finished_blocks: set[int] = set()
        """Blocs `done`/`error` : seuls ceux-là voient leur `.md` effacé si
        `keep_blocks = false` — effacer un bloc resté `built` bloque la campagne."""

    # ---------------------------------------------------------------- utilitaires
    @property
    def run_id(self) -> int:
        """Identifiant de la ligne `runs` de ce run."""
        return self.report.run_id

    @property
    def model(self) -> str:
        """Modèle configuré pour ce run."""
        return self.cfg.llm.model

    def close(self, status: str) -> None:
        """Clôt la ligne `runs`, une seule fois (le premier statut posé fait foi)."""
        if self.closed_status is not None:
            return
        self.closed_status = status
        self.db.finish_run(self.run_id, status)

    def emit(self, kind: str, message: str = "", **extra: object) -> None:
        """Événement structuré vers `on_progress` (compteurs vivants du rapport)."""
        if self.on_progress is None:
            return
        report = self.report
        payload: dict[str, object] = {
            "event": kind,
            "message": message,
            "files_total": report.files_selected,
            "files_done": report.files_done,
            "files_error": report.files_error,
            "blocks_total": len(self.specs),
            "blocks_done": report.blocks_done,
            "blocks_error": report.blocks_error,
            "elapsed_s": round(time.monotonic() - self.started_at, 3),
            "finished": False,
            "cancelled": False,
        }
        payload.update(extra)
        self.on_progress(payload)

    def _count_done(self, file_id: int) -> None:
        """`files_done` compte des **fichiers**, pas des segments ni des blocs."""
        if file_id not in self.done_files:
            self.done_files.add(file_id)
            self.report.files_done += 1

    def _count_failed(self, file_id: int) -> None:
        if file_id not in self.failed_files:
            self.failed_files.add(file_id)
            self.report.files_error += 1

    def _fail_file(self, file_id: int, reason: str) -> None:
        """Passe un fichier en `error` avec sa raison et le compte une seule fois."""
        self.db.set_file_status(file_id, FileStatus.ERROR, reason)
        self._count_failed(file_id)

    def _known_segments(self, bf: BlockFile) -> list[tuple[int, int, dict[str, object]]]:
        """Segments déjà en base pour ce fichier, **dans le découpage courant** (K)."""
        return self.db.segment_analyses(
            bf.file_id,
            bf.content_version,
            prompt_hash=self.prompt_hash,
            model=self.model,
            segment_count=bf.segment_count,
        )

    def _aggregate_if_complete(self, bf: BlockFile, block_id: int) -> None:
        """Agrège les K segments d'un gros fichier — **uniquement** si les K segments
        du découpage courant sont tous en base.

        Sans filtrer sur `segment_count` et sans exiger l'ensemble {1..K} exactement,
        des segments d'un découpage plus fin laissés par un run précédent suffisaient
        à déclarer `done` un fichier analysé à 20 %.
        """
        known = self._known_segments(bf)
        if {index for index, _count, _raw in known} != set(range(1, bf.segment_count + 1)):
            return  # on attend les autres segments (même run ou suivant)
        merged = aggregate_segments(
            bf.file_ref.rsplit(" [partie", 1)[0], [raw for _i, _c, raw in known]
        )
        self.db.store_analysis(
            bf.file_id,
            block_id,
            bf.content_version,
            prompt_hash=self.prompt_hash,
            model=self.model,
            analysis=merged,
            segments=bf.segment_count,
        )
        self.db.set_block_file_outcome(block_id, bf.file_id, "done (segments agrégés)")
        self._count_done(bf.file_id)

    def _segments_already_analyzed(self, spec: BlockSpec) -> bool:
        """Vrai si tous les fichiers du bloc sont des segments déjà en base pour le
        découpage courant : reprise d'un gros fichier dont un seul segment a échoué —
        on ne repaie pas les K−1 autres (600 segments pour un fichier de 2 M tokens)."""
        if not spec.files or not all(bf.is_segment for bf in spec.files):
            return False
        return all(
            bf.segment_index in {index for index, _c, _raw in self._known_segments(bf)}
            for bf in spec.files
        )

    def _retry_or_fail(self, bf: BlockFile, block_id: int, reason: str) -> None:
        """Un fichier sans résultat repart `pending` pour un autre bloc, jusqu'à
        `MAX_FILE_ATTEMPTS` tentatives ; ensuite `error` avec la raison."""
        self.db.set_block_file_outcome(block_id, bf.file_id, f"failed: {reason[:200]}")
        attempts = self.db.file_attempts(
            bf.file_id,
            segment_index=bf.segment_index if bf.is_segment else None,
            segment_count=bf.segment_count if bf.is_segment else None,
        )
        if attempts >= MAX_FILE_ATTEMPTS:
            self._fail_file(bf.file_id, reason[:500])
        else:
            self.db.set_file_status(bf.file_id, FileStatus.PENDING, None)

    def _close_block_in_error(self, spec: BlockSpec, error: str) -> int:
        assert spec.block_id is not None
        logger.error("bloc %s en erreur : %s", spec.block_id, error)
        self.db.mark_block_error(spec.block_id, error)
        self.finished_blocks.add(spec.block_id)
        self.report.blocks_error += 1
        self.report.errors.append(f"bloc {spec.block_id} : {error}")
        return spec.block_id

    def _fail_block(self, spec: BlockSpec, error: str) -> None:
        """Bloc en échec (LLM, réponse illisible, erreur inattendue) : chaque fichier
        est retenté ou mis en erreur."""
        block_id = self._close_block_in_error(spec, error)
        for bf in spec.files:
            self._retry_or_fail(bf, block_id, error)
        self.emit("block_error", f"bloc {block_id} : {error}")

    def _lose_block(self, spec: BlockSpec, reason: str) -> None:
        """Le `.md` du bloc a disparu (annulation + `keep_blocks = false`, ménage
        disque, antivirus). Le bloc est mort : le clore en `error` — sinon il est
        repris et replante à chaque run — et remettre ses fichiers à analyser, qui
        seront reconstruits dans un bloc neuf."""
        block_id = self._close_block_in_error(spec, reason)
        for bf in spec.files:
            self.db.set_block_file_outcome(block_id, bf.file_id, "bloc perdu : à reconstruire")
            self.db.set_file_status(bf.file_id, FileStatus.PENDING, None)
        self.say(f"bloc {block_id} : {reason} — fichiers remis à analyser")
        self.emit("block_error", f"bloc {block_id} : {reason}")

    # -------------------------------------------------------------------- étapes
    async def execute(self) -> RunReport:
        """Déroule les étapes ; `_run` garantit la clôture quoi qu'il arrive."""
        self._resume()
        async with LLMClient(self.cfg.llm, self.system_prompt) as client:
            self.client = client
            if not await self._negotiate_context():
                return self.report
            self._build_selected()
            self.emit(
                "start",
                f"{self.report.files_selected} fichier(s) à analyser, "
                f"{len(self.specs)} bloc(s) construit(s)",
            )
            if self.dry_run:
                return self._finish_dry_run()
            await self._send(list(self.specs))
            await self._second_pass()
        self._inherit_duplicates()
        self._cleanup_blocks()
        return self._finish()

    def _resume(self) -> None:
        """1. Reprise : blocs d'un run précédent restés sans résultat, fichiers `queued` orphelins."""
        requeued = self.db.requeue_stale()
        if requeued:
            self.say(f"reprise : {requeued} fichier(s) remis à analyser")
        leftover = self.db.pending_blocks(prompt_hash=self.prompt_hash, model=self.model)
        if leftover:
            self.report.blocks_resumed = len(leftover)
            self.say(f"reprise : {len(leftover)} bloc(s) à renvoyer")
        self.specs = list(leftover)
        self.engaged = {bf.file_id for spec in leftover for bf in spec.files}

    async def _negotiate_context(self) -> bool:
        """2. Le contexte réellement servi fait foi, et il doit être connu AVANT de
        découper : le plafond par fichier (`file_cap`) et le budget estimé d'un
        segment (`max_file_tokens`) en dérivent.

        Rend False — run clos en erreur — si le serveur est injoignable.
        """
        assert self.client is not None
        cfg = self.cfg
        prompt_reserve = self.client.prompt_reserve
        if not self.dry_run:
            served = await self.client.server_max_model_len()
            if served is not None and served != cfg.llm.max_context_tokens:
                self.say(
                    f"attention : llm.max_context_tokens={cfg.llm.max_context_tokens} mais le "
                    f"serveur sert {served} (--max-model-len) — la valeur du serveur fait foi"
                )
                if served < cfg.llm.max_context_tokens:
                    cfg.llm.max_context_tokens = served
                    # Le serveur sert moins que prévu : les blocs doivent être bâtis
                    # à sa mesure, sinon TOUS dépassent le plafond et toute la
                    # campagne part en erreur sans avoir envoyé une seule requête.
                    budget = segment_budget(cfg, prompt_reserve)
                    cfg.blocks.max_file_tokens = min(cfg.blocks.max_file_tokens or budget, budget)
                    if cfg.blocks.block_tokens > cfg.blocks.max_file_tokens:
                        cfg.blocks.block_tokens = cfg.blocks.max_file_tokens
                        self.say(
                            f"blocs ramenés à {cfg.blocks.block_tokens} tokens "
                            "pour tenir dans le contexte servi"
                        )
        self.file_cap = file_cap(cfg, prompt_reserve)
        if cfg.blocks.max_file_tokens <= 0:
            # Réserve pour le prompt système et la réponse JSON du fichier découpé.
            cfg.blocks.max_file_tokens = segment_budget(cfg, prompt_reserve)
        else:
            # Réglage explicite : il vaut aussi plafond exact, sinon le compte du
            # serveur passerait outre ce que l'opérateur a demandé.
            self.file_cap = min(self.file_cap, cfg.blocks.max_file_tokens)
        if cfg.blocks.block_tokens > cfg.blocks.max_file_tokens:
            # La part par fichier est une part par REQUÊTE : un bloc multi-fichiers
            # plus gros qu'elle ferait la même chose au débit du serveur.
            cfg.blocks.block_tokens = cfg.blocks.max_file_tokens
            self.say(
                f"blocs ramenés à {cfg.blocks.block_tokens} tokens : part par requête "
                f"blocks.max_file_share={cfg.blocks.max_file_share:g} du contexte servi"
            )
        if not self.dry_run and not await self.client.health():
            self.close("error")
            message = f"serveur LLM injoignable : {cfg.llm.base_url}"
            self.report.errors.append(message)
            self.emit("finished", message, finished=True)
            return False
        return True

    def _build_selected(self) -> None:
        """3. Sélection + construction des blocs, par lots.

        Seuls les **identifiants** sont retenus pour toute la durée du run : la liste
        complète des `FileRow` pesait 1 722 Mo pour 700 797 fichiers et restait en
        mémoire des heures durant, sur un serveur qui n'a que 8 à 16 Go. Chaque lot
        est relu juste avant d'être traité (`files_by_ids`). La liste d'identifiants
        est un instantané : elle ne bouge pas quand le run change les statuts,
        contrairement à un curseur ouvert sur la connexion qui écrit.
        """
        cfg = self.cfg
        selected = self.db.select_pending_ids(
            self.limit or 10**9, prompt_hash=self.prompt_hash, model=self.model
        )
        self.report.files_selected = len(self.engaged | set(selected))
        self.say(f"{len(selected)} fichier(s) à analyser")

        batch_size = cfg.blocks.batch_files
        with self._token_counter() as counter:
            policy = SegmentPolicy(self.file_cap, counter if counter.available else None)
            for start in range(0, len(selected), batch_size):
                if self.cancelled():
                    self.say("annulation demandée : construction des blocs interrompue")
                    break
                batch = self.db.files_by_ids(selected[start : start + batch_size])
                label = f"b{start // batch_size + 1:04d}"
                built = build_blocks(
                    batch, cfg.blocks, self.work_dir, batch_label=label, policy=policy
                )
                self._register_built(built, first_pass=True)
                self.say(
                    f"lot {label} : {len(built.blocks)} bloc(s), "
                    f"{len(built.failed)} échec(s) d'extraction"
                )

    def _token_counter(self) -> ServerTokenCounter:
        """Compteur exact du serveur pour trancher les découpages ; muet (`available`
        faux) en `--dry-run`, où aucun serveur n'est requis."""
        assert self.client is not None
        if self.dry_run:
            return ServerTokenCounter(replace(self.cfg.llm, transport="none"))
        return self.client.token_counter()

    def _register_built(self, built: BuildResult, *, first_pass: bool) -> list[BlockSpec]:
        """Enregistre en base les blocs d'un lot construit, compte les échecs
        d'extraction et retient les doublons. Rend les blocs créés.

        En première passe, un bloc au-delà du contexte du modèle n'est pas envoyé
        (ne devrait plus arriver : le builder tronque au-delà de `max_file_tokens` —
        garde-fou explicite, jamais silencieux) et les fichiers découpés comptent
        dans `files_segmented`. En seconde passe le comptage exact tranche, pas
        l'estimation, et les fichiers re-découpés comptent dans `files_resplit`.
        """
        for failed in built.failed:
            self._fail_file(failed.file_id, failed.reason)
            self.emit("file_error", f"extraction impossible : {failed.reason}")
        self.duplicates.extend(built.duplicates)
        created: list[BlockSpec] = []
        ceiling = self.cfg.llm.max_context_tokens
        for spec in built.blocks:
            if first_pass and spec.tokens_with_margin > ceiling:
                reason = (
                    f"hors plafond du modèle malgré troncature : {spec.tokens_with_margin} "
                    f"tokens > {ceiling} (llm.max_context_tokens)"
                )
                for bf in spec.files:
                    self._fail_file(bf.file_id, reason)
                self.report.blocks_skipped += 1
                self.report.errors.append(reason)
                continue
            self.db.create_block(self.run_id, spec, prompt_hash=self.prompt_hash, model=self.model)
            self.specs.append(spec)
            created.append(spec)
            self.engaged.update(bf.file_id for bf in spec.files)
            self.report.blocks_built += 1
            segmented = sum(1 for bf in spec.files if bf.segment_index == 1)
            if first_pass:
                self.report.files_segmented += segmented
            else:
                self.report.files_resplit += segmented
        return created

    def _finish_dry_run(self) -> RunReport:
        message = "dry-run : blocs construits, aucun envoi"
        self.say(message)
        for dup_id, _orig in self.duplicates:
            self.db.set_file_status(dup_id, FileStatus.PENDING)
        self.close("dry-run")
        self.emit("finished", message, finished=True)
        return self.report

    # ---------------------------------------------------------------------- envoi
    async def _send(self, specs: Sequence[BlockSpec]) -> None:
        """4. Envoie les blocs, au plus `max_in_flight` en vol, alimentés **à la demande**.

        Un `gather` sur la totalité des blocs ordonnance toutes les coroutines
        d'emblée : le test d'annulation de `_send_one` était évalué pour tous les
        blocs à l'instant zéro (donc jamais après le premier envoi), et un million de
        blocs créait un million de coroutines et de transactions SQLite. Ici chaque
        travailleur prend le bloc suivant seulement quand il est libre, et vérifie
        l'annulation à ce moment-là : « n'envoie plus de nouveau bloc » est tenu.

        Aucune exception ne doit emporter le run : une coupure de flux
        (`httpx.RemoteProtocolError`, hors `LLMError`) tuait tous les blocs en vol,
        sautait la clôture du run et laissait les fichiers `queued` pour toujours.
        """
        if not specs:
            return
        queue = iter(specs)

        async def worker() -> None:
            for spec in queue:
                if self.cancelled():
                    return
                try:
                    await self._send_one(spec)
                except Exception as exc:  # noqa: BLE001
                    self._fail_block(spec, f"erreur inattendue : {type(exc).__name__} : {exc}")

        workers = max(1, min(self.cfg.llm.max_in_flight, len(specs)))
        issues = await asyncio.gather(*(worker() for _ in range(workers)), return_exceptions=True)
        for issue in issues:
            if isinstance(issue, BaseException):
                logger.error("envoi interrompu : %s", issue)
                self.report.errors.append(f"envoi interrompu : {type(issue).__name__} : {issue}")

    async def _send_one(self, spec: BlockSpec) -> None:
        """Un bloc : contrôles, envoi, persistance de la réponse."""
        assert spec.block_id is not None
        assert self.client is not None
        if self.cancelled():
            return  # reste `built`, repris au prochain run
        if self._segments_already_analyzed(spec):
            self._skip_analyzed_segments(spec)
            return
        if not spec.path.is_file():
            self._lose_block(spec, f"bloc introuvable sur le disque : {spec.path}")
            return
        self.db.mark_block_sent(spec.block_id)
        try:
            result = await self.client.analyze_block(spec)
        except BlockTooLongError as exc:
            self._defer_too_long(spec, exc)
            return
        except OSError as exc:
            if spec.path.is_file():
                raise  # ce n'est pas la perte du bloc : au traitement générique
            self._lose_block(spec, f"bloc introuvable sur le disque : {exc}")
            return
        except LLMError as exc:
            self._fail_block(spec, f"LLM : {exc}")
            return
        try:
            parsed = parse_block_response(result.content, spec.files)
        except ParseError as exc:
            self._fail_block(spec, f"réponse illisible : {exc}")
            return
        for bf in spec.files:
            self._store_file_result(spec.block_id, bf, parsed)
        if parsed.unknown_refs:
            logger.warning(
                "bloc %s : file_ref inconnus ignorés : %s", spec.block_id, parsed.unknown_refs[:5]
            )
        self.db.mark_block_done(spec.block_id, result.usage)
        self.finished_blocks.add(spec.block_id)
        self.report.blocks_done += 1
        self.report.prompt_tokens += result.usage.prompt_tokens
        self.report.completion_tokens += result.usage.completion_tokens
        message = (
            f"bloc {spec.block_id} : {len(parsed.analyses)}/{len(spec.files)} fichiers, "
            f"{result.usage.prompt_tokens} tok prompt, {result.usage.latency_ms} ms"
        )
        self.say(message)
        self.emit("block_done", message)

    def _skip_analyzed_segments(self, spec: BlockSpec) -> None:
        """Bloc repris dont tous les segments sont déjà en base : non renvoyé."""
        assert spec.block_id is not None
        self.db.mark_block_done(spec.block_id, None)
        self.finished_blocks.add(spec.block_id)
        self.report.blocks_skipped += 1
        for bf in spec.files:
            self.db.set_block_file_outcome(spec.block_id, bf.file_id, "segment déjà analysé")
            self._aggregate_if_complete(bf, spec.block_id)
        self.say(f"bloc {spec.block_id} : segments déjà analysés, non renvoyés")

    def _defer_too_long(self, spec: BlockSpec, exc: BlockTooLongError) -> None:
        """Refusé AVANT envoi par le comptage exact : ce n'est pas une tentative du
        modèle. Le fichier repart `pending` sans consommer d'essai (un gros fichier a
        K segments : compter K échecs le mettrait en erreur avant même la seconde
        passe) et sera re-découpé sur la place réellement disponible."""
        assert spec.block_id is not None
        self.db.mark_block_error(spec.block_id, f"comptage exact : {exc}")
        self.finished_blocks.add(spec.block_id)
        self.report.blocks_error += 1
        for bf in spec.files:
            ratio, room = self.too_long.get(bf.file_id, (0.0, exc.room))
            self.too_long[bf.file_id] = (max(ratio, exc.ratio), min(room, exc.room))
            if self.second_pass:
                self.still_too_long.add(bf.file_id)
            self.db.set_block_file_outcome(spec.block_id, bf.file_id, "too long: re-découpage")
            self.db.set_file_status(bf.file_id, FileStatus.PENDING, None)
        self.emit("block_error", f"bloc {spec.block_id} : {exc} — re-découpage automatique")

    def _store_file_result(self, block_id: int, bf: BlockFile, parsed: ParsedBlock) -> None:
        """Persiste la réponse d'un fichier du bloc (analyse entière ou segment)."""
        analysis = parsed.analyses.get(bf.file_id)
        if analysis is None:
            reason = next((r for ref, r in parsed.invalid if ref == bf.file_ref), None)
            self._retry_or_fail(bf, block_id, reason or "absent de la réponse")
            return
        if bf.is_segment:
            self.db.store_segment_analysis(
                bf.file_id,
                block_id,
                bf.content_version,
                prompt_hash=self.prompt_hash,
                model=self.model,
                segment_index=bf.segment_index,
                segment_count=bf.segment_count,
                raw=analysis.raw,
            )
            self.db.set_block_file_outcome(block_id, bf.file_id, "segment done")
            self._aggregate_if_complete(bf, block_id)
            return
        self.db.store_analysis(
            bf.file_id,
            block_id,
            bf.content_version,
            prompt_hash=self.prompt_hash,
            model=self.model,
            analysis=analysis,
        )
        self.db.set_block_file_outcome(block_id, bf.file_id, "done")
        self._count_done(bf.file_id)

    # -------------------------------------------------------------- seconde passe
    async def _second_pass(self) -> None:
        """5. Fichiers dont un bloc était trop long au comptage exact : re-découpage
        sur la place réellement disponible (`BlockTooLongError.room`) et non sur le
        seul ratio réel/estimé — quand le serveur sert moins que prévu, le ratio vaut
        ~1 et le budget ne bougeait pas : le fichier n'était alors jamais analysé,
        jamais en erreur, et chaque relance recommençait."""
        if not self.too_long or self.cancelled():
            return
        retry_rows = [
            row
            for fid in sorted(self.too_long)
            if (row := self.db.get_file(fid)) is not None and row.status == FileStatus.PENDING
        ]
        if retry_rows:
            worst_ratio = max(self.too_long[row.id][0] for row in retry_rows)
            room = min(self.too_long[row.id][1] for row in retry_rows)
            budget = max(2_000, int(room / max(worst_ratio, 0.01) * RESPLIT_SAFETY))
            retry_cfg = replace(
                self.cfg.blocks,
                max_file_tokens=budget,
                block_tokens=min(self.cfg.blocks.block_tokens, budget),
            )
            self.say(
                f"seconde passe : {len(retry_rows)} fichier(s) re-découpé(s) "
                f"(ratio réel/estimé {worst_ratio:.2f}, place {room} tokens, "
                f"budget {budget} tokens)"
            )
            self.db.set_files_status([row.id for row in retry_rows], FileStatus.QUEUED)
            built = build_blocks(retry_rows, retry_cfg, self.work_dir, batch_label="r0001")
            retry_specs = self._register_built(built, first_pass=False)
            self.second_pass = True
            await self._send(retry_specs)

        # Toujours trop long après re-découpage : le dire, et le mettre en erreur.
        # Sans cela le fichier repart `pending` à chaque run, indéfiniment, et le
        # run sort « done » avec le code de retour 0.
        for fid in sorted(self.still_too_long):
            row = self.db.get_file(fid)
            if row is None or row.status != FileStatus.PENDING:
                continue
            reason = (
                "bloc encore trop long après re-découpage pour le contexte servi "
                f"({self.cfg.llm.max_context_tokens} tokens) : réduisez blocks.block_tokens / "
                "llm.max_tokens_cap, ou servez un --max-model-len plus grand"
            )
            self._fail_file(fid, reason)
            self.report.errors.append(f"fichier {fid} ({row.name}) : {reason}")

    # -------------------------------------------------------------------- clôture
    def _inherit_duplicates(self) -> None:
        """Doublons exacts : héritent de l'analyse de leur original (même contenu)."""
        for dup_id, orig_id in self.duplicates:
            dup = self.db.get_file(dup_id)
            if dup is None:
                continue
            if self.db.copy_analysis(
                orig_id, dup_id, dup.content_version, prompt_hash=self.prompt_hash, model=self.model
            ):
                self._count_done(dup_id)
                self.report.files_duplicates += 1
            else:
                # Original pas (encore) analysé : le doublon sera analysé pour
                # lui-même au prochain run (l'original ne sera plus dans le lot).
                self.db.set_file_status(dup_id, FileStatus.PENDING)
        if self.report.files_duplicates:
            self.say(
                f"{self.report.files_duplicates} doublon(s) exact(s) : analyse héritée de l'original"
            )

    def _cleanup_blocks(self) -> None:
        """`keep_blocks = false` : n'efface que les `.md` des blocs `done`/`error` —
        effacer un bloc resté `built` (annulation) le rend illisible à la reprise et
        bloque la campagne."""
        if self.cfg.blocks.keep_blocks:
            return
        for spec in self.specs:
            if spec.block_id in self.finished_blocks:
                spec.path.unlink(missing_ok=True)

    def _finish(self) -> RunReport:
        if self.cancelled():
            self.close("cancelled")
            self.say("run annulé — relancer pour reprendre")
            self.emit("cancelled", "run annulé — relancer pour reprendre", cancelled=True)
            self.emit("finished", "run annulé", finished=True, cancelled=True)
            return self.report

        # Aucun fichier embarqué dans un bloc ne doit rester en plan sans que le run le
        # dise : un run qui n'a rien analysé ne sort jamais « done » avec le code 0.
        remaining, examples = self.db.unfinished_files(sorted(self.engaged))
        if remaining:
            more = "…" if remaining > len(examples) else ""
            self.report.errors.append(
                f"{remaining} fichier(s) engagé(s) dans un bloc n'ont été ni analysés ni mis en "
                f"erreur (à reprendre) : {', '.join(examples)}{more}"
            )
            self.say(self.report.errors[-1])
        self.close("done" if not self.report.errors else "error")
        self.emit("finished", f"run {self.run_id} terminé", finished=True)
        return self.report
