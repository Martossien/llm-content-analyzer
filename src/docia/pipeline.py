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
from docia.llm.client import LLMClient, LLMError
from docia.llm.parse import ParseError, parse_block_response
from docia.llm.schema import load_system_prompt, prompt_hash
from docia.models import BlockSpec, FileStatus

logger = logging.getLogger(__name__)

MAX_FILE_ATTEMPTS = 2
SYSTEM_PROMPT_RESERVE_TOKENS = 2_000
"""Tokens gardés libres pour le prompt système et le gabarit de conversation."""


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
fois (dans un autre bloc), puis passe en `error` — jamais de boucle infinie."""


@dataclass
class RunReport:
    run_id: int
    files_selected: int = 0
    files_done: int = 0
    files_error: int = 0
    blocks_built: int = 0
    blocks_resumed: int = 0
    blocks_skipped: int = 0
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
    if cfg.blocks.max_file_tokens <= 0:
        # Réserve pour le prompt système et la réponse JSON du fichier tronqué.
        cfg.blocks.max_file_tokens = max(
            2_000, cfg.llm.max_context_tokens - output_reserve_tokens(cfg.llm)
        )

    # 1. Reprise : blocs d'un run précédent restés sans résultat, fichiers `queued` orphelins.
    requeued = db.requeue_stale()
    if requeued:
        say(f"reprise : {requeued} fichier(s) remis à analyser")
    leftover = db.pending_blocks(prompt_hash=phash, model=cfg.llm.model)
    if leftover:
        report.blocks_resumed = len(leftover)
        say(f"reprise : {len(leftover)} bloc(s) à renvoyer")

    # 2. Sélection + construction des blocs, par lots.
    selected = db.select_pending(limit or 10**9, prompt_hash=phash, model=cfg.llm.model)
    report.files_selected = len(selected)
    say(f"{len(selected)} fichier(s) à analyser")
    specs: list[BlockSpec] = list(leftover)
    duplicates: list[tuple[int, int]] = []

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

    for start in range(0, len(selected), cfg.blocks.batch_files):
        if cancelled():
            say("annulation demandée : construction des blocs interrompue")
            break
        batch = selected[start : start + cfg.blocks.batch_files]
        label = f"b{start // cfg.blocks.batch_files + 1:04d}"
        built = build_blocks(batch, cfg.blocks, work_dir, batch_label=label)
        for failed in built.failed:
            db.set_file_status(failed.file_id, FileStatus.ERROR, failed.reason)
            report.files_error += 1
            emit("file_error", f"extraction impossible : {failed.reason}")
        duplicates.extend(built.duplicates)
        for spec in built.blocks:
            if spec.tokens_with_margin > cfg.llm.max_context_tokens:
                # Ne devrait plus arriver : le builder tronque au-delà de
                # `max_file_tokens`. Garde-fou explicite, jamais silencieux.
                reason = (
                    f"hors plafond du modèle malgré troncature : {spec.tokens_with_margin} tokens "
                    f"> {cfg.llm.max_context_tokens} (llm.max_context_tokens)"
                )
                for bf in spec.files:
                    db.set_file_status(bf.file_id, FileStatus.ERROR, reason)
                    report.files_error += 1
                report.blocks_skipped += 1
                continue
            db.create_block(run_id, spec, prompt_hash=phash, model=cfg.llm.model)
            specs.append(spec)
            report.blocks_built += 1
            report.files_segmented += sum(1 for bf in spec.files if bf.segment_index == 1)
        say(f"lot {label} : {len(built.blocks)} bloc(s), {len(built.failed)} échec(s) d'extraction")

    emit("start", f"{len(selected)} fichier(s) à analyser, {len(specs)} bloc(s) construit(s)")

    if dry_run:
        say("dry-run : blocs construits, aucun envoi")
        for dup_id, _orig in duplicates:
            db.set_file_status(dup_id, FileStatus.PENDING)
        db.finish_run(run_id, "dry-run")
        emit("finished", "dry-run : blocs construits, aucun envoi", finished=True)
        return report

    # 3. Envoi asynchrone, N en vol (sémaphore dans le client).
    async with LLMClient(cfg.llm, system_prompt) as client:
        if not await client.health():
            db.finish_run(run_id, "error")
            report.errors.append(f"serveur LLM injoignable : {cfg.llm.base_url}")
            emit("finished", f"serveur LLM injoignable : {cfg.llm.base_url}", finished=True)
            return report

        async def one(spec: BlockSpec) -> None:
            assert spec.block_id is not None
            if cancelled():
                return  # reste `built`, repris au prochain run
            db.mark_block_sent(spec.block_id)
            try:
                result = await client.analyze_block(spec)
            except LLMError as exc:
                _fail_block(db, spec, f"LLM : {exc}", report)
                emit("block_error", f"bloc {spec.block_id} : LLM : {exc}")
                return
            try:
                parsed = parse_block_response(result.content, spec.files)
            except ParseError as exc:
                _fail_block(db, spec, f"réponse illisible : {exc}", report)
                emit("block_error", f"bloc {spec.block_id} : réponse illisible : {exc}")
                return
            for bf in spec.files:
                analysis = parsed.analyses.get(bf.file_id)
                if analysis is None:
                    reason = next((r for ref, r in parsed.invalid if ref == bf.file_ref), None)
                    _retry_or_fail(
                        db, bf.file_id, spec.block_id, reason or "absent de la réponse", report
                    )
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
                    done_segments = db.segment_analyses(
                        bf.file_id, bf.content_version, prompt_hash=phash, model=cfg.llm.model
                    )
                    if len(done_segments) < bf.segment_count:
                        continue  # on attend les autres segments (même run ou suivant)
                    merged = aggregate_segments(
                        bf.file_ref.rsplit(" [partie", 1)[0], [raw for _, _, raw in done_segments]
                    )
                    db.store_analysis(
                        bf.file_id,
                        spec.block_id,
                        bf.content_version,
                        prompt_hash=phash,
                        model=cfg.llm.model,
                        analysis=merged,
                        segments=bf.segment_count,
                    )
                    report.files_done += 1
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
                report.files_done += 1
            if parsed.unknown_refs:
                logger.warning(
                    "bloc %s : file_ref inconnus ignorés : %s",
                    spec.block_id,
                    parsed.unknown_refs[:5],
                )
            db.mark_block_done(spec.block_id, result.usage)
            report.blocks_done += 1
            report.prompt_tokens += result.usage.prompt_tokens
            report.completion_tokens += result.usage.completion_tokens
            message = (
                f"bloc {spec.block_id} : {len(parsed.analyses)}/{len(spec.files)} fichiers, "
                f"{result.usage.prompt_tokens} tok prompt, {result.usage.latency_ms} ms"
            )
            say(message)
            emit("block_done", message)

        await asyncio.gather(*(one(spec) for spec in specs))

    # Doublons exacts : héritent de l'analyse de leur original (même contenu).
    for dup_id, orig_id in duplicates:
        dup = db.get_file(dup_id)
        if dup is None:
            continue
        if db.copy_analysis(
            orig_id, dup_id, dup.content_version, prompt_hash=phash, model=cfg.llm.model
        ):
            report.files_done += 1
            report.files_duplicates += 1
        else:
            # Original pas (encore) analysé : le doublon sera analysé pour
            # lui-même au prochain run (l'original ne sera plus dans le lot).
            db.set_file_status(dup_id, FileStatus.PENDING)
    if report.files_duplicates:
        say(f"{report.files_duplicates} doublon(s) exact(s) : analyse héritée de l'original")

    if not cfg.blocks.keep_blocks:
        for spec in specs:
            spec.path.unlink(missing_ok=True)
    if cancelled():
        db.finish_run(run_id, "cancelled")
        say("run annulé — relancer pour reprendre")
        emit("cancelled", "run annulé — relancer pour reprendre", cancelled=True)
        emit("finished", "run annulé", finished=True, cancelled=True)
    else:
        db.finish_run(run_id, "done" if not report.errors else "error")
        emit("finished", f"run {run_id} terminé", finished=True)
    return report


def _fail_block(db: Database, spec: BlockSpec, error: str, report: RunReport) -> None:
    assert spec.block_id is not None
    logger.error("bloc %s en erreur : %s", spec.block_id, error)
    db.mark_block_error(spec.block_id, error)
    report.blocks_error += 1
    report.errors.append(f"bloc {spec.block_id} : {error}")
    for bf in spec.files:
        _retry_or_fail(db, bf.file_id, spec.block_id, error, report)


def _retry_or_fail(
    db: Database, file_id: int, block_id: int, reason: str, report: RunReport
) -> None:
    """Un fichier sans résultat repart `pending` pour un autre bloc, jusqu'à
    `MAX_FILE_ATTEMPTS` blocs tentés ; ensuite `error` avec la raison."""
    attempts = db.file_attempts(file_id)
    db.set_block_file_outcome(block_id, file_id, f"failed: {reason[:200]}")
    if attempts >= MAX_FILE_ATTEMPTS:
        db.set_file_status(file_id, FileStatus.ERROR, reason[:500])
        report.files_error += 1
    else:
        db.set_file_status(file_id, FileStatus.PENDING, None)
