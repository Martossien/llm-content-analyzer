"""Étape 0 : import d'un CSV SMBeagle, scan piloté, préparation (plan)."""

from __future__ import annotations

import json
import os
import threading
import time
from collections.abc import Callable
from pathlib import Path

from docia.config import Config
from docia.db import Database
from docia.filter import PlanProgress, PlanReport, plan_files
from docia.ingest.smbeagle_csv import ImportProgress, ImportReport, import_csv
from docia.scan import ScanError, ScanEvent, ScanProfile, ScanResult, run_scan, scope_warnings
from docia.service._common import ServiceError, _stamp, logger
from docia.service.campaigns import remember_campaign
from docia.views import format_int


def format_import_progress(progress: ImportProgress) -> str:
    """Ligne de journal d'un import en cours : lignes, pourcentage, durée.

    Le pourcentage vient des octets lus (voir `ImportProgress`) : il est honnête
    dès la première seconde, alors que le nombre total de lignes reste inconnu.
    """
    lines = format_int(progress.rows)
    invalid = f" ({progress.invalid} invalides)" if progress.invalid else ""
    return (
        f"intégration : {lines} lignes{invalid} — "
        f"{progress.percent:.0f} % — {progress.elapsed_s:.0f} s"
    )


def format_import_report(report: ImportReport, *, prefix: str = "import") -> str:
    """Bilan d'un import terminé, en une ligne — la même pour tous les clients.

    `prefix` porte le contexte : `docia ingest` annonce « scan 12 : … » (le
    numéro de scan sert à `docia status`), l'interface et `docia scan` se
    contentent de « import : … ». La ligne elle-même — total, nouveaux,
    modifiés, inchangés, invalides — n'est écrite qu'ici : elle était recopiée à
    trois endroits, et les trois avaient déjà divergé.
    """
    # Une taille illisible retombe à zéro, donc le fichier sera exclu « trop petit » :
    # sans ce compteur, il sortait de l'audit sans que personne ne l'apprenne.
    tailles = f" — {report.size_defaulted} taille(s) illisible(s)" if report.size_defaulted else ""
    return (
        f"{prefix} : {report.total} lignes — {report.new} nouveaux, "
        f"{report.updated} modifiés, {report.unchanged} inchangés, "
        f"{report.invalid} invalides{tailles}"
    )


def import_progress_logger(
    log: Callable[[str], None], *, min_seconds: float = 2.0, min_rows: int = 50_000
) -> Callable[[ImportProgress], None]:
    """Rappel d'avancement d'import qui écrit dans `log` sans l'inonder.

    Une ligne au démarrage, puis au plus une toutes les `min_seconds` secondes ou
    tous les `min_rows` lignes, **et toujours une à la fin** : le dernier appel
    porte `ImportProgress.final` et court-circuite l'étranglement. Sans cela, un
    import de 934 028 lignes s'arrêtait sur « 900 000 lignes — 96 % » et un
    import de trois lignes sur « 0 lignes — 0 % ». Partagé par la CLI,
    l'interface et le futur serveur web : la même progression pour tout le monde.
    """
    last_rows = 0
    last_at = 0.0
    last_line = ""
    started = False

    def emit(progress: ImportProgress) -> None:
        nonlocal last_rows, last_at, last_line, started
        now = time.monotonic()
        if (
            not progress.final
            and started
            and progress.rows - last_rows < min_rows
            and now - last_at < min_seconds
        ):
            return
        line = format_import_progress(progress)
        if progress.final and started and line == last_line:
            return  # le dernier lot vient d'annoncer exactement la même chose
        started, last_rows, last_at, last_line = True, progress.rows, now, line
        log(line)

    return emit


def import_scan(
    db: Database,
    csv_path: Path,
    *,
    strict: bool = False,
    progress: Callable[[ImportProgress], None] | None = None,
) -> ImportReport:
    """Importe un CSV SMBeagle et mémorise la campagne dans les récentes.

    `strict=False` (défaut de l'interface) tolère les lignes invalides : elles
    sont comptées dans le rapport plutôt que d'interrompre l'import.

    `progress` est le rappel d'avancement d'`import_csv` (voir
    `import_progress_logger` pour la version « une ligne de journal »).
    """
    path = Path(csv_path)
    if not path.exists():
        raise ServiceError(f"fichier de scan introuvable : {path}")
    try:
        report = import_csv(db, path, strict=strict, progress=progress)
    except OSError as exc:
        raise ServiceError(f"lecture impossible du scan {path} : {exc}") from exc
    logger.info(
        "import %s : %s lignes (%s nouveaux, %s modifiés, %s invalides)",
        path,
        report.total,
        report.new,
        report.updated,
        report.invalid,
    )
    remember_campaign(db.path, path)
    return report


def scan_campaign(
    db: Database,
    cfg: Config,
    profile: ScanProfile,
    *,
    csv_out: Path | None = None,
    on_event: Callable[[ScanEvent], None] | None = None,
    on_line: Callable[[str], None] | None = None,
    on_import_progress: Callable[[ImportProgress], None] | None = None,
    on_plan_progress: Callable[[PlanProgress], None] | None = None,
    cancel: threading.Event | None = None,
    password: str = "",
    do_plan: bool = True,
) -> tuple[ScanResult, ImportReport, PlanReport]:
    """Étape 0 complète : scanner SMBeagle → import du CSV → préparation (plan).

    Le CSV est écrit à côté de la base (`<base>.scans/scan_AAAAMMJJ-HHMMSS.csv`) avec son
    manifeste ; le scan importé porte `kind='scan'` et le manifeste. Un scan arrêté
    par `cancel` est quand même importé (CSV partiel : ce qui a été vu est utile).
    Le mot de passe SMB ne vient jamais de la config : argument ou `DOCIA_SMB_PASSWORD`.

    Un périmètre amputé — cible écartée par le scanner, arrêt demandé — est
    **écrit en base** (`scans.complete`, `skipped_json`, `cancelled`) en même temps
    que le manifeste, et annoncé à l'appelant par `on_line`. Importer un scan
    partiel reste le bon choix (ce qui a été vu est utile) ; n'en garder aucune
    trace ne l'était pas : le rapport présentait ensuite un fragment comme un
    inventaire exhaustif.
    """
    profile.preserve_access_time = cfg.scan.preserve_access_time
    profile.skip_acls = cfg.scan.skip_acls
    profile.exclude_hidden_shares = cfg.scan.exclude_hidden_shares
    if cfg.scan.username and not profile.username:
        profile.domain, profile.username = cfg.scan.domain, cfg.scan.username
    if profile.username and not profile.password:
        profile.password = password or os.environ.get("DOCIA_SMB_PASSWORD", "")
    target = csv_out or scans_dir_for(db.path) / f"scan_{_stamp()}.csv"
    try:
        result = run_scan(
            profile,
            target,
            configured_exe=cfg.scan.smbeagle_path,
            on_event=on_event,
            on_line=on_line,
            cancel=cancel,
        )
    except ScanError as exc:
        raise ServiceError(str(exc)) from exc
    report = import_scan(db, result.csv_path, strict=False, progress=on_import_progress)
    db.annotate_scan(
        report.scan_id,
        manifest_json=json.dumps(result.manifest, ensure_ascii=False) if result.manifest else "",
        scanner_elapsed_s=result.elapsed_s,
        skipped=result.skipped,
        cancelled=result.cancelled,
        exit_code=result.exit_code,
        expected_files=result.expected_files,
    )
    if not result.complete:
        # Journal seulement, jamais `on_line` : les deux façades affichent déjà ces
        # avertissements pour leur compte (la CLI par le gestionnaire console du
        # journal, la fenêtre par `tab_home`), et les pousser aussi ici les faisait
        # sortir **deux fois** à l'écran. `logger` reste le chemin unique vers
        # `docia.log`, le fichier qu'on demande de joindre en cas de souci.
        for ligne in scope_warnings(
            skipped=result.skipped,
            cancelled=result.cancelled,
            expected_files=result.expected_files,
            files=result.files,
        ):
            logger.warning("%s", ligne)
    plan_report = (
        plan(db, cfg, progress=on_plan_progress) if do_plan else PlanReport(pending=0, excluded=0)
    )
    return result, report, plan_report


def scans_dir_for(db_path: Path) -> Path:
    """Dossier des CSV produits par le scanner, à côté de la base (`<base>.scans/`)."""
    return Path(str(db_path) + ".scans")


def plan(
    db: Database, cfg: Config, *, progress: Callable[[PlanProgress], None] | None = None
) -> PlanReport:
    """Applique exclusions et scores de priorité à toute la base.

    `progress` : rappel d'avancement (voir `filter.plan_progress_logger`) — une
    préparation d'un million de fichiers dure une minute, muette sans lui.
    """
    report = plan_files(db, cfg.filter, progress=progress)
    logger.info("plan : %s à analyser, %s exclus", report.pending, report.excluded)
    return report


# ------------------------------------------------------------------------ run
