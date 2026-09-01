"""Couche service : toute opération métier en fonctions typées, sans Tk ni argparse.

La CLI et la GUI sont des clients minces de ce module ; l'API REST prévue en v4
(`docia serve`) exposera ces fonctions 1 : 1. Rien ici n'imprime : les
opérations journalisent (`logging`) et lèvent `ServiceError` avec un message en
français lisible par un utilisateur non technique — jamais de trace brute.
"""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import sqlite3
import threading
import time
from collections.abc import Callable, Sequence
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from docia.config import Config
from docia.db import Database, backup_dir_for
from docia.filter import PlanProgress, PlanReport, plan_files
from docia.ingest.smbeagle_csv import ImportProgress, ImportReport, import_csv
from docia.llm.schema import prompt_hash
from docia.models import FileStatus
from docia.pipeline import RunReport, resolve_system_prompt, run_pipeline
from docia.scan import ScanError, ScanEvent, ScanProfile, ScanResult, run_scan, scope_warnings
from docia.views import RunStat, format_int, runs_summary

logger = logging.getLogger(__name__)

__all__ = [
    "scan_campaign",
    "scans_dir_for",
    "ScanProfile",
    "ScanEvent",
    "ScanResult",
    "BACKUP_SUFFIX",
    "DEFAULT_KEEP_BACKUPS",
    "HOME_ENV",
    "MAX_RECENT",
    "REANALYZE_SCOPES",
    "RECENT_FILE",
    "SAFETY_LABEL_PREFIX",
    "WHERE_KEYS",
    "CampaignStatus",
    "ImportProgress",
    "RecentCampaign",
    "RunEvent",
    "ServiceError",
    "backup_database",
    "backup_dir_for",
    "campaign_status",
    "docia_home",
    "format_import_progress",
    "format_import_report",
    "forget_campaign",
    "import_progress_logger",
    "import_scan",
    "list_backups",
    "plan",
    "reanalyze",
    "recent_campaigns",
    "remember_campaign",
    "restore_database",
    "run_campaign",
    "set_review",
]
"""Surface publique du service : ce que la CLI, la GUI et l'API REST utilisent."""

HOME_ENV = "DOCIA_HOME"
"""Variable d'environnement qui redirige le dossier de configuration (tests, poste verrouillé)."""

RECENT_FILE = "recent.json"
MAX_RECENT = 20
DEFAULT_KEEP_BACKUPS = 10
"""Sauvegardes **courantes** conservées par la rotation — seule source de vérité.

`cli.py` doit importer cette valeur (`--keep`) plutôt que d'en redéfinir une : elle
gouverne ce que la rotation supprime, et deux valeurs qui divergent, c'est une
campagne effacée un jour où l'on croyait en garder dix.
"""
BACKUP_SUFFIX = ".sqlite"
SAFETY_LABEL_PREFIX = "avant_"
"""Étiquette d'une **copie de sûreté** : filet posé juste avant une opération
destructrice (`avant_migration_*`, `avant_restauration`, `avant_reanalyse_*`).

Ce ne sont pas des sauvegardes courantes : elles n'entrent jamais dans le vivier
des `DEFAULT_KEEP_BACKUPS` copies tournantes. Une rotation qui les emportait
supprimait précisément le filet dont on a besoin quand l'opération a mal tourné.
Elles restent listées par `list_backups` (l'utilisateur doit pouvoir les
restaurer) et se suppriment à la main.
"""
REANALYZE_SCOPES = ("all", "errors", "pending_only", "filter")
WHERE_KEYS = ("security", "rgpd", "owner", "extension", "path_like")


class ServiceError(Exception):
    """Erreur métier destinée à l'utilisateur (message en français, sans trace)."""


# --------------------------------------------------------------------- modèles


@dataclass(frozen=True)
class CampaignStatus:
    """Photographie d'une campagne : avancement, risques, prompt actif, dernier run."""

    db_path: Path
    files: int
    pending: int
    queued: int
    done: int
    error: int
    excluded: int
    analyses: int
    blocks_built: int
    blocks_sent: int
    blocks_done: int
    blocks_error: int
    reviewed: int
    to_review: int
    security: dict[str, int]
    rgpd: dict[str, int]
    active_prompt: str
    last_run: RunStat | None
    schema_version: int

    @property
    def percent_done(self) -> float:
        """Part des fichiers analysés parmi ceux qui doivent l'être (hors exclus)."""
        target = self.files - self.excluded
        return round(100.0 * self.done / target, 1) if target > 0 else 0.0


@dataclass(frozen=True)
class RunEvent:
    """Un événement de progression d'un run (ce que voit une barre d'avancement)."""

    kind: str
    """`info` | `block_done` | `block_error` | `file_error` | `cancelled` | `finished`."""
    message: str
    files_done: int
    files_total: int
    files_error: int
    blocks_done: int
    blocks_total: int
    elapsed_s: float
    eta_s: float | None
    files_per_hour: float | None


@dataclass(frozen=True)
class RecentCampaign:
    """Une campagne récemment ouverte (liste `recent.json`)."""

    db_path: Path
    csv_path: Path | None
    last_opened: str
    label: str


# ------------------------------------------------------------------- helpers


def _now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def _stamp() -> str:
    return datetime.now().strftime("%Y%m%d-%H%M%S")


def _slug(text: str) -> str:
    """Étiquette réduite aux caractères sûrs dans un nom de fichier Windows."""
    keep = [c if (c.isalnum() or c in "-_") else "_" for c in text.strip()]
    return "".join(keep).strip("_")[:40]


def _effective_keys(db: Database, cfg: Config) -> tuple[str, str]:
    """(empreinte de prompt effective, modèle courant) — la clé d'une analyse."""
    return prompt_hash(resolve_system_prompt(db, cfg), cfg.llm.model), cfg.llm.model


def docia_home() -> Path:
    """Dossier de configuration : `$DOCIA_HOME`, `%APPDATA%/docia` ou `~/.config/docia`."""
    override = os.environ.get(HOME_ENV, "").strip()
    if override:
        return Path(override)
    appdata = os.environ.get("APPDATA", "").strip()
    if os.name == "nt" and appdata:
        return Path(appdata) / "docia"
    return Path.home() / ".config" / "docia"


# -------------------------------------------------------------------- statut


def campaign_status(db: Database) -> CampaignStatus:
    """Compteurs, classifications, revues, prompt actif et dernier run d'une campagne."""
    counts = db.counts()
    classes = db.classification_summary()
    reviews = db.review_counts()
    runs = runs_summary(db)
    active = db.active_prompt()
    last_run = max(runs, key=lambda r: r.run_id) if runs else None
    return CampaignStatus(
        db_path=db.path,
        files=counts.get("files", 0),
        pending=counts.get(FileStatus.PENDING.value, 0),
        queued=counts.get(FileStatus.QUEUED.value, 0),
        done=counts.get(FileStatus.DONE.value, 0),
        error=counts.get(FileStatus.ERROR.value, 0),
        excluded=counts.get(FileStatus.EXCLUDED.value, 0),
        analyses=counts.get("analyses", 0),
        blocks_built=counts.get("blocks_built", 0),
        blocks_sent=counts.get("blocks_sent", 0),
        blocks_done=counts.get("blocks_done", 0),
        blocks_error=counts.get("blocks_error", 0),
        reviewed=reviews.get("validated", 0) + reviews.get("corrected", 0),
        to_review=reviews.get("to_review", 0),
        security=dict(classes.get("security", {})),
        rgpd=dict(classes.get("rgpd", {})),
        active_prompt=active[0] if active else "(embarqué)",
        last_run=last_run,
        schema_version=db.schema_version,
    )


# ------------------------------------------------------------------ ingestion


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


class _Pace:
    """Cadence observée d'un run : débit à partir du premier bloc terminé, et ETA."""

    def __init__(self) -> None:
        self.started = time.monotonic()
        self.first_done_at: float | None = None
        self.files_at_first_done = 0

    def note_block_done(self, files_done: int) -> None:
        if self.first_done_at is None:
            self.first_done_at = time.monotonic()
            self.files_at_first_done = files_done

    def rate_per_s(self, files_done: int) -> float | None:
        """Fichiers par seconde depuis le premier bloc terminé (régime établi)."""
        if self.first_done_at is not None:
            span = time.monotonic() - self.first_done_at
            produced = files_done - self.files_at_first_done
            if span > 0.0 and produced > 0:
                return produced / span
        span = time.monotonic() - self.started
        if span > 0.0 and files_done > 0:
            return files_done / span
        return None

    def eta_s(self, files_done: int, files_error: int, files_total: int) -> float | None:
        rate = self.rate_per_s(files_done)
        remaining = files_total - files_done - files_error
        if rate is None or rate <= 0.0 or remaining <= 0:
            return None
        return round(remaining / rate, 1)

    def elapsed_s(self) -> float:
        return round(time.monotonic() - self.started, 3)


def _as_int(payload: dict[str, object], key: str) -> int:
    """Entier d'un événement du pipeline (0 si absent ou inattendu)."""
    value = payload.get(key)
    return value if isinstance(value, int) else 0


_EVENT_KINDS = {
    "start": "info",
    "info": "info",
    "block_done": "block_done",
    "block_error": "block_error",
    "file_error": "file_error",
    "cancelled": "cancelled",
    "finished": "finished",
}


def run_campaign(
    db: Database,
    cfg: Config,
    *,
    limit: int | None = None,
    dry_run: bool = False,
    on_event: Callable[[RunEvent], None] | None = None,
    cancel: threading.Event | None = None,
) -> RunReport:
    """Lance un run et rend un `RunEvent` enrichi (durée, reste à faire, débit) par étape.

    Enveloppe `pipeline.run_pipeline` : le pipeline reste la seule implémentation,
    ce service n'ajoute que la mesure de cadence. Une erreur de configuration ou
    d'accès à la base devient un `ServiceError`.
    """
    pace = _Pace()

    def forward(payload: dict[str, object]) -> None:
        if on_event is None:
            return
        raw_kind = str(payload.get("event", "info"))
        files_done = _as_int(payload, "files_done")
        files_total = _as_int(payload, "files_total")
        files_error = _as_int(payload, "files_error")
        if raw_kind == "block_done":
            pace.note_block_done(files_done)
        event = RunEvent(
            kind=_EVENT_KINDS.get(raw_kind, "info"),
            message=str(payload.get("message", "")),
            files_done=files_done,
            files_total=files_total,
            files_error=files_error,
            blocks_done=_as_int(payload, "blocks_done"),
            blocks_total=_as_int(payload, "blocks_total"),
            elapsed_s=pace.elapsed_s(),
            eta_s=pace.eta_s(files_done, files_error, files_total),
            files_per_hour=_per_hour(pace.rate_per_s(files_done)),
        )
        try:
            on_event(event)
        except Exception:  # pragma: no cover - un afficheur ne doit jamais casser le run
            logger.exception("callback de progression en erreur (ignoré)")

    try:
        return run_pipeline(
            db,
            cfg,
            limit=limit,
            dry_run=dry_run,
            on_progress=forward if on_event is not None else None,
            cancel=cancel,
        )
    except sqlite3.Error as exc:
        raise ServiceError(f"base inutilisable pendant le run : {exc}") from exc
    except OSError as exc:
        raise ServiceError(f"run impossible (accès fichier) : {exc}") from exc


def _per_hour(rate_per_s: float | None) -> float | None:
    return round(rate_per_s * 3600.0, 1) if rate_per_s else None


# ------------------------------------------------------------------ réanalyse


def _where_clauses(where: dict[str, str]) -> tuple[list[str], list[object]]:
    """Traduit `where` en conditions SQL sur `files f` (+ dernière analyse `a`)."""
    clauses: list[str] = []
    params: list[object] = []
    for key, value in where.items():
        if key not in WHERE_KEYS:
            raise ServiceError(
                f"critère de sélection inconnu : « {key} » (attendu : {', '.join(WHERE_KEYS)})"
            )
        text = str(value).strip()
        if not text:
            raise ServiceError(f"critère « {key} » sans valeur")
        if key == "security":
            clauses.append("a.security_classification = ?")
            params.append(text)
        elif key == "rgpd":
            clauses.append("a.rgpd_risk_level = ?")
            params.append(text)
        elif key == "owner":
            clauses.append("f.owner = ?")
            params.append(text)
        elif key == "extension":
            clauses.append("LOWER(f.extension) = ?")
            params.append(text.lower().lstrip("."))
        else:  # path_like
            clauses.append("f.path LIKE ?")
            params.append(text)
    return clauses, params


def _targets(db: Database, scope: str, where: dict[str, str] | None) -> list[int]:
    """Identifiants des fichiers visés par une réanalyse (jamais les exclus)."""
    latest = (
        " LEFT JOIN analyses a ON a.id = (SELECT id FROM analyses WHERE file_id=f.id"
        " ORDER BY created_at DESC, id DESC LIMIT 1)"
    )
    clauses = ["f.status <> 'excluded'"]
    params: list[object] = []
    if scope == "pending_only":
        clauses = ["f.status = 'pending'"]
    elif scope == "filter":
        if not where:
            raise ServiceError(
                "réanalyse ciblée : préciser au moins un critère (--where clé=valeur)"
            )
        extra, extra_params = _where_clauses(where)
        clauses.extend(extra)
        params.extend(extra_params)
    sql = f"SELECT f.id AS id FROM files f{latest} WHERE {' AND '.join(clauses)} ORDER BY f.id"  # noqa: S608
    return [int(r["id"]) for r in db.query(sql, tuple(params))]


def reanalyze(
    db: Database,
    cfg: Config,
    *,
    scope: str,
    where: dict[str, str] | None = None,
    backup: bool = True,
) -> int:
    """Force la réanalyse de fichiers déjà traités et rend leur nombre.

    `scope` : `all` (toute la campagne, hors exclus), `errors` (fichiers en
    erreur remis à analyser, sans rien supprimer), `pending_only` (nettoie les
    analyses des fichiers déjà à analyser), `filter` (sélection `where` :
    `security`, `rgpd`, `owner`, `extension`, `path_like`).

    Les analyses supprimées sont celles de la clé courante — empreinte du prompt
    effectif et modèle configuré : changer de prompt ou de modèle provoque déjà
    une réanalyse sans rien effacer. Une sauvegarde est prise avant l'opération
    (`backup=False` pour la désactiver, à réserver aux tests).

    **Atomicité — limite connue.** Remettre les fichiers `pending` et supprimer
    leurs analyses sont deux écritures, et `Database` n'expose aujourd'hui aucune
    opération qui les enchaîne dans une seule transaction (`set_files_status` et
    `delete_analyses` ouvrent chacune la leur). Une coupure entre les deux laisse
    donc un état intermédiaire ; l'ordre choisi ci-dessous le rend visible et
    réparable en rejouant la même commande, mais seule une méthode
    `Database.reset_for_reanalysis(...)` (une transaction, les deux écritures)
    supprimerait complètement la fenêtre.
    """
    if scope not in REANALYZE_SCOPES:
        raise ServiceError(
            f"portée de réanalyse inconnue : « {scope} » (attendu : {', '.join(REANALYZE_SCOPES)})"
        )
    if backup:
        backup_database(db.path, label=f"avant_reanalyse_{scope}", db=db)
    if scope == "errors":
        count = db.reset_errors()
        logger.info("réanalyse (erreurs) : %s fichier(s) remis à analyser", count)
        return count

    file_ids = _targets(db, scope, where)
    if not file_ids:
        logger.info("réanalyse (%s) : aucun fichier ciblé", scope)
        return 0
    phash, model = _effective_keys(db, cfg)
    # L'ordre compte, et il n'est pas anodin : `db.py` n'expose pas d'opération qui
    # fasse les deux écritures dans une seule transaction (voir la note ci-dessus),
    # donc une coupure entre les deux est possible. Remettre les fichiers `pending`
    # **d'abord** rend cet état intermédiaire visible et réparable :
    #   - visible : la campagne affiche 0 % au lieu de mentir avec « 100 % analysé » ;
    #   - réparable : rejouer *la même* commande de réanalyse retrouve les fichiers
    #     (ils sont `pending`, et leurs analyses sont toujours là pour un `--where`)
    #     et termine le travail.
    # Dans l'ordre inverse, la coupure laissait des fichiers `done` sans analyse :
    # `run`, `retry`, `plan` et un `reanalyze` ciblé n'en voyaient plus aucun, et
    # seul `reanalyze --all` — que personne n'a de raison de tenter — réparait.
    db.set_files_status(file_ids, FileStatus.PENDING, None)
    deleted = db.delete_analyses(file_ids, prompt_hash=phash, model=model)
    logger.info(
        "réanalyse (%s) : %s fichier(s) remis à analyser, %s analyse(s) supprimée(s)",
        scope,
        len(file_ids),
        deleted,
    )
    return len(file_ids)


# -------------------------------------------------------- vérification humaine


def set_review(
    db: Database,
    file_id: int,
    status: str,
    *,
    comment: str = "",
    reviewer: str = "",
    corrected_security: str | None = None,
    corrected_rgpd: str | None = None,
    corrected_retention_years: int | None = None,
) -> sqlite3.Row | None:
    """Enregistre la vérification humaine d'un fichier et rend sa fiche relue.

    `status` : `to_review`, `validated` ou `corrected`. Rend la ligne telle qu'elle
    est **après** écriture (`None` si le fichier a disparu) : l'appelant réaffiche
    la seule ligne concernée sans rouvrir la base.

    Cette fonction n'ajoute rien à `Database.set_review` — c'est justement le point.
    La doctrine du module annonce que toute écriture de campagne passe par le
    service ; l'onglet Résultats était la seule exception, et une doctrine avec une
    exception ne protège plus rien (l'API REST de la v4 n'aurait pas eu de revue à
    exposer). `cli.py` appelle encore `Database.set_review` en direct : à basculer
    ici aussi.
    """
    try:
        db.set_review(
            file_id,
            status,
            comment=comment,
            reviewer=reviewer,
            corrected_security=corrected_security,
            corrected_rgpd=corrected_rgpd,
            corrected_retention_years=corrected_retention_years,
        )
    except ValueError as exc:
        raise ServiceError(f"statut de vérification inconnu : « {status} »") from exc
    except sqlite3.Error as exc:
        raise ServiceError(f"vérification non enregistrée : {exc}") from exc
    return next(iter(db.latest_analyses(file_id=file_id)), None)


# ----------------------------------------------------------------- sauvegarde


def _unique_backup_path(directory: Path, stem: str, label: str) -> Path:
    """Chemin libre `<stem>_<horodatage>[_label][_n].sqlite` (deux appels dans la même seconde).

    Le `.sqlite.tmp` correspondant compte comme occupé : la copie n'apparaît sous
    son nom définitif qu'au `os.replace` final, et deux sauvegardes lancées dans la
    même seconde choisiraient sinon le même nom — la seconde écraserait la première.
    """
    suffix = f"_{_slug(label)}" if _slug(label) else ""
    base = f"{stem}_{_stamp()}{suffix}"
    candidate = directory / f"{base}{BACKUP_SUFFIX}"
    counter = 2
    while candidate.exists() or candidate.with_name(candidate.name + ".tmp").exists():
        candidate = directory / f"{base}_{counter}{BACKUP_SUFFIX}"
        counter += 1
    return candidate


def backup_database(
    db_path: Path,
    *,
    out_dir: Path | None = None,
    label: str = "",
    keep: int = DEFAULT_KEEP_BACKUPS,
    db: Database | None = None,
) -> Path:
    """Sauvegarde horodatée d'une base, cohérente même pendant un run.

    Écrit `<base>.backups/<nom>_AAAAMMJJ-HHMMSS[_étiquette].sqlite` via l'API
    `sqlite3` de sauvegarde, puis ne garde que les `keep` copies courantes les
    plus récentes **de cette campagne** (`keep <= 0` : aucune rotation ; les
    copies de sûreté, voir `SAFETY_LABEL_PREFIX`, ne sont jamais tournées).
    `db` évite d'ouvrir une seconde connexion quand la base est déjà ouverte.

    La copie est écrite dans `<nom>.sqlite.tmp` puis renommée par `os.replace`,
    comme `restore_database` et `_write_recent`. Sans cela, une machine éteinte
    au milieu d'une sauvegarde de 932 Mo laissait un fichier tronqué que
    `list_backups` présentait comme « la plus récente » — donc celle qu'un
    utilisateur restaure. Un `.tmp` abandonné, lui, n'est jamais listé.
    """
    source = Path(db_path)
    if db is None and not source.exists():
        raise ServiceError(f"base introuvable : {source}")
    directory = Path(out_dir) if out_dir is not None else backup_dir_for(source)
    try:
        directory.mkdir(parents=True, exist_ok=True)
        target = _unique_backup_path(directory, source.stem, label)
        temporary = target.with_name(target.name + ".tmp")
        try:
            if db is not None:
                db.backup_to(temporary)
            else:
                src = sqlite3.connect(str(source))
                try:
                    dest = sqlite3.connect(str(temporary))
                    try:
                        src.backup(dest)
                    finally:
                        dest.close()
                finally:
                    src.close()
            os.replace(temporary, target)
        except BaseException:
            with suppress(OSError):  # une copie partielle ne protège rien
                temporary.unlink(missing_ok=True)
            raise
    except (OSError, sqlite3.Error) as exc:
        raise ServiceError(f"sauvegarde impossible dans {directory} : {exc}") from exc
    logger.info("sauvegarde → %s", target)
    _rotate(directory, source.stem, keep)
    return target


def _rotate(directory: Path, stem: str, keep: int) -> None:
    """Supprime les sauvegardes **courantes** de cette campagne au-delà de `keep`.

    Ne voit ni les sauvegardes d'une autre campagne du même dossier, ni les
    copies de sûreté : voir `_backups_in` et `SAFETY_LABEL_PREFIX`.
    """
    if keep <= 0:
        return
    for old in _rotatable_in(directory, stem)[keep:]:
        try:
            old.unlink()
            logger.info("sauvegarde éliminée par rotation : %s", old.name)
        except OSError as exc:  # pragma: no cover - fichier verrouillé
            logger.warning("suppression impossible de %s : %s", old, exc)


_CURRENT_TAIL = re.compile(r"^\d{8}-\d{6}(?:_(?P<label>.+))?$")
"""Ce qui suit `<campagne>_` dans une sauvegarde de `backup_database` :
l'horodatage de `_stamp()`, puis l'étiquette et le rang éventuels."""

_MIGRATION_TAIL = re.compile(r"^avant_migration_v\d+_\d{8}T\d{6}(?:_\d+)?$")
"""Idem pour une copie d'avant-migration, écrite par `Database._backup_before_migration`
(horodatage `AAAAMMJJTHHMMSS`, sans tiret)."""


def _backup_tail(path: Path, stem: str) -> str | None:
    """Ce qui suit `<stem>_` si `path` est une sauvegarde de **cette** campagne, sinon `None`.

    Le nom complet est exigé : `<stem>_` suivi d'un horodatage reconnu. Un simple
    `glob("audit_*.sqlite")` réclamait aussi les sauvegardes de la campagne
    `audit_2024_direction` — que l'écran Rapports invite précisément à ranger dans
    le même dossier. `list_backups` les présentait comme siennes et la rotation
    d'`audit` les supprimait. Le `.tmp` d'une sauvegarde en cours (ou abandonnée)
    n'est pas non plus une sauvegarde : il n'a pas le suffixe attendu.
    """
    if path.suffix != BACKUP_SUFFIX:
        return None
    prefix = f"{stem}_"
    name = path.stem
    if not name.startswith(prefix):
        return None
    tail = name[len(prefix) :]
    if not (_CURRENT_TAIL.match(tail) or _MIGRATION_TAIL.match(tail)):
        return None
    return tail if path.is_file() else None


def _is_safety_copy(tail: str) -> bool:
    """Une copie de sûreté (`avant_migration_*`, `avant_restauration`, `avant_reanalyse_*`) ?"""
    if _MIGRATION_TAIL.match(tail):
        return True
    match = _CURRENT_TAIL.match(tail)
    label = str(match.group("label") or "") if match else ""
    return label.startswith(SAFETY_LABEL_PREFIX)


def _backups_in(directory: Path, stem: str) -> list[Path]:
    """Sauvegardes de **cette** campagne, de la plus récente à la plus ancienne.

    Copies de sûreté comprises : elles sont restaurables comme les autres, et
    l'utilisateur doit les voir. Seule la rotation les ignore (`_rotatable_in`).
    """
    if not directory.is_dir():
        return []
    found = [p for p in directory.iterdir() if _backup_tail(p, stem) is not None]
    return sorted(found, key=lambda p: (p.stat().st_mtime_ns, p.name), reverse=True)


def _rotatable_in(directory: Path, stem: str) -> list[Path]:
    """Vivier de la rotation : les sauvegardes courantes, hors copies de sûreté."""
    out: list[Path] = []
    for path in _backups_in(directory, stem):
        tail = _backup_tail(path, stem)
        if tail is not None and not _is_safety_copy(tail):
            out.append(path)
    return out


def list_backups(db_path: Path) -> list[Path]:
    """Sauvegardes d'une base, de la plus récente à la plus ancienne.

    Seulement celles de cette campagne, même si le dossier en abrite d'autres, et
    jamais une copie en cours d'écriture (`.sqlite.tmp`).
    """
    source = Path(db_path)
    return _backups_in(backup_dir_for(source), source.stem)


def restore_database(db_path: Path, backup_path: Path) -> Path:
    """Restaure une sauvegarde par-dessus la base et rend le chemin restauré.

    La sauvegarde est **d'abord** recopiée dans `<base>.tmp`, ensuite seulement la
    base courante est mise de côté (étiquette `avant_restauration`), et enfin le
    `.tmp` prend la place de la base par `os.replace` (atomique sous Windows comme
    sous POSIX). Les journaux `-wal`/`-shm` de l'ancienne base sont retirés pour ne
    pas être rejoués sur la nouvelle.

    Cet ordre n'est pas un détail : la sauvegarde préalable déclenche une rotation,
    et la rotation supprimait le fichier que l'on est en train de restaurer dès
    qu'il était le plus ancien des dix. La restauration échouait (« No such file or
    directory ») **et** la copie visée était perdue — juste au moment où l'on comptait
    dessus. Copier avant de tourner met la source à l'abri quoi qu'il advienne.

    Aucun verrou n'est posé : c'est à l'appelant de s'assurer qu'aucun run ni
    aucune interface n'a la base ouverte (sinon le remplacement échoue sous
    Windows, et les lecteurs en cours voient l'ancienne base sous POSIX).
    """
    source = Path(backup_path)
    target = Path(db_path)
    if not source.is_file():
        raise ServiceError(f"sauvegarde introuvable : {source}")
    try:
        probe = sqlite3.connect(f"file:{source}?mode=ro", uri=True)
        try:
            probe.execute("SELECT COUNT(*) FROM sqlite_master").fetchone()
        finally:
            probe.close()
    except sqlite3.Error as exc:
        raise ServiceError(f"sauvegarde illisible ({source.name}) : {exc}") from exc

    temporary = target.with_name(target.name + ".tmp")
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, temporary)  # avant toute rotation : voir la docstring
        if target.exists():
            backup_database(target, label="avant_restauration")
        os.replace(temporary, target)
        for sidecar in (
            target.with_name(target.name + "-wal"),
            target.with_name(target.name + "-shm"),
        ):
            sidecar.unlink(missing_ok=True)
    except OSError as exc:
        with suppress(OSError):
            temporary.unlink(missing_ok=True)
        raise ServiceError(f"restauration impossible vers {target} : {exc}") from exc
    except BaseException:  # `ServiceError` de la sauvegarde préalable, interruption…
        with suppress(OSError):
            temporary.unlink(missing_ok=True)
        raise
    logger.info("restauration : %s → %s", source, target)
    return target


# ------------------------------------------------------------------ campagnes


def _recent_path() -> Path:
    return docia_home() / RECENT_FILE


def _read_recent() -> list[dict[str, str]]:
    """Contenu de `recent.json`, ou une liste vide si absent ou illisible."""
    path = _recent_path()
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return []
    entries = raw.get("campaigns") if isinstance(raw, dict) else raw
    if not isinstance(entries, list):
        return []
    out: list[dict[str, str]] = []
    for item in entries:
        if isinstance(item, dict) and str(item.get("db_path", "")).strip():
            out.append({str(k): str(v) for k, v in item.items() if v is not None})
    return out


def _write_recent(entries: Sequence[dict[str, str]]) -> None:
    """Écrit `recent.json` (fichier temporaire puis `os.replace`) sans jamais lever."""
    path = _recent_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(path.name + ".tmp")
        temporary.write_text(
            json.dumps({"campaigns": list(entries)}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        os.replace(temporary, path)
    except OSError as exc:
        logger.warning("liste des campagnes récentes non enregistrée (%s) : %s", path, exc)


def _same_db(left: str, right: str) -> bool:
    """Deux chemins désignent la même base (comparaison insensible à la casse sous Windows)."""
    if os.name == "nt":
        return left.casefold() == right.casefold()
    return left == right


def remember_campaign(db_path: Path, csv_path: Path | None = None, label: str = "") -> None:
    """Place une campagne en tête des récentes (20 au plus, chemins absolus)."""
    key = str(Path(db_path).resolve())
    entry = {
        "db_path": key,
        "csv_path": str(Path(csv_path).resolve()) if csv_path is not None else "",
        "last_opened": _now_iso(),
        "label": label,
    }
    existing = _read_recent()
    kept = [e for e in existing if not _same_db(str(e.get("db_path", "")), key)]
    previous = next((e for e in existing if _same_db(str(e.get("db_path", "")), key)), None)
    if previous is not None:
        if not entry["csv_path"]:
            entry["csv_path"] = str(previous.get("csv_path", ""))
        if not entry["label"]:
            entry["label"] = str(previous.get("label", ""))
    _write_recent([entry, *kept][:MAX_RECENT])


def recent_campaigns() -> list[RecentCampaign]:
    """Campagnes récemment ouvertes, de la plus récente à la plus ancienne."""
    out: list[RecentCampaign] = []
    for entry in _read_recent():
        csv_text = str(entry.get("csv_path", ""))
        out.append(
            RecentCampaign(
                db_path=Path(entry["db_path"]),
                csv_path=Path(csv_text) if csv_text else None,
                last_opened=str(entry.get("last_opened", "")),
                label=str(entry.get("label", "")),
            )
        )
    return out


def forget_campaign(db_path: Path) -> None:
    """Retire une campagne de la liste des récentes (la base n'est pas touchée)."""
    key = str(Path(db_path).resolve())
    _write_recent([e for e in _read_recent() if not _same_db(str(e.get("db_path", "")), key)])
