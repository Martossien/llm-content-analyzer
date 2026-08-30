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
import shutil
import sqlite3
import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from docia.config import Config
from docia.db import Database, backup_dir_for
from docia.filter import PlanReport, plan_files
from docia.ingest.smbeagle_csv import ImportReport, import_csv
from docia.llm.schema import prompt_hash
from docia.models import FileStatus
from docia.pipeline import RunReport, resolve_system_prompt, run_pipeline
from docia.views import RunStat, runs_summary

logger = logging.getLogger(__name__)

__all__ = [
    "BACKUP_SUFFIX",
    "DEFAULT_KEEP_BACKUPS",
    "HOME_ENV",
    "MAX_RECENT",
    "REANALYZE_SCOPES",
    "RECENT_FILE",
    "WHERE_KEYS",
    "CampaignStatus",
    "RecentCampaign",
    "RunEvent",
    "ServiceError",
    "backup_database",
    "backup_dir_for",
    "campaign_status",
    "docia_home",
    "forget_campaign",
    "import_scan",
    "list_backups",
    "plan",
    "reanalyze",
    "recent_campaigns",
    "remember_campaign",
    "restore_database",
    "run_campaign",
]
"""Surface publique du service : ce que la CLI, la GUI et l'API REST utilisent."""

HOME_ENV = "DOCIA_HOME"
"""Variable d'environnement qui redirige le dossier de configuration (tests, poste verrouillé)."""

RECENT_FILE = "recent.json"
MAX_RECENT = 20
DEFAULT_KEEP_BACKUPS = 10
BACKUP_SUFFIX = ".sqlite"
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


def import_scan(db: Database, csv_path: Path, *, strict: bool = False) -> ImportReport:
    """Importe un CSV SMBeagle et mémorise la campagne dans les récentes.

    `strict=False` (défaut de l'interface) tolère les lignes invalides : elles
    sont comptées dans le rapport plutôt que d'interrompre l'import.
    """
    path = Path(csv_path)
    if not path.exists():
        raise ServiceError(f"fichier de scan introuvable : {path}")
    try:
        report = import_csv(db, path, strict=strict)
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


def plan(db: Database, cfg: Config) -> PlanReport:
    """Applique exclusions et scores de priorité à toute la base."""
    report = plan_files(db, cfg.filter)
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
    deleted = db.delete_analyses(file_ids, prompt_hash=phash, model=model)
    db.set_files_status(file_ids, FileStatus.PENDING, None)
    logger.info(
        "réanalyse (%s) : %s fichier(s) remis à analyser, %s analyse(s) supprimée(s)",
        scope,
        len(file_ids),
        deleted,
    )
    return len(file_ids)


# ----------------------------------------------------------------- sauvegarde


def _unique_backup_path(directory: Path, stem: str, label: str) -> Path:
    """Chemin libre `<stem>_<horodatage>[_label][_n].sqlite` (deux appels dans la même seconde)."""
    suffix = f"_{_slug(label)}" if _slug(label) else ""
    base = f"{stem}_{_stamp()}{suffix}"
    candidate = directory / f"{base}{BACKUP_SUFFIX}"
    counter = 2
    while candidate.exists():
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
    `sqlite3` de sauvegarde, puis ne garde que les `keep` copies les plus
    récentes du dossier (`keep <= 0` : aucune rotation). `db` évite d'ouvrir une
    seconde connexion quand la base est déjà ouverte.
    """
    source = Path(db_path)
    if db is None and not source.exists():
        raise ServiceError(f"base introuvable : {source}")
    directory = Path(out_dir) if out_dir is not None else backup_dir_for(source)
    try:
        directory.mkdir(parents=True, exist_ok=True)
        target = _unique_backup_path(directory, source.stem, label)
        if db is not None:
            db.backup_to(target)
        else:
            src = sqlite3.connect(str(source))
            try:
                dest = sqlite3.connect(str(target))
                try:
                    src.backup(dest)
                finally:
                    dest.close()
            finally:
                src.close()
    except (OSError, sqlite3.Error) as exc:
        raise ServiceError(f"sauvegarde impossible dans {directory} : {exc}") from exc
    logger.info("sauvegarde → %s", target)
    _rotate(directory, source.stem, keep)
    return target


def _rotate(directory: Path, stem: str, keep: int) -> None:
    """Supprime les sauvegardes les plus anciennes au-delà de `keep`."""
    if keep <= 0:
        return
    for old in _backups_in(directory, stem)[keep:]:
        try:
            old.unlink()
            logger.info("sauvegarde éliminée par rotation : %s", old.name)
        except OSError as exc:  # pragma: no cover - fichier verrouillé
            logger.warning("suppression impossible de %s : %s", old, exc)


def _backups_in(directory: Path, stem: str) -> list[Path]:
    """Sauvegardes du dossier, de la plus récente à la plus ancienne."""
    if not directory.is_dir():
        return []
    found = [p for p in directory.glob(f"{stem}_*{BACKUP_SUFFIX}") if p.is_file()]
    return sorted(found, key=lambda p: (p.stat().st_mtime_ns, p.name), reverse=True)


def list_backups(db_path: Path) -> list[Path]:
    """Sauvegardes d'une base, de la plus récente à la plus ancienne."""
    source = Path(db_path)
    return _backups_in(backup_dir_for(source), source.stem)


def restore_database(db_path: Path, backup_path: Path) -> Path:
    """Restaure une sauvegarde par-dessus la base et rend le chemin restauré.

    La base courante est d'abord sauvegardée (étiquette `avant_restauration`),
    puis le fichier est remplacé en deux temps (`<base>.tmp` puis `os.replace`,
    atomique sous Windows comme sous POSIX). Les journaux `-wal`/`-shm` de
    l'ancienne base sont retirés pour ne pas être rejoués sur la nouvelle.

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

    if target.exists():
        backup_database(target, label="avant_restauration")
    temporary = target.with_name(target.name + ".tmp")
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, temporary)
        os.replace(temporary, target)
        for sidecar in (
            target.with_name(target.name + "-wal"),
            target.with_name(target.name + "-shm"),
        ):
            sidecar.unlink(missing_ok=True)
    except OSError as exc:
        temporary.unlink(missing_ok=True)
        raise ServiceError(f"restauration impossible vers {target} : {exc}") from exc
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
