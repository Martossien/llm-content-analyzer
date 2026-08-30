"""Analyse immédiate (`docia quick`, `docs/DESIGN_V3.md` §10 lot C).

« Je veux savoir ce qu'il y a dans ce dossier, tout de suite » : `quick` fabrique
les lignes SMBeagle des fichiers demandés (comme `scripts/csv_from_dir.py`, mais
sans passer par un CSV), les ingère dans une base temporaire, applique le filtre
et lance le pipeline habituel. Le résultat est un tableau texte, pas une trace
Python.

La base est jetée en fin d'analyse, sauf `db_path` : dans ce cas l'historique est
conservé et une seconde passe ne renvoie rien à la LLM (reprise).
"""

from __future__ import annotations

import getpass
import hashlib
import logging
import os
import shutil
import socket
import sqlite3
import tempfile
import threading
import time
from collections.abc import Callable, Iterable, Iterator, Sequence
from copy import deepcopy
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path

from docia.config import Config
from docia.db import Database
from docia.filter import plan_files
from docia.models import SmbeagleRow, path_key
from docia.pipeline import run_pipeline

logger = logging.getLogger(__name__)

HASH_HEAD_BYTES = 64 * 1024
"""Comme SMBeagle : seuls les 64 premiers Ko servent à l'empreinte de contenu —
jamais le fichier entier (un modèle de 7 Go dans le dossier bloquerait tout)."""

LOCAL_BASE = "\\\\localhost\\LOCAL_SCAN\\"
SUMMARY_WIDTH = 120
"""Le tableau texte doit rester lisible dans une console standard."""

ProgressCallback = Callable[[str], None]


# ------------------------------------------------------------ lignes SMBeagle


def fast_hash(path: Path) -> str:
    """Empreinte des 64 premiers Ko (xxHash64 si disponible, sinon SHA-256 tronqué)."""
    with path.open("rb") as handle:
        head = handle.read(HASH_HEAD_BYTES)
    try:
        import xxhash  # type: ignore[import-not-found]
    except ImportError:
        return hashlib.sha256(head).hexdigest()[:16]
    return str(xxhash.xxh64(head).hexdigest())


def _stamp(timestamp: float) -> str:
    """Horodatage au format SMBeagle (`dd/MM/yyyy HH:mm:ss`)."""
    return datetime.fromtimestamp(timestamp).strftime("%d/%m/%Y %H:%M:%S")


def _identity() -> tuple[str, str]:
    """Utilisateur et machine courants (valeurs de repli si le système les refuse)."""
    try:
        user = getpass.getuser()
    except (OSError, KeyError):  # pragma: no cover - environnement sans compte nommé
        user = "inconnu"
    try:
        host = socket.gethostname()
    except OSError:  # pragma: no cover - pile réseau indisponible
        host = "localhost"
    return user, host


def iter_local_files(paths: Iterable[Path]) -> Iterator[Path]:
    """Fichiers désignés : un fichier tel quel, un dossier parcouru récursivement.

    Les doublons (même chemin donné deux fois, ou fichier contenu dans un dossier
    également listé) ne sortent qu'une fois.
    """
    seen: set[str] = set()
    for raw in paths:
        candidates = sorted(p for p in raw.rglob("*") if p.is_file()) if raw.is_dir() else [raw]
        for candidate in candidates:
            key = path_key(candidate)
            if key in seen:
                continue
            seen.add(key)
            yield candidate


def csv_rows_from_paths(
    paths: Iterable[Path], *, unreadable: list[str] | None = None
) -> Iterator[SmbeagleRow]:
    """Rend une `SmbeagleRow` par fichier lisible (mêmes règles que `csv_from_dir.py`).

    Args:
        paths: Fichiers ou dossiers (parcourus récursivement).
        unreadable: Liste alimentée avec les chemins illisibles (comptés, ignorés).
    """
    user, host = _identity()
    for path in iter_local_files(paths):
        try:
            stat = path.stat()
            digest = fast_hash(path)
        except OSError as exc:  # verrou, montage cassé, E/S : compté, jamais fatal
            logger.warning("quick : fichier illisible ignoré (%s) : %s", type(exc).__name__, path)
            if unreadable is not None:
                unreadable.append(str(path))
            continue
        yield SmbeagleRow(
            name=path.name,
            host="localhost",
            extension=path.suffix.lstrip(".").lower(),
            username=user,
            hostname=host,
            unc_directory=str(path.parent),
            creation_time=_stamp(stat.st_ctime),
            last_write_time=_stamp(stat.st_mtime),
            readable=True,
            writeable=os.access(path, os.W_OK),
            deletable=os.access(path.parent, os.W_OK),
            directory_type="LOCAL_FIXED",
            base=LOCAL_BASE,
            file_size=stat.st_size,
            access_time=_stamp(stat.st_atime),
            file_attributes="Archive",
            owner=user,
            fast_hash=digest,
            file_signature="unknown",
        )


# ------------------------------------------------------------------- rapport


@dataclass
class QuickFileResult:
    """Une ligne du tableau : le fichier, ses cinq domaines et son résumé."""

    name: str
    path: str
    status: str
    resume: str = ""
    security: str = ""
    rgpd: str = ""
    finance: str = ""
    legal: str = ""
    retention: str = ""
    segments: int = 1
    reason: str = ""
    """Raison d'exclusion ou d'erreur (vide si le fichier est analysé)."""


@dataclass
class QuickReport:
    """Bilan d'un `quick_analyze`. `ok is False` ⇒ `message` dit pourquoi."""

    ok: bool = True
    message: str = ""
    requested: int = 0
    analyzed: int = 0
    excluded: int = 0
    errors: int = 0
    unreadable: int = 0
    duration_s: float = 0.0
    db_path: str = ""
    kept_db: bool = False
    files: list[QuickFileResult] = field(default_factory=list)
    llm_errors: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, object]:
        return asdict(self)

    def as_lines(self) -> list[str]:
        """Tableau texte lisible (≤ 120 colonnes)."""
        if not self.ok:
            return [f"ÉCHEC : {self.message}"]
        header = _row(("fichier", "sécu", "RGPD", "finance", "juridique", "conserv.", "résumé"))
        lines = [header, "-" * min(SUMMARY_WIDTH, len(header))]
        for item in self.files:
            comment = item.resume if item.status == "done" else f"[{item.status}] {item.reason}"
            if item.segments > 1:
                comment = f"({item.segments} parties) {comment}"
            lines.append(
                _row(
                    (
                        item.name,
                        item.security,
                        item.rgpd,
                        item.finance,
                        item.legal,
                        item.retention,
                        comment,
                    )
                )
            )
        lines.append(
            f"{self.requested} fichier(s) : {self.analyzed} analysé(s), {self.excluded} exclu(s), "
            f"{self.errors} en erreur, {self.unreadable} illisible(s) — {self.duration_s:.1f} s"
        )
        if self.kept_db:
            lines.append(f"base conservée : {self.db_path}")
        lines.extend(f"  erreur : {error}" for error in self.llm_errors[:5])
        return lines


_DEFAULT_WIDTHS = (28, 5, 8, 10, 10, 14, 37)


def _row(values: Sequence[str], widths: Sequence[int] = _DEFAULT_WIDTHS) -> str:
    """Une ligne de tableau : colonnes tronquées à leur largeur, séparées d'un espace."""
    cells = []
    for value, width in zip(values, widths, strict=True):
        text = " ".join(str(value).split())
        cells.append(text[: width - 1] + "…" if len(text) > width else text.ljust(width))
    return " ".join(cells).rstrip()


# -------------------------------------------------------------------- analyse


def quick_analyze(
    cfg: Config,
    paths: Sequence[Path],
    *,
    db_path: Path | None = None,
    progress: ProgressCallback | None = None,
    cancel: threading.Event | None = None,
) -> QuickReport:
    """Analyse des fichiers ou dossiers locaux sans CSV ni base préalable.

    Args:
        cfg: Configuration (LLM, blocs, filtre) ; `cfg.db_path` est ignoré.
        paths: Fichiers et/ou dossiers à analyser.
        db_path: Base à réutiliser pour garder l'historique (reprise) ; défaut :
            une base temporaire supprimée en fin d'analyse.
        progress: Rappel d'avancement (lignes du pipeline).
        cancel: Événement d'annulation (GUI), transmis au pipeline.

    Returns:
        Le rapport ; `ok is False` avec un `message` si un chemin est introuvable,
        si aucun fichier n'est lisible ou si le serveur LLM ne répond pas.
    """
    say = progress or (lambda _m: None)
    started = time.perf_counter()
    report = QuickReport(db_path=str(db_path or ""), kept_db=db_path is not None)

    def failed(message: str) -> QuickReport:
        report.ok = False
        report.message = message
        report.duration_s = time.perf_counter() - started
        return report

    if not paths:
        return failed("aucun chemin fourni")
    missing = [str(p) for p in paths if not p.exists()]
    if missing:
        return failed("chemin introuvable : " + ", ".join(missing[:5]))

    unreadable: list[str] = []
    rows = list(csv_rows_from_paths([p.resolve() for p in paths], unreadable=unreadable))
    report.unreadable = len(unreadable)
    report.requested = len(rows)
    if not rows:
        return failed(
            f"aucun fichier lisible ({len(unreadable)} illisible(s))"
            if unreadable
            else "aucun fichier à analyser"
        )
    say(f"{len(rows)} fichier(s) repéré(s)")

    temp_dir: Path | None = None
    if db_path is None:
        temp_dir = Path(tempfile.mkdtemp(prefix="docia_quick_"))
        db_file = temp_dir / "quick.sqlite"
    else:
        db_file = db_path
    report.db_path = str(db_file)

    local = deepcopy(cfg)
    local.db_path = str(db_file)
    # Chemins locaux : les marqueurs de dossiers système (dont `\AppData\`, où vit
    # le dossier temporaire sous Windows) écarteraient à tort ce que l'on demande.
    local.filter.excluded_dir_markers = []

    try:
        with Database(db_file) as db:
            scan_id = db.start_scan(f"quick:{paths[0]}")
            new, updated, unchanged = db.upsert_files(rows, scan_id)
            db.finish_scan(
                scan_id,
                total=len(rows),
                new=new,
                updated=updated,
                unchanged=unchanged,
                invalid=len(unreadable),
            )
            plan = plan_files(db, local.filter)
            say(f"à analyser : {plan.pending} — exclus : {plan.excluded}")
            run = run_pipeline(db, local, progress=progress, cancel=cancel)
            report.llm_errors = list(run.errors)
            wanted = {path_key(row.path) for row in rows}
            report.files = [
                _result(record)
                for record in db.latest_analyses()
                if path_key(str(record["path"])) in wanted
            ]
    except sqlite3.Error as exc:
        return failed(f"base inutilisable ({db_file}) : {exc}")
    finally:
        if temp_dir is not None:
            shutil.rmtree(temp_dir, ignore_errors=True)

    report.analyzed = sum(1 for f in report.files if f.status == "done")
    report.excluded = sum(1 for f in report.files if f.status == "excluded")
    report.errors = sum(1 for f in report.files if f.status == "error")
    report.duration_s = time.perf_counter() - started
    if report.llm_errors and report.analyzed == 0:
        return failed(report.llm_errors[0])
    return report


def _result(record: sqlite3.Row) -> QuickFileResult:
    """Une ligne de `latest_analyses` → une ligne du tableau."""
    keys = record.keys()

    def value(name: str) -> str:
        return "" if name not in keys or record[name] is None else str(record[name])

    retention = value("retention_basis")
    years = value("retention_years")
    if retention and years:
        retention = f"{retention} {years} ans"
    return QuickFileResult(
        name=value("name"),
        path=value("path"),
        status=value("status"),
        resume=value("resume"),
        security=value("security_classification"),
        rgpd=value("rgpd_risk_level"),
        finance=value("finance_document_type"),
        legal=value("legal_contract_type"),
        retention=retention,
        segments=int(record["segments"]) if "segments" in keys and record["segments"] else 1,
        reason=value("exclusion_reason"),
    )
