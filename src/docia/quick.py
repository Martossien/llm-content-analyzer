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


def _walk_files(root: Path, on_error: Callable[[OSError], None] | None) -> list[Path]:
    """Fichiers d'un dossier, en profondeur, **sans avaler les dossiers refusés**.

    `Path.rglob` ignore silencieusement une `PermissionError` : sur un partage
    cloisonné (`\\\\srv\\partage\\Compta` avec un compte sans droits sur un
    sous-dossier), les fichiers concernés disparaissaient de l'audit sans être
    ni comptés ni signalés. `os.walk(..., onerror=…)` rend la main sur chaque
    échec d'énumération, qui part alors dans `on_error`.
    """
    found: list[Path] = []
    for dirpath, dirnames, filenames in os.walk(root, onerror=on_error):
        dirnames.sort()
        base = Path(dirpath)
        found += [base / name for name in filenames if (base / name).is_file()]
    return sorted(found)


def iter_local_files(
    paths: Iterable[Path], *, on_error: Callable[[OSError], None] | None = None
) -> Iterator[Path]:
    """Fichiers désignés : un fichier tel quel, un dossier parcouru récursivement.

    Les doublons (même chemin donné deux fois, ou fichier contenu dans un dossier
    également listé) ne sortent qu'une fois.

    Args:
        on_error: Rappel reçu pour chaque dossier dont l'énumération échoue
            (permissions, montage cassé) — `exc.filename` porte le dossier.
    """
    seen: set[str] = set()
    for raw in paths:
        candidates = _walk_files(raw, on_error) if raw.is_dir() else [raw]
        for candidate in candidates:
            key = path_key(candidate)
            if key in seen:
                continue
            seen.add(key)
            yield candidate


def csv_rows_from_paths(
    paths: Iterable[Path],
    *,
    unreadable: list[str] | None = None,
    denied_dirs: list[str] | None = None,
) -> Iterator[SmbeagleRow]:
    """Rend une `SmbeagleRow` par fichier lisible (mêmes règles que `csv_from_dir.py`).

    Args:
        paths: Fichiers ou dossiers (parcourus récursivement).
        unreadable: Liste alimentée avec les chemins illisibles (comptés, ignorés).
        denied_dirs: Liste alimentée avec les dossiers dont l'énumération est
            refusée. Pour un outil d'audit, « manquant sans le dire » est la pire
            des sorties : ces dossiers sont comptés et affichés (`as_lines`).
            À défaut, ils retombent dans `unreadable` — aucun appelant ne doit
            pouvoir les perdre par simple omission d'un argument.
    """

    def dir_failed(exc: OSError) -> None:
        target = str(exc.filename or exc)
        logger.warning("quick : dossier illisible ignoré (%s) : %s", type(exc).__name__, target)
        sink = denied_dirs if denied_dirs is not None else unreadable
        if sink is not None:
            sink.append(target)

    user, host = _identity()
    for path in iter_local_files(paths, on_error=dir_failed):
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
    denied_dirs: int = 0
    """Dossiers dont l'énumération a été refusée : leur contenu n'a **pas** été
    audité. Compté et affiché à part — un audit de conformité qui saute un dossier
    sans le dire ment sur son périmètre."""
    duration_s: float = 0.0
    db_path: str = ""
    kept_db: bool = False
    files: list[QuickFileResult] = field(default_factory=list)
    llm_errors: list[str] = field(default_factory=list)
    dry_run: bool = False
    blocks_built: int = 0
    extraction_errors: int = 0

    def as_dict(self) -> dict[str, object]:
        """Rapport sous forme de dictionnaire (sortie `--json`)."""
        return asdict(self)

    def _denied_lines(self) -> list[str]:
        """L'avertissement sur les dossiers refusés, ou rien s'il n'y en a pas."""
        if not self.denied_dirs:
            return []
        return [
            f"ATTENTION : {self.denied_dirs} dossier(s) refusé(s) (permissions) — "
            "leur contenu n'a pas été analysé"
        ]

    def as_lines(self) -> list[str]:
        """Tableau texte lisible (≤ 120 colonnes)."""
        if not self.ok:
            return [f"ÉCHEC : {self.message}"]
        if self.dry_run:
            return [
                f"extraction seule (sans LLM) : {self.requested} fichier(s) demandés, "
                f"{self.blocks_built} bloc(s) construits, {self.extraction_errors} en erreur, "
                f"{self.excluded} exclus — {self.duration_s:.1f} s",
                *self._denied_lines(),
            ]
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
        lines.extend(self._denied_lines())
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
    dry_run: bool = False,
) -> QuickReport:
    """Analyse des fichiers ou dossiers locaux sans CSV ni base préalable.

    `dry_run` : extraction DocFuse et construction des blocs seulement, sans LLM —
    sert à contrôler qu'un exécutable empaqueté embarque bien tous les extracteurs.

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

    rows = _gather_inputs(paths, report, say)
    if rows is None:
        return failed(report.message)

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
            _analyze_rows(
                db,
                local,
                rows,
                report,
                paths,
                progress=progress,
                cancel=cancel,
                dry_run=dry_run,
                say=say,
            )
    except sqlite3.Error as exc:
        return failed(f"base inutilisable ({db_file}) : {exc}")
    finally:
        if temp_dir is not None:
            shutil.rmtree(temp_dir, ignore_errors=True)

    report.analyzed = sum(1 for f in report.files if f.status == "done")
    report.excluded = sum(1 for f in report.files if f.status == "excluded")
    report.errors = sum(1 for f in report.files if f.status == "error")
    report.duration_s = time.perf_counter() - started
    if report.dry_run:
        if report.blocks_built == 0 and report.requested > 0:
            return failed("aucun bloc construit : extraction impossible")
        return report
    if report.llm_errors and report.analyzed == 0:
        return failed(report.llm_errors[0])
    return report


def _gather_inputs(
    paths: Sequence[Path], report: QuickReport, say: ProgressCallback
) -> list[SmbeagleRow] | None:
    """Lignes « scan » des chemins demandés ; `None` (et `report.message`) s'il n'y a rien à lire."""
    unreadable: list[str] = []
    denied: list[str] = []
    rows = list(
        csv_rows_from_paths([p.resolve() for p in paths], unreadable=unreadable, denied_dirs=denied)
    )
    report.unreadable = len(unreadable)
    report.denied_dirs = len(denied)
    report.requested = len(rows)
    if denied:
        say(f"{len(denied)} dossier(s) refusé(s) : {', '.join(denied[:3])}")
    if not rows:
        report.message = (
            f"aucun fichier lisible ({len(unreadable)} illisible(s), "
            f"{len(denied)} dossier(s) refusé(s))"
            if unreadable or denied
            else "aucun fichier à analyser"
        )
        return None
    say(f"{len(rows)} fichier(s) repéré(s)")
    return rows


def _analyze_rows(
    db: Database,
    local: Config,
    rows: list[SmbeagleRow],
    report: QuickReport,
    paths: Sequence[Path],
    *,
    progress: ProgressCallback | None,
    cancel: threading.Event | None,
    dry_run: bool,
    say: ProgressCallback,
) -> None:
    """Import des lignes, préparation, run, puis relecture des fiches demandées."""
    scan_id = db.start_scan(f"quick:{paths[0]}")
    new, updated, unchanged = db.upsert_files(rows, scan_id)
    db.finish_scan(
        scan_id,
        total=len(rows),
        new=new,
        updated=updated,
        unchanged=unchanged,
        invalid=report.unreadable,
    )
    plan = plan_files(db, local.filter)
    say(f"à analyser : {plan.pending} — exclus : {plan.excluded}")
    run = run_pipeline(db, local, progress=progress, cancel=cancel, dry_run=dry_run)
    report.llm_errors = list(run.errors)
    report.dry_run = dry_run
    report.blocks_built = run.blocks_built
    report.extraction_errors = run.files_error
    wanted = {path_key(row.path) for row in rows}
    report.files = [
        _result(record)
        for record in db.latest_analyses()
        if path_key(str(record["path"])) in wanted
    ]


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
