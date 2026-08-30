"""Filtre : exclusions (extension, taille, dossiers système) et score de priorité.

Deux fonctions pures — `exclusion_reason` et `priority_score` — décident pour
une ligne ; `plan_files` les applique à toute la base via `Database.apply_plan`.
Un fichier déjà `done` n'est jamais rétrogradé (c'est `apply_plan` qui garantit
cette règle).
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime

from docia.config import FilterConfig
from docia.db import Database
from docia.ingest.smbeagle_csv import parse_smbeagle_datetime
from docia.models import FileRow, FileStatus

HIGH_VALUE_EXTENSIONS: frozenset[str] = frozenset(
    {"docx", "doc", "pdf", "xlsx", "xls", "odt", "ods", "msg", "eml"}
)
"""Documents bureautiques : 40 points de type."""

MEDIUM_VALUE_EXTENSIONS: frozenset[str] = frozenset({"txt", "md", "csv", "rtf", "pptx", "ppt"})
"""Texte brut et présentations : 25 points de type."""

SENSITIVE_KEYWORDS: tuple[str, ...] = (
    "contrat",
    "facture",
    "salaire",
    "paie",
    "rgpd",
    "mot de passe",
    "password",
    "confidentiel",
    "budget",
    "bilan",
)
"""Mots-clés du nom de fichier valant les 10 points « sensible »."""

_SMALL_FILE = 10 * 1024
_LARGE_FILE = 5 * 1024 * 1024
_ONE_YEAR_DAYS = 365
_THREE_YEARS_DAYS = 3 * 365


@dataclass(frozen=True)
class PlanReport:
    """Bilan d'un `plan_files` : fichiers retenus, exclus, et exclusions par raison."""

    pending: int
    excluded: int
    by_reason: dict[str, int] = field(default_factory=dict)


def _dotted(extension: str) -> str:
    """`"PDF"` ou `".pdf"` → `".pdf"` ; chaîne vide si pas d'extension."""
    value = extension.strip().lower().lstrip(".")
    return f".{value}" if value else ""


def _normalized_path(path: str) -> str:
    """Chemin comparable : antislashs et minuscules (Windows comme POSIX)."""
    return path.replace("/", "\\").lower()


def exclusion_reason(row: FileRow, cfg: FilterConfig) -> str | None:
    """Raison d'exclusion du fichier, ou `None` s'il est à analyser.

    Ordre : extension, taille minimale, taille maximale, dossier exclu.
    """
    extension = _dotted(row.extension)
    if extension and extension in {_dotted(e) for e in cfg.excluded_extensions}:
        return f"extension exclue ({extension})"
    if row.size_bytes < cfg.min_size_bytes:
        return f"fichier trop petit ({row.size_bytes} o < {cfg.min_size_bytes} o)"
    if row.size_bytes > cfg.max_size_bytes:
        return f"fichier trop volumineux ({row.size_bytes} o > {cfg.max_size_bytes} o)"
    path = _normalized_path(row.path)
    for marker in cfg.excluded_dir_markers:
        normalized = _normalized_path(marker)
        if normalized and normalized in path:
            return f"dossier exclu ({marker})"
    return None


def _type_score(extension: str) -> int:
    value = extension.strip().lower().lstrip(".")
    if value in HIGH_VALUE_EXTENSIONS:
        return 40
    if value in MEDIUM_VALUE_EXTENSIONS:
        return 25
    return 10


def _size_score(size_bytes: int) -> int:
    if size_bytes < _SMALL_FILE:
        return 10
    if size_bytes <= _LARGE_FILE:
        return 30
    return 15


def _age_score(last_write_time: str, now: datetime) -> int:
    modified = parse_smbeagle_datetime(last_write_time)
    if modified is None:
        return 10
    days = (now - modified).days
    if days < _ONE_YEAR_DAYS:
        return 20
    if days < _THREE_YEARS_DAYS:
        return 12
    return 5


def _keyword_score(name: str) -> int:
    haystack = name.lower().replace("_", " ").replace("-", " ")
    return 10 if any(keyword in haystack for keyword in SENSITIVE_KEYWORDS) else 0


def priority_score(row: FileRow, now: datetime) -> int:
    """Score 0–100 : type (40), taille (30), fraîcheur (20), mots-clés (10)."""
    reference = now.replace(tzinfo=None) if now.tzinfo is not None else now
    total = (
        _type_score(row.extension)
        + _size_score(row.size_bytes)
        + _age_score(row.last_write_time, reference)
        + _keyword_score(row.name)
    )
    return max(0, min(total, 100))


def plan_files(db: Database, cfg: FilterConfig) -> PlanReport:
    """Applique exclusions et scores à toute la base.

    Les compteurs ne portent que sur les fichiers que le plan peut réellement
    changer (`pending`, `excluded`, `queued`) : un fichier `done` voit son score
    rafraîchi mais reste `done`.
    """
    now = datetime.now()
    decisions: list[tuple[int, FileStatus, str | None, int]] = []
    by_reason: Counter[str] = Counter()
    pending = excluded = 0
    plannable = {FileStatus.PENDING, FileStatus.EXCLUDED, FileStatus.QUEUED}

    for row in list(db.iter_files()):
        score = priority_score(row, now)
        reason = exclusion_reason(row, cfg)
        if reason is None:
            decisions.append((row.id, FileStatus.PENDING, None, score))
            if row.status in plannable:
                pending += 1
        else:
            decisions.append((row.id, FileStatus.EXCLUDED, reason, score))
            if row.status in plannable:
                excluded += 1
                by_reason[reason] += 1

    db.apply_plan(decisions)
    return PlanReport(pending=pending, excluded=excluded, by_reason=dict(by_reason))
