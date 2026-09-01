"""Filtre : exclusions (extension, taille, dossiers système) et score de priorité.

Deux fonctions pures — `exclusion_reason` et `priority_score` — décident pour
une ligne ; `plan_files` les applique à toute la base via `Database.apply_plan`.
Un fichier déjà `done` n'est jamais rétrogradé (c'est `apply_plan` qui garantit
cette règle).

`plan_files` travaille **en flux** : les fichiers sont lus par tranches, les
décisions remises groupées à la base. Une campagne de 934 000 fichiers coûtait
1,1 Go de mémoire (la table entière en `FileRow`) et 934 000 `UPDATE` un par un.
"""

from __future__ import annotations

import logging
import time
from collections import Counter
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import datetime

from docia.config import FilterConfig
from docia.db import Database
from docia.ingest.smbeagle_csv import parse_smbeagle_datetime
from docia.models import FileRow, FileStatus
from docia.views import format_int

logger = logging.getLogger(__name__)

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

PLAN_CHUNK = 5_000
"""Fichiers par tranche : rythme des rappels d'avancement et des `executemany`."""


@dataclass(frozen=True)
class PlanReport:
    """Bilan d'un `plan_files` : fichiers retenus, exclus, et exclusions par raison."""

    pending: int
    excluded: int
    by_reason: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class PlanProgress:
    """Avancement d'une préparation en cours, passé au rappel `progress` de `plan_files`.

    `files` compte les fichiers déjà décidés, `total` le nombre de fichiers de la
    base (connu d'avance, contrairement à l'import où seuls les octets le sont).
    """

    files: int
    total: int
    elapsed_s: float
    final: bool = False
    """Vrai pour le tout dernier appel : le bilan complet, jamais étranglé.

    Même rôle que `ImportProgress.final` — sans lui, la préparation d'une grosse
    campagne s'arrêtait sur « 97 % » et n'annonçait jamais sa fin.
    """

    @property
    def percent(self) -> float:
        """Avancement en pourcentage — 100 % dès que la préparation est terminée.

        Une base vide n'a pas de dénominateur : sans le cas `final`, la dernière
        ligne annoncerait « 0 % » pour un travail pourtant achevé.
        """
        if self.total <= 0:
            return 100.0 if self.final else 0.0
        return min(100.0, 100.0 * self.files / self.total)


def format_plan_progress(progress: PlanProgress) -> str:
    """Ligne de journal d'une préparation en cours : fichiers, pourcentage, durée.

    Même forme que `service.format_import_progress` : l'utilisateur retrouve
    exactement la même ligne pendant l'import et pendant la préparation.
    """
    files = format_int(progress.files)
    return f"préparation : {files} fichiers — {progress.percent:.0f} % — {progress.elapsed_s:.0f} s"


def plan_progress_logger(
    log: Callable[[str], None], *, min_seconds: float = 2.0, min_files: int = 50_000
) -> Callable[[PlanProgress], None]:
    """Rappel d'avancement de préparation qui écrit dans `log` sans l'inonder.

    Même cadence que `service.import_progress_logger` : une ligne au démarrage,
    puis au plus une toutes les `min_seconds` secondes ou tous les `min_files`
    fichiers, **et toujours une à la fin** (`PlanProgress.final`, qui court-circuite
    l'étranglement). Partagé par la CLI et l'interface — sans lui, `plan` reste
    muet une minute entière et l'utilisateur croit à un blocage.
    """
    last_files = 0
    last_at = 0.0
    last_line = ""
    started = False

    def emit(progress: PlanProgress) -> None:
        nonlocal last_files, last_at, last_line, started
        now = time.monotonic()
        if (
            not progress.final
            and started
            and progress.files - last_files < min_files
            and now - last_at < min_seconds
        ):
            return
        line = format_plan_progress(progress)
        if progress.final and started and line == last_line:
            return  # la dernière tranche vient d'annoncer exactement la même chose
        started, last_files, last_at, last_line = True, progress.files, now, line
        log(line)

    return emit


def _dotted(extension: str) -> str:
    """`"PDF"` ou `".pdf"` → `".pdf"` ; chaîne vide si pas d'extension."""
    value = extension.strip().lower().lstrip(".")
    return f".{value}" if value else ""


def _normalized_path(path: str) -> str:
    """Chemin comparable : antislashs et minuscules (Windows comme POSIX)."""
    return path.replace("/", "\\").lower()


@dataclass(frozen=True)
class _Rules:
    """Forme normalisée d'un `FilterConfig`, calculée une fois pour toute la base.

    Sans elle, `exclusion_reason` renormalisait extensions et marqueurs de dossier
    **à chaque fichier** : sur 934 000 fichiers et une vingtaine d'extensions
    exclues, cela faisait près de vingt millions de normalisations inutiles.
    """

    extensions: frozenset[str]
    markers: tuple[tuple[str, str], ...]
    """(marqueur normalisé, libellé d'origine — celui du message d'exclusion)."""
    min_size_bytes: int
    max_size_bytes: int


def _rules(cfg: FilterConfig) -> _Rules:
    markers = ((_normalized_path(m), m) for m in cfg.excluded_dir_markers)
    return _Rules(
        extensions=frozenset(_dotted(e) for e in cfg.excluded_extensions),
        markers=tuple((normalized, marker) for normalized, marker in markers if normalized),
        min_size_bytes=cfg.min_size_bytes,
        max_size_bytes=cfg.max_size_bytes,
    )


TOO_SMALL = "fichier trop petit"
TOO_LARGE = "fichier trop volumineux"
"""Raisons de taille, **sans la taille du fichier**.

La raison sert de clé de regroupement — `PlanReport.by_reason`, la colonne
`files.exclusion_reason` et le tableau « 5.2 Exclusions et erreurs » du rapport,
borné au top 10. En y écrivant les octets du fichier, chaque vidéo, chaque PST et
chaque VHD avait sa propre raison : 30 098 raisons distinctes pour 60 000
fichiers, un rapport qui n'affichait que dix tailles arbitraires au lieu de
« 60 000 fichiers trop petits », et `docia plan` qui déversait 30 103 lignes dans
la console. Le détail chiffré est dans le journal (les seuils appliqués, une fois
par préparation) et la taille de chaque fichier reste dans sa ligne (`size_bytes`).
"""


def _exclusion_reason(row: FileRow, rules: _Rules) -> str | None:
    """Cœur de `exclusion_reason`, sur une configuration déjà normalisée."""
    extension = _dotted(row.extension)
    if extension and extension in rules.extensions:
        return f"extension exclue ({extension})"
    if row.size_bytes < rules.min_size_bytes:
        return TOO_SMALL
    if row.size_bytes > rules.max_size_bytes:
        return TOO_LARGE
    path = _normalized_path(row.path)
    for normalized, marker in rules.markers:
        if normalized in path:
            return f"dossier exclu ({marker})"
    return None


def exclusion_reason(row: FileRow, cfg: FilterConfig) -> str | None:
    """Raison d'exclusion du fichier, ou `None` s'il est à analyser.

    Ordre : extension, taille minimale, taille maximale, dossier exclu.
    Usage unitaire : la configuration est normalisée à chaque appel. `plan_files`,
    qui parcourt toute la base, la normalise une seule fois (`_rules`).
    """
    return _exclusion_reason(row, _rules(cfg))


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


def plan_files(
    db: Database,
    cfg: FilterConfig,
    *,
    progress: Callable[[PlanProgress], None] | None = None,
    chunk_size: int = PLAN_CHUNK,
) -> PlanReport:
    """Applique exclusions et scores à toute la base, en flux.

    Les fichiers sont lus par tranches, sans tri (`iter_files(ordered=False)` :
    le plan les traite tous, l'ordre ne lui sert à rien et coûte un tri complet),
    et les décisions sont remises à `Database.apply_plan` sous forme de
    générateur — la base les groupe en `executemany` par `chunk_size`. Ni la
    liste des fichiers ni celle des décisions n'existe donc jamais en entier.

    Les compteurs ne portent que sur les fichiers que le plan peut réellement
    changer (`pending`, `excluded`, `queued`) : un fichier `done` voit son score
    rafraîchi mais reste `done`.

    Args:
        progress: rappel d'avancement, appelé au démarrage, tous les
            `chunk_size` fichiers, puis une dernière fois à la fin
            (voir `plan_progress_logger` pour la version « ligne de journal »).
        chunk_size: fichiers entre deux rappels, et taille des lots d'`UPDATE`.
    """
    now = datetime.now()
    rules = _rules(cfg)  # normalisée une fois, pas à chaque fichier
    by_reason: Counter[str] = Counter()
    pending = excluded = seen = 0
    plannable = {FileStatus.PENDING, FileStatus.EXCLUDED, FileStatus.QUEUED}
    started = time.monotonic()
    total = int(db.query("SELECT COUNT(*) AS n FROM files")[0]["n"])
    # Le détail chiffré des exclusions de taille est ici, une fois : les raisons
    # elles-mêmes doivent rester stables pour pouvoir être regroupées (`TOO_SMALL`).
    logger.info(
        "préparation : taille retenue entre %s o et %s o, %d extension(s) et %d dossier(s) exclus",
        format_int(rules.min_size_bytes),
        format_int(rules.max_size_bytes),
        len(rules.extensions),
        len(rules.markers),
    )

    def notify(*, final: bool = False) -> None:
        """Rend compte de l'avancement. `final=True` marque le tout dernier appel :
        l'afficheur ne doit pas l'étrangler, sinon la préparation s'arrête sur 97 %.

        Le rappel écrit dans une console, un journal ou une fenêtre : comme pour
        `import_csv`, un tube fermé ou une fenêtre détruite ne doit pas faire
        perdre la préparation d'une campagne entière.
        """
        if progress is None:
            return
        try:
            progress(
                PlanProgress(
                    files=seen, total=total, elapsed_s=time.monotonic() - started, final=final
                )
            )
        except Exception:  # noqa: BLE001 — l'affichage n'est jamais critique
            logger.debug("rappel de progression en échec, préparation poursuivie", exc_info=True)

    def decisions() -> Iterator[tuple[int, FileStatus, str | None, int]]:
        nonlocal pending, excluded, seen
        notify()
        for row in db.iter_files(ordered=False):
            score = priority_score(row, now)
            reason = _exclusion_reason(row, rules)
            decision: tuple[int, FileStatus, str | None, int]
            if reason is None:
                decision = (row.id, FileStatus.PENDING, None, score)
                if row.status in plannable:
                    pending += 1
            else:
                decision = (row.id, FileStatus.EXCLUDED, reason, score)
                if row.status in plannable:
                    excluded += 1
                    by_reason[reason] += 1
            seen += 1
            if seen % chunk_size == 0:
                notify()
            yield decision
        notify(final=True)

    db.apply_plan(decisions(), batch=chunk_size)
    return PlanReport(pending=pending, excluded=excluded, by_reason=dict(by_reason))
