"""Socle des vues : constantes, fragments SQL, aides de format, dataclasses, compteur de base.

Importé par toutes les autres vues ; n'importe aucune d'elles.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

from docia.db import Database, first_access_sql, latest_analysis_sql

SECURITY_CLASSES: tuple[str, ...] = ("C0", "C1", "C2", "C3", "N/A")
"""Classes de sécurité, dans l'ordre d'affichage (du moins au plus sensible)."""

RGPD_LEVELS: tuple[str, ...] = ("none", "low", "medium", "high", "critical", "N/A")
"""Niveaux de risque RGPD, dans l'ordre d'affichage."""

RETENTION_BASIS_LABELS: dict[str, str] = {
    "none": "aucun",
    "proof": "valeur probante",
    "legal": "obligation légale",
    "fiscal": "obligation fiscale",
    "rh": "ressources humaines",
    "contractual": "contractuel",
    "N/A": "non déterminé",
}

RETENTION_UNDETERMINED = "non déterminée"
"""Ce que porte la colonne « durée » quand le modèle exige la conservation sans durée.

`retention_required=1` avec `retention_years=0` : voir `RetentionRow.undetermined`.
Écrit une seule fois ici pour que le rapport, le classeur et le Markdown le disent
tous de la même façon."""

SIZE_BUCKETS: tuple[tuple[str, int, int], ...] = (
    ("0–10 Ko", 0, 10 * 1024),
    ("10 Ko–1 Mo", 10 * 1024, 1024 * 1024),
    ("1–10 Mo", 1024 * 1024, 10 * 1024 * 1024),
    ("10–100 Mo", 10 * 1024 * 1024, 100 * 1024 * 1024),
    ("> 100 Mo", 100 * 1024 * 1024, -1),
)
"""Tranches de taille (libellé, borne basse incluse, borne haute exclue ; -1 = ∞)."""

STALE_YEARS: tuple[int, ...] = (1, 3, 5, 10)
"""Seuils d'ancienneté par défaut, en années."""

REASON_TOP = 10
"""Raisons d'exclusion ou d'erreur listées par défaut (`status_summary`).

Borne explicite, et non plus un `LIMIT 10` en dur dans le SQL : les raisons
d'exclusion sont bornées par la configuration (extensions, marqueurs de dossier),
mais les raisons d'**erreur** sont du texte libre — une campagne peut en produire
des milliers. Ce qui manquait n'était pas la borne, c'était de le dire :
`StatusSummary.reasons_total` et `reasons_hidden` le disent maintenant."""

THOUSANDS_SEPARATOR = "\u00a0"
"""Espace insécable : les nombres ne se coupent pas en fin de ligne."""

_FROM_LATEST = " FROM analyses a JOIN files f ON f.id = a.file_id"
"""Clause `FROM` des vues « fichier + dernière analyse », à filtrer par `_IS_LATEST`.

Le parcours part des analyses : `analyses.file_id` référence toujours un fichier
existant (clé étrangère), l'ensemble des lignes est donc celui de
`files f JOIN analyses a ON a.id = (dernière analyse de f)`, mais sans balayer
les fichiers jamais analysés.

La jointure **doit** amener `files` (alias `f`) : `_IS_LATEST` y compare
`content_version`. Sans cette comparaison, un fichier modifié depuis son analyse
gardait sa classification, et le rapport combinait la **nouvelle** taille avec
l'**ancienne** classe — mesuré, un fichier passé de 2 à 9 Mo avec un contenu tout
autre, que la base marque pourtant `pending`, restait « candidat au nettoyage »
pour 9 Mo. Un rapport qui justifie des suppressions ne peut pas attribuer une
analyse à un contenu sur lequel elle n'a pas été faite.

La règle n'est pas neuve : `db._PENDING_WHERE` l'applique déjà pour décider ce qui
reste **à analyser** — c'est ce qui remet le fichier en file d'attente. Elle était
simplement oubliée du côté **lecture**."""


_IS_LATEST = latest_analysis_sql("a.file_id")
"""Analyse faisant foi : la dernière, **et** portant sur le contenu actuel.

La règle est définie une seule fois, dans `docia.db.latest_analysis_sql`, et
importée ici — elle y a été descendue précisément parce que `db` ne pouvait pas
l'importer de `views` (le cycle va dans l'autre sens) et en gardait une copie
textuelle, qui a fini par diverger."""

_SENSITIVE = "a.security_classification IN ('C2','C3')"
"""Classes de sécurité comptées comme sensibles."""

_RGPD_AT_RISK = "a.rgpd_risk_level IN ('high','critical')"
"""Niveaux RGPD comptés comme à risque."""

_CLEANUP_WHERE = (
    "a.retention_required=0 AND a.security_classification IN ('C0','C1')"
    " AND f.access_key <> '' AND f.access_key < ?"
)
"""Candidat au nettoyage : ni à conserver, ni sensible, ni accédé depuis le seuil.

La liste blanche `IN ('C0','C1')` est **la** garantie de sûreté de cette vue : un
fichier classé C2 ou C3 — comme un fichier non classé (`''`, `N/A`) — ne peut pas
y entrer. Écrite en liste noire (`NOT IN ('C2','C3')`), la moindre classe
nouvelle ou vide y serait tombée par défaut. `tests/test_views.py` interdit
explicitement C2 et C3 dans les candidats, quelle que soit l'ancienneté."""


# --------------------------------------------------------------------- helpers


FIRST_ACCESS_F = first_access_sql("f.")
"""Date d'accès affichée pour un candidat au nettoyage (voir `docia.db`)."""


def shift_years(day: date, years: int) -> date:
    """`day` décalé de `years` années, **borné** aux dates représentables.

    29 février → 28 février. Au-delà de l'an 9999 le résultat est `date.max`,
    en deçà de l'an 1 `date.min` : `date.replace(year=10009)` lèverait sinon
    `ValueError: year 10009 is out of range`, et un seul fichier daté de
    `DateTime.MaxValue` (9999-12-31 — ce que rend un FILETIME corrompu ou saturé,
    donc un fichier restauré d'une archive abîmée ou vu par un NAS à horloge
    cassée) faisait échouer **tous** les rapports de la campagne : `html`,
    `markdown`, `powerbi` et `xlsx` (via `retention_plan` et `powerbi._analyses_rows`).
    Un fichier aberrant ne doit jamais coûter le rapport.
    """
    year = day.year + years
    if year > date.max.year:
        return date.max
    if year < date.min.year:
        return date.min
    try:
        return day.replace(year=year)
    except ValueError:
        return day.replace(year=year, day=28)


def _key(day: date) -> str:
    return f"{day.year:04d}{day.month:02d}{day.day:02d}"


def _today(today: date | None) -> date:
    return today if today is not None else date.today()


def share_from_base(base: str) -> str:
    """Nom du partage tiré de la seule colonne `base` (`''` si elle est vide)."""
    return base.strip().rstrip("\\/")


def share_label(base: str, unc_directory: str) -> str:
    """Nom du partage : colonne `base` si présente, sinon `\\\\serveur\\partage`."""
    stripped = share_from_base(base)
    if stripped:
        return stripped
    text = unc_directory.replace("/", "\\").strip().rstrip("\\")
    if text.startswith("\\\\"):
        parts = [p for p in text[2:].split("\\") if p]
        if len(parts) >= 2:
            return f"\\\\{parts[0]}\\{parts[1]}"
        if parts:
            return f"\\\\{parts[0]}"
    parts = [p for p in text.split("\\") if p]
    return parts[0] if parts else "(inconnu)"


def directory_label(base: str, unc_directory: str, depth: int) -> str:
    """Partage + `depth` premiers niveaux de répertoire (regroupement lisible)."""
    share = share_label(base, unc_directory)
    text = unc_directory.replace("/", "\\").strip().rstrip("\\")
    rest = text[len(share) :] if text.lower().startswith(share.lower()) else text
    segments = [p for p in rest.split("\\") if p][:depth]
    return share + ("\\" + "\\".join(segments) if segments else "")


def format_bytes(value: int | float) -> str:
    """Octets → texte français court (`1,4 Go`)."""
    amount = float(value)
    for unit, limit in (("o", 1024.0), ("Ko", 1024.0**2), ("Mo", 1024.0**3), ("Go", 1024.0**4)):
        if abs(amount) < limit:
            scaled = amount / (limit / 1024.0)
            text = f"{scaled:.0f}" if unit == "o" else f"{scaled:.1f}"
            return f"{text.replace('.', ',')} {unit}"
    return f"{amount / 1024.0**4:.1f}".replace(".", ",") + " To"


def format_int(value: int) -> str:
    """Entier avec séparateur de milliers insécable."""
    return f"{value:,}".replace(",", THOUSANDS_SEPARATOR)


def percent(part: int, whole: int) -> float:
    """Pourcentage arrondi au dixième (0 si le total est nul)."""
    return round(100.0 * part / whole, 1) if whole else 0.0


# ------------------------------------------------------------------ dataclasses


@dataclass(frozen=True)
class GroupStat:
    """Un regroupement (extension, propriétaire, partage, tranche…)."""

    label: str
    files: int
    bytes: int
    percent_files: float = 0.0
    percent_bytes: float = 0.0


@dataclass(frozen=True)
class DuplicateFamily:
    """Une famille de doublons : même `fast_hash` et même taille."""

    family_id: str
    fast_hash: str
    size_bytes: int
    copies: int
    reclaimable_bytes: int
    paths: list[str] = field(default_factory=list)
    file_ids: list[int] = field(default_factory=list)


@dataclass(frozen=True)
class DuplicateReport:
    """Bilan des doublons : familles triées par octets récupérables décroissants."""

    families: list[DuplicateFamily]
    total_families: int
    total_copies: int
    total_reclaimable_bytes: int


@dataclass(frozen=True)
class StaleBucket:
    """Fichiers non accédés / non modifiés depuis `years` années."""

    years: int
    cutoff: date
    not_accessed_files: int
    not_accessed_bytes: int
    not_modified_files: int
    not_modified_bytes: int


@dataclass(frozen=True)
class TinyReport:
    """Fichiers vides ou minuscules (bruit de stockage).

    Pas d'échantillon de chemins : le champ `samples` existait, personne ne
    l'affichait, et la requête qui le remplissait (`SELECT path … ORDER BY path
    LIMIT 20`) était exécutée à chaque rapport, chaque classeur et chaque export.
    """

    max_bytes: int
    files: int
    bytes: int
    empty_files: int


@dataclass(frozen=True)
class StatusSummary:
    """Répartition des statuts et principales raisons d'exclusion."""

    counts: dict[str, int]
    bytes: dict[str, int]
    total_files: int
    total_bytes: int
    reasons: list[GroupStat] = field(default_factory=list)
    reasons_total: int = 0
    """Nombre **total** de raisons distinctes, `reasons` fût-il tronqué (voir `REASON_TOP`)."""

    @property
    def reasons_hidden(self) -> int:
        """Raisons que `reasons` ne montre pas — 0 quand rien n'est tronqué."""
        return max(self.reasons_total - len(self.reasons), 0)


@dataclass(frozen=True)
class AxisRow:
    """Une ligne de la matrice de classification (une valeur d'axe)."""

    label: str
    files: int
    bytes: int
    analyzed: int
    security: dict[str, int] = field(default_factory=dict)
    rgpd: dict[str, int] = field(default_factory=dict)

    @property
    def sensitive(self) -> int:
        """Fichiers classés C2 ou C3."""
        return self.security.get("C2", 0) + self.security.get("C3", 0)


@dataclass(frozen=True)
class SensitiveFile:
    """Un fichier du top sensible."""

    file_id: int
    path: str
    owner: str
    size_bytes: int
    security: str
    security_confidence: int
    rgpd: str
    rgpd_confidence: int
    resume: str
    justification: str
    review_status: str


@dataclass(frozen=True)
class RetentionRow:
    """Un fichier à conserver, avec sa date de fin de conservation.

    `end_date is None` signifie « fin de conservation non calculable » — durée non
    déterminée (voir `undetermined`) ou date de dernière écriture illisible. Un
    fichier sans date de fin n'est **jamais** `expired` : on ne propose pas à la
    suppression un fichier dont on ne sait pas quand sa conservation s'achève.
    """

    file_id: int
    path: str
    owner: str
    size_bytes: int
    years: int
    basis: str
    justification: str
    last_write_time: str
    end_date: date | None
    expired: bool

    @property
    def undetermined(self) -> bool:
        """Conservation exigée mais **durée non déterminée** (`years == 0`).

        Le modèle répond parfois « à conserver » avec une durée de zéro année —
        le schéma l'autorise (`minimum: 0`) et l'analyseur l'accepte. La durée est
        alors absente, pas nulle : c'est une réponse incohérente, pas une échéance
        immédiate.
        """
        return self.years <= 0


@dataclass(frozen=True)
class RetentionPlan:
    """Plan de conservation : lignes triées par échéance, totaux par fondement."""

    rows: list[RetentionRow]
    total_files: int
    total_bytes: int
    expired_files: int
    by_basis: list[GroupStat] = field(default_factory=list)
    undetermined_files: int = 0
    """Fichiers à conserver dont le modèle n'a pas donné de durée (`years == 0`).

    À faire trancher par un humain : ils ne sont ni échus, ni datés."""


@dataclass(frozen=True)
class CleanupRow:
    """Un candidat au nettoyage."""

    file_id: int
    path: str
    owner: str
    size_bytes: int
    access_time: str
    security: str


@dataclass(frozen=True)
class CleanupReport:
    """Candidats au nettoyage : ni à conserver, ni sensibles, ni accédés récemment."""

    years: int
    cutoff: date
    rows: list[CleanupRow]
    total_files: int
    total_bytes: int


@dataclass(frozen=True)
class Discrepancy:
    """Écart entre la classe rendue par la LLM et la classe corrigée par l'humain."""

    file_id: int
    path: str
    llm_security: str
    corrected_security: str
    llm_rgpd: str
    corrected_rgpd: str


@dataclass(frozen=True)
class ReviewProgress:
    """Avancement de la vérification humaine."""

    to_review: int
    validated: int
    corrected: int
    not_reviewed: int
    analyzed: int
    discrepancies: list[Discrepancy] = field(default_factory=list)
    total_discrepancies: int = 0
    """Nombre réel d'écarts, avant la coupe éventuelle de `discrepancies`."""

    @property
    def reviewed(self) -> int:
        return self.validated + self.corrected

    @property
    def percent_reviewed(self) -> float:
        return percent(self.reviewed, self.analyzed)


@dataclass(frozen=True)
class RunStat:
    """Un run : blocs, tokens, durée, coût moyen par fichier."""

    run_id: int
    started_at: str
    finished_at: str
    status: str
    model: str
    prompt_hash: str
    blocks: int
    blocks_done: int
    blocks_error: int
    files: int
    prompt_tokens: int
    completion_tokens: int
    duration_s: float
    avg_latency_ms: float
    tokens_per_file: float


@dataclass(frozen=True)
class Overview:
    """Chiffres clés de la synthèse (tuiles du rapport)."""

    generated_at: date
    db_path: str
    model: str
    prompt_name: str
    prompt_hash: str
    total_files: int
    total_bytes: int
    analyzed: int
    pending: int
    excluded: int
    errors: int
    duplicate_families: int
    duplicate_reclaimable_bytes: int
    stale_files: int
    stale_bytes: int
    stale_years: int
    sensitive_files: int
    rgpd_at_risk: int
    retention_files: int
    cleanup_files: int
    cleanup_bytes: int
    reviewed: int


def _count_latest(db: Database, condition: str, params: tuple[object, ...] = ()) -> int:
    """Nombre de fichiers dont la dernière analyse vérifie `condition`."""
    return int(
        db.query_values(
            f"SELECT COUNT(*){_FROM_LATEST} WHERE {_IS_LATEST} AND {condition}", params
        )[0][0]
    )
