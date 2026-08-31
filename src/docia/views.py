"""Vues d'analyse : la seule source de vérité pour la CLI, la GUI et le rapport.

Fonctions pures : elles prennent un `Database`, ne font que des `SELECT`
(via `Database.query` / `Database.query_values`) et rendent des dataclasses
triées, avec totaux.

Les dates SMBeagle sont stockées en TEXT (`dd/MM/yyyy HH:mm:ss`). Les
comparaisons d'ancienneté se font sur les clés `yyyymmdd` normalisées à
l'écriture (`files.access_key`, `files.write_key`, schéma v6, indexées), et les
calculs de dates (fin de conservation) en Python via `parse_smbeagle_datetime`.
Toutes les vues qui dépendent de « aujourd'hui » acceptent `today=` pour être
testables.

Les vues qui croisent fichiers et analyses partent de la table `analyses`
(`_FROM_LATEST`) et non des fichiers : seule une minorité des fichiers est
analysée, et la clé étrangère garantit le même ensemble de lignes.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date
from typing import Any

from docia.db import Database, first_access_sql
from docia.ingest.smbeagle_csv import parse_smbeagle_datetime

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

THOUSANDS_SEPARATOR = "\u00a0"
"""Espace insécable : les nombres ne se coupent pas en fin de ligne."""

_FROM_LATEST = " FROM analyses a JOIN files f ON f.id = a.file_id"
"""Clause `FROM` des vues « fichier + dernière analyse », à filtrer par `_IS_LATEST`.

Le parcours part des analyses : `analyses.file_id` référence toujours un fichier
existant (clé étrangère), l'ensemble des lignes est donc celui de
`files f JOIN analyses a ON a.id = (dernière analyse de f)`, mais sans balayer
les fichiers jamais analysés."""

_IS_LATEST = (
    "a.id = (SELECT id FROM analyses WHERE file_id = a.file_id"
    " ORDER BY created_at DESC, id DESC LIMIT 1)"
)
"""Ne retient que la dernière analyse d'un fichier (comme `Database.latest_analyses`)."""

_SENSITIVE = "a.security_classification IN ('C2','C3')"
"""Classes de sécurité comptées comme sensibles."""

_RGPD_AT_RISK = "a.rgpd_risk_level IN ('high','critical')"
"""Niveaux RGPD comptés comme à risque."""

_CLEANUP_WHERE = (
    "a.retention_required=0 AND a.security_classification IN ('C0','C1')"
    " AND f.access_key <> '' AND f.access_key < ?"
)
"""Candidat au nettoyage : ni à conserver, ni sensible, ni accédé depuis le seuil."""


# --------------------------------------------------------------------- helpers


FIRST_ACCESS_F = first_access_sql("f.")
"""Date d'accès affichée pour un candidat au nettoyage (voir `docia.db`)."""


def shift_years(day: date, years: int) -> date:
    """`day` décalé de `years` années (29 février → 28 février)."""
    try:
        return day.replace(year=day.year + years)
    except ValueError:
        return day.replace(year=day.year + years, day=28)


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
    """Fichiers vides ou minuscules (bruit de stockage)."""

    max_bytes: int
    files: int
    bytes: int
    empty_files: int
    samples: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class StatusSummary:
    """Répartition des statuts et principales raisons d'exclusion."""

    counts: dict[str, int]
    bytes: dict[str, int]
    total_files: int
    total_bytes: int
    reasons: list[GroupStat] = field(default_factory=list)


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
    """Un fichier à conserver, avec sa date de fin de conservation."""

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


@dataclass(frozen=True)
class RetentionPlan:
    """Plan de conservation : lignes triées par échéance, totaux par fondement."""

    rows: list[RetentionRow]
    total_files: int
    total_bytes: int
    expired_files: int
    by_basis: list[GroupStat] = field(default_factory=list)


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


# ------------------------------------------------------------------ hygiène


def duplicates(db: Database, *, min_copies: int = 2, limit: int | None = None) -> DuplicateReport:
    """Familles de fichiers identiques (`fast_hash` + taille) et espace récupérable.

    L'espace récupérable d'une famille vaut `taille × (exemplaires − 1)` : un
    exemplaire est conservé. Les fichiers sans empreinte sont ignorés.
    """
    rows = db.query_values(
        "SELECT fast_hash, size_bytes, COUNT(*) AS copies,"
        " size_bytes*(COUNT(*)-1) AS reclaimable"
        " FROM files WHERE fast_hash <> '' GROUP BY fast_hash, size_bytes"
        " HAVING COUNT(*) >= ? ORDER BY reclaimable DESC, copies DESC, fast_hash",
        (min_copies,),
    )
    total_families = len(rows)
    total_copies = sum(int(r[2]) for r in rows)
    total_reclaimable = sum(int(r[3]) for r in rows)
    kept = rows if limit is None else rows[:limit]
    families: list[DuplicateFamily] = []
    for fast_hash, size_bytes, copies, reclaimable in kept:
        members = db.query_values(
            "SELECT id, path FROM files WHERE fast_hash=? AND size_bytes=? ORDER BY path",
            (fast_hash, size_bytes),
        )
        families.append(
            DuplicateFamily(
                family_id=f"{fast_hash}-{int(size_bytes)}",
                fast_hash=str(fast_hash),
                size_bytes=int(size_bytes),
                copies=int(copies),
                reclaimable_bytes=int(reclaimable),
                paths=[str(member[1]) for member in members],
                file_ids=[int(member[0]) for member in members],
            )
        )
    return DuplicateReport(families, total_families, total_copies, total_reclaimable)


def _below(histogram: list[tuple[str, int, int]], key: str) -> tuple[int, int]:
    """(fichiers, octets) des dates strictement antérieures à `key`."""
    files = size = 0
    for day, count, total in histogram:
        if day < key:
            files += count
            size += total
    return files, size


def stale_files(
    db: Database, *, years: tuple[int, ...] = STALE_YEARS, today: date | None = None
) -> list[StaleBucket]:
    """Pour chaque seuil : fichiers non accédés et non modifiés depuis N années.

    Une seule requête, quel que soit le nombre de seuils : les clés de date
    indexées sont réduites en histogrammes (une ligne par jour), que chaque seuil
    se contente ensuite de cumuler.
    """
    reference = _today(today)
    rows = db.query_values(
        "SELECT 'a' AS src, access_key AS k, COUNT(*) AS n, COALESCE(SUM(size_bytes),0) AS b"
        " FROM files WHERE access_key <> '' GROUP BY k"
        " UNION ALL"
        " SELECT 'w', write_key, COUNT(*), COALESCE(SUM(size_bytes),0)"
        " FROM files WHERE write_key <> '' GROUP BY write_key"
    )
    accessed = [(str(r[1]), int(r[2]), int(r[3])) for r in rows if r[0] == "a"]
    written = [(str(r[1]), int(r[2]), int(r[3])) for r in rows if r[0] == "w"]
    buckets: list[StaleBucket] = []
    for n in sorted(years):
        cutoff = shift_years(reference, -n)
        key = _key(cutoff)
        access_files, access_bytes = _below(accessed, key)
        write_files, write_bytes = _below(written, key)
        buckets.append(
            StaleBucket(
                years=n,
                cutoff=cutoff,
                not_accessed_files=access_files,
                not_accessed_bytes=access_bytes,
                not_modified_files=write_files,
                not_modified_bytes=write_bytes,
            )
        )
    return buckets


def _totals(db: Database) -> tuple[int, int]:
    row = db.query("SELECT COUNT(*) AS n, COALESCE(SUM(size_bytes),0) AS b FROM files")[0]
    return int(row["n"]), int(row["b"])


def _grouped(
    db: Database, expression: str, *, limit: int | None, empty_label: str
) -> list[GroupStat]:
    total_files, total_bytes = _totals(db)
    rows = db.query(
        f"SELECT {expression} AS k, COUNT(*) AS n, COALESCE(SUM(size_bytes),0) AS b"
        " FROM files GROUP BY k ORDER BY b DESC, n DESC, k"
    )
    stats = [
        GroupStat(
            label=str(r["k"]) or empty_label,
            files=int(r["n"]),
            bytes=int(r["b"]),
            percent_files=percent(int(r["n"]), total_files),
            percent_bytes=percent(int(r["b"]), total_bytes),
        )
        for r in rows
    ]
    return stats if limit is None else stats[:limit]


def by_extension(db: Database, *, limit: int | None = None) -> list[GroupStat]:
    """Répartition par extension (volume décroissant)."""
    return _grouped(db, "extension", limit=limit, empty_label="(sans extension)")


def by_owner(db: Database, *, limit: int | None = None) -> list[GroupStat]:
    """Répartition par propriétaire (volume décroissant)."""
    return _grouped(db, "owner", limit=limit, empty_label="(inconnu)")


def by_share(db: Database, *, limit: int | None = None) -> list[GroupStat]:
    """Répartition par partage (`base`, sinon `\\\\serveur\\partage`)."""
    total_files, total_bytes = _totals(db)
    keys = _axis_group(db, "share")
    width = len(keys)
    label_of = _axis_labeller("share", 0)
    files: dict[str, int] = {}
    volume: dict[str, int] = {}
    for row in _axis_volumes(db, keys):
        label = label_of(row)
        files[label] = files.get(label, 0) + int(row[width])
        volume[label] = volume.get(label, 0) + int(row[width + 1])
    stats = [
        GroupStat(
            label,
            files[label],
            volume[label],
            percent(files[label], total_files),
            percent(volume[label], total_bytes),
        )
        for label in files
    ]
    stats.sort(key=lambda s: (-s.bytes, -s.files, s.label))
    return stats if limit is None else stats[:limit]


def size_buckets(db: Database) -> list[GroupStat]:
    """Répartition par tranche de taille.

    Une requête par tranche : bornée sur `size_bytes`, chacune se lit dans un
    index couvrant, ce qui reste moins cher qu'un balayage unique évaluant
    autant de `CASE` que de tranches sur chaque ligne.
    """
    total_files, total_bytes = _totals(db)
    stats: list[GroupStat] = []
    for label, low, high in SIZE_BUCKETS:
        clause = "size_bytes >= ?" + ("" if high < 0 else " AND size_bytes < ?")
        params: tuple[object, ...] = (low,) if high < 0 else (low, high)
        count, size = db.query_values(
            f"SELECT COUNT(*), COALESCE(SUM(size_bytes),0) FROM files WHERE {clause}", params
        )[0]
        stats.append(
            GroupStat(
                label,
                int(count),
                int(size),
                percent(int(count), total_files),
                percent(int(size), total_bytes),
            )
        )
    return stats


def empty_or_tiny(db: Database, *, max_bytes: int = 100, samples: int = 20) -> TinyReport:
    """Fichiers vides ou d'au plus `max_bytes` octets."""
    row = db.query(
        "SELECT COUNT(*) AS n, COALESCE(SUM(size_bytes),0) AS b,"
        " SUM(CASE WHEN size_bytes=0 THEN 1 ELSE 0 END) AS z FROM files WHERE size_bytes <= ?",
        (max_bytes,),
    )[0]
    paths = db.query(
        "SELECT path FROM files WHERE size_bytes <= ? ORDER BY path LIMIT ?", (max_bytes, samples)
    )
    return TinyReport(
        max_bytes=max_bytes,
        files=int(row["n"] or 0),
        bytes=int(row["b"] or 0),
        empty_files=int(row["z"] or 0),
        samples=[str(p["path"]) for p in paths],
    )


def status_summary(db: Database) -> StatusSummary:
    """Compteurs par statut et top 10 des raisons d'exclusion ou d'erreur."""
    total_files, total_bytes = _totals(db)
    counts: dict[str, int] = {}
    volume: dict[str, int] = {}
    for r in db.query(
        "SELECT status, COUNT(*) AS n, COALESCE(SUM(size_bytes),0) AS b FROM files GROUP BY status"
    ):
        counts[str(r["status"])] = int(r["n"])
        volume[str(r["status"])] = int(r["b"])
    reasons = [
        GroupStat(
            str(r["reason"]),
            int(r["n"]),
            int(r["b"]),
            percent(int(r["n"]), total_files),
            percent(int(r["b"]), total_bytes),
        )
        for r in db.query(
            "SELECT exclusion_reason AS reason, COUNT(*) AS n, COALESCE(SUM(size_bytes),0) AS b"
            " FROM files WHERE exclusion_reason IS NOT NULL AND exclusion_reason <> ''"
            " GROUP BY reason ORDER BY n DESC, reason LIMIT 10"
        )
    ]
    return StatusSummary(counts, volume, total_files, total_bytes, reasons)


# ------------------------------------------------------------------ risque


AXES: tuple[str, ...] = ("share", "owner", "directory", "extension")
"""Axes acceptés par `classification_matrix`."""

_SIMPLE_AXES: dict[str, tuple[str, str]] = {
    "owner": ("f.owner", "(inconnu)"),
    "extension": ("f.extension", "(sans extension)"),
}
"""Axes dont l'étiquette est la colonne SQL elle-même : (colonne, libellé si vide)."""


def _share_named_by_base(db: Database) -> bool:
    """Vrai si toutes les valeurs de `base` nomment déjà leur partage.

    Dans ce cas — celui de tout scan SMBeagle — `share_label` ne regarde jamais
    `unc_directory` : le regroupement SQL peut l'ignorer et rendre un groupe par
    partage au lieu d'un groupe par répertoire.
    """
    rows = db.query_values("SELECT DISTINCT base FROM files")
    return all(share_from_base(str(r[0])) for r in rows)


_FILLER = "''"
"""Clé d'axe inutilisée : garde la largeur des lignes sans peser sur le regroupement."""


def _axis_group(db: Database, axis: str) -> list[str]:
    """Expressions SQL identifiant un groupe pour cet axe, dans l'ordre des colonnes."""
    if axis in _SIMPLE_AXES:
        return [_SIMPLE_AXES[axis][0]]
    if axis == "share" and _share_named_by_base(db):
        return ["f.base", _FILLER]
    return ["f.base", "f.unc_directory"]


def _group_by(keys: list[str], extra: int) -> str:
    """Clause `GROUP BY` positionnelle : les clés réelles, puis `extra` colonnes."""
    positions = [i + 1 for i, key in enumerate(keys) if key != _FILLER]
    positions += [len(keys) + i + 1 for i in range(extra)]
    return ", ".join(str(position) for position in positions)


def _axis_volumes(db: Database, keys: list[str]) -> list[tuple[Any, ...]]:
    """Clés d'axe, puis nombre de fichiers et octets (tous les fichiers)."""
    return db.query_values(
        f"SELECT {', '.join(keys)}, COUNT(*), COALESCE(SUM(f.size_bytes),0)"
        f" FROM files f GROUP BY {_group_by(keys, 0)}"
    )


def _axis_risk(db: Database, keys: list[str]) -> list[tuple[Any, ...]]:
    """Clés d'axe, puis sécurité, RGPD et nombre de fichiers (fichiers analysés)."""
    return db.query_values(
        f"SELECT {', '.join(keys)}, a.security_classification, a.rgpd_risk_level, COUNT(*)"
        f"{_FROM_LATEST} WHERE {_IS_LATEST} GROUP BY {_group_by(keys, 2)}"
    )


def _run_prefix(text: str, segments: int) -> str:
    """Préfixe de `text` couvrant ses `segments` premiers niveaux (`''` s'il en manque).

    Le préfixe s'arrête juste après le `segments`-ième antislash : deux chemins
    qui le partagent ont exactement les mêmes premiers niveaux.
    """
    position = 0
    for _ in range(segments):
        position = text.find("\\", position) + 1
        if position == 0:
            return ""
    return text[:position]


def _axis_labeller(axis: str, depth: int) -> Callable[[tuple[Any, ...]], str]:
    """Rend la fonction qui étiquette une ligne d'axe (clés d'axe en tête de ligne).

    `share_label` ne lit que les deux premiers niveaux d'un répertoire, et
    `directory_label` `depth` de plus : deux répertoires qui partagent ces
    niveaux portent la même étiquette. Les lignes arrivant groupées par
    répertoire, l'étiquette n'est donc recalculée qu'au changement
    d'arborescence — sur un parc où presque chaque fichier a son répertoire,
    c'est l'essentiel du coût de la vue.
    """
    if axis in _SIMPLE_AXES:
        empty = _SIMPLE_AXES[axis][1]

        def by_column(row: tuple[Any, ...]) -> str:
            return str(row[0] or empty)

        return by_column

    segments = 4 if axis == "share" else depth + 4
    previous_base: Any = None
    prefix = ""
    label = ""

    def by_path(row: tuple[Any, ...]) -> str:
        nonlocal previous_base, prefix, label
        base, directory = row[0], row[1]  # colonnes TEXT NOT NULL : toujours des chaînes
        if base != previous_base or not prefix or not directory.startswith(prefix):
            label = (
                share_label(base, directory)
                if axis == "share"
                else directory_label(base, directory, depth)
            )
            prefix = _run_prefix(directory, segments)
            previous_base = base
        return label

    return by_path


def classification_matrix(
    db: Database, *, axis: str = "share", depth: int = 2, limit: int | None = None
) -> list[AxisRow]:
    """Classification par valeur d'axe : `share`, `owner`, `directory` ou `extension`.

    Deux requêtes ciblées sur l'axe demandé : la volumétrie sur l'index couvrant
    des fichiers, la répartition sécurité/RGPD sur les seuls fichiers analysés.

    Raises:
        ValueError: axe inconnu.
    """
    if axis not in AXES:
        raise ValueError(f"axe inconnu : {axis}")
    keys = _axis_group(db, axis)
    width = len(keys)
    label_of = _axis_labeller(axis, depth)
    risk: dict[str, list[tuple[str, str, int]]] = {}
    for row in _axis_risk(db, keys):
        classification = row[width]
        if classification:
            risk.setdefault(label_of(row), []).append(
                (classification, row[width + 1], row[width + 2])
            )
    label_of = _axis_labeller(axis, depth)
    totals: dict[str, list[int]] = {}
    for row in _axis_volumes(db, keys):
        label = label_of(row)
        entry = totals.get(label)
        if entry is None:
            totals[label] = [row[width], row[width + 1]]
        else:
            entry[0] += row[width]
            entry[1] += row[width + 1]
    out: list[AxisRow] = []
    for label, (count, size) in totals.items():
        security = dict.fromkeys(SECURITY_CLASSES, 0)
        rgpd = dict.fromkeys(RGPD_LEVELS, 0)
        analyzed = 0
        for classification, level, number in risk.get(label, ()):
            analyzed += number
            security[classification] = security.get(classification, 0) + number
            rgpd[level] = rgpd.get(level, 0) + number
        out.append(
            AxisRow(
                label=label,
                files=count,
                bytes=size,
                analyzed=analyzed,
                security=security,
                rgpd=rgpd,
            )
        )
    out.sort(key=lambda r: (-r.sensitive, -r.analyzed, -r.bytes, r.label))
    return out if limit is None else out[:limit]


def by_directory(db: Database, *, depth: int = 2, limit: int | None = None) -> list[AxisRow]:
    """Répertoires (partage + `depth` niveaux) : volume et répartition sécurité."""
    rows = classification_matrix(db, axis="directory", depth=depth)
    rows.sort(key=lambda r: (-r.bytes, -r.files, r.label))
    return rows if limit is None else rows[:limit]


def top_sensitive(db: Database, *, limit: int = 50) -> list[SensitiveFile]:
    """Fichiers les plus sensibles : C2/C3, ou RGPD `high`/`critical`."""
    rows = db.query(
        "SELECT f.id AS id, f.path AS path, f.owner AS owner, f.size_bytes AS size,"
        " a.security_classification AS sec, a.security_confidence AS secc,"
        " a.security_justification AS just, a.rgpd_risk_level AS rgpd,"
        " a.rgpd_confidence AS rgpdc, a.resume AS resume,"
        " COALESCE(r.status,'') AS review"
        f"{_FROM_LATEST} LEFT JOIN reviews r ON r.file_id = f.id"
        f" WHERE {_IS_LATEST} AND ({_SENSITIVE} OR {_RGPD_AT_RISK})"
        " ORDER BY CASE a.security_classification WHEN 'C3' THEN 0 WHEN 'C2' THEN 1"
        " WHEN 'C1' THEN 2 WHEN 'C0' THEN 3 ELSE 4 END,"
        " CASE a.rgpd_risk_level WHEN 'critical' THEN 0 WHEN 'high' THEN 1 WHEN 'medium' THEN 2"
        " WHEN 'low' THEN 3 ELSE 4 END, f.size_bytes DESC, f.path LIMIT ?",
        (limit,),
    )
    return [
        SensitiveFile(
            file_id=int(r["id"]),
            path=str(r["path"]),
            owner=str(r["owner"]),
            size_bytes=int(r["size"]),
            security=str(r["sec"]),
            security_confidence=int(r["secc"]),
            rgpd=str(r["rgpd"]),
            rgpd_confidence=int(r["rgpdc"]),
            resume=str(r["resume"]),
            justification=str(r["just"]),
            review_status=str(r["review"]),
        )
        for r in rows
    ]


def retention_plan(
    db: Database, *, today: date | None = None, limit: int | None = None
) -> RetentionPlan:
    """Fichiers à conserver, avec la date de fin = dernière écriture + `years`."""
    reference = _today(today)
    rows = db.query_values(
        "SELECT f.id, f.path, f.owner, f.size_bytes, f.last_write_time,"
        " a.retention_years, a.retention_basis, a.retention_justification"
        f"{_FROM_LATEST} WHERE {_IS_LATEST} AND a.retention_required=1 ORDER BY f.path"
    )
    plan: list[RetentionRow] = []
    by_basis_files: dict[str, int] = {}
    by_basis_bytes: dict[str, int] = {}
    total_bytes = 0
    expired = 0
    for file_id, path, owner, size, written_at, retained, basis, justification in rows:
        years = int(retained or 0)
        written = parse_smbeagle_datetime(str(written_at))
        end = shift_years(written.date(), years) if written is not None else None
        is_expired = end is not None and end <= reference
        expired += int(is_expired)
        total_bytes += int(size)
        by_basis_files[basis] = by_basis_files.get(basis, 0) + 1
        by_basis_bytes[basis] = by_basis_bytes.get(basis, 0) + int(size)
        plan.append(
            RetentionRow(
                file_id=int(file_id),
                path=str(path),
                owner=str(owner),
                size_bytes=int(size),
                years=years,
                basis=str(basis),
                justification=str(justification),
                last_write_time=str(written_at),
                end_date=end,
                expired=is_expired,
            )
        )
    plan.sort(key=lambda x: (x.end_date or date.max, x.path))
    total_files = len(plan)
    by_basis = [
        GroupStat(
            RETENTION_BASIS_LABELS.get(basis, basis),
            by_basis_files[basis],
            by_basis_bytes[basis],
            percent(by_basis_files[basis], total_files),
            percent(by_basis_bytes[basis], total_bytes),
        )
        for basis in sorted(by_basis_files, key=lambda b: (-by_basis_files[b], b))
    ]
    return RetentionPlan(
        rows=plan if limit is None else plan[:limit],
        total_files=total_files,
        total_bytes=total_bytes,
        expired_files=expired,
        by_basis=by_basis,
    )


def cleanup_candidates(
    db: Database, *, years: int = 5, today: date | None = None, limit: int | None = None
) -> CleanupReport:
    """Fichiers libérables : sans obligation de conservation, non sensibles (C0/C1)
    et non accédés depuis `years` années."""
    reference = _today(today)
    cutoff = shift_years(reference, -years)
    rows = db.query(
        "SELECT f.id AS id, f.path AS path, f.owner AS owner, f.size_bytes AS size,"
        f" {FIRST_ACCESS_F} AS at, a.security_classification AS sec"
        f"{_FROM_LATEST} WHERE {_IS_LATEST} AND {_CLEANUP_WHERE}"
        " ORDER BY f.size_bytes DESC, f.path",
        (_key(cutoff),),
    )
    candidates = [
        CleanupRow(
            file_id=int(r["id"]),
            path=str(r["path"]),
            owner=str(r["owner"]),
            size_bytes=int(r["size"]),
            access_time=str(r["at"]),
            security=str(r["sec"]),
        )
        for r in rows
    ]
    return CleanupReport(
        years=years,
        cutoff=cutoff,
        rows=candidates if limit is None else candidates[:limit],
        total_files=len(candidates),
        total_bytes=sum(c.size_bytes for c in candidates),
    )


def _review_counts(db: Database) -> dict[str, int]:
    """Nombre de fichiers par statut de vérification humaine."""
    return {
        str(r[0]): int(r[1])
        for r in db.query_values("SELECT status, COUNT(*) FROM reviews GROUP BY status")
    }


def _analyzed_files(db: Database) -> int:
    """Nombre de fichiers ayant au moins une analyse."""
    return int(db.query_values("SELECT COUNT(DISTINCT file_id) FROM analyses")[0][0])


def review_progress(db: Database, *, limit: int | None = None) -> ReviewProgress:
    """Avancement des revues et écarts entre classe LLM et classe corrigée."""
    counts = _review_counts(db)
    analyzed = _analyzed_files(db)
    reviewed = sum(counts.values())
    if limit == 0:  # seuls les compteurs sont demandés : pas de recherche d'écarts
        return ReviewProgress(
            to_review=counts.get("to_review", 0),
            validated=counts.get("validated", 0),
            corrected=counts.get("corrected", 0),
            not_reviewed=max(analyzed - reviewed, 0),
            analyzed=analyzed,
        )
    rows = db.query(
        "SELECT f.id AS id, f.path AS path, a.security_classification AS sec,"
        " a.rgpd_risk_level AS rgpd, COALESCE(r.corrected_security,'') AS csec,"
        " COALESCE(r.corrected_rgpd,'') AS crgpd"
        f"{_FROM_LATEST} JOIN reviews r ON r.file_id = f.id"
        f" WHERE {_IS_LATEST} AND ("
        "       (r.corrected_security IS NOT NULL AND r.corrected_security <> ''"
        "        AND r.corrected_security <> a.security_classification)"
        "    OR (r.corrected_rgpd IS NOT NULL AND r.corrected_rgpd <> ''"
        "        AND r.corrected_rgpd <> a.rgpd_risk_level))"
        " ORDER BY f.path"
    )
    gaps = [
        Discrepancy(
            file_id=int(r["id"]),
            path=str(r["path"]),
            llm_security=str(r["sec"]),
            corrected_security=str(r["csec"]),
            llm_rgpd=str(r["rgpd"]),
            corrected_rgpd=str(r["crgpd"]),
        )
        for r in rows
    ]
    return ReviewProgress(
        to_review=counts.get("to_review", 0),
        validated=counts.get("validated", 0),
        corrected=counts.get("corrected", 0),
        not_reviewed=max(analyzed - reviewed, 0),
        analyzed=analyzed,
        discrepancies=gaps if limit is None else gaps[:limit],
    )


def runs_summary(db: Database) -> list[RunStat]:
    """Un enregistrement par run : blocs, tokens, durée, tokens moyens par fichier."""
    rows = db.query(
        "SELECT r.id AS id, r.started_at AS started, COALESCE(r.finished_at,'') AS finished,"
        " r.status AS status, r.model AS model, r.prompt_hash AS ph,"
        " COUNT(b.id) AS blocks,"
        " SUM(CASE WHEN b.status='done' THEN 1 ELSE 0 END) AS done,"
        " SUM(CASE WHEN b.status='error' THEN 1 ELSE 0 END) AS err,"
        " COALESCE(SUM(b.file_count),0) AS files,"
        " COALESCE(SUM(b.usage_prompt_tokens),0) AS ptok,"
        " COALESCE(SUM(b.usage_completion_tokens),0) AS ctok,"
        " COALESCE(AVG(b.latency_ms),0) AS lat"
        " FROM runs r LEFT JOIN blocks b ON b.run_id = r.id"
        " GROUP BY r.id ORDER BY r.id DESC"
    )
    out: list[RunStat] = []
    for r in rows:
        started = parse_smbeagle_datetime(str(r["started"]))
        finished = parse_smbeagle_datetime(str(r["finished"]))
        duration = (finished - started).total_seconds() if started and finished else 0.0
        files = int(r["files"])
        tokens = int(r["ptok"]) + int(r["ctok"])
        out.append(
            RunStat(
                run_id=int(r["id"]),
                started_at=str(r["started"]),
                finished_at=str(r["finished"]),
                status=str(r["status"]),
                model=str(r["model"]),
                prompt_hash=str(r["ph"]),
                blocks=int(r["blocks"]),
                blocks_done=int(r["done"] or 0),
                blocks_error=int(r["err"] or 0),
                files=files,
                prompt_tokens=int(r["ptok"]),
                completion_tokens=int(r["ctok"]),
                duration_s=round(max(duration, 0.0), 1),
                avg_latency_ms=round(float(r["lat"] or 0.0), 1),
                tokens_per_file=round(tokens / files, 1) if files else 0.0,
            )
        )
    return out


# ------------------------------------------------------------------ synthèse


def _count_latest(db: Database, condition: str, params: tuple[object, ...] = ()) -> int:
    """Nombre de fichiers dont la dernière analyse vérifie `condition`."""
    return int(
        db.query_values(
            f"SELECT COUNT(*){_FROM_LATEST} WHERE {_IS_LATEST} AND {condition}", params
        )[0][0]
    )


def overview(db: Database, *, today: date | None = None, stale_years: int = 5) -> Overview:
    """Chiffres clés : volumétrie, hygiène, risque, vérification.

    Ne demande que des agrégats : aucune des vues détaillées (doublons, plan de
    conservation, écarts de revue…) n'est reconstruite pour n'en garder qu'un
    total, et la volumétrie n'est comptée qu'une fois.
    """
    reference = _today(today)
    total_files, total_bytes = _totals(db)
    status = {
        str(r[0]): int(r[1])
        for r in db.query_values("SELECT status, COUNT(*) FROM files GROUP BY status")
    }
    families, reclaimable = db.query_values(
        "SELECT COUNT(*), COALESCE(SUM(reclaimable),0) FROM"
        " (SELECT size_bytes*(COUNT(*)-1) AS reclaimable FROM files WHERE fast_hash <> ''"
        "  GROUP BY fast_hash, size_bytes HAVING COUNT(*) >= 2)"
    )[0]
    stale_key = _key(shift_years(reference, -stale_years))
    stale_count, stale_bytes = db.query_values(
        "SELECT COUNT(*), COALESCE(SUM(size_bytes),0) FROM files"
        " WHERE access_key <> '' AND access_key < ?",
        (stale_key,),
    )[0]
    cleanup_count, cleanup_bytes = db.query_values(
        f"SELECT COUNT(*), COALESCE(SUM(f.size_bytes),0){_FROM_LATEST}"
        f" WHERE {_IS_LATEST} AND {_CLEANUP_WHERE}",
        (stale_key,),
    )[0]
    reviews = _review_counts(db)
    model_row = db.query(
        "SELECT model, prompt_hash FROM analyses ORDER BY created_at DESC, id DESC LIMIT 1"
    )
    active = db.active_prompt()
    return Overview(
        generated_at=reference,
        db_path=str(db.path),
        model=str(model_row[0]["model"]) if model_row else "",
        prompt_name=active[0] if active else "(embarqué)",
        prompt_hash=str(model_row[0]["prompt_hash"]) if model_row else "",
        total_files=total_files,
        total_bytes=total_bytes,
        analyzed=_analyzed_files(db),
        pending=status.get("pending", 0),
        excluded=status.get("excluded", 0),
        errors=status.get("error", 0),
        duplicate_families=int(families),
        duplicate_reclaimable_bytes=int(reclaimable),
        stale_files=int(stale_count),
        stale_bytes=int(stale_bytes),
        stale_years=stale_years,
        sensitive_files=_count_latest(db, _SENSITIVE),
        rgpd_at_risk=_count_latest(db, _RGPD_AT_RISK),
        retention_files=_count_latest(db, "a.retention_required=1"),
        cleanup_files=int(cleanup_count),
        cleanup_bytes=int(cleanup_bytes),
        reviewed=reviews.get("validated", 0) + reviews.get("corrected", 0),
    )
