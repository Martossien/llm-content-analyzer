"""Vues d'analyse : la seule source de vérité pour la CLI, la GUI et le rapport.

Fonctions pures : elles prennent un `Database`, ne font que des `SELECT`
(via `Database.query`) et rendent des dataclasses triées, avec totaux.

Les dates SMBeagle sont stockées en TEXT (`dd/MM/yyyy HH:mm:ss`). Les
comparaisons se font en SQL sur une clé `yyyyMMdd` reconstruite par `substr()`
(comparaison lexicographique correcte), et les calculs de dates (fin de
conservation) en Python via `parse_smbeagle_datetime`. Toutes les vues qui
dépendent de « aujourd'hui » acceptent `today=` pour être testables.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

from docia.db import Database
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

_LATEST = (
    "LEFT JOIN analyses a ON a.id = (SELECT id FROM analyses WHERE file_id=f.id"
    " ORDER BY created_at DESC, id DESC LIMIT 1)"
)
"""Jointure « dernière analyse du fichier » (identique à `Database.latest_analyses`)."""

_LATEST_INNER = _LATEST.replace("LEFT JOIN", "JOIN", 1)


# --------------------------------------------------------------------- helpers


FIRST_ACCESS = "COALESCE(NULLIF(access_time_first, ''), access_time)"
"""Date d'accès retenue pour l'ancienneté : la première observée (schéma v5), pour
que le hachage/l'extraction de l'audit ne rajeunisse pas les fichiers inchangés."""
FIRST_ACCESS_F = "COALESCE(NULLIF(f.access_time_first, ''), f.access_time)"


def _date_key(column: str) -> str:
    """Expression SQL rendant `yyyyMMdd` (ou `''`) pour une date SMBeagle ou ISO."""
    return (
        f"CASE WHEN length({column})>=10 AND substr({column},3,1)='/' AND substr({column},6,1)='/'"
        f" THEN substr({column},7,4)||substr({column},4,2)||substr({column},1,2)"
        f" WHEN length({column})>=10 AND substr({column},5,1)='-'"
        f" THEN substr({column},1,4)||substr({column},6,2)||substr({column},9,2)"
        f" ELSE '' END"
    )


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


def share_label(base: str, unc_directory: str) -> str:
    """Nom du partage : colonne `base` si présente, sinon `\\\\serveur\\partage`."""
    stripped = base.strip().rstrip("\\/")
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
    rows = db.query(
        "SELECT fast_hash, size_bytes, COUNT(*) AS copies,"
        " size_bytes*(COUNT(*)-1) AS reclaimable"
        " FROM files WHERE fast_hash <> '' GROUP BY fast_hash, size_bytes"
        " HAVING COUNT(*) >= ? ORDER BY reclaimable DESC, copies DESC, fast_hash",
        (min_copies,),
    )
    total_families = len(rows)
    total_copies = sum(int(r["copies"]) for r in rows)
    total_reclaimable = sum(int(r["reclaimable"]) for r in rows)
    kept = rows if limit is None else rows[:limit]
    families: list[DuplicateFamily] = []
    for row in kept:
        members = db.query(
            "SELECT id, path FROM files WHERE fast_hash=? AND size_bytes=? ORDER BY path",
            (row["fast_hash"], row["size_bytes"]),
        )
        families.append(
            DuplicateFamily(
                family_id=f"{row['fast_hash']}-{int(row['size_bytes'])}",
                fast_hash=str(row["fast_hash"]),
                size_bytes=int(row["size_bytes"]),
                copies=int(row["copies"]),
                reclaimable_bytes=int(row["reclaimable"]),
                paths=[str(m["path"]) for m in members],
                file_ids=[int(m["id"]) for m in members],
            )
        )
    return DuplicateReport(families, total_families, total_copies, total_reclaimable)


def stale_files(
    db: Database, *, years: tuple[int, ...] = STALE_YEARS, today: date | None = None
) -> list[StaleBucket]:
    """Pour chaque seuil : fichiers non accédés et non modifiés depuis N années."""
    reference = _today(today)
    access = _date_key(FIRST_ACCESS)
    write = _date_key("last_write_time")
    buckets: list[StaleBucket] = []
    for n in sorted(years):
        cutoff = shift_years(reference, -n)
        key = _key(cutoff)
        row = db.query(
            f"SELECT SUM(CASE WHEN ({access}) <> '' AND ({access}) < ? THEN 1 ELSE 0 END) AS an,"
            f" SUM(CASE WHEN ({access}) <> '' AND ({access}) < ? THEN size_bytes ELSE 0 END) AS ab,"
            f" SUM(CASE WHEN ({write}) <> '' AND ({write}) < ? THEN 1 ELSE 0 END) AS wn,"
            f" SUM(CASE WHEN ({write}) <> '' AND ({write}) < ? THEN size_bytes ELSE 0 END) AS wb"
            " FROM files",
            (key, key, key, key),
        )[0]
        buckets.append(
            StaleBucket(
                years=n,
                cutoff=cutoff,
                not_accessed_files=int(row["an"] or 0),
                not_accessed_bytes=int(row["ab"] or 0),
                not_modified_files=int(row["wn"] or 0),
                not_modified_bytes=int(row["wb"] or 0),
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
    rows = db.query(
        "SELECT base, unc_directory, COUNT(*) AS n, COALESCE(SUM(size_bytes),0) AS b"
        " FROM files GROUP BY base, unc_directory"
    )
    files: dict[str, int] = {}
    volume: dict[str, int] = {}
    for r in rows:
        label = share_label(str(r["base"]), str(r["unc_directory"]))
        files[label] = files.get(label, 0) + int(r["n"])
        volume[label] = volume.get(label, 0) + int(r["b"])
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
    """Répartition par tranche de taille."""
    total_files, total_bytes = _totals(db)
    stats: list[GroupStat] = []
    for label, low, high in SIZE_BUCKETS:
        clause = "size_bytes >= ?" + ("" if high < 0 else " AND size_bytes < ?")
        params: tuple[object, ...] = (low,) if high < 0 else (low, high)
        row = db.query(
            f"SELECT COUNT(*) AS n, COALESCE(SUM(size_bytes),0) AS b FROM files WHERE {clause}",
            params,
        )[0]
        stats.append(
            GroupStat(
                label,
                int(row["n"]),
                int(row["b"]),
                percent(int(row["n"]), total_files),
                percent(int(row["b"]), total_bytes),
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


def _axis_rows(db: Database) -> list[tuple[str, str, str, str, str, str, int, int]]:
    """(base, répertoire, propriétaire, extension, sécurité, RGPD, fichiers, octets)."""
    rows = db.query(
        "SELECT f.base AS base, f.unc_directory AS dir, f.owner AS owner, f.extension AS ext,"
        " COALESCE(a.security_classification,'') AS sec, COALESCE(a.rgpd_risk_level,'') AS rgpd,"
        " COUNT(*) AS n, COALESCE(SUM(f.size_bytes),0) AS b"
        f" FROM files f {_LATEST}"
        " GROUP BY base, dir, owner, ext, sec, rgpd"
    )
    return [
        (
            str(r["base"]),
            str(r["dir"]),
            str(r["owner"]),
            str(r["ext"]),
            str(r["sec"]),
            str(r["rgpd"]),
            int(r["n"]),
            int(r["b"]),
        )
        for r in rows
    ]


def classification_matrix(
    db: Database, *, axis: str = "share", depth: int = 2, limit: int | None = None
) -> list[AxisRow]:
    """Classification par valeur d'axe : `share`, `owner`, `directory` ou `extension`.

    Raises:
        ValueError: axe inconnu.
    """
    if axis not in ("share", "owner", "directory", "extension"):
        raise ValueError(f"axe inconnu : {axis}")
    files: dict[str, int] = {}
    volume: dict[str, int] = {}
    analyzed: dict[str, int] = {}
    security: dict[str, dict[str, int]] = {}
    rgpd: dict[str, dict[str, int]] = {}
    for base, directory, owner, extension, sec, level, count, size in _axis_rows(db):
        if axis == "share":
            label = share_label(base, directory)
        elif axis == "directory":
            label = directory_label(base, directory, depth)
        elif axis == "owner":
            label = owner or "(inconnu)"
        else:
            label = extension or "(sans extension)"
        files[label] = files.get(label, 0) + count
        volume[label] = volume.get(label, 0) + size
        sec_map = security.setdefault(label, dict.fromkeys(SECURITY_CLASSES, 0))
        rgpd_map = rgpd.setdefault(label, dict.fromkeys(RGPD_LEVELS, 0))
        analyzed.setdefault(label, 0)
        if sec:
            analyzed[label] += count
            sec_map[sec] = sec_map.get(sec, 0) + count
            rgpd_map[level] = rgpd_map.get(level, 0) + count
    out = [
        AxisRow(
            label=label,
            files=files[label],
            bytes=volume[label],
            analyzed=analyzed[label],
            security=security[label],
            rgpd=rgpd[label],
        )
        for label in files
    ]
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
        f" FROM files f {_LATEST_INNER} LEFT JOIN reviews r ON r.file_id = f.id"
        " WHERE a.security_classification IN ('C2','C3')"
        " OR a.rgpd_risk_level IN ('high','critical')"
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
    rows = db.query(
        "SELECT f.id AS id, f.path AS path, f.owner AS owner, f.size_bytes AS size,"
        " f.last_write_time AS lwt, a.retention_years AS years, a.retention_basis AS basis,"
        " a.retention_justification AS just"
        f" FROM files f {_LATEST_INNER} WHERE a.retention_required=1 ORDER BY f.path"
    )
    plan: list[RetentionRow] = []
    by_basis_files: dict[str, int] = {}
    by_basis_bytes: dict[str, int] = {}
    total_bytes = 0
    expired = 0
    for r in rows:
        years = int(r["years"] or 0)
        written = parse_smbeagle_datetime(str(r["lwt"]))
        end = shift_years(written.date(), years) if written is not None else None
        is_expired = end is not None and end <= reference
        expired += int(is_expired)
        size = int(r["size"])
        total_bytes += size
        basis = str(r["basis"])
        by_basis_files[basis] = by_basis_files.get(basis, 0) + 1
        by_basis_bytes[basis] = by_basis_bytes.get(basis, 0) + size
        plan.append(
            RetentionRow(
                file_id=int(r["id"]),
                path=str(r["path"]),
                owner=str(r["owner"]),
                size_bytes=size,
                years=years,
                basis=basis,
                justification=str(r["just"]),
                last_write_time=str(r["lwt"]),
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
    access = _date_key(FIRST_ACCESS_F)
    rows = db.query(
        "SELECT f.id AS id, f.path AS path, f.owner AS owner, f.size_bytes AS size,"
        f" {FIRST_ACCESS_F} AS at, a.security_classification AS sec"
        f" FROM files f {_LATEST_INNER}"
        " WHERE a.retention_required=0 AND a.security_classification IN ('C0','C1')"
        f" AND ({access}) <> '' AND ({access}) < ?"
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


def review_progress(db: Database, *, limit: int | None = None) -> ReviewProgress:
    """Avancement des revues et écarts entre classe LLM et classe corrigée."""
    counts = {
        r["status"]: int(r["n"])
        for r in db.query("SELECT status, COUNT(*) AS n FROM reviews GROUP BY status")
    }
    analyzed = int(db.query("SELECT COUNT(DISTINCT file_id) AS n FROM analyses")[0]["n"])
    reviewed = sum(counts.values())
    rows = db.query(
        "SELECT f.id AS id, f.path AS path, a.security_classification AS sec,"
        " a.rgpd_risk_level AS rgpd, COALESCE(r.corrected_security,'') AS csec,"
        " COALESCE(r.corrected_rgpd,'') AS crgpd"
        f" FROM files f {_LATEST_INNER} JOIN reviews r ON r.file_id = f.id"
        " WHERE (r.corrected_security IS NOT NULL AND r.corrected_security <> ''"
        "        AND r.corrected_security <> a.security_classification)"
        "    OR (r.corrected_rgpd IS NOT NULL AND r.corrected_rgpd <> ''"
        "        AND r.corrected_rgpd <> a.rgpd_risk_level)"
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


def overview(db: Database, *, today: date | None = None, stale_years: int = 5) -> Overview:
    """Chiffres clés : volumétrie, hygiène, risque, vérification."""
    reference = _today(today)
    total_files, total_bytes = _totals(db)
    status = status_summary(db)
    dupes = duplicates(db, limit=0)
    stale = stale_files(db, years=(stale_years,), today=reference)[0]
    cleanup = cleanup_candidates(db, years=stale_years, today=reference, limit=0)
    plan = retention_plan(db, today=reference, limit=0)
    reviews = review_progress(db, limit=0)
    sensitive = int(
        db.query(
            f"SELECT COUNT(*) AS n FROM files f {_LATEST_INNER}"
            " WHERE a.security_classification IN ('C2','C3')"
        )[0]["n"]
    )
    rgpd = int(
        db.query(
            f"SELECT COUNT(*) AS n FROM files f {_LATEST_INNER}"
            " WHERE a.rgpd_risk_level IN ('high','critical')"
        )[0]["n"]
    )
    analyzed = int(db.query("SELECT COUNT(DISTINCT file_id) AS n FROM analyses")[0]["n"])
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
        analyzed=analyzed,
        pending=status.counts.get("pending", 0),
        excluded=status.counts.get("excluded", 0),
        errors=status.counts.get("error", 0),
        duplicate_families=dupes.total_families,
        duplicate_reclaimable_bytes=dupes.total_reclaimable_bytes,
        stale_files=stale.not_accessed_files,
        stale_bytes=stale.not_accessed_bytes,
        stale_years=stale_years,
        sensitive_files=sensitive,
        rgpd_at_risk=rgpd,
        retention_files=plan.total_files,
        cleanup_files=cleanup.total_files,
        cleanup_bytes=cleanup.total_bytes,
        reviewed=reviews.reviewed,
    )
