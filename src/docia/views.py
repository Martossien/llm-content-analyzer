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

from collections.abc import Callable, Iterable, Iterator, Sequence
from dataclasses import dataclass, field
from datetime import date
from itertools import chain
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

_FROM_LATEST = (
    " FROM analyses a JOIN files f ON f.id = a.file_id AND a.content_version = f.content_version"
)
"""Clause `FROM` des vues « fichier + dernière analyse », à filtrer par `_IS_LATEST`.

Le parcours part des analyses : `analyses.file_id` référence toujours un fichier
existant (clé étrangère), l'ensemble des lignes est donc celui de
`files f JOIN analyses a ON a.id = (dernière analyse de f)`, mais sans balayer
les fichiers jamais analysés.

**`a.content_version = f.content_version` est dans la jointure, pas dans un `WHERE`
que chaque vue devrait penser à écrire.** Sans elle, un fichier modifié depuis son
analyse gardait sa classification : le rapport combinait la **nouvelle** taille et
l'**ancienne** classe. Mesuré — un fichier passé de 2 à 9 Mo avec un contenu tout
autre, que la base marque pourtant `pending` et `content_version=2`, restait
« candidat au nettoyage » pour 9 Mo. Un rapport qui justifie des suppressions ne
peut pas attribuer une analyse à un contenu sur lequel elle n'a pas été faite.

La règle n'est pas neuve : `db._PENDING_WHERE` l'applique déjà pour décider ce qui
reste **à analyser** — c'est ce qui remet le fichier en file d'attente. Elle était
simplement oubliée du côté **lecture** : la chaîne savait que l'analyse était
périmée, et les rapports s'en servaient quand même."""


def latest_analysis_sql(file_id: str, *, alias: str = "a") -> str:
    """Condition SQL « `alias` est la **dernière analyse** du fichier `file_id` ».

    Définition unique de la règle « dernière analyse » : la plus récente par
    `created_at`, départagée par `id` décroissant quand deux analyses portent le
    même horodatage (réanalyse dans la même seconde, ou horloge à la seconde).

    Toutes les vues de risque en dépendent — classification, top sensible, plan de
    conservation, candidats au nettoyage — et elle existait **en trois
    exemplaires** que rien n'obligeait à rester d'accord : ici, dans
    `docia.db._LATEST_JOINS` (écran Résultats, `latest_analyses`, donc l'export
    CSV/JSON et l'onglet « Fichiers ») et dans
    `docia.report.powerbi._analyses_rows` (`analyses.csv`). Trois copies, trois
    façons possibles de désigner « l'analyse qui fait foi » : le rapport, le
    classeur et l'export Power BI pouvaient montrer trois classifications
    différentes du même fichier. `tests/test_views.py` verrouille les trois, par
    le texte SQL **et** par le comportement.

    `docia.db` ne peut pas l'importer (`views` importe déjà `db`) : sa copie est
    donc comparée à celle-ci par un test, en attendant que la fonction descende
    dans `docia.db` à côté de `first_access_sql`.
    """
    return (
        f"{alias}.id = (SELECT id FROM analyses WHERE file_id = {file_id}"
        " ORDER BY created_at DESC, id DESC LIMIT 1)"
    )


_IS_LATEST = latest_analysis_sql("a.file_id")
"""Ne retient que la dernière analyse d'un fichier (comme `Database.latest_analyses`)."""

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


# ------------------------------------------------------------------ hygiène


MEMBER_BATCH = 200
"""Familles dont les exemplaires sont lus **en une seule requête** (voir `_family_members`).

Une requête par famille, c'était un `N+1` : sur un parc de 934 028 fichiers
groupés en 155 672 familles, `duplicates()` sans limite lançait 155 673
requêtes. Par paquets de 200 familles, il en lance 780.
"""


def _duplicate_groups(db: Database, min_copies: int) -> list[tuple[Any, ...]]:
    """(empreinte, taille, exemplaires, octets récupérables) — plus gros gains d'abord."""
    return db.query_values(
        "SELECT fast_hash, size_bytes, COUNT(*) AS copies,"
        " size_bytes*(COUNT(*)-1) AS reclaimable"
        " FROM files WHERE fast_hash <> '' GROUP BY fast_hash, size_bytes"
        " HAVING COUNT(*) >= ? ORDER BY reclaimable DESC, copies DESC, fast_hash",
        (min_copies,),
    )


def _family_members(
    db: Database, batch: Sequence[tuple[Any, ...]]
) -> dict[tuple[Any, Any], list[tuple[int, str]]]:
    """Exemplaires de tout un paquet de familles, en une requête, triés par chemin."""
    clause = " OR ".join(["(fast_hash=? AND size_bytes=?)"] * len(batch))
    params = tuple(chain.from_iterable((group[0], group[1]) for group in batch))
    members: dict[tuple[Any, Any], list[tuple[int, str]]] = {}
    for file_id, path, fast_hash, size_bytes in db.query_values(
        f"SELECT id, path, fast_hash, size_bytes FROM files WHERE {clause} ORDER BY path", params
    ):
        members.setdefault((fast_hash, size_bytes), []).append((int(file_id), str(path)))
    return members


def _duplicate_families(
    db: Database, groups: Sequence[tuple[Any, ...]]
) -> Iterator[DuplicateFamily]:
    """Familles complètes (exemplaires compris), dans l'ordre de `groups`."""
    for start in range(0, len(groups), MEMBER_BATCH):
        batch = groups[start : start + MEMBER_BATCH]
        members = _family_members(db, batch)
        for fast_hash, size_bytes, copies, reclaimable in batch:
            found = members.get((fast_hash, size_bytes), [])
            yield DuplicateFamily(
                family_id=f"{fast_hash}-{int(size_bytes)}",
                fast_hash=str(fast_hash),
                size_bytes=int(size_bytes),
                copies=int(copies),
                reclaimable_bytes=int(reclaimable),
                paths=[path for _, path in found],
                file_ids=[file_id for file_id, _ in found],
            )


def iter_duplicate_families(db: Database, *, min_copies: int = 2) -> Iterator[DuplicateFamily]:
    """Toutes les familles de doublons, **une par une**, sans les garder en mémoire.

    Pour les sorties écrites au fil de l'eau (`duplicates.csv` de l'export
    Power BI) : seul le résumé d'une famille par ligne (empreinte + trois
    entiers) et le paquet d'exemplaires courant sont en mémoire, jamais la liste
    des chemins de toute la campagne — ce que `duplicates()` doit, lui, construire
    puisqu'il rend un `DuplicateReport` complet.
    """
    yield from _duplicate_families(db, _duplicate_groups(db, min_copies))


DUPLICATE_BASIS = "même empreinte des 64 premiers Ko et même taille"
"""Ce sur quoi une « famille de doublons » est réellement fondée — à citer tel quel.

`fast_hash` ne couvre que les **64 premiers kilo-octets** du fichier
(`quick.HASH_HEAD_BYTES`, comme SMBeagle : hacher entièrement un modèle de 7 Go
bloquerait le scan). Deux fichiers de 200 Ko dont seuls les 64 premiers coïncident
sont donc regroupés — cas courant des formats à en-tête fixe, des images disque et
des exports au même gabarit. Les rendus annonçaient « fichiers **identiques** » sans
le dire : le lecteur croyait à une comparaison octet à octet, sur un tableau qui
chiffre un espace « récupérable » et invite donc à supprimer."""

DUPLICATE_CAUTION = (
    "Regroupement par empreinte partielle : seuls les 64 premiers Ko sont comparés. "
    "Vérifiez octet à octet avant toute suppression."
)
"""Mise en garde affichée sous chaque tableau de doublons, dans les quatre rendus."""


def duplicates(db: Database, *, min_copies: int = 2, limit: int | None = None) -> DuplicateReport:
    """Familles de fichiers de **même empreinte partielle** et espace récupérable.

    « Même empreinte des 64 premiers Ko et même taille » — pas « identiques » : voir
    `DUPLICATE_BASIS`, dont la formulation est reprise telle quelle par les rendus.

    L'espace récupérable d'une famille vaut `taille × (exemplaires − 1)` : un
    exemplaire est conservé. Les fichiers sans empreinte sont ignorés.

    Les totaux portent sur **toutes** les familles ; `limit` ne borne que les
    familles détaillées. Les exemplaires sont lus par paquets de `MEMBER_BATCH`
    familles (voir `iter_duplicate_families` pour la variante en flux).
    """
    groups = _duplicate_groups(db, min_copies)
    total_families = len(groups)
    total_copies = sum(int(g[2]) for g in groups)
    total_reclaimable = sum(int(g[3]) for g in groups)
    kept = groups if limit is None else groups[:limit]
    families = list(_duplicate_families(db, kept))
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


def empty_or_tiny(db: Database, *, max_bytes: int = 100) -> TinyReport:
    """Fichiers vides ou d'au plus `max_bytes` octets (compteurs seuls)."""
    row = db.query(
        "SELECT COUNT(*) AS n, COALESCE(SUM(size_bytes),0) AS b,"
        " SUM(CASE WHEN size_bytes=0 THEN 1 ELSE 0 END) AS z FROM files WHERE size_bytes <= ?",
        (max_bytes,),
    )[0]
    return TinyReport(
        max_bytes=max_bytes,
        files=int(row["n"] or 0),
        bytes=int(row["b"] or 0),
        empty_files=int(row["z"] or 0),
    )


def status_summary(db: Database, *, reason_limit: int | None = REASON_TOP) -> StatusSummary:
    """Compteurs par statut et principales raisons d'exclusion ou d'erreur.

    `reason_limit` borne la liste (`None` : toutes). La borne est **légitime** —
    les raisons d'erreur sont du texte libre tronqué à 500 caractères
    (`pipeline` → `Database.set_file_status`), donc de cardinalité non bornée par
    la configuration — mais elle ne doit pas être muette : `reasons_total` dit
    combien il y en a réellement, et `reasons_hidden` combien manquent. Elle
    valait 10, en dur dans le SQL, sans que rien ne le signale : un rapport qui
    justifie des suppressions taisait ainsi des motifs de **non**-analyse.
    """
    total_files, total_bytes = _totals(db)
    counts: dict[str, int] = {}
    volume: dict[str, int] = {}
    for r in db.query(
        "SELECT status, COUNT(*) AS n, COALESCE(SUM(size_bytes),0) AS b FROM files GROUP BY status"
    ):
        counts[str(r["status"])] = int(r["n"])
        volume[str(r["status"])] = int(r["b"])
    where = " FROM files WHERE exclusion_reason IS NOT NULL AND exclusion_reason <> ''"
    reasons_total = int(
        db.query_values(f"SELECT COUNT(DISTINCT exclusion_reason){where}")[0][0]  # noqa: S608
    )
    reasons = [
        GroupStat(
            str(r["reason"]),
            int(r["n"]),
            int(r["b"]),
            percent(int(r["n"]), total_files),
            percent(int(r["b"]), total_bytes),
        )
        for r in db.query(
            "SELECT exclusion_reason AS reason, COUNT(*) AS n,"  # noqa: S608 — clause interne
            f" COALESCE(SUM(size_bytes),0) AS b{where}"
            " GROUP BY reason ORDER BY n DESC, reason LIMIT ?",
            (-1 if reason_limit is None else reason_limit,),
        )
    ]
    return StatusSummary(counts, volume, total_files, total_bytes, reasons, reasons_total)


# ------------------------------------------------------------------ risque


AXES: tuple[str, ...] = ("share", "owner", "directory", "extension")
"""Axes acceptés par `classification_matrix`."""

_SIMPLE_AXES: dict[str, tuple[str, str]] = {
    "owner": ("f.owner", "(inconnu)"),
    "extension": ("f.extension", "(sans extension)"),
}
"""Axes dont l'étiquette est la colonne SQL elle-même : (colonne, libellé si vide)."""


_BASE_UNNAMED = "TRIM(TRIM(f.base), '\\/') = ''"
"""`base` ne nomme pas son partage : vide, blancs ou séparateurs seuls.

Transcription SQL de `share_from_base(base) == ''` : `TRIM` retire les blancs,
le second retire les séparateurs. Rogner les deux bouts au lieu de la seule fin
ne change pas le *test de vacuité* — seule une valeur réduite à des blancs et à
des séparateurs est vide dans les deux cas."""


def _all_shares_named(db: Database) -> bool:
    """Vrai si toute valeur de `base` nomme déjà son partage.

    Dans ce cas — celui de tout scan SMBeagle — `share_label` ne regarde jamais
    `unc_directory` : le regroupement SQL peut l'ignorer et rendre un groupe par
    partage au lieu d'un groupe par répertoire. Test d'existence borné, et non
    `SELECT DISTINCT base` : la réponse tient au premier enregistrement fautif.
    """
    return not db.query_values(f"SELECT 1 FROM files f WHERE {_BASE_UNNAMED} LIMIT 1")


_FILLER = "''"
"""Clé d'axe inutilisée : garde la largeur des lignes sans peser sur le regroupement."""

_SHARE_FALLBACK = f"CASE WHEN {_BASE_UNNAMED} THEN f.unc_directory ELSE '' END"
"""Second niveau de regroupement de l'axe « partage », **seulement** quand `base` est vide.

Un unique enregistrement à `base` vide suffisait à faire retomber tout l'axe sur
`f.unc_directory` : le regroupement passait d'un groupe par partage à un groupe
par répertoire — 6 groupes contre 521 718 sur un parc de 934 028 fichiers, soit
19 Mo de mémoire contre 462 (× 24). Le repli est ici **borné aux seules lignes
concernées** : les fichiers dont la `base` nomme le partage restent regroupés
par partage, quoi qu'il arrive ailleurs dans la campagne."""


def _axis_group(db: Database, axis: str) -> list[str]:
    """Expressions SQL identifiant un groupe pour cet axe, dans l'ordre des colonnes."""
    if axis in _SIMPLE_AXES:
        return [_SIMPLE_AXES[axis][0]]
    if axis == "share":
        return ["f.base", _FILLER if _all_shares_named(db) else _SHARE_FALLBACK]
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
    """Préfixe de `text` couvrant ses `segments` premiers niveaux **non vides**
    (`''` s'il en manque).

    Les niveaux sont comptés comme `share_label` et `directory_label` les comptent :
    les séparateurs vides ne comptent pas (`\\\\srv\\part` a deux niveaux, pas trois).
    Compter les antislashs bruts décalait le compte dès qu'un chemin portait un
    séparateur doublé, et le préfixe couvrait alors moins de niveaux que l'étiquette
    n'en consomme : deux partages distincts se retrouvaient sous la même étiquette.
    """
    seen = 0
    position = 0
    length = len(text)
    while position < length:
        if text[position] == "\\":
            position += 1
            continue
        end = text.find("\\", position)
        if end == -1:
            return ""
        seen += 1
        if seen == segments:
            return text[: end + 1]
        position = end + 1
    return ""


def _path_levels(text: str) -> int:
    """Nombre de niveaux non vides d'un chemin (`\\\\srv\\part` → 2, `\\\\srv\\part\\a` → 3).

    Compté comme `_run_prefix` compte : les séparateurs vides ne font pas un niveau."""
    return sum(1 for part in text.replace("/", "\\").split("\\") if part)


def _axis_labeller(axis: str, depth: int) -> Callable[[tuple[Any, ...]], str]:
    """Rend la fonction qui étiquette une ligne d'axe (clés d'axe en tête de ligne).

    `share_label` ne lit que les deux premiers niveaux d'un répertoire, et
    `directory_label` `depth` de plus **à partir du partage** : deux répertoires
    qui partagent ces niveaux portent la même étiquette. Les lignes arrivant
    groupées par répertoire, l'étiquette n'est donc recalculée qu'au changement
    d'arborescence — sur un parc où presque chaque fichier a son répertoire,
    c'est l'essentiel du coût de la vue.

    Le nombre de niveaux couverts par le préfixe est tiré de la **profondeur
    réelle du partage**, celle de la colonne `base`. Le supposer égal à deux
    (`\\\\serveur\\partage`) était juste pour un scan SMB, faux dès qu'une campagne
    porte sur un sous-arbre : avec `base = \\\\srv\\part\\Direction\\RH`, le préfixe
    couvrait deux niveaux de moins que l'étiquette n'en consomme, et
    `\\\\srv\\part\\Direction\\RH\\Paie` réutilisait l'étiquette de
    `\\\\srv\\part\\Direction\\RH\\Contrats` : deux répertoires distincts additionnés
    sur une même ligne du tableau « Répertoires », le second disparaissant.
    Un préfixe trop long ne fait, lui, que recalculer inutilement.
    """
    if axis in _SIMPLE_AXES:
        empty = _SIMPLE_AXES[axis][1]

        def by_column(row: tuple[Any, ...]) -> str:
            return str(row[0] or empty)

        return by_column

    previous_base: Any = None
    prefix = ""
    label = ""

    def by_path(row: tuple[Any, ...]) -> str:
        nonlocal previous_base, prefix, label
        base, directory = row[0], row[1]  # colonnes TEXT NOT NULL : toujours des chaînes
        # Le préfixe est tiré et comparé sur le texte normalisé, celui-là même dont
        # les étiquettes sont tirées : sinon `/` et `\` ne se comparent pas entre eux,
        # et un chemin à slashs ne se compte pas dans les mêmes niveaux.
        text = directory.replace("/", "\\").strip()
        if base != previous_base or not prefix or not text.startswith(prefix):
            share = share_label(base, directory)
            if axis == "share":
                label = share
                segments = 2  # `share_label` ne lit jamais plus de deux niveaux
            else:
                label = directory_label(base, directory, depth)
                segments = _path_levels(share) + depth
            prefix = _run_prefix(text, segments)
            previous_base = base
        return label

    return by_path


RiskTally = tuple[dict[str, int], dict[str, int], list[int]]
"""Compteurs d'une étiquette d'axe : (sécurité, RGPD, [analysés])."""


def _new_tally() -> RiskTally:
    return (dict.fromkeys(SECURITY_CLASSES, 0), dict.fromkeys(RGPD_LEVELS, 0), [0])


def _fold_risk(
    rows: Iterable[tuple[Any, ...]], width: int, label_of: Callable[[tuple[Any, ...]], str]
) -> dict[str, RiskTally]:
    """Répartition sécurité/RGPD par étiquette, **cumulée au fil des lignes**.

    Ce que retient ce repli ne dépend que du nombre d'**étiquettes**, jamais du
    nombre de lignes lues : chaque ligne est ajoutée aux compteurs de son
    étiquette puis oubliée. La version précédente empilait un tuple Python par
    ligne source dans une liste par étiquette, pour les cumuler ensuite : sur une
    campagne de 934 028 fichiers tous analysés, réduits à 5 862 étiquettes,
    c'étaient 934 028 tuples retenus — 444 Mo — pour rendre exactement le même
    résultat. `tests/test_views.py` mesure que la taille du repli ne bouge pas
    quand le nombre de lignes est multiplié par cinquante mille.
    """
    tallies: dict[str, RiskTally] = {}
    for row in rows:
        classification = row[width]
        if not classification:
            continue
        label = label_of(row)
        entry = tallies.get(label)
        if entry is None:
            entry = _new_tally()
            tallies[label] = entry
        security, rgpd, analyzed = entry
        number = int(row[width + 2])
        security[classification] = security.get(classification, 0) + number
        rgpd[row[width + 1]] = rgpd.get(row[width + 1], 0) + number
        analyzed[0] += number
    return tallies


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
    risk = _fold_risk(_axis_risk(db, keys), width, _axis_labeller(axis, depth))
    label_of = _axis_labeller(axis, depth)
    totals: dict[str, list[int]] = {}
    for row in _axis_volumes(db, keys):
        label = label_of(row)
        entry_totals = totals.get(label)
        if entry_totals is None:
            totals[label] = [row[width], row[width + 1]]
        else:
            entry_totals[0] += row[width]
            entry_totals[1] += row[width + 1]
    out: list[AxisRow] = []
    for label, (count, size) in totals.items():
        security, rgpd, analyzed = risk.get(label) or _new_tally()
        out.append(
            AxisRow(
                label=label,
                files=count,
                bytes=size,
                analyzed=analyzed[0],
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


def count_sensitive(db: Database) -> int:
    """Nombre de fichiers que `top_sensitive` classerait, **sans la borne**.

    Ce que le « top 50 » ne montre pas doit pouvoir être annoncé : un simple
    `COUNT`, pas la liste.
    """
    return _count_latest(db, f"({_SENSITIVE} OR {_RGPD_AT_RISK})")


def top_sensitive(db: Database, *, limit: int | None = 50) -> list[SensitiveFile]:
    """Les `limit` fichiers les plus sensibles : C2/C3, ou RGPD `high`/`critical`.

    C'est un **classement borné**, pas la liste exhaustive : `count_sensitive`
    donne le total, l'onglet « Fichiers » du classeur et `analyses.csv` de
    l'export Power BI donnent la totalité des analyses. `limit=None` lève la
    borne (`LIMIT -1` : la convention SQLite pour « pas de limite »).
    """
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
        (-1 if limit is None else limit,),
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
    """Fichiers à conserver, avec la date de fin = dernière écriture + `years`.

    **Une durée de zéro année n'est pas une échéance immédiate.** Le schéma LLM
    accepte `retention.years = 0` (`llm/schema.py`, `minimum: 0`) et
    `llm/parse.py` la laisse passer : « à conserver, pendant 0 an » est une
    réponse *incohérente* du modèle, pas une durée. Calculée, elle donnait une fin
    de conservation égale à la date d'écriture, donc « échu : oui » pour tout
    fichier écrit avant aujourd'hui — 155 218 fichiers déclarés échus à tort sur
    une base réelle de 280 208. Ces lignes sont désormais **sans date de fin et
    jamais échues** (`RetentionRow.undetermined`), et comptées à part dans
    `undetermined_files` : c'est une question posée à un agent, pas un feu vert à
    la suppression.
    """
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
    undetermined = 0
    for file_id, path, owner, size, written_at, retained, basis, justification in rows:
        years = int(retained or 0)
        written = parse_smbeagle_datetime(str(written_at))
        if years <= 0:
            undetermined += 1
            end = None
        else:
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
        undetermined_files=undetermined,
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
    """Nombre de fichiers ayant au moins une analyse.

    Délégué à `Database.count_analyzed_files` : c'est le même chiffre que
    `counts()["analyses"]` de la fenêtre et de `docia status`, et il n'existe
    donc plus qu'une seule façon de le calculer.
    """
    return db.count_analyzed_files()


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
        total_discrepancies=len(gaps),
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
