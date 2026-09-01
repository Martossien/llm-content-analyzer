"""Hygiène du stockage : doublons, ancienneté, tailles, extensions, propriétaires, partages, statuts."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from datetime import date
from itertools import chain
from typing import Any

from docia.db import Database
from docia.views._common import (
    REASON_TOP,
    SIZE_BUCKETS,
    STALE_YEARS,
    DuplicateFamily,
    DuplicateReport,
    GroupStat,
    StaleBucket,
    StatusSummary,
    TinyReport,
    _key,
    _today,
    percent,
    shift_years,
)
from docia.views.axes import _axis_group, _axis_labeller, _axis_volumes

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
