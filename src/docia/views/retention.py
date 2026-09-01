"""Plan de conservation et candidats au nettoyage."""

from __future__ import annotations

from datetime import date

from docia.db import Database
from docia.ingest.smbeagle_csv import parse_smbeagle_datetime
from docia.views._common import (
    _CLEANUP_WHERE,
    _FROM_LATEST,
    _IS_LATEST,
    FIRST_ACCESS_F,
    RETENTION_BASIS_LABELS,
    CleanupReport,
    CleanupRow,
    GroupStat,
    RetentionPlan,
    RetentionRow,
    _key,
    _today,
    percent,
    shift_years,
)


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
