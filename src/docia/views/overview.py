"""Synthèse (`Overview`) : les chiffres de tête du rapport et de l'accueil."""

from __future__ import annotations

from datetime import date

from docia.db import Database
from docia.views._common import (
    _CLEANUP_WHERE,
    _FROM_LATEST,
    _IS_LATEST,
    _RGPD_AT_RISK,
    _SENSITIVE,
    Overview,
    _count_latest,
    _key,
    _today,
    shift_years,
)
from docia.views.hygiene import _totals
from docia.views.review import _analyzed_files, _review_counts


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
