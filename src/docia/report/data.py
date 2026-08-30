"""Collecte des vues nécessaires à un rapport, en une passe.

`collect()` est appelé une fois ; HTML, Markdown et Excel consomment le même
`ReportData` — les trois rendus ne peuvent donc pas diverger.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

from docia import views
from docia.db import Database

TOP_ROWS = 50
"""Nombre de lignes détaillées gardées dans les tableaux « top »."""


@dataclass(frozen=True)
class ReportData:
    """Toutes les vues d'un rapport, déjà triées et bornées."""

    overview: views.Overview
    status: views.StatusSummary
    duplicates: views.DuplicateReport
    stale: list[views.StaleBucket]
    extensions: list[views.GroupStat]
    owners: list[views.GroupStat]
    shares: list[views.GroupStat]
    directories: list[views.AxisRow]
    sizes: list[views.GroupStat]
    tiny: views.TinyReport
    by_share: list[views.AxisRow]
    by_owner: list[views.AxisRow]
    sensitive: list[views.SensitiveFile]
    retention: views.RetentionPlan
    cleanup: views.CleanupReport
    reviews: views.ReviewProgress
    runs: list[views.RunStat] = field(default_factory=list)


def collect(
    db: Database, *, today: date | None = None, top: int = TOP_ROWS, cleanup_years: int = 5
) -> ReportData:
    """Exécute toutes les vues du rapport pour la base `db`."""
    return ReportData(
        overview=views.overview(db, today=today, stale_years=cleanup_years),
        status=views.status_summary(db),
        duplicates=views.duplicates(db, limit=top),
        stale=views.stale_files(db, today=today),
        extensions=views.by_extension(db, limit=top // 2),
        owners=views.by_owner(db, limit=top // 2),
        shares=views.by_share(db),
        directories=views.by_directory(db, depth=2, limit=top // 2),
        sizes=views.size_buckets(db),
        tiny=views.empty_or_tiny(db),
        by_share=views.classification_matrix(db, axis="share"),
        by_owner=views.classification_matrix(db, axis="owner", limit=top // 2),
        sensitive=views.top_sensitive(db, limit=top),
        retention=views.retention_plan(db, today=today, limit=top),
        cleanup=views.cleanup_candidates(db, years=cleanup_years, today=today, limit=top),
        reviews=views.review_progress(db, limit=top),
        runs=views.runs_summary(db),
    )
