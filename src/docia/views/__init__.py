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

Paquet (3.1) découpé de l'ancien `views.py` (1 400 lignes) : `_common` (socle),
`axes`, `hygiene`, `risk`, `retention`, `review`, `overview`. Cette façade
réexporte tout — `from docia import views ; views.overview(db)` ne change pas.
"""

from __future__ import annotations

from docia.db import Database, first_access_sql, latest_analysis_sql
from docia.views._common import (
    _CLEANUP_WHERE as _CLEANUP_WHERE,
)
from docia.views._common import (
    _FROM_LATEST as _FROM_LATEST,
)
from docia.views._common import (
    _IS_LATEST as _IS_LATEST,
)
from docia.views._common import (
    _RGPD_AT_RISK as _RGPD_AT_RISK,
)
from docia.views._common import (
    _SENSITIVE as _SENSITIVE,
)
from docia.views._common import (
    FIRST_ACCESS_F,
    REASON_TOP,
    RETENTION_BASIS_LABELS,
    RETENTION_UNDETERMINED,
    RGPD_LEVELS,
    SECURITY_CLASSES,
    SIZE_BUCKETS,
    STALE_YEARS,
    THOUSANDS_SEPARATOR,
    AxisRow,
    CleanupReport,
    CleanupRow,
    Discrepancy,
    DuplicateFamily,
    DuplicateReport,
    GroupStat,
    Overview,
    RetentionPlan,
    RetentionRow,
    ReviewProgress,
    RunStat,
    SensitiveFile,
    StaleBucket,
    StatusSummary,
    TinyReport,
    directory_label,
    format_bytes,
    format_int,
    percent,
    share_from_base,
    share_label,
    shift_years,
)
from docia.views._common import (
    _count_latest as _count_latest,
)
from docia.views._common import (
    _key as _key,
)
from docia.views._common import (
    _today as _today,
)
from docia.views.axes import (
    _BASE_UNNAMED as _BASE_UNNAMED,
)
from docia.views.axes import (
    _FILLER as _FILLER,
)
from docia.views.axes import (
    _SHARE_FALLBACK as _SHARE_FALLBACK,
)
from docia.views.axes import (
    _SIMPLE_AXES as _SIMPLE_AXES,
)
from docia.views.axes import (
    AXES,
    RiskTally,
)
from docia.views.axes import (
    _all_shares_named as _all_shares_named,
)
from docia.views.axes import (
    _axis_group as _axis_group,
)
from docia.views.axes import (
    _axis_labeller as _axis_labeller,
)
from docia.views.axes import (
    _axis_risk as _axis_risk,
)
from docia.views.axes import (
    _axis_volumes as _axis_volumes,
)
from docia.views.axes import (
    _group_by as _group_by,
)
from docia.views.axes import (
    _path_levels as _path_levels,
)
from docia.views.axes import (
    _run_prefix as _run_prefix,
)
from docia.views.hygiene import (
    DUPLICATE_BASIS,
    DUPLICATE_CAUTION,
    MEMBER_BATCH,
    by_extension,
    by_owner,
    by_share,
    duplicates,
    empty_or_tiny,
    iter_duplicate_families,
    size_buckets,
    stale_files,
    status_summary,
)
from docia.views.hygiene import (
    _below as _below,
)
from docia.views.hygiene import (
    _duplicate_families as _duplicate_families,
)
from docia.views.hygiene import (
    _duplicate_groups as _duplicate_groups,
)
from docia.views.hygiene import (
    _family_members as _family_members,
)
from docia.views.hygiene import (
    _grouped as _grouped,
)
from docia.views.hygiene import (
    _totals as _totals,
)
from docia.views.overview import (
    overview,
)
from docia.views.retention import (
    cleanup_candidates,
    retention_plan,
)
from docia.views.review import (
    _analyzed_files as _analyzed_files,
)
from docia.views.review import (
    _review_counts as _review_counts,
)
from docia.views.review import (
    review_progress,
    runs_summary,
)
from docia.views.risk import (
    _fold_risk as _fold_risk,
)
from docia.views.risk import (
    _new_tally as _new_tally,
)
from docia.views.risk import (
    by_directory,
    classification_matrix,
    count_sensitive,
    top_sensitive,
)

__all__ = [
    "AXES",
    "AxisRow",
    "CleanupReport",
    "CleanupRow",
    "DUPLICATE_BASIS",
    "DUPLICATE_CAUTION",
    "Database",
    "Discrepancy",
    "DuplicateFamily",
    "DuplicateReport",
    "FIRST_ACCESS_F",
    "GroupStat",
    "MEMBER_BATCH",
    "Overview",
    "REASON_TOP",
    "RETENTION_BASIS_LABELS",
    "RETENTION_UNDETERMINED",
    "RGPD_LEVELS",
    "RetentionPlan",
    "RetentionRow",
    "ReviewProgress",
    "RiskTally",
    "RunStat",
    "SECURITY_CLASSES",
    "SIZE_BUCKETS",
    "STALE_YEARS",
    "SensitiveFile",
    "StaleBucket",
    "StatusSummary",
    "THOUSANDS_SEPARATOR",
    "TinyReport",
    "by_directory",
    "by_extension",
    "by_owner",
    "by_share",
    "classification_matrix",
    "cleanup_candidates",
    "count_sensitive",
    "directory_label",
    "duplicates",
    "empty_or_tiny",
    "first_access_sql",
    "format_bytes",
    "format_int",
    "iter_duplicate_families",
    "latest_analysis_sql",
    "overview",
    "percent",
    "retention_plan",
    "review_progress",
    "runs_summary",
    "share_from_base",
    "share_label",
    "shift_years",
    "size_buckets",
    "stale_files",
    "status_summary",
    "top_sensitive",
]
