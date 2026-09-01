"""Couche service : toute opération métier en fonctions typées, sans Tk ni argparse.

La CLI et la GUI sont des clients minces de ce paquet ; l'API REST prévue en v4
(`docia serve`) exposera ces fonctions 1 : 1. Rien ici n'imprime : les
opérations journalisent (`logging`) et lèvent `ServiceError` avec un message en
français lisible par un utilisateur non technique — jamais de trace brute.

Paquet (3.1) découpé de l'ancien `service.py` (1 000 lignes) : `_common` (socle),
`campaigns`, `backups`, `ingest`, `runs`. Cette façade réexporte tout —
`service.run_campaign(db, cfg)` ne change pas.
"""

from __future__ import annotations

from docia.db import backup_dir_for
from docia.filter import PlanProgress, PlanReport
from docia.ingest.smbeagle_csv import ImportProgress, ImportReport
from docia.pipeline import RunReport
from docia.scan import ScanError, ScanEvent, ScanProfile, ScanResult
from docia.service._common import (
    BACKUP_SUFFIX,
    DEFAULT_KEEP_BACKUPS,
    HOME_ENV,
    MAX_RECENT,
    REANALYZE_SCOPES,
    RECENT_FILE,
    SAFETY_LABEL_PREFIX,
    WHERE_KEYS,
    CampaignStatus,
    RecentCampaign,
    RunEvent,
    ServiceError,
    docia_home,
    logger,
)
from docia.service._common import (
    _effective_keys as _effective_keys,
)
from docia.service._common import (
    _now_iso as _now_iso,
)
from docia.service._common import (
    _slug as _slug,
)
from docia.service._common import (
    _stamp as _stamp,
)
from docia.service.backups import (
    _CURRENT_TAIL as _CURRENT_TAIL,
)
from docia.service.backups import (
    _MIGRATION_TAIL as _MIGRATION_TAIL,
)
from docia.service.backups import (
    _backup_tail as _backup_tail,
)
from docia.service.backups import (
    _backups_in as _backups_in,
)
from docia.service.backups import (
    _is_safety_copy as _is_safety_copy,
)
from docia.service.backups import (
    _rotatable_in as _rotatable_in,
)
from docia.service.backups import (
    _rotate as _rotate,
)
from docia.service.backups import (
    _unique_backup_path as _unique_backup_path,
)
from docia.service.backups import (
    backup_database,
    list_backups,
    restore_database,
)
from docia.service.campaigns import (
    _read_recent as _read_recent,
)
from docia.service.campaigns import (
    _recent_path as _recent_path,
)
from docia.service.campaigns import (
    _same_db as _same_db,
)
from docia.service.campaigns import (
    _write_recent as _write_recent,
)
from docia.service.campaigns import (
    campaign_status,
    forget_campaign,
    recent_campaigns,
    remember_campaign,
)
from docia.service.ingest import (
    format_import_progress,
    format_import_report,
    import_progress_logger,
    import_scan,
    plan,
    scan_campaign,
    scans_dir_for,
)
from docia.service.runs import (
    _EVENT_KINDS as _EVENT_KINDS,
)
from docia.service.runs import (
    _as_int as _as_int,
)
from docia.service.runs import (
    _Pace as _Pace,
)
from docia.service.runs import (
    _per_hour as _per_hour,
)
from docia.service.runs import (
    _targets as _targets,
)
from docia.service.runs import (
    _where_clauses as _where_clauses,
)
from docia.service.runs import (
    reanalyze,
    run_campaign,
    set_review,
)

__all__ = [
    "ImportProgress",
    "ImportReport",
    "PlanProgress",
    "PlanReport",
    "RunReport",
    "ScanError",
    "BACKUP_SUFFIX",
    "CampaignStatus",
    "DEFAULT_KEEP_BACKUPS",
    "HOME_ENV",
    "MAX_RECENT",
    "REANALYZE_SCOPES",
    "RECENT_FILE",
    "RecentCampaign",
    "RunEvent",
    "SAFETY_LABEL_PREFIX",
    "ScanEvent",
    "ScanProfile",
    "ScanResult",
    "ServiceError",
    "WHERE_KEYS",
    "backup_database",
    "backup_dir_for",
    "campaign_status",
    "docia_home",
    "forget_campaign",
    "format_import_progress",
    "format_import_report",
    "import_progress_logger",
    "import_scan",
    "list_backups",
    "logger",
    "plan",
    "reanalyze",
    "recent_campaigns",
    "remember_campaign",
    "restore_database",
    "run_campaign",
    "scan_campaign",
    "scans_dir_for",
    "set_review",
]
