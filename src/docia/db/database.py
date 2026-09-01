"""Base SQLite : accès aux fichiers, blocs, analyses, revues et prompts.

Une seule connexion par `Database` (mode WAL, `check_same_thread=False` car
le pipeline est asynchrone mais mono-thread). Toutes les écritures passent par
des méthodes explicites ; aucun `ALTER` implicite hors `docia.db.schema`.

`Database` assemble le socle (`docia.db.core`) et une opération par table : chaque
mixin vit dans son module, l'API reste `db.upsert_files(...)`, `db.store_analysis(...)`.
"""

from __future__ import annotations

from docia.db.analyses import AnalysesOps
from docia.db.blocks import BlocksOps
from docia.db.core import (
    APPLY_PLAN_BATCH,
    BACKUP_DIR_SUFFIX,
    BULK_CACHE_PAGES,
    BULK_LOCK_KEY,
    BULK_LOCK_TTL_S,
    CAMPAIGN_DOCIA,
    CAMPAIGN_FOREIGN,
    CAMPAIGN_NEW,
    ITER_FILES_BATCH,
    REVIEW_STATUSES,
    MigrationBackupError,
    backup_dir_for,
    campaign_kind,
)
from docia.db.files import FilesOps
from docia.db.prompts import PromptsOps
from docia.db.stats import StatsOps

__all__ = [
    "APPLY_PLAN_BATCH",
    "BACKUP_DIR_SUFFIX",
    "BULK_CACHE_PAGES",
    "BULK_LOCK_KEY",
    "BULK_LOCK_TTL_S",
    "CAMPAIGN_DOCIA",
    "CAMPAIGN_FOREIGN",
    "CAMPAIGN_NEW",
    "Database",
    "ITER_FILES_BATCH",
    "MigrationBackupError",
    "REVIEW_STATUSES",
    "backup_dir_for",
    "campaign_kind",
]


class Database(FilesOps, BlocksOps, AnalysesOps, PromptsOps, StatsOps):
    """Accès à la base. Utiliser comme gestionnaire de contexte ou appeler `close()`."""
