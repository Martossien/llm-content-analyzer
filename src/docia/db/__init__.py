"""Base SQLite de la campagne — paquet.

Découpage (3.1) de l'ancien `db.py` monolithique (2 450 lignes) :

- `docia.db.database` : la classe `Database` (connexion, migrations, chargement en
  masse, requêtes) et ce qui décide de l'ouverture d'une base (`campaign_kind`) ;
- `docia.db.schema`   : `SCHEMA_VERSION`, les scripts de migration, les index attendus ;
- `docia.db.sql`      : fragments SQL partagés et traductions SQL ↔ Python.

`from docia.db import Database, …` reste le point d'entrée : tous les noms publics
de l'ancien module — et les quelques noms privés que `views` et les tests
confrontent (`_IS_LATEST`, `_LATEST_JOINS`, `_PENDING_WHERE`, `_SCHEMA_V1`) — sont
réexportés ici.
"""

from __future__ import annotations

from docia.db.database import (
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
    Database,
    MigrationBackupError,
    backup_dir_for,
    campaign_kind,
)
from docia.db.schema import _MIGRATIONS, _SCHEMA_V1, FILES_INDEXES, SCHEMA_VERSION
from docia.db.sql import (
    _IS_LATEST,
    _LATEST_JOINS,
    _PENDING_WHERE,
    date_key,
    date_key_sql,
    first_access_sql,
    latest_analysis_sql,
    normalize_index_sql,
    split_sql_statements,
)

__all__ = [
    "APPLY_PLAN_BATCH",
    "BACKUP_DIR_SUFFIX",
    "BULK_CACHE_PAGES",
    "BULK_LOCK_KEY",
    "BULK_LOCK_TTL_S",
    "CAMPAIGN_DOCIA",
    "CAMPAIGN_FOREIGN",
    "CAMPAIGN_NEW",
    "FILES_INDEXES",
    "ITER_FILES_BATCH",
    "REVIEW_STATUSES",
    "SCHEMA_VERSION",
    "Database",
    "MigrationBackupError",
    "_IS_LATEST",
    "_LATEST_JOINS",
    "_MIGRATIONS",
    "_PENDING_WHERE",
    "_SCHEMA_V1",
    "backup_dir_for",
    "campaign_kind",
    "date_key",
    "date_key_sql",
    "first_access_sql",
    "latest_analysis_sql",
    "normalize_index_sql",
    "split_sql_statements",
]
