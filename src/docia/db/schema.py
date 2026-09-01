"""Schéma SQLite versionné de la campagne : une chaîne SQL par version.

`SCHEMA_VERSION` est la version courante ; `_MIGRATIONS` liste, dans l'ordre, le
script qui amène une base de la version précédente à chaque version. Une base est
migrée **une version à la fois, une transaction par version**
(`Database._migrate`). Ajouter une version = ajouter un `_SCHEMA_Vn` ici et
l'inscrire dans `_MIGRATIONS` — jamais d'`ALTER` ailleurs.
"""

from __future__ import annotations

from docia.db.sql import date_key_sql, first_access_sql

SCHEMA_VERSION = 7


_SCHEMA_V1 = """
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);

CREATE TABLE IF NOT EXISTS scans (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    csv_path TEXT NOT NULL,
    imported_at TEXT NOT NULL,
    rows_total INTEGER NOT NULL DEFAULT 0,
    rows_new INTEGER NOT NULL DEFAULT 0,
    rows_updated INTEGER NOT NULL DEFAULT 0,
    rows_unchanged INTEGER NOT NULL DEFAULT 0,
    rows_invalid INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    path_key TEXT NOT NULL UNIQUE,
    path TEXT NOT NULL,
    name TEXT NOT NULL,
    extension TEXT NOT NULL DEFAULT '',
    host TEXT NOT NULL DEFAULT '',
    hostname TEXT NOT NULL DEFAULT '',
    username TEXT NOT NULL DEFAULT '',
    unc_directory TEXT NOT NULL DEFAULT '',
    base TEXT NOT NULL DEFAULT '',
    directory_type TEXT NOT NULL DEFAULT '',
    size_bytes INTEGER NOT NULL DEFAULT 0,
    creation_time TEXT NOT NULL DEFAULT '',
    last_write_time TEXT NOT NULL DEFAULT '',
    access_time TEXT NOT NULL DEFAULT '',
    file_attributes TEXT NOT NULL DEFAULT '',
    owner TEXT NOT NULL DEFAULT '',
    fast_hash TEXT NOT NULL DEFAULT '',
    file_signature TEXT NOT NULL DEFAULT '',
    readable INTEGER NOT NULL DEFAULT 1,
    writeable INTEGER NOT NULL DEFAULT 0,
    deletable INTEGER NOT NULL DEFAULT 0,
    first_seen_scan_id INTEGER NOT NULL,
    last_seen_scan_id INTEGER NOT NULL,
    content_version INTEGER NOT NULL DEFAULT 1,
    status TEXT NOT NULL DEFAULT 'pending',
    exclusion_reason TEXT,
    priority_score INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_files_status ON files(status);
CREATE INDEX IF NOT EXISTS idx_files_fast_hash ON files(fast_hash);
CREATE INDEX IF NOT EXISTS idx_files_extension ON files(extension);
CREATE INDEX IF NOT EXISTS idx_files_priority ON files(priority_score DESC, path);

CREATE TABLE IF NOT EXISTS runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT NOT NULL DEFAULT 'running',
    model TEXT NOT NULL,
    prompt_hash TEXT NOT NULL,
    config_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS blocks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL REFERENCES runs(id),
    path TEXT NOT NULL,
    tokens_estimated INTEGER NOT NULL DEFAULT 0,
    tokens_with_margin INTEGER NOT NULL DEFAULT 0,
    file_count INTEGER NOT NULL DEFAULT 0,
    oversized INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'built',
    attempts INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    prompt_hash TEXT NOT NULL,
    model TEXT NOT NULL,
    usage_prompt_tokens INTEGER,
    usage_completion_tokens INTEGER,
    latency_ms INTEGER,
    created_at TEXT NOT NULL,
    sent_at TEXT,
    completed_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_blocks_status ON blocks(status);

CREATE TABLE IF NOT EXISTS block_files (
    block_id INTEGER NOT NULL REFERENCES blocks(id),
    file_id INTEGER NOT NULL REFERENCES files(id),
    file_ref TEXT NOT NULL,
    content_version INTEGER NOT NULL,
    oversized INTEGER NOT NULL DEFAULT 0,
    outcome TEXT,
    PRIMARY KEY (block_id, file_id)
);
CREATE INDEX IF NOT EXISTS idx_block_files_file ON block_files(file_id);

CREATE TABLE IF NOT EXISTS analyses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id INTEGER NOT NULL REFERENCES files(id),
    block_id INTEGER REFERENCES blocks(id),
    content_version INTEGER NOT NULL,
    prompt_hash TEXT NOT NULL,
    model TEXT NOT NULL,
    resume TEXT NOT NULL DEFAULT '',
    security_classification TEXT NOT NULL,
    security_confidence INTEGER NOT NULL,
    security_justification TEXT NOT NULL DEFAULT '',
    rgpd_risk_level TEXT NOT NULL,
    rgpd_data_types TEXT NOT NULL DEFAULT '[]',
    rgpd_confidence INTEGER NOT NULL,
    finance_document_type TEXT NOT NULL,
    finance_amounts TEXT NOT NULL DEFAULT '[]',
    finance_confidence INTEGER NOT NULL,
    legal_contract_type TEXT NOT NULL,
    legal_parties TEXT NOT NULL DEFAULT '[]',
    legal_confidence INTEGER NOT NULL,
    raw_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (file_id, content_version, prompt_hash, model)
);
CREATE INDEX IF NOT EXISTS idx_analyses_file ON analyses(file_id);
CREATE INDEX IF NOT EXISTS idx_analyses_security ON analyses(security_classification);
CREATE INDEX IF NOT EXISTS idx_analyses_rgpd ON analyses(rgpd_risk_level);
"""

_SCHEMA_V2 = """
ALTER TABLE block_files ADD COLUMN segment_index INTEGER NOT NULL DEFAULT 0;
ALTER TABLE block_files ADD COLUMN segment_count INTEGER NOT NULL DEFAULT 1;
ALTER TABLE analyses ADD COLUMN segments INTEGER NOT NULL DEFAULT 1;

CREATE TABLE IF NOT EXISTS segment_analyses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id INTEGER NOT NULL REFERENCES files(id),
    block_id INTEGER REFERENCES blocks(id),
    content_version INTEGER NOT NULL,
    prompt_hash TEXT NOT NULL,
    model TEXT NOT NULL,
    segment_index INTEGER NOT NULL,
    segment_count INTEGER NOT NULL,
    raw_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (file_id, content_version, prompt_hash, model, segment_index)
);
CREATE INDEX IF NOT EXISTS idx_segment_analyses_file ON segment_analyses(file_id);
"""

_SCHEMA_V3 = """
ALTER TABLE analyses ADD COLUMN retention_required INTEGER NOT NULL DEFAULT 0;
ALTER TABLE analyses ADD COLUMN retention_years INTEGER NOT NULL DEFAULT 0;
ALTER TABLE analyses ADD COLUMN retention_basis TEXT NOT NULL DEFAULT 'none';
ALTER TABLE analyses ADD COLUMN retention_justification TEXT NOT NULL DEFAULT '';
ALTER TABLE analyses ADD COLUMN retention_confidence INTEGER NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS prompts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    text TEXT NOT NULL,
    hash TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS reviews (
    file_id INTEGER PRIMARY KEY REFERENCES files(id),
    status TEXT NOT NULL DEFAULT 'to_review',
    comment TEXT NOT NULL DEFAULT '',
    corrected_security TEXT,
    corrected_rgpd TEXT,
    corrected_retention_years INTEGER,
    reviewer TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL
);
"""

_SCHEMA_V4 = """
CREATE INDEX IF NOT EXISTS idx_files_hash_size ON files(fast_hash, size_bytes);
CREATE INDEX IF NOT EXISTS idx_files_owner ON files(owner);
CREATE INDEX IF NOT EXISTS idx_files_access_time ON files(access_time);
CREATE INDEX IF NOT EXISTS idx_files_last_write ON files(last_write_time);
CREATE INDEX IF NOT EXISTS idx_files_base ON files(base);
CREATE INDEX IF NOT EXISTS idx_analyses_retention ON analyses(retention_required);
CREATE INDEX IF NOT EXISTS idx_reviews_status ON reviews(status);
"""
"""v4 : index de restitution (`views.py`) — doublons, ancienneté, propriétaire, partage."""

_SCHEMA_V5 = """
ALTER TABLE files ADD COLUMN access_time_first TEXT NOT NULL DEFAULT '';
UPDATE files SET access_time_first = access_time;
ALTER TABLE scans ADD COLUMN kind TEXT NOT NULL DEFAULT 'import';
ALTER TABLE scans ADD COLUMN manifest_json TEXT NOT NULL DEFAULT '';
ALTER TABLE scans ADD COLUMN scanner_elapsed_s REAL NOT NULL DEFAULT 0;
"""
"""v5 : `access_time_first` = date d'accès observée au premier scan (ou au dernier
changement de contenu). L'audit lui-même lit les fichiers (hachage, signature,
extraction) et peut mettre à jour la date d'accès NTFS : les statistiques
« non accédé depuis N ans » s'appuient sur cette première observation, jamais
sur une date rafraîchie par un rescan d'un fichier inchangé. `scans.kind` =
`scan` (SMBeagle piloté par docia, manifeste conservé) ou `import` (CSV fourni)."""

_SCHEMA_V6 = f"""
ALTER TABLE files ADD COLUMN access_key TEXT NOT NULL DEFAULT '';
ALTER TABLE files ADD COLUMN write_key TEXT NOT NULL DEFAULT '';
UPDATE files SET access_key = {date_key_sql(first_access_sql())},
                 write_key = {date_key_sql("last_write_time")};

CREATE INDEX IF NOT EXISTS idx_files_access_key ON files(access_key, size_bytes);
CREATE INDEX IF NOT EXISTS idx_files_write_key ON files(write_key, size_bytes);
CREATE INDEX IF NOT EXISTS idx_files_extension_size ON files(extension, size_bytes);
CREATE INDEX IF NOT EXISTS idx_files_owner_size ON files(owner, size_bytes);
CREATE INDEX IF NOT EXISTS idx_files_share_size ON files(base, unc_directory, size_bytes);
CREATE INDEX IF NOT EXISTS idx_files_status_size ON files(status, size_bytes);
CREATE INDEX IF NOT EXISTS idx_analyses_file_latest ON analyses(file_id, created_at, id);

DROP INDEX IF EXISTS idx_files_extension;
DROP INDEX IF EXISTS idx_files_owner;
DROP INDEX IF EXISTS idx_files_base;
DROP INDEX IF EXISTS idx_files_status;

ANALYZE;
"""
"""v6 : `access_key` / `write_key` = dates d'accès et d'écriture normalisées en
`yyyymmdd` (`''` si absente ou illisible), remplies à l'insertion comme à la mise
à jour par `upsert_files` et rétro-remplies ici. Les vues d'ancienneté comparaient
jusqu'ici des `substr()` calculés ligne par ligne : aucun index n'était utilisable
et chaque seuil relançait un balayage complet. Les index sont couvrants (la taille
suit la clé) pour que les totaux se lisent sans toucher la table ; les index d'une
seule colonne qu'ils remplacent (préfixe identique) sont supprimés. `ANALYZE` donne
au planificateur les cardinalités réelles dès la migration ; il est rejoué à la fin
de chaque scan (`finish_scan`)."""

_SCHEMA_V7 = """
ALTER TABLE scans ADD COLUMN complete INTEGER NOT NULL DEFAULT 1;
ALTER TABLE scans ADD COLUMN skipped_json TEXT NOT NULL DEFAULT '';
ALTER TABLE scans ADD COLUMN cancelled INTEGER NOT NULL DEFAULT 0;
ALTER TABLE scans ADD COLUMN exit_code INTEGER NOT NULL DEFAULT 0;
ALTER TABLE scans ADD COLUMN expected_files INTEGER NOT NULL DEFAULT -1;
"""
"""v7 : le **périmètre** du scan, conservé avec la campagne.

Un audit sert à décider de suppressions : « cette campagne porte-t-elle sur tout
ce qu'on a demandé ? » doit trouver sa réponse en base des mois plus tard, sans
le manifeste ni le CSV sous la main. Jusqu'ici la table `scans` ne portait que ce
qui avait été importé, jamais ce qui manquait : un partage refusé par une ACL,
un scan arrêté par l'utilisateur et un scan complet y étaient identiques.

- `complete` : 0 dès qu'une cible a été écartée, que le scan a été arrêté ou que
  le CSV est plus court que le compte annoncé. **1 par défaut** — les scans déjà
  en base, et ceux d'un `SMBeagle.exe` antérieur qui rend 0 et n'écrit aucun
  `skipped`, restent réputés complets : la migration n'invente pas de faux
  positif « périmètre incomplet ».
- `skipped_json` : liste JSON des cibles demandées mais non scannées (`''` = aucune).
- `cancelled` : l'utilisateur a arrêté le scan (attendu) — à ne pas confondre
  avec un scanner mort en écrivant, que `scan.run_scan` refuse d'importer.
- `exit_code` : code de retour du scanner (0 par défaut ; 4 = périmètre amputé).
- `expected_files` : nombre de fichiers annoncé par le scanner, à comparer à
  `rows_total`. **-1 = inconnu** (import d'un CSV fourni, scan d'avant la v7) :
  sans cette valeur distincte de 0, un import ordinaire aurait paru tronqué.
"""

_MIGRATIONS: list[tuple[int, str]] = [
    (1, _SCHEMA_V1),
    (2, _SCHEMA_V2),
    (3, _SCHEMA_V3),
    (4, _SCHEMA_V4),
    (5, _SCHEMA_V5),
    (6, _SCHEMA_V6),
    (7, _SCHEMA_V7),
]

FILES_INDEXES: dict[str, str] = {
    "idx_files_fast_hash": "CREATE INDEX IF NOT EXISTS idx_files_fast_hash ON files(fast_hash)",
    "idx_files_priority": (
        "CREATE INDEX IF NOT EXISTS idx_files_priority ON files(priority_score DESC, path)"
    ),
    "idx_files_hash_size": (
        "CREATE INDEX IF NOT EXISTS idx_files_hash_size ON files(fast_hash, size_bytes)"
    ),
    "idx_files_access_time": (
        "CREATE INDEX IF NOT EXISTS idx_files_access_time ON files(access_time)"
    ),
    "idx_files_last_write": (
        "CREATE INDEX IF NOT EXISTS idx_files_last_write ON files(last_write_time)"
    ),
    "idx_files_access_key": (
        "CREATE INDEX IF NOT EXISTS idx_files_access_key ON files(access_key, size_bytes)"
    ),
    "idx_files_write_key": (
        "CREATE INDEX IF NOT EXISTS idx_files_write_key ON files(write_key, size_bytes)"
    ),
    "idx_files_extension_size": (
        "CREATE INDEX IF NOT EXISTS idx_files_extension_size ON files(extension, size_bytes)"
    ),
    "idx_files_owner_size": (
        "CREATE INDEX IF NOT EXISTS idx_files_owner_size ON files(owner, size_bytes)"
    ),
    "idx_files_share_size": (
        "CREATE INDEX IF NOT EXISTS idx_files_share_size ON files(base, unc_directory, size_bytes)"
    ),
    "idx_files_status_size": (
        "CREATE INDEX IF NOT EXISTS idx_files_status_size ON files(status, size_bytes)"
    ),
}
"""Index secondaires de `files` attendus au schéma courant, avec leur définition.

Miroir de ce que produisent les migrations (v1 + v4 + v6, moins ceux que la v6
supprime) : `tests/test_db.py` compare ce dictionnaire aux index réellement
présents dans une base neuve, il ne peut donc pas dériver en silence. Il sert de
filet à l'ouverture (`_ensure_files_indexes`) quand un chargement en masse
(`bulk_load`) a été interrompu avant d'avoir recréé les index.
"""
