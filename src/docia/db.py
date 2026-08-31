"""Base SQLite : schéma versionné, accès aux fichiers, blocs et analyses.

Une seule connexion par `Database` (mode WAL, `check_same_thread=False` car
le pipeline est asynchrone mais mono-thread). Toutes les écritures passent par
des méthodes explicites ; aucun `ALTER` implicite hors `_MIGRATIONS`.
"""

from __future__ import annotations

import json
import logging
import shutil
import sqlite3
from collections.abc import Iterable, Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from docia.models import (
    BlockFile,
    BlockSpec,
    BlockStatus,
    FileAnalysis,
    FileRow,
    FileStatus,
    LLMUsage,
    SmbeagleRow,
    path_key,
)

SCHEMA_VERSION = 6

BACKUP_DIR_SUFFIX = ".backups"
"""Suffixe du dossier de sauvegardes, à côté de la base (`docia.sqlite.backups`)."""

logger = logging.getLogger(__name__)


def backup_dir_for(db_path: Path) -> Path:
    """Dossier de sauvegardes d'une base : `<base>.backups` (à côté du fichier)."""
    return db_path.with_name(db_path.name + BACKUP_DIR_SUFFIX)


def date_key_sql(column: str) -> str:
    """Expression SQL rendant `yyyymmdd` (ou `''`) pour une date SMBeagle ou ISO.

    Miroir exact de `date_key` : les deux doivent rendre la même chaîne pour
    toute valeur (vérifié par `tests/test_db.py`). Sert à remplir `files.access_key`
    et `files.write_key` (schéma v6) et à les rétro-remplir à la migration.
    """
    return (
        f"CASE WHEN length({column})>=10 AND substr({column},3,1)='/' AND substr({column},6,1)='/'"
        f" THEN substr({column},7,4)||substr({column},4,2)||substr({column},1,2)"
        f" WHEN length({column})>=10 AND substr({column},5,1)='-'"
        f" THEN substr({column},1,4)||substr({column},6,2)||substr({column},9,2)"
        f" ELSE '' END"
    )


def date_key(value: str) -> str:
    """`yyyymmdd` d'une date SMBeagle (`dd/MM/yyyy…`) ou ISO, `''` si illisible.

    Clé comparable lexicographiquement : c'est elle qui est stockée dans
    `files.access_key` / `files.write_key` pour que les vues d'ancienneté
    s'appuient sur un index au lieu de reformater chaque ligne.
    """
    if len(value) >= 10:
        if value[2] == "/" and value[5] == "/":
            return value[6:10] + value[3:5] + value[0:2]
        if value[4] == "-":
            return value[0:4] + value[5:7] + value[8:10]
    return ""


def first_access_sql(prefix: str = "") -> str:
    """Date d'accès retenue pour l'ancienneté : la première observée (schéma v5).

    Le hachage et l'extraction de l'audit lisent les fichiers et peuvent
    rafraîchir la date d'accès NTFS : la statistique « non accédé depuis N ans »
    s'appuie donc sur `access_time_first`, et ne retombe sur `access_time` que
    si cette première observation manque.
    """
    return f"COALESCE(NULLIF({prefix}access_time_first, ''), {prefix}access_time)"


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


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

_MIGRATIONS: list[tuple[int, str]] = [
    (1, _SCHEMA_V1),
    (2, _SCHEMA_V2),
    (3, _SCHEMA_V3),
    (4, _SCHEMA_V4),
    (5, _SCHEMA_V5),
    (6, _SCHEMA_V6),
]

REVIEW_STATUSES = ("to_review", "validated", "corrected")


class Database:
    """Accès à la base. Utiliser comme gestionnaire de contexte ou appeler `close()`."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._migrate()

    # ------------------------------------------------------------------ infra
    def __enter__(self) -> Database:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def close(self) -> None:
        self._conn.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        try:
            self._conn.execute("BEGIN")
            yield self._conn
            self._conn.execute("COMMIT")
        except BaseException:
            self._conn.execute("ROLLBACK")
            raise

    def query(self, sql: str, params: tuple[object, ...] = ()) -> list[sqlite3.Row]:
        """Exécute un `SELECT` et rend les lignes (accès lecture seule pour `views.py`)."""
        return list(self._conn.execute(sql, params))

    def query_values(self, sql: str, params: tuple[object, ...] = ()) -> list[tuple[Any, ...]]:
        """Comme `query`, mais rend des tuples bruts, sans `sqlite3.Row`.

        Réservé aux agrégations qui parcourent des centaines de milliers de
        lignes (répartition par répertoire) : un objet de moins par ligne.
        """
        cursor = self._conn.cursor()
        cursor.row_factory = None
        try:
            rows: list[tuple[Any, ...]] = cursor.execute(sql, params).fetchall()
            return rows
        finally:
            cursor.close()

    def backup_to(self, path: str | Path) -> None:
        """Copie cohérente de la base vers `path` (API `sqlite3.Connection.backup`).

        Utilisable pendant un run : SQLite garantit un instantané cohérent.
        """
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        dest = sqlite3.connect(str(target))
        try:
            self._conn.backup(dest)
        finally:
            dest.close()

    @property
    def schema_version(self) -> int:
        row = self._conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
        return int(row["value"]) if row else 0

    def _backup_before_migration(self) -> None:
        """Copie la base telle quelle avant d'appliquer une migration de schéma.

        Ne fait rien pour une base neuve (aucune table) ni pour une base déjà à
        jour. Une copie impossible (disque plein, droits) est journalisée sans
        interrompre l'ouverture : la migration reste possible.
        """
        names = {
            str(r[0])
            for r in self._conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        if not names:
            return  # base neuve : rien à sauvegarder
        current = self.schema_version if "meta" in names else 0
        if current >= SCHEMA_VERSION:
            return
        target = (
            backup_dir_for(self.path) / f"{self.path.stem}_avant_migration_v{SCHEMA_VERSION}.sqlite"
        )
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(self.path, target)
        except OSError as exc:  # pragma: no cover - dépend du système de fichiers
            logger.warning("sauvegarde avant migration impossible (%s) : %s", target, exc)
            return
        logger.info("sauvegarde avant migration v%s → %s", SCHEMA_VERSION, target)

    def _migrate(self) -> None:
        self._backup_before_migration()
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        current = self.schema_version
        for version, sql in _MIGRATIONS:
            if version > current:
                with self.transaction() as conn:
                    conn.executescript(sql)
                    conn.execute(
                        "INSERT INTO meta(key, value) VALUES('schema_version', ?) "
                        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                        (str(version),),
                    )
                current = version

    # ------------------------------------------------------------------ scans
    def start_scan(self, csv_path: str, *, kind: str = "import") -> int:
        cur = self._conn.execute(
            "INSERT INTO scans(csv_path, imported_at, kind) VALUES(?, ?, ?)",
            (csv_path, _now(), kind),
        )
        self._conn.commit()
        return int(cur.lastrowid or 0)

    def annotate_scan(self, scan_id: int, *, manifest_json: str, scanner_elapsed_s: float) -> None:
        """Attache le manifeste du scanner (options, cibles, compteurs) au scan importé."""
        self._conn.execute(
            "UPDATE scans SET kind='scan', manifest_json=?, scanner_elapsed_s=? WHERE id=?",
            (manifest_json, scanner_elapsed_s, scan_id),
        )
        self._conn.commit()

    def last_scan(self) -> sqlite3.Row | None:
        row = self._conn.execute("SELECT * FROM scans ORDER BY id DESC LIMIT 1").fetchone()
        return row if isinstance(row, sqlite3.Row) else None

    def finish_scan(
        self, scan_id: int, *, total: int, new: int, updated: int, unchanged: int, invalid: int
    ) -> None:
        """Clôt un scan et rafraîchit les statistiques d'index.

        `ANALYZE` (moins d'une seconde pour 200 000 fichiers) donne au planificateur
        les cardinalités réelles : sans elles, plusieurs vues statistiques
        choisissent un index moins bon que le balayage couvrant attendu.
        """
        self._conn.execute(
            "UPDATE scans SET rows_total=?, rows_new=?, rows_updated=?, rows_unchanged=?, rows_invalid=? WHERE id=?",
            (total, new, updated, unchanged, invalid, scan_id),
        )
        self._conn.execute("ANALYZE")
        self._conn.commit()

    def upsert_files(self, rows: Iterable[SmbeagleRow], scan_id: int) -> tuple[int, int, int]:
        """Insère ou met à jour des lignes SMBeagle.

        Un fichier connu dont `fast_hash`, `size` ou `last_write_time` change
        prend `content_version + 1` et repasse `pending` (sauf s'il est
        `excluded`, l'exclusion étant une règle, pas un état de contenu).

        `access_key` / `write_key` (schéma v6) sont recalculées à chaque écriture :
        elles doivent rester le reflet exact de `COALESCE(NULLIF(access_time_first,
        ''), access_time)` et de `last_write_time`.

        Returns:
            (nouveaux, modifiés, inchangés).
        """
        new = updated = unchanged = 0
        now = _now()
        with self.transaction() as conn:
            for row in rows:
                key = path_key(row.path)
                existing = conn.execute(
                    "SELECT id, fast_hash, size_bytes, last_write_time, access_time_first, status,"
                    " content_version FROM files WHERE path_key=?",
                    (key,),
                ).fetchone()
                if existing is None:
                    conn.execute(
                        """INSERT INTO files(path_key, path, name, extension, host, hostname, username,
                           unc_directory, base, directory_type, size_bytes, creation_time, last_write_time,
                           access_time, access_time_first, file_attributes, owner, fast_hash, file_signature,
                           readable, writeable, deletable, first_seen_scan_id, last_seen_scan_id,
                           access_key, write_key,
                           content_version, status, updated_at)
                           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,'pending',?)""",
                        (
                            key,
                            row.path,
                            row.name,
                            row.extension,
                            row.host,
                            row.hostname,
                            row.username,
                            row.unc_directory,
                            row.base,
                            row.directory_type,
                            row.file_size,
                            row.creation_time,
                            row.last_write_time,
                            row.access_time,
                            row.access_time,
                            row.file_attributes,
                            row.owner,
                            row.fast_hash,
                            row.file_signature,
                            int(row.readable),
                            int(row.writeable),
                            int(row.deletable),
                            scan_id,
                            scan_id,
                            date_key(row.access_time),
                            date_key(row.last_write_time),
                            now,
                        ),
                    )
                    new += 1
                    continue
                changed = (
                    existing["fast_hash"] != row.fast_hash
                    or int(existing["size_bytes"]) != row.file_size
                    or existing["last_write_time"] != row.last_write_time
                )
                if changed:
                    new_status = (
                        existing["status"]
                        if existing["status"] == FileStatus.EXCLUDED
                        else FileStatus.PENDING
                    )
                    conn.execute(
                        """UPDATE files SET size_bytes=?, creation_time=?, last_write_time=?, access_time=?,
                           access_time_first=?, access_key=?, write_key=?,
                           file_attributes=?, owner=?, fast_hash=?, file_signature=?, readable=?, writeable=?,
                           deletable=?, last_seen_scan_id=?, content_version=content_version+1, status=?,
                           exclusion_reason=CASE WHEN ?='excluded' THEN exclusion_reason ELSE NULL END, updated_at=?
                           WHERE id=?""",
                        (
                            row.file_size,
                            row.creation_time,
                            row.last_write_time,
                            row.access_time,
                            row.access_time,
                            date_key(row.access_time),
                            date_key(row.last_write_time),
                            row.file_attributes,
                            row.owner,
                            row.fast_hash,
                            row.file_signature,
                            int(row.readable),
                            int(row.writeable),
                            int(row.deletable),
                            scan_id,
                            str(new_status),
                            str(new_status),
                            now,
                            existing["id"],
                        ),
                    )
                    updated += 1
                else:
                    # `access_time_first` ne bouge pas : la clé d'accès ne retombe sur
                    # `access_time` que si la première observation manque.
                    first_access = str(existing["access_time_first"]) or row.access_time
                    conn.execute(
                        "UPDATE files SET last_seen_scan_id=?, access_time=?, access_key=?,"
                        " updated_at=? WHERE id=?",
                        (scan_id, row.access_time, date_key(first_access), now, existing["id"]),
                    )
                    unchanged += 1
        return new, updated, unchanged

    # ------------------------------------------------------------------ files
    @staticmethod
    def _file_row(r: sqlite3.Row) -> FileRow:
        return FileRow(
            id=int(r["id"]),
            path=r["path"],
            name=r["name"],
            extension=r["extension"],
            size_bytes=int(r["size_bytes"]),
            fast_hash=r["fast_hash"],
            last_write_time=r["last_write_time"],
            content_version=int(r["content_version"]),
            status=FileStatus(r["status"]),
            exclusion_reason=r["exclusion_reason"],
            priority_score=int(r["priority_score"]),
            owner=r["owner"],
            host=r["host"],
            unc_directory=r["unc_directory"],
        )

    def get_file(self, file_id: int) -> FileRow | None:
        r = self._conn.execute("SELECT * FROM files WHERE id=?", (file_id,)).fetchone()
        return self._file_row(r) if r else None

    def iter_files(self, status: FileStatus | None = None) -> Iterator[FileRow]:
        sql = "SELECT * FROM files"
        params: tuple[object, ...] = ()
        if status is not None:
            sql += " WHERE status=?"
            params = (str(status),)
        sql += " ORDER BY priority_score DESC, path"
        for r in self._conn.execute(sql, params):
            yield self._file_row(r)

    def select_pending(self, limit: int, *, prompt_hash: str, model: str) -> list[FileRow]:
        """Fichiers à analyser : `pending`, sans analyse pour leur version de contenu
        courante avec ce prompt et ce modèle. Ordre : priorité, puis chemin."""
        rows = self._conn.execute(
            """SELECT f.* FROM files f
               WHERE f.status='pending'
                 AND NOT EXISTS (SELECT 1 FROM analyses a WHERE a.file_id=f.id
                                 AND a.content_version=f.content_version
                                 AND a.prompt_hash=? AND a.model=?)
               ORDER BY f.priority_score DESC, f.path LIMIT ?""",
            (prompt_hash, model, limit),
        ).fetchall()
        return [self._file_row(r) for r in rows]

    def set_file_status(self, file_id: int, status: FileStatus, reason: str | None = None) -> None:
        self._conn.execute(
            "UPDATE files SET status=?, exclusion_reason=?, updated_at=? WHERE id=?",
            (str(status), reason, _now(), file_id),
        )
        self._conn.commit()

    def set_files_status(
        self, file_ids: Sequence[int], status: FileStatus, reason: str | None = None
    ) -> None:
        now = _now()
        with self.transaction() as conn:
            conn.executemany(
                "UPDATE files SET status=?, exclusion_reason=?, updated_at=? WHERE id=?",
                [(str(status), reason, now, fid) for fid in file_ids],
            )

    def apply_plan(
        self, decisions: Iterable[tuple[int, FileStatus, str | None, int]]
    ) -> tuple[int, int]:
        """Applique les décisions du filtre : (file_id, statut, raison, score).
        Ne touche pas aux fichiers `done`/`error` sauf pour le score.

        Returns:
            (fichiers pending, fichiers exclus).
        """
        pending = excluded = 0
        now = _now()
        with self.transaction() as conn:
            for file_id, status, reason, score in decisions:
                if status == FileStatus.EXCLUDED:
                    conn.execute(
                        "UPDATE files SET status='excluded', exclusion_reason=?, priority_score=?, updated_at=? WHERE id=? AND status IN ('pending','excluded','queued')",
                        (reason, score, now, file_id),
                    )
                    excluded += 1
                else:
                    conn.execute(
                        "UPDATE files SET status=CASE WHEN status IN ('excluded','queued') THEN 'pending' ELSE status END, exclusion_reason=CASE WHEN status='excluded' THEN NULL ELSE exclusion_reason END, priority_score=?, updated_at=? WHERE id=?",
                        (score, now, file_id),
                    )
                    pending += 1
        return pending, excluded

    def reset_errors(self) -> int:
        cur = self._conn.execute(
            "UPDATE files SET status='pending', exclusion_reason=NULL, updated_at=? WHERE status='error'",
            (_now(),),
        )
        self._conn.commit()
        return int(cur.rowcount)

    def requeue_stale(self) -> int:
        """Fichiers `queued` sans bloc en vol (interruption) → `pending`."""
        cur = self._conn.execute(
            """UPDATE files SET status='pending', updated_at=? WHERE status='queued' AND id NOT IN (
                 SELECT bf.file_id FROM block_files bf JOIN blocks b ON b.id=bf.block_id WHERE b.status IN ('built','sent'))""",
            (_now(),),
        )
        self._conn.commit()
        return int(cur.rowcount)

    # ------------------------------------------------------------------ runs
    def start_run(self, *, model: str, prompt_hash: str, config_json: str) -> int:
        cur = self._conn.execute(
            "INSERT INTO runs(started_at, model, prompt_hash, config_json) VALUES(?,?,?,?)",
            (_now(), model, prompt_hash, config_json),
        )
        self._conn.commit()
        return int(cur.lastrowid or 0)

    def finish_run(self, run_id: int, status: str = "done") -> None:
        self._conn.execute(
            "UPDATE runs SET finished_at=?, status=? WHERE id=?", (_now(), status, run_id)
        )
        self._conn.commit()

    # ------------------------------------------------------------------ blocks
    def create_block(self, run_id: int, spec: BlockSpec, *, prompt_hash: str, model: str) -> int:
        with self.transaction() as conn:
            cur = conn.execute(
                """INSERT INTO blocks(run_id, path, tokens_estimated, tokens_with_margin, file_count, oversized,
                   status, prompt_hash, model, created_at) VALUES(?,?,?,?,?,?,'built',?,?,?)""",
                (
                    run_id,
                    str(spec.path),
                    spec.tokens_estimated,
                    spec.tokens_with_margin,
                    len(spec.files),
                    int(spec.oversized),
                    prompt_hash,
                    model,
                    _now(),
                ),
            )
            block_id = int(cur.lastrowid or 0)
            conn.executemany(
                "INSERT INTO block_files(block_id, file_id, file_ref, content_version, oversized,"
                " segment_index, segment_count) VALUES(?,?,?,?,?,?,?)",
                [
                    (
                        block_id,
                        bf.file_id,
                        bf.file_ref,
                        bf.content_version,
                        int(bf.oversized),
                        bf.segment_index,
                        bf.segment_count,
                    )
                    for bf in spec.files
                ],
            )
            conn.executemany(
                "UPDATE files SET status='queued', updated_at=? WHERE id=?",
                [(_now(), bf.file_id) for bf in spec.files],
            )
        spec.block_id = block_id
        return block_id

    def mark_block_sent(self, block_id: int) -> None:
        self._conn.execute(
            "UPDATE blocks SET status='sent', attempts=attempts+1, sent_at=? WHERE id=?",
            (_now(), block_id),
        )
        self._conn.commit()

    def mark_block_done(self, block_id: int, usage: LLMUsage | None) -> None:
        self._conn.execute(
            """UPDATE blocks SET status='done', completed_at=?, usage_prompt_tokens=?, usage_completion_tokens=?,
               latency_ms=?, error=NULL WHERE id=?""",
            (
                _now(),
                usage.prompt_tokens if usage else None,
                usage.completion_tokens if usage else None,
                usage.latency_ms if usage else None,
                block_id,
            ),
        )
        self._conn.commit()

    def mark_block_error(self, block_id: int, error: str) -> None:
        self._conn.execute(
            "UPDATE blocks SET status='error', completed_at=?, error=? WHERE id=?",
            (_now(), error[:2000], block_id),
        )
        self._conn.commit()

    def block_files(self, block_id: int) -> list[BlockFile]:
        rows = self._conn.execute(
            "SELECT file_id, file_ref, content_version, oversized, segment_index, segment_count"
            " FROM block_files WHERE block_id=? ORDER BY rowid",
            (block_id,),
        ).fetchall()
        return [
            BlockFile(
                int(r["file_id"]),
                r["file_ref"],
                int(r["content_version"]),
                bool(r["oversized"]),
                int(r["segment_index"]),
                int(r["segment_count"]),
            )
            for r in rows
        ]

    def file_attempts(self, file_id: int) -> int:
        """Nombre de blocs dans lesquels ce fichier a déjà été envoyé (tentatives)."""
        row = self._conn.execute(
            """SELECT COUNT(*) FROM block_files bf JOIN blocks b ON b.id=bf.block_id
               WHERE bf.file_id=? AND b.status IN ('sent','done','error')""",
            (file_id,),
        ).fetchone()
        return int(row[0])

    def set_block_file_outcome(self, block_id: int, file_id: int, outcome: str) -> None:
        self._conn.execute(
            "UPDATE block_files SET outcome=? WHERE block_id=? AND file_id=?",
            (outcome, block_id, file_id),
        )
        self._conn.commit()

    def pending_blocks(self, *, prompt_hash: str, model: str) -> list[BlockSpec]:
        """Blocs `built`/`sent` d'un run précédent, à (re)envoyer — reprise."""
        rows = self._conn.execute(
            "SELECT * FROM blocks WHERE status IN ('built','sent') AND prompt_hash=? AND model=? ORDER BY id",
            (prompt_hash, model),
        ).fetchall()
        specs: list[BlockSpec] = []
        for r in rows:
            specs.append(
                BlockSpec(
                    path=Path(r["path"]),
                    files=self.block_files(int(r["id"])),
                    tokens_estimated=int(r["tokens_estimated"]),
                    tokens_with_margin=int(r["tokens_with_margin"]),
                    oversized=bool(r["oversized"]),
                    block_id=int(r["id"]),
                )
            )
        return specs

    # ------------------------------------------------------------------ analyses
    def store_analysis(
        self,
        file_id: int,
        block_id: int | None,
        content_version: int,
        *,
        prompt_hash: str,
        model: str,
        analysis: FileAnalysis,
        segments: int = 1,
    ) -> None:
        """Insère (ou remplace) l'analyse d'un fichier et le passe `done`, en une transaction."""
        with self.transaction() as conn:
            conn.execute(
                """INSERT INTO analyses(file_id, block_id, content_version, prompt_hash, model, resume,
                   security_classification, security_confidence, security_justification,
                   rgpd_risk_level, rgpd_data_types, rgpd_confidence,
                   finance_document_type, finance_amounts, finance_confidence,
                   legal_contract_type, legal_parties, legal_confidence, raw_json, created_at,
                   segments, retention_required, retention_years, retention_basis,
                   retention_justification, retention_confidence)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(file_id, content_version, prompt_hash, model) DO UPDATE SET
                   block_id=excluded.block_id, resume=excluded.resume,
                   security_classification=excluded.security_classification, security_confidence=excluded.security_confidence,
                   security_justification=excluded.security_justification, rgpd_risk_level=excluded.rgpd_risk_level,
                   rgpd_data_types=excluded.rgpd_data_types, rgpd_confidence=excluded.rgpd_confidence,
                   finance_document_type=excluded.finance_document_type, finance_amounts=excluded.finance_amounts,
                   finance_confidence=excluded.finance_confidence, legal_contract_type=excluded.legal_contract_type,
                   legal_parties=excluded.legal_parties, legal_confidence=excluded.legal_confidence,
                   raw_json=excluded.raw_json, created_at=excluded.created_at,
                   segments=excluded.segments, retention_required=excluded.retention_required,
                   retention_years=excluded.retention_years, retention_basis=excluded.retention_basis,
                   retention_justification=excluded.retention_justification,
                   retention_confidence=excluded.retention_confidence""",
                (
                    file_id,
                    block_id,
                    content_version,
                    prompt_hash,
                    model,
                    analysis.resume,
                    analysis.security.label,
                    analysis.security.confidence,
                    str(analysis.security.details.get("justification", "")),
                    analysis.rgpd.label,
                    json.dumps(analysis.rgpd.details.get("data_types", []), ensure_ascii=False),
                    analysis.rgpd.confidence,
                    analysis.finance.label,
                    json.dumps(analysis.finance.details.get("amounts", []), ensure_ascii=False),
                    analysis.finance.confidence,
                    analysis.legal.label,
                    json.dumps(analysis.legal.details.get("parties", []), ensure_ascii=False),
                    analysis.legal.confidence,
                    json.dumps(analysis.raw, ensure_ascii=False),
                    _now(),
                    segments,
                    int(bool(analysis.retention.details.get("required", False))),
                    int(str(analysis.retention.details.get("years", 0)) or 0),
                    analysis.retention.label,
                    str(analysis.retention.details.get("justification", "")),
                    analysis.retention.confidence,
                ),
            )
            conn.execute(
                "UPDATE files SET status='done', exclusion_reason=NULL, updated_at=? WHERE id=?",
                (_now(), file_id),
            )

    def store_segment_analysis(
        self,
        file_id: int,
        block_id: int | None,
        content_version: int,
        *,
        prompt_hash: str,
        model: str,
        segment_index: int,
        segment_count: int,
        raw: dict[str, object],
    ) -> None:
        """Analyse d'un segment d'un fichier découpé (le fichier reste `queued`
        jusqu'à l'agrégation des K segments)."""
        self._conn.execute(
            """INSERT INTO segment_analyses(file_id, block_id, content_version, prompt_hash, model,
               segment_index, segment_count, raw_json, created_at) VALUES(?,?,?,?,?,?,?,?,?)
               ON CONFLICT(file_id, content_version, prompt_hash, model, segment_index)
               DO UPDATE SET block_id=excluded.block_id, raw_json=excluded.raw_json,
               created_at=excluded.created_at""",
            (
                file_id,
                block_id,
                content_version,
                prompt_hash,
                model,
                segment_index,
                segment_count,
                json.dumps(raw, ensure_ascii=False),
                _now(),
            ),
        )
        self._conn.commit()

    def segment_analyses(
        self, file_id: int, content_version: int, *, prompt_hash: str, model: str
    ) -> list[tuple[int, int, dict[str, object]]]:
        """Segments déjà analysés : (index, count, JSON brut), triés par index."""
        rows = self._conn.execute(
            """SELECT segment_index, segment_count, raw_json FROM segment_analyses
               WHERE file_id=? AND content_version=? AND prompt_hash=? AND model=? ORDER BY segment_index""",
            (file_id, content_version, prompt_hash, model),
        ).fetchall()
        out: list[tuple[int, int, dict[str, object]]] = []
        for r in rows:
            raw = json.loads(r["raw_json"])
            out.append(
                (
                    int(r["segment_index"]),
                    int(r["segment_count"]),
                    raw if isinstance(raw, dict) else {},
                )
            )
        return out

    def copy_analysis(
        self,
        src_file_id: int,
        dst_file_id: int,
        dst_content_version: int,
        *,
        prompt_hash: str,
        model: str,
    ) -> bool:
        """Copie l'analyse courante de `src` vers `dst` (contenu identique — doublon
        DocFuse) et passe `dst` en `done`. False si `src` n'a pas d'analyse."""
        src = self._conn.execute(
            """SELECT * FROM analyses WHERE file_id=? AND prompt_hash=? AND model=?
               AND content_version=(SELECT content_version FROM files WHERE id=?)
               ORDER BY id DESC LIMIT 1""",
            (src_file_id, prompt_hash, model, src_file_id),
        ).fetchone()
        if src is None:
            return False
        # sqlite3.Row : itérer la ligne donne les VALEURS, pas les clés → `.keys()` obligatoire.
        skip = ("id", "file_id", "content_version", "created_at")
        cols = [k for k in src.keys() if k not in skip]  # noqa: SIM118
        with self.transaction() as conn:
            conn.execute(
                "DELETE FROM analyses WHERE file_id=? AND content_version=? AND prompt_hash=? AND model=?",
                (dst_file_id, dst_content_version, prompt_hash, model),
            )
            conn.execute(
                f"INSERT INTO analyses(file_id, content_version, created_at, {', '.join(cols)}) "  # noqa: S608
                f"VALUES(?, ?, ?, {', '.join('?' for _ in cols)})",
                (dst_file_id, dst_content_version, _now(), *[src[c] for c in cols]),
            )
            conn.execute(
                "UPDATE files SET status='done', exclusion_reason=NULL, updated_at=? WHERE id=?",
                (_now(), dst_file_id),
            )
        return True

    def delete_analyses(self, file_ids: Sequence[int], *, prompt_hash: str, model: str) -> int:
        """Supprime analyses et segments de ces fichiers pour ce prompt et ce modèle.

        Une seule transaction (par paquets de 500 identifiants, limite SQLite sur
        le nombre de paramètres). Rend le nombre de lignes `analyses` supprimées.
        """
        if not file_ids:
            return 0
        deleted = 0
        with self.transaction() as conn:
            for start in range(0, len(file_ids), 500):
                chunk = tuple(file_ids[start : start + 500])
                marks = ",".join("?" for _ in chunk)
                params: tuple[object, ...] = (*chunk, prompt_hash, model)
                conn.execute(
                    f"DELETE FROM segment_analyses WHERE file_id IN ({marks})"  # noqa: S608
                    " AND prompt_hash=? AND model=?",
                    params,
                )
                cur = conn.execute(
                    f"DELETE FROM analyses WHERE file_id IN ({marks})"  # noqa: S608
                    " AND prompt_hash=? AND model=?",
                    params,
                )
                deleted += int(cur.rowcount)
        return deleted

    def latest_analyses(self, *, file_id: int | None = None) -> Iterator[sqlite3.Row]:
        """Dernière analyse de chaque fichier (jointe au fichier), pour l'export.

        `file_id` : une seule fiche. L'écran Résultats relisait toute la campagne à
        chaque clic sur une ligne (plusieurs secondes sur 200 000 fichiers).
        """
        where = "WHERE f.id = ? " if file_id is not None else ""
        params: tuple[object, ...] = (file_id,) if file_id is not None else ()
        return iter(
            self._conn.execute(
                f"""SELECT f.path, f.name, f.extension, f.size_bytes, f.owner, f.host, f.status, f.exclusion_reason,
                          f.content_version, a.model, a.prompt_hash, a.resume,
                          a.security_classification, a.security_confidence, a.security_justification,
                          a.rgpd_risk_level, a.rgpd_data_types, a.rgpd_confidence,
                          a.finance_document_type, a.finance_amounts, a.finance_confidence,
                          a.legal_contract_type, a.legal_parties, a.legal_confidence, a.created_at,
                          a.segments, a.retention_required, a.retention_years, a.retention_basis,
                          a.retention_justification, a.retention_confidence,
                          r.status AS review_status, r.comment AS review_comment,
                          r.corrected_security, r.corrected_rgpd, r.corrected_retention_years,
                          r.reviewer, r.updated_at AS reviewed_at, f.id AS id
                   FROM files f LEFT JOIN analyses a ON a.id = (
                        SELECT id FROM analyses WHERE file_id=f.id ORDER BY created_at DESC, id DESC LIMIT 1)
                   LEFT JOIN reviews r ON r.file_id = f.id
                   {where}ORDER BY f.path""",
                params,
            )
        )

    # ------------------------------------------------------------------ prompts
    def save_prompt(self, name: str, text: str, *, activate: bool = False) -> int:
        """Crée ou met à jour un profil de prompt nommé."""
        import hashlib

        digest = hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]
        now = _now()
        with self.transaction() as conn:
            conn.execute(
                """INSERT INTO prompts(name, text, hash, active, created_at, updated_at)
                   VALUES(?,?,?,0,?,?)
                   ON CONFLICT(name) DO UPDATE SET text=excluded.text, hash=excluded.hash,
                   updated_at=excluded.updated_at""",
                (name, text, digest, now, now),
            )
            if activate:
                conn.execute("UPDATE prompts SET active=0")
                conn.execute("UPDATE prompts SET active=1 WHERE name=?", (name,))
            row = conn.execute("SELECT id FROM prompts WHERE name=?", (name,)).fetchone()
        return int(row["id"])

    def list_prompts(self) -> list[sqlite3.Row]:
        return list(
            self._conn.execute(
                "SELECT id, name, hash, active, length(text) AS chars, created_at, updated_at"
                " FROM prompts ORDER BY name"
            )
        )

    def get_prompt(self, name: str) -> str | None:
        row = self._conn.execute("SELECT text FROM prompts WHERE name=?", (name,)).fetchone()
        return str(row["text"]) if row else None

    def set_active_prompt(self, name: str | None) -> bool:
        """Active un profil (None = aucun : prompt embarqué). False si inconnu."""
        with self.transaction() as conn:
            conn.execute("UPDATE prompts SET active=0")
            if name is None:
                return True
            cur = conn.execute("UPDATE prompts SET active=1 WHERE name=?", (name,))
            return cur.rowcount == 1

    def active_prompt(self) -> tuple[str, str] | None:
        """(nom, texte) du profil actif, ou None (prompt embarqué)."""
        row = self._conn.execute("SELECT name, text FROM prompts WHERE active=1 LIMIT 1").fetchone()
        return (str(row["name"]), str(row["text"])) if row else None

    def delete_prompt(self, name: str) -> bool:
        cur = self._conn.execute("DELETE FROM prompts WHERE name=?", (name,))
        self._conn.commit()
        return cur.rowcount == 1

    # ------------------------------------------------------------------ reviews
    def set_review(
        self,
        file_id: int,
        status: str,
        *,
        comment: str = "",
        reviewer: str = "",
        corrected_security: str | None = None,
        corrected_rgpd: str | None = None,
        corrected_retention_years: int | None = None,
    ) -> None:
        """Statut de vérification humaine d'un fichier (`to_review` / `validated` / `corrected`)."""
        if status not in REVIEW_STATUSES:
            raise ValueError(f"statut de revue inconnu : {status}")
        self._conn.execute(
            """INSERT INTO reviews(file_id, status, comment, corrected_security, corrected_rgpd,
               corrected_retention_years, reviewer, updated_at) VALUES(?,?,?,?,?,?,?,?)
               ON CONFLICT(file_id) DO UPDATE SET status=excluded.status, comment=excluded.comment,
               corrected_security=excluded.corrected_security, corrected_rgpd=excluded.corrected_rgpd,
               corrected_retention_years=excluded.corrected_retention_years,
               reviewer=excluded.reviewer, updated_at=excluded.updated_at""",
            (
                file_id,
                status,
                comment,
                corrected_security,
                corrected_rgpd,
                corrected_retention_years,
                reviewer,
                _now(),
            ),
        )
        self._conn.commit()

    def review_counts(self) -> dict[str, int]:
        out = dict.fromkeys(REVIEW_STATUSES, 0)
        for r in self._conn.execute("SELECT status, COUNT(*) AS n FROM reviews GROUP BY status"):
            out[str(r["status"])] = int(r["n"])
        return out

    # ------------------------------------------------------------------ stats
    def counts(self) -> dict[str, int]:
        counts = {s.value: 0 for s in FileStatus}
        for r in self._conn.execute("SELECT status, COUNT(*) AS n FROM files GROUP BY status"):
            counts[r["status"]] = int(r["n"])
        counts["files"] = sum(counts[s.value] for s in FileStatus)
        counts["analyses"] = int(self._conn.execute("SELECT COUNT(*) FROM analyses").fetchone()[0])
        for status in BlockStatus:
            counts[f"blocks_{status.value}"] = int(
                self._conn.execute(
                    "SELECT COUNT(*) FROM blocks WHERE status=?", (status.value,)
                ).fetchone()[0]
            )
        return counts

    def classification_summary(self) -> dict[str, dict[str, int]]:
        out: dict[str, dict[str, int]] = {}
        for column, key in (
            ("security_classification", "security"),
            ("rgpd_risk_level", "rgpd"),
            ("finance_document_type", "finance"),
            ("legal_contract_type", "legal"),
        ):
            out[key] = {
                r[0]: int(r[1])
                for r in self._conn.execute(
                    f"SELECT {column}, COUNT(*) FROM analyses GROUP BY {column}"
                )  # noqa: S608 — colonnes internes
            }
        return out
