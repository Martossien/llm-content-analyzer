import logging
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, List
import re

import pandas as pd
import yaml

logger = logging.getLogger(__name__)


class CSVParser:
    """Parse les fichiers CSV SMBeagle vers SQLite."""

    def __init__(self, config_path: Path) -> None:
        with open(config_path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
        module_cfg = cfg.get("modules", {}).get("csv_parser", {})
        self.chunk_size = module_cfg.get("chunk_size", 10000)
        self.validation_strict = module_cfg.get("validation_strict", True)
        self.encoding = module_cfg.get("encoding", "utf-8")
        self.required_columns = [
            "Name",
            "Host",
            "Extension",
            "Username",
            "Hostname",
            "UNCDirectory",
            "CreationTime",
            "LastWriteTime",
            "Readable",
            "Writeable",
            "Deletable",
            "DirectoryType",
            "Base",
            "FileSize",
            "Owner",
            "FastHash",
            "AccessTime",
            "FileAttributes",
            "FileSignature",
        ]

    # Schema creation
    def _ensure_schema(self, conn: sqlite3.Connection) -> None:
        """Create or migrate the fichiers table to store all SMBeagle columns."""

        expected_columns = {
            "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
            "name": "TEXT NOT NULL",
            "host": "TEXT",
            "extension": "TEXT",
            "username": "TEXT",
            "hostname": "TEXT",
            "unc_directory": "TEXT",
            "creation_time": "TEXT",
            "last_write_time": "TEXT",
            "readable": "BOOLEAN",
            "writeable": "BOOLEAN",
            "deletable": "BOOLEAN",
            "directory_type": "TEXT",
            "base": "TEXT",
            "path": "TEXT UNIQUE NOT NULL",
            "file_size": "INTEGER NOT NULL",
            "owner": "TEXT",
            "fast_hash": "TEXT",
            "access_time": "TEXT",
            "file_attributes": "TEXT",
            "file_signature": "TEXT",
            "last_modified": "TEXT NOT NULL",
            "status": "TEXT DEFAULT 'pending'",
            "exclusion_reason": "TEXT",
            "priority_score": "INTEGER DEFAULT 0",
            "special_flags": "TEXT",
            "processed_at": "TIMESTAMP",
        }

        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS fichiers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                host TEXT,
                extension TEXT,
                username TEXT,
                hostname TEXT,
                unc_directory TEXT,
                creation_time TEXT,
                last_write_time TEXT,
                readable BOOLEAN,
                writeable BOOLEAN,
                deletable BOOLEAN,
                directory_type TEXT,
                base TEXT,
                path TEXT UNIQUE NOT NULL,
                file_size INTEGER NOT NULL,
                owner TEXT,
                fast_hash TEXT,
                access_time TEXT,
                file_attributes TEXT,
                file_signature TEXT,
                last_modified TEXT NOT NULL,
                status TEXT DEFAULT 'pending',
                exclusion_reason TEXT,
                priority_score INTEGER DEFAULT 0,
                special_flags TEXT,
                processed_at TIMESTAMP
            )
        """
        )

        # Migration for existing databases: add missing columns
        cursor.execute("PRAGMA table_info(fichiers)")
        existing_cols = {row[1] for row in cursor.fetchall()}
        for col, col_type in expected_columns.items():
            if col not in existing_cols:
                cursor.execute(f"ALTER TABLE fichiers ADD COLUMN {col} {col_type}")

        # Indexes for performance
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_status ON fichiers(status)")
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_fast_hash ON fichiers(fast_hash)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_priority_score ON fichiers(priority_score DESC)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_extension ON fichiers(extension)"
        )
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_host ON fichiers(host)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_username ON fichiers(username)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_name ON fichiers(name)")
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_unc_directory ON fichiers(unc_directory)"
        )
        conn.commit()

    def validate_csv_format(self, df: pd.DataFrame) -> List[str]:
        """Valide la présence des colonnes obligatoires."""

        errors: List[str] = []
        for col in self.required_columns:
            if col not in df.columns:
                errors.append(f"Missing column: {col}")
        return errors

    def transform_metadata(self, row: pd.Series) -> Dict[str, Any]:
        """Transforme une ligne CSV en dict complet pour SQLite."""

        unc_dir = str(row.get("UNCDirectory", "")).strip()
        name = str(row.get("Name", "")).strip()

        if unc_dir.endswith("\\") or unc_dir.endswith("/"):
            path = f"{unc_dir}{name}"
        else:
            sep = "\\" if "\\" in unc_dir else "/"
            path = f"{unc_dir}{sep}{name}"

        if len(path) > 32767:
            logger.warning("Path très long tronqué: %s caractères", len(path))
            path = path[:32767]

        return {
            "name": name[:255],
            "host": str(row.get("Host", "")).strip(),
            "extension": str(row.get("Extension", "")).strip().lower(),
            "username": str(row.get("Username", "")).strip(),
            "hostname": str(row.get("Hostname", "")).strip(),
            "unc_directory": unc_dir,
            "creation_time": str(row.get("CreationTime", "")).strip(),
            "last_write_time": str(row.get("LastWriteTime", "")).strip(),
            "readable": bool(row.get("Readable", False)),
            "writeable": bool(row.get("Writeable", False)),
            "deletable": bool(row.get("Deletable", False)),
            "directory_type": str(row.get("DirectoryType", "")).strip(),
            "base": str(row.get("Base", "")).strip(),
            "path": path,
            "file_size": int(row.get("FileSize", 0)),
            "owner": str(row.get("Owner", "") or "").strip(),
            "fast_hash": str(row.get("FastHash", "") or "").strip(),
            "access_time": str(row.get("AccessTime", "") or "").strip(),
            "file_attributes": str(row.get("FileAttributes", "") or "").strip(),
            "file_signature": str(row.get("FileSignature", "") or "").strip(),
            "last_modified": str(
                row.get("LastWriteTime", "") or row.get("CreationTime", "") or ""
            ).strip(),
        }

    def parse_csv(
        self,
        csv_file: Path,
        db_file: Path,
        chunk_size: int = 10000,
    ) -> Dict[str, Any]:
        """Parse le CSV et importe dans SQLite sans altérer les chemins."""

        chunk = chunk_size or self.chunk_size
        start = time.perf_counter()
        total_files = 0
        imported_files = 0
        errors: List[str] = []
        validation_stats = {"invalid_rows": 0}

        conn = sqlite3.connect(db_file)
        self._ensure_schema(conn)

        try:
            for df in pd.read_csv(
                csv_file,
                chunksize=chunk,
                encoding=self.encoding,
                encoding_errors="replace",
            ):
                total_files += len(df)
                validation_errors = self.validate_csv_format(df)
                if validation_errors:
                    errors.extend(validation_errors)
                    if self.validation_strict:
                        conn.close()
                        return {
                            "total_files": total_files,
                            "imported_files": imported_files,
                            "errors": errors,
                            "processing_time": time.perf_counter() - start,
                            "validation_stats": validation_stats,
                        }
                for _, row in df.iterrows():
                    try:
                        data = self.transform_metadata(row)
                        conn.execute(
                            """
                            INSERT OR IGNORE INTO fichiers (
                                name, host, extension, username, hostname,
                                unc_directory, creation_time, last_write_time,
                                readable, writeable, deletable, directory_type,
                                base, path, file_size, owner, fast_hash,
                                access_time, file_attributes, file_signature,
                                last_modified
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                data["name"],
                                data["host"],
                                data["extension"],
                                data["username"],
                                data["hostname"],
                                data["unc_directory"],
                                data["creation_time"],
                                data["last_write_time"],
                                data["readable"],
                                data["writeable"],
                                data["deletable"],
                                data["directory_type"],
                                data["base"],
                                data["path"],
                                data["file_size"],
                                data["owner"],
                                data["fast_hash"],
                                data["access_time"],
                                data["file_attributes"],
                                data["file_signature"],
                                data["last_modified"],
                            ),
                        )
                        imported_files += 1
                    except Exception as exc:  # pragma: no cover - unexpected
                        logger.warning("Erreur lors de l'insertion: %s", exc)
                        validation_stats["invalid_rows"] += 1
                        errors.append(str(exc))
                conn.commit()
        finally:
            conn.close()

        processing_time = time.perf_counter() - start
        return {
            "total_files": total_files,
            "imported_files": imported_files,
            "errors": errors,
            "processing_time": processing_time,
            "validation_stats": validation_stats,
        }
