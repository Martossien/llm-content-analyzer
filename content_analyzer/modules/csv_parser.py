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
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS fichiers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
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
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_status ON fichiers(status)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_fast_hash ON fichiers(fast_hash)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_priority_score ON fichiers(priority_score DESC)"
        )
        conn.commit()

    def validate_csv_format(self, df: pd.DataFrame) -> List[str]:
        """Valide la présence des colonnes obligatoires."""

        errors: List[str] = []
        for col in self.required_columns:
            if col not in df.columns:
                errors.append(f"Missing column: {col}")
        return errors

    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Nettoie le DataFrame issu du CSV."""

        df = df.copy()
        if "UNCDirectory" in df.columns:
            df["UNCDirectory"] = df["UNCDirectory"].astype(str).str.replace(
                r"\\{2,}", r"\\",
                regex=True,
            )
        for col in ["Readable", "Writeable", "Deletable"]:
            if col in df.columns:
                df[col] = df[col].astype(str).str.lower().map({"true": True, "false": False})
        for col in ["CreationTime", "LastWriteTime", "AccessTime"]:
            if col in df.columns:
                df[col] = (
                    pd.to_datetime(df[col], errors="coerce", dayfirst=True)
                    .dt.strftime("%Y-%m-%d %H:%M:%S")
                )
        if "Extension" in df.columns:
            df["Extension"] = df["Extension"].fillna("unknown")
        if "Owner" in df.columns:
            df["Owner"] = df["Owner"].replace("<ERROR_5>", None)
        return df

    def transform_metadata(self, row: pd.Series) -> Dict[str, Any]:
        """Transforme une ligne du CSV en dict compatible SQLite."""

        path = f"{row.get('UNCDirectory', '')}/{row.get('Name', '')}"
        return {
            "path": path.replace("\\", "/"),
            "file_size": int(row.get("FileSize", 0)),
            "owner": row.get("Owner"),
            "fast_hash": row.get("FastHash"),
            "access_time": row.get("AccessTime"),
            "file_attributes": row.get("FileAttributes"),
            "file_signature": row.get("FileSignature"),
            "last_modified": row.get("LastWriteTime") or row.get("CreationTime"),
        }

    def parse_csv(
        self,
        csv_file: Path,
        db_file: Path,
        chunk_size: int = 10000,
    ) -> Dict[str, Any]:
        """Parse le CSV et importe dans SQLite."""

        chunk = chunk_size or self.chunk_size
        start = time.perf_counter()
        total_files = 0
        imported_files = 0
        errors: List[str] = []
        validation_stats = {"invalid_rows": 0}

        conn = sqlite3.connect(db_file)
        self._ensure_schema(conn)

        try:
            for df in pd.read_csv(csv_file, chunksize=chunk, encoding=self.encoding):
                df = self._clean_dataframe(df)
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
                                path, file_size, owner, fast_hash, access_time,
                                file_attributes, file_signature, last_modified
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
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
