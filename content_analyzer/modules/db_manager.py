import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional
import threading
from threading import Timer
import logging

from content_analyzer.utils import SQLiteConnectionManager

logger = logging.getLogger(__name__)


class DBManager:
    """Gestionnaire SQLite pour stocker les analyses."""

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self._ensure_schema()
        self._maintenance_timer: Optional[Timer] = None
        # Schedule periodic maintenance without blocking
        try:
            self.schedule_maintenance()
        except Exception as exc:  # pragma: no cover - maintenance issues
            logger.warning("Failed to schedule DB maintenance: %s", exc)

    def __del__(self) -> None:
        self.close()

    def close(self) -> None:
        """Cancel scheduled maintenance timer."""
        if self._maintenance_timer:
            self._maintenance_timer.cancel()
            self._maintenance_timer = None

    def _connect(self) -> SQLiteConnectionManager:
        return SQLiteConnectionManager(self.db_path, check_same_thread=False)

    def __enter__(self) -> "DBManager":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS reponses_llm (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fichier_id INTEGER REFERENCES fichiers(id),
                task_id TEXT NOT NULL,
                security_analysis TEXT,
                rgpd_analysis TEXT,
                finance_analysis TEXT,
                legal_analysis TEXT,
                confidence_global INTEGER,
                processing_time_ms INTEGER,
                api_tokens_used INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_fichier_id ON reponses_llm(fichier_id)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_task_id ON reponses_llm(task_id)"
        )
        cursor.execute("PRAGMA table_info(reponses_llm)")
        existing_cols = [row[1] for row in cursor.fetchall()]
        expected = {
            "security_analysis": "TEXT",
            "rgpd_analysis": "TEXT",
            "finance_analysis": "TEXT",
            "legal_analysis": "TEXT",
            "confidence_global": "INTEGER",
            "security_confidence": "INTEGER DEFAULT 0",
            "rgpd_confidence": "INTEGER DEFAULT 0",
            "finance_confidence": "INTEGER DEFAULT 0",
            "legal_confidence": "INTEGER DEFAULT 0",
            "processing_time_ms": "INTEGER",
            "api_tokens_used": "INTEGER",
            "created_at": "TIMESTAMP",
            "document_resume": "TEXT",
            "llm_response_complete": "TEXT",
            "security_classification_cached": "TEXT",
            "rgpd_risk_cached": "TEXT",
            "finance_type_cached": "TEXT",
            "legal_type_cached": "TEXT",
        }
        for col, col_type in expected.items():
            if col not in existing_cols:
                cursor.execute(f"ALTER TABLE reponses_llm ADD COLUMN {col} {col_type}")

        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_confidence ON reponses_llm(confidence_global DESC)"
        )

        # Specialized indexes for GUI filtering performance
        try:
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_gui_status_priority ON fichiers(status, priority_score DESC, id DESC)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_gui_classification_filter ON reponses_llm(security_classification_cached, confidence_global DESC)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_gui_composite_main ON fichiers(status, last_modified DESC, id DESC)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_fast_hash_duplicates ON fichiers(fast_hash) WHERE fast_hash IS NOT NULL AND fast_hash != ''"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_duplicate_detection ON fichiers(fast_hash, file_size) WHERE fast_hash IS NOT NULL AND fast_hash != ''"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_covering_results ON fichiers(id, name, status, file_size, last_modified, path) WHERE status IN ('completed', 'error')"
            )
        except sqlite3.OperationalError:
            # Table may not have all columns in minimal test schema
            pass
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_security_class_cached ON reponses_llm(security_classification_cached)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_rgpd_risk_cached ON reponses_llm(rgpd_risk_cached)"
        )

        cursor.execute(
            """
            CREATE TRIGGER IF NOT EXISTS trigger_denormalize_security
                AFTER INSERT ON reponses_llm
                BEGIN
                    UPDATE reponses_llm
                    SET security_classification_cached = json_extract(security_analysis, '$.classification'),
                        rgpd_risk_cached = json_extract(rgpd_analysis, '$.risk_level'),
                        finance_type_cached = json_extract(finance_analysis, '$.document_type'),
                        legal_type_cached = json_extract(legal_analysis, '$.contract_type')
                    WHERE id = NEW.id;
                END;
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS metriques_performance (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                files_processed INTEGER,
                avg_processing_time REAL,
                cache_hit_rate REAL,
                api_success_rate REAL,
                memory_usage_mb INTEGER
            )
            """
        )
            conn.commit()

    def store_analysis_result(
        self,
        file_id: int,
        task_id: str,
        llm_response: Dict[str, Any],
        document_resume: str,
        llm_response_complete: str,
    ) -> None:
        with self._connect() as conn:
            if "confidence_global" not in llm_response:
                confs = [
                    llm_response.get("security_confidence", 0),
                    llm_response.get("rgpd_confidence", 0),
                    llm_response.get("finance_confidence", 0),
                    llm_response.get("legal_confidence", 0),
                ]
                valid = [c for c in confs if c]
                llm_response["confidence_global"] = int(sum(valid) / len(valid)) if valid else 0
            conn.execute(
                """
                INSERT INTO reponses_llm (
                fichier_id, task_id, security_analysis, rgpd_analysis,
                finance_analysis, legal_analysis,
                confidence_global, security_confidence, rgpd_confidence,
                finance_confidence, legal_confidence,
                processing_time_ms, api_tokens_used,
                document_resume, llm_response_complete
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                file_id,
                task_id,
                json.dumps(llm_response.get("security")),
                json.dumps(llm_response.get("rgpd")),
                json.dumps(llm_response.get("finance")),
                json.dumps(llm_response.get("legal")),
                llm_response.get("confidence_global", 0),
                llm_response.get("security_confidence", 0),
                llm_response.get("rgpd_confidence", 0),
                llm_response.get("finance_confidence", 0),
                llm_response.get("legal_confidence", 0),
                llm_response.get("processing_time_ms", 0),
                llm_response.get("api_tokens_used", 0),
                document_resume,
                llm_response_complete,
            ),
        )
            conn.commit()

    def get_pending_files(
        self,
        limit: Optional[int] = None,
        priority_threshold: int = 0,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """Return pending files ordered by priority.

        Args:
            limit: Maximum files to return. ``None`` means no limit.
            priority_threshold: Minimum priority score required.
            offset: Row offset for pagination.
        """
        with self._connect() as conn:
            cursor = conn.cursor()
            query = (
                "SELECT * FROM fichiers\n"
                "WHERE status = 'pending' AND priority_score >= ?\n"
                "ORDER BY priority_score DESC"
            )
            params = [priority_threshold]
            if limit is not None and limit > 0:
                query += " LIMIT ?"
                params.append(limit)
            if offset > 0:
                if "LIMIT" not in query:
                    query += " LIMIT -1"
                query += " OFFSET ?"
                params.append(offset)

            rows = cursor.execute(query, params).fetchall()
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in rows]

    def update_file_status(
        self, file_id: int, status: str, error_message: Optional[str] = None
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE fichiers SET status = ?, exclusion_reason = ? WHERE id = ?",
                (status, error_message, file_id),
            )
            conn.commit()

    def get_processing_stats(self) -> Dict[str, Any]:
        with self._connect() as conn:
            cursor = conn.cursor()
            total = cursor.execute("SELECT COUNT(*) FROM fichiers").fetchone()[0]
            pending = cursor.execute(
                "SELECT COUNT(*) FROM fichiers WHERE status = 'pending'"
            ).fetchone()[0]
            processing = cursor.execute(
                "SELECT COUNT(*) FROM fichiers WHERE status = 'processing'"
            ).fetchone()[0]
            completed = cursor.execute(
                "SELECT COUNT(*) FROM fichiers WHERE status = 'completed'"
            ).fetchone()[0]
            errors = cursor.execute(
                "SELECT COUNT(*) FROM fichiers WHERE status = 'error'"
            ).fetchone()[0]
            avg_time_row = cursor.execute(
                "SELECT AVG(processing_time_ms) FROM reponses_llm"
            ).fetchone()[0]
            return {
                "total_files": total,
                "pending": pending,
                "processing": processing,
                "completed": completed,
                "errors": errors,
                "avg_processing_time": float(avg_time_row or 0.0),
            }

    # ------------------------------------------------------------------
    # Performance optimization helpers
    # ------------------------------------------------------------------

    def _optimize_connection(self, conn: sqlite3.Connection) -> None:
        """Configure SQLite pragmas for large databases."""
        optimizations = [
            "PRAGMA journal_mode = WAL",
            "PRAGMA cache_size = 150000",
            "PRAGMA mmap_size = 629145600",
            "PRAGMA synchronous = NORMAL",
            "PRAGMA temp_store = MEMORY",
            "PRAGMA optimize",
        ]
        for pragma in optimizations:
            try:
                conn.execute(pragma)
            except sqlite3.OperationalError:
                # pragma may not be supported; continue
                pass
        conn.execute("PRAGMA wal_autocheckpoint = 32000")

    def optimize_database_performance(self) -> Dict[str, Any]:
        """Run periodic maintenance and return basic stats."""
        with self._connect() as conn:
            conn.execute("ANALYZE")
            conn.execute("PRAGMA optimize")
            conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
            stats = {
                "cache_hit_rate": conn.execute("PRAGMA cache_spill").fetchone()[0],
                "wal_size_mb": Path(f"{self.db_path}-wal").stat().st_size / 1024 / 1024
                if Path(f"{self.db_path}-wal").exists()
                else 0,
            }
            return stats

    def schedule_maintenance(self) -> None:
        """Schedule hourly optimization in a background thread."""

        def _task() -> None:
            try:
                self.optimize_database_performance()
            except Exception as exc:  # pragma: no cover - runtime issues
                logger.warning("Maintenance failed: %s", exc)
            finally:
                self._maintenance_timer = Timer(3600, _task)
                self._maintenance_timer.daemon = True
                self._maintenance_timer.start()

        self._maintenance_timer = Timer(3600, _task)
        self._maintenance_timer.daemon = True
        self._maintenance_timer.start()
