import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional


class DBManager:
    """Gestionnaire SQLite pour stocker les analyses."""

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def _ensure_schema(self) -> None:
        conn = self._connect()
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
            "processing_time_ms": "INTEGER",
            "api_tokens_used": "INTEGER",
            "created_at": "TIMESTAMP",
            "document_resume": "TEXT",
            "llm_response_complete": "TEXT",
        }
        for col, col_type in expected.items():
            if col not in existing_cols:
                cursor.execute(f"ALTER TABLE reponses_llm ADD COLUMN {col} {col_type}")

        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_confidence ON reponses_llm(confidence_global DESC)"
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
        conn.close()

    def store_analysis_result(
        self,
        file_id: int,
        task_id: str,
        llm_response: Dict[str, Any],
        document_resume: str,
        llm_response_complete: str,
    ) -> None:
        conn = self._connect()
        conn.execute(
            """
            INSERT INTO reponses_llm (
                fichier_id, task_id, security_analysis, rgpd_analysis,
                finance_analysis, legal_analysis, confidence_global,
                processing_time_ms, api_tokens_used,
                document_resume, llm_response_complete
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                file_id,
                task_id,
                json.dumps(llm_response.get("security")),
                json.dumps(llm_response.get("rgpd")),
                json.dumps(llm_response.get("finance")),
                json.dumps(llm_response.get("legal")),
                llm_response.get("confidence", 0),
                llm_response.get("processing_time_ms", 0),
                llm_response.get("api_tokens_used", 0),
                document_resume,
                llm_response_complete,
            ),
        )
        conn.commit()
        conn.close()

    def get_pending_files(
        self, limit: int = 100, priority_threshold: int = 0
    ) -> List[Dict[str, Any]]:
        conn = self._connect()
        cursor = conn.cursor()
        rows = cursor.execute(
            """
            SELECT * FROM fichiers
            WHERE status = 'pending' AND priority_score >= ?
            ORDER BY priority_score DESC
            LIMIT ?
            """,
            (priority_threshold, limit),
        ).fetchall()
        columns = [desc[0] for desc in cursor.description]
        conn.close()
        return [dict(zip(columns, row)) for row in rows]

    def update_file_status(
        self, file_id: int, status: str, error_message: Optional[str] = None
    ) -> None:
        conn = self._connect()
        conn.execute(
            "UPDATE fichiers SET status = ?, exclusion_reason = ? WHERE id = ?",
            (status, error_message, file_id),
        )
        conn.commit()
        conn.close()

    def get_processing_stats(self) -> Dict[str, Any]:
        conn = self._connect()
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
        conn.close()
        return {
            "total_files": total,
            "pending": pending,
            "processing": processing,
            "completed": completed,
            "errors": errors,
            "avg_processing_time": float(avg_time_row or 0.0),
        }
