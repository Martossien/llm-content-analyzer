import json
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, Optional


class CacheManager:
    """Cache SQLite intelligent basé sur FastHash."""

    def __init__(
        self, db_path: Path, ttl_hours: int = 168, max_size_mb: int = 1024
    ) -> None:
        self.db_path = db_path
        self.ttl_hours = ttl_hours
        self.max_size_mb = max_size_mb
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def _ensure_schema(self) -> None:
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS cache_prompts (
                cache_key TEXT PRIMARY KEY,
                prompt_hash TEXT NOT NULL,
                response_content TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                hits_count INTEGER DEFAULT 1,
                ttl_expiry TIMESTAMP,
                file_size INTEGER
            )
            """
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_ttl_expiry ON cache_prompts(ttl_expiry)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_hits_count ON cache_prompts(hits_count DESC)"
        )

        cursor.execute("PRAGMA table_info(cache_prompts)")
        existing_cols = [row[1] for row in cursor.fetchall()]
        for col in ["document_resume", "raw_llm_response"]:
            if col not in existing_cols:
                cursor.execute(f"ALTER TABLE cache_prompts ADD COLUMN {col} TEXT")

        conn.commit()
        conn.close()

    def get_cached_result(
        self, fast_hash: str, prompt_hash: str
    ) -> Optional[Dict[str, Any]]:
        key = f"{fast_hash}_{prompt_hash}"
        conn = self._connect()
        cursor = conn.cursor()
        row = cursor.execute(
            "SELECT response_content, document_resume, raw_llm_response, hits_count, ttl_expiry FROM cache_prompts WHERE cache_key = ?",
            (key,),
        ).fetchone()
        result = None
        if row:
            expiry = row[4]
            if expiry and time.time() > float(expiry):
                conn.execute("DELETE FROM cache_prompts WHERE cache_key = ?", (key,))
                conn.commit()
            else:
                conn.execute(
                    "UPDATE cache_prompts SET hits_count = hits_count + 1 WHERE cache_key = ?",
                    (key,),
                )
                conn.commit()
                result = {
                    "analysis_data": json.loads(row[0]),
                    "resume": row[1] or "",
                    "raw_response": row[2] or "",
                }
        conn.close()
        return result

    def store_result(
        self,
        fast_hash: str,
        prompt_hash: str,
        result: Dict[str, Any],
        document_resume: str = "",
        raw_llm_response: str = "",
    ) -> None:
        key = f"{fast_hash}_{prompt_hash}"
        expiry = time.time() + self.ttl_hours * 3600
        conn = self._connect()
        conn.execute(
            """
            INSERT OR REPLACE INTO cache_prompts (
                cache_key,
                prompt_hash,
                response_content,
                ttl_expiry,
                file_size,
                document_resume,
                raw_llm_response
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                key,
                prompt_hash,
                json.dumps(result),
                expiry,
                result.get("file_size"),
                document_resume,
                raw_llm_response,
            ),
        )
        conn.commit()
        conn.close()

    def cleanup_expired(self) -> int:
        now = time.time()
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(
            "DELETE FROM cache_prompts WHERE ttl_expiry IS NOT NULL AND ttl_expiry <= ?",
            (now,),
        )
        deleted = cursor.rowcount
        conn.commit()
        conn.close()
        return deleted

    def cleanup_expired_and_oversized(self) -> Dict[str, int]:
        """Nettoie les entrées expirées et si la taille dépasse la limite."""
        stats = {"expired_deleted": 0, "oversized_deleted": 0}

        stats["expired_deleted"] = self.cleanup_expired()

        size_mb = Path(self.db_path).stat().st_size / (1024 * 1024)
        if size_mb <= self.max_size_mb:
            return stats

        conn = self._connect()
        cursor = conn.cursor()
        to_remove = size_mb - self.max_size_mb
        cursor.execute(
            "SELECT cache_key, file_size FROM cache_prompts ORDER BY hits_count ASC, created_at ASC"
        )
        removed_mb = 0.0
        keys_to_delete = []
        for key, file_size in cursor.fetchall():
            removed_mb += (file_size or 0) / (1024 * 1024)
            keys_to_delete.append(key)
            if removed_mb >= to_remove:
                break
        for key in keys_to_delete:
            cursor.execute("DELETE FROM cache_prompts WHERE cache_key = ?", (key,))
        stats["oversized_deleted"] = len(keys_to_delete)
        conn.commit()
        conn.close()
        return stats

    def schedule_automatic_cleanup(self) -> None:
        """Planifie un nettoyage automatique quotidien."""
        from threading import Timer

        def _cleanup() -> None:
            try:
                self.cleanup_expired_and_oversized()
            finally:
                Timer(24 * 3600, _cleanup).start()

        Timer(24 * 3600, _cleanup).start()

    def get_stats(self) -> Dict[str, Any]:
        conn = self._connect()
        cursor = conn.cursor()
        total = cursor.execute("SELECT COUNT(*) FROM cache_prompts").fetchone()[0]
        hits = (
            cursor.execute("SELECT SUM(hits_count) FROM cache_prompts").fetchone()[0]
            or 0
        )
        oldest_row = cursor.execute(
            "SELECT MIN(created_at) FROM cache_prompts"
        ).fetchone()[0]
        size_bytes = Path(self.db_path).stat().st_size
        conn.close()
        hit_rate = 0.0
        if hits and total:
            hit_rate = (hits - total) / hits * 100
        return {
            "total_entries": total,
            "hit_rate": round(hit_rate, 2),
            "cache_size_mb": round(size_bytes / (1024 * 1024), 2),
            "oldest_entry": oldest_row,
            "cleanup_needed": total > 10000,
        }
