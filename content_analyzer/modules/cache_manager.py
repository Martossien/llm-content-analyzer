import json
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, Optional


class CacheManager:
    """Cache SQLite intelligent basé sur FastHash."""

    def __init__(self, db_path: Path, ttl_hours: int = 168) -> None:
        self.db_path = db_path
        self.ttl_hours = ttl_hours
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
        conn.commit()
        conn.close()

    def get_cached_result(
        self, fast_hash: str, prompt_hash: str
    ) -> Optional[Dict[str, Any]]:
        key = f"{fast_hash}_{prompt_hash}"
        conn = self._connect()
        cursor = conn.cursor()
        row = cursor.execute(
            "SELECT response_content, hits_count, ttl_expiry FROM cache_prompts WHERE cache_key = ?",
            (key,),
        ).fetchone()
        result = None
        if row:
            expiry = row[2]
            if expiry and time.time() > float(expiry):
                conn.execute("DELETE FROM cache_prompts WHERE cache_key = ?", (key,))
                conn.commit()
            else:
                conn.execute(
                    "UPDATE cache_prompts SET hits_count = hits_count + 1 WHERE cache_key = ?",
                    (key,),
                )
                conn.commit()
                result = json.loads(row[0])
        conn.close()
        return result

    def store_result(
        self, fast_hash: str, prompt_hash: str, result: Dict[str, Any]
    ) -> None:
        key = f"{fast_hash}_{prompt_hash}"
        expiry = time.time() + self.ttl_hours * 3600
        conn = self._connect()
        conn.execute(
            """
            INSERT OR REPLACE INTO cache_prompts (
                cache_key, prompt_hash, response_content, ttl_expiry, file_size
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                key,
                prompt_hash,
                json.dumps(result),
                expiry,
                result.get("file_size"),
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
