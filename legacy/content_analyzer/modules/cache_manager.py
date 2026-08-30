import json
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, Optional
import threading
from threading import Timer
from content_analyzer.modules.duplicate_detector import DuplicateDetector, FileInfo

from content_analyzer.utils import (
    create_enhanced_duplicate_key,
    SQLiteConnectionPool,
)


class CacheManager:
    """Cache SQLite intelligent basé sur FastHash."""

    def __init__(
        self,
        db_path: Path,
        ttl_hours: int = 168,
        max_size_mb: int = 1024,
        pool_size: int = 5,
    ) -> None:
        self.db_path = db_path
        self.ttl_hours = ttl_hours
        self.max_size_mb = max_size_mb
        self._lock = threading.RLock()
        self._pool = SQLiteConnectionPool(db_path, pool_size)
        self._ensure_schema()
        self._cleanup_timer: Optional[Timer] = None
        self.detector = DuplicateDetector()

    def __del__(self) -> None:
        if hasattr(self, "_pool"):
            self._pool.close()
        if self._cleanup_timer:
            self._cleanup_timer.cancel()

    def close(self) -> None:
        """Cancel scheduled cleanup and close connections."""
        if self._cleanup_timer:
            self._cleanup_timer.cancel()
            self._cleanup_timer = None
        self._pool.close()

    def force_close_all_connections_windows_safe(self) -> None:
        """Ferme toutes les connexions cache de manière sûre pour Windows.
        
        Cette méthode est spécifiquement conçue pour résoudre les WinError 32
        lors du clear cache sur Windows.
        """
        import time
        import gc
        import platform
        import logging
        
        logger = logging.getLogger(__name__)
        logger.info("Closing all cache connections for Windows-safe maintenance")
        
        try:
            # 1. Cancel cleanup timer first
            if self._cleanup_timer:
                self._cleanup_timer.cancel()
                self._cleanup_timer = None
            
            # 2. Close connection pool
            if hasattr(self, "_pool"):
                self._pool.close()
            
            # 3. Force garbage collection
            gc.collect()
            
            # 4. Windows-specific waiting
            if platform.system() == "Windows":
                time.sleep(1.0)  # Windows needs more time
            else:
                time.sleep(0.1)  # Linux/macOS is faster
            
            logger.info("All cache connections closed successfully")
            
        except Exception as e:
            logger.error(f"Error during cache connection cleanup: {e}")
            # Continue anyway

    def __enter__(self) -> "CacheManager":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    from contextlib import contextmanager

    @contextmanager
    def _connection(self) -> "sqlite3.Connection":
        with self._pool.get() as conn:
            yield conn

    def _ensure_schema(self) -> None:
        with self._lock, self._connection() as conn:
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
                    cursor.execute(
                        f"ALTER TABLE cache_prompts ADD COLUMN {col} TEXT"
                    )

            conn.commit()

    def get_cached_result(
        self, fast_hash: str, prompt_hash: str, file_size: Optional[int] = None
    ) -> Optional[Dict[str, Any]]:
        info = FileInfo(0, "", fast_hash, file_size or 0)
        if self.detector.should_ignore_file(info)[0]:
            return None
        with self._lock, self._connection() as conn:
            enhanced = create_enhanced_duplicate_key(fast_hash, file_size)
            key = f"{enhanced}_{prompt_hash}"
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
            if row is None and file_size is not None:
                legacy_key = f"{fast_hash}_{prompt_hash}"
                row = cursor.execute(
                    "SELECT response_content, document_resume, raw_llm_response, hits_count, ttl_expiry FROM cache_prompts WHERE cache_key = ?",
                    (legacy_key,),
                ).fetchone()
                key = legacy_key if row else key
                if row:
                    expiry = row[4]
                    if not (expiry and time.time() > float(expiry)):
                        conn.execute(
                            "UPDATE cache_prompts SET hits_count = hits_count + 1 WHERE cache_key = ?",
                            (legacy_key,),
                        )
                        conn.commit()
                        result = {
                            "analysis_data": json.loads(row[0]),
                            "resume": row[1] or "",
                            "raw_response": row[2] or "",
                        }

            return result

    def store_result(
        self,
        fast_hash: str,
        prompt_hash: str,
        result: Dict[str, Any],
        document_resume: str = "",
        raw_llm_response: str = "",
        file_size: Optional[int] = None,
    ) -> None:
        info = FileInfo(0, "", fast_hash, file_size or result.get("file_size", 0))
        if self.detector.should_ignore_file(info)[0]:
            return
        with self._lock, self._connection() as conn:
            enhanced = create_enhanced_duplicate_key(fast_hash, file_size)
            key = f"{enhanced}_{prompt_hash}"
            expiry = time.time() + self.ttl_hours * 3600
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
                    file_size if file_size is not None else result.get("file_size"),
                    document_resume,
                    raw_llm_response,
                ),
            )
            conn.commit()

    def cleanup_expired(self) -> int:
        with self._lock, self._connection() as conn:
            now = time.time()
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM cache_prompts WHERE ttl_expiry IS NOT NULL AND ttl_expiry <= ?",
                (now,),
            )
            deleted = cursor.rowcount
            conn.commit()
            return deleted

    def cleanup_expired_and_oversized(self) -> Dict[str, int]:
        """Nettoie les entrées expirées et si la taille dépasse la limite."""
        with self._lock, self._connection() as conn:
            stats = {"expired_deleted": 0, "oversized_deleted": 0}
            stats["expired_deleted"] = self.cleanup_expired()

            size_mb = Path(self.db_path).stat().st_size / (1024 * 1024)
            if size_mb <= self.max_size_mb:
                return stats

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
            return stats

    def schedule_automatic_cleanup(self) -> None:
        """Planifie un nettoyage automatique quotidien."""

        def _cleanup() -> None:
            try:
                self.cleanup_expired_and_oversized()
            finally:
                self._cleanup_timer = Timer(24 * 3600, _cleanup)
                self._cleanup_timer.daemon = True
                self._cleanup_timer.start()

        self._cleanup_timer = Timer(24 * 3600, _cleanup)
        self._cleanup_timer.daemon = True
        self._cleanup_timer.start()

    def get_stats(self) -> Dict[str, Any]:
        with self._lock, self._connection() as conn:
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
