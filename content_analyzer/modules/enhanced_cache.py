from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from typing import Any, Dict, Optional

from content_analyzer.utils import SQLiteConnectionPool


class EnhancedResultsCache:
    """Cache multi-niveaux avec persistance SQLite."""

    def __init__(self, db_path: Path, max_memory_mb: int = 64) -> None:
        self.l1_memory: Dict[str, Any] = {}
        self.l2_filters: Dict[str, str] = {}
        self.access_times: Dict[str, float] = {}
        self.max_memory = max_memory_mb * 1024 * 1024
        self.pool = SQLiteConnectionPool(db_path)
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        with self.pool.get() as conn:
            conn.execute(
                (
                    "CREATE TABLE IF NOT EXISTS cache ("
                    "key TEXT PRIMARY KEY, data TEXT)"
                )
            )
            conn.commit()

    def _generate_filter_key(self, filters: Dict[str, Any]) -> str:
        payload = json.dumps(filters, sort_keys=True).encode()
        return hashlib.md5(payload).hexdigest()

    def get_with_filters(
        self, key: str, filters: Dict[str, Any]
    ) -> Optional[Any]:  # noqa: E501
        filter_key = self._generate_filter_key(filters)
        comp_key = f"{key}_{filter_key}"
        if comp_key in self.l1_memory:
            self.access_times[comp_key] = time.time()
            return self.l1_memory[comp_key]
        with self.pool.get() as conn:
            row = conn.execute(
                "SELECT data FROM cache WHERE key=?",
                (comp_key,),
            ).fetchone()
            if row:
                data = json.loads(row[0])
                self.put_with_filters(key, data, filters)
                return data
        return None

    def put_with_filters(
        self, key: str, data: Any, filters: Dict[str, Any]
    ) -> None:  # noqa: E501
        filter_key = self._generate_filter_key(filters)
        comp_key = f"{key}_{filter_key}"
        self.l1_memory[comp_key] = data
        self.access_times[comp_key] = time.time()
        self._evict_lru_entries()
        with self.pool.get() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO cache(key, data) VALUES (?, ?)",
                (comp_key, json.dumps(data)),
            )
            conn.commit()

    def invalidate_by_pattern(self, pattern: str) -> None:
        to_delete = [k for k in self.l1_memory if pattern in k]
        for key in to_delete:
            self.l1_memory.pop(key, None)
            self.access_times.pop(key, None)
        with self.pool.get() as conn:
            conn.execute(
                "DELETE FROM cache WHERE key LIKE ?",
                (f"%{pattern}%",),
            )
            conn.commit()

    def _evict_lru_entries(self) -> None:
        mem_usage = sum(len(json.dumps(v)) for v in self.l1_memory.values())
        while mem_usage > self.max_memory and self.access_times:
            oldest = min(self.access_times, key=self.access_times.get)
            mem_usage -= len(json.dumps(self.l1_memory.get(oldest, "")))
            self.l1_memory.pop(oldest, None)
            self.access_times.pop(oldest, None)
