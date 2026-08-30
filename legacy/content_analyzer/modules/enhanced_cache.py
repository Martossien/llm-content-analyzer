from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, Optional

from content_analyzer.utils.sqlite_utils import SQLiteConnectionPool


class EnhancedResultsCache:
    """Cache multi-niveaux avec invalidation intelligente."""

    def __init__(self, db_path: Path, max_memory_mb: int = 512, pool_size: int = 2) -> None:
        self.l1_memory: Dict[str, Any] = {}
        self.l2_filters: Dict[str, Dict[str, Any]] = {}
        self.l3_disk = SQLiteConnectionPool(db_path, pool_size)
        self.access_times: Dict[str, float] = {}
        self.max_memory = max_memory_mb * 1024 * 1024
        self._ensure_schema(Path(db_path))

    # ------------------------------------------------------------------
    def _ensure_schema(self, db_path: Path) -> None:
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS cache (key TEXT PRIMARY KEY, data TEXT, access REAL)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_cache_access ON cache(access)"
            )
            conn.commit()

    # ------------------------------------------------------------------
    def _update_access_time(self, key: str) -> None:
        self.access_times[key] = time.time()
        with self.l3_disk.get() as conn:
            conn.execute("UPDATE cache SET access=? WHERE key=?", (self.access_times[key], key))
            conn.commit()

    # ------------------------------------------------------------------
    def get_with_filters(self, key: str, filters: Dict[str, Any]) -> Optional[Any]:
        filter_key = self._generate_filter_key(filters)
        composite_key = f"{key}_{filter_key}"

        if composite_key in self.l1_memory:
            self._update_access_time(composite_key)
            return self.l1_memory[composite_key]

        if filter_key in self.l2_filters and composite_key in self.l2_filters[filter_key]:
            value = self.l2_filters[filter_key][composite_key]
            self.l1_memory[composite_key] = value
            self._update_access_time(composite_key)
            return value

        with self.l3_disk.get() as conn:
            row = conn.execute("SELECT data FROM cache WHERE key=?", (composite_key,)).fetchone()
            if row:
                value = json.loads(row[0])
                self.put_with_filters(key, value, filters)
                return value
        return None

    # ------------------------------------------------------------------
    def put_with_filters(self, key: str, data: Any, filters: Dict[str, Any]) -> None:
        filter_key = self._generate_filter_key(filters)
        composite_key = f"{key}_{filter_key}"

        self.l1_memory[composite_key] = data
        self.l2_filters.setdefault(filter_key, {})[composite_key] = data
        self.access_times[composite_key] = time.time()

        self._evict_lru_entries()

        with self.l3_disk.get() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO cache (key, data, access) VALUES (?, ?, ?)",
                (composite_key, json.dumps(data), self.access_times[composite_key]),
            )
            conn.commit()

    # ------------------------------------------------------------------
    def invalidate_by_pattern(self, pattern: str) -> None:
        import re

        regex = re.compile(pattern)
        keys_to_delete = [k for k in list(self.l1_memory.keys()) if regex.search(k)]
        for k in keys_to_delete:
            self.l1_memory.pop(k, None)
            self.access_times.pop(k, None)
        for fk in list(self.l2_filters.keys()):
            for k in list(self.l2_filters[fk].keys()):
                if regex.search(k):
                    del self.l2_filters[fk][k]
            if not self.l2_filters[fk]:
                del self.l2_filters[fk]
        like_pattern = f"%{pattern.replace('*', '%')}%"
        with self.l3_disk.get() as conn:
            conn.execute("DELETE FROM cache WHERE key LIKE ?", (like_pattern,))
            conn.commit()

    # ------------------------------------------------------------------
    def _generate_filter_key(self, filters: Dict[str, Any]) -> str:
        return hashlib.md5(json.dumps(filters, sort_keys=True).encode()).hexdigest()

    def _evict_lru_entries(self) -> None:
        current_size = sum(len(json.dumps(v)) for v in self.l1_memory.values())
        if current_size <= self.max_memory:
            return
        # remove oldest
        sorted_keys = sorted(self.access_times.items(), key=lambda x: x[1])
        for key, _ in sorted_keys:
            self.l1_memory.pop(key, None)
            self.access_times.pop(key, None)
            if sum(len(json.dumps(v)) for v in self.l1_memory.values()) <= self.max_memory:
                break

