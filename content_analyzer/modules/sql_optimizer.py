from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterator, List, Tuple

from content_analyzer.utils import SQLiteConnectionPool


class SQLQueryOptimizer:
    """Optimiseur SQLite avec keyset pagination et chunking."""

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.pool = SQLiteConnectionPool(db_path)
        self.chunk_size = 10000

    def get_paginated_files_optimized(
        self,
        filters: Dict[str, Any] | None = None,
        cursor_id: int = 0,
        limit: int = 1000,
    ) -> List[Tuple]:
        """Retourne une page de fichiers en utilisant l'approche keyset."""
        filters = filters or {}
        query = "SELECT * FROM fichiers WHERE id > ?"
        params: List[Any] = [cursor_id]
        if "status" in filters:
            query += " AND status = ?"
            params.append(filters["status"])
        query += " ORDER BY id LIMIT ?"
        params.append(limit)
        with self.pool.get() as conn:
            cur = conn.execute(query, params)
            return cur.fetchall()

    def get_duplicate_files_chunked(
        self,
        filters: Dict[str, Any] | None = None,
        chunk_size: int | None = None,
    ) -> Iterator[List[int]]:
        """Yield lists of duplicate file IDs by chunks."""
        filters = filters or {}
        chunk = chunk_size or self.chunk_size
        base = (
            "SELECT id FROM fichiers WHERE fast_hash IN ("
            "SELECT fast_hash FROM fichiers WHERE fast_hash != ''"
            " GROUP BY fast_hash HAVING COUNT(*) > 1)"
        )
        params: List[Any] = []
        if "status" in filters:
            base += " AND status = ?"
            params.append(filters["status"])
        base += " ORDER BY id"
        with self.pool.get() as conn:
            cur = conn.execute(base, params)
            while True:
                rows = cur.fetchmany(chunk)
                if not rows:
                    break
                yield [r[0] for r in rows]

    def create_specialized_indexes(self, conn: sqlite3.Connection) -> None:
        """Crée des indexes spécialisés pour accélérer la GUI analytics."""
        specialized_indexes = [
            (
                "CREATE INDEX IF NOT EXISTS idx_gui_analytics_composite "
                "ON fichiers(status, file_size, last_modified, fast_hash, "
                "extension)"
            ),
            (
                "CREATE INDEX IF NOT EXISTS idx_duplicate_detection_enhanced "
                "ON fichiers(fast_hash, file_size) "
                "WHERE fast_hash IS NOT NULL"
            ),
            (
                "CREATE INDEX IF NOT EXISTS idx_age_analysis "
                "ON fichiers(last_modified, creation_time) "
                "WHERE status='completed'"
            ),
            (
                "CREATE INDEX IF NOT EXISTS idx_size_analysis "
                "ON fichiers(file_size, extension) "
                "WHERE file_size > 0"
            ),
        ]
        for sql in specialized_indexes:
            try:
                conn.execute(sql)
            except sqlite3.OperationalError:
                continue

    def execute_chunked_query(
        self,
        base_query: str,
        params: List[Any] | Tuple[Any, ...],
        chunk_size: int | None = None,
    ) -> Iterator[List[Tuple]]:
        """Exécute une requête en renvoyant les résultats par blocs."""
        chunk = chunk_size or self.chunk_size
        with self.pool.get() as conn:
            cur = conn.execute(base_query, params)
            while True:
                rows = cur.fetchmany(chunk)
                if not rows:
                    break
                yield rows
