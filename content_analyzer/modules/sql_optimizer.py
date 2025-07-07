from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterator, List, Tuple

from content_analyzer.utils.sqlite_utils import SQLiteConnectionManager


class SQLQueryOptimizer:
    """Optimiseur de requêtes SQLite avec techniques avancées 2024"""

    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path)
        self.chunk_size = 10000

    def _connect(self) -> SQLiteConnectionManager:
        return SQLiteConnectionManager(self.db_path, check_same_thread=False)

    # ------------------------------------------------------------------
    def get_paginated_files_optimized(
        self, filters: Dict[str, Any], cursor_id: int = 0, limit: int = 1000
    ) -> List[Tuple]:
        """Pagination par curseur plus performante que OFFSET."""
        query = "SELECT * FROM fichiers WHERE id > ?"
        params: List[Any] = [cursor_id]
        for key, value in filters.items():
            query += f" AND {key} = ?"
            params.append(value)
        query += " ORDER BY id LIMIT ?"
        params.append(limit)

        with self._connect() as conn:
            cur = conn.cursor()
            rows = cur.execute(query, params).fetchall()
        return rows

    # ------------------------------------------------------------------
    def get_duplicate_files_chunked(
        self, filters: Dict[str, Any], chunk_size: int = 10000
    ) -> Iterator[List[int]]:
        """Yield des listes d'IDs de fichiers en doublon par chunks."""
        base_query = (
            "SELECT id FROM fichiers WHERE fast_hash IN ("
            "SELECT fast_hash FROM fichiers WHERE fast_hash IS NOT NULL GROUP BY fast_hash, file_size HAVING COUNT(*) > 1)"
        )
        params: List[Any] = []
        for key, value in filters.items():
            base_query += f" AND {key} = ?"
            params.append(value)
        base_query += " ORDER BY id"

        with self._connect() as conn:
            cur = conn.cursor()
            offset = 0
            while True:
                query = f"{base_query} LIMIT ? OFFSET ?"
                rows = cur.execute(query, params + [chunk_size, offset]).fetchall()
                if not rows:
                    break
                yield [row[0] for row in rows]
                offset += chunk_size

    # ------------------------------------------------------------------
    @staticmethod
    def get_specialized_index_definitions() -> List[Tuple[str, str]]:
        """Return SQL definitions for specialized indexes."""
        return [
            (
                "CREATE INDEX IF NOT EXISTS idx_gui_analytics_composite ON fichiers(status, file_size, last_modified, fast_hash, extension)",
                "idx_gui_analytics_composite",
            ),
            (
                "CREATE INDEX IF NOT EXISTS idx_duplicate_detection_enhanced ON fichiers(fast_hash, file_size) WHERE fast_hash IS NOT NULL",
                "idx_duplicate_detection_enhanced",
            ),
            (
                "CREATE INDEX IF NOT EXISTS idx_age_analysis ON fichiers(last_modified, creation_time) WHERE status='completed'",
                "idx_age_analysis",
            ),
            (
                "CREATE INDEX IF NOT EXISTS idx_size_analysis ON fichiers(file_size, extension) WHERE file_size > 0",
                "idx_size_analysis",
            ),
        ]

    # ------------------------------------------------------------------
    def execute_chunked_query(
        self, base_query: str, params: List[Any], chunk_size: int = 10000
    ) -> Iterator[List[Tuple]]:
        """Exécute une requête en renvoyant les résultats par morceaux."""
        with self._connect() as conn:
            cur = conn.cursor()
            offset = 0
            while True:
                query = f"{base_query} LIMIT ? OFFSET ?"
                rows = cur.execute(query, params + [chunk_size, offset]).fetchall()
                if not rows:
                    break
                yield rows
                offset += chunk_size
