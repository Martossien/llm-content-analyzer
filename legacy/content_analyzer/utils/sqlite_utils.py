import sqlite3
from pathlib import Path
from queue import Queue
from contextlib import contextmanager
from typing import Iterator


class SQLiteConnectionManager:
    """Context manager for SQLite connections."""

    def __init__(self, db_path: Path, check_same_thread: bool = False) -> None:
        self.db_path = str(db_path)
        self.check_same_thread = check_same_thread
        self.conn: sqlite3.Connection | None = None

    def __enter__(self) -> sqlite3.Connection:
        self.conn = sqlite3.connect(self.db_path, check_same_thread=self.check_same_thread)
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self.conn:
            self.conn.close()
            self.conn = None


class SQLiteConnectionPool:
    """Simple thread-safe connection pool."""

    def __init__(self, db_path: Path, pool_size: int = 5) -> None:
        self.db_path = str(db_path)
        self.pool: Queue[sqlite3.Connection] = Queue(maxsize=pool_size)
        for _ in range(pool_size):
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self.pool.put(conn)

    @contextmanager
    def get(self) -> Iterator[sqlite3.Connection]:
        conn = self.pool.get()
        try:
            yield conn
        finally:
            self.pool.put(conn)

    def close(self) -> None:
        while not self.pool.empty():
            conn = self.pool.get_nowait()
            conn.close()

