import sys
from pathlib import Path
import sqlite3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))  # noqa: E402

from content_analyzer.modules.sql_optimizer import (  # noqa: E402
    SQLQueryOptimizer,
)


def setup_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    conn.execute(
        (
            "CREATE TABLE fichiers ("
            "id INTEGER PRIMARY KEY, fast_hash TEXT, status TEXT, "
            "file_size INTEGER, last_modified TEXT, creation_time TEXT, "
            "extension TEXT)"
        )
    )
    for i in range(1, 6):
        conn.execute(
            (
                "INSERT INTO fichiers (id, fast_hash, status, file_size, "
                "last_modified, creation_time, extension) VALUES "
                "(?, ?, 'completed', ?, '2024-01-01', '2024-01-01', '.txt')"
            ),
            (i, f"h{i % 2}", i * 1000),
        )
    conn.commit()
    conn.close()


def test_paginated_files(tmp_path):
    db_file = tmp_path / "t.db"
    setup_db(db_file)
    opt = SQLQueryOptimizer(db_file)
    rows = opt.get_paginated_files_optimized({}, cursor_id=2, limit=2)
    assert len(rows) == 2
    assert rows[0][0] == 3


def test_duplicate_chunked(tmp_path):
    db_file = tmp_path / "t.db"
    setup_db(db_file)
    opt = SQLQueryOptimizer(db_file)
    chunks = list(opt.get_duplicate_files_chunked({}))
    all_ids = [i for chunk in chunks for i in chunk]
    assert set(all_ids) == {1, 2, 3, 4, 5}


def test_execute_chunked(tmp_path):
    db_file = tmp_path / "t.db"
    setup_db(db_file)
    opt = SQLQueryOptimizer(db_file)
    query = "SELECT id FROM fichiers ORDER BY id"
    chunks = list(opt.execute_chunked_query(query, [], chunk_size=2))
    assert len(chunks) >= 2
    assert chunks[0][0][0] == 1
