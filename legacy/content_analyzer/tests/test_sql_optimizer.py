import sqlite3
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from content_analyzer.modules.sql_optimizer import SQLQueryOptimizer


def setup_db(tmp_path: Path) -> Path:
    db = tmp_path / "test.db"
    conn = sqlite3.connect(db)
    conn.execute(
        "CREATE TABLE fichiers (id INTEGER PRIMARY KEY AUTOINCREMENT, status TEXT, file_size INTEGER, fast_hash TEXT, last_modified TEXT, creation_time TEXT, extension TEXT)"
    )
    for i in range(1, 11):
        for _ in range(2):
            conn.execute(
                "INSERT INTO fichiers (status, file_size, fast_hash, extension) VALUES (?, ?, ?, ?)",
                ("completed", 100, f"dup{i}", ".txt"),
            )
    conn.commit()
    conn.close()
    return db


def test_paginated_query(tmp_path):
    db = setup_db(tmp_path)
    opt = SQLQueryOptimizer(db)
    rows = opt.get_paginated_files_optimized({}, cursor_id=0, limit=5)
    assert len(rows) == 5
    rows2 = opt.get_paginated_files_optimized({}, cursor_id=5, limit=5)
    assert rows2[0][0] == 6


def test_duplicate_chunked(tmp_path):
    db = setup_db(tmp_path)
    opt = SQLQueryOptimizer(db)
    chunks = list(opt.get_duplicate_files_chunked({}, chunk_size=5))
    assert chunks


def test_execute_chunked(tmp_path):
    db = setup_db(tmp_path)
    opt = SQLQueryOptimizer(db)
    query = "SELECT id FROM fichiers ORDER BY id"
    chunks = list(opt.execute_chunked_query(query, [], chunk_size=7))
    assert len(chunks) > 1


def test_sql_injection_prevention(tmp_path):
    db = setup_db(tmp_path)
    opt = SQLQueryOptimizer(db)
    malicious_filters = {
        "status = 1; --": "any",
        "id > 0) OR (1=1": "x",
        "'; DROP TABLE fichiers; --": "y",
    }
    rows = opt.get_paginated_files_optimized(malicious_filters, cursor_id=0, limit=5)
    assert rows
    conn = sqlite3.connect(db)
    count = conn.execute("SELECT COUNT(*) FROM fichiers").fetchone()[0]
    conn.close()
    assert count == 20


def test_column_whitelist_enforcement(tmp_path):
    db = setup_db(tmp_path)
    opt = SQLQueryOptimizer(db)
    invalid_filters = {"malicious_col": "x", "../etc/passwd": "y"}
    rows = opt.get_paginated_files_optimized(invalid_filters, cursor_id=0, limit=10)
    assert len(rows) == 10
