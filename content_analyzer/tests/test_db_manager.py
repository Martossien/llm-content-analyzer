import sqlite3
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from content_analyzer.modules.db_manager import DBManager


def setup_db(path: Path) -> DBManager:
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE fichiers (id INTEGER PRIMARY KEY, priority_score INTEGER, status TEXT, exclusion_reason TEXT)"
    )
    conn.commit()
    conn.close()
    return DBManager(path)


def test_store_analysis_result(tmp_path):
    db_file = tmp_path / "test.db"
    db = setup_db(db_file)
    conn = sqlite3.connect(db_file)
    conn.execute(
        "INSERT INTO fichiers (id, priority_score, status) VALUES (1, 10, 'pending')"
    )
    conn.commit()
    conn.close()
    db.store_analysis_result(
        1,
        "t1",
        {"security": {}, "rgpd": {}, "finance": {}, "legal": {}},
        "",
        "{}",
    )
    conn = sqlite3.connect(db_file)
    count = conn.execute("SELECT COUNT(*) FROM reponses_llm").fetchone()[0]
    conn.close()
    assert count == 1


def test_get_pending_files(tmp_path):
    db_file = tmp_path / "test.db"
    db = setup_db(db_file)
    conn = sqlite3.connect(db_file)
    conn.executemany(
        "INSERT INTO fichiers (id, priority_score, status) VALUES (?, ?, 'pending')",
        [(1, 50), (2, 20)],
    )
    conn.commit()
    conn.close()
    files = db.get_pending_files(limit=2, priority_threshold=10)
    assert len(files) == 2


def test_get_pending_files_no_limit(tmp_path):
    db_file = tmp_path / "test.db"
    db = setup_db(db_file)
    conn = sqlite3.connect(db_file)
    conn.executemany(
        "INSERT INTO fichiers (id, priority_score, status) VALUES (?, ?, 'pending')",
        [(1, 10), (2, 5), (3, 1)],
    )
    conn.commit()
    conn.close()
    files = db.get_pending_files(limit=None, priority_threshold=0)
    assert len(files) == 3


def test_dynamic_column_mapping(tmp_path):
    db_file = tmp_path / "test.db"
    conn = sqlite3.connect(db_file)
    conn.execute(
        "CREATE TABLE fichiers (id INTEGER PRIMARY KEY, path TEXT, status TEXT)"
    )
    conn.execute(
        "INSERT INTO fichiers (id, path, status) VALUES (1, '/tmp/a.txt', 'error')"
    )
    conn.commit()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM fichiers WHERE status='error'")
    row = cursor.fetchone()
    columns = [d[0] for d in cursor.description]
    conn.close()
    row_dict = dict(zip(columns, row))
    assert row_dict["path"] == "/tmp/a.txt"


def test_processing_stats(tmp_path):
    db_file = tmp_path / "test.db"
    db = setup_db(db_file)
    conn = sqlite3.connect(db_file)
    conn.executemany(
        "INSERT INTO fichiers (id, priority_score, status) VALUES (?, ?, ?)",
        [(1, 0, "pending"), (2, 0, "completed"), (3, 0, "error")],
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS reponses_llm (task_id TEXT, processing_time_ms INTEGER)"
    )
    conn.execute(
        "INSERT INTO reponses_llm (task_id, processing_time_ms) VALUES ('t', 100)"
    )
    conn.commit()
    conn.close()
    stats = db.get_processing_stats()
    assert stats["total_files"] == 3
    assert stats["completed"] == 1


def test_concurrent_access(tmp_path):
    db_file = tmp_path / "test.db"
    db1 = setup_db(db_file)
    db2 = DBManager(db_file)
    conn = sqlite3.connect(db_file)
    conn.execute(
        "INSERT INTO fichiers (id, priority_score, status, exclusion_reason) VALUES (1, 0, 'pending', '')"
    )
    conn.commit()
    conn.close()
    db1.update_file_status(1, "processing")
    db2.update_file_status(1, "completed")
    conn = sqlite3.connect(db_file)
    status = conn.execute("SELECT status FROM fichiers WHERE id=1").fetchone()[0]
    conn.close()
    assert status == "completed"
