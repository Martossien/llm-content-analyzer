import sqlite3
import threading
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from content_analyzer.modules.cache_manager import CacheManager
from content_analyzer.utils import create_enhanced_duplicate_key


def test_cache_thread_safety(tmp_path):
    db_file = tmp_path / "cache.db"
    cache = CacheManager(db_file, ttl_hours=1)
    cache.store_result("h", "p", {"r": 1}, file_size=1)

    def worker():
        cache.get_cached_result("h", "p", file_size=1)

    threads = [threading.Thread(target=worker) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    key = f"{create_enhanced_duplicate_key('h', 1)}_p"
    conn = sqlite3.connect(db_file)
    hits = conn.execute(
        "SELECT hits_count FROM cache_prompts WHERE cache_key=?", (key,)
    ).fetchone()[0]
    conn.close()
    assert hits == 11
