import threading
import sqlite3
from pathlib import Path
from content_analyzer.modules.cache_manager import CacheManager
from content_analyzer.utils import create_enhanced_duplicate_key


def test_connection_pool_thread_safety(tmp_path: Path) -> None:
    db_file = tmp_path / "cache.db"
    cache = CacheManager(db_file, ttl_hours=1)
    cache.store_result("h", "p", {"r": 1}, file_size=1)

    def worker() -> None:
        for _ in range(5):
            cache.get_cached_result("h", "p", file_size=1)

    threads = [threading.Thread(target=worker) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    key = f"{create_enhanced_duplicate_key('h', 1)}_p"
    conn = sqlite3.connect(db_file)
    hits = conn.execute("SELECT hits_count FROM cache_prompts WHERE cache_key=?", (key,)).fetchone()[0]
    conn.close()
    assert hits == 1 + 20 * 5
