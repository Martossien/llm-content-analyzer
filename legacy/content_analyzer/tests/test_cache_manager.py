import json
import sqlite3
import time
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from content_analyzer.modules.cache_manager import CacheManager


def test_store_and_retrieve(tmp_path):
    db_file = tmp_path / "cache.db"
    cache = CacheManager(db_file, ttl_hours=1)
    cache.store_result("hash1", "ph1", {"result": "ok"}, file_size=1)
    result = cache.get_cached_result("hash1", "ph1", file_size=1)
    assert result["analysis_data"] == {"result": "ok"}
    assert result["resume"] == ""
    assert result["raw_response"] == ""


def test_cache_expiration(tmp_path):
    db_file = tmp_path / "cache.db"
    cache = CacheManager(db_file, ttl_hours=0)
    cache.store_result("h", "p", {"a": 1}, file_size=2)
    expired = cache.get_cached_result("h", "p", file_size=2)
    assert expired is None


def test_hit_rate_calculation(tmp_path):
    db_file = tmp_path / "cache.db"
    cache = CacheManager(db_file, ttl_hours=1)
    cache.store_result("h", "p", {"a": 1}, file_size=3)
    cache.get_cached_result("h", "p", file_size=3)
    cache.get_cached_result("h", "p", file_size=3)
    stats = cache.get_stats()
    assert stats["total_entries"] == 1
    assert stats["hit_rate"] > 0


def test_cleanup_expired(tmp_path):
    db_file = tmp_path / "cache.db"
    cache = CacheManager(db_file, ttl_hours=1)
    cache.store_result("h1", "p", {"a": 1}, file_size=4)
    conn = sqlite3.connect(db_file)
    conn.execute("UPDATE cache_prompts SET ttl_expiry = ?", (time.time() - 10,))
    conn.commit()
    conn.close()
    deleted = cache.cleanup_expired()
    assert deleted == 1
