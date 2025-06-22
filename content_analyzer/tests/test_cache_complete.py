import sqlite3
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from content_analyzer.modules.cache_manager import CacheManager


def test_cache_stores_resume_and_raw_response(tmp_path):
    db_file = tmp_path / "cache.db"
    cache = CacheManager(db_file, ttl_hours=1)
    cache.store_result("fast", "prompt", {"a": 1}, "short resume", '{"raw": true}')
    res = cache.get_cached_result("fast", "prompt")
    assert res["analysis_data"] == {"a": 1}
    assert res["resume"] == "short resume"
    assert res["raw_response"] == '{"raw": true}'


def test_backward_compatibility_existing_cache(tmp_path):
    db_file = tmp_path / "cache.db"
    conn = sqlite3.connect(db_file)
    conn.execute(
        """
        CREATE TABLE cache_prompts (
            cache_key TEXT PRIMARY KEY,
            prompt_hash TEXT NOT NULL,
            response_content TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            hits_count INTEGER DEFAULT 1,
            ttl_expiry TIMESTAMP,
            file_size INTEGER
        )
        """
    )
    conn.commit()
    conn.close()

    cache = CacheManager(db_file, ttl_hours=1)
    cache.store_result("f", "p", {"b": 2}, "r", "{}")
    res = cache.get_cached_result("f", "p")
    assert res["analysis_data"] == {"b": 2}
    assert res["resume"] == "r"
    assert res["raw_response"] == "{}"
