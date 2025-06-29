import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))  # noqa: E402

from content_analyzer.modules.enhanced_cache import (  # noqa: E402
    EnhancedResultsCache,
)


def test_cache_get_put(tmp_path):
    db = tmp_path / "cache.db"
    cache = EnhancedResultsCache(db)
    cache.put_with_filters("k", {"a": 1}, {"f": 2})
    res = cache.get_with_filters("k", {"f": 2})
    assert res == {"a": 1}


def test_cache_invalidation(tmp_path):
    db = tmp_path / "cache.db"
    cache = EnhancedResultsCache(db)
    cache.put_with_filters("k1", 1, {"x": 1})
    cache.put_with_filters("k2", 2, {"x": 2})
    cache.invalidate_by_pattern("k1")
    assert cache.get_with_filters("k1", {"x": 1}) is None
    assert cache.get_with_filters("k2", {"x": 2}) == 2
