from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from content_analyzer.modules.enhanced_cache import EnhancedResultsCache


def test_cache_put_get(tmp_path):
    db = tmp_path / "cache.db"
    cache = EnhancedResultsCache(db, max_memory_mb=1)
    cache.put_with_filters("k", {"a": 1}, {"f": 1})
    assert cache.get_with_filters("k", {"f": 1}) == {"a": 1}


def test_cache_invalidate(tmp_path):
    db = tmp_path / "cache.db"
    cache = EnhancedResultsCache(db)
    cache.put_with_filters("k1", {"v": 1}, {"f": 1})
    cache.put_with_filters("k2", {"v": 2}, {"f": 2})
    cache.invalidate_by_pattern("k1")
    assert cache.get_with_filters("k1", {"f": 1}) is None
    assert cache.get_with_filters("k2", {"f": 2}) == {"v": 2}
