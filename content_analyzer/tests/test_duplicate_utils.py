import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from content_analyzer.utils.duplicate_utils import (
    create_enhanced_duplicate_key,
    detect_duplicates,
)


def test_create_enhanced_duplicate_key():
    assert create_enhanced_duplicate_key("abc", 10) == "abc_10"
    invalid = create_enhanced_duplicate_key("abc", None)
    assert invalid.startswith("INVALID_SIZE_abc_")
    invalid2 = create_enhanced_duplicate_key("", 10)
    assert invalid2.startswith("INVALID_HASH_")


def test_detect_duplicates_basic():
    assert detect_duplicates("h1", 1, "h1", 1) is True
    assert detect_duplicates("h1", 1, "h1", 2) is False
