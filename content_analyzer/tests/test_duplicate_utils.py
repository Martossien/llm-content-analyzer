import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from content_analyzer.utils.duplicate_utils import create_enhanced_duplicate_key


def test_create_enhanced_duplicate_key():
    assert create_enhanced_duplicate_key("abc", 10) == "abc_10"
    assert create_enhanced_duplicate_key("abc", None) == "abc"
    assert create_enhanced_duplicate_key("", 10) == ""
