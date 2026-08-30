import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from content_analyzer.utils.duplicate_utils import detect_duplicates


def test_duplicate_detection_scenarios():
    scenarios = [
        ("abc123", 1000, "abc123", 1000, True),
        ("abc123", 1000, "abc123", 2000, False),
        ("abc123", 1000, "def456", 1000, False),
        ("", 1000, "", 1000, False),
        (None, 1000, None, 1000, False),
        ("abc123", 0, "abc123", 0, True),
        ("abc123", None, "abc123", None, False),
        ("abc123", 281474976710656, "abc123", 281474976710656, True),
        ("abc123", -1, "abc123", -1, False),
        ("A" * 64, 500, "A" * 64, 500, True),
        ("abc", 123, "ABC", 123, False),
        ("hash", 999, "hash", 998, False),
        ("hash", 999, "hash", 999, True),
        ("hash1", 0, "hash2", 0, False),
        ("same", 1, "same", 1, True),
        ("same", 1, "same", 2, False),
        ("same", 1, "other", 1, False),
        ("dup", 281474976710657, "dup", 281474976710657, False),
        (None, None, None, None, False),
        ("", None, "", None, False),
    ]
    for h1, s1, h2, s2, expected in scenarios:
        assert detect_duplicates(h1, s1, h2, s2) is expected, f"Failed: {h1},{s1} vs {h2},{s2}"
