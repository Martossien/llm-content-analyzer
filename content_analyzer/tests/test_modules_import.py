import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from content_analyzer.modules import DuplicateDetector


def test_module_export():
    det = DuplicateDetector()
    assert hasattr(det, "detect_duplicate_family")

