from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from gui.utils.progress_tracker import ProgressTracker


def test_progress_tracker_monotonic():
    tracker = ProgressTracker()
    assert tracker.update_progress(10.0) == 10.0
    assert tracker.update_progress(5.0) == 10.0
    assert tracker.update_progress(15.5) == 15.5
