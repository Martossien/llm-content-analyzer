import sys
from pathlib import Path
import tkinter as tk
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))  # noqa: E402

from gui.analytics_panel import AnalyticsPanel  # noqa: E402
from content_analyzer.modules.duplicate_detector import FileInfo  # noqa: E402


def test_analytics_panel_basic():
    try:
        root = tk.Tk()
        root.withdraw()
    except tk.TclError:
        pytest.skip("no display")
    panel = AnalyticsPanel(root)
    files = [FileInfo(1, "a", "h1", 1000, "2024-01-01", "2024-01-01")]
    panel.refresh(files)
    root.destroy()
    assert panel.notebook.tabs()
