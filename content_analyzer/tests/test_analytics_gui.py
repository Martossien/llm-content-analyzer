import tkinter as tk
from pathlib import Path
import sys
import os
import pytest

if not os.environ.get("DISPLAY"):
    pytest.skip("no display", allow_module_level=True)

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from gui.analytics_panel import AnalyticsPanel


def test_analytics_panel_creation():
    root = tk.Tk()
    panel = AnalyticsPanel(root)
    assert 'age' in panel.tabs
    root.destroy()
