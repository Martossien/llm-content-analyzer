from pathlib import Path


def test_no_sql_leaks():
    gui_path = Path("gui/main_window.py")
    content = gui_path.read_text(encoding="utf-8")
    assert "SELECT 1 FROM fichiers f2" not in content
