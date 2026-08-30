import sqlite3
from pathlib import Path
import sys
import tkinter as tk
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from content_analyzer.content_analyzer import ContentAnalyzer
from content_analyzer.modules.db_manager import DBManager
from gui.main_window import MainWindow

CFG = Path(__file__).resolve().parents[1] / "config" / "analyzer_config.yaml"


def test_resume_extraction():
    analyzer = ContentAnalyzer(CFG)
    api_res = {
        "status": "completed",
        "result": {
            "content": '{"resume": "court", "security": {"classification": "C1"}}'
        },
        "task_id": "1",
    }
    parsed = analyzer._parse_api_response(api_res)
    assert parsed["resume"] == "court"

    api_res["result"]["content"] = '{"security": {"classification": "C0"}}'
    parsed = analyzer._parse_api_response(api_res)
    assert parsed["resume"] == ""

    long_text = "word " * 60
    api_res["result"]["content"] = '{"resume": "' + long_text + '"}'
    parsed = analyzer._parse_api_response(api_res)
    assert len(parsed["resume"].split()) <= 50


def test_database_migration(tmp_path):
    db_file = tmp_path / "migr.db"
    conn = sqlite3.connect(db_file)
    conn.execute("CREATE TABLE fichiers (id INTEGER PRIMARY KEY)")
    conn.execute(
        "CREATE TABLE reponses_llm (id INTEGER PRIMARY KEY, fichier_id INTEGER, task_id TEXT)"
    )
    conn.commit()
    conn.close()
    db = DBManager(db_file)
    conn = sqlite3.connect(db_file)
    cols = [r[1] for r in conn.execute("PRAGMA table_info(reponses_llm)").fetchall()]
    for col in [
        "document_resume",
        "llm_response_complete",
        "security_confidence",
        "rgpd_confidence",
        "finance_confidence",
        "legal_confidence",
    ]:
        assert col in cols
    db.store_analysis_result(1, "t1", {"security": {}}, "resume", "{}")
    row = conn.execute(
        "SELECT document_resume, llm_response_complete FROM reponses_llm WHERE id=1"
    ).fetchone()
    conn.close()
    assert row == ("resume", "{}")


def test_gui_display(monkeypatch):
    try:
        root = tk.Tk()
        root.withdraw()
    except tk.TclError:
        pytest.skip("no display")
    mw = MainWindow(root)
    text = mw._format_analysis_display({"resume": "short"})
    root.destroy()
    assert "short" in text


def test_corrupted_state_recovery():
    try:
        root = tk.Tk()
        root.withdraw()
    except tk.TclError:
        pytest.skip("no display")
    mw = MainWindow(root)
    assert "ERREUR" in mw._format_analysis_display(123.4)
    text = mw._format_analysis_display({"security": {"classification": "C0", "confidence": 0}})
    root.destroy()
    assert "SÉCURITÉ" in text
