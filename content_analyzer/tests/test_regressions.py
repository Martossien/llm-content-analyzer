import threading
import time
import tkinter as tk
from pathlib import Path
import pytest
import warnings
warnings.filterwarnings("ignore", category=pytest.PytestUnraisableExceptionWarning)

from content_analyzer.content_analyzer import ContentAnalyzer
from content_analyzer.modules.db_manager import DBManager
from gui.analytics_panel import AnalyticsPanel


def test_cache_metrics_fix(monkeypatch):
    try:
        root = tk.Tk()
        root.withdraw()
    except tk.TclError:
        return
    panel = AnalyticsPanel(root)
    # patch calculation core to avoid DB
    def fake_core():
        return {"global": {"total_files": 1, "total_size_gb": 1}}
    monkeypatch.setattr(panel, "_calculate_metrics_core", fake_core)

    panel.threshold_age_years.set("1")
    metrics_a = panel.calculate_business_metrics()
    panel.threshold_age_years.set("2")
    metrics_b = panel.calculate_business_metrics()
    panel.threshold_age_years.set("1")
    start = time.time()
    metrics_a2 = panel.calculate_business_metrics()
    elapsed = time.time() - start
    root.destroy()
    panel.db_manager = None
    assert elapsed < 0.1
    assert len(panel._metrics_cache) >= 2
    assert metrics_a == metrics_a2


def test_async_calculations(monkeypatch):
    try:
        root = tk.Tk()
        root.withdraw()
    except tk.TclError:
        return
    panel = AnalyticsPanel(root)
    def fake_core():
        time.sleep(0.2)
        return {"global": {"total_files": 1, "total_size_gb": 1}}
    monkeypatch.setattr(panel, "_calculate_metrics_core", fake_core)
    panel.update_alert_cards()
    assert panel._calculation_in_progress
    while panel._calculation_in_progress:
        root.update()
        time.sleep(0.05)
    root.destroy()
    panel.db_manager = None
    assert "✅" in panel.progress_label.cget("text")


def test_concurrent_json_parsing():
    analyzer = ContentAnalyzer()
    api_res = {"status": "completed", "result": {"content": "{}"}, "task_id": "1"}
    def worker():
        return analyzer._thread_safe_parse_api_response(api_res)
    results = []
    threads = [threading.Thread(target=lambda: results.append(worker())) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(results) == 20
    assert all(r["status"] == "completed" for r in results)
    analyzer.close()


def test_index_consolidation(tmp_path):
    db_file = tmp_path / "test.db"
    with DBManager(db_file) as db:
        with db._connect() as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS fichiers ("
                "id INTEGER PRIMARY KEY,"
                "status TEXT,"
                "priority_score INTEGER,"
                "fast_hash TEXT,"
                "file_size INTEGER,"
                "last_modified TEXT,"
                "creation_time TEXT,"
                "extension TEXT,"
                "owner TEXT"
                ")"
            )
            db._ensure_indexes_with_validation(conn)
        report = db.verify_index_health()
        assert report["health_status"] == "OK"
