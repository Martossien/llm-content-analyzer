import sys
import time
import threading
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from gui.utils.multi_worker_analysis_thread import SmartMultiWorkerAnalysisThread
from gui.utils.api_test_thread import APITestThread


def test_threading_responsiveness(monkeypatch, tmp_path):
    class DummyDB:
        def __init__(self, path):
            pass

        def close(self):
            pass

        def get_pending_files(self, limit=None):
            return [{"id": 1, "path": str(tmp_path / "f.txt")}]

        def store_analysis_result(self, *a, **kw):
            pass

        def update_file_status(self, *a, **kw):
            pass

    class DummyAnalyzer:
        def __init__(self, *a, stop_event=None, **kw):
            self.stop_event = stop_event
            self.csv_parser = self
            self.prompt_manager = self

        def parse_csv(self, *a, **kw):
            pass

        def build_analysis_prompt(self, *a, **kw):
            return "p"

        def _format_file_size(self, size):
            return str(size)

        def analyze_single_file(self, file_row, force_analysis=False):
            for _ in range(100):
                if self.stop_event and self.stop_event.is_set():
                    break
                time.sleep(0.01)
            return {"status": "completed", "result": {}, "task_id": "t"}

    monkeypatch.setattr(
        "gui.utils.multi_worker_analysis_thread.DBManager", DummyDB
    )
    monkeypatch.setattr(
        "gui.utils.multi_worker_analysis_thread.ContentAnalyzer", DummyAnalyzer
    )

    csv_file = tmp_path / "dummy.csv"
    csv_file.write_text("p")
    config_file = tmp_path / "cfg.yaml"
    config_file.write_text("{}")
    output_db = tmp_path / "o.db"
    thread = SmartMultiWorkerAnalysisThread(config_file, csv_file, output_db, max_workers=1)
    thread.start()
    time.sleep(0.5)
    start = time.time()
    thread.stop()
    thread.join(timeout=5)
    duration = time.time() - start
    assert duration < 5


def test_api_test_thread_stop(monkeypatch, tmp_path):
    class DummyAnalyzer:
        def __init__(self, *a, stop_event=None, **kw):
            self.stop_event = stop_event
            self.csv_parser = self
            self.prompt_manager = self

        def build_analysis_prompt(self, *a, **kw):
            return "p"

        def _format_file_size(self, size):
            return str(size)

        def analyze_single_file(self, file_row, force_analysis=False):
            for _ in range(100):
                if self.stop_event and self.stop_event.is_set():
                    return {"status": "cancelled"}
                time.sleep(0.01)
            return {"status": "completed"}

    monkeypatch.setattr("gui.utils.api_test_thread.ContentAnalyzer", DummyAnalyzer)

    f = tmp_path / "f.txt"
    f.write_text("x")
    thread = APITestThread(Path("cfg"), f, 1, 1, 0, "comprehensive")

    result = {}

    def worker():
        nonlocal result
        result = thread._test_api_worker(0, 0)

    t = threading.Thread(target=worker)
    t.start()
    time.sleep(0.5)
    thread.stop()
    t.join(timeout=5)
    assert result.get("status") == "cancelled"
