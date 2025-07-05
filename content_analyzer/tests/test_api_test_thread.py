import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from gui.utils.api_test_thread import APITestThread

CFG = (
    Path(__file__).resolve().parents[2]
    / "content_analyzer"
    / "config"
    / "analyzer_config.yaml"
)


def test_quality_detection_corrupted():
    thread = APITestThread(CFG, Path(__file__), 1, 1, 0, "comprehensive")
    quality = thread._analyze_response_quality({}, "a b c d e f g }")
    assert quality["status"] == "corrupted"


def test_export_csv(tmp_path):
    thread = APITestThread(CFG, Path(__file__), 0, 1, 0, "comprehensive")
    thread.test_results = [
        {
            "iteration": 0,
            "worker_id": 0,
            "status": "completed",
            "quality": {"status": "success", "issues": []},
            "raw_response": "{}",
            "api_duration": 0,
            "total_duration": 0,
        }
    ]
    export = thread.export_test_results("csv")
    assert export.exists()
    assert export.stat().st_size > 0


def test_analyze_llm_reliability_with_none_results(tmp_path):
    thread = APITestThread(CFG, tmp_path / "f.txt", 1, 1, 0, "comprehensive")

    test_results = [
        None,
        {"status": "error", "result": None},
        {"status": "completed", "result": {}},
        {"status": "completed", "result": {"security": {"classification": "safe"}}},
    ]

    analysis = thread.analyze_llm_reliability(test_results)

    assert analysis["valid_responses"] >= 1
    assert isinstance(analysis.get("security_variance", 0), (int, float))


def test_analyze_llm_reliability_empty_results(tmp_path):
    thread = APITestThread(CFG, tmp_path / "f.txt", 1, 1, 0, "comprehensive")

    analysis = thread.analyze_llm_reliability([])

    assert analysis["valid_responses"] == 0
    assert analysis["failed_responses"] == 0


def test_analyze_response_variance_with_none(tmp_path):
    thread = APITestThread(CFG, tmp_path / "f.txt", 1, 1, 0, "comprehensive")

    thread._analyze_response_variance(None)
    thread._analyze_response_variance({})
    thread._analyze_response_variance({"security": None})
