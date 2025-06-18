from pathlib import Path
from unittest import mock
import pytest
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from content_analyzer.modules.api_client import APIClient
from circuitbreaker import CircuitBreakerError, CircuitBreakerMonitor
from tenacity import RetryError

CONFIG = {
    "api_config": {"url": "http://localhost:8080", "token": "t", "timeout_seconds": 5},
}


def test_health_check():
    client = APIClient(CONFIG)
    CircuitBreakerMonitor.get("APIClient.analyze_file").reset()
    with mock.patch.object(client.session, "get") as mget:
        mget.return_value.status_code = 200
        assert client.health_check() is True


def test_analyze_file_success(tmp_path):
    client = APIClient(CONFIG)
    CircuitBreakerMonitor.get("APIClient.analyze_file").reset()
    file_path = tmp_path / "f.txt"
    file_path.write_text("data")
    with (
        mock.patch.object(client.session, "post") as mpost,
        mock.patch.object(client.session, "get") as mget,
    ):
        mpost.return_value.json.return_value = {"task_id": "1"}
        mpost.return_value.raise_for_status.return_value = None
        mget.return_value.json.return_value = {
            "status": "completed",
            "result": {"ok": True},
        }
        mget.return_value.raise_for_status.return_value = None
        result = client.analyze_file(str(file_path), "prompt")
    assert result["status"] == "completed"


def test_analyze_file_with_retry(tmp_path):
    client = APIClient(CONFIG)
    CircuitBreakerMonitor.get("APIClient.analyze_file").reset()
    file_path = tmp_path / "f.txt"
    file_path.write_text("data")
    with (
        mock.patch.object(client.session, "post") as mpost,
        mock.patch.object(client.session, "get") as mget,
    ):
        resp = mock.Mock()
        resp.json.return_value = {"task_id": "2"}
        resp.raise_for_status.return_value = None
        mpost.side_effect = [Exception("fail"), resp]
        mget.return_value.json.return_value = {"status": "completed", "result": {}}
        mget.return_value.raise_for_status.return_value = None
        result = client.analyze_file(str(file_path), "prompt")
    assert result["task_id"] == "2"


def test_circuit_breaker_activation(tmp_path):
    client = APIClient(CONFIG)
    CircuitBreakerMonitor.get("APIClient.analyze_file").reset()
    file_path = tmp_path / "f.txt"
    file_path.write_text("data")
    with mock.patch.object(client.session, "post", side_effect=Exception("boom")):
        for _ in range(5):
            try:
                client.analyze_file(str(file_path), "p")
            except Exception:
                pass
        with pytest.raises(RetryError):
            client.analyze_file(str(file_path), "p")


def test_timeout_handling(tmp_path):
    client = APIClient(CONFIG)
    CircuitBreakerMonitor.get("APIClient.analyze_file").reset()
    file_path = tmp_path / "f.txt"
    file_path.write_text("data")
    with (
        mock.patch.object(client.session, "post") as mpost,
        mock.patch.object(client.session, "get") as mget,
    ):
        mpost.return_value.json.return_value = {"task_id": "3"}
        mpost.return_value.raise_for_status.return_value = None
        mget.return_value.json.return_value = {"status": "processing"}
        mget.return_value.raise_for_status.return_value = None
        result = client.analyze_file(str(file_path), "prompt")
    assert result["status"] == "failed"
