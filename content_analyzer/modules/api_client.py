import logging
import time
from typing import Any, Dict

import requests
from tenacity import retry, stop_after_attempt, wait_exponential
from circuitbreaker import circuit

logger = logging.getLogger(__name__)


class APIClient:
    """Client pour communiquer avec l'API-DOC-IA."""

    def __init__(self, config: Dict[str, Any]) -> None:
        self.url = config["api_config"]["url"].rstrip("/")
        self.token = config["api_config"].get("token")
        self.timeout = config["api_config"].get("timeout_seconds", 300)
        self.session = requests.Session()

    def _headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"}

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=4, max=10))
    @circuit(failure_threshold=5, recovery_timeout=30)
    def analyze_file(self, file_path: str, prompt: str) -> Dict[str, Any]:
        """Analyse un fichier via l'API."""
        logger.info("Upload %s", file_path)
        task_id = self._upload_file(file_path, prompt)
        logger.debug("Task id obtenu: %s", task_id)
        result = self._poll_result(task_id, timeout=self.timeout)
        result["task_id"] = task_id
        return result

    def _upload_file(self, file_path: str, prompt: str) -> str:
        data = {"prompt": prompt}
        with open(file_path, "rb") as fh:
            files = {"file": fh}
            resp = self.session.post(
                f"{self.url}/api/v2/process",
                headers=self._headers(),
                files=files,
                data=data,
                timeout=self.timeout,
            )
        resp.raise_for_status()
        payload = resp.json()
        return payload.get("task_id")

    def _poll_result(self, task_id: str, timeout: int = 300) -> Dict[str, Any]:
        start = time.time()
        while True:
            if time.time() - start > timeout:
                return {"status": "failed", "error": "timeout"}
            resp = self.session.get(
                f"{self.url}/api/v2/status/{task_id}",
                headers=self._headers(),
                timeout=10,
            )
            resp.raise_for_status()
            payload = resp.json()
            status = payload.get("status")
            if status in {"completed", "failed"}:
                return payload
            time.sleep(2)

    def health_check(self) -> bool:
        try:
            resp = self.session.get(f"{self.url}/api/v2/health", timeout=5)
            return resp.status_code == 200
        except requests.RequestException as exc:
            logger.warning("Health check failed: %s", exc)
            return False
