import logging
import time
import threading
from typing import Any, Dict, Optional

import requests
from tenacity import retry, stop_after_attempt, wait_exponential
from circuitbreaker import circuit

logger = logging.getLogger(__name__)


class APIClient:
    """Client pour communiquer avec l'API-DOC-IA."""

    def __init__(self, config: Dict[str, Any]) -> None:
        self.url = config["api_config"]["url"].rstrip("/")
        self.token = config["api_config"].get("token")
        self.base_timeout = config["api_config"].get("timeout_seconds", 300)
        self.base_http_timeout = config["api_config"].get(
            "http_timeout_seconds", 60
        )
        self.session = requests.Session()
        self._closed = False

    def __del__(self) -> None:
        self.close()

    def _headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"}

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=4, max=10))
    @circuit(failure_threshold=5, recovery_timeout=30)
    def analyze_file(
        self,
        file_path: str,
        prompt: str,
        adaptive_timeouts: Optional[Dict[str, int]] = None,
        stop_event: Optional[threading.Event] = None,
    ) -> Dict[str, Any]:
        """Analyse un fichier avec timeouts adaptatifs."""
        if adaptive_timeouts:
            timeout = adaptive_timeouts.get("global_timeout", self.base_timeout)
            http_timeout = adaptive_timeouts.get("http_timeout", self.base_http_timeout)
        else:
            timeout = self.base_timeout
            http_timeout = self.base_http_timeout

        logger.info("Upload %s (timeout: %ds, http: %ds)", file_path, timeout, http_timeout)
        task_id = self._upload_file(file_path, prompt, http_timeout)
        logger.debug("Task id obtenu: %s", task_id)
        result = self._poll_result(
            task_id, timeout=timeout, http_timeout=http_timeout, stop_event=stop_event
        )
        result["task_id"] = task_id
        return result

    def _upload_file(self, file_path: str, prompt: str, http_timeout: int = 30) -> str:
        data = {"prompt": prompt}
        with open(file_path, "rb") as fh:
            files = {"file": fh}
            resp = self.session.post(
                f"{self.url}/api/v2/process",
                headers=self._headers(),
                files=files,
                data=data,
                timeout=http_timeout,
            )
        resp.raise_for_status()
        payload = resp.json()
        return payload.get("task_id")

    def _poll_result(
        self,
        task_id: str,
        timeout: int = 300,
        http_timeout: int = 30,
        stop_event: Optional[threading.Event] = None,
    ) -> Dict[str, Any]:
        start = time.time()
        while True:
            if stop_event and stop_event.is_set():
                logger.info("Polling interrupted for task %s", task_id)
                return {"status": "cancelled", "error": "interrupted_by_user"}

            if time.time() - start > timeout:
                return {"status": "failed", "error": "timeout"}

            try:
                resp = self.session.get(
                    f"{self.url}/api/v2/status/{task_id}",
                    headers=self._headers(),
                    timeout=http_timeout,
                )
                resp.raise_for_status()
                payload = resp.json()
            except requests.exceptions.Timeout:
                logger.warning("HTTP timeout (%ds) polling task %s", http_timeout, task_id)
                time.sleep(5)
                continue

            status = payload.get("status")
            if status in {"completed", "failed"}:
                return payload

            for _ in range(20):
                if stop_event and stop_event.is_set():
                    return {"status": "cancelled", "error": "interrupted_during_sleep"}
                time.sleep(0.1)

    def health_check(self) -> bool:
        try:
            resp = self.session.get(f"{self.url}/api/v2/health", timeout=5)
            return resp.status_code == 200
        except requests.RequestException as exc:
            logger.warning("Health check failed: %s", exc)
            return False

    def close(self) -> None:
        """Close underlying HTTP session."""
        if not self._closed:
            self.session.close()
            self._closed = True

    def __enter__(self) -> "APIClient":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
