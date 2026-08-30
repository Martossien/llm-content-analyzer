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
        self.base_http_timeout = config["api_config"].get("http_timeout_seconds", 60)
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

        logger.info(
            "Upload %s (timeout: %ds, http: %ds)", file_path, timeout, http_timeout
        )
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
        poll_attempts = 0

        while True:
            poll_attempts += 1

            if stop_event and stop_event.is_set():
                logger.info(
                    "[CANCELLED] Polling interrompu par utilisateur: task=%s après %d tentatives",
                    task_id,
                    poll_attempts,
                )
                return {"status": "cancelled", "error": "interrupted_by_user"}

            elapsed = time.time() - start
            if elapsed > timeout:
                logger.error(
                    "[TIMEOUT GLOBAL] task=%s | Durée: %.1fs > %ds | Tentatives: %d | URL: %s",
                    task_id,
                    elapsed,
                    timeout,
                    poll_attempts,
                    self.url,
                )
                return {"status": "failed", "error": f"global_timeout_{timeout}s"}

            try:
                logger.debug(
                    "[POLL] Polling tentative %d: task=%s (%.1fs écoulées)",
                    poll_attempts,
                    task_id,
                    elapsed,
                )

                resp = self.session.get(
                    f"{self.url}/api/v2/status/{task_id}",
                    headers=self._headers(),
                    timeout=http_timeout,
                )
                resp.raise_for_status()
                payload = resp.json()
                status = payload.get("status")
                if status in {"completed", "failed"}:
                    logger.info(
                        "[SUCCESS] Polling terminé: task=%s | Status: %s | Durée: %.1fs | Tentatives: %d",
                        task_id,
                        status,
                        elapsed,
                        poll_attempts,
                    )
                    return payload
            except requests.exceptions.Timeout:
                logger.warning(
                    "[HTTP TIMEOUT] task=%s | Timeout: %ds | Tentative: %d/%d | URL: %s",
                    task_id,
                    http_timeout,
                    poll_attempts,
                    int(timeout / 2),
                    self.url,
                )
                time.sleep(5)
                continue
            except requests.exceptions.ConnectionError as exc:
                logger.error(
                    "[CONNECTION ERROR] task=%s | Erreur: %s | URL: %s | Tentative: %d",
                    task_id,
                    str(exc),
                    self.url,
                    poll_attempts,
                )
                time.sleep(10)
                continue
            except requests.exceptions.HTTPError as exc:
                status_code = getattr(exc.response, "status_code", "unknown")
                logger.error(
                    "[HTTP ERROR] task=%s | Code: %s | URL: %s | Réponse: %s",
                    task_id,
                    status_code,
                    self.url,
                    getattr(exc.response, "text", "no_response")[:200],
                )
                if status_code in [429, 503, 502]:
                    time.sleep(30)
                    continue
                else:
                    return {"status": "failed", "error": f"http_error_{status_code}"}
            except Exception as exc:
                logger.error(
                    "[EXCEPTION] ERREUR INATTENDUE POLLING: task=%s | Type: %s | Détail: %s",
                    task_id,
                    type(exc).__name__,
                    str(exc),
                )
                return {
                    "status": "failed",
                    "error": f"unexpected_error_{type(exc).__name__}",
                }

            # Sleep interruptible
            for _ in range(20):
                if stop_event and stop_event.is_set():
                    logger.info("[CANCELLED] Interruption pendant sleep: task=%s", task_id)
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
