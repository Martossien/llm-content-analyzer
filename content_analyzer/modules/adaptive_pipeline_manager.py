import time
import threading
import queue
from collections import deque
from typing import Dict, Any
from dataclasses import dataclass
import logging


@dataclass
class PipelineMetrics:
    """Métriques temps réel du pipeline"""

    uploads_completed: int = 0
    llm_processing_completed: int = 0
    current_upload_spacing: float = 5.0
    avg_api_response_time: float = 0.0
    queue_depth: int = 0
    llm_idle_time: float = 0.0
    throughput_per_minute: float = 0.0


class AdaptivePipelineManager:
    """Gestionnaire de pipeline avec feedback automatique"""

    def __init__(self, config: Dict[str, Any], max_workers: int = 1):
        self.config = config.get("pipeline_config", {}).get("upload_spacing", {})
        self._max_workers = max_workers

        self.current_spacing = self.config.get("initial_delay_seconds", 5.0)
        self.min_spacing = self.config.get("min_delay_seconds", 1.0)
        self.max_spacing = self.config.get("max_delay_seconds", 99.0)
        self.threshold = self.config.get("response_time_threshold", 5.0)
        self.adjustment_step = self.config.get("adjustment_step", 1.0)
        self.buffer_size = self.config.get("buffer_size", 2)
        self.adaptive_enabled = self.config.get("enable_adaptive_spacing", True)

        self.metrics = PipelineMetrics()
        self.response_times = deque(maxlen=10)
        self.lock = threading.Lock()

        self.upload_queue: "queue.Queue" = queue.Queue()
        self.processing_queue: "queue.Queue" = queue.Queue(maxsize=self.buffer_size)

        self.last_upload_time = 0.0
        self.llm_start_time = None
        logging.info(
            f"Pipeline adaptatif initialisé: espacement={self.current_spacing}s, seuil={self.threshold}s"
        )

    def record_api_response_time(self, response_time: float) -> None:
        if response_time < 0.001:
            logging.warning(f"Ignoring unrealistic API time: {response_time}s")
            return
        with self.lock:
            self.response_times.append(response_time)
            if self.adaptive_enabled and len(self.response_times) >= 3:
                avg_time = sum(self.response_times) / len(self.response_times)
                self.metrics.avg_api_response_time = avg_time

                max_workers = getattr(self, "_max_workers", None)
                if max_workers == 1:
                    logging.debug("Single worker mode: adaptive spacing disabled")
                    return

                min_safe_spacing = max(self.min_spacing, max_workers) if max_workers else self.min_spacing

                if avg_time > self.threshold:
                    new_spacing = min(self.current_spacing + self.adjustment_step, self.max_spacing)
                    if new_spacing != self.current_spacing:
                        logging.info(
                            f"API protection: {avg_time:.1f}s > {self.threshold}s → espacement {self.current_spacing}s → {new_spacing}s"
                        )
                        self.current_spacing = new_spacing
                elif avg_time < self.threshold * 0.5 and self.current_spacing > min_safe_spacing:
                    new_spacing = max(self.current_spacing - self.adjustment_step, min_safe_spacing)
                    if new_spacing != self.current_spacing:
                        logging.info(
                            f"API optimization: {avg_time:.1f}s < {self.threshold * 0.5:.1f}s → espacement {self.current_spacing}s → {new_spacing}s (min_safe: {min_safe_spacing}s)"
                        )
                        self.current_spacing = new_spacing
                else:
                    logging.debug(
                        f"API stable: {avg_time:.1f}s, espacement maintenu à {self.current_spacing}s"
                    )

                self.metrics.current_upload_spacing = self.current_spacing

    def should_delay_upload(self) -> float:
        now = time.time()
        elapsed = now - self.last_upload_time
        if elapsed < self.current_spacing:
            return self.current_spacing - elapsed
        return 0.0

    def register_upload_start(self) -> None:
        self.last_upload_time = time.time()
        with self.lock:
            self.metrics.uploads_completed += 1

    def register_llm_processing_start(self) -> None:
        self.llm_start_time = time.time()

    def register_llm_processing_complete(self) -> None:
        if self.llm_start_time:
            processing_time = time.time() - self.llm_start_time
            with self.lock:
                self.metrics.llm_processing_completed += 1
                if self.metrics.llm_processing_completed > 0:
                    elapsed_total = time.time() - self.llm_start_time
                    self.metrics.throughput_per_minute = (
                        self.metrics.llm_processing_completed / elapsed_total * 60
                    )

    def get_pipeline_status(self) -> Dict[str, Any]:
        with self.lock:
            self.metrics.queue_depth = self.processing_queue.qsize()
            return {
                "current_spacing": self.current_spacing,
                "avg_response_time": self.metrics.avg_api_response_time,
                "uploads_completed": self.metrics.uploads_completed,
                "llm_completed": self.metrics.llm_processing_completed,
                "throughput_per_minute": self.metrics.throughput_per_minute,
                "queue_depth": self.processing_queue.qsize(),
                "adaptive_enabled": self.adaptive_enabled,
                "last_adjustment": f"Threshold: {self.threshold}s, Range: {self.min_spacing}-{self.max_spacing}s",
            }
