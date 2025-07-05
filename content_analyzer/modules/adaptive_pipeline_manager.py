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

        api_cfg = config.get("api_config", {})
        self.base_timeout = api_cfg.get("timeout_seconds", 300)
        self.base_http_timeout = api_cfg.get("http_timeout_seconds", 60)

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
        """Système d'espacement adaptatif avec sécurité primaire et seuils relatifs."""
        if response_time < 0.001:
            logging.warning(f"Ignoring unrealistic API time: {response_time}s")
            return

        with self.lock:
            self.response_times.append(response_time)
            if not (self.adaptive_enabled and len(self.response_times) >= 3):
                return

            if getattr(self, "_max_workers", 1) == 1:
                logging.debug("Single worker mode: adaptive spacing disabled")
                return

            max_workers = getattr(self, "_max_workers", None)
            min_safe_spacing = (
                max(self.min_spacing, max_workers) if max_workers else self.min_spacing
            )

            # Sécurité primaire: réduction immédiate si le temps API est inférieur à l'espacement actuel
            if response_time < self.current_spacing:
                immediate_spacing = max(response_time - 1.0, min_safe_spacing)
                if immediate_spacing < self.current_spacing:
                    logging.info(
                        f"⚡ RESET IMMÉDIAT: API {response_time:.1f}s < espacement {self.current_spacing:.1f}s → {immediate_spacing:.1f}s"
                    )
                    self.current_spacing = immediate_spacing
                    self.metrics.current_upload_spacing = self.current_spacing
                    return

            if len(self.response_times) >= 5:
                baseline_time = sum(self.response_times) / len(self.response_times)
                self.metrics.avg_api_response_time = baseline_time

                zone_red_threshold = baseline_time * 1.8
                zone_green_threshold = baseline_time * 0.7

                if baseline_time > zone_red_threshold:
                    new_spacing = min(
                        self.current_spacing + self.adjustment_step, self.max_spacing
                    )
                    if new_spacing != self.current_spacing:
                        logging.info(
                            f"🔴 API dégradée: {baseline_time:.1f}s > {zone_red_threshold:.1f}s (baseline×1.8) → espacement {self.current_spacing:.1f}s → {new_spacing:.1f}s"
                        )
                        self.current_spacing = new_spacing

                elif (
                    baseline_time < zone_green_threshold
                    and self.current_spacing > min_safe_spacing
                ):
                    new_spacing = max(
                        self.current_spacing - self.adjustment_step, min_safe_spacing
                    )
                    if new_spacing != self.current_spacing:
                        logging.info(
                            f"🟢 API performante: {baseline_time:.1f}s < {zone_green_threshold:.1f}s (baseline×0.7) → espacement {self.current_spacing:.1f}s → {new_spacing:.1f}s"
                        )
                        self.current_spacing = new_spacing
                else:
                    logging.debug(
                        f"🟡 API stable: {baseline_time:.1f}s (baseline: {baseline_time:.1f}s, seuils: {zone_green_threshold:.1f}s-{zone_red_threshold:.1f}s) → espacement maintenu à {self.current_spacing:.1f}s"
                    )

                if self.current_spacing > baseline_time * 4:
                    emergency_spacing = max(baseline_time * 1.5, min_safe_spacing)
                    logging.warning(
                        f"🆘 ESPACEMENT EXCESSIF: {self.current_spacing:.1f}s > {baseline_time * 4:.1f}s (4×baseline) → correction {emergency_spacing:.1f}s"
                    )
                    self.current_spacing = emergency_spacing

            self.metrics.current_upload_spacing = self.current_spacing

    def get_adaptive_timeouts(self) -> Dict[str, int]:
        """Calcule timeouts adaptatifs basés sur l'espacement actuel."""
        base_timeout = self.base_timeout
        base_http_timeout = self.base_http_timeout

        adaptive_factor = max(1.0, self.current_spacing / 10.0)

        return {
            "global_timeout": int(base_timeout * adaptive_factor),
            "http_timeout": int(base_http_timeout * adaptive_factor),
        }

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

    # ------------------------------------------------------------------
    # Diagnostic helpers
    # ------------------------------------------------------------------
    def get_detailed_status(self) -> Dict[str, Any]:
        """Status détaillé pour monitoring en temps réel."""
        with self.lock:
            recent_times = list(self.response_times)[-5:]
            baseline = (
                sum(self.response_times) / len(self.response_times)
                if self.response_times
                else 0
            )
            return {
                "espacement_actuel": self.current_spacing,
                "seuils": {
                    "vert": self.threshold * 0.6,
                    "rouge": self.threshold * 3.0,
                    "reset": self.current_spacing * 0.5,
                },
                "api_times": {
                    "moyenne_10": baseline,
                    "recent_5": recent_times,
                    "min_max": (
                        [min(self.response_times), max(self.response_times)]
                        if self.response_times
                        else [0, 0]
                    ),
                },
                "metriques": {
                    "uploads": self.metrics.uploads_completed,
                    "throughput_min": self.metrics.throughput_per_minute,
                    "adaptive_enabled": self.adaptive_enabled,
                },
                "diagnostic": self._diagnostic_spacing(),
            }

    def _diagnostic_spacing(self) -> str:
        """Diagnostic automatique de l'espacement avec baseline adaptative."""
        if not self.response_times:
            return "🔄 Collecte de données en cours"

        if len(self.response_times) < 5:
            return "📊 Collecte baseline adaptative en cours"

        baseline = sum(self.response_times) / len(self.response_times)
        latest_response = self.response_times[-1]
        ratio = self.current_spacing / baseline if baseline > 0 else 0

        if latest_response < self.current_spacing:
            return f"⚡ Sécurité primaire: API {latest_response:.1f}s < espacement {self.current_spacing:.1f}s → Reset requis"

        if ratio > 4:
            return f"⚠️ Espacement excessif (x{ratio:.1f} baseline {baseline:.1f}s)"
        elif ratio < 1.2:
            return f"🚀 Espacement optimal (x{ratio:.1f} baseline {baseline:.1f}s)"
        elif ratio > 2.5:
            return f"⚡ Espacement élevé (x{ratio:.1f} baseline {baseline:.1f}s)"
        else:
            return f"✅ Espacement normal (x{ratio:.1f} baseline {baseline:.1f}s)"

    def auto_recovery_check(self) -> None:
        """Vérifie et corrige automatiquement les états incohérents."""
        if len(self.response_times) < 5:
            return

        avg_time = sum(self.response_times) / len(self.response_times)

        if self.current_spacing > avg_time * 10:
            emergency_spacing = max(avg_time * 2, self.min_spacing)
            logging.warning(
                f"🆘 AUTO-RECOVERY: Espacement aberrant {self.current_spacing:.1f}s → {emergency_spacing:.1f}s"
            )
            self.current_spacing = emergency_spacing
            self.metrics.current_upload_spacing = emergency_spacing
