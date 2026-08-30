import threading
import time
from collections import deque
import logging

logger = logging.getLogger(__name__)

class ProgressTracker:
    """Thread-safe monotonic progress tracking with regression prevention."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._max_progress = 0.0
        self._progress_history = deque(maxlen=100)
        self._last_update_time = time.time()

    def update_progress(self, current_progress: float) -> float:
        """Update progress ensuring monotonic advancement."""
        with self._lock:
            validated_progress = max(current_progress, self._max_progress)
            if current_progress < self._max_progress:
                regression_amount = self._max_progress - current_progress
                logger.warning(
                    "Progress regression blocked: %.2f%%", regression_amount
                )
                self._progress_history.append(
                    {
                        "timestamp": time.time(),
                        "attempted": current_progress,
                        "blocked_regression": regression_amount,
                    }
                )
            self._max_progress = validated_progress
            return validated_progress
