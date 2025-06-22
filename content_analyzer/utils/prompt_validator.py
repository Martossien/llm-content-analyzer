"""Utility functions to validate prompt sizes for LLM templates."""

import logging
import threading
from typing import Dict, Union, Any, Optional
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)


class PromptSizeValidator:
    """Thread-safe prompt size validator with configurable limits."""

    def __init__(self, config_path: Optional[Path] = None) -> None:
        self.config_path = config_path
        self._load_config()

    def _load_config(self) -> None:
        """Load limits from config file with fallbacks."""
        self.warning_threshold = 3500
        self.critical_threshold = 3950
        self.max_size = 4000

        if self.config_path and self.config_path.exists():
            try:
                with open(self.config_path, "r", encoding="utf-8") as f:
                    config = yaml.safe_load(f)
                limits = config.get("llm_limits", {})
                self.warning_threshold = limits.get("warning_threshold", 3500)
                self.critical_threshold = limits.get("critical_threshold", 3950)
                self.max_size = limits.get("max_prompt_size", 4000)
            except Exception as e:  # pragma: no cover
                logger.warning(f"Could not load limits from config: {e}")


def calculate_real_prompt_size(text: str) -> int:
    """Calculate actual UTF-8 byte size of prompt."""
    try:
        return len(text.encode("utf-8"))
    except Exception as e:  # pragma: no cover
        logger.error(f"Error calculating prompt size: {e}")
        return len(text)


def get_prompt_size_color(size: int, validator: Optional[PromptSizeValidator] = None) -> str:
    """Return tkinter color based on prompt size thresholds."""
    if validator is None:
        warning = 3500
        critical = 3950
    else:
        warning = validator.warning_threshold
        critical = validator.critical_threshold

    if size < warning:
        return "green"
    elif size <= critical:
        return "orange"
    return "red"


def validate_prompt_size(
    system_prompt: str,
    user_template: str,
    max_size: int = 4000,
) -> Dict[str, Union[int, bool, str]]:
    """Validate prompt sizes and return comprehensive info."""
    try:
        system_size = calculate_real_prompt_size(system_prompt)
        user_size = calculate_real_prompt_size(user_template)
        total_size = system_size + user_size

        validator = PromptSizeValidator()
        color = get_prompt_size_color(total_size, validator)

        return {
            "system_size": system_size,
            "user_size": user_size,
            "total_size": total_size,
            "color": color,
            "within_limit": total_size <= max_size,
            "warning_threshold": validator.warning_threshold,
            "critical_threshold": validator.critical_threshold,
        }
    except Exception as e:  # pragma: no cover
        logger.error(f"Error validating prompt size: {e}")
        return {
            "system_size": 0,
            "user_size": 0,
            "total_size": 0,
            "color": "red",
            "within_limit": False,
            "error": str(e),
        }


class DebouncedCalculator:
    """Debounced calculator for real-time GUI updates."""

    def __init__(self, delay_ms: int = 500) -> None:
        self.delay_ms = delay_ms
        self._timer: Optional[threading.Timer] = None
        self._lock = threading.Lock()

    def schedule_calculation(self, callback, *args, **kwargs) -> None:
        """Schedule calculation with debouncing."""
        with self._lock:
            if self._timer:
                self._timer.cancel()
            self._timer = threading.Timer(
                self.delay_ms / 1000.0,
                callback,
                args,
                kwargs,
            )
            self._timer.start()

    def cancel(self) -> None:
        """Cancel pending calculation."""
        with self._lock:
            if self._timer:
                self._timer.cancel()
                self._timer = None

