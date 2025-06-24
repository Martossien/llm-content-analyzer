"""Utility functions to validate prompt sizes for LLM templates."""

import logging
from typing import Dict, Union, Any, Optional
import tkinter as tk
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


class TkinterDebouncer:
    """Debouncer using Tkinter's event loop for thread safety."""

    def __init__(self, root: tk.Misc, delay_ms: int = 500) -> None:
        self.root = root
        self.delay_ms = delay_ms
        self._scheduled_id: Optional[str] = None

    def schedule_calculation(self, callback, *args, **kwargs) -> None:
        """Schedule callback with debouncing via ``root.after``."""
        if self._scheduled_id:
            self.root.after_cancel(self._scheduled_id)

        self._scheduled_id = self.root.after(
            self.delay_ms,
            lambda: self._execute_callback(callback, *args, **kwargs),
        )

    def _execute_callback(self, callback, *args, **kwargs) -> None:
        self._scheduled_id = None
        try:
            callback(*args, **kwargs)
        except Exception as e:  # pragma: no cover - log and continue
            print(f"Debouncer callback error: {e}")

    def cancel(self) -> None:
        """Cancel any pending callback."""
        if self._scheduled_id:
            self.root.after_cancel(self._scheduled_id)
            self._scheduled_id = None

