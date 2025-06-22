import time
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from content_analyzer.utils.prompt_validator import (
    PromptSizeValidator,
    DebouncedCalculator,
    calculate_real_prompt_size,
    validate_prompt_size,
)

CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "analyzer_config.yaml"


def test_prompt_validator_config():
    validator = PromptSizeValidator(CONFIG_PATH)
    assert validator.warning_threshold == 3500
    assert validator.critical_threshold == 3950
    assert validator.max_size == 4000


def test_utf8_edge_cases():
    text = "é 😊"
    size = calculate_real_prompt_size(text)
    # "é" = 2 bytes, space = 1, emoji = 4 bytes
    assert size == len(text.encode("utf-8"))


def test_debounced_calculator():
    calls = []

    def callback(arg):
        calls.append(arg)

    debouncer = DebouncedCalculator(delay_ms=200)
    debouncer.schedule_calculation(callback, 1)
    debouncer.schedule_calculation(callback, 2)
    time.sleep(0.3)
    assert calls == [2]
    debouncer.cancel()


