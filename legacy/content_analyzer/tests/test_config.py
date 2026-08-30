from pathlib import Path
import yaml

CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "analyzer_config.yaml"

def test_consolidated_sections():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    assert "exclusions" in cfg
    assert "templates" in cfg
    assert "api_config" in cfg
