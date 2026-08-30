import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from content_analyzer.content_analyzer import ContentAnalyzer

CFG = Path(__file__).resolve().parents[1] / "config" / "analyzer_config.yaml"


def test_prompt_error_handling():
    analyzer = ContentAnalyzer(CFG)
    api_res = {"status": "completed", "result": {"content": "notjson"}, "task_id": "1"}
    parsed = analyzer._parse_api_response(api_res)
    assert parsed["result"].get("parsing_error")
