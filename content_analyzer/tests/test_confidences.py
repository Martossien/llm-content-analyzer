import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from content_analyzer.content_analyzer import ContentAnalyzer

CFG = Path(__file__).resolve().parents[1] / "config" / "analyzer_config.yaml"


def test_confidence_extraction():
    analyzer = ContentAnalyzer(CFG)
    payload_json = {
        "resume": "ok",
        "security": {"classification": "C1", "confidence": 90},
        "rgpd": {"risk_level": "low", "confidence": 80},
        "finance": {"document_type": "none", "confidence": 0},
        "legal": {"contract_type": "none", "confidence": 70},
    }
    api_res = {
        "status": "completed",
        "result": {"content": json.dumps(payload_json)},
        "task_id": "t1",
    }
    parsed = analyzer._parse_api_response(api_res)
    res = parsed["result"]
    assert res["security_confidence"] == 90
    assert res["rgpd_confidence"] == 80
    assert res["finance_confidence"] == 0
    assert res["legal_confidence"] == 70
    assert res["confidence_global"] == int((90 + 80 + 70) / 3)
