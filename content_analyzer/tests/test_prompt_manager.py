from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from content_analyzer.modules.prompt_manager import PromptManager

CONFIG = (
    Path(__file__).resolve().parents[2]
    / "content_analyzer"
    / "config"
    / "prompts_config.yaml"
)


def test_build_comprehensive_prompt():
    pm = PromptManager(CONFIG)
    prompt = pm.build_analysis_prompt(
        {
            "file_name": "test.txt",
            "file_size_readable": "1KB",
            "owner": "me",
            "last_modified": "today",
        }
    )
    assert "test.txt" in prompt


def test_template_validation():
    pm = PromptManager(CONFIG)
    ok, reason = pm.validate_template("comprehensive")
    assert ok


def test_metadata_injection():
    pm = PromptManager(CONFIG)
    prompt = pm.build_analysis_prompt(
        {
            "file_name": "doc.pdf",
            "file_size_readable": "2KB",
            "owner": "me",
            "last_modified": "now",
        },
        analysis_type="security_focused",
    )
    assert "doc.pdf" in prompt


def test_security_focused_prompt():
    pm = PromptManager(CONFIG)
    prompt = pm.build_analysis_prompt(
        {"file_name": "a.pdf", "metadata_summary": "meta"},
        analysis_type="security_focused",
    )
    assert "Classification" in prompt
