from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from content_analyzer.modules.file_filter import FileFilter

CONFIG = (
    Path(__file__).resolve().parents[2]
    / "content_analyzer"
    / "config"
    / "analyzer_config.yaml"
)


def sample_row(**kwargs):
    base = {
        "path": "/share/documents/report.pdf",
        "extension": ".pdf",
        "file_size": 2000,
        "file_attributes": "",
        "last_modified": "2024-01-01",
    }
    base.update(kwargs)
    return base


def test_should_process_valid_file():
    ffilter = FileFilter(CONFIG)
    ok, reason = ffilter.should_process_file(sample_row())
    assert ok and reason == "ok"


def test_exclusion_rules():
    ffilter = FileFilter(CONFIG)
    ok, reason = ffilter.should_process_file(sample_row(extension=".tmp"))
    assert not ok and reason == "blocked_extension"


def test_extension_filtering_without_dot():
    ffilter = FileFilter(CONFIG)
    ok, reason = ffilter.should_process_file(sample_row(extension="zip"))
    assert not ok and reason == "blocked_extension"


def test_extension_filtering_case_insensitive():
    ffilter = FileFilter(CONFIG)
    ok, reason = ffilter.should_process_file(sample_row(extension=".ZIP"))
    assert not ok and reason == "blocked_extension"


def test_priority_scoring():
    ffilter = FileFilter(CONFIG)
    score = ffilter.calculate_priority_score(sample_row())
    assert 0 <= score <= 100


def test_special_flags_detection():
    ffilter = FileFilter(CONFIG)
    flags = ffilter.get_special_flags(sample_row(file_attributes="hidden system"))
    assert "hidden_file" in flags and "system_file" in flags


def test_file_attributes_variations():
    ffilter = FileFilter(CONFIG)
    ok, reason = ffilter.should_process_file(sample_row(file_attributes="Hidden, System"))
    assert not ok and reason == "system_file"
