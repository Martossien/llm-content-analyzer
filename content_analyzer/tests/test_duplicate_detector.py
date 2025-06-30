from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from content_analyzer.modules.duplicate_detector import DuplicateDetector, FileInfo


def test_should_ignore_zero_size_files():
    det = DuplicateDetector()
    info = FileInfo(id=1, path="/tmp/a.txt", fast_hash="abc", file_size=0)
    assert det.should_ignore_file(info)[0] is True


def test_identify_source_by_creation_date():
    det = DuplicateDetector()
    f1 = FileInfo(1, "a", "h", 1, "01/01/2020 10:00:00", None)
    f2 = FileInfo(2, "b", "h", 1, "02/01/2020 10:00:00", None)
    src = det.identify_source([f2, f1])
    assert src.id == 1


def test_duplicate_family_detection():
    det = DuplicateDetector()
    f1 = FileInfo(1, "a", "h", 1)
    f2 = FileInfo(2, "b", "h", 1)
    f3 = FileInfo(3, "c", "x", 2)
    fams = det.detect_duplicate_family([f1, f2, f3])
    assert len(fams) == 1
    key, group = next(iter(fams.items()))
    assert len(group) == 2


def test_statistics_calculation():
    det = DuplicateDetector()
    f1 = FileInfo(1, "a", "h", 1)
    f2 = FileInfo(2, "b", "h", 1)
    fams = det.detect_duplicate_family([f1, f2])
    stats = det.get_duplicate_statistics(fams)
    assert stats["total_families"] == 1
    assert stats["total_copies"] == 1


def test_edge_cases_hash_errors():
    det = DuplicateDetector()
    info = FileInfo(1, "a.tmp", "ERROR123", 10)
    ignore, reason = det.should_ignore_file(info)
    assert ignore is True
    assert reason == "hash_error"


def test_get_copy_statistics():
    det = DuplicateDetector()
    f1 = FileInfo(1, "a", "h", 1, "2020-01-01 00:00:00", None)
    f2 = FileInfo(2, "b", "h", 1, "2020-01-02 00:00:00", None)
    f3 = FileInfo(3, "c", "h", 1, "2020-01-03 00:00:00", None)
    stats = det.get_copy_statistics([f1, f2, f3])
    assert stats["copies_count"] == 2
    assert stats["source_file"]["path"] == "a"
    assert len(stats["copies"]) == 2


def test_identify_source_rename():
    det = DuplicateDetector()
    f1 = FileInfo(1, "a", "h", 1, "2020-01-01 00:00:00", None)
    f2 = FileInfo(2, "b", "h", 1, "2020-01-02 00:00:00", None)
    src = det.identify_source([f1, f2])
    assert src.path == "a"


def test_stats_keys_present_when_no_duplicates():
    det = DuplicateDetector()
    stats = det.get_duplicate_statistics({})
    expected_keys = {
        "total_families",
        "total_duplicates",
        "total_sources",
        "total_copies",
        "space_wasted_bytes",
        "space_wasted_mb",
        "largest_family_size",
        "families_by_size",
        "average_family_size",
    }
    assert set(stats.keys()) == expected_keys
