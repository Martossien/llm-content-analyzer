from pathlib import Path
import sys
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from content_analyzer.modules.age_analyzer import AgeAnalyzer
from content_analyzer.modules.duplicate_detector import FileInfo


def make_file(id: int, days: int) -> FileInfo:
    dt = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    return FileInfo(id, f"file{id}.txt", "h", 1024, dt, dt)


def test_age_distribution():
    analyzer = AgeAnalyzer()
    files = [make_file(1, 10), make_file(2, 400)]
    dist = analyzer.analyze_age_distribution(files)
    assert dist["total_files"] == 2
    assert sum(dist["distribution_by_year"].values()) > 0


def test_identify_stale_files():
    analyzer = AgeAnalyzer()
    files = [make_file(1, 10), make_file(2, 400)]
    stale = analyzer.identify_stale_files(files, threshold_days=365)
    assert len(stale) == 1
    stale = analyzer.identify_stale_files(files, threshold_days=30)
    assert len(stale) == 1


def test_archival_candidates():
    analyzer = AgeAnalyzer()
    files = [make_file(1, 200), make_file(2, 300)]
    stats = analyzer.calculate_archival_candidates(files, threshold_days=180)
    assert stats["count"] == 2
    assert stats["total_size_bytes"] == 2048


def test_age_statistics():
    analyzer = AgeAnalyzer()
    files = [make_file(1, 1), make_file(2, 2)]
    stats = analyzer.get_age_statistics(files)
    assert stats["max_days"] >= 2
    assert stats["min_days"] >= 1


def test_parse_time_additional_formats():
    analyzer = AgeAnalyzer()

    dt_str_micro = "2024-03-01 12:34:56.123456"
    dt_micro = analyzer._parse_time(dt_str_micro)
    assert dt_micro == datetime(2024, 3, 1, 12, 34, 56, 123456)

    dt_str_short = "01/03/2024"
    dt_short = analyzer._parse_time(dt_str_short)
    assert dt_short == datetime(2024, 3, 1)

    timestamp = datetime(2024, 3, 1, 12, 0, 0).timestamp()
    dt_ts = analyzer._parse_time(str(timestamp))
    assert dt_ts == datetime.fromtimestamp(timestamp)
