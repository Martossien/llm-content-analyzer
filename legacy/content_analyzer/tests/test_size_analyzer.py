from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from content_analyzer.modules.size_analyzer import SizeAnalyzer
from content_analyzer.modules.duplicate_detector import FileInfo


def make_file(id: int, size_mb: int) -> FileInfo:
    return FileInfo(
        id, f"file{id}.bin", "h", size_mb * 1024 * 1024, "2024-01-01", "2024-01-02"
    )


def test_size_distribution():
    analyzer = SizeAnalyzer()
    files = [make_file(1, 1), make_file(2, 50), make_file(3, 200)]
    dist = analyzer.analyze_size_distribution(files)
    assert dist["total_files"] == 3
    assert dist["distribution"][">100MB"] == round(1 / 3 * 100, 2)


def test_identify_large_files():
    analyzer = SizeAnalyzer()
    files = [make_file(1, 1), make_file(2, 20)]
    large = analyzer.identify_large_files(files, threshold_mb=10)
    assert len(large) == 1


def test_space_optimization():
    analyzer = SizeAnalyzer()
    files = [make_file(1, 5), make_file(2, 20)]
    stats = analyzer.calculate_space_optimization(files, threshold_mb=10)
    assert stats["large_files"] == 1
    assert stats["size_bytes"] == 20 * 1024 * 1024


def test_size_statistics():
    analyzer = SizeAnalyzer()
    files = [make_file(1, 2), make_file(2, 4)]
    stats = analyzer.get_size_statistics(files)
    assert stats["max_mb"] >= 4
    assert stats["min_mb"] >= 2
