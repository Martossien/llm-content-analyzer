from __future__ import annotations

import logging
from typing import Any, Dict, List

from .duplicate_detector import FileInfo

logger = logging.getLogger(__name__)


class SizeAnalyzer:
    """Perform analysis on file sizes."""

    def analyze_size_distribution(self, files: List[FileInfo]) -> Dict[str, Any]:
        """Return distribution across size buckets."""
        buckets = {
            "<1MB": 0,
            "1-10MB": 0,
            "10-100MB": 0,
            ">100MB": 0,
        }
        for f in files:
            size_mb = f.file_size / (1024 * 1024)
            if size_mb < 1:
                buckets["<1MB"] += 1
            elif size_mb < 10:
                buckets["1-10MB"] += 1
            elif size_mb < 100:
                buckets["10-100MB"] += 1
            else:
                buckets[">100MB"] += 1
        total = len(files)
        distribution = {
            k: round(v / total * 100, 2) if total else 0 for k, v in buckets.items()
        }
        return {"distribution": distribution, "total_files": total}

    def identify_large_files(
        self, files: List[FileInfo], threshold_mb: int
    ) -> List[FileInfo]:
        """Return files larger than threshold."""
        limit = threshold_mb * 1024 * 1024
        return [f for f in files if f.file_size >= limit]

    def calculate_space_optimization(
        self, files: List[FileInfo], threshold_mb: int
    ) -> Dict[str, Any]:
        """Return potential space reclaimed by handling large files."""
        large = self.identify_large_files(files, threshold_mb)
        total_size = sum(f.file_size for f in large)
        return {
            "large_files": len(large),
            "size_bytes": total_size,
            "size_mb": round(total_size / (1024 * 1024), 2),
        }

    def get_size_statistics(self, files: List[FileInfo]) -> Dict[str, Any]:
        """Return basic statistics about sizes."""
        if not files:
            return {"average_mb": 0, "min_mb": 0, "max_mb": 0}
        sizes = [f.file_size for f in files]
        return {
            "average_mb": sum(sizes) / len(sizes) / (1024 * 1024),
            "min_mb": min(sizes) / (1024 * 1024),
            "max_mb": max(sizes) / (1024 * 1024),
        }


__all__ = ["SizeAnalyzer"]
