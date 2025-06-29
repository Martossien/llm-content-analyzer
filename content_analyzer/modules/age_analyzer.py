from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List

from .duplicate_detector import FileInfo

logger = logging.getLogger(__name__)


class AgeAnalyzer:
    """Perform analysis on file ages."""

    def _parse_time(self, value: str) -> datetime:
        if not value:
            return datetime.max
        for fmt in ("%d/%m/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(value.strip(), fmt)
            except ValueError:
                continue
        logger.warning("Could not parse date: %s", value)
        return datetime.max

    def _get_reliable_time(self, info: FileInfo) -> datetime:
        for attr in (info.last_modified, info.creation_time):
            if attr:
                dt = self._parse_time(attr)
                if dt != datetime.max:
                    return dt
        return datetime.max

    def analyze_age_distribution(self, files: List[FileInfo]) -> Dict[str, Any]:
        """Return distribution of files per year."""
        counts: Dict[int, int] = {}
        for f in files:
            dt = self._get_reliable_time(f)
            if dt == datetime.max:
                continue
            counts[dt.year] = counts.get(dt.year, 0) + 1
        total = sum(counts.values())
        distribution = {
            str(year): round(count / total * 100, 2) if total else 0
            for year, count in counts.items()
        }
        return {"distribution_by_year": distribution, "total_files": total}

    def identify_stale_files(
        self, files: List[FileInfo], threshold_days: int
    ) -> List[FileInfo]:
        """Return files not modified since threshold."""
        cutoff = datetime.now() - timedelta(days=threshold_days)
        stale: List[FileInfo] = []
        for f in files:
            dt = self._get_reliable_time(f)
            if dt != datetime.max and dt <= cutoff:
                stale.append(f)
        return stale

    def calculate_archival_candidates(
        self, files: List[FileInfo], threshold_days: int
    ) -> Dict[str, Any]:
        """Return number and size of stale files."""
        stale = self.identify_stale_files(files, threshold_days)
        total_size = sum(f.file_size for f in stale)
        return {
            "count": len(stale),
            "total_size_bytes": total_size,
            "total_size_mb": round(total_size / (1024 * 1024), 2),
        }

    def get_age_statistics(self, files: List[FileInfo]) -> Dict[str, Any]:
        """Return basic statistics about file ages."""
        ages = []
        now = datetime.now()
        for f in files:
            dt = self._get_reliable_time(f)
            if dt != datetime.max:
                ages.append((now - dt).days)
        if not ages:
            return {"average_days": 0, "min_days": 0, "max_days": 0}
        return {
            "average_days": sum(ages) / len(ages),
            "min_days": min(ages),
            "max_days": max(ages),
        }


__all__ = ["AgeAnalyzer"]
