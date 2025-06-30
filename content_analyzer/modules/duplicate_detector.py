"""Centralized duplicate detection utilities."""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from content_analyzer.utils.duplicate_utils import (
    create_enhanced_duplicate_key,
    detect_duplicates,
)

logger = logging.getLogger(__name__)


@dataclass(eq=True, frozen=True)
class FileInfo:
    """Standardized structure for file metadata."""

    id: int
    path: str
    fast_hash: Optional[str]
    file_size: int
    creation_time: Optional[str] = None
    last_modified: Optional[str] = None


class DuplicateDetector:
    """Central duplicate detection class."""

    def __init__(self, config: Optional[Dict[str, Any]] = None) -> None:
        self.config = config or {}
        self.size_limit = 281_474_976_710_656
        self._lock = threading.RLock()

    # ------------------------------------------------------------------
    # Filtering helpers
    # ------------------------------------------------------------------
    def should_ignore_file(self, file_info: FileInfo) -> Tuple[bool, str]:
        """Return whether a file should be ignored from duplicate detection."""
        if file_info.file_size == 0:
            return True, "zero_size_file"
        if file_info.file_size > self.size_limit:
            return True, "file_too_large"
        if file_info.fast_hash and "ERROR" in file_info.fast_hash.upper():
            logger.warning("FastHash error detected for %s", file_info.path)
            return True, "hash_error"
        temp_extensions = {".tmp", ".temp", ".~"}
        if Path(file_info.path).suffix.lower() in temp_extensions:
            return True, "temporary_file"
        return False, "ok"

    # ------------------------------------------------------------------
    def detect_duplicate_family(
        self, files_list: List[FileInfo]
    ) -> Dict[str, List[FileInfo]]:
        """Group files by duplicate family based on hash and size."""
        families: Dict[str, List[FileInfo]] = {}
        for info in files_list:
            ignore, reason = self.should_ignore_file(info)
            if ignore:
                logger.debug("Ignoring %s: %s", info.path, reason)
                continue
            if not info.fast_hash or not info.fast_hash.strip():
                logger.debug("No valid hash for %s", info.path)
                continue
            key = create_enhanced_duplicate_key(info.fast_hash, info.file_size)
            families.setdefault(key, []).append(info)
        return {k: v for k, v in families.items() if len(v) > 1}

    # ------------------------------------------------------------------
    def is_duplicate_pair(self, file1: FileInfo, file2: FileInfo) -> bool:
        """Return True if the two file infos represent duplicates."""
        return detect_duplicates(
            file1.fast_hash,
            file1.file_size,
            file2.fast_hash,
            file2.file_size,
        )

    # ------------------------------------------------------------------
    def identify_source(self, duplicate_group: List[FileInfo]) -> FileInfo:
        """Identify the original source file within a duplicate group."""
        if not duplicate_group:
            raise ValueError("Empty duplicate group")
        if len(duplicate_group) == 1:
            return duplicate_group[0]
        sorted_files = sorted(
            duplicate_group, key=lambda f: self._parse_creation_time(f.creation_time)
        )
        source_file = sorted_files[0]
        logger.info(
            "Source identified: %s (created: %s)",
            source_file.path,
            source_file.creation_time,
        )
        return source_file

    # ------------------------------------------------------------------
    def get_copy_statistics(self, duplicate_group: List[FileInfo]) -> Dict[str, Any]:
        """Return metrics about copies within a duplicate family."""
        if not duplicate_group:
            raise ValueError("Empty duplicate group")

        source = self.identify_source(duplicate_group)
        copies = [f for f in duplicate_group if f.id != source.id]

        family_id = create_enhanced_duplicate_key(
            source.fast_hash or "", source.file_size
        )

        return {
            "family_id": family_id,
            "total_files": len(duplicate_group),
            "source_file": {
                "path": source.path,
                "creation_date": self._parse_creation_time(source.creation_time),
                "is_original": True,
            },
            "copies": [
                {
                    "path": c.path,
                    "creation_date": self._parse_creation_time(c.creation_time),
                    "is_original": False,
                }
                for c in copies
            ],
            "copies_count": len(copies),
        }

    # ------------------------------------------------------------------
    def get_duplicate_statistics(
        self, duplicate_families: Dict[str, List[FileInfo]]
    ) -> Dict[str, Any]:
        """Compute statistics about detected duplicates."""
        if not duplicate_families:
            return {
                "total_families": 0,
                "total_duplicates": 0,
                "total_sources": 0,
                "total_copies": 0,
                "space_wasted_bytes": 0,
                "space_wasted_mb": 0,
                "largest_family_size": 0,
                "families_by_size": {},
                "average_family_size": 0,
            }
        total_families = len(duplicate_families)
        total_files = sum(len(f) for f in duplicate_families.values())
        total_sources = total_families
        total_copies = total_files - total_sources
        space_wasted = 0
        largest_family = 0
        families_by_size: Dict[int, int] = {}
        for fam in duplicate_families.values():
            family_size = len(fam)
            largest_family = max(largest_family, family_size)
            families_by_size[family_size] = families_by_size.get(family_size, 0) + 1
            if fam:
                file_size = fam[0].file_size
                space_wasted += (family_size - 1) * file_size
        return {
            "total_families": total_families,
            "total_duplicates": total_files,
            "total_sources": total_sources,
            "total_copies": total_copies,
            "space_wasted_bytes": space_wasted,
            "space_wasted_mb": round(space_wasted / (1024 * 1024), 2),
            "largest_family_size": largest_family,
            "families_by_size": families_by_size,
            "average_family_size": (
                round(total_files / total_families, 2) if total_families else 0
            ),
        }

    # ------------------------------------------------------------------
    def _parse_creation_time(self, time_str: Optional[str]) -> datetime:
        if not time_str:
            return datetime.max
        formats = ["%d/%m/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"]
        for fmt in formats:
            try:
                return datetime.strptime(time_str.strip(), fmt)
            except ValueError:
                continue
        logger.warning("Could not parse date: %s", time_str)
        return datetime.max


__all__ = [
    "FileInfo",
    "DuplicateDetector",
]
