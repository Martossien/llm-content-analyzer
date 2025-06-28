"""Utilities for duplicate detection using FastHash and file size."""

from __future__ import annotations


def create_enhanced_duplicate_key(fast_hash: str, file_size: int | None) -> str:
    """Return composite key combining fast hash and file size.

    Args:
        fast_hash: Hash of the first 64KB of the file.
        file_size: Exact file size in bytes.

    Returns:
        Composite key formatted as "fasthash_size".
    """
    if not fast_hash or file_size is None:
        return fast_hash or ""
    return f"{fast_hash}_{file_size}"
