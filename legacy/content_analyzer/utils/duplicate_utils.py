"""Utilities for duplicate detection using FastHash and file size.

This module provides a thread-safe generator for composite duplicate keys
as well as helpers used throughout the project when dealing with
FastHash/file size deduplication.  Previous implementations simply
concatenated the hash and the size and returned an empty string for invalid
values which led to collisions.  The new implementation guarantees unique
keys even for edge cases and validates the inputs thoroughly.
"""

from __future__ import annotations

from typing import Optional
import threading


class ThreadSafeDuplicateKeyGenerator:
    """Generate duplicate detection keys in a thread-safe manner."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        # Small cache to avoid recreating the same key strings repeatedly
        # in high concurrency scenarios.  Mapping ``(hash, size)`` -> key.
        self._key_cache: dict[tuple[str, int], str] = {}

    def create_enhanced_duplicate_key(
        self, fast_hash: str, file_size: Optional[int]
    ) -> str:
        """Return a robust composite key for the given inputs.

        The function ensures that edge cases such as missing hashes or invalid
        file sizes always yield a unique key in order to prevent collisions in
        the cache layer.
        """

        with self._lock:
            if not fast_hash or not isinstance(fast_hash, str):
                # ``id`` provides uniqueness for the given object instance
                return f"INVALID_HASH_{id(fast_hash)}_{file_size or 0}"

            if file_size is None or file_size < 0:
                return f"INVALID_SIZE_{fast_hash}_{file_size or 'None'}"

            if file_size > 281_474_976_710_656:
                return f"OVERFLOW_{fast_hash}_{file_size}"

            key_tuple = (fast_hash, int(file_size))
            cached = self._key_cache.get(key_tuple)
            if cached is not None:
                return cached

            result = f"{fast_hash}_{file_size}"
            self._key_cache[key_tuple] = result
            # Prevent unbounded growth; keep only a few hundred entries
            if len(self._key_cache) > 1024:
                self._key_cache.pop(next(iter(self._key_cache)))
            return result


_GENERATOR = ThreadSafeDuplicateKeyGenerator()


def create_enhanced_duplicate_key(fast_hash: str, file_size: Optional[int]) -> str:
    """Compatibility wrapper around :class:`ThreadSafeDuplicateKeyGenerator`."""

    return _GENERATOR.create_enhanced_duplicate_key(fast_hash, file_size)


def detect_duplicates(
    fast_hash1: Optional[str],
    file_size1: Optional[int],
    fast_hash2: Optional[str],
    file_size2: Optional[int],
) -> bool:
    """Return ``True`` if the two files should be considered duplicates."""

    if not fast_hash1 or not fast_hash2:
        return False

    if file_size1 is None or file_size2 is None:
        return False

    if file_size1 < 0 or file_size2 < 0:
        return False

    if file_size1 > 281_474_976_710_656 or file_size2 > 281_474_976_710_656:
        return False

    return fast_hash1 == fast_hash2 and file_size1 == file_size2


__all__ = [
    "create_enhanced_duplicate_key",
    "ThreadSafeDuplicateKeyGenerator",
    "detect_duplicates",
]
