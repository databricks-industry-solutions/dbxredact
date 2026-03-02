"""Utility functions for PHI/PII detection and matching."""

import re
from typing import List

from rapidfuzz import fuzz

_WHITESPACE_RE = re.compile(r"\s+")


def build_offset_map(original: str) -> List[int]:
    """Map each character position in normalized text to its position in original.

    Mirrors the behaviour of ``re.sub(r"\\s+", " ", original).strip()``:
    leading/trailing whitespace is dropped and interior runs of whitespace
    collapse to a single space.
    """
    mapping: List[int] = []
    oi = 0
    while oi < len(original) and original[oi] in " \t\n\r":
        oi += 1
    in_ws = False
    for oi_scan in range(oi, len(original)):
        ch = original[oi_scan]
        if ch in " \t\n\r":
            if not in_ws:
                mapping.append(oi_scan)
                in_ws = True
        else:
            mapping.append(oi_scan)
            in_ws = False
    while mapping and original[mapping[-1]] in " \t\n\r":
        mapping.pop()
    return mapping


def is_fuzzy_match(str1: str, str2: str, threshold: int = 50) -> bool:
    """
    Check if two strings are similar using fuzzy matching.

    Uses token set ratio similarity score to handle word order variations
    and partial matches. Case-insensitive comparison.

    Args:
        str1: First string to compare
        str2: Second string to compare
        threshold: Minimum similarity score (0-100) to consider a match

    Returns:
        True if the token set ratio similarity score is >= threshold
    """
    if not str1 or not str2:
        return False

    score = fuzz.token_set_ratio(str1.lower(), str2.lower())
    return score >= threshold


def is_overlap(
    start1: int, end1: int, start2: int, end2: int, tolerance: int = 0
) -> bool:
    """
    Check if two half-open intervals overlap.

    Args:
        start1: Start position of first interval
        end1: End position of first interval (exclusive)
        start2: Start position of second interval
        end2: End position of second interval (exclusive)
        tolerance: Extends each end position by this amount (default: 0)

    Returns:
        True if intervals [start1, end1) and [start2, end2) overlap
    """
    return max(start1, start2) < min(end1, end2) + tolerance


def calculate_overlap(start1: int, end1: int, start2: int, end2: int) -> int:
    """
    Calculate the length of overlap between two intervals.

    Args:
        start1: Start position of first interval
        end1: End position of first interval
        start2: Start position of second interval
        end2: End position of second interval

    Returns:
        Length of the overlap (negative if no overlap)
    """
    return min(end1, end2) - max(start1, start2)


def calculate_string_overlap(s1: str, s2: str) -> float:
    """
    Calculate the normalized overlap between two strings.

    Finds the maximum suffix-prefix overlap between the strings and
    normalizes by the length of the shorter string.

    Args:
        s1: First string
        s2: Second string

    Returns:
        Normalized overlap ratio (0.0 to 1.0)
    """
    if not s1 or not s2:
        return 0.0

    max_overlap = min(len(s1), len(s2))
    for i in range(max_overlap, 0, -1):
        if s1[-i:] == s2[:i]:
            return i / max_overlap
    return 0.0

