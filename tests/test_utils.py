"""Unit tests for utility functions."""

import pytest

from dbxredact.utils import (
    is_fuzzy_match,
    is_overlap,
    calculate_overlap,
    calculate_string_overlap,
)


class TestIsFuzzyMatch:
    """Tests for fuzzy string matching."""

    def test_exact_match(self):
        """Exact strings should match."""
        assert is_fuzzy_match("John Smith", "John Smith", threshold=80)

    def test_case_insensitive_match(self):
        """Case differences should still match."""
        assert is_fuzzy_match("john smith", "JOHN SMITH", threshold=80)

    def test_word_order_match(self):
        """Different word orders should match with token set ratio."""
        assert is_fuzzy_match("John Smith", "Smith John", threshold=80)

    def test_partial_match(self):
        """Partial matches should work with lower threshold."""
        assert is_fuzzy_match("John", "John Smith", threshold=50)

    def test_no_match(self):
        """Different strings should not match."""
        assert not is_fuzzy_match("Alice", "Bob", threshold=80)

    def test_empty_strings(self):
        """Empty strings should not match."""
        assert not is_fuzzy_match("", "test")
        assert not is_fuzzy_match("test", "")
        assert not is_fuzzy_match("", "")


class TestIsOverlap:
    """Tests for half-open interval [start, end) overlap detection."""

    def test_complete_overlap(self):
        """Completely overlapping intervals should return True."""
        assert is_overlap(0, 10, 0, 10)

    def test_partial_overlap(self):
        """Partially overlapping intervals should return True."""
        assert is_overlap(0, 10, 5, 15)

    def test_contained_interval(self):
        """Interval contained in another should return True."""
        assert is_overlap(0, 20, 5, 15)

    def test_no_overlap(self):
        """Non-overlapping intervals should return False."""
        assert not is_overlap(0, 5, 10, 15)

    def test_adjacent_intervals(self):
        """Adjacent intervals [0,5) and [6,10) should not overlap."""
        assert not is_overlap(0, 5, 6, 10)

    def test_adjacent_with_tolerance(self):
        """Adjacent intervals should overlap with sufficient tolerance."""
        assert is_overlap(0, 5, 6, 10, tolerance=2)

    def test_adjacent_intervals_exclusive(self):
        """[0,5) and [5,10) are adjacent (not overlapping) with exclusive end."""
        assert not is_overlap(0, 5, 5, 10)


class TestCalculateOverlap:
    """Tests for overlap length calculation."""

    def test_complete_overlap(self):
        """Completely overlapping intervals."""
        assert calculate_overlap(0, 10, 0, 10) == 10

    def test_partial_overlap(self):
        """Partially overlapping intervals."""
        assert calculate_overlap(0, 10, 5, 15) == 5

    def test_no_overlap(self):
        """Non-overlapping intervals return negative."""
        assert calculate_overlap(0, 5, 10, 15) < 0

    def test_contained_interval(self):
        """Interval contained in another."""
        assert calculate_overlap(0, 20, 5, 10) == 5


class TestCalculateStringOverlap:
    """Tests for string overlap calculation."""

    def test_no_overlap(self):
        """Strings with no overlap."""
        assert calculate_string_overlap("hello", "world") == 0.0

    def test_empty_strings(self):
        """Empty strings return 0."""
        assert calculate_string_overlap("", "test") == 0.0
        assert calculate_string_overlap("test", "") == 0.0

    def test_suffix_prefix_overlap(self):
        """Strings with suffix-prefix overlap."""
        result = calculate_string_overlap("hello", "lowing")
        assert result > 0.0
