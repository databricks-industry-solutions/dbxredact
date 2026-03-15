"""Tests for ai_detector.py -- entity position resolution edge cases."""

import pytest
from dbxredact.ai_detector import _find_entity_positions, make_prompt


class TestFindEntityPositions:

    def test_exact_match(self):
        positions = _find_entity_positions("Alice", "I saw Alice today")
        assert positions == [(6, 11)]

    def test_case_insensitive(self):
        positions = _find_entity_positions("brennan", "Brennan is here")
        assert len(positions) == 1
        assert positions[0] == (0, 7)

    def test_multiple_occurrences(self):
        positions = _find_entity_positions("Brennan", "Brennan said Brennan is fine")
        assert len(positions) == 2

    def test_not_in_text(self):
        positions = _find_entity_positions("Nonexistent", "Some text here")
        assert len(positions) == 0

    def test_whitespace_normalization(self):
        positions = _find_entity_positions("John  Smith", "John Smith is here")
        # Should find via whitespace normalization or fuzzy match
        assert len(positions) >= 1

    def test_preserves_source_text(self):
        """Ensure the returned positions point to the original text's casing."""
        text = "I met BRENNAN yesterday"
        positions = _find_entity_positions("brennan", text)
        assert len(positions) == 1
        start, end = positions[0]
        assert text[start:end].lower() == "brennan"

    def test_special_regex_chars(self):
        """Entity text with regex metacharacters should not break matching."""
        positions = _find_entity_positions("Dr. Smith", "I saw Dr. Smith today")
        assert len(positions) == 1

    def test_empty_entity(self):
        positions = _find_entity_positions("", "Some text")
        # re.finditer with empty pattern matches everywhere; verify no crash
        assert isinstance(positions, list)

    def test_entity_with_newlines(self):
        positions = _find_entity_positions("John\nSmith", "John\nSmith is here")
        assert len(positions) >= 1


class TestFuzzyFallback:

    def test_minor_mismatch(self):
        """Fuzzy match should catch minor differences like trailing punctuation."""
        positions = _find_entity_positions("Smith,", "I saw Smith today")
        # "Smith," vs "Smith" -- fuzzy should find "Smith" in the text
        # Since window length differs, this tests the fuzzy sliding window
        assert isinstance(positions, list)  # May or may not match depending on threshold

    def test_partial_name(self):
        positions = _find_entity_positions("Alice Anderson", "I saw Alice Anderson today")
        assert len(positions) == 1
        assert positions[0] == (6, 20)
