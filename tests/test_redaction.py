"""Unit tests for redaction functions."""

import sys
from unittest.mock import MagicMock

_pyspark_mods = [
    "pyspark", "pyspark.sql", "pyspark.sql.functions", "pyspark.sql.types",
    "pyspark.sql.streaming",
]
for _mod in _pyspark_mods:
    if _mod not in sys.modules:
        sys.modules[_mod] = MagicMock()

import pytest

from dbxredact.redaction import redact_text, _safe_entity_list


class TestRedactText:
    """Tests for text redaction function."""

    def test_generic_redaction(self):
        """Test generic redaction strategy."""
        text = "John Smith emailed at test@email.com"
        entities = [
            {"entity": "John Smith", "start": 0, "end": 10, "entity_type": "PERSON"},
            {"entity": "test@email.com", "start": 22, "end": 36, "entity_type": "EMAIL"},
        ]

        result = redact_text(text, entities, strategy="generic")

        assert "[REDACTED]" in result
        assert "John Smith" not in result
        assert "test@email.com" not in result

    def test_typed_redaction(self):
        """Test typed redaction strategy."""
        text = "John Smith emailed at test@email.com"
        entities = [
            {"entity": "John Smith", "start": 0, "end": 10, "entity_type": "PERSON"},
            {"entity": "test@email.com", "start": 22, "end": 36, "entity_type": "EMAIL"},
        ]

        result = redact_text(text, entities, strategy="typed")

        assert "[PERSON]" in result
        assert "[EMAIL]" in result
        assert "John Smith" not in result
        assert "test@email.com" not in result

    def test_no_entities(self):
        """Test redaction with no entities."""
        text = "This is a test."
        entities = []

        result = redact_text(text, entities, strategy="generic")

        assert result == text

    def test_empty_text(self):
        """Test redaction with empty text."""
        text = ""
        entities = []

        result = redact_text(text, entities, strategy="generic")

        assert result == ""

    def test_overlapping_entities(self):
        """Test redaction with overlapping entities."""
        text = "John Smith lives here"
        entities = [
            {"entity": "John Smith", "start": 0, "end": 10, "entity_type": "PERSON"},
            {"entity": "John", "start": 0, "end": 4, "entity_type": "NAME"},
        ]

        # Should handle overlapping entities (sorted by start position in reverse)
        result = redact_text(text, entities, strategy="generic")

        assert "John" not in result

    def test_entity_at_end(self):
        """Test redaction when entity is at end of text."""
        text = "Contact John"
        entities = [
            {"entity": "John", "start": 8, "end": 12, "entity_type": "PERSON"},
        ]

        result = redact_text(text, entities, strategy="typed")

        assert result == "Contact [PERSON]"

    def test_multiple_entities_same_type(self):
        """Test redaction with multiple entities of same type."""
        text = "John and Jane are here"
        entities = [
            {"entity": "John", "start": 0, "end": 4, "entity_type": "PERSON"},
            {"entity": "Jane", "start": 9, "end": 13, "entity_type": "PERSON"},
        ]

        result = redact_text(text, entities, strategy="typed")

        assert result == "[PERSON] and [PERSON] are here"

    def test_entity_missing_fields(self):
        """Test redaction handles entities with missing fields."""
        text = "John Smith here"
        entities = [
            {"entity": "John Smith", "entity_type": "PERSON"},  # Missing start/end
        ]

        result = redact_text(text, entities, strategy="generic")

        # Should return original text when entity has missing fields
        assert result == text

    def test_entity_missing_entity_type(self):
        """Test redaction handles entity with missing type."""
        text = "John Smith here"
        entities = [
            {"entity": "John Smith", "start": 0, "end": 10},  # Missing entity_type
        ]

        result = redact_text(text, entities, strategy="typed")

        # Should use REDACTED as default type
        assert "[REDACTED]" in result

    def test_overlapping_spans_both_redacted(self):
        """Overlapping spans should be merged so all PII is covered."""
        text = "John Smith lives here"
        entities = [
            {"start": 0, "end": 4, "entity_type": "FIRST_NAME"},
            {"start": 0, "end": 10, "entity_type": "PERSON"},
        ]
        result = redact_text(text, entities, strategy="generic")
        assert "John" not in result
        assert "Smith" not in result
        assert result.count("[REDACTED]") == 1

    def test_adjacent_spans_merged(self):
        """Adjacent/touching spans should be merged into one redaction."""
        text = "AB CD EF"
        entities = [
            {"start": 0, "end": 2, "entity_type": "X"},
            {"start": 2, "end": 5, "entity_type": "Y"},
        ]
        result = redact_text(text, entities, strategy="generic")
        assert result.count("[REDACTED]") == 1

    def test_bounds_clamping_end_beyond_text(self):
        """Entity end beyond text length should be clamped, not error."""
        text = "Hello"
        entities = [{"start": 0, "end": 999, "entity_type": "PERSON"}]
        result = redact_text(text, entities, strategy="generic")
        assert result == "[REDACTED]"

    def test_bounds_clamping_negative_start(self):
        """Negative start should be clamped to 0."""
        text = "Alice here"
        entities = [{"start": -5, "end": 5, "entity_type": "PERSON"}]
        result = redact_text(text, entities, strategy="generic")
        assert result == "[REDACTED] here"

    def test_zero_length_span_skipped(self):
        """Span with start >= end after clamping should be silently skipped."""
        text = "Hello world"
        entities = [{"start": 5, "end": 5, "entity_type": "X"}]
        result = redact_text(text, entities, strategy="generic")
        assert result == "Hello world"


class TestSafeEntityList:
    """_safe_entity_list converts UDF inputs to list-of-dicts. A bug here
    means redaction silently does nothing (PII leaks)."""

    def test_none_returns_empty(self):
        assert _safe_entity_list(None) == []

    def test_empty_list_returns_empty(self):
        assert _safe_entity_list([]) == []

    def test_dict_list_passthrough(self):
        entities = [{"entity": "John", "start": 0, "end": 4}]
        result = _safe_entity_list(entities)
        assert result == entities

    def test_mock_row_converted_to_dict(self):
        class FakeRow:
            def asDict(self):
                return {"entity": "Alice", "start": 0, "end": 5}
        result = _safe_entity_list([FakeRow()])
        assert len(result) == 1
        assert result[0] == {"entity": "Alice", "start": 0, "end": 5}

    def test_non_iterable_returns_empty(self):
        assert _safe_entity_list(42) == []

    def test_tuple_of_dicts_converted(self):
        entities = ({"entity": "X", "start": 0, "end": 1},)
        result = _safe_entity_list(entities)
        assert len(result) == 1
        assert result[0]["entity"] == "X"
