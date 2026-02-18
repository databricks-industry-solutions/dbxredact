"""Unit tests for redaction functions."""

import pytest

from dbxredact.redaction import redact_text


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
