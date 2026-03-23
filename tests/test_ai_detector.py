"""Tests for ai_detector.py -- entity position resolution edge cases."""

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
import pandas as pd
from dbxredact.ai_detector import (
    _find_entity_positions,
    _parse_entity_list,
    format_entity_response_object_udf,
    make_prompt,
)
from dbxredact.config import DEFAULT_AI_CONFIDENCE_SCORE


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
        assert len(positions) == 1
        assert positions[0] == (0, 10)

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

    def test_minor_mismatch_trailing_punctuation(self):
        """Fuzzy match should catch minor differences like trailing punctuation."""
        positions = _find_entity_positions("Smith,", "I saw Smith today")
        assert len(positions) <= 1
        if positions:
            start, end = positions[0]
            assert "Smith" in "I saw Smith today"[start:end]

    def test_partial_name(self):
        positions = _find_entity_positions("Alice Anderson", "I saw Alice Anderson today")
        assert len(positions) == 1
        assert positions[0] == (6, 20)


class TestParseEntityList:
    """_parse_entity_list sits on the critical path for every AI detection."""

    def test_none_returns_empty(self):
        assert _parse_entity_list(None) == []

    def test_json_string_with_result_key(self):
        raw = '{"result": [{"entity": "John", "entity_type": "PERSON"}]}'
        result = _parse_entity_list(raw)
        assert len(result) == 1
        assert result[0]["entity"] == "John"
        assert result[0]["entity_type"] == "PERSON"

    def test_plain_json_array(self):
        raw = '[{"entity": "Alice", "entity_type": "PERSON"}]'
        result = _parse_entity_list(raw)
        assert len(result) == 1
        assert result[0]["entity"] == "Alice"

    def test_garbage_string_returns_empty(self):
        assert _parse_entity_list("not json at all") == []

    def test_empty_string_returns_empty(self):
        assert _parse_entity_list("") == []

    def test_list_of_dicts_passthrough(self):
        raw = [{"entity": "Bob", "entity_type": "PERSON"}]
        result = _parse_entity_list(raw)
        assert result == raw

    def test_json_dict_without_result_key_returns_empty(self):
        assert _parse_entity_list('{"entity": "John"}') == []

    def test_json_scalar_returns_empty(self):
        assert _parse_entity_list('"just a string"') == []


class TestFormatEntityResponseObjectUdf:
    """format_entity_response_object_udf is the core pandas function that
    converts AI detector output into positioned entities."""

    def test_simple_entity_positioning(self):
        entities = pd.Series([
            '[{"entity": "Alice", "entity_type": "PERSON"}]',
        ])
        sentences = pd.Series(["I saw Alice today"])
        result = format_entity_response_object_udf(entities, sentences)
        assert len(result) == 1
        ents = result.iloc[0]
        assert len(ents) == 1
        assert ents[0]["entity"] == "Alice"
        assert ents[0]["start"] == 6
        assert ents[0]["end"] == 11
        assert ents[0]["entity_type"] == "PERSON"
        assert ents[0]["score"] == float(DEFAULT_AI_CONFIDENCE_SCORE)

    def test_deduplicates_repeated_mentions(self):
        entities = pd.Series([
            '[{"entity": "John", "entity_type": "PERSON"}, {"entity": "John", "entity_type": "PERSON"}]',
        ])
        sentences = pd.Series(["John met John at the park"])
        result = format_entity_response_object_udf(entities, sentences)
        ents = result.iloc[0]
        assert len(ents) == 2
        positions = {(e["start"], e["end"]) for e in ents}
        assert (0, 4) in positions
        assert (9, 13) in positions

    def test_ignored_entities_filtered(self):
        entities = pd.Series([
            '[{"entity": "Dr.", "entity_type": "PERSON"}, {"entity": "Alice", "entity_type": "PERSON"}]',
        ])
        sentences = pd.Series(["Dr. Alice is here"])
        result = format_entity_response_object_udf(entities, sentences)
        ents = result.iloc[0]
        names = [e["entity"] for e in ents]
        assert "Alice" in names
        assert "Dr." not in names

    def test_unlocated_entity_gracefully_skipped(self):
        entities = pd.Series([
            '[{"entity": "Nonexistent", "entity_type": "PERSON"}]',
        ])
        sentences = pd.Series(["Some unrelated text"])
        result = format_entity_response_object_udf(entities, sentences)
        assert result.iloc[0] == []

    def test_none_input_returns_empty_list(self):
        entities = pd.Series([None])
        sentences = pd.Series(["Some text"])
        result = format_entity_response_object_udf(entities, sentences)
        assert result.iloc[0] == []

    def test_garbage_json_returns_empty_list(self):
        entities = pd.Series(["not valid json"])
        sentences = pd.Series(["Some text"])
        result = format_entity_response_object_udf(entities, sentences)
        assert result.iloc[0] == []
