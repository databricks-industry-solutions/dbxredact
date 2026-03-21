"""Tests for gliner_detector.py -- helper functions with mocked model."""

import pytest
from dbxredact.gliner_detector import (
    _merge_adjacent_names,
    _build_offset_map,
    _chunk_and_predict,
    _map_label,
)


class TestMergeAdjacentNames:

    def test_merge_first_last(self):
        entities = [
            {"text": "John", "label": "first_name", "start": 0, "end": 4, "score": 0.9},
            {"text": "Smith", "label": "last_name", "start": 5, "end": 10, "score": 0.8},
        ]
        merged = _merge_adjacent_names(entities)
        assert len(merged) == 1
        assert merged[0]["text"] == "John Smith"
        assert merged[0]["start"] == 0
        assert merged[0]["end"] == 10
        assert merged[0]["score"] == 0.8

    def test_no_merge_same_label(self):
        entities = [
            {"text": "John", "label": "first_name", "start": 0, "end": 4, "score": 0.9},
            {"text": "James", "label": "first_name", "start": 5, "end": 10, "score": 0.8},
        ]
        merged = _merge_adjacent_names(entities)
        assert len(merged) == 2

    def test_no_merge_too_far_apart(self):
        entities = [
            {"text": "John", "label": "first_name", "start": 0, "end": 4, "score": 0.9},
            {"text": "Smith", "label": "last_name", "start": 20, "end": 25, "score": 0.8},
        ]
        merged = _merge_adjacent_names(entities)
        assert len(merged) == 2

    def test_empty_list(self):
        assert _merge_adjacent_names([]) == []

    def test_single_entity(self):
        entities = [{"text": "John", "label": "first_name", "start": 0, "end": 4, "score": 0.9}]
        assert len(_merge_adjacent_names(entities)) == 1


class TestBuildOffsetMap:

    def test_simple_text(self):
        mapping = _build_offset_map("hello world")
        assert len(mapping) == 11
        assert mapping[0] == 0   # 'h'
        assert mapping[5] == 5   # ' '
        assert mapping[6] == 6   # 'w'

    def test_leading_trailing_whitespace(self):
        # "  hello  " normalizes to "hello" (len 5)
        mapping = _build_offset_map("  hello  ")
        assert len(mapping) == 5
        assert mapping[0] == 2   # 'h' at original pos 2
        assert mapping[4] == 6   # 'o' at original pos 6

    def test_collapsed_interior_whitespace(self):
        # "a  b" normalizes to "a b" (len 3)
        mapping = _build_offset_map("a  b")
        assert len(mapping) == 3
        assert mapping[0] == 0   # 'a' at original pos 0
        assert mapping[1] == 1   # ' ' maps to first space at original pos 1
        assert mapping[2] == 3   # 'b' at original pos 3

    def test_empty_string(self):
        assert _build_offset_map("") == []

    def test_tabs_and_newlines(self):
        # "a\t\nb" normalizes to "a b" (len 3)
        mapping = _build_offset_map("a\t\nb")
        assert len(mapping) == 3
        assert mapping[0] == 0   # 'a'
        assert mapping[2] == 3   # 'b'


class TestChunkAndPredict:

    def test_short_text_no_chunking(self):
        class MockModel:
            def predict_entities(self, text, labels, threshold=0.5):
                return [{"text": "John", "label": "person", "start": 0, "end": 4, "score": 0.9}]

        result = _chunk_and_predict(MockModel(), "John went home", ["person"], 0.5)
        assert len(result) == 1
        assert result[0]["text"] == "John"

    def test_deduplication(self):
        call_count = 0

        class MockModel:
            def predict_entities(self, text, labels, threshold=0.5):
                nonlocal call_count
                call_count += 1
                return [{"text": "John", "label": "person", "start": 0, "end": 4, "score": 0.9}]

        result = _chunk_and_predict(MockModel(), "John", ["person"], 0.5)
        assert len(result) == 1

    def test_long_text_triggers_chunking(self):
        """Verify chunking is triggered when text exceeds MAX_WORDS."""
        words = ["word"] * 600
        text = " ".join(words)
        call_count = 0

        class MockModel:
            def predict_entities(self, text, labels, threshold=0.5):
                nonlocal call_count
                call_count += 1
                return []

        _chunk_and_predict(MockModel(), text, ["person"], 0.5)
        assert call_count > 1

    def test_chunk_offset_adjustment(self):
        """Entities from later chunks should have adjusted offsets."""
        words = ["word"] * 600
        text = " ".join(words)

        class MockModel:
            def predict_entities(self, text_input, labels, threshold=0.5):
                if text_input.startswith("word word"):
                    return [{"text": "word", "label": "person", "start": 0, "end": 4, "score": 0.9}]
                return []

        result = _chunk_and_predict(MockModel(), text, ["person"], 0.5)
        assert len(result) >= 2
        starts = sorted(e["start"] for e in result)
        assert starts[0] == 0
        assert starts[-1] > 0, "Later chunks should produce offset-adjusted entities"
        for e in result:
            assert e["end"] - e["start"] == 4


class TestMapLabel:

    def test_known_label(self):
        assert _map_label("first_name") == "PERSON"
        assert _map_label("email") == "EMAIL_ADDRESS"
        assert _map_label("ssn") == "US_SSN"

    def test_hospital_maps_to_hospital_name(self):
        assert _map_label("hospital_or_medical_facility") == "HOSPITAL_NAME"

    def test_unknown_label_uppercased(self):
        assert _map_label("custom type") == "CUSTOM_TYPE"


class TestOffsetMapRoundTrip:

    def test_remap_preserves_text(self):
        original = "John  Smith\nfrom  NYC"
        mapping = _build_offset_map(original)
        import re
        normalized = re.sub(r"\s+", " ", original).strip()
        for ni, oi in enumerate(mapping):
            if ni < len(normalized):
                nc, oc = normalized[ni], original[oi]
                if nc == " ":
                    assert oc in " \t\n\r", f"pos {ni}: expected whitespace, got {oc!r}"
                else:
                    assert nc == oc, f"pos {ni}: {nc!r} != {oc!r}"
