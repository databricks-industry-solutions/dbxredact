"""Tests for gliner_detector.py -- helper functions with mocked model."""

import pytest
from dbxredact.gliner_detector import (
    _merge_adjacent_names,
    _build_offset_map,
    _chunk_and_predict,
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

    def test_leading_trailing_whitespace(self):
        mapping = _build_offset_map("  hello  ")
        assert len(mapping) == 5

    def test_collapsed_interior_whitespace(self):
        mapping = _build_offset_map("a  b")
        assert len(mapping) == 3

    def test_empty_string(self):
        mapping = _build_offset_map("")
        assert mapping == []

    def test_tabs_and_newlines(self):
        mapping = _build_offset_map("a\t\nb")
        assert len(mapping) == 3


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
