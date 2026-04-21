"""Tests for AI query text chunking helpers (pure Python)."""

from dbxredact.ai_detector import _deduplicate_chunk_entities, _offset_entities


class TestOffsetEntities:

    def test_offset_entities(self):
        ents = [{"entity": "a", "entity_type": "X", "start": 0, "end": 1}]
        out = _offset_entities(ents, 5)
        assert out[0]["start"] == 5
        assert out[0]["end"] == 6

    def test_offset_entities_zero_noop(self):
        ents = [{"entity": "a", "entity_type": "X", "start": 2, "end": 3}]
        assert _offset_entities(ents, 0) is ents

    def test_offset_entities_empty(self):
        assert _offset_entities([], 10) == []


class TestDeduplicateChunkEntities:

    def test_deduplicate_exact_match(self):
        a = {"entity": "x", "entity_type": "PERSON", "start": 0, "end": 3}
        b = {"entity": "x", "entity_type": "PERSON", "start": 0, "end": 3}
        out = _deduplicate_chunk_entities([a, b])
        assert len(out) == 1
        assert out[0]["start"] == 0 and out[0]["end"] == 3

    def test_deduplicate_overlapping_same_type(self):
        shorter = {"entity": "ab", "entity_type": "PERSON", "start": 0, "end": 2}
        longer = {"entity": "abc", "entity_type": "PERSON", "start": 0, "end": 3}
        out = _deduplicate_chunk_entities([shorter, longer])
        assert len(out) == 1
        assert out[0]["end"] == 3

    def test_deduplicate_different_types_preserved(self):
        a = {"entity": "x", "entity_type": "PERSON", "start": 0, "end": 3}
        b = {"entity": "x", "entity_type": "ORG", "start": 1, "end": 3}
        out = _deduplicate_chunk_entities([a, b])
        assert len(out) == 2

    def test_deduplicate_empty(self):
        assert _deduplicate_chunk_entities([]) == []
