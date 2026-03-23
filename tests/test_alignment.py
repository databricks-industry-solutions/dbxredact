"""Unit tests for entity alignment module."""

import pytest

from dbxredact.alignment import (
    Entity,
    MatchType,
    normalize_entity,
    calculate_match_score,
    find_best_match,
    merge_entities,
    calculate_confidence,
    MultiSourceAligner,
    align_entities_multi_source,
    align_entities_row,
    _merge_overlapping_spans,
)
from dbxredact.config import EXACT_MATCH_SCORE, OVERLAP_MATCH_SCORE
from dbxredact.redaction import redact_text


class TestEntityNormalization:
    """Tests for entity normalization."""

    def test_normalize_basic_entity(self):
        """Test normalization of entity with minimal fields."""
        entity_dict = {"entity": "John Smith", "start": 0, "end": 10}
        entity = normalize_entity(entity_dict, "presidio", "doc1")

        assert entity.entity == "John Smith"
        assert entity.start == 0
        assert entity.end == 10
        assert entity.source == "presidio"
        assert entity.doc_id == "doc1"

    def test_normalize_entity_with_extra_fields(self):
        """Test that extra fields don't cause errors."""
        entity_dict = {
            "entity": "John",
            "start": 0,
            "end": 4,
            "entity_type": "PERSON",
            "extra_field": "should_not_error",
        }
        entity = normalize_entity(entity_dict, "ai", "doc1")

        assert entity.entity == "John"
        assert entity.entity_type == "PERSON"
        assert entity.extra_fields["extra_field"] == "should_not_error"

    def test_normalize_entity_missing_required_field(self):
        """Test that missing required fields raise ValueError."""
        entity_dict = {"entity": "John", "start": 0}  # Missing 'end'

        with pytest.raises(ValueError, match="Missing required fields"):
            normalize_entity(entity_dict, "presidio")


class TestMatchScoring:
    """Tests for match scoring logic."""

    def test_exact_match(self):
        """Test exact match detection."""
        e1 = Entity("John Smith", 0, 10, "PERSON")
        e2 = Entity("John Smith", 0, 10, "PERSON")

        score, match_type = calculate_match_score(e1, e2)

        assert score == EXACT_MATCH_SCORE
        assert match_type == MatchType.EXACT

    def test_overlap_fuzzy_match(self):
        """Test overlap with fuzzy text match."""
        e1 = Entity("John Smith", 0, 10, "PERSON")
        e2 = Entity("Smith John", 0, 10, "PERSON")

        score, match_type = calculate_match_score(e1, e2, fuzzy_threshold=80)

        assert score == OVERLAP_MATCH_SCORE
        assert match_type == MatchType.OVERLAP_FUZZY

    def test_no_match_different_positions(self):
        """Test no match when positions don't overlap."""
        e1 = Entity("John", 0, 4, "PERSON")
        e2 = Entity("Smith", 10, 15, "PERSON")

        score, match_type = calculate_match_score(e1, e2)

        assert score == 0.0
        assert match_type == MatchType.NO_MATCH


class TestFindBestMatch:
    """Tests for finding best match among candidates."""

    def test_find_exact_match(self):
        """Test finding exact match from candidates."""
        target = Entity("John", 0, 4, "PERSON")
        candidates = [
            Entity("Alice", 10, 15, "PERSON"),
            Entity("John", 0, 4, "PERSON"),
            Entity("Bob", 20, 23, "PERSON"),
        ]

        best_match, score, match_type = find_best_match(target, candidates)

        assert best_match.entity == "John"
        assert score == EXACT_MATCH_SCORE
        assert match_type == MatchType.EXACT

    def test_find_no_match(self):
        """Test when no suitable match exists."""
        target = Entity("John", 0, 4, "PERSON")
        candidates = [
            Entity("Alice", 10, 15, "PERSON"),
            Entity("Bob", 20, 23, "PERSON"),
        ]

        best_match, score, match_type = find_best_match(target, candidates)

        assert best_match is None
        assert score == 0.0
        assert match_type == MatchType.NO_MATCH


class TestMergeEntities:
    """Tests for merging entities from multiple sources."""

    def test_merge_single_entity(self):
        """Test merging a single entity."""
        entities = [Entity("John", 0, 4, "PERSON", source="presidio", score=0.9)]

        merged = merge_entities(entities, MatchType.EXACT)

        assert merged["entity"] == "John"
        assert merged["entity_type"] == "PERSON"
        assert merged["presidio_score"] == 0.9
        assert merged["sources"] == ["presidio"]

    def test_merge_two_entities(self):
        """Test merging two entities with same text."""
        entities = [
            Entity("John", 0, 4, "PERSON", source="presidio", score=0.9),
            Entity("John", 0, 4, "PERSON", source="ai"),
        ]

        merged = merge_entities(entities, MatchType.EXACT)

        assert merged["entity"] == "John"
        assert merged["presidio_score"] == 0.9
        assert set(merged["sources"]) == {"presidio", "ai"}

    def test_merge_empty_list_raises_error(self):
        """Test that merging empty list raises ValueError."""
        with pytest.raises(ValueError, match="Cannot merge empty list"):
            merge_entities([], MatchType.EXACT)


class TestMultiSourceAligner:
    """Tests for MultiSourceAligner class."""

    def test_single_source_presidio(self):
        """Test alignment with only Presidio entities."""
        aligner = MultiSourceAligner()

        presidio_entities = [
            {
                "entity": "John",
                "start": 0,
                "end": 4,
                "entity_type": "PERSON",
                "score": 0.9,
            }
        ]

        result = aligner.align(
            doc_id="doc1",
            presidio_entities=presidio_entities,
            gliner_entities=None,
            ai_entities=None,
        )

        assert len(result) == 1
        assert result[0]["entity"] == "John"
        assert result[0]["presidio_score"] == 0.9

    def test_two_sources_exact_match(self):
        """Test alignment with two sources matching exactly."""
        aligner = MultiSourceAligner()

        presidio_entities = [
            {
                "entity": "John",
                "start": 0,
                "end": 4,
                "entity_type": "PERSON",
                "score": 0.9,
            }
        ]
        ai_entities = [
            {"entity": "John", "start": 0, "end": 4, "entity_type": "PERSON"}
        ]

        result = aligner.align(
            doc_id="doc1",
            presidio_entities=presidio_entities,
            gliner_entities=None,
            ai_entities=ai_entities,
        )

        assert len(result) == 1
        assert result[0]["entity"] == "John"
        assert result[0]["presidio_score"] == 0.9

    def test_empty_sources(self):
        """Test alignment with no entities from any source."""
        aligner = MultiSourceAligner()

        result = aligner.align(
            doc_id="doc1",
            presidio_entities=None,
            gliner_entities=None,
            ai_entities=None,
        )

        assert result == []


class TestAlignEntitiesMultiSource:
    """Tests for the standalone align_entities_multi_source function."""

    def test_basic_alignment(self):
        """Test basic alignment functionality."""
        presidio_entities = [
            {
                "entity": "John",
                "start": 0,
                "end": 4,
                "entity_type": "PERSON",
                "score": 0.9,
            }
        ]
        ai_entities = [
            {"entity": "John", "start": 0, "end": 4, "entity_type": "PERSON"}
        ]

        result = align_entities_multi_source(
            presidio_entities=presidio_entities,
            gliner_entities=None,
            ai_entities=ai_entities,
            doc_id="doc1",
        )

        assert len(result) == 1
        assert result[0]["entity"] == "John"


class TestBackwardCompatibility:
    """Tests for backward compatibility with old align_entities_row function."""

    def test_align_entities_row_basic(self):
        """Test that old function still works."""
        ai_entities = [
            {"entity": "John", "start": 0, "end": 4, "entity_type": "PERSON"}
        ]
        presidio_entities = [
            {
                "entity": "John",
                "start": 0,
                "end": 4,
                "entity_type": "PERSON",
                "score": 0.9,
            }
        ]

        result = align_entities_row(
            ai_entities=ai_entities, presidio_entities=presidio_entities, doc_id="doc1"
        )

        assert len(result) == 1
        assert result[0]["entity"] == "John"
        assert result[0]["presidio_score"] == 0.9


class TestMergeOverlappingSpans:
    """Tests for _merge_overlapping_spans deduplication."""

    def test_no_overlap(self):
        entities = [
            {
                "entity": "John",
                "entity_type": "PERSON",
                "start": 0,
                "end": 4,
                "doc_id": "d1",
                "presidio_score": 0.9,
                "gliner_score": None,
                "ai_score": None,
                "confidence": "high",
            },
            {
                "entity": "NYC",
                "entity_type": "LOCATION",
                "start": 20,
                "end": 23,
                "doc_id": "d1",
                "presidio_score": 0.8,
                "gliner_score": None,
                "ai_score": None,
                "confidence": "medium",
            },
        ]
        result = _merge_overlapping_spans(entities)
        assert len(result) == 2

    def test_overlapping_spans_merged(self):
        entities = [
            {
                "entity": "Austin",
                "entity_type": "LOCATION",
                "start": 50,
                "end": 56,
                "doc_id": "d1",
                "presidio_score": 0.9,
                "gliner_score": None,
                "ai_score": None,
                "confidence": "high",
            },
            {
                "entity": "Austin,",
                "entity_type": "LOCATION",
                "start": 50,
                "end": 57,
                "doc_id": "d1",
                "presidio_score": None,
                "gliner_score": None,
                "ai_score": 0.8,
                "confidence": "medium",
            },
        ]
        result = _merge_overlapping_spans(entities)
        assert len(result) == 1
        assert result[0]["start"] == 50
        assert result[0]["end"] == 57
        assert result[0]["confidence"] == "high"

    def test_subset_span_absorbed(self):
        entities = [
            {
                "entity": "John Smith",
                "entity_type": "PERSON",
                "start": 0,
                "end": 10,
                "doc_id": "d1",
                "presidio_score": 0.9,
                "gliner_score": None,
                "ai_score": None,
                "confidence": "high",
            },
            {
                "entity": "John",
                "entity_type": "NAME",
                "start": 0,
                "end": 4,
                "doc_id": "d1",
                "presidio_score": None,
                "gliner_score": 0.7,
                "ai_score": None,
                "confidence": "medium",
            },
        ]
        result = _merge_overlapping_spans(entities)
        assert len(result) == 1
        assert result[0]["entity"] == "John Smith"
        assert result[0]["end"] == 10

    def test_empty_and_single(self):
        assert _merge_overlapping_spans([]) == []
        single = [
            {
                "entity": "X",
                "entity_type": "T",
                "start": 0,
                "end": 1,
                "doc_id": "d1",
                "presidio_score": None,
                "gliner_score": None,
                "ai_score": None,
                "confidence": "low",
            }
        ]
        assert _merge_overlapping_spans(single) == single

    def test_redaction_clean_after_merge(self):
        """End-to-end: overlapping entities should produce clean redaction."""
        from dbxredact.redaction import redact_text

        text = "I live in Austin, Texas"
        entities = [
            {
                "entity": "Austin",
                "entity_type": "LOCATION",
                "start": 10,
                "end": 16,
                "doc_id": "d1",
                "presidio_score": 0.9,
                "gliner_score": None,
                "ai_score": None,
                "confidence": "high",
            },
            {
                "entity": "Austin,",
                "entity_type": "LOCATION",
                "start": 10,
                "end": 17,
                "doc_id": "d1",
                "presidio_score": None,
                "gliner_score": None,
                "ai_score": 0.8,
                "confidence": "medium",
            },
        ]
        merged = _merge_overlapping_spans(entities)
        result = redact_text(text, merged, strategy="typed")
        assert "[LOCATION]" in result
        assert result.replace("[LOCATION]", "").find("N]") == -1
        assert result.count("[LOCATION]") == 1


class TestAlignmentStress:
    """Tests exercising alignment beyond trivial 1-2 entity cases."""

    def test_same_text_different_positions_stay_separate(self):
        """'John' at position 0 and 'John' at position 50 should remain two
        separate entities, not merged into one."""
        aligner = MultiSourceAligner()
        presidio_entities = [
            {"entity": "John", "start": 0, "end": 4, "entity_type": "PERSON", "score": 0.9},
            {"entity": "John", "start": 50, "end": 54, "entity_type": "PERSON", "score": 0.8},
        ]
        result = aligner.align(
            doc_id="d1",
            presidio_entities=presidio_entities,
            ai_entities=None,
            gliner_entities=None,
        )
        assert len(result) == 2
        starts = {r["start"] for r in result}
        assert starts == {0, 50}

    def test_multi_source_five_entities(self):
        """Five entities from two sources -- verify all are present and matched
        correctly where positions overlap."""
        aligner = MultiSourceAligner()
        presidio_entities = [
            {"entity": "John Smith", "start": 0, "end": 10, "entity_type": "PERSON", "score": 0.95},
            {"entity": "test@email.com", "start": 25, "end": 39, "entity_type": "EMAIL_ADDRESS", "score": 0.99},
            {"entity": "123-45-6789", "start": 50, "end": 61, "entity_type": "US_SSN", "score": 0.92},
        ]
        ai_entities = [
            {"entity": "John Smith", "start": 0, "end": 10, "entity_type": "PERSON"},
            {"entity": "test@email.com", "start": 25, "end": 39, "entity_type": "EMAIL_ADDRESS"},
            {"entity": "123-45-6789", "start": 50, "end": 61, "entity_type": "US_SSN"},
            {"entity": "San Francisco", "start": 70, "end": 83, "entity_type": "LOCATION"},
            {"entity": "2024-01-15", "start": 90, "end": 100, "entity_type": "DATE_TIME"},
        ]
        result = aligner.align(
            doc_id="d1",
            presidio_entities=presidio_entities,
            ai_entities=ai_entities,
            gliner_entities=None,
        )
        entity_texts = {r["entity"] for r in result}
        assert "John Smith" in entity_texts
        assert "test@email.com" in entity_texts
        assert "123-45-6789" in entity_texts
        assert "San Francisco" in entity_texts
        assert "2024-01-15" in entity_texts
        assert len(result) == 5

    def test_near_overlap_does_not_spuriously_merge(self):
        """Two entities close together but not overlapping should not merge."""
        aligner = MultiSourceAligner()
        presidio_entities = [
            {"entity": "John", "start": 0, "end": 4, "entity_type": "PERSON", "score": 0.9},
        ]
        ai_entities = [
            {"entity": "Smith", "start": 5, "end": 10, "entity_type": "PERSON"},
        ]
        result = aligner.align(
            doc_id="d1",
            presidio_entities=presidio_entities,
            ai_entities=ai_entities,
            gliner_entities=None,
        )
        assert len(result) == 2
        texts = {r["entity"] for r in result}
        assert texts == {"John", "Smith"}


class TestSchemaContract:
    """End-to-end: entities in the format produced by format_entity_response_object_udf
    must flow through alignment and into redact_text without key mismatches."""

    def test_detection_to_alignment_to_redaction(self):
        text = "Patient John Smith (SSN: 123-45-6789) visited Dr. Jones."

        ai_detection_output = [
            {"entity": "John Smith", "entity_type": "PERSON", "score": 0.85,
             "start": 8, "end": 18, "doc_id": None},
            {"entity": "123-45-6789", "entity_type": "US_SSN", "score": 0.85,
             "start": 25, "end": 36, "doc_id": None},
        ]
        presidio_detection_output = [
            {"entity": "John Smith", "entity_type": "PERSON", "score": 0.95,
             "start": 8, "end": 18},
            {"entity": "123-45-6789", "entity_type": "US_SSN", "score": 0.99,
             "start": 25, "end": 36},
            {"entity": "Dr. Jones", "entity_type": "PERSON", "score": 0.7,
             "start": 46, "end": 55},
        ]

        aligned = align_entities_multi_source(
            presidio_entities=presidio_detection_output,
            ai_entities=ai_detection_output,
            gliner_entities=None,
            doc_id="d1",
        )

        assert len(aligned) >= 3
        for ent in aligned:
            assert "start" in ent
            assert "end" in ent
            assert "entity_type" in ent

        result = redact_text(text, aligned, strategy="typed")
        assert "John Smith" not in result
        assert "123-45-6789" not in result
        assert "[PERSON]" in result
        assert "[US_SSN]" in result
