"""Tests for presidio.py -- format_presidio_batch_results and related logic."""

from unittest.mock import MagicMock

from dbxredact.presidio import format_presidio_batch_results


def _make_recognizer_result(entity_type, score, start, end):
    """Create a mock RecognizerResult."""
    r = MagicMock()
    r.start = start
    r.end = end
    r.to_dict.return_value = {
        "entity_type": entity_type,
        "score": score,
        "start": start,
        "end": end,
    }
    return r


def _make_batch_results(doc_ids, texts, recognizer_results_per_doc):
    """Create an iterator mimicking BatchAnalyzerEngine.analyze_dict output.

    Returns an iterator of two DictAnalyzerResult-like objects:
      col1 (doc_id column) -> .value = doc_ids list
      col2 (text column) -> .value = texts list, .recognizer_results = per-doc lists
    """
    col1 = MagicMock()
    col1.value = doc_ids

    col2 = MagicMock()
    col2.value = texts
    col2.recognizer_results = recognizer_results_per_doc

    return iter([col1, col2])


class TestFormatPresidioBatchResults:

    def test_single_entity_above_threshold(self):
        results = _make_batch_results(
            doc_ids=["d1"],
            texts=["John Smith lives here"],
            recognizer_results_per_doc=[
                [_make_recognizer_result("PERSON", 0.9, 0, 10)],
            ],
        )
        output = format_presidio_batch_results(results, score_threshold=0.5)
        assert len(output) == 1
        assert len(output[0]) == 1
        assert output[0][0]["entity"] == "John Smith"
        assert output[0][0]["entity_type"] == "PERSON"
        assert output[0][0]["score"] == 0.9
        assert output[0][0]["start"] == 0
        assert output[0][0]["end"] == 10
        assert output[0][0]["doc_id"] == "d1"

    def test_entity_below_threshold_filtered(self):
        results = _make_batch_results(
            doc_ids=["d1"],
            texts=["John Smith lives here"],
            recognizer_results_per_doc=[
                [_make_recognizer_result("PERSON", 0.3, 0, 10)],
            ],
        )
        output = format_presidio_batch_results(results, score_threshold=0.5)
        assert len(output) == 1
        assert len(output[0]) == 0

    def test_entity_at_threshold_boundary_filtered(self):
        """Score must be strictly greater than threshold."""
        results = _make_batch_results(
            doc_ids=["d1"],
            texts=["John Smith lives here"],
            recognizer_results_per_doc=[
                [_make_recognizer_result("PERSON", 0.5, 0, 10)],
            ],
        )
        output = format_presidio_batch_results(results, score_threshold=0.5)
        assert len(output[0]) == 0

    def test_multiple_docs_multiple_entities(self):
        results = _make_batch_results(
            doc_ids=["d1", "d2"],
            texts=["Call 555-1234 today", "Email john@test.com"],
            recognizer_results_per_doc=[
                [_make_recognizer_result("PHONE_NUMBER", 0.8, 5, 13)],
                [_make_recognizer_result("EMAIL_ADDRESS", 0.95, 6, 19)],
            ],
        )
        output = format_presidio_batch_results(results, score_threshold=0.5)
        assert len(output) == 2
        assert output[0][0]["entity"] == "555-1234"
        assert output[0][0]["doc_id"] == "d1"
        assert output[1][0]["entity"] == "john@test.com"
        assert output[1][0]["doc_id"] == "d2"

    def test_empty_results_returns_empty_lists(self):
        results = _make_batch_results(
            doc_ids=["d1"],
            texts=["No PII here"],
            recognizer_results_per_doc=[[]],
        )
        output = format_presidio_batch_results(results, score_threshold=0.5)
        assert len(output) == 1
        assert output[0] == []

    def test_single_char_entity_ignored(self):
        """should_ignore_entity filters single-char entities."""
        results = _make_batch_results(
            doc_ids=["d1"],
            texts=["A random sentence here"],
            recognizer_results_per_doc=[
                [_make_recognizer_result("PERSON", 0.9, 0, 1)],
            ],
        )
        output = format_presidio_batch_results(results, score_threshold=0.5)
        assert len(output[0]) == 0

    def test_mixed_scores_partial_filter(self):
        results = _make_batch_results(
            doc_ids=["d1"],
            texts=["John Smith called 555-1234"],
            recognizer_results_per_doc=[
                [
                    _make_recognizer_result("PERSON", 0.9, 0, 10),
                    _make_recognizer_result("PHONE_NUMBER", 0.2, 18, 26),
                ],
            ],
        )
        output = format_presidio_batch_results(results, score_threshold=0.5)
        assert len(output[0]) == 1
        assert output[0][0]["entity_type"] == "PERSON"

    def test_custom_threshold(self):
        results = _make_batch_results(
            doc_ids=["d1"],
            texts=["John Smith lives here"],
            recognizer_results_per_doc=[
                [_make_recognizer_result("PERSON", 0.85, 0, 10)],
            ],
        )
        output = format_presidio_batch_results(results, score_threshold=0.9)
        assert len(output[0]) == 0
