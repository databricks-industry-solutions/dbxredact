"""Tests for evaluation.py -- calculate_metrics and diagnose_strict_failures."""

import pytest
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from dbxredact.evaluation import calculate_metrics, diagnose_strict_failures

_EVAL_ROW_SCHEMA = StructType(
    [
        StructField(
            "gt",
            StructType(
                [
                    StructField("doc_id", StringType()),
                    StructField("begin", IntegerType()),
                    StructField("end", IntegerType()),
                    StructField("chunk", StringType()),
                ]
            ),
        ),
        StructField(
            "det",
            StructType(
                [
                    StructField("doc_id", StringType()),
                    StructField("start", IntegerType()),
                    StructField("end", IntegerType()),
                    StructField("entity", StringType()),
                ]
            ),
        ),
    ]
)


@pytest.fixture
def _eval_schema():
    """Column names used by calculate_metrics."""
    return {
        "chunk_column": "chunk",
        "entity_column": "entity",
        "doc_id_column": "doc_id",
        "begin_column": "begin",
        "end_column": "end",
        "start_column": "start",
    }


def _make_eval_row(
    gt_doc_id, gt_begin, gt_end, gt_chunk, det_doc_id, det_start, det_end, det_entity
):
    return Row(
        gt=Row(doc_id=gt_doc_id, begin=gt_begin, end=gt_end, chunk=gt_chunk),
        det=Row(doc_id=det_doc_id, start=det_start, end=det_end, entity=det_entity),
    )


class TestCalculateMetrics:

    def test_perfect_match(self, spark, _eval_schema):
        rows = [_make_eval_row("d1", 0, 5, "John", "d1", 0, 5, "John")]
        df = spark.createDataFrame(rows)
        m = calculate_metrics(df, total_tokens=100, **_eval_schema)
        assert m["true_positives"] == 1
        assert m["false_positives"] == 0
        assert m["false_negatives"] == 0
        assert m["precision"] == 1.0
        assert m["recall"] == 1.0

    def test_all_false_positives(self, spark, _eval_schema):
        rows = [_make_eval_row(None, None, None, None, "d1", 0, 5, "John")]
        df = spark.createDataFrame(rows, schema=_EVAL_ROW_SCHEMA)
        m = calculate_metrics(df, total_tokens=100, **_eval_schema)
        assert m["true_positives"] == 0
        assert m["false_positives"] == 1
        assert m["precision"] == 0.0

    def test_all_false_negatives(self, spark, _eval_schema):
        rows = [_make_eval_row("d1", 0, 5, "John", None, None, None, None)]
        df = spark.createDataFrame(rows, schema=_EVAL_ROW_SCHEMA)
        m = calculate_metrics(df, total_tokens=100, **_eval_schema)
        assert m["true_positives"] == 0
        assert m["false_negatives"] == 1
        assert m["recall"] == 0.0

    def test_empty_dataframe(self, spark, _eval_schema):
        df = spark.createDataFrame([], schema=_EVAL_ROW_SCHEMA)
        m = calculate_metrics(df, total_tokens=100, **_eval_schema)
        assert m["true_positives"] == 0
        assert m["precision"] == 0.0
        assert m["recall"] == 0.0

    def test_mixed_tp_fp_fn(self, spark, _eval_schema):
        rows = [
            _make_eval_row("d1", 0, 5, "John", "d1", 0, 5, "John"),  # TP
            _make_eval_row(None, None, None, None, "d1", 10, 15, "X"),  # FP
            _make_eval_row("d1", 20, 25, "Jane", None, None, None, None),  # FN
        ]
        df = spark.createDataFrame(rows)
        m = calculate_metrics(df, total_tokens=100, **_eval_schema)
        assert m["true_positives"] == 1
        assert m["false_positives"] == 1
        assert m["false_negatives"] == 1
        assert m["precision"] == 0.5
        assert m["recall"] == 0.5


# ---------------------------------------------------------------------------
# Helper to build GT / detection DataFrames for diagnose_strict_failures
# ---------------------------------------------------------------------------
def _make_gt_df(spark, rows):
    """rows: list of (doc_id, begin, end, chunk)"""
    schema = StructType(
        [
            StructField("doc_id", StringType()),
            StructField("begin", IntegerType()),
            StructField("end", IntegerType()),
            StructField("chunk", StringType()),
        ]
    )
    return spark.createDataFrame(
        [Row(doc_id=r[0], begin=r[1], end=r[2], chunk=r[3]) for r in rows],
        schema=schema,
    )


def _make_det_df(spark, rows):
    """rows: list of (doc_id, start, end, entity)"""
    schema = StructType(
        [
            StructField("doc_id", StringType()),
            StructField("start", IntegerType()),
            StructField("end", IntegerType()),
            StructField("entity", StringType()),
        ]
    )
    return spark.createDataFrame(
        [Row(doc_id=r[0], start=r[1], end=r[2], entity=r[3]) for r in rows],
        schema=schema,
    )


class TestDiagnoseStrictFailures:
    """Tests for diagnose_strict_failures -- intent: surface detections that
    overlap with ground truth but don't fully contain it, and correctly
    classify the boundary error."""

    def test_exact_match_produces_no_failures(self, spark):
        """A perfect detection should NOT appear in the diagnostic output."""
        gt = _make_gt_df(spark, [("d1", 10, 20, "John Smith")])
        det = _make_det_df(spark, [("d1", 10, 20, "John Smith")])
        result = diagnose_strict_failures(gt, det)
        assert result.empty

    def test_end_clipped_detection(self, spark):
        """Detection ends 2 chars before GT end -> end_clipped."""
        gt = _make_gt_df(spark, [("d1", 10, 20, "John Smith")])
        det = _make_det_df(spark, [("d1", 10, 17, "John Smi")])
        result = diagnose_strict_failures(gt, det)
        assert len(result) == 1
        row = result.iloc[0]
        assert row["boundary_type"] == "end_clipped"
        assert row["start_delta"] == 0
        assert row["end_delta"] == 3

    def test_start_clipped_detection(self, spark):
        """Detection starts after GT begin -> start_clipped."""
        gt = _make_gt_df(spark, [("d1", 10, 20, "John Smith")])
        det = _make_det_df(spark, [("d1", 12, 20, "hn Smith")])
        result = diagnose_strict_failures(gt, det)
        assert len(result) == 1
        assert result.iloc[0]["boundary_type"] == "start_clipped"
        assert result.iloc[0]["start_delta"] == 2

    def test_both_clipped(self, spark):
        """Detection is narrower on both sides -> both."""
        gt = _make_gt_df(spark, [("d1", 10, 20, "John Smith")])
        det = _make_det_df(spark, [("d1", 12, 17, "hn Smi")])
        result = diagnose_strict_failures(gt, det)
        assert len(result) == 1
        assert result.iloc[0]["boundary_type"] == "both"

    def test_non_overlapping_entities_excluded(self, spark):
        """Entities that don't overlap at all should never appear."""
        gt = _make_gt_df(spark, [("d1", 10, 20, "John Smith")])
        det = _make_det_df(spark, [("d1", 50, 60, "Jane Doe")])
        result = diagnose_strict_failures(gt, det)
        assert result.empty

    def test_different_docs_not_matched(self, spark):
        """Entities from different documents should never match."""
        gt = _make_gt_df(spark, [("d1", 10, 20, "John Smith")])
        det = _make_det_df(spark, [("d2", 12, 20, "hn Smith")])
        result = diagnose_strict_failures(gt, det)
        assert result.empty

    def test_strict_pass_with_minus_one_tolerance(self, spark):
        """Strict allows det.end == gt.end - 1, so off-by-one at end should pass."""
        gt = _make_gt_df(spark, [("d1", 10, 20, "John Smith")])
        det = _make_det_df(spark, [("d1", 10, 19, "John Smit")])
        result = diagnose_strict_failures(gt, det)
        assert result.empty
