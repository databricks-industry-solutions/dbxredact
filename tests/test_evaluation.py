"""Tests for evaluation.py -- calculate_metrics."""

import pytest
from pyspark.sql import Row
from dbxredact.evaluation import calculate_metrics


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


def _make_eval_row(gt_doc_id, gt_begin, gt_end, gt_chunk,
                    det_doc_id, det_start, det_end, det_entity):
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
        df = spark.createDataFrame(rows)
        m = calculate_metrics(df, total_tokens=100, **_eval_schema)
        assert m["true_positives"] == 0
        assert m["false_positives"] == 1
        assert m["precision"] == 0.0

    def test_all_false_negatives(self, spark, _eval_schema):
        rows = [_make_eval_row("d1", 0, 5, "John", None, None, None, None)]
        df = spark.createDataFrame(rows)
        m = calculate_metrics(df, total_tokens=100, **_eval_schema)
        assert m["true_positives"] == 0
        assert m["false_negatives"] == 1
        assert m["recall"] == 0.0

    def test_empty_dataframe(self, spark, _eval_schema):
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType
        gt_schema = StructType([
            StructField("doc_id", StringType()),
            StructField("begin", IntegerType()),
            StructField("end", IntegerType()),
            StructField("chunk", StringType()),
        ])
        det_schema = StructType([
            StructField("doc_id", StringType()),
            StructField("start", IntegerType()),
            StructField("end", IntegerType()),
            StructField("entity", StringType()),
        ])
        schema = StructType([
            StructField("gt", gt_schema),
            StructField("det", det_schema),
        ])
        df = spark.createDataFrame([], schema)
        m = calculate_metrics(df, total_tokens=100, **_eval_schema)
        assert m["true_positives"] == 0
        assert m["precision"] == 0.0
        assert m["recall"] == 0.0

    def test_mixed_tp_fp_fn(self, spark, _eval_schema):
        rows = [
            _make_eval_row("d1", 0, 5, "John", "d1", 0, 5, "John"),   # TP
            _make_eval_row(None, None, None, None, "d1", 10, 15, "X"), # FP
            _make_eval_row("d1", 20, 25, "Jane", None, None, None, None), # FN
        ]
        df = spark.createDataFrame(rows)
        m = calculate_metrics(df, total_tokens=100, **_eval_schema)
        assert m["true_positives"] == 1
        assert m["false_positives"] == 1
        assert m["false_negatives"] == 1
        assert m["precision"] == 0.5
        assert m["recall"] == 0.5
