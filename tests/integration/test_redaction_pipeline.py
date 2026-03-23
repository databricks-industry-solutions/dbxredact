"""Integration tests for run_redaction_pipeline with real Spark and mocked detectors.

Uses a real SparkSession for DataFrame operations but patches run_detection_pipeline
and external I/O (spark.table, saveAsTable, spark.sql) so no catalog is needed.
"""

import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, ArrayType,
)

from dbxredact.pipeline import (
    run_redaction_pipeline,
    _select_output_columns,
    _apply_redaction,
    _add_detection_status,
)


ENTITY_SCHEMA = ArrayType(StructType([
    StructField("entity", StringType()),
    StructField("entity_type", StringType()),
    StructField("score", DoubleType()),
    StructField("start", IntegerType()),
    StructField("end", IntegerType()),
    StructField("doc_id", StringType()),
]))


def _make_detection_df(spark, entities_per_doc):
    """Build a detection-like DataFrame with aligned_entities column."""
    schema = StructType([
        StructField("doc_id", StringType()),
        StructField("text", StringType()),
        StructField("aligned_entities", ENTITY_SCHEMA),
    ])
    rows = []
    for doc_id, text, entities in entities_per_doc:
        entity_rows = [Row(
            entity=e["entity"], entity_type=e["entity_type"],
            score=e.get("score", 0.9), start=e["start"], end=e["end"],
            doc_id=doc_id,
        ) for e in entities]
        rows.append(Row(doc_id=doc_id, text=text, aligned_entities=entity_rows))
    return spark.createDataFrame(rows, schema)


class TestApplyRedactionIntegration:
    """_apply_redaction with real Spark."""

    def test_typed_redaction_adds_columns(self, spark):
        det_df = _make_detection_df(spark, [
            ("d1", "John Smith lives here", [
                {"entity": "John Smith", "entity_type": "PERSON", "start": 0, "end": 10},
            ]),
        ])
        result = _apply_redaction(det_df, "text", "aligned_entities", "typed")
        cols = result.columns
        assert "text_redacted" in cols
        assert "_entity_count" in cols
        assert "_detection_status" in cols

        row = result.collect()[0]
        assert "[PERSON]" in row["text_redacted"]
        assert "John Smith" not in row["text_redacted"]
        assert row["_entity_count"] == 1
        assert row["_detection_status"] == "ok"

    def test_no_entities_status(self, spark):
        det_df = _make_detection_df(spark, [
            ("d2", "Nothing to see here", []),
        ])
        result = _apply_redaction(det_df, "text", "aligned_entities", "generic")
        row = result.collect()[0]
        assert row["_entity_count"] == 0
        assert row["_detection_status"] == "no_entities"
        assert row["text_redacted"] == "Nothing to see here"


class TestSelectOutputColumns:
    """_select_output_columns with real Spark."""

    def test_production_strips_raw_columns(self, spark):
        det_df = _make_detection_df(spark, [
            ("d1", "John Smith lives here", [
                {"entity": "John Smith", "entity_type": "PERSON", "start": 0, "end": 10},
            ]),
        ])
        result = _apply_redaction(det_df, "text", "aligned_entities", "typed")
        prod_df = _select_output_columns(result, "doc_id", "text", "production")
        assert set(prod_df.columns) == {"doc_id", "text_redacted", "_detection_status", "_entity_count"}

    def test_validation_keeps_all_columns(self, spark):
        det_df = _make_detection_df(spark, [
            ("d1", "John Smith lives here", [
                {"entity": "John Smith", "entity_type": "PERSON", "start": 0, "end": 10},
            ]),
        ])
        result = _apply_redaction(det_df, "text", "aligned_entities", "typed")
        val_df = _select_output_columns(result, "doc_id", "text", "validation")
        assert "aligned_entities" in val_df.columns
        assert "text" in val_df.columns


class TestMaxRowsTruncation:
    """max_rows param limits processed rows."""

    @patch("dbxredact.pipeline.run_detection_pipeline")
    def test_max_rows_truncates(self, mock_det, spark):
        det_df = _make_detection_df(spark, [
            ("d1", "John Smith", [
                {"entity": "John Smith", "entity_type": "PERSON", "start": 0, "end": 10},
            ]),
        ])
        mock_det.return_value = det_df

        source_df = spark.createDataFrame([
            Row(doc_id="d1", text="John Smith"),
            Row(doc_id="d2", text="Jane Doe"),
            Row(doc_id="d3", text="Bob Jones"),
        ])

        with patch.object(spark, "table", return_value=source_df), \
             patch("dbxredact.metadata._parse_table_name"), \
             patch("dbxredact.metadata._validate_identifier"), \
             patch("dbxredact.pipeline._select_output_columns") as mock_out:
            mock_out.return_value = MagicMock()
            run_redaction_pipeline(
                spark=spark,
                source_table="cat.sch.src",
                text_column="text",
                output_table="cat.sch.out",
                use_presidio=True, use_ai_query=False, use_gliner=False,
                max_rows=2,
                output_mode="separate",
            )
        det_call_kwargs = mock_det.call_args[1]
        passed_df = det_call_kwargs["source_df"]
        assert passed_df.count() <= 2


class TestGovernanceGuardsIntegration:
    """Governance guards work with real Spark (not mocked)."""

    def test_in_place_without_confirm_raises(self, spark):
        with pytest.raises(ValueError, match="destructive"):
            run_redaction_pipeline(
                spark=spark,
                source_table="cat.sch.tbl",
                text_column="text",
                output_mode="in_place",
                confirm_destructive=False,
                use_presidio=True, use_ai_query=False, use_gliner=False,
            )

    def test_consensus_without_opt_in_raises(self, spark):
        with pytest.raises(ValueError, match="unsafe for redaction"):
            run_redaction_pipeline(
                spark=spark,
                source_table="cat.sch.tbl",
                text_column="text",
                output_table="cat.sch.out",
                alignment_mode="consensus",
                allow_consensus_redaction=False,
                use_presidio=True, use_ai_query=False, use_gliner=False,
            )

    def test_validation_without_confirm_raises(self, spark):
        with pytest.raises(ValueError, match="writes raw PII"):
            run_redaction_pipeline(
                spark=spark,
                source_table="cat.sch.tbl",
                text_column="text",
                output_table="cat.sch.out",
                output_strategy="validation",
                confirm_validation_output=False,
                use_presidio=True, use_ai_query=False, use_gliner=False,
            )

    def test_separate_without_output_table_raises(self, spark):
        with pytest.raises(ValueError, match="output_table is required"):
            run_redaction_pipeline(
                spark=spark,
                source_table="cat.sch.tbl",
                text_column="text",
                output_table=None,
                output_mode="separate",
                use_presidio=True, use_ai_query=False, use_gliner=False,
            )

    def test_no_detectors_raises(self, spark):
        with pytest.raises(ValueError, match="At least one detection method"):
            run_redaction_pipeline(
                spark=spark,
                source_table="cat.sch.tbl",
                text_column="text",
                output_table="cat.sch.out",
                use_presidio=False, use_ai_query=False, use_gliner=False,
            )


class TestDetectionStatusColumn:

    def test_detection_status_values(self, spark):
        det_df = _make_detection_df(spark, [
            ("d1", "John Smith here", [
                {"entity": "John Smith", "entity_type": "PERSON", "start": 0, "end": 10},
            ]),
            ("d2", "Nothing here", []),
        ])
        result = _apply_redaction(det_df, "text", "aligned_entities", "typed")
        rows = {r["doc_id"]: r for r in result.collect()}
        assert rows["d1"]["_detection_status"] == "ok"
        assert rows["d2"]["_detection_status"] == "no_entities"
