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
    run_table_redaction,
    _select_output_columns,
    _apply_redaction,
    _add_detection_status,
)
from dbxredact.config import RedactionConfig


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
    """max_rows param limits processed rows and produces valid output."""

    @patch("pyspark.sql.readwriter.DataFrameWriter.saveAsTable")
    @patch("dbxredact.pipeline.run_detection_pipeline")
    def test_max_rows_truncates(self, mock_det, mock_save, spark):
        source_df = spark.createDataFrame(
            [(f"d{i}", f"Text for doc {i}") for i in range(5)],
            ["doc_id", "text"],
        )

        def pass_through(spark, source_df, text_column, **kw):
            from pyspark.sql.functions import lit
            return source_df.withColumn(
                "aligned_entities", lit(None).cast(ENTITY_SCHEMA)
            )
        mock_det.side_effect = pass_through

        with patch.object(spark, "table", return_value=source_df), \
             patch("dbxredact.metadata._parse_table_name"), \
             patch("dbxredact.metadata._validate_identifier"):
            result = run_redaction_pipeline(
                spark=spark,
                source_table="cat.sch.src",
                text_column="text",
                output_table="cat.sch.out",
                use_presidio=True, use_ai_query=False, use_gliner=False,
                max_rows=2,
                output_mode="separate",
            )

        assert result.count() <= 2
        row = result.collect()[0]
        assert "text_redacted" in result.columns
        assert row["_detection_status"] == "no_entities"
        mock_save.assert_called_once_with("cat.sch.out")


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


# ---------------------------------------------------------------------------
# run_table_redaction integration tests
# ---------------------------------------------------------------------------

def _make_detection_side_effect(entity_map):
    """Build a side_effect for run_detection_pipeline that injects entities
    based on text_column name and the actual text values in source_df.

    *entity_map*: dict mapping text_column name -> list of
        (match_substring, entity_type, start, end) tuples.
    Returns entities only for rows whose text contains match_substring.
    """
    from pyspark.sql.functions import lit

    def side_effect(spark, source_df, text_column, **kw):
        mappings = entity_map.get(text_column, [])
        if not mappings:
            return source_df.withColumn(
                "aligned_entities", lit(None).cast(ENTITY_SCHEMA)
            )
        rows = source_df.collect()
        result_rows = []
        for row in rows:
            text_val = row[text_column]
            entities = []
            for substr, etype, start, end in mappings:
                if substr in (text_val or ""):
                    entities.append(Row(
                        entity=substr, entity_type=etype,
                        score=0.95, start=start, end=end, doc_id=row["doc_id"],
                    ))
            result_rows.append(Row(**row.asDict(), aligned_entities=entities or None))

        schema = StructType(list(source_df.schema.fields) + [
            StructField("aligned_entities", ENTITY_SCHEMA),
        ])
        return spark.createDataFrame(result_rows, schema)

    return side_effect


class TestRunTableRedactionConfigPropagation:
    """Verify RedactionConfig values flow through run_table_redaction to detectors."""

    def test_config_selects_detectors(self, spark):
        """Config with use_presidio=False, use_gliner=True is forwarded to detection."""
        df = spark.createDataFrame(
            [("d1", "John Smith is here")],
            ["doc_id", "notes"],
        )
        df.createOrReplaceTempView("_integ_cfg_det")

        captured_calls = []

        def spy_detection(spark, source_df, text_column, **kw):
            captured_calls.append(kw)
            from pyspark.sql.functions import lit
            return source_df.withColumn(
                "aligned_entities", lit(None).cast(ENTITY_SCHEMA)
            )

        cfg = RedactionConfig(
            use_presidio=False,
            use_ai_query=False,
            use_gliner=True,
            score_threshold=0.85,
            num_cores=7,
            gliner_model="custom/model",
        )

        with patch("dbxredact.pipeline.run_detection_pipeline", side_effect=spy_detection):
            run_table_redaction(
                spark=spark,
                source_table="_integ_cfg_det",
                text_columns=["notes"],
                structured_columns={},
                max_rows=None,
                config=cfg,
            )

        assert len(captured_calls) == 1
        kw = captured_calls[0]
        assert kw["use_presidio"] is False
        assert kw["use_ai_query"] is False
        assert kw["use_gliner"] is True
        assert kw["score_threshold"] == 0.85
        assert kw["num_cores"] == 7
        assert kw["gliner_model"] == "custom/model"


class TestRunTableRedactionMultiColumn:
    """Multi-column NER: each text column gets independently redacted."""

    def test_two_text_columns_redacted_correctly(self, spark):
        """Two text columns each get their own _redacted column with correct content."""
        df = spark.createDataFrame(
            [("d1", "John Smith lives here", "Call 555-0100 now")],
            ["doc_id", "col_a", "col_b"],
        )
        df.createOrReplaceTempView("_integ_multi_col")

        entity_map = {
            "col_a": [("John Smith", "PERSON", 0, 10)],
            "col_b": [("555-0100", "PHONE_NUMBER", 5, 13)],
        }
        side_effect = _make_detection_side_effect(entity_map)

        with patch("dbxredact.pipeline.run_detection_pipeline", side_effect=side_effect):
            result = run_table_redaction(
                spark=spark,
                source_table="_integ_multi_col",
                text_columns=["col_a", "col_b"],
                structured_columns={},
                redaction_strategy="typed",
                max_rows=None,
            )

        row = result.collect()[0]

        assert "col_a_redacted" in result.columns
        assert "col_b_redacted" in result.columns
        assert "[PERSON]" in row["col_a_redacted"]
        assert "John Smith" not in row["col_a_redacted"]
        assert "[PHONE_NUMBER]" in row["col_b_redacted"]
        assert "555-0100" not in row["col_b_redacted"]

        assert row["col_a"] == "John Smith lives here"
        assert row["col_b"] == "Call 555-0100 now"

        intermediates = {
            "aligned_entities", "presidio_results_struct",
            "ai_results_struct", "gliner_results_struct",
            "_entity_count", "_detection_status",
        }
        assert not (intermediates & set(result.columns)), \
            "Detection intermediates should be cleaned up"


class TestRunTableRedactionMixed:
    """Mixed text + structured columns in a single run_table_redaction call."""

    def test_text_and_structured_both_redacted(self, spark):
        df = spark.createDataFrame(
            [("d1", "Dr. Jane Doe prescribed medication", "123-45-6789", "555-867-5309")],
            ["doc_id", "clinical_notes", "ssn", "phone"],
        )
        df.createOrReplaceTempView("_integ_mixed")

        entity_map = {
            "clinical_notes": [("Jane Doe", "PERSON", 4, 12)],
        }
        side_effect = _make_detection_side_effect(entity_map)

        with patch("dbxredact.pipeline.run_detection_pipeline", side_effect=side_effect):
            result = run_table_redaction(
                spark=spark,
                source_table="_integ_mixed",
                text_columns=["clinical_notes"],
                structured_columns={"ssn": "ssn", "phone": "phone"},
                masking_strategy="mask",
                redaction_strategy="typed",
                max_rows=None,
            )

        row = result.collect()[0]

        assert "[PERSON]" in row["clinical_notes_redacted"]
        assert "Jane Doe" not in row["clinical_notes_redacted"]

        assert row["ssn"] == "[SSN]"
        assert row["phone"] == "[PHONE]"

        assert row["doc_id"] == "d1"


class TestRunTableRedactionStrategy:
    """redaction_strategy from config controls typed vs generic output."""

    def test_typed_via_config(self, spark):
        df = spark.createDataFrame(
            [("d1", "John Smith is here")], ["doc_id", "notes"],
        )
        df.createOrReplaceTempView("_integ_strat_typed")

        entity_map = {"notes": [("John Smith", "PERSON", 0, 10)]}
        side_effect = _make_detection_side_effect(entity_map)

        cfg = RedactionConfig(
            use_presidio=True, use_ai_query=False, use_gliner=False,
            redaction_strategy="typed",
        )

        with patch("dbxredact.pipeline.run_detection_pipeline", side_effect=side_effect):
            result = run_table_redaction(
                spark=spark,
                source_table="_integ_strat_typed",
                text_columns=["notes"],
                structured_columns={},
                max_rows=None,
                config=cfg,
            )

        row = result.collect()[0]
        assert "[PERSON]" in row["notes_redacted"]
        assert "[REDACTED]" not in row["notes_redacted"]

    def test_generic_via_config(self, spark):
        df = spark.createDataFrame(
            [("d1", "John Smith is here")], ["doc_id", "notes"],
        )
        df.createOrReplaceTempView("_integ_strat_generic")

        entity_map = {"notes": [("John Smith", "PERSON", 0, 10)]}
        side_effect = _make_detection_side_effect(entity_map)

        cfg = RedactionConfig(
            use_presidio=True, use_ai_query=False, use_gliner=False,
            redaction_strategy="generic",
        )

        with patch("dbxredact.pipeline.run_detection_pipeline", side_effect=side_effect):
            result = run_table_redaction(
                spark=spark,
                source_table="_integ_strat_generic",
                text_columns=["notes"],
                structured_columns={},
                max_rows=None,
                config=cfg,
            )

        row = result.collect()[0]
        assert "[REDACTED]" in row["notes_redacted"]
        assert "[PERSON]" not in row["notes_redacted"]


class TestRunTableRedactionMaxRows:
    """max_rows from config limits the rows processed."""

    def test_max_rows_via_config(self, spark):
        rows = [(f"d{i}", f"Text {i}") for i in range(10)]
        df = spark.createDataFrame(rows, ["doc_id", "notes"])
        df.createOrReplaceTempView("_integ_max_rows")

        processed_counts = []

        def counting_detection(spark, source_df, text_column, **kw):
            from pyspark.sql.functions import lit
            count = source_df.count()
            processed_counts.append(count)
            return source_df.withColumn(
                "aligned_entities", lit(None).cast(ENTITY_SCHEMA)
            )

        cfg = RedactionConfig(
            use_presidio=True, use_ai_query=False, use_gliner=False,
            max_rows=3,
        )

        with patch("dbxredact.pipeline.run_detection_pipeline", side_effect=counting_detection):
            result = run_table_redaction(
                spark=spark,
                source_table="_integ_max_rows",
                text_columns=["notes"],
                structured_columns={},
                config=cfg,
            )

        assert processed_counts[0] <= 3
        assert result.count() <= 3


class TestRunTableRedactionStructuredOnly:
    """run_table_redaction with only structured columns (no text columns)."""

    def test_structured_only_masks_correctly(self, spark):
        df = spark.createDataFrame(
            [("d1", "123-45-6789", "user@test.com"),
             ("d2", "987-65-4321", "other@test.com")],
            ["doc_id", "ssn", "email"],
        )
        df.createOrReplaceTempView("_integ_struct_only")

        result = run_table_redaction(
            spark=spark,
            source_table="_integ_struct_only",
            text_columns=[],
            structured_columns={"ssn": "ssn", "email": "email"},
            masking_strategy="mask",
            max_rows=None,
        )

        rows = {r["doc_id"]: r for r in result.collect()}
        assert rows["d1"]["ssn"] == "[SSN]"
        assert rows["d1"]["email"] == "[EMAIL]"
        assert rows["d2"]["ssn"] == "[SSN]"
        assert rows["d2"]["email"] == "[EMAIL]"
        assert rows["d1"]["doc_id"] == "d1"

    def test_no_columns_raises_valueerror(self, spark):
        df = spark.createDataFrame([("d1", "safe")], ["doc_id", "notes"])
        df.createOrReplaceTempView("_integ_no_cols")

        with pytest.raises(ValueError, match="No PII columns"):
            run_table_redaction(
                spark=spark,
                source_table="_integ_no_cols",
                text_columns=[],
                structured_columns={},
                max_rows=None,
            )


class TestRunRedactionPipelineReturnValue:
    """Verify run_redaction_pipeline return value shape in production vs validation mode."""

    @patch("pyspark.sql.readwriter.DataFrameWriter.saveAsTable")
    @patch("dbxredact.pipeline.run_detection_pipeline")
    def test_production_separate_returns_narrow_df(self, mock_det, mock_save, spark):
        """Production + separate mode returns only doc_id, text_redacted, status, count."""
        det_df = _make_detection_df(spark, [
            ("d1", "John Smith lives here", [
                {"entity": "John Smith", "entity_type": "PERSON", "start": 0, "end": 10},
            ]),
        ])
        mock_det.return_value = det_df

        source_df = spark.createDataFrame(
            [Row(doc_id="d1", text="John Smith lives here")],
        )

        with patch.object(spark, "table", return_value=source_df), \
             patch("dbxredact.metadata._parse_table_name"), \
             patch("dbxredact.metadata._validate_identifier"):
            result = run_redaction_pipeline(
                spark=spark,
                source_table="cat.sch.src",
                text_column="text",
                output_table="cat.sch.out",
                use_presidio=True, use_ai_query=False, use_gliner=False,
                redaction_strategy="typed",
                output_strategy="production",
                output_mode="separate",
            )

        assert set(result.columns) == {
            "doc_id", "text_redacted", "_detection_status", "_entity_count",
        }
        row = result.collect()[0]
        assert "[PERSON]" in row["text_redacted"]
        assert "John Smith" not in row["text_redacted"]
        assert row["_detection_status"] == "ok"
        assert row["_entity_count"] == 1
        assert "text" not in result.columns, "Raw text must be stripped in production mode"
        mock_save.assert_called_once_with("cat.sch.out")

    @patch("pyspark.sql.readwriter.DataFrameWriter.saveAsTable")
    @patch("dbxredact.pipeline.run_detection_pipeline")
    def test_validation_separate_returns_wide_df(self, mock_det, mock_save, spark):
        """Validation + separate mode returns all columns including raw PII and entities."""
        det_df = _make_detection_df(spark, [
            ("d1", "John Smith lives here", [
                {"entity": "John Smith", "entity_type": "PERSON", "start": 0, "end": 10},
            ]),
        ])
        mock_det.return_value = det_df

        source_df = spark.createDataFrame(
            [Row(doc_id="d1", text="John Smith lives here")],
        )

        with patch.object(spark, "table", return_value=source_df), \
             patch("dbxredact.metadata._parse_table_name"), \
             patch("dbxredact.metadata._validate_identifier"):
            result = run_redaction_pipeline(
                spark=spark,
                source_table="cat.sch.src",
                text_column="text",
                output_table="cat.sch.out",
                use_presidio=True, use_ai_query=False, use_gliner=False,
                output_strategy="validation",
                confirm_validation_output=True,
                output_mode="separate",
            )

        row = result.collect()[0]
        assert row["text"] == "John Smith lives here", "Validation mode must preserve raw PII text"
        assert "aligned_entities" in result.columns
        assert row["aligned_entities"] is not None and len(row["aligned_entities"]) == 1
        assert row["aligned_entities"][0]["entity"] == "John Smith"
        assert "text_redacted" in result.columns
        mock_save.assert_called_once_with("cat.sch.out")


class TestRunTableRedactionPersistence:
    """Verify run_table_redaction writes to output_table when provided."""

    @patch("pyspark.sql.readwriter.DataFrameWriter.saveAsTable")
    def test_writes_to_output_table(self, mock_save, spark):
        df = spark.createDataFrame(
            [("d1", "123-45-6789", "user@test.com")],
            ["doc_id", "ssn", "email"],
        )
        df.createOrReplaceTempView("_integ_persist_src")

        result = run_table_redaction(
            spark=spark,
            source_table="_integ_persist_src",
            text_columns=[],
            structured_columns={"ssn": "ssn", "email": "email"},
            masking_strategy="mask",
            max_rows=None,
            output_table="cat.sch.redacted_output",
        )

        mock_save.assert_called_once_with("cat.sch.redacted_output")
        row = result.collect()[0]
        assert row["ssn"] == "[SSN]"
        assert row["email"] == "[EMAIL]"

    def test_no_output_table_skips_write_returns_df(self, spark):
        df = spark.createDataFrame(
            [("d1", "123-45-6789")], ["doc_id", "ssn"],
        )
        df.createOrReplaceTempView("_integ_persist_no_out")

        result = run_table_redaction(
            spark=spark,
            source_table="_integ_persist_no_out",
            text_columns=[],
            structured_columns={"ssn": "ssn"},
            max_rows=None,
            output_table=None,
        )

        assert result.collect()[0]["ssn"] == "[SSN]"


class TestApplyRedactionMultiEntity:
    """Multi-entity and multi-document redaction correctness."""

    def test_overlapping_entities_handled(self, spark):
        """Two entities in the same doc are both redacted without corruption."""
        det_df = _make_detection_df(spark, [
            ("d1", "John Smith called Jane Doe yesterday", [
                {"entity": "John Smith", "entity_type": "PERSON", "start": 0, "end": 10},
                {"entity": "Jane Doe", "entity_type": "PERSON", "start": 18, "end": 26},
            ]),
        ])
        result = _apply_redaction(det_df, "text", "aligned_entities", "typed")
        row = result.collect()[0]
        assert "John Smith" not in row["text_redacted"]
        assert "Jane Doe" not in row["text_redacted"]
        assert row["text_redacted"].count("[PERSON]") == 2
        assert row["_entity_count"] == 2

    def test_multi_doc_independent_redaction(self, spark):
        """Each document is redacted independently."""
        det_df = _make_detection_df(spark, [
            ("d1", "John Smith is here", [
                {"entity": "John Smith", "entity_type": "PERSON", "start": 0, "end": 10},
            ]),
            ("d2", "Email user@test.com now", [
                {"entity": "user@test.com", "entity_type": "EMAIL", "start": 6, "end": 19},
            ]),
            ("d3", "Nothing special", []),
        ])
        result = _apply_redaction(det_df, "text", "aligned_entities", "typed")
        rows = {r["doc_id"]: r for r in result.collect()}

        assert "[PERSON]" in rows["d1"]["text_redacted"]
        assert "[EMAIL]" in rows["d2"]["text_redacted"]
        assert rows["d3"]["text_redacted"] == "Nothing special"
        assert rows["d1"]["_entity_count"] == 1
        assert rows["d2"]["_entity_count"] == 1
        assert rows["d3"]["_entity_count"] == 0
