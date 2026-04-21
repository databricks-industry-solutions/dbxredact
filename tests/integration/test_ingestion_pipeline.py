"""Integration tests for ingestion pipeline components.

Full end-to-end tests with cloudFiles require Databricks runtime and are in
notebooks/integration_tests/. These tests exercise the pipeline helpers that
work with local PySpark.
"""

import pytest
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType

from dbxredact.pipeline import (
    _build_autoloader_options,
    _concat_text_columns,
    _split_and_redact_columns,
    _FIELD_BOUNDARY_SENTINEL,
)


class TestConcatSplitRoundtrip:
    """End-to-end concat -> split with real Spark DataFrames."""

    def test_two_columns_roundtrip(self, spark):
        df = spark.createDataFrame([
            ("Hello John Smith", "Email: john@example.com"),
            ("No PII here", "Another field"),
        ], ["notes", "contact"])

        text_columns = ["notes", "contact"]
        combined_df = _concat_text_columns(df, text_columns)
        combined_df = combined_df.withColumnRenamed("_combined_text", "_combined_text_redacted")

        split_udf = _split_and_redact_columns(text_columns, "generic")
        result = combined_df.withColumn(
            "_split_map",
            split_udf(col("_combined_text_redacted"), col("_field_lengths"))
        )

        rows = result.collect()
        assert rows[0]["_split_map"]["notes"] == "Hello John Smith"
        assert rows[0]["_split_map"]["contact"] == "Email: john@example.com"
        assert rows[1]["_split_map"]["notes"] == "No PII here"
        assert rows[1]["_split_map"]["contact"] == "Another field"

    def test_three_columns_roundtrip(self, spark):
        df = spark.createDataFrame([
            ("first", "second", "third"),
        ], ["a", "b", "c"])

        text_columns = ["a", "b", "c"]
        combined_df = _concat_text_columns(df, text_columns)
        combined_df = combined_df.withColumnRenamed("_combined_text", "_combined_text_redacted")

        split_udf = _split_and_redact_columns(text_columns, "generic")
        result = combined_df.withColumn(
            "_split_map",
            split_udf(col("_combined_text_redacted"), col("_field_lengths"))
        )
        row = result.collect()[0]
        assert row["_split_map"]["a"] == "first"
        assert row["_split_map"]["b"] == "second"
        assert row["_split_map"]["c"] == "third"

    def test_sentinel_preserved_through_redaction_simulation(self, spark):
        """Simulate redaction that changes text length but preserves sentinels."""
        df = spark.createDataFrame([
            ("John Smith is here", "Call 555-1234"),
        ], ["notes", "contact"])

        text_columns = ["notes", "contact"]
        combined_df = _concat_text_columns(df, text_columns)

        sentinel = _FIELD_BOUNDARY_SENTINEL
        simulated_redacted = f"[REDACTED] is here{sentinel}Call [REDACTED]"

        redacted_df = combined_df.withColumn(
            "_combined_text_redacted", lit(simulated_redacted)
        )

        split_udf = _split_and_redact_columns(text_columns, "generic")
        result = redacted_df.withColumn(
            "_split_map",
            split_udf(col("_combined_text_redacted"), col("_field_lengths"))
        )
        row = result.collect()[0]
        assert row["_split_map"]["notes"] == "[REDACTED] is here"
        assert row["_split_map"]["contact"] == "Call [REDACTED]"


class TestStructuredMaskingIntegration:
    """Structured masking with real Spark DataFrames."""

    def test_mask_strategy(self, spark):
        from dbxredact.masking import apply_structured_masking

        df = spark.createDataFrame([
            ("text body", "123-45-6789", "555-123-4567"),
            ("more text", "987-65-4321", "800-555-0100"),
        ], ["body", "ssn", "phone"])

        result = apply_structured_masking(
            df, {"ssn": "ssn", "phone": "phone"}, strategy="mask"
        )
        rows = result.collect()

        for row in rows:
            assert "123-45-6789" not in str(row["ssn"])
            assert "555-123-4567" not in str(row["phone"])
            assert row["body"] in ("text body", "more text")

    def test_hash_strategy(self, spark):
        from dbxredact.masking import apply_structured_masking

        df = spark.createDataFrame([("123-45-6789",)], ["ssn"])
        result = apply_structured_masking(df, {"ssn": "ssn"}, strategy="hash")
        row = result.collect()[0]
        assert row["ssn"] != "123-45-6789"
        assert len(row["ssn"]) == 64  # SHA-256 hex


class TestAutoloaderOptionsBuild:
    """Option building for all supported formats."""

    def test_all_formats_produce_valid_options(self):
        for fmt in ["json", "csv", "parquet", "text", "avro"]:
            opts = _build_autoloader_options(fmt, "/Volumes/cat/sch/vol/data")
            assert "cloudFiles.format" in opts
            assert "cloudFiles.schemaLocation" in opts
            assert opts["cloudFiles.format"] == fmt

    def test_user_options_override_defaults(self):
        opts = _build_autoloader_options(
            "csv", "/path",
            extra_options={"header": "false", "cloudFiles.useNotifications": "true"},
        )
        assert opts["header"] == "false"
        assert opts["cloudFiles.useNotifications"] == "true"

    def test_max_bytes_per_trigger_passthrough(self):
        opts = _build_autoloader_options(
            "json", "/path",
            extra_options={"cloudFiles.maxBytesPerTrigger": "10g"},
        )
        assert opts["cloudFiles.maxBytesPerTrigger"] == "10g"
