"""Unit tests for the concatenate-redact-expand multi-column strategy."""

import pytest
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from dbxredact.pipeline import (
    _concat_text_columns,
    _split_and_redact_columns,
    _FIELD_BOUNDARY_SENTINEL,
)


@pytest.fixture
def spark(spark):
    """Reuse session-scoped spark from conftest."""
    return spark


class TestConcatTextColumns:
    """Tests for _concat_text_columns Spark expression."""

    def test_two_columns_concatenated_with_sentinel(self, spark):
        df = spark.createDataFrame(
            [("Hello John", "Call 555-1234")],
            ["notes", "contact"],
        )
        result = _concat_text_columns(df, ["notes", "contact"])
        row = result.collect()[0]
        expected = f"Hello John{_FIELD_BOUNDARY_SENTINEL}Call 555-1234"
        assert row["_combined_text"] == expected

    def test_field_lengths_computed_correctly(self, spark):
        df = spark.createDataFrame(
            [("abc", "defgh", "ij")],
            ["a", "b", "c"],
        )
        result = _concat_text_columns(df, ["a", "b", "c"])
        row = result.collect()[0]
        assert list(row["_field_lengths"]) == [3, 5, 2]

    def test_single_column_no_sentinel(self, spark):
        df = spark.createDataFrame([("text only",)], ["body"])
        result = _concat_text_columns(df, ["body"])
        row = result.collect()[0]
        assert _FIELD_BOUNDARY_SENTINEL not in row["_combined_text"]
        assert row["_combined_text"] == "text only"

    def test_null_column_handled(self, spark):
        from pyspark.sql.types import StructType, StructField, StringType
        schema = StructType([
            StructField("a", StringType(), True),
            StructField("b", StringType(), True),
        ])
        df = spark.createDataFrame([(None, "some text")], schema=schema)
        result = _concat_text_columns(df, ["a", "b"])
        row = result.collect()[0]
        # concat_ws skips nulls
        assert "some text" in row["_combined_text"]


class TestSplitAndRedactColumns:
    """Tests for _split_and_redact_columns pandas UDF."""

    def test_basic_split(self, spark):
        text_columns = ["notes", "contact"]
        sentinel = _FIELD_BOUNDARY_SENTINEL
        redacted_text = f"Hello [REDACTED]{sentinel}Call [REDACTED]"

        df = spark.createDataFrame(
            [(redacted_text, [len("Hello John"), len("Call 555-1234")])],
            ["_combined_text_redacted", "_field_lengths"],
        )

        split_udf = _split_and_redact_columns(text_columns, "generic")
        result = df.withColumn("_split_map", split_udf(
            col("_combined_text_redacted"), col("_field_lengths")
        ))
        row = result.collect()[0]
        mapping = row["_split_map"]
        assert mapping["notes"] == "Hello [REDACTED]"
        assert mapping["contact"] == "Call [REDACTED]"

    def test_null_redacted_text_returns_nulls(self, spark):
        from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType
        text_columns = ["a", "b"]
        schema = StructType([
            StructField("_redacted", StringType(), True),
            StructField("_field_lengths", ArrayType(IntegerType()), True),
        ])
        df = spark.createDataFrame([(None, [5, 5])], schema=schema)
        split_udf = _split_and_redact_columns(text_columns, "generic")
        result = df.withColumn("_split_map", split_udf(col("_redacted"), col("_field_lengths")))
        row = result.collect()[0]
        assert row["_split_map"]["a"] is None
        assert row["_split_map"]["b"] is None

    def test_fewer_parts_than_columns_pads_empty(self, spark):
        text_columns = ["a", "b", "c"]
        # Only one sentinel -> 2 parts, but 3 columns expected
        sentinel = _FIELD_BOUNDARY_SENTINEL
        redacted = f"part1{sentinel}part2"

        df = spark.createDataFrame(
            [(redacted, [5, 5, 5])],
            ["_redacted", "_field_lengths"],
        )
        split_udf = _split_and_redact_columns(text_columns, "generic")
        result = df.withColumn("_split_map", split_udf(col("_redacted"), col("_field_lengths")))
        row = result.collect()[0]
        assert row["_split_map"]["a"] == "part1"
        assert row["_split_map"]["b"] == "part2"
        assert row["_split_map"]["c"] == ""

    def test_single_column_passthrough(self, spark):
        text_columns = ["body"]
        df = spark.createDataFrame(
            [("Hello [REDACTED]", [16])],
            ["_redacted", "_field_lengths"],
        )
        split_udf = _split_and_redact_columns(text_columns, "generic")
        result = df.withColumn("_split_map", split_udf(col("_redacted"), col("_field_lengths")))
        row = result.collect()[0]
        assert row["_split_map"]["body"] == "Hello [REDACTED]"
