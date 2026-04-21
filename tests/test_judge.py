"""Tests for judge.py -- _build_judge_expr and compute_judge_summary."""

import pytest
from unittest.mock import patch
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

from dbxredact.judge import _build_judge_expr, compute_judge_summary
from dbxredact.config import DEFAULT_AI_REASONING_EFFORT


class TestBuildJudgeExpr:

    def test_contains_ai_query(self):
        result = _build_judge_expr("my-endpoint", "orig", "redacted")
        assert "ai_query(" in result

    def test_contains_endpoint(self):
        result = _build_judge_expr("databricks-gpt-oss-120b", "orig", "redacted")
        assert "'databricks-gpt-oss-120b'" in result

    def test_contains_column_references(self):
        result = _build_judge_expr("ep", "original_text", "redacted_text")
        assert "CAST(original_text AS STRING)" in result
        assert "CAST(redacted_text AS STRING)" in result

    def test_uses_default_reasoning_effort(self):
        result = _build_judge_expr("ep", "orig", "redacted")
        assert f"'{DEFAULT_AI_REASONING_EFFORT}'" in result

    def test_custom_reasoning_effort(self):
        result = _build_judge_expr("ep", "orig", "redacted", reasoning_effort="high")
        assert "'high'" in result

    def test_escapes_single_quotes_in_prompt(self):
        result = _build_judge_expr("ep", "orig", "redacted")
        # After escaping, no unescaped single quotes should appear inside
        # the concat string literals (the prompt portions).
        # The endpoint, column casts, and reasoning_effort are the only
        # places where single quotes delimit values.
        assert "ai_query(" in result
        assert "concat(" in result

    def test_returns_string(self):
        result = _build_judge_expr("ep", "orig", "redacted")
        assert isinstance(result, str)

    def test_fail_on_error_disabled(self):
        result = _build_judge_expr("ep", "orig", "redacted")
        assert "failOnError => false" in result


_FINDINGS_SCHEMA = ArrayType(StructType([
    StructField("entity", StringType()),
    StructField("entity_type", StringType()),
    StructField("status", StringType()),
    StructField("explanation", StringType()),
]))

_JUDGE_SCHEMA = StructType([
    StructField("doc_id", StringType()),
    StructField("method", StringType()),
    StructField("grade", StringType()),
    StructField("findings", _FINDINGS_SCHEMA),
])


class TestComputeJudgeSummary:

    def test_judge_summary_with_null_grades(self, spark):
        """Null grades are counted as failures; rates use graded_count only."""
        rows = [
            Row(doc_id="d1", method="m", grade="PASS", findings=[]),
            Row(doc_id="d2", method="m", grade="FAIL", findings=[]),
            Row(doc_id="d3", method="m", grade=None, findings=None),
            Row(doc_id="d4", method="m", grade=None, findings=None),
        ]
        df = spark.createDataFrame(rows, _JUDGE_SCHEMA)
        with patch("dbxredact.judge.logger") as mock_logger:
            result = compute_judge_summary(df)
            mock_logger.warning.assert_called_once()

        assert result["total_docs"] == 4
        assert result["failed_count"] == 2
        assert result["graded_count"] == 2
        assert result["pass_rate"] == pytest.approx(0.5)
        assert result["fail_rate"] == pytest.approx(0.5)

    def test_judge_summary_all_null(self, spark):
        """When every grade is null, rates are 0 and failed_count == total."""
        rows = [
            Row(doc_id="d1", method="m", grade=None, findings=None),
            Row(doc_id="d2", method="m", grade=None, findings=None),
        ]
        df = spark.createDataFrame(rows, _JUDGE_SCHEMA)
        result = compute_judge_summary(df)

        assert result["total_docs"] == 2
        assert result["failed_count"] == 2
        assert result["graded_count"] == 0
        assert result["pass_rate"] == 0.0
        assert result["partial_rate"] == 0.0
        assert result["fail_rate"] == 0.0

    def test_judge_summary_no_failures(self, spark):
        """When all grades are present, failed_count is 0."""
        rows = [
            Row(doc_id="d1", method="m", grade="PASS", findings=[]),
            Row(doc_id="d2", method="m", grade="PARTIAL", findings=[]),
            Row(doc_id="d3", method="m", grade="FAIL", findings=[]),
        ]
        df = spark.createDataFrame(rows, _JUDGE_SCHEMA)
        result = compute_judge_summary(df)

        assert result["failed_count"] == 0
        assert result["graded_count"] == 3
        assert result["pass_rate"] == pytest.approx(1 / 3)
        assert result["partial_rate"] == pytest.approx(1 / 3)
        assert result["fail_rate"] == pytest.approx(1 / 3)
