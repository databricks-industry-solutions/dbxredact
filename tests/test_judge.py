"""Tests for judge.py -- _build_judge_expr SQL expression builder."""

from dbxredact.judge import _build_judge_expr
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
