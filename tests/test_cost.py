"""Tests for cost.py -- cost estimation utilities."""

import logging
import pytest

from dbxredact.cost import (
    TOKENS_PER_CHAR,
    COST_PER_1K_INPUT_TOKENS,
    COST_PER_1K_OUTPUT_TOKENS,
    PROMPT_OVERHEAD_CHARS,
    ESTIMATED_OUTPUT_RATIO,
    print_cost_estimate,
)


class TestCostConstants:
    def test_tokens_per_char_sane(self):
        assert 0 < TOKENS_PER_CHAR < 1

    def test_prompt_overhead_positive(self):
        assert PROMPT_OVERHEAD_CHARS > 0

    def test_output_ratio_between_zero_and_one(self):
        assert 0 < ESTIMATED_OUTPUT_RATIO < 1

    def test_cost_maps_have_matching_keys(self):
        assert set(COST_PER_1K_INPUT_TOKENS.keys()) == set(COST_PER_1K_OUTPUT_TOKENS.keys())

    def test_cost_values_positive(self):
        for k, v in COST_PER_1K_INPUT_TOKENS.items():
            assert v > 0, f"Input cost for {k} should be positive"
        for k, v in COST_PER_1K_OUTPUT_TOKENS.items():
            assert v > 0, f"Output cost for {k} should be positive"


class TestPrintCostEstimate:
    def test_logs_all_fields(self, caplog):
        estimate = {
            "endpoint": "databricks-gpt-4o-mini",
            "row_count": 100,
            "total_chars": 50000,
            "estimated_input_tokens": 25000,
            "estimated_output_tokens": 7500,
            "estimated_cost_usd": 0.0083,
        }
        with caplog.at_level(logging.INFO, logger="dbxredact.cost"):
            print_cost_estimate(estimate)
        output = caplog.text
        assert "databricks-gpt-4o-mini" in output
        assert "100" in output
        assert "50,000" in output
        assert "25,000" in output
        assert "7,500" in output
        assert "$0.0083" in output
        assert "approximate" in output.lower()


class TestCostMath:
    """Verify the math that estimate_ai_query_cost would do, without Spark."""

    def _compute(self, total_chars, row_count, endpoint, overhead=PROMPT_OVERHEAD_CHARS, ratio=ESTIMATED_OUTPUT_RATIO):
        input_chars = total_chars + (row_count * overhead)
        input_tokens = int(input_chars * TOKENS_PER_CHAR)
        output_tokens = int(input_tokens * ratio)
        input_cost_per_1k = COST_PER_1K_INPUT_TOKENS.get(endpoint, 0.001)
        output_cost_per_1k = COST_PER_1K_OUTPUT_TOKENS.get(endpoint, 0.002)
        cost = (input_tokens / 1000 * input_cost_per_1k) + (output_tokens / 1000 * output_cost_per_1k)
        return round(cost, 4)

    def test_known_endpoint(self):
        cost = self._compute(total_chars=10000, row_count=10, endpoint="databricks-gpt-4o-mini")
        assert cost > 0

    def test_unknown_endpoint_uses_defaults(self):
        cost = self._compute(total_chars=10000, row_count=10, endpoint="unknown-endpoint")
        assert cost > 0

    def test_zero_chars_still_has_overhead(self):
        cost = self._compute(total_chars=0, row_count=10, endpoint="databricks-gpt-4o-mini")
        assert cost > 0

    def test_scales_with_rows(self):
        small = self._compute(total_chars=1000, row_count=10, endpoint="databricks-gpt-4o-mini")
        large = self._compute(total_chars=1000, row_count=1000, endpoint="databricks-gpt-4o-mini")
        assert large > small
