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
    """Verify cost math against hand-computed expected values.

    Formula: input_chars = total_chars + row_count * OVERHEAD
             input_tokens = int(input_chars * 0.25)
             output_tokens = int(input_tokens * 0.3)
             cost = input_tokens/1000 * input_rate + output_tokens/1000 * output_rate
    """

    def test_gpt4o_mini_10rows_10k_chars(self):
        # input_chars = 10000 + 10*5500 = 65000
        # input_tokens = int(65000 * 0.25) = 16250
        # output_tokens = int(16250 * 0.3) = 4875
        # cost = 16.25*0.00015 + 4.875*0.0006 = 0.0024375 + 0.002925 = 0.0053625
        assert TOKENS_PER_CHAR == 0.25
        assert COST_PER_1K_INPUT_TOKENS["databricks-gpt-4o-mini"] == 0.00015
        assert COST_PER_1K_OUTPUT_TOKENS["databricks-gpt-4o-mini"] == 0.0006
        input_chars = 10000 + 10 * PROMPT_OVERHEAD_CHARS
        input_tokens = int(input_chars * TOKENS_PER_CHAR)
        assert input_tokens == 16250
        output_tokens = int(input_tokens * ESTIMATED_OUTPUT_RATIO)
        assert output_tokens == 4875

    def test_zero_chars_overhead_still_produces_cost(self):
        # With 10 rows and 0 chars, overhead alone = 55000 chars -> 13750 tokens
        input_tokens = int(10 * PROMPT_OVERHEAD_CHARS * TOKENS_PER_CHAR)
        assert input_tokens == 13750
        output_tokens = int(input_tokens * ESTIMATED_OUTPUT_RATIO)
        assert output_tokens == 4125

    def test_cost_scales_linearly_with_rows(self):
        # 1 row vs 100 rows with 0 chars: cost should scale ~100x
        one_row_tokens = int(1 * PROMPT_OVERHEAD_CHARS * TOKENS_PER_CHAR)
        hundred_row_tokens = int(100 * PROMPT_OVERHEAD_CHARS * TOKENS_PER_CHAR)
        assert hundred_row_tokens == 100 * one_row_tokens
