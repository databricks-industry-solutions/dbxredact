"""AI Query cost estimation utilities."""

import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count as spark_count, length, sum as spark_sum

logger = logging.getLogger(__name__)


TOKENS_PER_CHAR = 0.25

# Token prices per 1K tokens. Not fetched live -- no API exists.
# Source: https://www.databricks.com/product/pricing/foundation-model-training
# LAST_UPDATED: 2025-06-01
COST_PER_1K_INPUT_TOKENS = {
    "databricks-gpt-oss-120b": 0.001,
    "databricks-meta-llama-3-3-70b-instruct": 0.001,
    "databricks-claude-sonnet-4": 0.003,
    "databricks-gpt-4o-mini": 0.00015,
}
COST_PER_1K_OUTPUT_TOKENS = {
    "databricks-gpt-oss-120b": 0.002,
    "databricks-meta-llama-3-3-70b-instruct": 0.002,
    "databricks-claude-sonnet-4": 0.015,
    "databricks-gpt-4o-mini": 0.0006,
}

PROMPT_OVERHEAD_CHARS = 5500
ESTIMATED_OUTPUT_RATIO = 0.3


def estimate_ai_query_cost(
    df: DataFrame,
    text_column: str,
    endpoint: str,
    prompt_overhead_chars: int = PROMPT_OVERHEAD_CHARS,
    output_ratio: float = ESTIMATED_OUTPUT_RATIO,
) -> dict:
    """Estimate the cost of running AI Query detection on a DataFrame.

    Args:
        df: Source DataFrame
        text_column: Column containing text to analyze
        endpoint: Databricks serving endpoint name
        prompt_overhead_chars: Chars added by the system/user prompt template
        output_ratio: Estimated output tokens as fraction of input tokens

    Returns:
        Dict with estimated_input_tokens, estimated_output_tokens, estimated_cost_usd, row_count
    """
    stats = df.select(
        spark_sum(length(col(text_column))).alias("total_chars"),
        spark_count("*").alias("row_count"),
    ).first()

    total_chars = stats["total_chars"] or 0
    row_count = stats["row_count"]
    input_chars = total_chars + (row_count * prompt_overhead_chars)
    input_tokens = int(input_chars * TOKENS_PER_CHAR)
    output_tokens = int(input_tokens * output_ratio)

    input_cost_per_1k = COST_PER_1K_INPUT_TOKENS.get(endpoint, 0.001)
    output_cost_per_1k = COST_PER_1K_OUTPUT_TOKENS.get(endpoint, 0.002)

    cost = (input_tokens / 1000 * input_cost_per_1k) + (output_tokens / 1000 * output_cost_per_1k)

    return {
        "row_count": row_count,
        "total_chars": total_chars,
        "estimated_input_tokens": input_tokens,
        "estimated_output_tokens": output_tokens,
        "estimated_cost_usd": round(cost, 4),
        "endpoint": endpoint,
    }


def print_cost_estimate(estimate: dict) -> None:
    """Pretty-print a cost estimate."""
    lines = [
        "--- AI Query Cost Estimate ---",
        f"Endpoint:        {estimate['endpoint']}",
        f"Rows:            {estimate['row_count']:,}",
        f"Total chars:     {estimate['total_chars']:,}",
        f"Input tokens:    {estimate['estimated_input_tokens']:,}",
        f"Output tokens:   {estimate['estimated_output_tokens']:,}",
        f"Estimated cost:  ${estimate['estimated_cost_usd']:.4f}",
        "(Prices are approximate and may not reflect current Databricks pricing.)",
        "------------------------------",
    ]
    logger.info("\n".join(lines))
