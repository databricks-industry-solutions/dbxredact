"""AI Query cost estimation utilities."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, length, sum as spark_sum


TOKENS_PER_CHAR = 0.25

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

PROMPT_OVERHEAD_CHARS = 500
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
    ).first()

    total_chars = stats["total_chars"] or 0
    row_count = df.count()
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
    print("--- AI Query Cost Estimate ---")
    print(f"Endpoint:        {estimate['endpoint']}")
    print(f"Rows:            {estimate['row_count']:,}")
    print(f"Total chars:     {estimate['total_chars']:,}")
    print(f"Input tokens:    {estimate['estimated_input_tokens']:,}")
    print(f"Output tokens:   {estimate['estimated_output_tokens']:,}")
    print(f"Estimated cost:  ${estimate['estimated_cost_usd']:.4f}")
    print("------------------------------")
