"""AI Judge for grading redaction quality and recommending improvements."""

from typing import Dict, Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, expr, from_json, lit

from .config import (
    JUDGE_PROMPT_SKELETON,
    NEXT_ACTION_PROMPT_SKELETON,
    DEFAULT_AI_REASONING_EFFORT,
)


def _build_judge_expr(
    endpoint: str,
    original_col: str,
    redacted_col: str,
    reasoning_effort: str = DEFAULT_AI_REASONING_EFFORT,
) -> str:
    """Build the ai_query SQL expression for the judge prompt."""
    prompt = JUDGE_PROMPT_SKELETON
    parts = prompt.split("{original_text}")
    prefix = parts[0]
    rest = parts[1]  # contains {redacted_text}
    mid_parts = rest.split("{redacted_text}")
    middle = mid_parts[0]
    suffix = mid_parts[1]

    prefix = prefix.replace("'", "''")
    middle = middle.replace("'", "''")
    suffix = suffix.replace("'", "''")

    return f"""
        ai_query(
            '{endpoint}',
            concat(
                '{prefix}',
                CAST({original_col} AS STRING),
                '{middle}',
                CAST({redacted_col} AS STRING),
                '{suffix}'
            ),
            failOnError => false,
            modelParameters => named_struct('reasoning_effort', '{reasoning_effort}')
        )
    """


def run_judge_evaluation(
    spark: SparkSession,
    df: DataFrame,
    original_text_col: str,
    redacted_text_col: str,
    endpoint: str,
    method_name: str,
    reasoning_effort: str = DEFAULT_AI_REASONING_EFFORT,
    num_cores: int = 10,
) -> DataFrame:
    """Run the AI judge on redacted text, returning grade + findings per row.

    Args:
        df: DataFrame containing both original and redacted text columns
        original_text_col: Column with the original (unredacted) text
        redacted_text_col: Column with the redacted text
        endpoint: Databricks model serving endpoint
        method_name: Detection method name (for labeling)
        reasoning_effort: AI reasoning effort level
        num_cores: Repartition parallelism

    Returns:
        DataFrame with columns: doc_id, method, grade, findings
    """
    judge_expr = _build_judge_expr(
        endpoint, original_text_col, redacted_text_col, reasoning_effort
    )

    result_col = f"judge_{method_name}"

    parsed_col = f"{result_col}_parsed"

    result_df = (
        df.repartition(num_cores)
        .withColumn(result_col, expr(judge_expr))
        .withColumn(parsed_col, from_json(col(f"{result_col}.result"), JUDGE_RESULT_SCHEMA))
        .select(
            "doc_id",
            lit(method_name).alias("method"),
            col(f"{parsed_col}.grade").alias("grade"),
            col(f"{parsed_col}.findings").alias("findings"),
        )
    )

    return result_df


JUDGE_RESULT_SCHEMA = (
    "struct<grade:string, findings:array<struct<"
    "entity:string, entity_type:string, status:string, explanation:string>>>"
)


def compute_judge_summary(judge_df: DataFrame) -> Dict[str, Any]:
    """Aggregate judge results into pass/partial/fail rates.

    Args:
        judge_df: DataFrame with at least 'method' and 'grade' columns

    Returns:
        Dict with pass_rate, partial_rate, fail_rate, total_docs, top_missed
    """
    total = judge_df.count()
    if total == 0:
        return {
            "pass_rate": 0.0,
            "partial_rate": 0.0,
            "fail_rate": 0.0,
            "total_docs": 0,
            "top_missed": [],
        }

    grade_counts = (
        judge_df.groupBy("grade").count().toPandas().set_index("grade")["count"]
    )
    pass_n = int(grade_counts.get("PASS", 0))
    partial_n = int(grade_counts.get("PARTIAL", 0))
    fail_n = int(grade_counts.get("FAIL", 0))

    from pyspark.sql.functions import explode_outer

    top_missed = (
        judge_df.select(explode_outer("findings").alias("f"))
        .where(col("f.status").isNotNull())
        .groupBy("f.entity_type", "f.status")
        .count()
        .orderBy(col("count").desc())
        .limit(15)
        .toPandas()
        .to_dict(orient="records")
    )

    return {
        "pass_rate": pass_n / total,
        "partial_rate": partial_n / total,
        "fail_rate": fail_n / total,
        "total_docs": total,
        "top_missed": top_missed,
    }


def run_next_action_query(
    spark: SparkSession,
    context: str,
    endpoint: str,
    reasoning_effort: str = "high",
) -> str:
    """Call AI Query with the next-action recommender prompt.

    Args:
        context: Serialized benchmark context string
        endpoint: Databricks model serving endpoint
        reasoning_effort: AI reasoning effort level

    Returns:
        Raw JSON string of recommendations
    """
    prompt = NEXT_ACTION_PROMPT_SKELETON.replace("{context}", context)
    prompt_escaped = prompt.replace("'", "''")

    query = f"""
        SELECT ai_query(
            '{endpoint}',
            '{prompt_escaped}',
            failOnError => false,
            modelParameters => named_struct('reasoning_effort', '{reasoning_effort}')
        ) AS recommendations
    """
    rows = spark.sql(query).collect()
    if not rows:
        return "No recommendations generated."
    result = rows[0]["recommendations"]
    if hasattr(result, "result"):
        return result.result
    return result
