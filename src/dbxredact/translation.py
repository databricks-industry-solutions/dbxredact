"""Translation via Databricks AI Query with redaction marker preservation."""

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import expr

from .config import TRANSLATION_PROMPT_SKELETON

logger = logging.getLogger(__name__)

# Language name mapping for prompt readability
_LANG_NAMES = {"en": "English", "es": "Spanish"}


def translate_column(
    spark: SparkSession,
    df: DataFrame,
    text_column: str,
    source_lang: str,
    target_lang: str,
    endpoint: str = "databricks-gpt-oss-120b",
    num_cores: int = 10,
    reasoning_effort: str = "low",
    ai_model_type: str = "foundation",
    _repartition: bool = False,
) -> DataFrame:
    """Translate a text column using ai_query, preserving redaction markers.

    Args:
        spark: Active SparkSession.
        df: Input DataFrame (typically already redacted).
        text_column: Column containing text to translate.
        source_lang: Source language code (e.g. "es").
        target_lang: Target language code (e.g. "en").
        endpoint: Model serving endpoint.
        num_cores: Partition hint.
        reasoning_effort: LLM reasoning effort.
        ai_model_type: "foundation" or "external".
        _repartition: Whether to repartition before the query.

    Returns:
        DataFrame with ``{text_column}_translated`` column added.
    """
    src_name = _LANG_NAMES.get(source_lang, source_lang)
    tgt_name = _LANG_NAMES.get(target_lang, target_lang)

    prompt = TRANSLATION_PROMPT_SKELETON.format(
        source_lang=src_name,
        target_lang=tgt_name,
    )

    prompt_parts = prompt.split("{text}")
    prompt_prefix = prompt_parts[0].replace("'", "''")
    prompt_suffix = prompt_parts[1].replace("'", "''") if len(prompt_parts) > 1 else ""

    prompt_concat = (
        f"concat('{prompt_prefix}', CAST({text_column} AS STRING), '{prompt_suffix}')"
    )

    if ai_model_type == "external":
        query_expr = f"""
            ai_query('{endpoint}', {prompt_concat}, failOnError => false)
        """
    else:
        query_expr = f"""
            ai_query(
                '{endpoint}',
                {prompt_concat},
                failOnError => false,
                modelParameters => named_struct('reasoning_effort', '{reasoning_effort}')
            )
        """

    if _repartition:
        df = df.repartition(num_cores)

    out_col = f"{text_column}_translated"
    result_df = df.withColumn(out_col, expr(query_expr))
    logger.info("Translation column '%s' added (%s -> %s).", out_col, source_lang, target_lang)
    return result_df
