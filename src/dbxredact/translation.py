"""Post-redaction translation via ai_query, preserving redaction markers."""

import logging
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import expr

from .config import TRANSLATION_PROMPT_SKELETON

logger = logging.getLogger(__name__)

_LANG_NAMES = {"en": "English", "es": "Spanish"}


def translate_column(
    spark: SparkSession,
    df: DataFrame,
    text_column: str,
    source_lang: str,
    target_lang: str,
    endpoint: str = "databricks-gpt-oss-120b",
    reasoning_effort: str = "low",
) -> DataFrame:
    """Translate a text column while preserving bracket redaction markers.

    Adds ``{text_column}_translated`` to the DataFrame using ``ai_query``.
    """
    src_name = _LANG_NAMES.get(source_lang, source_lang)
    tgt_name = _LANG_NAMES.get(target_lang, target_lang)

    prompt = TRANSLATION_PROMPT_SKELETON.format(
        source_lang=src_name, target_lang=tgt_name, text="{text}",
    )
    parts = prompt.split("{text}")
    prefix = parts[0].replace("'", "''")
    suffix = parts[1].replace("'", "''") if len(parts) > 1 else ""

    prompt_concat = f"concat('{prefix}', CAST({text_column} AS STRING), '{suffix}')"
    ai_expr = f"""
        ai_query(
            '{endpoint}',
            {prompt_concat},
            failOnError => false,
            modelParameters => named_struct('reasoning_effort', '{reasoning_effort}')
        )
    """

    out_col = f"{text_column}_translated"
    logger.info("Translating %s (%s -> %s) via %s", text_column, src_name, tgt_name, endpoint)
    return df.withColumn(out_col, expr(ai_expr).getField("result"))
