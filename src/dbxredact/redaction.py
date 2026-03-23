"""Text redaction functions for PHI/PII removal."""

import logging
from typing import List, Dict, Any, Literal
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import pandas_udf, col
from pyspark.sql.types import StringType, StructType, StructField, IntegerType

logger = logging.getLogger(__name__)

RedactionStrategy = Literal["generic", "typed"]


def redact_text(
    text: str, entities: List[Dict[str, Any]], strategy: RedactionStrategy = "generic"
) -> str:
    """
    Redact PII/PHI entities from text.

    Overlapping spans are merged so that all detected PII is covered.
    Out-of-range start/end values are clamped to text boundaries.

    Args:
        text: Original text containing PII/PHI
        entities: List of entity dicts with 'start', 'end', 'entity_type' keys
        strategy: Redaction strategy:
            - 'generic': Replace with '[REDACTED]'
            - 'typed': Replace with '[ENTITY_TYPE]' (e.g., '[PERSON]')

    Returns:
        Text with entities redacted
    """
    if not entities:
        return text

    text_len = len(text)
    valid = [
        (max(0, e["start"]), min(text_len, e["end"]), e.get("entity_type", "REDACTED"))
        for e in entities
        if e.get("start") is not None and e.get("end") is not None
    ]
    valid.sort(key=lambda t: (t[0], -t[1]))

    merged: List[tuple] = []
    for start, end, etype in valid:
        if start >= end:
            continue
        if merged and start <= merged[-1][1]:
            prev_s, prev_e, prev_t = merged[-1]
            merged[-1] = (prev_s, max(prev_e, end), prev_t)
        else:
            merged.append((start, end, etype))

    parts = []
    prev_end = 0
    for start, end, entity_type in merged:
        parts.append(text[prev_end:start])
        parts.append(f"[{entity_type}]" if strategy == "typed" else "[REDACTED]")
        prev_end = end
    parts.append(text[prev_end:])
    return "".join(parts)


def _safe_entity_list(entities) -> list:
    """Convert entities to a list of dicts, returning [] on any failure."""
    if entities is None:
        return []
    try:
        if len(entities) == 0:
            return []
    except (TypeError, AttributeError):
        return []
    if not isinstance(entities, list):
        try:
            entities = list(entities)
        except (TypeError, ValueError):
            return []
    return [e.asDict() if hasattr(e, "asDict") else (dict(e) if not isinstance(e, dict) else e) for e in entities]


def create_redaction_udf(strategy: RedactionStrategy = "generic"):
    """
    Create a Pandas UDF for redacting text in DataFrames.

    Args:
        strategy: Redaction strategy ('generic' or 'typed')

    Returns:
        Pandas UDF that takes (text, entities) and returns redacted text
    """

    @pandas_udf(StringType())
    def redact_udf(texts: pd.Series, entities_series: pd.Series) -> pd.Series:
        results = []
        for text, entities in zip(texts, entities_series):
            ent_list = _safe_entity_list(entities)
            results.append(redact_text(text, ent_list, strategy=strategy))
        return pd.Series(results)

    return redact_udf


_AUDIT_SCHEMA = StructType([
    StructField("redacted_text", StringType()),
    StructField("entity_count", IntegerType()),
])


def create_redaction_audit_udf(strategy: RedactionStrategy = "generic"):
    """Like :func:`create_redaction_udf` but returns a struct with
    ``(redacted_text, entity_count)`` for audit / detection-status tracking.
    """

    @pandas_udf(_AUDIT_SCHEMA)
    def redact_audit_udf(texts: pd.Series, entities_series: pd.Series) -> pd.DataFrame:
        rows = []
        for text, entities in zip(texts, entities_series):
            ent_list = _safe_entity_list(entities)
            redacted = redact_text(text, ent_list, strategy=strategy)
            rows.append((redacted, len(ent_list)))
        return pd.DataFrame(rows, columns=["redacted_text", "entity_count"])

    return redact_audit_udf


def create_redacted_table(
    spark: SparkSession,
    source_df: DataFrame,
    text_column: str,
    entities_column: str,
    output_table: str,
    strategy: RedactionStrategy = "generic",
    suffix: str = "_redacted",
) -> DataFrame:
    """
    Create a redacted version of a table.

    Args:
        spark: Active SparkSession
        source_df: DataFrame with text and detected entities
        text_column: Name of column containing text to redact
        entities_column: Name of column containing entity lists
        output_table: Fully qualified output table name
        strategy: Redaction strategy ('generic' or 'typed')
        suffix: Suffix for redacted column name (default: '_redacted')

    Returns:
        DataFrame with redacted text
    """
    redact_udf = create_redaction_udf(strategy=strategy)

    redacted_col_name = f"{text_column}{suffix}"
    result_df = source_df.withColumn(
        redacted_col_name, redact_udf(col(text_column), col(entities_column))
    )

    result_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
        output_table
    )

    return result_df

