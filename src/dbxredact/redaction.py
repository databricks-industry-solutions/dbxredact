"""Text redaction functions for PHI/PII removal."""

from typing import List, Dict, Any, Literal
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import pandas_udf, col
from pyspark.sql.types import StringType


RedactionStrategy = Literal["generic", "typed"]


def redact_text(
    text: str, entities: List[Dict[str, Any]], strategy: RedactionStrategy = "generic"
) -> str:
    """
    Redact PII/PHI entities from text.

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

    valid = [(e["start"], e["end"], e.get("entity_type", "REDACTED"))
             for e in entities if e.get("start") is not None and e.get("end") is not None]
    valid.sort(key=lambda t: t[0])

    parts = []
    prev_end = 0
    for start, end, entity_type in valid:
        if start < prev_end:
            continue
        parts.append(text[prev_end:start])
        parts.append(f"[{entity_type}]" if strategy == "typed" else "[REDACTED]")
        prev_end = end
    parts.append(text[prev_end:])
    return "".join(parts)


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
        """Redact entities from text for each row."""
        results = []
        for text, entities in zip(texts, entities_series):
            if entities is None:
                results.append(text)
                continue

            try:
                if len(entities) == 0:
                    results.append(text)
                    continue
            except (TypeError, AttributeError):
                results.append(text)
                continue

            if not isinstance(entities, list):
                try:
                    entities = list(entities)
                except (TypeError, ValueError):
                    results.append(text)
                    continue

            results.append(redact_text(text, entities, strategy=strategy))

        return pd.Series(results)

    return redact_udf


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

