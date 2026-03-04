"""Unity Catalog metadata query functions for PHI/PII redaction."""

import re
from typing import List, Dict
from pyspark.sql import SparkSession

_SAFE_IDENTIFIER = re.compile(r"^[a-zA-Z0-9_]+$")


def _validate_identifier(value: str, name: str) -> str:
    if not _SAFE_IDENTIFIER.match(value):
        raise ValueError(f"Invalid {name}: {value!r}")
    return value


def _parse_table_name(table_name: str):
    parts = table_name.split(".")
    if len(parts) != 3:
        raise ValueError(
            f"Table name must be fully qualified (catalog.schema.table): {table_name}"
        )
    catalog = _validate_identifier(parts[0], "catalog")
    schema = _validate_identifier(parts[1], "schema")
    table = _validate_identifier(parts[2], "table")
    return catalog, schema, table


def get_columns_by_tag(
    spark: SparkSession, table_name: str, tag_name: str, tag_value: str
) -> List[str]:
    """
    Query Unity Catalog to find columns with a specific tag value.

    Args:
        spark: Active SparkSession
        table_name: Fully qualified table name (catalog.schema.table)
        tag_name: Name of the tag to filter by
        tag_value: Value of the tag to match

    Returns:
        List of column names that have the specified tag and value
    """
    catalog, schema, table = _parse_table_name(table_name)
    _validate_identifier(tag_name, "tag_name")
    _validate_identifier(tag_value, "tag_value")

    query = f"""
    SELECT column_name
    FROM system.information_schema.column_tags
    WHERE table_catalog = '{catalog}'
      AND table_schema = '{schema}'
      AND table_name = '{table}'
      AND tag_name = '{tag_name}'
      AND tag_value = '{tag_value}'
    """

    result_df = spark.sql(query)
    return [row.column_name for row in result_df.collect()]


def get_protected_columns(
    spark: SparkSession, table_name: str, tag_name: str = "data_classification"
) -> List[str]:
    """
    Get columns marked as protected in Unity Catalog.

    Args:
        spark: Active SparkSession
        table_name: Fully qualified table name (catalog.schema.table)
        tag_name: Name of the classification tag (default: 'data_classification')

    Returns:
        List of column names marked as protected
    """
    return get_columns_by_tag(spark, table_name, tag_name, "protected")


def get_table_metadata(
    spark: SparkSession, table_name: str
) -> Dict[str, Dict[str, str]]:
    """
    Get comprehensive metadata for all columns in a table.

    Args:
        spark: Active SparkSession
        table_name: Fully qualified table name (catalog.schema.table)

    Returns:
        Dictionary mapping column names to their metadata (type, tags)
    """
    catalog, schema, table = _parse_table_name(table_name)

    columns_query = f"""
    SELECT column_name, data_type
    FROM system.information_schema.columns
    WHERE table_catalog = '{catalog}'
      AND table_schema = '{schema}'
      AND table_name = '{table}'
    """

    columns_df = spark.sql(columns_query)
    metadata = {
        row.column_name: {"type": row.data_type, "tags": {}}
        for row in columns_df.collect()
    }

    tags_query = f"""
    SELECT column_name, tag_name, tag_value
    FROM system.information_schema.column_tags
    WHERE table_catalog = '{catalog}'
      AND table_schema = '{schema}'
      AND table_name = '{table}'
    """

    tags_df = spark.sql(tags_query)
    for row in tags_df.collect():
        if row.column_name in metadata:
            metadata[row.column_name]["tags"][row.tag_name] = row.tag_value

    return metadata

