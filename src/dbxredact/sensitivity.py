"""Table-level PII/PHI sensitivity classification.

Scans one or more Unity Catalog tables and classifies them at two granularities:

* **table level** -- ``data_sensitivity`` in {``none``, ``pii``, ``phi``, ``pci``}
* **column level** -- a ``pii_type`` (``ssn``, ``email``, ``free_text`` ...) per column

Detection reuses the existing ensemble (:func:`dbxredact.detection.run_detection`)
unchanged.  The table is *pivoted so each column becomes one "document"*: values are
sampled, unpivoted to ``(column_name, value)`` rows, grouped per column, and assembled
into a single bounded text payload.  Detection then runs with
``doc_id_column="column_name"``, so AI Query costs **one call per column, independent of
the row sample size** -- the sample size is a pure recall dial, not a cost multiplier.

Classification rolls up the ``entity_type`` values the detectors emit; it does not need a
custom AI prompt or return type.  See :data:`_ENTITY_TO_PII_TYPE`.

This module only *reads* and *analyses*.  Writing the resulting tags back to Unity
Catalog is a separate, governance-gated step (added later).
"""

import logging
from dataclasses import dataclass, field
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, collect_list, explode, expr

from .config import RedactionConfig
from .detection import run_detection
from .metadata import _parse_table_name, _validate_identifier, get_table_metadata

logger = logging.getLogger(__name__)

# --- Sensitivity levels -----------------------------------------------------
SENSITIVITY_LEVELS = ("none", "pii", "phi", "pci")

# Ranking for min_level filtering.  Matches the rollup precedence PHI > PCI > PII
# (see _rollup_table): a table tagged at or above the requested rank is returned.
_LEVEL_ORDER = {"none": 0, "pii": 1, "pci": 2, "phi": 3}

# UC tag names.
TABLE_SENSITIVITY_TAG = "data_sensitivity"
COLUMN_CLASSIFICATION_TAG = "data_classification"
COLUMN_PII_TYPE_TAG = "pii_type"
_PROTECTED_VALUE = "protected"

# Spark SQL string types eligible for value-level PII scanning.
_STRING_TYPES = {"string", "varchar", "char", "text"}

# Default payload caps (see _build_payload).
DEFAULT_SAMPLE_ROWS = 200
DEFAULT_MAX_VALUES_PER_COLUMN = 200
DEFAULT_MAX_CHARS_PER_VALUE = 200
DEFAULT_MAX_PAYLOAD_CHARS = 8000

# --- Entity type -> pii_type -----------------------------------------------
# Maps the ``entity_type`` values emitted by any detector (Presidio names,
# AI LABEL_ENUMS, and GLiNER's already-normalised names -- see
# PRESIDIO_ENTITY_TYPES / LABEL_ENUMS / GLINER_LABEL_MAP in config.py) to a
# coarse ``pii_type`` that redaction consumes.  Structured types get a specific
# tag (so masking can target them); everything contextual falls back to
# ``free_text`` (so span redaction runs).
_ENTITY_TO_PII_TYPE = {
    # Direct identifiers
    "US_SSN": "ssn",
    "EMAIL_ADDRESS": "email",
    "PHONE_NUMBER": "phone",
    "FAX_NUMBER": "phone",
    "CREDIT_CARD": "credit_card",
    "IBAN_CODE": "iban",
    "ACCOUNT_NUMBER": "account_number",
    "BANK_ROUTING_NUMBER": "bank_routing_number",
    "IP_ADDRESS": "ip_address",
    "IP": "ip_address",
    "MAC_ADDRESS": "mac_address",
    "URL": "url",
    "VIN": "vehicle_identifier",
    "VEHICLE_IDENTIFIER": "vehicle_identifier",
    "MEDICAL_RECORD_NUMBER": "medical_record_number",
    "HEALTH_PLAN_ID": "health_plan_number",
    "HEALTH_PLAN_NUMBER": "health_plan_number",
    "DEA_NUMBER": "dea_number",
    "NPI_NUMBER": "npi_number",
    "US_EIN": "ein",
    "US_DRIVER_LICENSE": "driver_license",
    "DRIVER_LICENSE": "driver_license",
    "LICENSE_NUMBER": "license_number",
    "DEVICE_ID": "device_id",
    "DATE_OF_BIRTH": "date_of_birth",
    "BIRTH_DATE": "date_of_birth",
    "ID_NUMBER": "id_number",
    "CUSTOMER_ID": "id_number",
    "PIN": "id_number",
    "UK_NHS": "id_number",
    "AU_ACN": "id_number",
    "AU_MEDICARE": "id_number",
    "AU_TFN": "id_number",
    # Contextual / free-text identifiers -> span redaction
    "PERSON": "free_text",
    "LOCATION": "free_text",
    "DATE_TIME": "free_text",
    "APPOINTMENT_DATE_TIME": "free_text",
    "AGE": "free_text",
    "AGE_GENDER": "free_text",
    "HOSPITAL_NAME": "free_text",
    "ORGANIZATION": "free_text",
}

# Entity types that make a column (and thus its table) PHI.
_PHI_ENTITY_TYPES = {
    "MEDICAL_RECORD_NUMBER",
    "HEALTH_PLAN_ID",
    "HEALTH_PLAN_NUMBER",
    "DEA_NUMBER",
    "NPI_NUMBER",
    "DATE_OF_BIRTH",
    "BIRTH_DATE",
    "APPOINTMENT_DATE_TIME",
    "HOSPITAL_NAME",
}

# Entity types that make a column (and thus its table) PCI.
_PCI_ENTITY_TYPES = {
    "CREDIT_CARD",
    "IBAN_CODE",
    "ACCOUNT_NUMBER",
    "BANK_ROUTING_NUMBER",
}

# Entity types that carry no PII signal on their own -- ignored in rollups.
# (Mirrors the suppression intent of config.py's entity filter for NRP.)
_IGNORED_ENTITY_TYPES = {"NRP", "MEDICAL_INFO"}


@dataclass
class ColumnSensitivity:
    """Per-column classification result."""

    column_name: str
    data_type: str
    pii_type: str  # a value from _ENTITY_TO_PII_TYPE, or "none"
    entity_types: List[str] = field(default_factory=list)
    hit_rate: float = 0.0
    confidence: float = 0.0


@dataclass
class TableSensitivity:
    """Table-level classification result."""

    table_name: str
    data_sensitivity: str  # one of SENSITIVITY_LEVELS
    columns: List[ColumnSensitivity] = field(default_factory=list)
    sampled_rows: int = 0
    warnings: List[str] = field(default_factory=list)

    @property
    def pii_columns(self) -> List[ColumnSensitivity]:
        """Columns that carry a PII signal (pii_type != 'none')."""
        return [c for c in self.columns if c.pii_type != "none"]


# ---------------------------------------------------------------------------
# Pure helpers (no Spark) -- unit-tested directly
# ---------------------------------------------------------------------------
def _build_payload(
    values: List[Optional[str]],
    *,
    max_chars_per_value: int = DEFAULT_MAX_CHARS_PER_VALUE,
    max_values_per_column: int = DEFAULT_MAX_VALUES_PER_COLUMN,
    max_payload_chars: int = DEFAULT_MAX_PAYLOAD_CHARS,
) -> tuple:
    """Assemble a bounded text payload from a column's sampled values.

    Three independent guards apply; whichever binds first wins, so neither a
    high-cardinality column nor a massive-unstructured column can produce a
    runaway AI Query payload:

    1. ``max_chars_per_value`` -- truncate each individual value.
    2. distinct + ``max_values_per_column`` -- dedupe, then cap the count.
    3. ``max_payload_chars`` -- hard ceiling on total payload length.

    Returns ``(payload_text, truncated)``.  When truncation occurs the payload
    ends with a ``... (truncated, N of M values)`` marker so a truncated scan is
    never mistaken for a complete one.
    """
    # Dedupe preserving first-seen order; drop nulls/blanks.
    seen = set()
    distinct: List[str] = []
    for v in values:
        if v is None:
            continue
        s = str(v).strip()
        if not s or s in seen:
            continue
        seen.add(s)
        distinct.append(s)

    total_distinct = len(distinct)
    if total_distinct == 0:
        return "", False

    truncated = False
    used = 0
    parts: List[str] = []
    running = 0
    for s in distinct:
        if used >= max_values_per_column:
            truncated = True
            break
        clipped = s[:max_chars_per_value]
        if len(s) > max_chars_per_value:
            truncated = True
        # +1 for the newline joiner.
        projected = running + len(clipped) + (1 if parts else 0)
        if projected > max_payload_chars:
            truncated = True
            break
        parts.append(clipped)
        running = projected
        used += 1

    if used < total_distinct:
        truncated = True

    payload = "\n".join(parts)
    if truncated:
        marker = f"\n... (truncated, {used} of {total_distinct} values)"
        payload = payload + marker
    return payload, truncated


def _rollup_column(entity_types: List[str], data_type: str, hit_rate: float) -> str:
    """Pick the dominant ``pii_type`` for a column from its detected entity types.

    Structured types win over ``free_text``: if any non-free-text pii_type was
    detected, the most frequent such type is chosen; otherwise ``free_text`` if
    any contextual entity was detected; else ``none``.
    """
    counts: dict = {}
    saw_free_text = False
    for et in entity_types:
        if et in _IGNORED_ENTITY_TYPES:
            continue
        pt = _ENTITY_TO_PII_TYPE.get(et)
        if pt is None:
            continue
        if pt == "free_text":
            saw_free_text = True
            continue
        counts[pt] = counts.get(pt, 0) + 1

    if counts:
        # Most frequent structured type; ties broken alphabetically for determinism.
        return sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]
    if saw_free_text:
        return "free_text"
    return "none"


def _rollup_table(columns: List[ColumnSensitivity]) -> str:
    """Apply PHI > PCI > PII > none precedence across a table's columns."""
    all_entity_types = {et for c in columns for et in c.entity_types}
    has_any_pii = any(c.pii_type != "none" for c in columns)

    if all_entity_types & _PHI_ENTITY_TYPES:
        return "phi"
    if all_entity_types & _PCI_ENTITY_TYPES:
        return "pci"
    if has_any_pii:
        return "pii"
    return "none"


# ---------------------------------------------------------------------------
# Spark classifier
# ---------------------------------------------------------------------------
def _pivot_to_column_payloads(
    spark: SparkSession,
    sample_df: DataFrame,
    string_columns: List[str],
    *,
    max_chars_per_value: int,
    max_values_per_column: int,
    max_payload_chars: int,
) -> tuple:
    """Unpivot the sample to (column_name, value), group per column, and build one
    bounded payload string per column.

    Returns ``(payload_df, warnings)`` where *payload_df* has columns
    ``column_name`` and ``payload`` -- one row per string column.
    """
    # stack(N, 'col1', CAST(col1 AS STRING), ...) -> (column_name, value)
    n = len(string_columns)
    stack_args = ", ".join(f"'{c}', CAST(`{c}` AS STRING)" for c in string_columns)
    long_df = sample_df.select(
        expr(f"stack({n}, {stack_args}) as (column_name, value)")
    )

    grouped = long_df.groupBy("column_name").agg(collect_list("value").alias("values"))

    rows = grouped.collect()
    warnings: List[str] = []
    payload_rows = []
    values_by_col = {r["column_name"]: (r["values"] or []) for r in rows}
    for col_name in string_columns:
        values = values_by_col.get(col_name, [])
        payload, truncated = _build_payload(
            values,
            max_chars_per_value=max_chars_per_value,
            max_values_per_column=max_values_per_column,
            max_payload_chars=max_payload_chars,
        )
        if truncated:
            warnings.append(
                f"Column '{col_name}': sample payload truncated by caps "
                f"(max_values_per_column={max_values_per_column}, "
                f"max_chars_per_value={max_chars_per_value}, "
                f"max_payload_chars={max_payload_chars})."
            )
        payload_rows.append((col_name, payload))

    payload_df = spark.createDataFrame(payload_rows, ["column_name", "payload"])
    return payload_df, warnings


def _aggregate_entity_types(detected_df: DataFrame) -> dict:
    """Collect the set/list of entity_types each column payload produced.

    Reads whichever detector struct columns are present after run_detection
    (``presidio_results_struct``, ``ai_results_struct``, ``gliner_results_struct``)
    -- all share the entity schema (entity, entity_type, score, start, end, doc_id).
    Returns ``{column_name: {"entity_types": [...], "hit_rate": float}}``.
    """
    struct_cols = [
        c
        for c in (
            "presidio_results_struct",
            "ai_results_struct",
            "gliner_results_struct",
        )
        if c in detected_df.columns
    ]
    result: dict = {}
    for scol in struct_cols:
        exploded = detected_df.select(
            "column_name", explode(col(scol)).alias("_e")
        ).select("column_name", col("_e.entity_type").alias("entity_type"))
        for r in exploded.collect():
            entry = result.setdefault(r["column_name"], {"entity_types": []})
            if r["entity_type"] is not None:
                entry["entity_types"].append(r["entity_type"])
    return result


def classify_table(
    spark: SparkSession,
    table_name: str,
    *,
    sample_rows: int = DEFAULT_SAMPLE_ROWS,
    max_values_per_column: int = DEFAULT_MAX_VALUES_PER_COLUMN,
    max_chars_per_value: int = DEFAULT_MAX_CHARS_PER_VALUE,
    max_payload_chars: int = DEFAULT_MAX_PAYLOAD_CHARS,
    use_presidio: bool = True,
    use_ai_query: bool = True,
    presidio_pattern_only: bool = True,
    config: Optional[RedactionConfig] = None,
) -> TableSensitivity:
    """Classify a UC table's PII/PHI sensitivity by scanning a row sample.

    The table is pivoted so each column becomes one "document" fed to the
    detection ensemble, making AI Query cost **one call per column regardless of
    ``sample_rows``** (the pivot invariant -- sample size is a recall dial, not a
    cost multiplier).  Sensitivity is rolled up from the detected entity types.

    Detector roles for the scan: **AI Query is the primary recall driver** (it
    sees the column name plus the full value list, so it types columns well even
    without sentence context), while **Presidio runs pattern-only as a free,
    local, high-precision confirmer** of structured identifiers (SSN, credit
    card, email, phone, HIPAA IDs).  Presidio's NER-dependent weakness (PERSON,
    LOCATION, free text) is both covered by AI Query and diluted by aggregation
    over the sampled values, so pattern-only mode -- which needs no spaCy
    download and cannot raise ``SpacyModelNotFoundError`` -- is the safe default.

    Args:
        table_name: Fully qualified name (catalog.schema.table).
        sample_rows: Rows to sample from the table (recall dial; no AI cost impact).
        max_values_per_column: Distinct-value count cap per column payload.
        max_chars_per_value: Per-value truncation length.
        max_payload_chars: Hard ceiling on each column's payload length.
        use_presidio / use_ai_query: Detectors to run during the scan.
        presidio_pattern_only: Run Presidio without spaCy (default True). Only
            applies to the default config; ignored when ``config`` is supplied.
        config: Optional RedactionConfig.  When supplied it is forwarded to
            run_detection **verbatim** and the ``use_*`` / ``presidio_pattern_only``
            kwargs above are ignored (the caller owns the trade-off).

    Returns:
        TableSensitivity with per-column results and a rolled-up data_sensitivity.
    """
    if config is None:
        config = RedactionConfig(
            use_presidio=use_presidio,
            use_ai_query=use_ai_query,
            presidio_pattern_only=presidio_pattern_only,
            fail_on_presidio_error=False,
        )
    metadata = get_table_metadata(spark, table_name)
    string_columns = [
        name
        for name, info in metadata.items()
        if info.get("type", "").lower() in _STRING_TYPES
    ]

    warnings: List[str] = []
    if not string_columns:
        logger.info("classify_table: no string columns in %s", table_name)
        return TableSensitivity(
            table_name=table_name,
            data_sensitivity="none",
            columns=[],
            sampled_rows=0,
            warnings=["No string columns to scan."],
        )

    sample_df = (
        spark.table(table_name)
        .select(*[col(f"`{c}`") for c in string_columns])
        .limit(sample_rows)
    )
    sample_df = sample_df.cache()
    actual_rows = sample_df.count()

    payload_df, pivot_warnings = _pivot_to_column_payloads(
        spark,
        sample_df,
        string_columns,
        max_chars_per_value=max_chars_per_value,
        max_values_per_column=max_values_per_column,
        max_payload_chars=max_payload_chars,
    )
    warnings.extend(pivot_warnings)
    sample_df.unpersist()

    # config is always set by here (default built above or caller-supplied) and
    # run_detection reads detector flags from it, overriding any kwargs.
    detected_df = run_detection(
        spark,
        payload_df,
        doc_id_column="column_name",
        text_column="payload",
        config=config,
    )

    agg = _aggregate_entity_types(detected_df)

    columns: List[ColumnSensitivity] = []
    for col_name in string_columns:
        entity_types = agg.get(col_name, {}).get("entity_types", [])
        # hit_rate: coarse saturation proxy -- detections per sampled value,
        # capped at 1.0.  actual_rows is an upper bound on distinct values.
        denom = max(1, min(actual_rows, max_values_per_column))
        hit_rate = min(1.0, len(entity_types) / denom)
        pii_type = _rollup_column(
            entity_types, metadata[col_name].get("type", ""), hit_rate
        )
        confidence = (
            1.0
            if pii_type not in ("none", "free_text")
            else (hit_rate if pii_type == "free_text" else 0.0)
        )
        columns.append(
            ColumnSensitivity(
                column_name=col_name,
                data_type=metadata[col_name].get("type", ""),
                pii_type=pii_type,
                entity_types=sorted(set(entity_types)),
                hit_rate=hit_rate,
                confidence=confidence,
            )
        )

    data_sensitivity = _rollup_table(columns)

    return TableSensitivity(
        table_name=table_name,
        data_sensitivity=data_sensitivity,
        columns=columns,
        sampled_rows=actual_rows,
        warnings=warnings,
    )


# ---------------------------------------------------------------------------
# Tag writing (governance-gated) and reading
# ---------------------------------------------------------------------------
def _check_tag_write_gate(dry_run: bool, confirm: bool) -> None:
    """Raise if a real tag write is requested without explicit opt-in.

    Writing UC tags mutates catalog metadata, so it follows the house pattern:
    warn, then require an explicit flag (mirrors ``confirm_destructive`` in
    pipeline.py).
    """
    if dry_run:
        return
    logger.warning(
        "GOVERNANCE: apply_sensitivity_tags(dry_run=False) will write Unity "
        "Catalog tags (data_sensitivity on the table, data_classification/"
        "pii_type on columns), mutating catalog metadata."
    )
    if not confirm:
        raise ValueError(
            "Writing sensitivity tags mutates Unity Catalog metadata. "
            "Pass confirm=True to proceed, or keep dry_run=True to preview the "
            "statements without executing them."
        )


def _tag_value_literal(value: str) -> str:
    """Escape a tag value for safe single-quoted SQL literal use.

    Tag *values* (unlike identifiers) may legitimately contain characters like
    spaces, so they cannot go through _validate_identifier; instead they are
    escaped as SQL string literals.
    """
    return value.replace("'", "''")


def build_sensitivity_tag_statements(result: TableSensitivity) -> List[str]:
    """Build the ALTER TABLE statements that would tag *result*'s table.

    Pure/side-effect-free: validates identifiers and returns the SQL strings so
    they can be previewed (dry-run) or executed.  One table-level statement plus
    one per PII column.
    """
    catalog, schema, table = _parse_table_name(result.table_name)
    fqn = f"{catalog}.{schema}.{table}"

    statements: List[str] = [
        f"ALTER TABLE {fqn} SET TAGS "
        f"('{TABLE_SENSITIVITY_TAG}' = '{_tag_value_literal(result.data_sensitivity)}')"
    ]

    for column in result.pii_columns:
        col_name = _validate_identifier(column.column_name, "column")
        pii_type = _tag_value_literal(column.pii_type)
        statements.append(
            f"ALTER TABLE {fqn} ALTER COLUMN {col_name} SET TAGS "
            f"('{COLUMN_CLASSIFICATION_TAG}' = '{_PROTECTED_VALUE}', "
            f"'{COLUMN_PII_TYPE_TAG}' = '{pii_type}')"
        )

    return statements


def apply_sensitivity_tags(
    spark: SparkSession,
    result: TableSensitivity,
    *,
    dry_run: bool = True,
    confirm: bool = False,
) -> List[str]:
    """Write the sensitivity tags from a classification result to Unity Catalog.

    Writes one table-level ``data_sensitivity`` tag plus, for each PII column,
    the ``data_classification=protected`` / ``pii_type`` tags that
    :func:`dbxredact.metadata.discover_pii_columns` (and thus
    ``run_table_redaction``) already consume -- closing the scan -> tag ->
    redact loop.

    Governance: this mutates catalog metadata, so it defaults to ``dry_run=True``
    (returns the statements, executes nothing) and requires an explicit
    ``confirm=True`` to actually write.

    Args:
        result: A TableSensitivity from :func:`classify_table`.
        dry_run: If True (default), return the SQL without executing it.
        confirm: Must be True to execute when ``dry_run=False``.

    Returns:
        The list of ALTER TABLE statements (executed when ``dry_run=False`` and
        ``confirm=True``; otherwise just previewed).
    """
    _check_tag_write_gate(dry_run, confirm)

    statements = build_sensitivity_tag_statements(result)

    if dry_run:
        logger.info(
            "apply_sensitivity_tags dry-run for %s: %d statement(s) (not executed).",
            result.table_name,
            len(statements),
        )
        return statements

    for stmt in statements:
        logger.info("Executing: %s", stmt)
        spark.sql(stmt)
    logger.info(
        "Applied %d sensitivity tag statement(s) to %s.",
        len(statements),
        result.table_name,
    )
    return statements


def get_tables_by_sensitivity(
    spark: SparkSession,
    catalog: str,
    schema: Optional[str] = None,
    min_level: str = "pii",
) -> List[str]:
    """List tables tagged at or above a sensitivity level.

    Reads ``system.information_schema.table_tags`` (note: this view uses
    ``catalog_name``/``schema_name``, unlike ``column_tags`` which uses
    ``table_catalog``/``table_schema``).

    Args:
        catalog: Catalog to scan.
        schema: Optional schema filter.
        min_level: Minimum sensitivity to include (one of SENSITIVITY_LEVELS,
            excluding "none").  Ranking: phi > pci > pii.

    Returns:
        Fully qualified ``catalog.schema.table`` names, sorted.
    """
    _validate_identifier(catalog, "catalog")
    if schema is not None:
        _validate_identifier(schema, "schema")
    if min_level not in _LEVEL_ORDER:
        raise ValueError(
            f"Invalid min_level={min_level!r}; choose from {SENSITIVITY_LEVELS}."
        )

    min_rank = _LEVEL_ORDER[min_level]
    included = [
        lvl for lvl, rank in _LEVEL_ORDER.items() if rank >= min_rank and lvl != "none"
    ]
    if not included:
        return []
    values_clause = ", ".join(f"'{lvl}'" for lvl in included)

    query = f"""
    SELECT catalog_name, schema_name, table_name
    FROM system.information_schema.table_tags
    WHERE catalog_name = '{catalog}'
      AND tag_name = '{TABLE_SENSITIVITY_TAG}'
      AND tag_value IN ({values_clause})
    """
    if schema is not None:
        query += f"      AND schema_name = '{schema}'\n"

    rows = spark.sql(query).collect()
    return sorted(
        f"{r['catalog_name']}.{r['schema_name']}.{r['table_name']}" for r in rows
    )


def get_table_sensitivity_level(spark: SparkSession, table_name: str) -> Optional[str]:
    """Read a single table's ``data_sensitivity`` tag value.

    Returns the level string (one of SENSITIVITY_LEVELS) or ``None`` if the table
    has no ``data_sensitivity`` tag.
    """
    catalog, schema, table = _parse_table_name(table_name)
    query = f"""
    SELECT tag_value
    FROM system.information_schema.table_tags
    WHERE catalog_name = '{catalog}'
      AND schema_name = '{schema}'
      AND table_name = '{table}'
      AND tag_name = '{TABLE_SENSITIVITY_TAG}'
    """
    rows = spark.sql(query).collect()
    return rows[0]["tag_value"] if rows else None
