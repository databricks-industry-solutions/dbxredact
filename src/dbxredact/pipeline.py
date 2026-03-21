"""End-to-end PHI/PII detection and redaction pipelines."""

import json
import logging
import math
import time
import uuid
from typing import Optional, Literal
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.functions import col, array, lit, when, size, explode, count as spark_count, current_timestamp
from pyspark.sql.types import (
    ArrayType,
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)

from .detection import run_detection
from .alignment import align_entities_udf
from .redaction import create_redaction_udf, create_redaction_audit_udf, RedactionStrategy
from .metadata import get_columns_by_tag
from .config import DEFAULT_GLINER_MODEL, DEFAULT_GLINER_THRESHOLD, DEFAULT_GLINER_MAX_WORDS, DEFAULT_AI_REASONING_EFFORT, RedactionConfig
from .entity_filter import EntityFilter, apply_safe_filter, apply_block_filter

logger = logging.getLogger(__name__)

_SENTINEL = object()


def _apply_config(config: RedactionConfig, local_vars: dict) -> dict:
    """Overlay RedactionConfig fields onto function locals.

    When ``config`` is provided, **all** matching fields from the config
    unconditionally replace the corresponding entries in *local_vars*.
    Callers should pass *either* ``config=`` *or* individual kwargs -- if
    both are supplied, the config values win.
    """
    if config is None:
        return local_vars
    from dataclasses import fields as dc_fields
    for f in dc_fields(config):
        if f.name in local_vars:
            local_vars[f.name] = getattr(config, f.name)
    return local_vars

OutputStrategy = Literal["validation", "production"]

# Governance floor constants
MIN_SCORE_THRESHOLD = 0.1
MIN_GLINER_THRESHOLD = 0.05


AlignmentMode = Literal["union", "consensus"]

OutputMode = Literal["separate", "in_place"]


def _check_validation_output(output_strategy: str, confirm_validation_output: bool) -> None:
    """Raise if validation/debug output is requested without explicit opt-in."""
    if output_strategy != "validation":
        return
    logger.warning(
        "GOVERNANCE: output_strategy='validation' will persist raw PII "
        "(original text, entity text, positions) to the output table. "
        "Use output_strategy='production' unless debugging."
    )
    if not confirm_validation_output:
        raise ValueError(
            "output_strategy='validation' writes raw PII to the output table. "
            "Pass confirm_validation_output=True to proceed, or use "
            "output_strategy='production' for safe output."
        )


def _check_consensus_safety(alignment_mode: str, allow_consensus_redaction: bool) -> None:
    """Raise if consensus alignment is used for redaction without explicit opt-in."""
    if alignment_mode != "consensus":
        return
    logger.warning(
        "SAFETY: consensus alignment mode requires multiple detectors to agree. "
        "This reduces recall and may leave PII unredacted. "
        "Use 'union' mode for production redaction."
    )
    if not allow_consensus_redaction:
        raise ValueError(
            "Consensus alignment mode is unsafe for redaction without explicit "
            "opt-in. Pass allow_consensus_redaction=True to proceed, or use "
            "alignment_mode='union' for safer recall."
        )


def _write_in_place(
    spark: SparkSession,
    result_df: DataFrame,
    source_table: str,
    doc_id_column: str,
    text_column: str,
) -> None:
    """Overwrite the text column in the source table with redacted text.

    This is a **destructive** operation -- the original text is permanently
    replaced.  The caller is responsible for gating access behind
    ``confirm_destructive=True``.

    Uses MERGE INTO so that only rows present in *result_df* are touched;
    rows that were excluded by ``max_rows`` or dedup remain unchanged.
    """
    from .metadata import _validate_identifier, _parse_table_name

    logger.warning(
        "DESTRUCTIVE OPERATION: in-place redaction will permanently overwrite "
        "column '%s' in %s. This cannot be undone.", text_column, source_table,
    )

    _parse_table_name(source_table)
    _validate_identifier(doc_id_column, "doc_id_column")
    _validate_identifier(text_column, "text_column")

    redacted_col_name = f"{text_column}_redacted"
    merge_df = result_df.select(col(doc_id_column), col(redacted_col_name))

    view_name = f"_dbxredact_inplace_{int(time.time())}"
    merge_df.createOrReplaceTempView(view_name)

    spark.sql(f"""
        MERGE INTO {source_table} t
        USING {view_name} s
        ON t.`{doc_id_column}` = s.`{doc_id_column}`
        WHEN MATCHED THEN UPDATE SET t.`{text_column}` = s.`{redacted_col_name}`
    """)
    logger.info("In-place redaction complete: %s.%s updated in %s", text_column, doc_id_column, source_table)


def _apply_alignment(
    df: DataFrame,
    doc_id_column: str,
    use_presidio: bool,
    use_ai_query: bool,
    use_gliner: bool,
    alignment_mode: AlignmentMode = "union",
    fuzzy_threshold: int = 50,
) -> DataFrame:
    """Apply entity alignment to detection results.
    
    Args:
        df: DataFrame with detection results
        doc_id_column: Name of document ID column
        use_presidio: Whether Presidio was used
        use_ai_query: Whether AI Query was used
        use_gliner: Whether GLiNER was used
        alignment_mode: "union" includes any entity found by at least one
            detector (recall-optimized). "consensus" requires agreement from
            at least ceil(active_detectors / 2) sources.
        
    Returns:
        DataFrame with aligned_entities column added
    """
    empty_entity_array = array().cast(
        ArrayType(
            StructType(
                [
                    StructField("entity", StringType()),
                    StructField("entity_type", StringType()),
                    StructField("start", IntegerType()),
                    StructField("end", IntegerType()),
                    StructField("score", DoubleType()),
                    StructField("doc_id", StringType()),
                ]
            )
        )
    )

    if "ai_results_struct" not in df.columns:
        df = df.withColumn("ai_results_struct", empty_entity_array)
    if "presidio_results_struct" not in df.columns:
        df = df.withColumn("presidio_results_struct", empty_entity_array)
    if "gliner_results_struct" not in df.columns:
        df = df.withColumn("gliner_results_struct", empty_entity_array)

    active = sum([use_presidio, use_ai_query, use_gliner])
    min_sources = math.ceil(active / 2) if alignment_mode == "consensus" else 1

    align_udf = align_entities_udf(
        fuzzy_threshold=fuzzy_threshold,
        include_presidio=use_presidio,
        include_gliner=use_gliner,
        include_ai=use_ai_query,
        min_sources=min_sources,
    )

    df = df.withColumn(
        "aligned_entities",
        align_udf(
            col("ai_results_struct"),
            col("presidio_results_struct"),
            col("gliner_results_struct"),
            col(doc_id_column),
        ),
    )

    return df


def _apply_redaction(
    df: DataFrame,
    text_column: str,
    entities_column: str,
    redaction_strategy: RedactionStrategy,
) -> DataFrame:
    """Apply redaction to text column.

    Uses the audit UDF so that ``_entity_count`` and ``_detection_status``
    columns are available for downstream safety checks.

    Returns:
        DataFrame with ``{text_column}_redacted``, ``_entity_count``, and
        ``_detection_status`` columns added.
    """
    audit_udf = create_redaction_audit_udf(strategy=redaction_strategy)
    redacted_col_name = f"{text_column}_redacted"
    audit_col = "_redaction_audit"
    result = (
        df
        .withColumn(audit_col, audit_udf(col(text_column), col(entities_column)))
        .withColumn(redacted_col_name, col(f"{audit_col}.redacted_text"))
        .withColumn("_entity_count", col(f"{audit_col}.entity_count"))
        .drop(audit_col)
    )
    return _add_detection_status(result, entities_column)


def _add_detection_status(df: DataFrame, entities_column: str) -> DataFrame:
    """Derive ``_detection_status`` from entity count and optional error flags."""
    status = when(col("_entity_count") > 0, lit("ok")).otherwise(lit("no_entities"))
    if "_ai_detection_failed" in df.columns:
        status = when(
            col("_ai_detection_failed") & (col("_entity_count") == 0),
            lit("detection_error"),
        ).otherwise(status)
    return df.withColumn("_detection_status", status)


def _write_audit_log(
    spark: SparkSession,
    result_df: DataFrame,
    doc_id_column: str,
    entities_column: str,
    audit_table: str,
    run_id: str,
    config_snapshot: str,
    detectors_used: str,
) -> None:
    """Write entity-level audit stats (no raw PII) to the audit log table."""
    try:
        if entities_column not in result_df.columns:
            logger.warning("Audit log: entities column '%s' not found, skipping.", entities_column)
            return
        status_col = "_detection_status" if "_detection_status" in result_df.columns else None

        select_cols = [col(doc_id_column).alias("doc_id")]
        if status_col:
            select_cols.append(col(status_col).alias("detection_status"))
        select_cols.append(explode(col(entities_column)).alias("ent"))
        exploded = result_df.select(*select_cols)

        audit_df = (
            exploded
            .groupBy("doc_id", exploded["ent.entity_type"].alias("entity_type"))
            .agg(spark_count("*").alias("entity_count"))
        )

        if status_col:
            status_lookup = (
                result_df.select(col(doc_id_column).alias("doc_id"), col(status_col).alias("detection_status"))
                .distinct()
            )
            audit_df = audit_df.join(status_lookup, "doc_id", "left")
        else:
            audit_df = audit_df.withColumn("detection_status", lit("unknown"))

        audit_df = (
            audit_df
            .withColumn("run_id", lit(run_id))
            .withColumn("detectors_used", lit(detectors_used))
            .withColumn("config_snapshot", lit(config_snapshot))
            .withColumn("created_at", current_timestamp())
            .select("run_id", "doc_id", "entity_type", "entity_count",
                     "detection_status", "detectors_used", "config_snapshot", "created_at")
        )

        audit_df.write.mode("append").saveAsTable(audit_table)
        logger.info("Audit log: wrote %d rows to %s", audit_df.count(), audit_table)
    except Exception as e:
        logger.warning("Audit log write failed (non-fatal): %s", type(e).__name__)


def _select_output_columns(
    df: DataFrame,
    doc_id_column: str,
    text_column: str,
    output_strategy: OutputStrategy,
) -> DataFrame:
    """Select columns based on output strategy.

    Production mode now always includes ``_detection_status`` and
    ``_entity_count`` alongside the redacted text.
    """
    redacted_col_name = f"{text_column}_redacted"

    if output_strategy == "production":
        wanted = [doc_id_column, redacted_col_name, "_detection_status", "_entity_count"]
        return df.select(*[c for c in wanted if c in df.columns])
    else:  # validation
        return df


def _get_entities_column(df: DataFrame, use_aligned: bool) -> str:
    """Determine which entities column to use for redaction."""
    if use_aligned and "aligned_entities" in df.columns:
        return "aligned_entities"
    elif "presidio_results_struct" in df.columns:
        return "presidio_results_struct"
    elif "ai_results_struct" in df.columns:
        return "ai_results_struct"
    else:
        raise ValueError("No entity results found in detection output")


def run_detection_pipeline(
    spark: SparkSession,
    source_df: DataFrame,
    doc_id_column: str,
    text_column: str,
    use_presidio: bool = True,
    use_ai_query: bool = True,
    use_gliner: bool = False,
    endpoint: Optional[str] = None,
    score_threshold: float = 0.5,
    gliner_model: str = DEFAULT_GLINER_MODEL,
    gliner_threshold: float = DEFAULT_GLINER_THRESHOLD,
    gliner_max_words: int = None,
    num_cores: int = 10,
    align_results: bool = True,
    fail_on_presidio_error: bool = True,
    reasoning_effort: str = DEFAULT_AI_REASONING_EFFORT,
    presidio_model_size: str = None,
    presidio_pattern_only: bool = False,
    ai_model_type: str = "foundation",
    alignment_mode: AlignmentMode = "union",
    fuzzy_threshold: int = 50,
    entity_filter: Optional[EntityFilter] = None,
    row_count: Optional[int] = None,
    config: Optional[RedactionConfig] = None,
) -> DataFrame:
    """Run complete detection pipeline with optional alignment.

    Returns:
        DataFrame with detection results and optional aligned entities
    """
    if config is not None:
        _v = _apply_config(config, {
            "use_presidio": use_presidio, "use_ai_query": use_ai_query,
            "use_gliner": use_gliner, "endpoint": endpoint,
            "score_threshold": score_threshold, "gliner_model": gliner_model,
            "gliner_threshold": gliner_threshold, "gliner_max_words": gliner_max_words,
            "num_cores": num_cores, "fail_on_presidio_error": fail_on_presidio_error,
            "reasoning_effort": reasoning_effort, "presidio_model_size": presidio_model_size,
            "presidio_pattern_only": presidio_pattern_only, "ai_model_type": ai_model_type,
            "alignment_mode": alignment_mode, "fuzzy_threshold": fuzzy_threshold,
            "entity_filter": entity_filter,
        })
        (use_presidio, use_ai_query, use_gliner, endpoint, score_threshold,
         gliner_model, gliner_threshold, gliner_max_words, num_cores,
         fail_on_presidio_error, reasoning_effort, presidio_model_size,
         presidio_pattern_only, ai_model_type, alignment_mode, fuzzy_threshold,
         entity_filter) = (
            _v["use_presidio"], _v["use_ai_query"], _v["use_gliner"],
            _v["endpoint"], _v["score_threshold"], _v["gliner_model"],
            _v["gliner_threshold"], _v["gliner_max_words"], _v["num_cores"],
            _v["fail_on_presidio_error"], _v["reasoning_effort"],
            _v["presidio_model_size"], _v["presidio_pattern_only"],
            _v["ai_model_type"], _v["alignment_mode"], _v["fuzzy_threshold"],
            _v["entity_filter"],
        )

    if not any([use_presidio, use_ai_query, use_gliner]):
        raise ValueError("At least one detection method must be enabled.")

    t0 = time.time()
    logger.info("1. Run detection pipeline.")
    result_df = run_detection(
        spark=spark,
        df=source_df,
        doc_id_column=doc_id_column,
        text_column=text_column,
        use_presidio=use_presidio,
        use_ai_query=use_ai_query,
        use_gliner=use_gliner,
        endpoint=endpoint,
        score_threshold=score_threshold,
        gliner_model=gliner_model,
        gliner_threshold=gliner_threshold,
        gliner_max_words=gliner_max_words,
        num_cores=num_cores,
        fail_on_presidio_error=fail_on_presidio_error,
        reasoning_effort=reasoning_effort,
        presidio_model_size=presidio_model_size,
        presidio_pattern_only=presidio_pattern_only,
        ai_model_type=ai_model_type,
        row_count=row_count,
    )
    t1 = time.time()
    logger.info("Detection total [%.1fs]", t1 - t0)

    methods_used = sum([use_presidio, use_ai_query, use_gliner])

    if align_results and methods_used >= 1:
        logger.info("2. Aligning entity results (mode=%s)...", alignment_mode)
        result_df = _apply_alignment(
            result_df, doc_id_column, use_presidio, use_ai_query, use_gliner,
            alignment_mode=alignment_mode,
            fuzzy_threshold=fuzzy_threshold,
        )
        result_df = result_df.cache()
        result_df.count()
        t2 = time.time()
        logger.info("Alignment materialized [%.1fs]", t2 - t1)

    if entity_filter is not None:
        from pyspark.sql.functions import pandas_udf as _pandas_udf
        from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType, DoubleType

        entity_struct = ArrayType(StructType([
            StructField("entity", StringType()),
            StructField("entity_type", StringType()),
            StructField("start", IntegerType()),
            StructField("end", IntegerType()),
            StructField("score", DoubleType()),
            StructField("source", StringType()),
        ]))
        ef = entity_filter
        has_block = bool(ef._block_set or ef._block_re)
        has_safe = bool(ef._safe_set or ef._safe_re)

        @_pandas_udf(entity_struct)
        def _apply_entity_filter(entities_col: pd.Series, text_col: pd.Series) -> pd.Series:
            out = []
            for entities, text in zip(entities_col, text_col):
                if entities is not None and len(entities) > 0:
                    ents = [e.asDict() if hasattr(e, 'asDict') else dict(e) for e in entities]
                else:
                    ents = []
                if has_safe:
                    ents = apply_safe_filter(ents, ef)
                if has_block and text:
                    ents.extend(apply_block_filter(text, ef))
                out.append(ents)
            return pd.Series(out)

        ent_col = "aligned_entities" if "aligned_entities" in result_df.columns else _get_entities_column(result_df, True)
        result_df = result_df.withColumn(ent_col, _apply_entity_filter(col(ent_col), col(text_column)))
        logger.info("Entity filter applied (deny + allow)")

    return result_df


def run_redaction_pipeline(
    spark: SparkSession,
    source_table: str,
    text_column: str,
    output_table: Optional[str] = None,
    doc_id_column: str = "doc_id",
    use_presidio: bool = True,
    use_ai_query: bool = True,
    use_gliner: bool = False,
    redaction_strategy: RedactionStrategy = "generic",
    endpoint: Optional[str] = None,
    score_threshold: float = 0.5,
    gliner_model: str = DEFAULT_GLINER_MODEL,
    gliner_threshold: float = DEFAULT_GLINER_THRESHOLD,
    gliner_max_words: int = None,
    num_cores: int = 10,
    use_aligned: bool = True,
    fail_on_presidio_error: bool = True,
    output_strategy: OutputStrategy = "production",
    max_rows: Optional[int] = 10000,
    reasoning_effort: str = DEFAULT_AI_REASONING_EFFORT,
    presidio_model_size: str = None,
    presidio_pattern_only: bool = False,
    ai_model_type: str = "foundation",
    alignment_mode: AlignmentMode = "union",
    fuzzy_threshold: int = 50,
    entity_filter: Optional[EntityFilter] = None,
    output_mode: OutputMode = "separate",
    confirm_destructive: bool = False,
    allow_consensus_redaction: bool = False,
    confirm_validation_output: bool = False,
    audit_table: Optional[str] = None,
    config: Optional[RedactionConfig] = None,
) -> DataFrame:
    """
    Run end-to-end detection and redaction pipeline.

    Args:
        spark: Active SparkSession
        source_table: Fully qualified source table name
        text_column: Name of text column to redact
        output_table: Fully qualified output table name (required for ``output_mode="separate"``)
        doc_id_column: Name of document ID column
        use_presidio: Whether to run Presidio detection
        use_ai_query: Whether to run AI Query detection
        use_gliner: Whether to run GLiNER detection
        redaction_strategy: Redaction strategy ('generic' or 'typed')
        endpoint: Databricks endpoint for AI detection
        score_threshold: Minimum confidence score for Presidio
        gliner_model: HuggingFace model for GLiNER
        num_cores: Number of cores for repartitioning
        use_aligned: If True, use aligned entities for redaction
        fail_on_presidio_error: If False, continue without Presidio if models unavailable
        output_strategy: 'validation' (all columns incl. raw PII) or 'production' (safe output)
        max_rows: Maximum rows to process after dedup. Set to None or 0 to process all rows. Default: 10000
        presidio_model_size: spaCy model size for Presidio ('sm', 'md', 'lg')
        presidio_pattern_only: If True, use only pattern recognizers (no spaCy).
        ai_model_type: "foundation" or "external" (for Claude, etc.)
        fuzzy_threshold: Fuzzy matching threshold for alignment (0-100, default 50)
        entity_filter: Optional EntityFilter for deny/allow list processing
        output_mode: ``"separate"`` writes to ``output_table``; ``"in_place"``
            destructively overwrites the text column in ``source_table``.
        confirm_destructive: Must be ``True`` when ``output_mode="in_place"``.
        allow_consensus_redaction: Must be ``True`` to use ``alignment_mode="consensus"``
            for redaction.  Consensus mode reduces recall and may leave PII unredacted.
        confirm_validation_output: Must be ``True`` when ``output_strategy="validation"``
            since validation mode persists raw PII (original text, entity values) to the
            output table.

    Note:
        For tables exceeding 100k rows, consider ``run_redaction_pipeline_streaming``
        for incremental processing with lower memory pressure and no row-count limits.

    Returns:
        DataFrame with redacted text
    """
    if config is not None:
        _v = _apply_config(config, {
            "use_presidio": use_presidio, "use_ai_query": use_ai_query,
            "use_gliner": use_gliner, "endpoint": endpoint,
            "score_threshold": score_threshold, "gliner_model": gliner_model,
            "gliner_threshold": gliner_threshold, "gliner_max_words": gliner_max_words,
            "num_cores": num_cores, "fail_on_presidio_error": fail_on_presidio_error,
            "reasoning_effort": reasoning_effort, "presidio_model_size": presidio_model_size,
            "presidio_pattern_only": presidio_pattern_only, "ai_model_type": ai_model_type,
            "alignment_mode": alignment_mode, "fuzzy_threshold": fuzzy_threshold,
            "allow_consensus_redaction": allow_consensus_redaction,
            "redaction_strategy": redaction_strategy, "output_strategy": output_strategy,
            "output_mode": output_mode, "confirm_destructive": confirm_destructive,
            "confirm_validation_output": confirm_validation_output,
            "max_rows": max_rows, "entity_filter": entity_filter,
        })
        (use_presidio, use_ai_query, use_gliner, endpoint, score_threshold,
         gliner_model, gliner_threshold, gliner_max_words, num_cores,
         fail_on_presidio_error, reasoning_effort, presidio_model_size,
         presidio_pattern_only, ai_model_type, alignment_mode, fuzzy_threshold,
         allow_consensus_redaction, redaction_strategy, output_strategy,
         output_mode, confirm_destructive, confirm_validation_output,
         max_rows, entity_filter) = (
            _v["use_presidio"], _v["use_ai_query"], _v["use_gliner"],
            _v["endpoint"], _v["score_threshold"], _v["gliner_model"],
            _v["gliner_threshold"], _v["gliner_max_words"], _v["num_cores"],
            _v["fail_on_presidio_error"], _v["reasoning_effort"],
            _v["presidio_model_size"], _v["presidio_pattern_only"],
            _v["ai_model_type"], _v["alignment_mode"], _v["fuzzy_threshold"],
            _v["allow_consensus_redaction"], _v["redaction_strategy"],
            _v["output_strategy"], _v["output_mode"], _v["confirm_destructive"],
            _v["confirm_validation_output"],
            _v["max_rows"], _v["entity_filter"],
        )

    if not any([use_presidio, use_ai_query, use_gliner]):
        raise ValueError("At least one detection method must be enabled.")

    _check_consensus_safety(alignment_mode, allow_consensus_redaction)
    _check_validation_output(output_strategy, confirm_validation_output)

    from .metadata import _validate_identifier, _parse_table_name

    if output_mode == "in_place":
        if not confirm_destructive:
            raise ValueError(
                "In-place redaction is destructive and will permanently overwrite "
                f"the '{text_column}' column in {source_table}. "
                "Pass confirm_destructive=True to proceed."
            )
        _parse_table_name(source_table)
    else:
        if not output_table:
            raise ValueError("output_table is required when output_mode='separate'.")
        _parse_table_name(output_table)

    _validate_identifier(doc_id_column, "doc_id_column")

    t_pipeline_start = time.time()

    # DO NOT remove .distinct() -- prevents redundant processing of duplicate rows
    # (e.g. ground-truth tables with many repeated rows).
    source_df = spark.table(source_table).select(doc_id_column, text_column).distinct()

    if max_rows:
        row_count = source_df.count()
        if row_count > max_rows:
            logger.warning("Source has %s rows (after dedup). Limiting to %s (ordered by %s).", f"{row_count:,}", f"{max_rows:,}", doc_id_column)
            source_df = source_df.orderBy(doc_id_column).limit(max_rows)
            row_count = max_rows
        if row_count > 100_000:
            logger.info("TIP: For %s+ rows, consider run_redaction_pipeline_streaming for incremental processing.", f"{row_count:,}")
    else:
        row_count = None

    # Cache and materialize once so downstream detectors never re-scan the source
    source_df = source_df.cache()
    source_df.count()
    if row_count is None:
        row_count = source_df.count()
    t_cache = time.time()
    logger.info("Cached %s rows for detection. [%.1fs]", f"{row_count:,}", t_cache - t_pipeline_start)

    detection_df = run_detection_pipeline(
        spark=spark,
        source_df=source_df,
        doc_id_column=doc_id_column,
        text_column=text_column,
        use_presidio=use_presidio,
        use_ai_query=use_ai_query,
        use_gliner=use_gliner,
        endpoint=endpoint,
        score_threshold=score_threshold,
        gliner_model=gliner_model,
        gliner_threshold=gliner_threshold,
        gliner_max_words=gliner_max_words,
        num_cores=num_cores,
        align_results=True,
        fail_on_presidio_error=fail_on_presidio_error,
        reasoning_effort=reasoning_effort,
        presidio_model_size=presidio_model_size,
        presidio_pattern_only=presidio_pattern_only,
        ai_model_type=ai_model_type,
        alignment_mode=alignment_mode,
        fuzzy_threshold=fuzzy_threshold,
        entity_filter=entity_filter,
        row_count=row_count,
    )

    detection_df = detection_df.cache()
    detection_df.count()
    t_detect_done = time.time()
    logger.info("Detection pipeline complete [%.1fs]", t_detect_done - t_cache)

    entities_column = _get_entities_column(detection_df, use_aligned)

    t_redact_start = time.time()
    logger.info("3. Applying redaction...")
    result_df = _apply_redaction(
        detection_df, text_column, entities_column, redaction_strategy
    )

    if output_mode == "in_place":
        logger.info("4. Updating %s.%s in-place...", source_table, text_column)
        _write_in_place(spark, result_df, source_table, doc_id_column, text_column)
    else:
        output_df = _select_output_columns(result_df, doc_id_column, text_column, output_strategy)
        logger.info("4. Writing to %s...", output_table)
        output_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_table)

    if audit_table:
        detectors = []
        if use_presidio:
            detectors.append("presidio")
        if use_ai_query:
            detectors.append("ai_query")
        if use_gliner:
            detectors.append("gliner")
        config_snap = json.dumps({
            "score_threshold": score_threshold, "gliner_threshold": gliner_threshold,
            "alignment_mode": alignment_mode, "redaction_strategy": redaction_strategy,
            "output_strategy": output_strategy, "output_mode": output_mode,
        })
        _write_audit_log(
            spark, result_df, doc_id_column, entities_column, audit_table,
            run_id=str(uuid.uuid4()), config_snapshot=config_snap,
            detectors_used=",".join(detectors),
        )

    t_write_done = time.time()
    logger.info("Redaction + write: %.1fs | Total pipeline: %.1fs", t_write_done - t_redact_start, t_write_done - t_pipeline_start)

    return result_df


def _ensure_checkpoint_volume_exists(spark: SparkSession, checkpoint_path: str) -> None:
    """Ensure the checkpoint volume exists, creating it if necessary.
    
    Args:
        spark: Active SparkSession
        checkpoint_path: Path like /Volumes/catalog/schema/volume_name/...
        
    Raises:
        Exception: If volume cannot be created due to permissions
    """
    import re
    
    # Parse /Volumes/catalog/schema/volume_name pattern
    match = re.match(r'^/Volumes/([^/]+)/([^/]+)/([^/]+)', checkpoint_path)
    if not match:
        import warnings
        warnings.warn(
            f"Checkpoint path '{checkpoint_path}' does not follow /Volumes/catalog/schema/volume_name/... format. "
            f"Ensure the path is a valid Unity Catalog Volume for production use. "
            f"Non-Volume paths (DBFS, local) may not persist across cluster restarts.",
            stacklevel=2,
        )
        return
    
    catalog, schema, volume_name = match.groups()
    from .metadata import _validate_identifier
    _validate_identifier(catalog, "catalog")
    _validate_identifier(schema, "schema")
    _validate_identifier(volume_name, "volume_name")
    volume_fqn = f"{catalog}.{schema}.{volume_name}"
    
    try:
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {volume_fqn}")
        logger.info(f"  Checkpoint volume ensured: {volume_fqn}")
    except Exception as e:
        error_msg = str(e)
        if "PERMISSION" in error_msg.upper() or "ACCESS" in error_msg.upper():
            raise Exception(
                f"Cannot create checkpoint volume {volume_fqn}: insufficient permissions. "
                f"Create it manually with: CREATE VOLUME {volume_fqn}"
            ) from e
        # Re-raise other errors
        raise


def run_redaction_pipeline_streaming(
    spark: SparkSession,
    source_table: str,
    text_column: str,
    output_table: Optional[str] = None,
    checkpoint_path: str = "",
    doc_id_column: str = "doc_id",
    use_presidio: bool = True,
    use_ai_query: bool = True,
    use_gliner: bool = False,
    redaction_strategy: RedactionStrategy = "generic",
    endpoint: Optional[str] = None,
    score_threshold: float = 0.5,
    gliner_model: str = DEFAULT_GLINER_MODEL,
    gliner_threshold: float = DEFAULT_GLINER_THRESHOLD,
    gliner_max_words: int = None,
    num_cores: int = 10,
    use_aligned: bool = True,
    fail_on_presidio_error: bool = True,
    output_strategy: OutputStrategy = "production",
    reasoning_effort: str = DEFAULT_AI_REASONING_EFFORT,
    presidio_model_size: str = None,
    presidio_pattern_only: bool = False,
    ai_model_type: str = "foundation",
    alignment_mode: AlignmentMode = "union",
    max_files_per_trigger: Optional[int] = 10,
    entity_filter=None,
    output_mode: OutputMode = "separate",
    confirm_destructive: bool = False,
    allow_consensus_redaction: bool = False,
    confirm_validation_output: bool = False,
    config: Optional[RedactionConfig] = None,
) -> StreamingQuery:
    """
    Run streaming redaction pipeline with incremental processing.

    Uses native streaming DataFrame operations for incremental updates.
    Output is written via ``foreachBatch`` + ``MERGE INTO`` on ``doc_id_column``
    so re-processed or retried documents replace their earlier result instead of
    creating duplicates.

    When ``output_mode="in_place"``, each micro-batch MERGEs back into the
    *source* table, overwriting the text column with redacted text.  This is
    destructive -- ``confirm_destructive=True`` is required.

    **Streaming caveats:**

    * The checkpoint is tightly coupled to the query plan.  Changing which
      detectors are enabled, or modifying the detection logic, requires deleting
      the checkpoint directory before restarting -- otherwise the stream may
      fail or silently skip data.
    * ``mergeSchema`` is enabled.  Adding new columns (e.g. switching from
      ``production`` to ``validation`` output_strategy) will widen the output
      table without a manual ALTER TABLE.
    * LLM-based detection (AI Query) is non-deterministic.  If a micro-batch is
      retried after a transient failure the redacted output for the same
      document may differ slightly from the first attempt.
    * When AI Query returns an error for a row, the row is flagged with
      ``_ai_detection_failed = True``.  A warning is logged per batch with the
      count of such failures.

    Args:
        spark: Active SparkSession
        source_table: Fully qualified source table name (must be Delta)
        text_column: Name of text column to redact
        output_table: Fully qualified output table name (required for ``output_mode="separate"``)
        checkpoint_path: Path for streaming checkpoints (e.g., /Volumes/catalog/schema/checkpoints/table)
        doc_id_column: Name of document ID column
        output_strategy: 'validation' (all columns incl. raw PII) or 'production' (safe output)
        confirm_destructive: Must be ``True`` when ``output_mode="in_place"``.
        allow_consensus_redaction: Must be ``True`` to use ``alignment_mode="consensus"``.
        confirm_validation_output: Must be ``True`` when ``output_strategy="validation"``.

    Returns:
        StreamingQuery (after blocking until all micro-batches complete, since
        ``trigger(availableNow=True)`` is used).
    """
    if config is not None:
        _v = _apply_config(config, {
            "use_presidio": use_presidio, "use_ai_query": use_ai_query,
            "use_gliner": use_gliner, "endpoint": endpoint,
            "score_threshold": score_threshold, "gliner_model": gliner_model,
            "gliner_threshold": gliner_threshold, "gliner_max_words": gliner_max_words,
            "num_cores": num_cores, "fail_on_presidio_error": fail_on_presidio_error,
            "reasoning_effort": reasoning_effort, "presidio_model_size": presidio_model_size,
            "presidio_pattern_only": presidio_pattern_only, "ai_model_type": ai_model_type,
            "alignment_mode": alignment_mode,
            "allow_consensus_redaction": allow_consensus_redaction,
            "redaction_strategy": redaction_strategy, "output_strategy": output_strategy,
            "output_mode": output_mode, "confirm_destructive": confirm_destructive,
            "confirm_validation_output": confirm_validation_output,
            "entity_filter": entity_filter,
        })
        (use_presidio, use_ai_query, use_gliner, endpoint, score_threshold,
         gliner_model, gliner_threshold, gliner_max_words, num_cores,
         fail_on_presidio_error, reasoning_effort, presidio_model_size,
         presidio_pattern_only, ai_model_type, alignment_mode,
         allow_consensus_redaction, redaction_strategy, output_strategy,
         output_mode, confirm_destructive, confirm_validation_output,
         entity_filter) = (
            _v["use_presidio"], _v["use_ai_query"], _v["use_gliner"],
            _v["endpoint"], _v["score_threshold"], _v["gliner_model"],
            _v["gliner_threshold"], _v["gliner_max_words"], _v["num_cores"],
            _v["fail_on_presidio_error"], _v["reasoning_effort"],
            _v["presidio_model_size"], _v["presidio_pattern_only"],
            _v["ai_model_type"], _v["alignment_mode"],
            _v["allow_consensus_redaction"], _v["redaction_strategy"],
            _v["output_strategy"], _v["output_mode"], _v["confirm_destructive"],
            _v["confirm_validation_output"],
            _v["entity_filter"],
        )

    from .metadata import _validate_identifier, _parse_table_name

    _check_consensus_safety(alignment_mode, allow_consensus_redaction)
    _check_validation_output(output_strategy, confirm_validation_output)

    if output_mode == "in_place":
        if not confirm_destructive:
            raise ValueError(
                "In-place redaction is destructive and will permanently overwrite "
                f"the '{text_column}' column in {source_table}. "
                "Pass confirm_destructive=True to proceed."
            )
        _parse_table_name(source_table)
    else:
        if not output_table:
            raise ValueError("output_table is required when output_mode='separate'.")
        _parse_table_name(output_table)

    _validate_identifier(doc_id_column, "doc_id_column")

    _is_in_place = output_mode == "in_place"
    _target_table = source_table if _is_in_place else output_table

    logger.info("Starting streaming redaction pipeline")
    logger.info(f"  Source: {source_table}")
    logger.info(f"  Output: {_target_table} {'(IN-PLACE)' if _is_in_place else ''}")
    logger.info(f"  Checkpoint: {checkpoint_path}")
    logger.info(f"  Output strategy: {output_strategy}")
    
    # Ensure checkpoint volume exists
    _ensure_checkpoint_volume_exists(spark, checkpoint_path)

    # Pre-check Presidio availability if enabled (skip check for pattern-only mode)
    if use_presidio and not presidio_pattern_only:
        from .detection import check_presidio_available
        is_available, error_msg = check_presidio_available()
        if not is_available:
            if fail_on_presidio_error:
                raise Exception(f"Presidio unavailable: {error_msg}")
            logger.warning(f"Presidio detection skipped - {error_msg}")
            use_presidio = False

    # Read source as stream -- repartition once here, detectors skip their own
    reader = spark.readStream
    if max_files_per_trigger is not None:
        reader = reader.option("maxFilesPerTrigger", max_files_per_trigger)
    stream_df = reader.table(source_table).repartition(num_cores)

    if use_presidio:
        from .presidio import make_presidio_batch_udf
        presidio_udf = make_presidio_batch_udf(
            score_threshold=score_threshold, model_size=presidio_model_size,
            pattern_only=presidio_pattern_only,
        )
        stream_df = stream_df.withColumn(
            "presidio_results_struct", presidio_udf(col(doc_id_column), col(text_column))
        )

    if use_ai_query:
        from .detection import run_ai_query_detection
        from pyspark.sql.functions import lit, when
        stream_df = run_ai_query_detection(
            spark=spark,
            df=stream_df,
            doc_id_column=doc_id_column,
            text_column=text_column,
            endpoint=endpoint or "databricks-gpt-oss-120b",
            num_cores=num_cores,
            reasoning_effort=reasoning_effort,
            ai_model_type=ai_model_type,
            _repartition=False,
        )
        # Flag rows where AI endpoint returned an error instead of results.
        # The 'response' column has .result and .errorMessage from ai_query(failOnError=>false).
        if "response" in stream_df.columns:
            stream_df = stream_df.withColumn(
                "_ai_detection_failed",
                when(col("response.errorMessage").isNotNull(), lit(True)).otherwise(lit(False)),
            )

    if use_gliner:
        from .gliner_detector import run_gliner_detection
        gliner_kwargs = dict(
            df=stream_df,
            doc_id_column=doc_id_column,
            text_column=text_column,
            model_name=gliner_model,
            num_cores=num_cores,
            threshold=gliner_threshold,
            _repartition=False,
        )
        if gliner_max_words is not None:
            gliner_kwargs["max_words"] = gliner_max_words
        stream_df = run_gliner_detection(**gliner_kwargs)

    # Apply alignment
    methods_used = sum([use_presidio, use_ai_query, use_gliner])
    if methods_used >= 1:
        stream_df = _apply_alignment(
            stream_df, doc_id_column, use_presidio, use_ai_query, use_gliner,
            alignment_mode=alignment_mode,
        )

    # Determine entities column
    if use_aligned and methods_used >= 1:
        entities_column = "aligned_entities"
    elif use_presidio:
        entities_column = "presidio_results_struct"
    elif use_ai_query:
        entities_column = "ai_results_struct"
    elif use_gliner:
        entities_column = "gliner_results_struct"
    else:
        raise ValueError("At least one detection method must be enabled")

    # Apply entity filter (block/safe lists) if provided
    if entity_filter is not None:
        from .entity_filter import apply_safe_filter, apply_block_filter
        from pyspark.sql.functions import pandas_udf as _pandas_udf
        from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType, DoubleType

        _ef_struct = ArrayType(StructType([
            StructField("entity", StringType()),
            StructField("entity_type", StringType()),
            StructField("start", IntegerType()),
            StructField("end", IntegerType()),
            StructField("score", DoubleType()),
            StructField("source", StringType()),
        ]))
        ef = entity_filter
        _has_block = bool(ef._block_set or ef._block_re)
        _has_safe = bool(ef._safe_set or ef._safe_re)

        @_pandas_udf(_ef_struct)
        def _stream_entity_filter(entities_col: pd.Series, text_col: pd.Series) -> pd.Series:
            out = []
            for entities, text in zip(entities_col, text_col):
                if entities is not None and len(entities) > 0:
                    ents = [e.asDict() if hasattr(e, 'asDict') else dict(e) for e in entities]
                else:
                    ents = []
                if _has_safe:
                    ents = apply_safe_filter(ents, ef)
                if _has_block and text:
                    ents.extend(apply_block_filter(text, ef))
                out.append(ents)
            return pd.Series(out)

        stream_df = stream_df.withColumn(
            entities_column, _stream_entity_filter(col(entities_column), col(text_column))
        )
        logger.info("Entity filter applied in streaming pipeline")

    # Apply redaction
    stream_df = _apply_redaction(stream_df, text_column, entities_column, redaction_strategy)

    # Select output columns (skip for in-place -- we only need doc_id + redacted col)
    if _is_in_place:
        _redacted_col = f"{text_column}_redacted"
        stream_df = stream_df.select(col(doc_id_column), col(_redacted_col))
    else:
        stream_df = _select_output_columns(stream_df, doc_id_column, text_column, output_strategy)

    # Ensure output table exists for MERGE INTO (create empty if needed)
    if not _is_in_place and not spark.catalog.tableExists(output_table):
        stream_df.limit(0).write.format("delta").option("mergeSchema", "true").saveAsTable(output_table)

    # Use foreachBatch with MERGE INTO to handle deduplication and updates.
    _doc_id_col = doc_id_column
    _merge_target = _target_table
    _text_col = text_column
    _batch_stats: list = []
    _stream_logger = logging.getLogger("dbxredact.streaming")

    def _write_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return

        total = batch_df.count()
        no_entities = 0
        errors = 0
        if "_detection_status" in batch_df.columns:
            status_counts = {
                r["_detection_status"]: r["count"]
                for r in batch_df.groupBy("_detection_status").count().collect()
            }
            no_entities = status_counts.get("no_entities", 0)
            errors = status_counts.get("detection_error", 0)
        _batch_stats.append({
            "batch_id": batch_id, "total": total,
            "no_entities": no_entities, "errors": errors,
        })
        if no_entities + errors > 0:
            _stream_logger.warning(
                "Batch %d: %d/%d docs with no entities, %d detection errors",
                batch_id, no_entities, total, errors,
            )

        view_name = f"_dbxredact_batch_{batch_id}"
        batch_df.createOrReplaceTempView(view_name)
        if _is_in_place:
            _rcol = f"{_text_col}_redacted"
            batch_df.sparkSession.sql(f"""
                MERGE INTO {_merge_target} t
                USING {view_name} s
                ON t.`{_doc_id_col}` = s.`{_doc_id_col}`
                WHEN MATCHED THEN UPDATE SET t.`{_text_col}` = s.`{_rcol}`
            """)
        else:
            batch_df.sparkSession.sql(f"""
                MERGE INTO {_merge_target} t
                USING {view_name} s
                ON t.`{_doc_id_col}` = s.`{_doc_id_col}`
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)

    def _log_streaming_summary():
        if not _batch_stats:
            return
        total_docs = sum(s["total"] for s in _batch_stats)
        total_no_ent = sum(s["no_entities"] for s in _batch_stats)
        total_errs = sum(s["errors"] for s in _batch_stats)
        _stream_logger.info(
            "Streaming complete: %d batches, %d docs, %d no-entity, %d errors",
            len(_batch_stats), total_docs, total_no_ent, total_errs,
        )

    query = (
        stream_df
        .writeStream
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .foreachBatch(_write_batch)
        .start()
    )

    query.awaitTermination()
    _log_streaming_summary()

    return query


def run_redaction_pipeline_by_tag(
    spark: SparkSession,
    source_table: str,
    output_table: Optional[str] = None,
    tag_name: str = "data_classification",
    tag_value: str = "protected",
    doc_id_column: str = "doc_id",
    use_presidio: bool = True,
    use_ai_query: bool = True,
    use_gliner: bool = False,
    redaction_strategy: RedactionStrategy = "generic",
    endpoint: Optional[str] = None,
    score_threshold: float = 0.5,
    gliner_model: str = DEFAULT_GLINER_MODEL,
    gliner_threshold: float = DEFAULT_GLINER_THRESHOLD,
    gliner_max_words: int = None,
    num_cores: int = 10,
    output_strategy: OutputStrategy = "production",
    max_rows: Optional[int] = 10000,
    reasoning_effort: str = DEFAULT_AI_REASONING_EFFORT,
    presidio_model_size: str = None,
    ai_model_type: str = "foundation",
    alignment_mode: AlignmentMode = "union",
    fuzzy_threshold: int = 50,
    presidio_pattern_only: bool = False,
    output_mode: OutputMode = "separate",
    confirm_destructive: bool = False,
    allow_consensus_redaction: bool = False,
    confirm_validation_output: bool = False,
    entity_filter: Optional[EntityFilter] = None,
    config: Optional[RedactionConfig] = None,
) -> DataFrame:
    """
    Run redaction pipeline on columns identified by Unity Catalog tags.

    Returns:
        DataFrame with redacted columns
    """
    if config is not None:
        _v = _apply_config(config, {
            "use_presidio": use_presidio, "use_ai_query": use_ai_query,
            "use_gliner": use_gliner, "endpoint": endpoint,
            "score_threshold": score_threshold, "gliner_model": gliner_model,
            "gliner_threshold": gliner_threshold, "gliner_max_words": gliner_max_words,
            "num_cores": num_cores, "reasoning_effort": reasoning_effort,
            "presidio_model_size": presidio_model_size, "ai_model_type": ai_model_type,
            "alignment_mode": alignment_mode, "fuzzy_threshold": fuzzy_threshold,
            "presidio_pattern_only": presidio_pattern_only,
            "redaction_strategy": redaction_strategy, "output_strategy": output_strategy,
            "output_mode": output_mode, "confirm_destructive": confirm_destructive,
            "max_rows": max_rows, "allow_consensus_redaction": allow_consensus_redaction,
            "confirm_validation_output": confirm_validation_output,
            "entity_filter": entity_filter,
        })
        (use_presidio, use_ai_query, use_gliner, endpoint, score_threshold,
         gliner_model, gliner_threshold, gliner_max_words, num_cores,
         reasoning_effort, presidio_model_size, ai_model_type,
         alignment_mode, fuzzy_threshold, presidio_pattern_only,
         redaction_strategy, output_strategy, output_mode,
         confirm_destructive, max_rows, allow_consensus_redaction,
         confirm_validation_output, entity_filter) = (
            _v["use_presidio"], _v["use_ai_query"], _v["use_gliner"],
            _v["endpoint"], _v["score_threshold"], _v["gliner_model"],
            _v["gliner_threshold"], _v["gliner_max_words"], _v["num_cores"],
            _v["reasoning_effort"], _v["presidio_model_size"],
            _v["ai_model_type"], _v["alignment_mode"], _v["fuzzy_threshold"],
            _v["presidio_pattern_only"], _v["redaction_strategy"],
            _v["output_strategy"], _v["output_mode"], _v["confirm_destructive"],
            _v["max_rows"], _v["allow_consensus_redaction"],
            _v["confirm_validation_output"], _v["entity_filter"],
        )

    protected_columns = get_columns_by_tag(
        spark=spark, table_name=source_table, tag_name=tag_name, tag_value=tag_value
    )

    if not protected_columns:
        raise ValueError(
            f"No columns found with {tag_name}={tag_value} in {source_table}"
        )

    logger.info(f"Found {len(protected_columns)} protected column(s): {protected_columns}")

    result_df = None
    for text_column in protected_columns:
        if output_mode == "in_place":
            col_output_table = None
            logger.info(f"Processing column: {text_column} (in-place)")
        else:
            col_output_table = f"{output_table}_{text_column}" if len(protected_columns) > 1 else output_table
            logger.info(f"Processing column: {text_column} -> {col_output_table}")

        result_df = run_redaction_pipeline(
            spark=spark,
            source_table=source_table,
            text_column=text_column,
            output_table=col_output_table,
            doc_id_column=doc_id_column,
            use_presidio=use_presidio,
            use_ai_query=use_ai_query,
            use_gliner=use_gliner,
            redaction_strategy=redaction_strategy,
            endpoint=endpoint,
            score_threshold=score_threshold,
            gliner_model=gliner_model,
            gliner_threshold=gliner_threshold,
            gliner_max_words=gliner_max_words,
            num_cores=num_cores,
            output_strategy=output_strategy,
            max_rows=max_rows,
            reasoning_effort=reasoning_effort,
            presidio_model_size=presidio_model_size,
            ai_model_type=ai_model_type,
            alignment_mode=alignment_mode,
            fuzzy_threshold=fuzzy_threshold,
            presidio_pattern_only=presidio_pattern_only,
            output_mode=output_mode,
            confirm_destructive=confirm_destructive,
            allow_consensus_redaction=allow_consensus_redaction,
            confirm_validation_output=confirm_validation_output,
            entity_filter=entity_filter,
        )

    return result_df
