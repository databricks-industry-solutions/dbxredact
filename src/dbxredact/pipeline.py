"""End-to-end PHI/PII detection and redaction pipelines."""

import logging
import math
from typing import Optional, Literal
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.functions import col, array
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
from .redaction import create_redaction_udf, RedactionStrategy
from .metadata import get_columns_by_tag
from .config import DEFAULT_GLINER_MODEL, DEFAULT_GLINER_THRESHOLD, DEFAULT_AI_REASONING_EFFORT
from .entity_filter import EntityFilter, apply_deny_filter, apply_allow_filter

logger = logging.getLogger(__name__)

OutputStrategy = Literal["validation", "production"]


AlignmentMode = Literal["union", "consensus"]


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
    
    Args:
        df: DataFrame with text and entities
        text_column: Name of text column
        entities_column: Name of entities column
        redaction_strategy: 'generic' or 'typed'
        
    Returns:
        DataFrame with redacted text column added
    """
    redact_udf = create_redaction_udf(strategy=redaction_strategy)
    redacted_col_name = f"{text_column}_redacted"
    return df.withColumn(redacted_col_name, redact_udf(col(text_column), col(entities_column)))


def _select_output_columns(
    df: DataFrame,
    doc_id_column: str,
    text_column: str,
    output_strategy: OutputStrategy,
) -> DataFrame:
    """Select columns based on output strategy.
    
    Args:
        df: DataFrame with all columns
        doc_id_column: Name of document ID column
        text_column: Name of original text column
        output_strategy: 'validation' (all columns) or 'production' (minimal)
        
    Returns:
        DataFrame with selected columns
    """
    redacted_col_name = f"{text_column}_redacted"
    
    if output_strategy == "production":
        return df.select(doc_id_column, redacted_col_name)
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
    num_cores: int = 10,
    align_results: bool = True,
    fail_on_presidio_error: bool = True,
    reasoning_effort: str = DEFAULT_AI_REASONING_EFFORT,
    presidio_model_size: str = None,
    alignment_mode: AlignmentMode = "union",
    fuzzy_threshold: int = 50,
    entity_filter: Optional[EntityFilter] = None,
) -> DataFrame:
    """
    Run complete detection pipeline with optional alignment.

    Args:
        spark: Active SparkSession
        source_df: Input DataFrame with text to analyze
        doc_id_column: Name of document ID column
        text_column: Name of text column to analyze
        use_presidio: Whether to run Presidio detection
        use_ai_query: Whether to run AI Query detection
        use_gliner: Whether to run GLiNER detection
        endpoint: Databricks endpoint for AI detection
        score_threshold: Minimum confidence score for Presidio
        gliner_model: HuggingFace model for GLiNER
        num_cores: Number of cores for repartitioning
        align_results: If True and multiple methods enabled, align results
        fail_on_presidio_error: If False, continue without Presidio if models unavailable
        reasoning_effort: AI Query reasoning effort ("low", "medium", "high")
        fuzzy_threshold: Fuzzy matching threshold for alignment (0-100, default 50)
        entity_filter: Optional EntityFilter for deny/allow list processing

    Returns:
        DataFrame with detection results and optional aligned entities
    """
    if not any([use_presidio, use_ai_query, use_gliner]):
        raise ValueError("At least one detection method must be enabled.")

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
        num_cores=num_cores,
        fail_on_presidio_error=fail_on_presidio_error,
        reasoning_effort=reasoning_effort,
        presidio_model_size=presidio_model_size,
    )

    methods_used = sum([use_presidio, use_ai_query, use_gliner])

    if align_results and methods_used >= 1:
        logger.info(f"2. Aligning entity results from multiple sources (mode={alignment_mode})...")
        result_df = _apply_alignment(
            result_df, doc_id_column, use_presidio, use_ai_query, use_gliner,
            alignment_mode=alignment_mode,
            fuzzy_threshold=fuzzy_threshold,
        )
        logger.info("Alignment complete")

    if entity_filter is not None:
        from pyspark.sql.functions import udf
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
        has_allow = bool(ef._allow_set or ef._allow_re)
        has_deny = bool(ef._deny_set or ef._deny_re)

        @udf(entity_struct)
        def _apply_entity_filter(entities, text):
            ents = [e.asDict() for e in entities] if entities else []
            if has_deny:
                ents = apply_deny_filter(ents, ef)
            if has_allow and text:
                forced = apply_allow_filter(text, ef)
                ents.extend(forced)
            return ents

        ent_col = "aligned_entities" if "aligned_entities" in result_df.columns else _get_entities_column(result_df, True)
        result_df = result_df.withColumn(ent_col, _apply_entity_filter(col(ent_col), col(text_column)))
        logger.info("Entity filter applied (deny + allow)")

    return result_df


def run_redaction_pipeline(
    spark: SparkSession,
    source_table: str,
    text_column: str,
    output_table: str,
    doc_id_column: str = "doc_id",
    use_presidio: bool = True,
    use_ai_query: bool = True,
    use_gliner: bool = False,
    redaction_strategy: RedactionStrategy = "generic",
    endpoint: Optional[str] = None,
    score_threshold: float = 0.5,
    gliner_model: str = DEFAULT_GLINER_MODEL,
    gliner_threshold: float = DEFAULT_GLINER_THRESHOLD,
    num_cores: int = 10,
    use_aligned: bool = True,
    fail_on_presidio_error: bool = True,
    output_strategy: OutputStrategy = "production",
    max_rows: Optional[int] = 10000,
    reasoning_effort: str = DEFAULT_AI_REASONING_EFFORT,
    presidio_model_size: str = None,
    alignment_mode: AlignmentMode = "union",
    fuzzy_threshold: int = 50,
    entity_filter: Optional[EntityFilter] = None,
) -> DataFrame:
    """
    Run end-to-end detection and redaction pipeline.

    Args:
        spark: Active SparkSession
        source_table: Fully qualified source table name
        text_column: Name of text column to redact
        output_table: Fully qualified output table name
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
        output_strategy: 'validation' (all columns) or 'production' (doc_id + redacted only)
        max_rows: Maximum rows to process after dedup. Set to None or 0 to process all rows. Default: 10000
        presidio_model_size: spaCy model size for Presidio ('sm', 'md', 'lg')
        fuzzy_threshold: Fuzzy matching threshold for alignment (0-100, default 50)
        entity_filter: Optional EntityFilter for deny/allow list processing

    Returns:
        DataFrame with redacted text
    """
    if not any([use_presidio, use_ai_query, use_gliner]):
        raise ValueError("At least one detection method must be enabled.")

    source_df = spark.table(source_table).select(doc_id_column, text_column).distinct()

    if max_rows:
        row_count = source_df.count()
        if row_count > max_rows:
            logger.warning(f"Source has {row_count:,} rows (after dedup). Limiting to {max_rows:,}.")
            source_df = source_df.limit(max_rows)

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
        num_cores=num_cores,
        align_results=True,
        fail_on_presidio_error=fail_on_presidio_error,
        reasoning_effort=reasoning_effort,
        presidio_model_size=presidio_model_size,
        alignment_mode=alignment_mode,
        fuzzy_threshold=fuzzy_threshold,
        entity_filter=entity_filter,
    )

    entities_column = _get_entities_column(detection_df, use_aligned)

    logger.info("3. Applying redaction...")
    result_df = _apply_redaction(
        detection_df, text_column, entities_column, redaction_strategy
    )

    # Select output columns
    output_df = _select_output_columns(result_df, doc_id_column, text_column, output_strategy)

    # Write to table
    logger.info(f"4. Writing to {output_table}...")
    output_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(output_table)

    return result_df  # Return full df for display/analysis


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
    output_table: str,
    checkpoint_path: str,
    doc_id_column: str = "doc_id",
    use_presidio: bool = True,
    use_ai_query: bool = True,
    use_gliner: bool = False,
    redaction_strategy: RedactionStrategy = "generic",
    endpoint: Optional[str] = None,
    score_threshold: float = 0.5,
    gliner_model: str = DEFAULT_GLINER_MODEL,
    gliner_threshold: float = DEFAULT_GLINER_THRESHOLD,
    num_cores: int = 10,
    use_aligned: bool = True,
    fail_on_presidio_error: bool = True,
    output_strategy: OutputStrategy = "production",
    reasoning_effort: str = DEFAULT_AI_REASONING_EFFORT,
    presidio_model_size: str = None,
    alignment_mode: AlignmentMode = "union",
    max_files_per_trigger: Optional[int] = 10,
) -> StreamingQuery:
    """
    Run streaming redaction pipeline with incremental processing.

    Uses native streaming DataFrame operations for incremental updates.
    Output is written via ``foreachBatch`` + ``MERGE INTO`` on ``doc_id_column``
    so re-processed or retried documents replace their earlier result instead of
    creating duplicates.

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
        output_table: Fully qualified output table name
        checkpoint_path: Path for streaming checkpoints (e.g., /Volumes/catalog/schema/checkpoints/table)
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
        output_strategy: 'validation' (all columns) or 'production' (doc_id + redacted only)
        presidio_model_size: spaCy model size for Presidio ('sm', 'md', 'lg')
        alignment_mode: 'union' (any detector) or 'consensus' (majority agreement)
        max_files_per_trigger: Limit files per micro-batch (None for unlimited)

    Returns:
        StreamingQuery that can be awaited or monitored
    """
    logger.info("Starting streaming redaction pipeline")
    logger.info(f"  Source: {source_table}")
    logger.info(f"  Output: {output_table}")
    logger.info(f"  Checkpoint: {checkpoint_path}")
    logger.info(f"  Output strategy: {output_strategy}")
    
    # Ensure checkpoint volume exists
    _ensure_checkpoint_volume_exists(spark, checkpoint_path)

    # Pre-check Presidio availability if enabled
    if use_presidio:
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
        from pyspark.sql.functions import from_json
        presidio_udf = make_presidio_batch_udf(
            score_threshold=score_threshold, model_size=presidio_model_size
        )
        stream_df = stream_df.withColumn(
            "presidio_results", presidio_udf(col(doc_id_column), col(text_column))
        ).withColumn(
            "presidio_results_struct",
            from_json(
                "presidio_results",
                "array<struct<entity:string, entity_type:string, score:double, start:integer, end:integer, doc_id:string>>",
            ),
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
        stream_df = run_gliner_detection(
            stream_df, doc_id_column, text_column, gliner_model, num_cores,
            threshold=gliner_threshold,
            _repartition=False,
        )

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

    # Apply redaction
    stream_df = _apply_redaction(stream_df, text_column, entities_column, redaction_strategy)

    # Select output columns
    stream_df = _select_output_columns(stream_df, doc_id_column, text_column, output_strategy)

    # Ensure output table exists for MERGE INTO (create empty if needed)
    if not spark.catalog.tableExists(output_table):
        stream_df.limit(0).write.format("delta").option("mergeSchema", "true").saveAsTable(output_table)

    # Use foreachBatch with MERGE INTO to handle deduplication and updates.
    _doc_id_col = doc_id_column
    _output_tbl = output_table
    _has_ai_flag = "_ai_detection_failed" in stream_df.columns

    def _write_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return
        # Log AI failures if the flag column is present
        if _has_ai_flag:
            fail_count = batch_df.filter(col("_ai_detection_failed")).count()
            if fail_count > 0:
                import logging
                logging.getLogger("dbxredact.streaming").warning(
                    f"Batch {batch_id}: {fail_count} document(s) had AI detection failures"
                )
        view_name = f"_dbxredact_batch_{batch_id}"
        batch_df.createOrReplaceTempView(view_name)
        batch_df.sparkSession.sql(f"""
            MERGE INTO {_output_tbl} t
            USING {view_name} s
            ON t.{_doc_id_col} = s.{_doc_id_col}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

    query = (
        stream_df
        .writeStream
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .foreachBatch(_write_batch)
        .start()
    )

    return query


def run_redaction_pipeline_by_tag(
    spark: SparkSession,
    source_table: str,
    output_table: str,
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
    num_cores: int = 10,
    output_strategy: OutputStrategy = "production",
    max_rows: Optional[int] = 10000,
    reasoning_effort: str = DEFAULT_AI_REASONING_EFFORT,
    presidio_model_size: str = None,
    alignment_mode: AlignmentMode = "union",
    fuzzy_threshold: int = 50,
) -> DataFrame:
    """
    Run redaction pipeline on columns identified by Unity Catalog tags.

    Args:
        spark: Active SparkSession
        source_table: Fully qualified source table name
        output_table: Fully qualified output table name
        tag_name: Name of tag to filter by
        tag_value: Value of tag to match
        doc_id_column: Name of document ID column
        use_presidio: Whether to run Presidio detection
        use_ai_query: Whether to run AI Query detection
        use_gliner: Whether to run GLiNER detection
        redaction_strategy: Redaction strategy ('generic' or 'typed')
        endpoint: Databricks endpoint for AI detection
        score_threshold: Minimum confidence score for Presidio
        gliner_model: HuggingFace model for GLiNER
        num_cores: Number of cores for repartitioning
        output_strategy: 'validation' (all columns) or 'production' (doc_id + redacted only)
        max_rows: Maximum rows to process after dedup. Set to None or 0 to process all rows. Default: 10000
        presidio_model_size: spaCy model size for Presidio ('sm', 'md', 'lg', 'trf')

    Returns:
        DataFrame with redacted columns
    """
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
            num_cores=num_cores,
            output_strategy=output_strategy,
            max_rows=max_rows,
            reasoning_effort=reasoning_effort,
            presidio_model_size=presidio_model_size,
            alignment_mode=alignment_mode,
            fuzzy_threshold=fuzzy_threshold,
        )

    return result_df
