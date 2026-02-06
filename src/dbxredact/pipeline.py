"""End-to-end PHI/PII detection and redaction pipelines."""

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


OutputStrategy = Literal["validation", "production"]


def _apply_alignment(
    df: DataFrame,
    doc_id_column: str,
    use_presidio: bool,
    use_ai_query: bool,
    use_gliner: bool,
) -> DataFrame:
    """Apply entity alignment to detection results.
    
    Args:
        df: DataFrame with detection results
        doc_id_column: Name of document ID column
        use_presidio: Whether Presidio was used
        use_ai_query: Whether AI Query was used
        use_gliner: Whether GLiNER was used
        
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

    align_udf = align_entities_udf(
        fuzzy_threshold=50,
        include_presidio=use_presidio,
        include_gliner=use_gliner,
        include_ai=use_ai_query,
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
    gliner_model: str = "Ihor/gliner-biomed-large-v1.0",
    num_cores: int = 10,
    align_results: bool = True,
    fail_on_presidio_error: bool = True,
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

    Returns:
        DataFrame with detection results and optional aligned entities
    """
    print("1. Run detection pipeline.")
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
        num_cores=num_cores,
        fail_on_presidio_error=fail_on_presidio_error,
    )

    methods_used = sum([use_presidio, use_ai_query, use_gliner])

    if align_results and methods_used >= 1:
        print("2. Aligning entity results from multiple sources...")
        result_df = _apply_alignment(
            result_df, doc_id_column, use_presidio, use_ai_query, use_gliner
        )
        print("Alignment complete")

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
    gliner_model: str = "Ihor/gliner-biomed-large-v1.0",
    num_cores: int = 10,
    use_aligned: bool = True,
    fail_on_presidio_error: bool = True,
    output_strategy: OutputStrategy = "validation",
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

    Returns:
        DataFrame with redacted text
    """
    source_df = spark.table(source_table).select(doc_id_column, text_column).distinct()

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
        num_cores=num_cores,
        align_results=True,
        fail_on_presidio_error=fail_on_presidio_error,
    )

    entities_column = _get_entities_column(detection_df, use_aligned)

    # Apply redaction
    print("3. Applying redaction...")
    result_df = _apply_redaction(
        detection_df, text_column, entities_column, redaction_strategy
    )

    # Select output columns
    output_df = _select_output_columns(result_df, doc_id_column, text_column, output_strategy)

    # Write to table
    print(f"4. Writing to {output_table}...")
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
        print(f"  Warning: Could not parse volume from checkpoint path: {checkpoint_path}")
        return
    
    catalog, schema, volume_name = match.groups()
    volume_fqn = f"{catalog}.{schema}.{volume_name}"
    
    try:
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {volume_fqn}")
        print(f"  Checkpoint volume ensured: {volume_fqn}")
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
    gliner_model: str = "Ihor/gliner-biomed-large-v1.0",
    num_cores: int = 10,
    use_aligned: bool = True,
    fail_on_presidio_error: bool = True,
    output_strategy: OutputStrategy = "production",
) -> StreamingQuery:
    """
    Run streaming redaction pipeline with incremental processing.

    Uses native streaming DataFrame operations for incremental updates.
    Suitable for Unity Catalog Delta tables with append-only incremental updates.

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

    Returns:
        StreamingQuery that can be awaited or monitored
    """
    print(f"Starting streaming redaction pipeline")
    print(f"  Source: {source_table}")
    print(f"  Output: {output_table}")
    print(f"  Checkpoint: {checkpoint_path}")
    print(f"  Output strategy: {output_strategy}")
    
    # Ensure checkpoint volume exists
    _ensure_checkpoint_volume_exists(spark, checkpoint_path)

    # Pre-check Presidio availability if enabled
    if use_presidio:
        from .detection import check_presidio_available
        is_available, error_msg = check_presidio_available()
        if not is_available:
            if fail_on_presidio_error:
                raise Exception(f"Presidio unavailable: {error_msg}")
            print(f"WARNING: Presidio detection skipped - {error_msg}")
            use_presidio = False

    # Read source as stream
    stream_df = spark.readStream.table(source_table)

    # Apply detection transformations (all streaming compatible)
    if use_presidio:
        from .presidio import make_presidio_batch_udf
        from pyspark.sql.functions import from_json
        presidio_udf = make_presidio_batch_udf(score_threshold=score_threshold)
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
        # run_ai_query_detection now uses expr() - streaming compatible
        stream_df = run_ai_query_detection(
            spark=spark,
            df=stream_df,
            doc_id_column=doc_id_column,
            text_column=text_column,
            endpoint=endpoint or "databricks-gpt-oss-120b",
            num_cores=num_cores,
        )

    if use_gliner:
        from .gliner_detector import run_gliner_detection
        stream_df = run_gliner_detection(
            stream_df, doc_id_column, text_column, gliner_model, num_cores
        )

    # Apply alignment
    methods_used = sum([use_presidio, use_ai_query, use_gliner])
    if methods_used >= 1:
        stream_df = _apply_alignment(
            stream_df, doc_id_column, use_presidio, use_ai_query, use_gliner
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

    # Write stream to output table
    query = (
        stream_df
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(output_table)
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
    gliner_model: str = "Ihor/gliner-biomed-large-v1.0",
    num_cores: int = 10,
    output_strategy: OutputStrategy = "validation",
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

    text_column = protected_columns[0]

    result_df = run_redaction_pipeline(
        spark=spark,
        source_table=source_table,
        text_column=text_column,
        output_table=output_table,
        doc_id_column=doc_id_column,
        use_presidio=use_presidio,
        use_ai_query=use_ai_query,
        use_gliner=use_gliner,
        redaction_strategy=redaction_strategy,
        endpoint=endpoint,
        score_threshold=score_threshold,
        gliner_model=gliner_model,
        num_cores=num_cores,
        output_strategy=output_strategy,
    )

    return result_df
