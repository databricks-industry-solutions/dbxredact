"""Unified PHI/PII detection interface."""

from typing import Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json, expr, concat, lit

from .presidio import make_presidio_batch_udf
from .ai_detector import make_prompt, format_entity_response_object_udf
from .gliner_detector import run_gliner_detection as _run_gliner_detection
from .analyzer import SpacyModelNotFoundError
from .config import LABEL_ENUMS, PHI_PROMPT_SKELETON, DEFAULT_PRESIDIO_SCORE_THRESHOLD


def check_presidio_available() -> tuple:
    """Check if Presidio detection is available (spaCy models installed).
    
    Returns:
        Tuple of (is_available: bool, error_message: str or None)
    """
    try:
        from .analyzer import check_spacy_models
        available, missing = check_spacy_models(["en"])
        if missing:
            return False, (
                f"spaCy models not installed: {missing}. "
                "Install with: %pip install https://github.com/explosion/spacy-models/releases/download/en_core_web_sm-3.8.0/en_core_web_sm-3.8.0-py3-none-any.whl"
            )
        return True, None
    except Exception as e:
        return False, str(e)


def run_presidio_detection(
    df: DataFrame,
    doc_id_column: str,
    text_column: str,
    score_threshold: float = DEFAULT_PRESIDIO_SCORE_THRESHOLD,
    num_cores: int = 10,
) -> DataFrame:
    """
    Run Presidio-based PHI detection on a DataFrame.

    Args:
        df: Input DataFrame with text to analyze
        doc_id_column: Name of document ID column
        text_column: Name of text column to analyze
        score_threshold: Minimum confidence score (0.0-1.0)
        num_cores: Number of cores for repartitioning

    Returns:
        DataFrame with 'presidio_results' and 'presidio_results_struct' columns
        
    Raises:
        SpacyModelNotFoundError: If required spaCy models are not installed
    """
    # Check if models are available before trying to run
    is_available, error_msg = check_presidio_available()
    if not is_available:
        raise SpacyModelNotFoundError(error_msg)
    
    presidio_udf = make_presidio_batch_udf(score_threshold=score_threshold)

    result_df = (
        df.repartition(num_cores)
        .withColumn(
            "presidio_results", presidio_udf(col(doc_id_column), col(text_column))
        )
        .withColumn(
            "presidio_results_struct",
            from_json(
                "presidio_results",
                "array<struct<entity:string, entity_type:string, score:double, start:integer, end:integer, doc_id:string>>",
            ),
        )
    )

    return result_df


def run_ai_query_detection(
    spark: SparkSession,
    df: DataFrame,
    doc_id_column: str,
    text_column: str,
    endpoint: str = "databricks-gpt-oss-120b",
    num_cores: int = 10,
    prompt_skeleton: str = PHI_PROMPT_SKELETON,
    labels: str = LABEL_ENUMS,
) -> DataFrame:
    """
    Run AI-based PHI detection using Databricks AI Query.

    Uses DataFrame API with expr() for streaming compatibility.

    Args:
        spark: Active SparkSession
        df: Input DataFrame with text to analyze (batch or streaming)
        doc_id_column: Name of document ID column
        text_column: Name of text column to analyze
        endpoint: Databricks model serving endpoint name
        num_cores: Number of cores for repartitioning
        prompt_skeleton: Template prompt for AI detection
        labels: Entity type labels for detection

    Returns:
        DataFrame with 'ai_query_results' and 'ai_results_struct' columns
    """
    prompt = make_prompt(prompt_skeleton, labels=labels)
    
    # Split prompt at {med_text} placeholder for concat
    prompt_parts = prompt.split("{med_text}")
    prompt_prefix = prompt_parts[0].replace("'", "''")  # Escape single quotes for SQL
    prompt_suffix = prompt_parts[1].replace("'", "''") if len(prompt_parts) > 1 else ""
    
    # Build ai_query expression using DataFrame API (streaming compatible)
    ai_query_expr = f"""
        ai_query(
            '{endpoint}',
            concat('{prompt_prefix}', CAST({text_column} AS STRING), '{prompt_suffix}'),
            failOnError => false,
            returnType => 'STRUCT<result: ARRAY<STRUCT<entity: STRING, entity_type: STRING>>>',
            modelParameters => named_struct('reasoning_effort', 'low')
        )
    """

    result_df = (
        df.repartition(num_cores)
        .withColumn("response", expr(ai_query_expr))
        .withColumn(
            "ai_query_results",
            format_entity_response_object_udf(col("response.result"), col(text_column)),
        )
        .withColumn(
            "ai_results_struct",
            from_json(
                "ai_query_results",
                "array<struct<entity:string, entity_type:string, score:double, start:integer, end:integer, doc_id:string>>",
            ),
        )
    )

    return result_df


def run_gliner_detection(
    df: DataFrame,
    doc_id_column: str,
    text_column: str,
    model_name: str = "Ihor/gliner-biomed-large-v1.0",
    num_cores: int = 10,
) -> DataFrame:
    """
    Run GLiNER NER model-based PHI detection.

    Args:
        df: Input DataFrame with text to analyze
        doc_id_column: Name of document ID column
        text_column: Name of text column to analyze
        model_name: HuggingFace model identifier for GLiNER
        num_cores: Number of cores for repartitioning

    Returns:
        DataFrame with 'gliner_results' and 'gliner_results_struct' columns
    """
    return _run_gliner_detection(df, doc_id_column, text_column, model_name, num_cores)


def run_detection(
    spark: SparkSession,
    df: DataFrame,
    doc_id_column: str,
    text_column: str,
    use_presidio: bool = True,
    use_ai_query: bool = True,
    use_gliner: bool = False,
    endpoint: Optional[str] = None,
    score_threshold: float = DEFAULT_PRESIDIO_SCORE_THRESHOLD,
    gliner_model: str = "Ihor/gliner-biomed-large-v1.0",
    num_cores: int = 10,
    fail_on_presidio_error: bool = True,
) -> DataFrame:
    """
    Run PHI/PII detection using selected method(s).

    Args:
        spark: Active SparkSession
        df: Input DataFrame with text to analyze
        doc_id_column: Name of document ID column
        text_column: Name of text column to analyze
        use_presidio: Whether to run Presidio detection
        use_ai_query: Whether to run AI Query detection
        use_gliner: Whether to run GLiNER NER detection
        endpoint: Databricks endpoint for AI detection
        score_threshold: Minimum confidence score for Presidio
        gliner_model: HuggingFace model name for GLiNER
        num_cores: Number of cores for repartitioning
        fail_on_presidio_error: If False, continue without Presidio if models unavailable

    Returns:
        DataFrame with detection results from enabled method(s)
    """
    result_df = df
    presidio_skipped = False

    if use_presidio:
        try:
            result_df = run_presidio_detection(
                result_df,
                doc_id_column=doc_id_column,
                text_column=text_column,
                score_threshold=score_threshold,
                num_cores=num_cores,
            )
        except SpacyModelNotFoundError as e:
            if fail_on_presidio_error:
                raise
            print(f"WARNING: Presidio detection skipped - {e}")
            print("Continuing with other detection methods...")
            presidio_skipped = True

    if use_ai_query:
        if endpoint is None:
            endpoint = "databricks-gpt-oss-120b"

        result_df = run_ai_query_detection(
            spark,
            result_df,
            doc_id_column=doc_id_column,
            text_column=text_column,
            endpoint=endpoint,
            num_cores=num_cores,
        )

    if use_gliner:
        result_df = run_gliner_detection(
            result_df,
            doc_id_column=doc_id_column,
            text_column=text_column,
            model_name=gliner_model,
            num_cores=num_cores,
        )

    return result_df

