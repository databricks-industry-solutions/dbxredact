"""Unified PHI/PII detection interface."""

import logging
import time
from typing import Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json, expr

from .presidio import make_presidio_batch_udf
from .ai_detector import make_prompt, format_entity_response_object_udf
from .analyzer import SpacyModelNotFoundError
from .config import (
    LABEL_ENUMS,
    PHI_PROMPT_SKELETON,
    DEFAULT_PRESIDIO_SCORE_THRESHOLD,
    DEFAULT_GLINER_MODEL,
    DEFAULT_GLINER_THRESHOLD,
    DEFAULT_GLINER_MAX_WORDS,
    DEFAULT_AI_REASONING_EFFORT,
)

logger = logging.getLogger(__name__)

# Minimum rows per partition to avoid excessive overhead (model loading, task startup)
_MIN_ROWS_PER_PARTITION = 5


def _smart_partitions(df: DataFrame, num_cores: int, row_count: Optional[int] = None) -> int:
    """Cap partitions so each has at least _MIN_ROWS_PER_PARTITION rows."""
    if row_count is None:
        try:
            row_count = df.count()
        except (ValueError, RuntimeError):
            return num_cores
    return max(1, min(num_cores, row_count // _MIN_ROWS_PER_PARTITION))


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
                "Install with: %pip install https://github.com/explosion/spacy-models/releases/download/en_core_web_trf-3.8.0/en_core_web_trf-3.8.0-py3-none-any.whl"
            )
        return True, None
    except (ImportError, OSError) as e:
        return False, str(e)


def run_presidio_detection(
    df: DataFrame,
    doc_id_column: str,
    text_column: str,
    score_threshold: float = DEFAULT_PRESIDIO_SCORE_THRESHOLD,
    num_cores: int = 10,
    model_size: str = None,
    pattern_only: bool = False,
    _repartition: bool = True,
) -> DataFrame:
    """
    Run Presidio-based PHI detection on a DataFrame.

    Args:
        df: Input DataFrame with text to analyze
        doc_id_column: Name of document ID column
        text_column: Name of text column to analyze
        score_threshold: Minimum confidence score (0.0-1.0)
        num_cores: Number of cores for repartitioning
        model_size: spaCy model size ('sm', 'md', 'lg'). Default 'lg'.
        pattern_only: If True, use only pattern recognizers (no spaCy).
        _repartition: Whether to repartition. Set False when caller already did.

    Returns:
        DataFrame with 'presidio_results' and 'presidio_results_struct' columns
        
    Raises:
        SpacyModelNotFoundError: If required spaCy models are not installed (unless pattern_only)
    """
    if not pattern_only:
        is_available, error_msg = check_presidio_available()
        if not is_available:
            raise SpacyModelNotFoundError(error_msg)

    presidio_udf = make_presidio_batch_udf(
        score_threshold=score_threshold, model_size=model_size, pattern_only=pattern_only,
    )

    base_df = df.repartition(_smart_partitions(df, num_cores)) if _repartition else df
    result_df = (
        base_df
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
    reasoning_effort: str = DEFAULT_AI_REASONING_EFFORT,
    ai_model_type: str = "foundation",
    _repartition: bool = True,
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
        reasoning_effort: Reasoning effort level ("low", "medium", "high")
        ai_model_type: "foundation" uses returnType + reasoning_effort;
            "external" sends plain STRING request (for Claude, etc.)
        _repartition: Whether to repartition. Set False when caller already did.

    Returns:
        DataFrame with 'ai_query_results' and 'ai_results_struct' columns
    """
    prompt = make_prompt(prompt_skeleton, labels=labels)
    
    # Split prompt at {med_text} placeholder for concat
    prompt_parts = prompt.split("{med_text}")
    prompt_prefix = prompt_parts[0].replace("'", "''")
    prompt_suffix = prompt_parts[1].replace("'", "''") if len(prompt_parts) > 1 else ""

    prompt_concat = f"concat('{prompt_prefix}', CAST({text_column} AS STRING), '{prompt_suffix}')"

    if ai_model_type == "external":
        ai_query_expr = f"""
            ai_query(
                '{endpoint}',
                {prompt_concat},
                failOnError => false
            )
        """
    else:
        ai_query_expr = f"""
            ai_query(
                '{endpoint}',
                {prompt_concat},
                failOnError => false,
                returnType => 'STRUCT<result: ARRAY<STRUCT<entity: STRING, entity_type: STRING>>>',
                modelParameters => named_struct('reasoning_effort', '{reasoning_effort}')
            )
        """

    base_df = df.repartition(_smart_partitions(df, num_cores)) if _repartition else df

    if ai_model_type == "external":
        result_df = (
            base_df
            .withColumn("raw_response", expr(ai_query_expr))
            .withColumn(
                "ai_query_results",
                format_entity_response_object_udf(col("raw_response"), col(text_column)),
            )
            .withColumn(
                "ai_results_struct",
                from_json(
                    "ai_query_results",
                    "array<struct<entity:string, entity_type:string, score:double, start:integer, end:integer, doc_id:string>>",
                ),
            )
        )
    else:
        result_df = (
            base_df
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
    model_name: str = DEFAULT_GLINER_MODEL,
    num_cores: int = 10,
    labels=None,
    threshold: float = DEFAULT_GLINER_THRESHOLD,
    gliner_max_words: int = None,
    _repartition: bool = True,
) -> DataFrame:
    """
    Run GLiNER NER model-based PHI detection.

    Args:
        df: Input DataFrame with text to analyze
        doc_id_column: Name of document ID column
        text_column: Name of text column to analyze
        model_name: HuggingFace model identifier for GLiNER
        num_cores: Number of cores for repartitioning
        labels: Entity labels for detection (defaults to PII labels)
        threshold: Minimum confidence threshold
        _repartition: Whether to repartition. Set False when caller already did.

    Returns:
        DataFrame with 'gliner_results' and 'gliner_results_struct' columns
    """
    from .gliner_detector import run_gliner_detection as _run_gliner_detection
    kwargs = dict(
        df=df,
        doc_id_column=doc_id_column,
        text_column=text_column,
        model_name=model_name,
        num_cores=num_cores,
        labels=labels,
        threshold=threshold,
        _repartition=_repartition,
    )
    if gliner_max_words is not None:
        kwargs["max_words"] = gliner_max_words
    return _run_gliner_detection(**kwargs)


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
    gliner_model: str = DEFAULT_GLINER_MODEL,
    gliner_threshold: float = DEFAULT_GLINER_THRESHOLD,
    gliner_max_words: int = None,
    num_cores: int = 10,
    fail_on_presidio_error: bool = True,
    reasoning_effort: str = DEFAULT_AI_REASONING_EFFORT,
    presidio_model_size: str = None,
    presidio_pattern_only: bool = False,
    ai_model_type: str = "foundation",
    row_count: Optional[int] = None,
) -> DataFrame:
    """
    Run PHI/PII detection using selected method(s).

    Repartitions once up-front so individual detectors skip their own
    count()/repartition, avoiding redundant Spark actions on the lazy DAG.
    """
    # Repartition once; all detectors will use _repartition=False
    n_parts = _smart_partitions(df, num_cores, row_count=row_count)
    result_df = df.repartition(n_parts)
    print(f"Repartitioned to {n_parts} partitions (row_count={row_count}).")

    presidio_skipped = False
    _prior_caches = []

    if use_presidio:
        t_presidio = time.time()
        try:
            result_df = run_presidio_detection(
                result_df,
                doc_id_column=doc_id_column,
                text_column=text_column,
                score_threshold=score_threshold,
                num_cores=num_cores,
                model_size=presidio_model_size,
                pattern_only=presidio_pattern_only,
                _repartition=False,
            )
            result_df = result_df.cache()
            result_df.count()
            _prior_caches.append(result_df)
            print(f"   Presidio detection: {time.time() - t_presidio:.1f}s")
        except SpacyModelNotFoundError as e:
            if fail_on_presidio_error:
                raise
            logger.warning("Presidio detection skipped - %s", e)
            logger.info("Continuing with other detection methods...")
            presidio_skipped = True

    if use_ai_query:
        if endpoint is None:
            endpoint = "databricks-gpt-oss-120b"

        t_ai = time.time()
        result_df = run_ai_query_detection(
            spark,
            result_df,
            doc_id_column=doc_id_column,
            text_column=text_column,
            endpoint=endpoint,
            num_cores=num_cores,
            reasoning_effort=reasoning_effort,
            ai_model_type=ai_model_type,
            _repartition=False,
        )
        result_df = result_df.cache()
        result_df.count()
        _prior_caches.append(result_df)
        print(f"   AI Query detection: {time.time() - t_ai:.1f}s")

    if use_gliner:
        t_gliner = time.time()
        gliner_kwargs = dict(
            df=result_df,
            doc_id_column=doc_id_column,
            text_column=text_column,
            model_name=gliner_model,
            num_cores=num_cores,
            threshold=gliner_threshold,
            _repartition=False,
        )
        if gliner_max_words is not None:
            gliner_kwargs["gliner_max_words"] = gliner_max_words
        result_df = run_gliner_detection(**gliner_kwargs)
        result_df = result_df.cache()
        result_df.count()
        _prior_caches.append(result_df)
        print(f"   GLiNER detection: {time.time() - t_gliner:.1f}s")

    # Release intermediate caches; only the final one is needed by the caller
    for c in _prior_caches[:-1]:
        c.unpersist()

    return result_df
