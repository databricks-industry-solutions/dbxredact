"""Active learning utilities -- uncertainty scoring and review queue building."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _best_score(ent_alias: str = "ent") -> F.Column:
    """Derive the best available score from presidio/gliner/ai score fields."""
    return F.coalesce(
        F.col(f"{ent_alias}.presidio_score"),
        F.col(f"{ent_alias}.gliner_score"),
        F.col(f"{ent_alias}.ai_score"),
    )


def _num_sources(ent_alias: str = "ent") -> F.Column:
    """Count how many detectors contributed by checking non-null score fields."""
    return (
        F.when(F.col(f"{ent_alias}.presidio_score").isNotNull(), F.lit(1)).otherwise(F.lit(0))
        + F.when(F.col(f"{ent_alias}.gliner_score").isNotNull(), F.lit(1)).otherwise(F.lit(0))
        + F.when(F.col(f"{ent_alias}.ai_score").isNotNull(), F.lit(1)).otherwise(F.lit(0))
    )


def compute_document_uncertainty(
    detection_df: DataFrame,
    doc_id_column: str = "doc_id",
    entities_column: str = "aligned_entities",
) -> DataFrame:
    """Score each document by detection uncertainty.

    Returns a DataFrame with columns: doc_id, avg_score, min_score, entity_count,
    low_confidence_count, uncertainty_score (0-1, higher = more uncertain).
    """
    exploded = detection_df.select(
        F.col(doc_id_column),
        F.explode_outer(F.col(entities_column)).alias("ent"),
    )

    score_col = _best_score("ent")

    agg = exploded.groupBy(doc_id_column).agg(
        F.avg(score_col).alias("avg_score"),
        F.min(score_col).alias("min_score"),
        F.count(F.col("ent")).alias("entity_count"),
        F.sum(F.when(score_col < 0.5, 1).otherwise(0)).alias("low_confidence_count"),
    )

    return agg.withColumn(
        "uncertainty_score",
        F.when(F.col("entity_count") == 0, F.lit(0.5))
        .otherwise(
            F.lit(1.0) - F.col("avg_score") + (F.col("low_confidence_count") / F.col("entity_count")) * 0.3
        ),
    )


def build_review_queue(
    detection_df: DataFrame,
    top_k: int = 100,
    doc_id_column: str = "doc_id",
    entities_column: str = "aligned_entities",
) -> DataFrame:
    """Return the top-K most uncertain documents for human review."""
    scored = compute_document_uncertainty(detection_df, doc_id_column, entities_column)
    return scored.orderBy(F.col("uncertainty_score").desc()).limit(top_k)


def compute_detector_disagreement(
    detection_df: DataFrame,
    doc_id_column: str = "doc_id",
) -> DataFrame:
    """Score documents by disagreement between detectors.

    Counts contributing detectors per entity via non-null score fields
    (presidio_score, gliner_score, ai_score). Documents where entities
    are only found by one detector get a higher disagreement score.
    """
    if "aligned_entities" not in detection_df.columns:
        raise ValueError("aligned_entities column required -- run alignment first")

    exploded = detection_df.select(
        F.col(doc_id_column),
        F.explode(F.col("aligned_entities")).alias("ent"),
    )

    per_entity = exploded.select(
        F.col(doc_id_column),
        _num_sources("ent").alias("num_sources"),
    )

    return per_entity.groupBy(doc_id_column).agg(
        F.avg("num_sources").alias("avg_source_count"),
        F.sum(F.when(F.col("num_sources") == 1, 1).otherwise(0)).alias("single_source_entities"),
        F.count("*").alias("total_entities"),
    ).withColumn(
        "disagreement_score",
        F.when(F.col("total_entities") == 0, F.lit(0.0))
        .otherwise(F.col("single_source_entities") / F.col("total_entities")),
    )
