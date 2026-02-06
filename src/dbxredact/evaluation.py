"""Evaluation and metrics functions for PHI/PII detection."""

from typing import Dict, Any
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, contains, asc_nulls_last


def evaluate_detection(
    ground_truth_df: DataFrame,
    detection_df: DataFrame,
    text_column: str = "text",
    chunk_column: str = "chunk",
    entity_column: str = "entity",
    doc_id_column: str = "doc_id",
    begin_column: str = "begin",
    end_column: str = "end",
    start_column: str = "start",
) -> DataFrame:
    """
    Evaluate detected entities against ground truth.

    Args:
        ground_truth_df: DataFrame with ground truth entities
        detection_df: DataFrame with detected entities
        text_column: Name of text column (excluded from output)
        chunk_column: Name of ground truth chunk column
        entity_column: Name of detected entity column
        doc_id_column: Name of document ID column
        begin_column: Name of ground truth start position column
        end_column: Name of ground truth end position column
        start_column: Name of detection start position column

    Returns:
        DataFrame with matched and unmatched entities for evaluation
    """
    gt = ground_truth_df.alias("gt")
    det = detection_df.alias("det")

    eval_df = gt.join(
        det,
        (col(f"det.{doc_id_column}") == col(f"gt.{doc_id_column}"))
        & (
            contains(col(f"gt.{chunk_column}"), col(f"det.{entity_column}"))
            | contains(col(f"det.{entity_column}"), col(f"gt.{chunk_column}"))
        )
        & (col(f"det.{start_column}") <= col(f"gt.{begin_column}"))
        & (col(f"det.{end_column}") >= col(f"gt.{end_column}") - 1),
        how="outer",
    )

    if text_column in eval_df.columns:
        eval_df = eval_df.drop(text_column)

    eval_df = eval_df.orderBy(
        asc_nulls_last(col(f"gt.{doc_id_column}")),
        asc_nulls_last(col(f"gt.{begin_column}")),
    )

    return eval_df


def calculate_metrics(
    eval_df: DataFrame,
    total_tokens: int,
    chunk_column: str = "chunk",
    entity_column: str = "entity",
) -> Dict[str, Any]:
    """
    Calculate classification metrics for PHI detection.

    Args:
        eval_df: Result from evaluate_detection()
        total_tokens: Total number of tokens/characters in corpus
        chunk_column: Name of ground truth chunk column
        entity_column: Name of detected entity column

    Returns:
        Dictionary with metrics and contingency table values
    """
    pos_actual = eval_df.where(col(chunk_column).isNotNull()).count()
    pos_pred = eval_df.where(col(entity_column).isNotNull()).count()
    tp = eval_df.where(
        col(chunk_column).isNotNull() & col(entity_column).isNotNull()
    ).count()

    fp = pos_pred - tp
    neg_actual = total_tokens - pos_actual
    tn = neg_actual - fp
    fn = pos_actual - tp
    neg_pred = tn + fn

    recall = tp / pos_actual if pos_actual > 0 else 0.0
    precision = tp / pos_pred if pos_pred > 0 else 0.0
    specificity = tn / neg_actual if neg_actual > 0 else 0.0
    npv = tn / neg_pred if neg_pred > 0 else 0.0
    accuracy = (
        (tp + tn) / (pos_actual + neg_actual) if (pos_actual + neg_actual) > 0 else 0.0
    )

    f1 = (
        2 * (precision * recall) / (precision + recall)
        if (precision + recall) > 0
        else 0.0
    )

    return {
        "true_positives": tp,
        "false_positives": fp,
        "true_negatives": tn,
        "false_negatives": fn,
        "pos_actual": pos_actual,
        "neg_actual": neg_actual,
        "pos_pred": pos_pred,
        "neg_pred": neg_pred,
        "total_tokens": total_tokens,
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "specificity": specificity,
        "npv": npv,
        "f1_score": f1,
    }


def format_contingency_table(metrics: Dict[str, Any]) -> pd.DataFrame:
    """Format metrics as a contingency table DataFrame."""
    tn = metrics["true_negatives"]
    fp = metrics["false_positives"]
    fn = metrics["false_negatives"]
    tp = metrics["true_positives"]

    contingency_data = {
        "": ["Neg_pred", "Pos_pred", "Total"],
        "Neg_actual": [tn, fp, tn + fp],
        "Pos_actual": [fn, tp, fn + tp],
        "Total": [tn + fn, fp + tp, tn + fp + fn + tp],
    }

    return pd.DataFrame(contingency_data)


def format_metrics_summary(metrics: Dict[str, Any]) -> pd.DataFrame:
    """Format key metrics as a summary DataFrame."""
    summary_data = {
        "Metric": ["Accuracy", "Precision", "Recall", "Specificity", "NPV", "F1 Score"],
        "Value": [
            metrics["accuracy"],
            metrics["precision"],
            metrics["recall"],
            metrics["specificity"],
            metrics["npv"],
            metrics["f1_score"],
        ],
    }

    return pd.DataFrame(summary_data)


def metrics_to_long_format(
    metrics: Dict[str, Any], dataset_name: str, method_name: str
) -> pd.DataFrame:
    """Convert metrics dictionary to long format for storage and analysis."""
    import datetime

    metric_names = [
        "accuracy",
        "precision",
        "recall",
        "specificity",
        "npv",
        "f1_score",
        "true_positives",
        "false_positives",
        "true_negatives",
        "false_negatives",
    ]

    rows = []
    timestamp = datetime.datetime.now()

    for metric_name in metric_names:
        if metric_name in metrics:
            rows.append(
                {
                    "dataset_name": dataset_name,
                    "method_name": method_name,
                    "metric_name": metric_name,
                    "metric_value": float(metrics[metric_name]),
                    "timestamp": timestamp,
                }
            )

    return pd.DataFrame(rows)


def save_evaluation_results(
    spark,
    metrics: Dict[str, Any],
    dataset_name: str,
    method_name: str,
    output_table: str,
    mode: str = "append",
) -> None:
    """Save evaluation metrics to a shared table."""
    long_df = metrics_to_long_format(metrics, dataset_name, method_name)
    spark_df = spark.createDataFrame(long_df)
    spark_df.write.mode(mode).option("mergeSchema", "true").saveAsTable(output_table)


def compare_methods_across_datasets(
    spark, evaluation_table: str, metric_name: str = "f1_score"
) -> DataFrame:
    """Compare detection methods across multiple datasets."""
    query = f"""
    SELECT 
        dataset_name,
        method_name,
        metric_value
    FROM {evaluation_table}
    WHERE metric_name = '{metric_name}'
    ORDER BY dataset_name, method_name
    """

    return spark.sql(query)


def get_best_method_per_dataset(
    spark, evaluation_table: str, metric_name: str = "f1_score"
) -> DataFrame:
    """Identify the best performing method for each dataset."""
    query = f"""
    WITH ranked AS (
        SELECT 
            dataset_name,
            method_name,
            metric_value,
            ROW_NUMBER() OVER (
                PARTITION BY dataset_name 
                ORDER BY metric_value DESC
            ) as rank
        FROM {evaluation_table}
        WHERE metric_name = '{metric_name}'
    )
    SELECT 
        dataset_name,
        method_name,
        metric_value
    FROM ranked
    WHERE rank = 1
    ORDER BY dataset_name
    """

    return spark.sql(query)

