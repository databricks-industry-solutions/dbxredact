"""Evaluation and metrics functions for PHI/PII detection."""

import re
from typing import Dict, Any, List, Literal
import pandas as pd
import numpy as np
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, contains, asc_nulls_last, struct, lower

_SAFE_METRIC_NAME = re.compile(r"^[a-zA-Z0-9_]+$")


def _validate_metric_name(name: str) -> str:
    if not _SAFE_METRIC_NAME.match(name):
        raise ValueError(f"Invalid metric name: {name!r}")
    return name

MatchMode = Literal["strict", "overlap"]


def _match_condition(
    doc_id_column: str,
    chunk_column: str,
    entity_column: str,
    begin_column: str,
    end_column: str,
    start_column: str,
    gt_prefix: str = "gt",
    det_prefix: str = "det",
    match_mode: MatchMode = "strict",
):
    """Build the match condition between GT and detection rows.

    Args:
        match_mode:
            "strict"  - Original full-containment with -1 tolerance.
                        det.start <= gt.begin AND det.end >= gt.end - 1
            "overlap" - Standard interval overlap.
                        det.start < gt.end AND det.end > gt.begin
    """
    gt_chunk = lower(col(f"{gt_prefix}.{chunk_column}"))
    det_entity = lower(col(f"{det_prefix}.{entity_column}"))
    base = (
        col(f"{det_prefix}.{doc_id_column}") == col(f"{gt_prefix}.{doc_id_column}")
    ) & (contains(gt_chunk, det_entity) | contains(det_entity, gt_chunk))
    if match_mode == "overlap":
        position = (
            col(f"{det_prefix}.{start_column}") < col(f"{gt_prefix}.{end_column}")
        ) & (col(f"{det_prefix}.{end_column}") > col(f"{gt_prefix}.{begin_column}"))
    else:  # strict
        position = (
            col(f"{det_prefix}.{start_column}") <= col(f"{gt_prefix}.{begin_column}")
        ) & (col(f"{det_prefix}.{end_column}") >= col(f"{gt_prefix}.{end_column}") - 1)
    return base & position


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
    match_mode: MatchMode = "strict",
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
        match_mode: "strict" (full containment with -1 tolerance) or
                    "overlap" (interval overlap for partial matches)

    Returns:
        DataFrame with matched and unmatched entities for evaluation
    """
    gt = ground_truth_df.alias("gt")
    det = detection_df.alias("det")

    cond = _match_condition(
        doc_id_column,
        chunk_column,
        entity_column,
        begin_column,
        end_column,
        start_column,
        match_mode=match_mode,
    )

    eval_df = gt.join(det, cond, how="outer")

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
    doc_id_column: str = "doc_id",
    begin_column: str = "begin",
    end_column: str = "end",
    start_column: str = "start",
) -> Dict[str, Any]:
    """
    Calculate classification metrics for PHI detection.

    Uses distinct-counting so that one GT entity matching multiple detections
    (or vice versa) is only counted once.

    Args:
        eval_df: Result from evaluate_detection()
        total_tokens: Total number of tokens/characters in corpus
        chunk_column: Name of ground truth chunk column
        entity_column: Name of detected entity column
        doc_id_column: Column identifying the document
        begin_column: GT start position column (used for dedup)
        end_column: GT end position column (used for dedup)
        start_column: Detection start position column (used for dedup)

    Returns:
        Dictionary with metrics and contingency table values
    """
    gt_id = struct(
        col(f"gt.{doc_id_column}"),
        col(f"gt.{begin_column}"),
        col(f"gt.{end_column}"),
        col(f"gt.{chunk_column}"),
    )
    det_id = struct(
        col(f"det.{doc_id_column}"),
        col(f"det.{start_column}"),
        col(f"det.{end_column}"),
        col(f"det.{entity_column}"),
    )

    # Distinct GT entities that appear in the eval_df (matched or not)
    all_gt = (
        eval_df.where(col(f"gt.{chunk_column}").isNotNull())
        .select(gt_id.alias("_gid"))
        .distinct()
    )
    pos_actual = all_gt.count()

    matched_rows = eval_df.where(
        col(f"gt.{chunk_column}").isNotNull() & col(f"det.{entity_column}").isNotNull()
    )

    # GT entities that matched at least one detection (for recall)
    tp_recall = matched_rows.select(gt_id.alias("_gid")).distinct().count()
    fn = pos_actual - tp_recall

    # Distinct detections
    all_det = (
        eval_df.where(col(f"det.{entity_column}").isNotNull())
        .select(det_id.alias("_did"))
        .distinct()
    )
    pos_pred = all_det.count()

    # Detections that matched at least one GT (for precision)
    tp_precision = matched_rows.select(det_id.alias("_did")).distinct().count()
    fp = pos_pred - tp_precision

    tp = tp_recall

    neg_actual = total_tokens - pos_actual
    tn = neg_actual - fp
    neg_pred = tn + fn

    recall = tp_recall / pos_actual if pos_actual > 0 else 0.0
    precision = tp_precision / pos_pred if pos_pred > 0 else 0.0
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
    metrics: Dict[str, Any],
    dataset_name: str,
    method_name: str,
    run_metadata: Dict[str, Any] = None,
) -> pd.DataFrame:
    """Convert metrics dictionary to long format for storage and analysis.

    Args:
        metrics: Dictionary of metric values
        dataset_name: Name of the evaluation dataset
        method_name: Detection method name
        run_metadata: Optional dict of run parameters (e.g. gliner_model,
            reasoning_effort, score_threshold, gliner_threshold, run_id)
    """
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
            row = {
                "dataset_name": dataset_name,
                "method_name": method_name,
                "metric_name": metric_name,
                "metric_value": float(metrics[metric_name]),
                "timestamp": timestamp,
            }
            if run_metadata:
                row.update(run_metadata)
            rows.append(row)

    return pd.DataFrame(rows)


def save_evaluation_results(
    spark,
    metrics: Dict[str, Any],
    dataset_name: str,
    method_name: str,
    output_table: str,
    mode: str = "append",
    run_metadata: Dict[str, Any] = None,
) -> None:
    """Save evaluation metrics to a shared table.

    Args:
        spark: Active SparkSession
        metrics: Dictionary of metric values
        dataset_name: Name of the evaluation dataset
        method_name: Detection method name
        output_table: Fully qualified output table name
        mode: Write mode ('append' or 'overwrite')
        run_metadata: Optional dict of run parameters for reproducibility
    """
    long_df = metrics_to_long_format(metrics, dataset_name, method_name, run_metadata)
    spark_df = spark.createDataFrame(long_df)
    spark_df.write.mode(mode).option("mergeSchema", "true").saveAsTable(output_table)


def compare_methods_across_datasets(
    spark, evaluation_table: str, metric_name: str = "f1_score"
) -> DataFrame:
    """Compare detection methods across multiple datasets."""
    _validate_metric_name(metric_name)
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
    _validate_metric_name(metric_name)
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


_GT_ENTITY_TYPE_CANDIDATES = [
    "entity_type",
    "ner_label",
    "label",
    "ner",
    "type",
    "ner_tag",
]


def _find_gt_entity_type_col(columns: list) -> str:
    """Find the entity-type column in the ground truth by trying common names."""
    for candidate in _GT_ENTITY_TYPE_CANDIDATES:
        if candidate in columns:
            return candidate
    return ""


def analyze_errors(
    ground_truth_df: DataFrame,
    detection_df: DataFrame,
    doc_id_column: str = "doc_id",
    chunk_column: str = "chunk",
    entity_column: str = "entity",
    begin_column: str = "begin",
    end_column: str = "end",
    start_column: str = "start",
    gt_entity_type_column: str = None,
    match_mode: MatchMode = "strict",
) -> Dict[str, pd.DataFrame]:
    """Analyze FP/FN errors with entity type breakdowns.

    Uses semi/anti joins so each GT entity and each detection is counted
    at most once (no double-counting).

    Args:
        gt_entity_type_column: Name of the entity-type column in ground_truth_df.
            If None, auto-detects from common names (entity_type, ner_label, label, ...).

    Returns:
        dict with keys:
            fp_by_type   - FP counts by detected entity_type
            fn_by_type   - FN counts by GT entity_type
            top_fps      - Most common false-positive entity strings
            top_fns      - Most common missed ground-truth entities
            recall_by_type - Recall per GT entity type
    """
    gt = ground_truth_df

    if gt_entity_type_column is None:
        gt_entity_type_column = _find_gt_entity_type_col(gt.columns)

    has_gt_type = bool(gt_entity_type_column)
    if has_gt_type:
        gt = gt.withColumnRenamed(gt_entity_type_column, "gt_entity_type")

    gt = gt.withColumnRenamed(doc_id_column, "gt_doc_id").withColumnRenamed(
        end_column, "gt_end"
    )

    det = detection_df.withColumnRenamed(doc_id_column, "det_doc_id").withColumnRenamed(
        end_column, "det_end"
    )

    # Build match condition with renamed column names
    base_cond = (col("det_doc_id") == col("gt_doc_id")) & (
        contains(col(chunk_column), col(entity_column))
        | contains(col(entity_column), col(chunk_column))
    )
    if match_mode == "overlap":
        pos_cond = (col(start_column) < col("gt_end")) & (
            col("det_end") > col(begin_column)
        )
    else:  # strict
        pos_cond = (col(start_column) <= col(begin_column)) & (
            col("det_end") >= col("gt_end") - 1
        )
    match_cond = base_cond & pos_cond

    # FPs: detections with no overlapping GT entity
    fps = det.join(gt, match_cond, "left_anti")

    # FNs: GT entities with no overlapping detection
    fns = gt.join(det, match_cond, "left_anti")

    # TPs (GT side): GT entities that matched at least one detection
    tps_gt = gt.join(det, match_cond, "left_semi")

    fp_by_type = (
        fps.groupBy("entity_type").count().orderBy(col("count").desc()).toPandas()
    )
    top_fps = (
        fps.groupBy(entity_column, "entity_type")
        .count()
        .orderBy(col("count").desc())
        .limit(25)
        .toPandas()
    )

    if has_gt_type:
        fn_by_type = (
            fns.groupBy("gt_entity_type")
            .count()
            .orderBy(col("count").desc())
            .toPandas()
        )
        top_fns = (
            fns.groupBy(chunk_column, "gt_entity_type")
            .count()
            .orderBy(col("count").desc())
            .limit(25)
            .toPandas()
        )
        tp_counts = (
            tps_gt.groupBy("gt_entity_type")
            .count()
            .withColumnRenamed("count", "tp")
            .toPandas()
        )
        fn_counts = (
            fns.groupBy("gt_entity_type")
            .count()
            .withColumnRenamed("count", "fn")
            .toPandas()
        )
        recall_df = tp_counts.merge(fn_counts, on="gt_entity_type", how="outer").fillna(
            0
        )
        recall_df["total"] = recall_df["tp"] + recall_df["fn"]
        recall_df["recall"] = recall_df["tp"] / recall_df["total"]
        recall_df = recall_df.sort_values("recall", ascending=False)
    else:
        fn_by_type = pd.DataFrame()
        top_fns = (
            fns.groupBy(chunk_column)
            .count()
            .orderBy(col("count").desc())
            .limit(25)
            .toPandas()
        )
        recall_df = pd.DataFrame()

    return {
        "fp_by_type": fp_by_type,
        "fn_by_type": fn_by_type,
        "top_fps": top_fps,
        "top_fns": top_fns,
        "recall_by_type": recall_df,
    }


def build_recall_matrix(
    error_analyses: Dict[str, Dict[str, pd.DataFrame]],
) -> tuple:
    """Build a recall-by-entity-type matrix across methods.

    Args:
        error_analyses: {method_name: analyze_errors() result}

    Returns:
        (matrix np.ndarray, entity_types list, methods list)
    """
    all_types: set = set()
    for errors in error_analyses.values():
        rt = errors.get("recall_by_type")
        if rt is not None and not rt.empty:
            all_types.update(rt["gt_entity_type"].tolist())

    all_types_sorted = sorted(all_types)
    methods = list(error_analyses.keys())
    matrix = np.zeros((len(all_types_sorted), len(methods)))

    for j, method in enumerate(methods):
        rt = error_analyses[method].get("recall_by_type")
        if rt is not None and not rt.empty:
            for i, etype in enumerate(all_types_sorted):
                match = rt[rt["gt_entity_type"] == etype]
                if len(match) > 0:
                    matrix[i, j] = match["recall"].values[0]

    return matrix, all_types_sorted, methods


def summarize_method_strengths(
    error_analyses: Dict[str, Dict[str, pd.DataFrame]],
    evaluation_results: Dict[str, Dict[str, Any]],
) -> pd.DataFrame:
    """Produce a per-method strengths/weaknesses summary.

    Returns a DataFrame with columns: method, fp_count, fn_count, top_fp_types,
    missed_types, best_recall_types.
    """
    rows: List[dict] = []
    for method, errors in error_analyses.items():
        metrics = evaluation_results.get(method, {})
        fp_types = errors["fp_by_type"]
        fn_types = errors["fn_by_type"]
        recall = errors["recall_by_type"]

        top_fp = (
            ", ".join(fp_types["entity_type"].head(3).tolist())
            if not fp_types.empty
            else "-"
        )

        if not fn_types.empty and "gt_entity_type" in fn_types.columns:
            missed = ", ".join(fn_types["gt_entity_type"].head(3).tolist())
        else:
            missed = "-"

        if not recall.empty:
            best = ", ".join(recall.head(3)["gt_entity_type"].tolist())
            worst = ", ".join(recall.tail(3)["gt_entity_type"].tolist())
        else:
            best = worst = "-"

        rows.append(
            {
                "method": method,
                "precision": round(metrics.get("precision", 0), 3),
                "recall": round(metrics.get("recall", 0), 3),
                "f1": round(metrics.get("f1_score", 0), 3),
                "fp_count": int(metrics.get("false_positives", 0)),
                "fn_count": int(metrics.get("false_negatives", 0)),
                "top_fp_types": top_fp,
                "most_missed_types": missed,
                "best_recall_types": best,
                "worst_recall_types": worst,
            }
        )

    return pd.DataFrame(rows)


def diagnose_strict_failures(
    ground_truth_df: DataFrame,
    detection_df: DataFrame,
    doc_id_column: str = "doc_id",
    chunk_column: str = "chunk",
    entity_column: str = "entity",
    begin_column: str = "begin",
    end_column: str = "end",
    start_column: str = "start",
) -> pd.DataFrame:
    """Find entities that match in overlap mode but fail strict containment.

    For each such pair, computes how many characters the detection span is
    off at the start and end boundaries.  Positive ``start_delta`` means the
    detection began *after* the GT span (front-clipped); positive ``end_delta``
    means the detection ended *before* the GT span (back-clipped).

    Returns:
        Pandas DataFrame with columns: doc_id, gt_text, gt_start, gt_end,
        det_text, det_start, det_end, start_delta, end_delta, boundary_type.
    """
    from pyspark.sql.functions import lit, when

    gt = ground_truth_df.alias("gt")
    det = detection_df.alias("det")

    overlap_base = (col(f"det.{doc_id_column}") == col(f"gt.{doc_id_column}")) & (
        contains(lower(col(f"gt.{chunk_column}")), lower(col(f"det.{entity_column}")))
        | contains(lower(col(f"det.{entity_column}")), lower(col(f"gt.{chunk_column}")))
    )
    overlap_pos = (col(f"det.{start_column}") < col(f"gt.{end_column}")) & (
        col(f"det.{end_column}") > col(f"gt.{begin_column}")
    )

    strict_ok = (col(f"det.{start_column}") <= col(f"gt.{begin_column}")) & (
        col(f"det.{end_column}") >= col(f"gt.{end_column}") - 1
    )

    joined = gt.join(det, overlap_base & overlap_pos, "inner").where(~strict_ok)

    result = joined.select(
        col(f"gt.{doc_id_column}").alias("doc_id"),
        col(f"gt.{chunk_column}").alias("gt_text"),
        col(f"gt.{begin_column}").alias("gt_start"),
        col(f"gt.{end_column}").alias("gt_end"),
        col(f"det.{entity_column}").alias("det_text"),
        col(f"det.{start_column}").alias("det_start"),
        col(f"det.{end_column}").alias("det_end"),
        (col(f"det.{start_column}") - col(f"gt.{begin_column}")).alias("start_delta"),
        (col(f"gt.{end_column}") - col(f"det.{end_column}")).alias("end_delta"),
    ).withColumn(
        "boundary_type",
        when((col("start_delta") > 0) & (col("end_delta") > 0), lit("both"))
        .when(col("start_delta") > 0, lit("start_clipped"))
        .when(col("end_delta") > 0, lit("end_clipped"))
        .otherwise(lit("extended")),
    )

    return result.toPandas()
