# Databricks notebook source
# MAGIC %md
# MAGIC # PHI Detection Evaluation
# MAGIC
# MAGIC This notebook evaluates the performance of different PHI detection methods against ground truth data.
# MAGIC
# MAGIC **Detection Methods:**
# MAGIC - Presidio-based detection
# MAGIC - AI-based detection
# MAGIC - GLiNER-based detection
# MAGIC - Aligned/combined detection
# MAGIC
# MAGIC **Metrics:**
# MAGIC - Accuracy, Precision, Recall, Specificity, NPV, F1 Score
# MAGIC
# MAGIC **Output:**
# MAGIC - Long-format evaluation table for cross-dataset comparison

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install dbxredact
# MAGIC
# MAGIC When running via a Databricks Asset Bundle job, the wheel is attached as a cluster library automatically.
# MAGIC For interactive use, uncomment and update the `%pip install` line below.

# COMMAND ----------

# MAGIC # For interactive use, uncomment one of the following:
# MAGIC # %pip install /Workspace/<path-to-bundle>/artifacts/dbxredact-0.1.0-py3-none-any.whl
# MAGIC # %pip install git+https://github.com/databricks-industry-solutions/dbxredact.git
# MAGIC # %restart_python

# COMMAND ----------

import uuid
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pyspark.sql.functions import col, explode

from dbxredact import (
    evaluate_detection,
    calculate_metrics,
    format_contingency_table,
    format_metrics_summary,
    save_evaluation_results,
    compare_methods_across_datasets,
    analyze_errors,
    build_recall_matrix,
    summarize_method_strengths,
)
from dbxredact.config import (
    DEFAULT_GLINER_MODEL,
    DEFAULT_GLINER_THRESHOLD,
    DEFAULT_PRESIDIO_SCORE_THRESHOLD,
    DEFAULT_AI_REASONING_EFFORT,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget Configuration

# COMMAND ----------

dbutils.widgets.text(
    name="ground_truth_table",
    defaultValue="your_catalog.your_schema.jsl_benchmark_ground_truth",
    label="0. Ground Truth Table",
)
dbutils.widgets.text(
    name="detection_results_table",
    defaultValue="your_catalog.your_schema.jsl_benchmark_detection_results",
    label="1. Detection Results Table",
)
dbutils.widgets.text(
    name="dataset_name",
    defaultValue="jsl_benchmark",
    label="2. Dataset Name (for tracking)",
)
dbutils.widgets.text(
    name="evaluation_output_table",
    defaultValue="your_catalog.your_schema.phi_evaluation_results",
    label="3. Evaluation Output Table",
)
dbutils.widgets.dropdown(
    name="write_mode",
    defaultValue="append",
    choices=["append", "overwrite"],
    label="4. Write Mode",
)

ground_truth_table = dbutils.widgets.get("ground_truth_table")
detection_results_table = dbutils.widgets.get("detection_results_table")
dataset_name = dbutils.widgets.get("dataset_name")
evaluation_output_table = dbutils.widgets.get("evaluation_output_table")
write_mode = dbutils.widgets.get("write_mode")

for _table in [ground_truth_table, detection_results_table, evaluation_output_table]:
    if "your_catalog" in _table or "your_schema" in _table:
        raise ValueError(
            "Please update all table widgets with your actual catalog and schema names. "
            "The defaults (your_catalog.your_schema) are placeholders."
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data

# COMMAND ----------

ground_truth_df = spark.table(ground_truth_table)
print(f"Ground truth records: {ground_truth_df.count()}")
display(ground_truth_df.limit(5))

# COMMAND ----------

detection_df = spark.table(detection_results_table)
print(f"Detection results: {detection_df.count()}")
display(detection_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare Data for Evaluation

# COMMAND ----------

available_methods = []
if "presidio_results_struct" in detection_df.columns:
    available_methods.append("presidio")
if "ai_results_struct" in detection_df.columns:
    available_methods.append("ai")
if "gliner_results_struct" in detection_df.columns:
    available_methods.append("gliner")
if "aligned_entities" in detection_df.columns:
    available_methods.append("aligned")

print(f"Available detection methods: {available_methods}")

# COMMAND ----------

exploded_results = {}

if "presidio" in available_methods:
    exploded_results["presidio"] = (
        detection_df.select("doc_id", "presidio_results_struct")
        .withColumn("presidio_results_exploded", explode("presidio_results_struct"))
        .select(
            "presidio_results_exploded.doc_id",
            "presidio_results_exploded.entity",
            "presidio_results_exploded.entity_type",
            "presidio_results_exploded.score",
            "presidio_results_exploded.start",
            "presidio_results_exploded.end",
        )
    )

if "ai" in available_methods:
    exploded_results["ai"] = (
        detection_df.select("doc_id", "ai_results_struct")
        .withColumn("ai_results_exploded", explode("ai_results_struct"))
        .select(
            "doc_id",
            "ai_results_exploded.entity",
            "ai_results_exploded.entity_type",
            "ai_results_exploded.score",
            "ai_results_exploded.start",
            "ai_results_exploded.end",
        )
    )

if "gliner" in available_methods:
    exploded_results["gliner"] = (
        detection_df.select("doc_id", "gliner_results_struct")
        .withColumn("gliner_results_exploded", explode("gliner_results_struct"))
        .select(
            "doc_id",
            "gliner_results_exploded.entity",
            "gliner_results_exploded.entity_type",
            "gliner_results_exploded.score",
            "gliner_results_exploded.start",
            "gliner_results_exploded.end",
        )
    )

if "aligned" in available_methods:
    exploded_results["aligned"] = (
        detection_df.select("doc_id", "aligned_entities")
        .withColumn("aligned_entities", explode("aligned_entities"))
        .select(
            "doc_id",
            "aligned_entities.entity",
            "aligned_entities.entity_type",
            "aligned_entities.start",
            "aligned_entities.end",
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculate Corpus Statistics

# COMMAND ----------

text_dict = (
    ground_truth_df.select("doc_id", "text")
    .distinct()
    .toPandas()
    .to_dict(orient="list")
)
corpus = "\n".join(text_dict["text"])
all_tokens = len(corpus)

print(f"Total tokens in corpus: {all_tokens:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evaluate Each Detection Method
# MAGIC
# MAGIC Runs both **strict** (full-containment with -1 tolerance) and **overlap** (interval
# MAGIC overlap for partial matches) evaluation modes.

# COMMAND ----------

MATCH_MODES = ["strict", "overlap"]

run_id = str(uuid.uuid4())
base_run_metadata = {
    "run_id": run_id,
    "gliner_model": DEFAULT_GLINER_MODEL,
    "reasoning_effort": DEFAULT_AI_REASONING_EFFORT,
    "score_threshold": DEFAULT_PRESIDIO_SCORE_THRESHOLD,
    "gliner_threshold": DEFAULT_GLINER_THRESHOLD,
}
print(f"Run metadata: {base_run_metadata}")

# evaluation_results[match_mode][method_name] = metrics dict
evaluation_results = {mode: {} for mode in MATCH_MODES}

for mode in MATCH_MODES:
    print(f"\n{'#'*80}")
    print(f"  MATCH MODE: {mode.upper()}")
    print(f"{'#'*80}")

    run_metadata = {**base_run_metadata, "match_mode": mode}

    for method_name, exploded_df in exploded_results.items():
        print(f"\n{'='*80}")
        print(f"Evaluating: {method_name.upper()} ({mode})")
        print(f"{'='*80}")

        eval_df = evaluate_detection(ground_truth_df, exploded_df, match_mode=mode)
        metrics = calculate_metrics(eval_df, all_tokens)
        evaluation_results[mode][method_name] = metrics

        print(f"\n{method_name.upper()} Contingency Table:")
        contingency_df = format_contingency_table(metrics)
        display(contingency_df)

        print(f"\n{method_name.upper()} Metrics Summary:")
        summary_df = format_metrics_summary(metrics)
        display(summary_df)

        save_evaluation_results(
            spark=spark,
            metrics=metrics,
            dataset_name=dataset_name,
            method_name=f"{method_name}_{mode}",
            output_table=evaluation_output_table,
            mode=write_mode,
            run_metadata=run_metadata,
        )

        print(f"\nResults saved to {evaluation_output_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compare Methods (both modes)

# COMMAND ----------

comparison_rows = []
for mode in MATCH_MODES:
    for method_name, metrics in evaluation_results[mode].items():
        comparison_rows.append({
            "Method": method_name.capitalize(),
            "Match Mode": mode,
            "Precision": metrics["precision"],
            "Recall": metrics["recall"],
            "F1 Score": metrics["f1_score"],
            "Accuracy": metrics["accuracy"],
        })

comparison_df = pd.DataFrame(comparison_rows)
display(comparison_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cross-Dataset Comparison

# COMMAND ----------

try:
    all_results = spark.table(evaluation_output_table)
    print(f"Total evaluation records: {all_results.count()}")
    display(all_results)
except Exception as e:
    print(f"Could not load evaluation table: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## F1 Score Comparison Across Datasets

# COMMAND ----------

try:
    f1_comparison = compare_methods_across_datasets(
        spark=spark, evaluation_table=evaluation_output_table, metric_name="f1_score"
    )
    display(f1_comparison)
except Exception as e:
    print(f"Could not compare across datasets: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Error Analysis per Method
# MAGIC
# MAGIC For each detection method, identify the top false positives (entities detected that
# MAGIC are not in ground truth) and false negatives (ground truth entities that were missed),
# MAGIC broken down by entity type.

# COMMAND ----------

# error_analyses[match_mode][method_name] = analyze_errors() dict
error_analyses = {mode: {} for mode in MATCH_MODES}

for mode in MATCH_MODES:
    print(f"\n{'#'*80}")
    print(f"  ERROR ANALYSIS -- MATCH MODE: {mode.upper()}")
    print(f"{'#'*80}")

    for method_name, exploded_df in exploded_results.items():
        print(f"\n{'='*80}")
        print(f"ERROR ANALYSIS: {method_name.upper()} ({mode})")
        print(f"{'='*80}")

        errors = analyze_errors(ground_truth_df, exploded_df, match_mode=mode)
        error_analyses[mode][method_name] = errors

        if not errors["fp_by_type"].empty:
            print(f"\nFalse Positives by Entity Type:")
            display(errors["fp_by_type"])

        if not errors["top_fps"].empty:
            print(f"\nTop 25 False Positive Entities:")
            display(errors["top_fps"])

        if not errors["fn_by_type"].empty:
            print(f"\nFalse Negatives by GT Entity Type:")
            display(errors["fn_by_type"])

        if not errors["top_fns"].empty:
            print(f"\nTop 25 Missed Ground Truth Entities:")
            display(errors["top_fns"])

        if not errors["recall_by_type"].empty:
            print(f"\nRecall by Entity Type:")
            display(errors["recall_by_type"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Method Strengths and Weaknesses

# COMMAND ----------

for mode in MATCH_MODES:
    print(f"\n--- {mode.upper()} mode ---")
    strengths_df = summarize_method_strengths(error_analyses[mode], evaluation_results[mode])
    display(strengths_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Precision / Recall / F1 Comparison

# COMMAND ----------

for mode in MATCH_MODES:
    methods = list(evaluation_results[mode].keys())
    metrics_to_plot = ["precision", "recall", "f1_score"]
    x = np.arange(len(methods))
    width = 0.25

    fig, ax = plt.subplots(figsize=(10, 5))
    for i, metric in enumerate(metrics_to_plot):
        values = [evaluation_results[mode][m][metric] for m in methods]
        bars = ax.bar(x + i * width, values, width, label=metric.replace("_", " ").title())
        for bar, v in zip(bars, values):
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.01,
                    f"{v:.2f}", ha="center", va="bottom", fontsize=8)

    ax.set_ylabel("Score")
    ax.set_title(f"Detection Method Comparison ({mode})")
    ax.set_xticks(x + width)
    ax.set_xticklabels([m.capitalize() for m in methods])
    ax.legend()
    ax.set_ylim(0, 1.1)
    plt.tight_layout()
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recall by Entity Type Heatmap
# MAGIC
# MAGIC Shows which entity types each detection method catches well (green) vs. misses (red).

# COMMAND ----------

for mode in MATCH_MODES:
    matrix, entity_types, method_names = build_recall_matrix(error_analyses[mode])

    if len(entity_types) > 0:
        fig, ax = plt.subplots(figsize=(max(8, len(method_names) * 2.5), max(6, len(entity_types) * 0.45)))
        im = ax.imshow(matrix, cmap="RdYlGn", aspect="auto", vmin=0, vmax=1)

        ax.set_xticks(range(len(method_names)))
        ax.set_xticklabels([m.capitalize() for m in method_names])
        ax.set_yticks(range(len(entity_types)))
        ax.set_yticklabels(entity_types)
        ax.set_title(f"Recall by Entity Type ({mode})")

        for i in range(len(entity_types)):
            for j in range(len(method_names)):
                val = matrix[i, j]
                color = "black" if 0.3 < val < 0.8 else "white"
                ax.text(j, i, f"{val:.2f}", ha="center", va="center", color=color, fontsize=8)

        fig.colorbar(im, ax=ax, label="Recall", shrink=0.8)
        plt.tight_layout()
        plt.show()
    else:
        print(f"No entity type recall data available for {mode} mode.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## False Positive Distribution by Entity Type

# COMMAND ----------

for mode in MATCH_MODES:
    n_methods = len(error_analyses[mode])
    fig, axes = plt.subplots(1, max(n_methods, 1), figsize=(5 * max(n_methods, 1), 6), squeeze=False)

    for idx, (method, errors) in enumerate(error_analyses[mode].items()):
        ax = axes[0][idx]
        fp_data = errors["fp_by_type"].head(10)
        if not fp_data.empty:
            ax.barh(fp_data["entity_type"], fp_data["count"], color="salmon")
            ax.set_xlabel("Count")
            ax.set_title(f"{method.capitalize()} - Top FP Types ({mode})")
            ax.invert_yaxis()
        else:
            ax.text(0.5, 0.5, "No FPs", ha="center", va="center", transform=ax.transAxes)
            ax.set_title(f"{method.capitalize()} - FP Types ({mode})")

    plt.tight_layout()
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## False Negative Distribution by Entity Type

# COMMAND ----------

for mode in MATCH_MODES:
    n_methods = len(error_analyses[mode])
    fig, axes = plt.subplots(1, max(n_methods, 1), figsize=(5 * max(n_methods, 1), 6), squeeze=False)

    for idx, (method, errors) in enumerate(error_analyses[mode].items()):
        ax = axes[0][idx]
        fn_data = errors["fn_by_type"].head(10)
        if not fn_data.empty:
            type_col = "gt_entity_type" if "gt_entity_type" in fn_data.columns else fn_data.columns[0]
            ax.barh(fn_data[type_col], fn_data["count"], color="steelblue")
            ax.set_xlabel("Count")
            ax.set_title(f"{method.capitalize()} - Top FN Types ({mode})")
            ax.invert_yaxis()
        else:
            ax.text(0.5, 0.5, "No FNs", ha="center", va="center", transform=ax.transAxes)
            ax.set_title(f"{method.capitalize()} - FN Types ({mode})")

    plt.tight_layout()
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## TP / FP / FN Stacked Bar Chart

# COMMAND ----------

for mode in MATCH_MODES:
    methods = list(evaluation_results[mode].keys())
    fig, ax = plt.subplots(figsize=(10, 5))
    method_labels = [m.capitalize() for m in methods]
    tp_vals = [evaluation_results[mode][m]["true_positives"] for m in methods]
    fp_vals = [evaluation_results[mode][m]["false_positives"] for m in methods]
    fn_vals = [evaluation_results[mode][m]["false_negatives"] for m in methods]

    x = np.arange(len(methods))
    bar_width = 0.5

    ax.bar(x, tp_vals, bar_width, label="True Positives", color="forestgreen")
    ax.bar(x, fp_vals, bar_width, bottom=tp_vals, label="False Positives", color="salmon")
    ax.bar(x, fn_vals, bar_width, bottom=[t + f for t, f in zip(tp_vals, fp_vals)],
           label="False Negatives", color="steelblue")

    ax.set_ylabel("Count")
    ax.set_title(f"TP / FP / FN by Method ({mode})")
    ax.set_xticks(x)
    ax.set_xticklabels(method_labels)
    ax.legend()
    plt.tight_layout()
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log Results to stdout

# COMMAND ----------

print("[BENCHMARK_RESULTS] === EVALUATION METRICS ===")
for mode in MATCH_MODES:
    for method_name, metrics in evaluation_results[mode].items():
        print(
            f"[BENCHMARK_RESULTS] {method_name} ({mode}): "
            f"P={metrics['precision']:.3f} R={metrics['recall']:.3f} "
            f"F1={metrics['f1_score']:.3f} TP={metrics['true_positives']} "
            f"FP={metrics['false_positives']} FN={metrics['false_negatives']}"
        )

print("[BENCHMARK_RESULTS] === ERROR ANALYSIS (overlap) ===")
for method_name, errors in error_analyses.get("overlap", {}).items():
    print(f"[BENCHMARK_RESULTS] --- {method_name.upper()} ---")
    if not errors["fn_by_type"].empty:
        print(f"[BENCHMARK_RESULTS] FN by type:")
        for _, r in errors["fn_by_type"].head(10).iterrows():
            print(f"[BENCHMARK_RESULTS]   {r['gt_entity_type']}: {r['count']}")
    if not errors["top_fps"].empty:
        print(f"[BENCHMARK_RESULTS] Top FPs:")
        for _, r in errors["top_fps"].head(10).iterrows():
            etype = r.get("entity_type", "")
            print(f"[BENCHMARK_RESULTS]   {r['entity']} ({etype}): {r['count']}")
    if not errors["top_fns"].empty:
        print(f"[BENCHMARK_RESULTS] Top FNs:")
        for _, r in errors["top_fns"].head(10).iterrows():
            print(f"[BENCHMARK_RESULTS]   {r['chunk']}: {r['count']}")
    if not errors.get("recall_by_type", pd.DataFrame()).empty:
        print(f"[BENCHMARK_RESULTS] Recall by type:")
        for _, r in errors["recall_by_type"].iterrows():
            print(f"[BENCHMARK_RESULTS]   {r['gt_entity_type']}: {r['recall']:.3f} ({r['tp']}/{r['tp']+r['fn']})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Strict vs Overlap Boundary Diagnostic
# MAGIC
# MAGIC For each detection method, find entities that match in **overlap** mode but fail **strict**
# MAGIC containment.  Reports how many characters the detected span is off at the start / end,
# MAGIC which reveals whether the detector systematically clips entity boundaries.

# COMMAND ----------

from dbxredact import diagnose_strict_failures

for method_name, exploded_df in exploded_results.items():
    diag = diagnose_strict_failures(ground_truth_df, exploded_df)
    if diag.empty:
        print(f"{method_name.upper()}: No overlap-only failures (strict matches everything overlap does).")
        continue

    print(f"\n{'='*80}")
    print(f"BOUNDARY DIAGNOSTIC: {method_name.upper()}  ({len(diag)} overlap-only matches)")
    print(f"{'='*80}")

    # Distribution of boundary_type
    type_dist = diag["boundary_type"].value_counts()
    print(f"\nBoundary type distribution:")
    for bt, cnt in type_dist.items():
        print(f"  {bt}: {cnt}")

    # Average deltas
    print(f"\nMean start_delta: {diag['start_delta'].mean():.2f}  (positive = front-clipped)")
    print(f"Mean end_delta:   {diag['end_delta'].mean():.2f}  (positive = back-clipped)")

    # Show sample rows
    print(f"\nSample boundary failures (up to 20):")
    display(diag.head(20))

    print(f"\n[BENCHMARK_RESULTS] BOUNDARY_DIAG {method_name}: "
          f"total={len(diag)} "
          + " ".join(f"{bt}={cnt}" for bt, cnt in type_dist.items())
          + f" mean_start_delta={diag['start_delta'].mean():.2f}"
          + f" mean_end_delta={diag['end_delta'].mean():.2f}")
