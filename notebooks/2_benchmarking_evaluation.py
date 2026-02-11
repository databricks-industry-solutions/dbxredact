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

import pandas as pd
from pyspark.sql.functions import col, explode

from dbxredact import (
    evaluate_detection,
    calculate_metrics,
    format_contingency_table,
    format_metrics_summary,
    save_evaluation_results,
    compare_methods_across_datasets,
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

# COMMAND ----------

evaluation_results = {}

for method_name, exploded_df in exploded_results.items():
    print(f"\n{'='*80}")
    print(f"Evaluating: {method_name.upper()}")
    print(f"{'='*80}")

    eval_df = evaluate_detection(ground_truth_df, exploded_df)
    metrics = calculate_metrics(eval_df, all_tokens)
    evaluation_results[method_name] = metrics

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
        method_name=method_name,
        output_table=evaluation_output_table,
        mode=write_mode,
    )

    print(f"\nResults saved to {evaluation_output_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compare Methods

# COMMAND ----------

comparison_data = {
    "Method": [],
    "Precision": [],
    "Recall": [],
    "F1 Score": [],
    "Accuracy": [],
}

for method_name, metrics in evaluation_results.items():
    comparison_data["Method"].append(method_name.capitalize())
    comparison_data["Precision"].append(metrics["precision"])
    comparison_data["Recall"].append(metrics["recall"])
    comparison_data["F1 Score"].append(metrics["f1_score"])
    comparison_data["Accuracy"].append(metrics["accuracy"])

comparison_df = pd.DataFrame(comparison_data)
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
# MAGIC ## False Negatives Analysis

# COMMAND ----------

if "aligned" in exploded_results:
    aligned_eval_df = evaluate_detection(ground_truth_df, exploded_results["aligned"])

    print("Top Missed Entities (False Negatives):")
    false_negatives = (
        aligned_eval_df.where(col("entity").isNull())
        .select("chunk")
        .groupBy("chunk")
        .count()
        .orderBy(col("count").desc())
    )
    display(false_negatives.limit(20))
