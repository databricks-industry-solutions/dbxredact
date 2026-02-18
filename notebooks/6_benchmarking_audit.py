# Databricks notebook source
# MAGIC %md
# MAGIC # Benchmark Audit
# MAGIC
# MAGIC Consolidates evaluation metrics and judge grades into a single audit table
# MAGIC that tracks performance across prompt versions, model versions, and config changes.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install dbxredact

# COMMAND ----------

# MAGIC # For interactive use, uncomment one of the following:
# MAGIC # %pip install /Workspace/<path-to-bundle>/artifacts/dbxredact-0.1.0-py3-none-any.whl
# MAGIC # %pip install git+https://github.com/databricks-industry-solutions/dbxredact.git
# MAGIC # %restart_python

# COMMAND ----------

import json
import datetime
import pandas as pd
from pyspark.sql.functions import col

from dbxredact.config import (
    PROMPT_VERSION,
    DEFAULT_GLINER_MODEL,
    DEFAULT_GLINER_THRESHOLD,
    DEFAULT_PRESIDIO_SCORE_THRESHOLD,
    DEFAULT_AI_REASONING_EFFORT,
)
from dbxredact.judge import compute_judge_summary

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget Configuration

# COMMAND ----------

dbutils.widgets.text(
    name="evaluation_table",
    defaultValue="your_catalog.your_schema.phi_evaluation_results",
    label="0. Evaluation Results Table",
)
dbutils.widgets.text(
    name="judge_table",
    defaultValue="your_catalog.your_schema.jsl_benchmark_judge_results",
    label="1. Judge Results Table",
)
dbutils.widgets.text(
    name="audit_table",
    defaultValue="your_catalog.your_schema.phi_benchmark_audit",
    label="2. Audit Output Table",
)
dbutils.widgets.text(
    name="presidio_spacy_model",
    defaultValue="en_core_web_trf",
    label="3. Presidio spaCy Model",
)

evaluation_table = dbutils.widgets.get("evaluation_table")
judge_table = dbutils.widgets.get("judge_table")
audit_table = dbutils.widgets.get("audit_table")
presidio_spacy_model = dbutils.widgets.get("presidio_spacy_model")

for _table in [evaluation_table, judge_table, audit_table]:
    if "your_catalog" in _table or "your_schema" in _table:
        raise ValueError("Please update table widgets with actual catalog/schema names.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Evaluation Metrics

# COMMAND ----------

eval_df = spark.table(evaluation_table)
print(f"Evaluation records: {eval_df.count()}")

# Get the latest run_id
latest_run_id = (
    eval_df.orderBy(col("timestamp").desc())
    .select("run_id")
    .first()["run_id"]
)
print(f"Latest run_id: {latest_run_id}")

latest_eval = eval_df.where(col("run_id") == latest_run_id)
display(latest_eval)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pivot Evaluation Metrics Per Method

# COMMAND ----------

eval_pd = latest_eval.toPandas()

# method_name may have _strict / _overlap suffix from evaluation notebook
# Extract base method and match_mode
def parse_method(method_name):
    for suffix in ("_strict", "_overlap"):
        if method_name.endswith(suffix):
            return method_name[: -len(suffix)], suffix[1:]
    return method_name, "unknown"

eval_pd[["base_method", "match_mode"]] = eval_pd["method_name"].apply(
    lambda x: pd.Series(parse_method(x))
)

metrics_pivot = eval_pd.pivot_table(
    index=["base_method", "match_mode", "run_id"],
    columns="metric_name",
    values="metric_value",
).reset_index()

print("Pivoted metrics:")
display(spark.createDataFrame(metrics_pivot))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Judge Summaries

# COMMAND ----------

judge_df = spark.table(judge_table)
print(f"Judge records: {judge_df.count()}")

methods_in_judge = [r["method"] for r in judge_df.select("method").distinct().collect()]
print(f"Methods in judge: {methods_in_judge}")

judge_summaries = {}
for method in methods_in_judge:
    method_df = judge_df.where(col("method") == method)
    judge_summaries[method] = compute_judge_summary(method_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Audit Rows

# COMMAND ----------

now = datetime.datetime.now()

audit_rows = []
for _, row in metrics_pivot.iterrows():
    base_method = row.get("base_method", "")
    match_mode = row.get("match_mode", "")
    run_id = row.get("run_id", latest_run_id)

    js = judge_summaries.get(base_method, {})

    audit_rows.append({
        "run_id": str(run_id),
        "timestamp": now,
        "match_mode": match_mode,
        "method": base_method,
        "prompt_version": PROMPT_VERSION,
        "gliner_model": DEFAULT_GLINER_MODEL,
        "gliner_threshold": float(DEFAULT_GLINER_THRESHOLD),
        "presidio_spacy_model": presidio_spacy_model,
        "score_threshold": float(DEFAULT_PRESIDIO_SCORE_THRESHOLD),
        "reasoning_effort": DEFAULT_AI_REASONING_EFFORT,
        "precision": float(row.get("precision", 0)),
        "recall": float(row.get("recall", 0)),
        "f1_score": float(row.get("f1_score", 0)),
        "judge_pass_rate": float(js.get("pass_rate", 0)),
        "judge_partial_rate": float(js.get("partial_rate", 0)),
        "judge_fail_rate": float(js.get("fail_rate", 0)),
        "top_missed_entities": json.dumps(js.get("top_missed", [])[:10]),
    })

audit_pd = pd.DataFrame(audit_rows)
print(f"Audit rows: {len(audit_pd)}")
display(audit_pd)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Audit Table

# COMMAND ----------

audit_spark_df = spark.createDataFrame(audit_pd)
audit_spark_df.write.mode("append").option("mergeSchema", "true").saveAsTable(
    audit_table
)
print(f"Audit data appended to {audit_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Audit History

# COMMAND ----------

full_audit = spark.table(audit_table).orderBy(col("timestamp").desc())
display(full_audit)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trend: F1 Score Over Time

# COMMAND ----------

import matplotlib.pyplot as plt

trend_pd = full_audit.select(
    "timestamp", "method", "match_mode", "f1_score"
).toPandas()

if not trend_pd.empty:
    fig, ax = plt.subplots(figsize=(12, 5))
    for key, grp in trend_pd.groupby(["method", "match_mode"]):
        label = f"{key[0]} ({key[1]})"
        ax.plot(grp["timestamp"], grp["f1_score"], marker="o", label=label)
    ax.set_ylabel("F1 Score")
    ax.set_title("F1 Score Trend by Method and Match Mode")
    ax.legend(bbox_to_anchor=(1.05, 1), loc="upper left")
    plt.tight_layout()
    plt.show()
else:
    print("No audit data available for trend visualization.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log Results to stdout

# COMMAND ----------

print("[BENCHMARK_RESULTS] === AUDIT SUMMARY ===")
for _, row in audit_pd.iterrows():
    print(
        f"[BENCHMARK_RESULTS] {row['method']} ({row['match_mode']}): "
        f"P={row['precision']:.3f} R={row['recall']:.3f} F1={row['f1_score']:.3f} "
        f"Judge PASS={row['judge_pass_rate']:.1%} PARTIAL={row['judge_partial_rate']:.1%} FAIL={row['judge_fail_rate']:.1%}"
    )
