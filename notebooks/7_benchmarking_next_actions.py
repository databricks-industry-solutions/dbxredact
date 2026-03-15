# Databricks notebook source
# MAGIC %md
# MAGIC # Next Best Action Recommender
# MAGIC
# MAGIC Reads the latest audit data, FP/FN patterns, and entity-type recall matrix,
# MAGIC then uses AI Query to recommend the top improvements for each detection method.

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
from pyspark.sql.functions import col

from dbxredact.judge import run_next_action_query

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget Configuration

# COMMAND ----------

dbutils.widgets.text(
    name="audit_table",
    defaultValue="your_catalog.your_schema.phi_benchmark_audit",
    label="0. Audit Table",
)
dbutils.widgets.text(
    name="evaluation_table",
    defaultValue="your_catalog.your_schema.phi_evaluation_results",
    label="1. Evaluation Results Table",
)
dbutils.widgets.text(
    name="endpoint",
    defaultValue="databricks-gpt-oss-120b",
    label="2. AI Query Endpoint",
)
dbutils.widgets.text(
    name="output_table",
    defaultValue="your_catalog.your_schema.phi_improvement_recommendations",
    label="3. Recommendations Output Table",
)

audit_table = dbutils.widgets.get("audit_table")
evaluation_table = dbutils.widgets.get("evaluation_table")
endpoint = dbutils.widgets.get("endpoint")
output_table = dbutils.widgets.get("output_table")

for _table in [audit_table, evaluation_table, output_table]:
    if "your_catalog" in _table or "your_schema" in _table:
        raise ValueError("Please update table widgets with actual catalog/schema names.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Latest Audit Data

# COMMAND ----------

audit_df = spark.table(audit_table)
latest_run_id = (
    audit_df.orderBy(col("timestamp").desc())
    .select("run_id")
    .first()["run_id"]
)
print(f"Latest audit run_id: {latest_run_id}")

latest_audit = audit_df.where(col("run_id") == latest_run_id)
audit_pd = latest_audit.toPandas()
display(audit_pd)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load FP/FN from Evaluation

# COMMAND ----------

eval_df = spark.table(evaluation_table)
latest_eval = eval_df.where(col("run_id") == latest_run_id).toPandas()

# Build per-method metric summaries
method_metrics = {}
for _, row in latest_eval.iterrows():
    mn = row["method_name"]
    if mn not in method_metrics:
        method_metrics[mn] = {}
    method_metrics[mn][row["metric_name"]] = row["metric_value"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Context for AI Recommender

# COMMAND ----------

context_parts = []

# Audit summary
context_parts.append("=== AUDIT SUMMARY (latest run) ===")
for _, row in audit_pd.iterrows():
    context_parts.append(
        f"Method: {row['method']} | Mode: {row['match_mode']} | "
        f"P: {row['precision']:.3f} | R: {row['recall']:.3f} | F1: {row['f1_score']:.3f} | "
        f"Judge PASS: {row['judge_pass_rate']:.1%} | PARTIAL: {row['judge_partial_rate']:.1%} | FAIL: {row['judge_fail_rate']:.1%}"
    )
    if row.get("top_missed_entities"):
        context_parts.append(f"  Top missed: {row['top_missed_entities']}")

# Per-method evaluation metrics
context_parts.append("\n=== EVALUATION METRICS ===")
for method, metrics in method_metrics.items():
    context_parts.append(f"{method}: {json.dumps(metrics, indent=2)}")

# Config
context_parts.append("\n=== CURRENT CONFIG ===")
for _, row in audit_pd.head(1).iterrows():
    context_parts.append(f"GLiNER model: {row['gliner_model']}")
    context_parts.append(f"GLiNER threshold: {row['gliner_threshold']}")
    context_parts.append(f"Presidio spaCy: {row['presidio_spacy_model']}")
    context_parts.append(f"Score threshold: {row['score_threshold']}")
    context_parts.append(f"Reasoning effort: {row['reasoning_effort']}")
    context_parts.append(f"Prompt version: {row['prompt_version']}")

context = "\n".join(context_parts)
print(context[:2000])
print(f"... ({len(context)} chars total)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Call AI Recommender

# COMMAND ----------

raw_recommendations = run_next_action_query(
    spark=spark,
    context=context,
    endpoint=endpoint,
    reasoning_effort="high",
)

print("Raw AI response:")
print(raw_recommendations)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse and Display Recommendations

# COMMAND ----------

import datetime
import pandas as pd

try:
    recs = json.loads(raw_recommendations)
    if not isinstance(recs, list):
        recs = [recs]
except (json.JSONDecodeError, TypeError):
    recs = [{"priority": 1, "method": "all", "action": raw_recommendations, "rationale": ""}]

recs_pd = pd.DataFrame(recs)
recs_pd["run_id"] = latest_run_id
recs_pd["timestamp"] = datetime.datetime.now()

display(recs_pd)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Recommendations

# COMMAND ----------

recs_spark = spark.createDataFrame(recs_pd)
recs_spark.write.mode("append").option("mergeSchema", "true").saveAsTable(output_table)
print(f"Recommendations saved to {output_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recommendation History

# COMMAND ----------

try:
    all_recs = spark.table(output_table).orderBy(col("timestamp").desc())
    display(all_recs)
except Exception as e:
    print(f"Could not load recommendations table: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log Results to stdout

# COMMAND ----------

print("[BENCHMARK_RESULTS] === NEXT BEST ACTIONS ===")
for _, row in recs_pd.iterrows():
    priority = row.get("priority", "?")
    method = row.get("method", "?")
    action = str(row.get("action", ""))[:300]
    rationale = str(row.get("rationale", ""))[:300]
    print(f"[BENCHMARK_RESULTS] P{priority} [{method}] {action}")
    if rationale:
        print(f"[BENCHMARK_RESULTS]   Rationale: {rationale}")
