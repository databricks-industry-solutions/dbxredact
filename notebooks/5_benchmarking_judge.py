# Databricks notebook source
# MAGIC %md
# MAGIC # AI Judge -- Redaction Quality Grading
# MAGIC
# MAGIC Uses AI Query to review each method's redacted output and identify
# MAGIC **MISSED** or **PARTIALLY_MISSED** PII.
# MAGIC
# MAGIC **Grades:**
# MAGIC - **PASS**: No identifiable PHI remains in the redacted text. The document is safe.
# MAGIC - **PARTIAL**: Some PHI was missed but the majority was redacted. Typically 1-3
# MAGIC   entities remain (e.g., a date or partial name).
# MAGIC - **FAIL**: Significant PHI remains -- multiple names, dates, or identifiers are
# MAGIC   still visible.
# MAGIC
# MAGIC A low PASS rate is expected on clinical notes, which are dense with PHI. A method
# MAGIC with 90%+ recall may still leave 1-2 entities in most documents, resulting in
# MAGIC PARTIAL grades. Focus on reducing FAIL rates first, then PARTIAL.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install dbxredact

# COMMAND ----------

# MAGIC # For interactive use, uncomment one of the following:
# MAGIC # %pip install /Workspace/<path-to-bundle>/artifacts/dbxredact-0.1.2-py3-none-any.whl
# MAGIC # %pip install git+https://github.com/databricks-industry-solutions/dbxredact.git
# MAGIC # %restart_python

# COMMAND ----------

import json
from functools import reduce
from pyspark.sql.functions import col, lit
from pyspark.sql import DataFrame

from dbxredact.judge import run_judge_evaluation, compute_judge_summary

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget Configuration

# COMMAND ----------

dbutils.widgets.text(
    name="redacted_table",
    defaultValue="your_catalog.your_schema.jsl_benchmark_redacted",
    label="0. Redacted Table (from notebook 3)",
)
dbutils.widgets.text(
    name="text_column", defaultValue="text", label="1. Original Text Column"
)
dbutils.widgets.text(
    name="endpoint",
    defaultValue="databricks-gpt-oss-120b",
    label="2. AI Query Endpoint",
)
dbutils.widgets.text(
    name="output_table",
    defaultValue="your_catalog.your_schema.jsl_benchmark_judge_results",
    label="3. Judge Output Table",
)
dbutils.widgets.text(
    name="judge_methods",
    defaultValue="aligned",
    label="4. Methods to Judge (comma-separated, or 'all')",
)

redacted_table = dbutils.widgets.get("redacted_table")
text_column = dbutils.widgets.get("text_column")
endpoint = dbutils.widgets.get("endpoint")
output_table = dbutils.widgets.get("output_table")
judge_methods_str = dbutils.widgets.get("judge_methods")

for _table in [redacted_table, output_table]:
    if "your_catalog" in _table or "your_schema" in _table:
        raise ValueError("Please update table widgets with actual catalog/schema names.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Redacted Data

# COMMAND ----------

redacted_df = spark.table(redacted_table)
print(f"Loaded {redacted_df.count()} records from {redacted_table}")

REDACTED_COLS = {
    "presidio": "text_redacted_presidio",
    "ai": "text_redacted_ai",
    "gliner": "text_redacted_gliner",
    "aligned": "text_redacted_aligned",
}

available_methods = {
    m: c for m, c in REDACTED_COLS.items() if c in redacted_df.columns
}

if judge_methods_str.strip().lower() == "all":
    methods_to_judge = available_methods
else:
    requested = {m.strip() for m in judge_methods_str.split(",")}
    methods_to_judge = {m: c for m, c in available_methods.items() if m in requested}

print(f"Available methods: {list(available_methods.keys())}")
print(f"Methods to judge:  {list(methods_to_judge.keys())}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Judge for Each Method

# COMMAND ----------

judge_dfs = []

for method, redacted_col in methods_to_judge.items():
    print(f"\nJudging: {method.upper()}")
    jdf = run_judge_evaluation(
        spark=spark,
        df=redacted_df,
        original_text_col=text_column,
        redacted_text_col=redacted_col,
        endpoint=endpoint,
        method_name=method,
    )
    judge_dfs.append(jdf)
    print(f"  Done -- {jdf.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Combine and Save

# COMMAND ----------

all_judge_df = reduce(DataFrame.unionByName, judge_dfs)

all_judge_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
    output_table
)
print(f"Judge results saved to {output_table}")

# Read from saved table to avoid re-executing lazy AI Query calls
all_judge_df = spark.table(output_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Per Method

# COMMAND ----------

summaries = {}

for method in methods_to_judge:
    method_df = all_judge_df.where(col("method") == method)
    summary = compute_judge_summary(method_df)
    summaries[method] = summary

    print(f"\n{'='*60}")
    print(f"  {method.upper()}")
    print(f"{'='*60}")
    print(f"  PASS:    {summary['pass_rate']:.1%}")
    print(f"  PARTIAL: {summary['partial_rate']:.1%}")
    print(f"  FAIL:    {summary['fail_rate']:.1%}")
    print(f"  Docs:    {summary['total_docs']}")
    if summary["top_missed"]:
        print(f"  Top missed types: {json.dumps(summary['top_missed'][:5], indent=2)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grade Distribution

# COMMAND ----------

display(
    all_judge_df.groupBy("method", "grade")
    .count()
    .orderBy("method", "grade")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Findings

# COMMAND ----------

from pyspark.sql.functions import explode_outer, size as spark_size

findings_df = (
    all_judge_df.where(spark_size("findings") > 0)
    .select("doc_id", "method", "grade", explode_outer("findings").alias("f"))
    .select("doc_id", "method", "grade", "f.entity", "f.entity_type", "f.status", "f.explanation")
)
display(findings_df.limit(30))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log Results to stdout

# COMMAND ----------

print("[BENCHMARK_RESULTS] === JUDGE SUMMARY ===")
for method, summary in summaries.items():
    print(f"[BENCHMARK_RESULTS] {method}: PASS={summary['pass_rate']:.1%} PARTIAL={summary['partial_rate']:.1%} FAIL={summary['fail_rate']:.1%} docs={summary['total_docs']}")
    if summary["top_missed"]:
        print(f"[BENCHMARK_RESULTS]   top_missed: {json.dumps(summary['top_missed'][:5])}")

partial_fail_rows = (
    all_judge_df.where(col("grade").isin("PARTIAL", "FAIL"))
    .select("doc_id", "method", "grade", explode_outer("findings").alias("f"))
    .select("doc_id", "method", "grade", "f.entity", "f.entity_type", "f.status")
    .collect()
)
if partial_fail_rows:
    print("[BENCHMARK_RESULTS] === PARTIAL/FAIL DETAILS (up to 20) ===")
    for row in partial_fail_rows[:20]:
        print(
            f"[BENCHMARK_RESULTS]   {row['method']} | {row['grade']} | doc={row['doc_id']} | "
            f"{row['entity_type']}: {row['entity']} ({row['status']})"
        )
