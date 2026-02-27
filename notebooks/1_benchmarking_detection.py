# Databricks notebook source
# MAGIC %md
# MAGIC # PII/PHI Detection Benchmarking
# MAGIC
# MAGIC This notebook performs PHI/PII detection on a dataset using configurable detection methods.
# MAGIC
# MAGIC **Detection Methods:**
# MAGIC - **Presidio**: Rule-based and NLP-based detection using Microsoft Presidio
# MAGIC - **AI Query**: LLM-based detection using Databricks AI endpoints
# MAGIC - **GLiNER**: NER-based detection using GLiNER models
# MAGIC
# MAGIC **Outputs:**
# MAGIC - Detection results with entity positions and types
# MAGIC - Aligned entities (when using multiple methods)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install dbxredact
# MAGIC
# MAGIC When running via a Databricks Asset Bundle job, the wheel is attached as a cluster library automatically.
# MAGIC For interactive use, uncomment and update the `%pip install` line below.

# COMMAND ----------

# MAGIC %pip install https://github.com/explosion/spacy-models/releases/download/en_core_web_trf-3.8.0/en_core_web_trf-3.8.0-py3-none-any.whl
# MAGIC # Fallback if GPU/trf not available:
# MAGIC # %pip install https://github.com/explosion/spacy-models/releases/download/en_core_web_lg-3.8.0/en_core_web_lg-3.8.0-py3-none-any.whl
# MAGIC # %pip install https://github.com/explosion/spacy-models/releases/download/es_core_news_lg-3.8.0/es_core_news_lg-3.8.0-py3-none-any.whl
# MAGIC # For interactive use (not running via DAB job), also uncomment one of the following:
# MAGIC # %pip install /Workspace/<path-to-bundle>/artifacts/dbxredact-0.1.0-py3-none-any.whl
# MAGIC # %pip install git+https://github.com/databricks-industry-solutions/dbxredact.git
# MAGIC %restart_python

# COMMAND ----------

import os
from pyspark.sql.functions import col

from dbxredact import run_detection_pipeline, load_filter_from_table, EntityFilter

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget Configuration

# COMMAND ----------

dbutils.widgets.text(
    name="source_table",
    defaultValue="your_catalog.your_schema.jsl_benchmark_source",
    label="0. Source Table",
)
dbutils.widgets.text(
    name="doc_id_column", defaultValue="doc_id", label="1. Document ID Column"
)
dbutils.widgets.text(name="text_column", defaultValue="text", label="2. Text Column")
dbutils.widgets.dropdown(
    name="use_presidio",
    defaultValue="true",
    choices=["true", "false"],
    label="3. Use Presidio Detection",
)
dbutils.widgets.dropdown(
    name="use_ai_query",
    defaultValue="true",
    choices=["true", "false"],
    label="4. Use AI Query Detection",
)
dbutils.widgets.dropdown(
    name="use_gliner",
    defaultValue="false",
    choices=["true", "false"],
    label="5. Use GLiNER Detection",
)
dbutils.widgets.text(
    name="endpoint",
    defaultValue="databricks-gpt-oss-120b",
    label="6. AI Endpoint (for AI Query method)",
)
dbutils.widgets.text(
    name="presidio_score_threshold",
    defaultValue="0.5",
    label="7. Presidio Score Threshold",
)
dbutils.widgets.text(name="num_cores", defaultValue="10", label="8. Number of Cores")
dbutils.widgets.text(
    name="output_table",
    defaultValue="",
    label="9. Output Table (leave blank for auto-suffix)",
)
dbutils.widgets.dropdown(
    name="alignment_mode",
    defaultValue="union",
    choices=["union", "consensus"],
    label="10. Alignment Mode (union=recall, consensus=precision)",
)
dbutils.widgets.text(
    name="max_rows",
    defaultValue="10000",
    label="11. Max Rows (0 for unlimited)",
)
dbutils.widgets.text(
    name="safe_list_table",
    defaultValue="",
    label="12. Safe List Table (optional, fully qualified)",
)
dbutils.widgets.text(
    name="block_list_table",
    defaultValue="",
    label="13. Block List Table (optional, fully qualified)",
)

source_table = dbutils.widgets.get("source_table")
doc_id_column = dbutils.widgets.get("doc_id_column")
text_column = dbutils.widgets.get("text_column")

if "your_catalog" in source_table or "your_schema" in source_table:
    raise ValueError(
        "Please update the 'source_table' widget with your actual catalog and schema names. "
        "The defaults (your_catalog.your_schema) are placeholders."
    )
use_presidio = dbutils.widgets.get("use_presidio") == "true"
use_ai_query = dbutils.widgets.get("use_ai_query") == "true"
use_gliner = dbutils.widgets.get("use_gliner") == "true"
endpoint = dbutils.widgets.get("endpoint")
score_threshold = float(dbutils.widgets.get("presidio_score_threshold"))
num_cores = int(dbutils.widgets.get("num_cores"))
output_table = dbutils.widgets.get("output_table")
alignment_mode = dbutils.widgets.get("alignment_mode")
max_rows_str = dbutils.widgets.get("max_rows")
max_rows = None if max_rows_str == "0" else int(max_rows_str)
safe_list_table = dbutils.widgets.get("safe_list_table").strip()
block_list_table = dbutils.widgets.get("block_list_table").strip()

entity_filter = None
if safe_list_table or block_list_table:
    ef = EntityFilter()
    if safe_list_table:
        safe_ef = load_filter_from_table(spark, safe_list_table, list_type="safe")
        ef.safe_list, ef.safe_patterns = safe_ef.safe_list, safe_ef.safe_patterns
        ef._safe_set, ef._safe_re = safe_ef._safe_set, safe_ef._safe_re
        print(f"Loaded safe list: {len(ef.safe_list)} exact, {len(ef.safe_patterns)} patterns")
    if block_list_table:
        block_ef = load_filter_from_table(spark, block_list_table, list_type="block")
        ef.block_list, ef.block_patterns = block_ef.block_list, block_ef.block_patterns
        ef._block_set, ef._block_re = block_ef._block_set, block_ef._block_re
        print(f"Loaded block list: {len(ef.block_list)} exact, {len(ef.block_patterns)} patterns")
    entity_filter = ef

if not any([use_presidio, use_ai_query, use_gliner]):
    raise ValueError("At least one detection method must be enabled.")
if not 0.0 <= score_threshold <= 1.0:
    raise ValueError(f"Presidio score threshold must be in [0.0, 1.0], got {score_threshold}")
if num_cores < 1:
    raise ValueError(f"Number of cores must be a positive integer, got {num_cores}")

if not output_table:
    output_table = f"{source_table}_detection_results"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbr_version = os.environ.get("DATABRICKS_RUNTIME_VERSION", "")
if "client" not in dbr_version:
    spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", 100)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Source Data

# COMMAND ----------

_source_columns = [c.name for c in spark.table(source_table).schema]
for _col in [doc_id_column, text_column]:
    if _col not in _source_columns:
        raise ValueError(f"Column '{_col}' not found in {source_table}. Available: {_source_columns}")

source_df = spark.table(source_table).select(doc_id_column, col(text_column)).distinct()
source_df_count = source_df.count()
print(f"Source has {source_df_count} distinct documents from {source_table}")

if max_rows and source_df_count > max_rows:
    print(f"WARNING: Limiting to {max_rows:,} rows (set max_rows=0 for unlimited).")
    source_df = source_df.limit(max_rows)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Detection Pipeline

# COMMAND ----------

results_df = run_detection_pipeline(
    spark=spark,
    source_df=source_df,
    doc_id_column=doc_id_column,
    text_column=text_column,
    use_presidio=use_presidio,
    use_ai_query=use_ai_query,
    use_gliner=use_gliner,
    endpoint=endpoint if use_ai_query else None,
    score_threshold=score_threshold,
    num_cores=num_cores,
    align_results=True,
    alignment_mode=alignment_mode,
    entity_filter=entity_filter,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Results

# COMMAND ----------

results_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
    output_table
)
print(f"Results saved to table: {output_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Results

# COMMAND ----------

results_df = spark.read.table(output_table)
display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results Summary

# COMMAND ----------

if use_presidio and "presidio_results_struct" in results_df.columns:
    print("=== Presidio Results ===")
    presidio_summary = results_df.selectExpr(
        f"{doc_id_column}", "SIZE(presidio_results_struct) as entity_count"
    )
    display(presidio_summary)

# COMMAND ----------

if use_ai_query and "ai_results_struct" in results_df.columns:
    print("=== AI Query Results ===")
    ai_summary = results_df.selectExpr(
        f"{doc_id_column}", "SIZE(ai_results_struct) as entity_count"
    )
    display(ai_summary)

# COMMAND ----------

if use_gliner and "gliner_results_struct" in results_df.columns:
    print("=== GLiNER Results ===")
    gliner_summary = results_df.selectExpr(
        f"{doc_id_column}", "SIZE(gliner_results_struct) as entity_count"
    )
    display(gliner_summary)

# COMMAND ----------

if "aligned_entities" in results_df.columns:
    print("=== Aligned Results ===")
    aligned_summary = results_df.selectExpr(
        f"{doc_id_column}", "SIZE(aligned_entities) as entity_count"
    )
    display(aligned_summary)
