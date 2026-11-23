# Databricks notebook source
# MAGIC %md
# MAGIC # Ingestion Redaction Pipeline (AutoLoader)
# MAGIC
# MAGIC Redacts PII from files landing in a UC Volume path using AutoLoader (Structured Streaming).
# MAGIC Raw PII never persists -- files are read, redacted in-flight, and written to a Delta output table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install dbxredact
# MAGIC
# MAGIC When running via a Databricks Asset Bundle job, the wheel is attached as a cluster library automatically.
# MAGIC For interactive use, uncomment and update the `%pip install` line below.

# COMMAND ----------

# MAGIC # For interactive use (not running via DAB job):
# MAGIC # %pip install /Workspace/<path-to-bundle>/artifacts/dbxredact-0.3.1-py3-none-any.whl
# MAGIC # dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("source_path", "", "Source Volume Path")
dbutils.widgets.dropdown("file_format", "json", ["json", "csv", "parquet", "text"], "File Format")
dbutils.widgets.text("output_table", "", "Output Table (catalog.schema.table)")
dbutils.widgets.text("text_columns", "", "Text Columns (comma-sep)")
dbutils.widgets.text("structured_columns", "", "Structured Columns JSON (optional)")
dbutils.widgets.dropdown("masking_strategy", "mask", ["mask", "hash", "encrypt"], "Masking Strategy")
dbutils.widgets.text("doc_id_column", "doc_id", "Doc ID Column")
dbutils.widgets.text("checkpoint_path", "", "Checkpoint Path (optional)")
dbutils.widgets.text("schema_location", "", "Schema Location (optional)")
dbutils.widgets.text("max_files_per_trigger", "50", "Max Files Per Trigger")
dbutils.widgets.text("trigger_processing_time", "", "Trigger Processing Time (e.g. '30 seconds', empty=availableNow)")
dbutils.widgets.dropdown("multi_column_strategy", "sequential", ["sequential", "concat"], "Multi-Column Strategy")
dbutils.widgets.text("encryption_secret", "", "Encryption Secret (for encrypt strategy)")

# Detection params
dbutils.widgets.dropdown("use_presidio", "true", ["true", "false"], "Use Presidio")
dbutils.widgets.dropdown("use_ai_query", "true", ["true", "false"], "Use AI Query")
dbutils.widgets.dropdown("use_gliner", "false", ["true", "false"], "Use GLiNER")
dbutils.widgets.text("endpoint", "databricks-gpt-oss-120b", "AI Query Endpoint")
dbutils.widgets.text("score_threshold", "0.5", "Score Threshold")
dbutils.widgets.dropdown("redaction_strategy", "generic", ["generic", "typed"], "Redaction Strategy")
dbutils.widgets.dropdown("alignment_mode", "union", ["union", "consensus"], "Alignment Mode")
dbutils.widgets.text("gliner_model", "urchade/gliner_multi_pii-v1", "GLiNER Model")
dbutils.widgets.text("gliner_threshold", "0.2", "GLiNER Threshold")

# COMMAND ----------

import json
from dbxredact.pipeline import run_ingestion_redaction_pipeline
from dbxredact.config import RedactionConfig

source_path = dbutils.widgets.get("source_path")
file_format = dbutils.widgets.get("file_format")
output_table = dbutils.widgets.get("output_table")
text_columns_raw = dbutils.widgets.get("text_columns")
structured_columns_raw = dbutils.widgets.get("structured_columns")
masking_strategy = dbutils.widgets.get("masking_strategy")
doc_id_column = dbutils.widgets.get("doc_id_column")
checkpoint_path = dbutils.widgets.get("checkpoint_path") or None
schema_location = dbutils.widgets.get("schema_location") or None
max_files_per_trigger = int(dbutils.widgets.get("max_files_per_trigger") or 50)
trigger_processing_time = dbutils.widgets.get("trigger_processing_time") or None
multi_column_strategy = dbutils.widgets.get("multi_column_strategy")
encryption_secret = dbutils.widgets.get("encryption_secret") or None

use_presidio = dbutils.widgets.get("use_presidio").lower() == "true"
use_ai_query = dbutils.widgets.get("use_ai_query").lower() == "true"
use_gliner = dbutils.widgets.get("use_gliner").lower() == "true"
endpoint = dbutils.widgets.get("endpoint")
score_threshold = float(dbutils.widgets.get("score_threshold") or 0.5)
redaction_strategy = dbutils.widgets.get("redaction_strategy")
alignment_mode = dbutils.widgets.get("alignment_mode")
gliner_model = dbutils.widgets.get("gliner_model")
gliner_threshold = float(dbutils.widgets.get("gliner_threshold") or 0.2)

assert source_path, "source_path is required"
assert output_table, "output_table is required"
assert text_columns_raw, "text_columns is required (comma-separated)"

text_columns = [c.strip() for c in text_columns_raw.split(",") if c.strip()]
structured_columns = json.loads(structured_columns_raw) if structured_columns_raw else None

print(f"Source: {source_path} ({file_format})")
print(f"Output: {output_table}")
print(f"Text columns: {text_columns}")
print(f"Strategy: {multi_column_strategy}, Masking: {masking_strategy}")

# COMMAND ----------

config = RedactionConfig(
    use_presidio=use_presidio,
    use_ai_query=use_ai_query,
    use_gliner=use_gliner,
    endpoint=endpoint,
    score_threshold=score_threshold,
    redaction_strategy=redaction_strategy,
    alignment_mode=alignment_mode,
    gliner_model=gliner_model,
    gliner_threshold=gliner_threshold,
)

query = run_ingestion_redaction_pipeline(
    spark=spark,
    source_path=source_path,
    file_format=file_format,
    output_table=output_table,
    text_columns=text_columns,
    structured_columns=structured_columns,
    masking_strategy=masking_strategy,
    encryption_key=encryption_secret,
    multi_column_strategy=multi_column_strategy,
    doc_id_column=doc_id_column,
    checkpoint_path=checkpoint_path,
    schema_location=schema_location,
    max_files_per_trigger=max_files_per_trigger,
    trigger_processing_time=trigger_processing_time,
    config=config,
)

# COMMAND ----------

query.awaitTermination()
print(f"Ingestion pipeline completed. Results in: {output_table}")
