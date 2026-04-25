# Databricks notebook source
# MAGIC %md
# MAGIC # dbxredact Quickstart
# MAGIC
# MAGIC > **Library usage:** This notebook demonstrates using dbxredact as a **pip-installable library**.
# MAGIC > It is intended for embedding dbxredact into your own projects or for ad-hoc use.
# MAGIC > For the full deployment with the interactive dashboard and pre-configured jobs,
# MAGIC > see the main repo [README](https://github.com/databricks-industry-solutions/dbxredact) and `deploy.sh`.
# MAGIC
# MAGIC Detect and redact PII/PHI in a Delta table using an ensemble of detectors
# MAGIC (Presidio NER + Databricks AI Query), then write clean output.
# MAGIC
# MAGIC **Prerequisites:** A Databricks workspace with Unity Catalog enabled,
# MAGIC a foundation model endpoint, and a source table with a text column.

# COMMAND ----------
# MAGIC %pip install -qqq git+https://github.com/databricks-industry-solutions/dbxredact.git@main
# MAGIC %pip install -qqq https://github.com/explosion/spacy-models/releases/download/en_core_web_lg-3.8.0/en_core_web_lg-3.8.0-py3-none-any.whl
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Configuration
# MAGIC Fill in the widgets above after running this cell.

# COMMAND ----------
dbutils.widgets.text("source_table", "", "Source Table (catalog.schema.table)")
dbutils.widgets.text("text_column", "text", "Text Column")
dbutils.widgets.text("doc_id_column", "doc_id", "Document ID Column")
dbutils.widgets.text("output_table", "", "Output Table (catalog.schema.table)")
dbutils.widgets.text("endpoint", "databricks-gpt-oss-120b", "Model Endpoint")

# COMMAND ----------
source_table = dbutils.widgets.get("source_table")
text_column = dbutils.widgets.get("text_column")
doc_id_column = dbutils.widgets.get("doc_id_column")
output_table = dbutils.widgets.get("output_table")
endpoint = dbutils.widgets.get("endpoint")

assert source_table, "source_table is required"
assert output_table, "output_table is required"

print(f"Source: {source_table}.{text_column}")
print(f"Output: {output_table}")
print(f"Endpoint: {endpoint}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Run Detection + Redaction

# COMMAND ----------
from dbxredact import run_redaction_pipeline, RedactionConfig

config = RedactionConfig(
    use_presidio=True,
    use_ai_query=True,
    endpoint=endpoint,
)

result_df = run_redaction_pipeline(
    spark=spark,
    source_table=source_table,
    text_column=text_column,
    output_table=output_table,
    doc_id_column=doc_id_column,
    config=config,
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Results

# COMMAND ----------
display(result_df.select(
    doc_id_column,
    text_column,
    f"{text_column}_redacted",
    "_entity_count",
).limit(20))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Entity Summary

# COMMAND ----------
from pyspark.sql.functions import explode, col

entities_col = "aligned_results_struct" if "aligned_results_struct" in result_df.columns else "ai_results_struct"

entity_counts = (
    result_df
    .select(explode(col(entities_col)).alias("entity"))
    .select(col("entity.entity_type"))
    .groupBy("entity_type")
    .count()
    .orderBy("count", ascending=False)
)
display(entity_counts)
