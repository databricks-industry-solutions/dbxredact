# Databricks notebook source
# MAGIC %md
# MAGIC # Unstructured Data Ingestion + Redaction
# MAGIC
# MAGIC > **Library usage:** This notebook demonstrates using dbxredact as a **pip-installable library**.
# MAGIC
# MAGIC End-to-end example: ingest text/PDF files from a Unity Catalog Volume into a Delta table,
# MAGIC then detect and redact PII/PHI using dbxredact.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - A UC Volume containing text or PDF files (e.g. clinical notes, emails, contracts)
# MAGIC - A foundation model endpoint for AI Query detection
# MAGIC - Unity Catalog catalog and schema where output tables will be written

# COMMAND ----------
# MAGIC %pip install -qqq git+https://github.com/databricks-industry-solutions/dbxredact.git@main
# MAGIC %pip install -qqq https://github.com/explosion/spacy-models/releases/download/en_core_web_lg-3.8.0/en_core_web_lg-3.8.0-py3-none-any.whl
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "", "Schema Name")
dbutils.widgets.text("volume_name", "raw_documents", "Volume Name (containing files)")
dbutils.widgets.text("endpoint", "databricks-gpt-oss-120b", "Model Endpoint")
dbutils.widgets.dropdown("file_format", "text", ["text", "pdf", "binaryFile"], "File Format")

# COMMAND ----------
catalog = dbutils.widgets.get("catalog_name")
schema = dbutils.widgets.get("schema_name")
volume = dbutils.widgets.get("volume_name")
endpoint = dbutils.widgets.get("endpoint")
file_format = dbutils.widgets.get("file_format")

assert catalog, "catalog_name is required"
assert schema, "schema_name is required"

volume_path = f"/Volumes/{catalog}/{schema}/{volume}"
staging_table = f"{catalog}.{schema}.raw_documents"
output_table = f"{catalog}.{schema}.redacted_documents"

print(f"Volume path: {volume_path}")
print(f"Staging table: {staging_table}")
print(f"Output table: {output_table}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1: Ingest Files into a Delta Table
# MAGIC
# MAGIC Read files from the UC Volume and create a table with `doc_id` and `text` columns.
# MAGIC Adjust the `file_format` widget based on your source files.

# COMMAND ----------
from pyspark.sql.functions import monotonically_increasing_id, col, input_file_name

if file_format == "text":
    raw_df = (
        spark.read.format("text")
        .option("wholetext", "true")
        .load(f"{volume_path}/*.txt")
    )
    docs_df = (
        raw_df
        .withColumn("doc_id", input_file_name())
        .withColumnRenamed("value", "text")
        .select("doc_id", "text")
    )

elif file_format == "pdf":
    docs_df = (
        spark.read.format("binaryFile")
        .load(f"{volume_path}/*.pdf")
        .selectExpr(
            "path AS doc_id",
            "CAST(content AS STRING) AS text",
        )
    )

elif file_format == "binaryFile":
    docs_df = (
        spark.read.format("binaryFile")
        .load(f"{volume_path}/*")
        .selectExpr(
            "path AS doc_id",
            "CAST(content AS STRING) AS text",
        )
    )

docs_df.write.mode("overwrite").saveAsTable(staging_table)
print(f"Ingested {spark.table(staging_table).count()} documents into {staging_table}")

# COMMAND ----------
display(spark.table(staging_table).limit(5))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 2: Detect and Redact PII/PHI

# COMMAND ----------
from dbxredact import run_redaction_pipeline, RedactionConfig

config = RedactionConfig(
    use_presidio=True,
    use_ai_query=True,
    endpoint=endpoint,
)

result_df = run_redaction_pipeline(
    spark=spark,
    source_table=staging_table,
    text_column="text",
    output_table=output_table,
    doc_id_column="doc_id",
    config=config,
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 3: Inspect Results

# COMMAND ----------
display(
    result_df
    .select("doc_id", "text", "text_redacted", "_entity_count")
    .orderBy("_entity_count", ascending=False)
    .limit(20)
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Entity Type Breakdown

# COMMAND ----------
from pyspark.sql.functions import explode

entities_col = "aligned_results_struct" if "aligned_results_struct" in result_df.columns else "ai_results_struct"

entity_summary = (
    result_df
    .select(explode(col(entities_col)).alias("e"))
    .select(col("e.entity_type"), col("e.entity"))
    .groupBy("entity_type")
    .count()
    .orderBy("count", ascending=False)
)
display(entity_summary)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Sample: Original vs Redacted

# COMMAND ----------
sample = spark.table(output_table).limit(3).collect()
for row in sample:
    print(f"--- {row['doc_id']} ---")
    print(f"ORIGINAL:\n{row['text'][:500]}\n")
    print(f"REDACTED:\n{row['text_redacted'][:500]}\n")
