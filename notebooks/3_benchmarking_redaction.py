# Databricks notebook source
# MAGIC %md
# MAGIC # PII/PHI Redaction -- Per-Method Benchmark
# MAGIC
# MAGIC Applies redaction separately for **each** detection method so redaction quality
# MAGIC can be compared side-by-side.
# MAGIC
# MAGIC **Output columns:** `text_redacted_presidio`, `text_redacted_ai`,
# MAGIC `text_redacted_gliner`, `text_redacted_aligned`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install dbxredact

# COMMAND ----------

# MAGIC # For interactive use, uncomment one of the following:
# MAGIC # %pip install /Workspace/<path-to-bundle>/artifacts/dbxredact-0.1.0-py3-none-any.whl
# MAGIC # %pip install git+https://github.com/databricks-industry-solutions/dbxredact.git
# MAGIC # %restart_python

# COMMAND ----------

from pyspark.sql.functions import col, explode, size

from dbxredact import create_redaction_udf

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget Configuration

# COMMAND ----------

dbutils.widgets.text(
    name="detection_table",
    defaultValue="your_catalog.your_schema.jsl_benchmark_detection_results",
    label="0. Detection Results Table",
)
dbutils.widgets.text(
    name="text_column", defaultValue="text", label="1. Text Column"
)
dbutils.widgets.dropdown(
    name="redaction_strategy",
    defaultValue="typed",
    choices=["generic", "typed"],
    label="2. Redaction Strategy",
)
dbutils.widgets.text(
    name="output_table",
    defaultValue="your_catalog.your_schema.jsl_benchmark_redacted",
    label="3. Output Table",
)

detection_table = dbutils.widgets.get("detection_table")
text_column = dbutils.widgets.get("text_column")
redaction_strategy = dbutils.widgets.get("redaction_strategy")
output_table = dbutils.widgets.get("output_table")

for _table in [detection_table, output_table]:
    if _table and ("your_catalog" in _table or "your_schema" in _table):
        raise ValueError(
            "Please update table widgets with your actual catalog and schema names."
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Detection Results

# COMMAND ----------

detection_df = spark.table(detection_table)
print(f"Loaded {detection_df.count()} records from {detection_table}")
print(f"Columns: {detection_df.columns}")
display(detection_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Redaction Per Method

# COMMAND ----------

METHOD_ENTITIES_MAP = {
    "presidio": "presidio_results_struct",
    "ai": "ai_results_struct",
    "gliner": "gliner_results_struct",
    "aligned": "aligned_entities",
}

redact_udf = create_redaction_udf(strategy=redaction_strategy)
redacted_df = detection_df

applied_methods = []

for method, entities_col in METHOD_ENTITIES_MAP.items():
    if entities_col not in detection_df.columns:
        print(f"Skipping {method}: column '{entities_col}' not found")
        continue
    redacted_col = f"text_redacted_{method}"
    redacted_df = redacted_df.withColumn(
        redacted_col, redact_udf(col(text_column), col(entities_col))
    )
    applied_methods.append(method)
    print(f"Applied {redaction_strategy} redaction for {method}")

print(f"\nMethods redacted: {applied_methods}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Results

# COMMAND ----------

redacted_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
    output_table
)
print(f"Saved to {output_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Side-by-Side Comparison

# COMMAND ----------

compare_cols = ["doc_id", text_column] + [
    f"text_redacted_{m}" for m in applied_methods
]
display(redacted_df.select(*[c for c in compare_cols if c in redacted_df.columns]).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Redaction Statistics Per Method

# COMMAND ----------

for method in applied_methods:
    entities_col = METHOD_ENTITIES_MAP[method]
    redacted_col = f"text_redacted_{method}"
    stats = redacted_df.selectExpr(
        f"'{method}' as method",
        "COUNT(*) as docs",
        f"SUM(SIZE({entities_col})) as total_entities",
        f"AVG(SIZE({entities_col})) as avg_entities_per_doc",
        f"AVG(LENGTH({text_column})) as avg_original_len",
        f"AVG(LENGTH({redacted_col})) as avg_redacted_len",
    )
    display(stats)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Redactions

# COMMAND ----------

for method in applied_methods:
    redacted_col = f"text_redacted_{method}"
    print(f"\n=== {method.upper()} ===")
    samples = redacted_df.select(text_column, redacted_col).limit(2).collect()
    for i, row in enumerate(samples, 1):
        print(f"  Doc {i} original : {row[text_column][:150]}...")
        print(f"  Doc {i} redacted : {row[redacted_col][:150]}...")
        print()
