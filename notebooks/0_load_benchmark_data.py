# Databricks notebook source
# MAGIC %md
# MAGIC # Load Benchmark Data
# MAGIC
# MAGIC Upload a CSV file (from Volumes, DBFS, or workspace files) into a Unity Catalog table
# MAGIC for use with the benchmarking and redaction pipeline notebooks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget Configuration

# COMMAND ----------

dbutils.widgets.text(
    name="csv_path",
    defaultValue="/Workspace/${workspace.current_user.home}/dbxredact/data/synthetic_benchmark_medical.csv",
    label="0. CSV File Path",
)
dbutils.widgets.text(
    name="target_table",
    defaultValue="your_catalog.your_schema.synthetic_benchmark_medical",
    label="1. Target Table (fully qualified)",
)
dbutils.widgets.dropdown(
    name="mode",
    defaultValue="overwrite",
    choices=["overwrite", "append"],
    label="2. Write Mode",
)

csv_path = dbutils.widgets.get("csv_path")
target_table = dbutils.widgets.get("target_table")
write_mode = dbutils.widgets.get("mode")

if "your_catalog" in target_table or "your_schema" in target_table:
    raise ValueError(
        "Please update the 'target_table' widget with your actual catalog and schema names."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load CSV and Write to Table

# COMMAND ----------

df = spark.read.csv(csv_path, header=True, inferSchema=True)
print(f"Loaded {df.count()} rows from {csv_path}")
print(f"Schema:")
df.printSchema()

# COMMAND ----------

df.write.mode(write_mode).option("mergeSchema", "true").saveAsTable(target_table)
print(f"Data written to {target_table} (mode={write_mode})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

result = spark.table(target_table)
print(f"Table {target_table} has {result.count()} rows")
display(result.limit(5))
