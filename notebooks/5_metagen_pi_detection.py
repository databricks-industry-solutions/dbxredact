# Databricks notebook source
# MAGIC %md
# MAGIC # PI Detection via dbxmetagen
# MAGIC
# MAGIC Installs the `dbxmetagen` library and runs **PI-only detection** against specified tables.
# MAGIC Supports two output modes:
# MAGIC - **tag**: Applies Unity Catalog tags (`data_classification` / `data_subclassification`) directly to tables/columns
# MAGIC - **knowledge_base**: Writes results to `metadata_generation_log` for downstream KB ingestion

# COMMAND ----------

# MAGIC %pip install -qqq "git+https://github.com/databricks-industry-solutions/dbxmetagen.git@main"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("table_names", "", "Table Names (wildcard or comma-sep)")
dbutils.widgets.text("catalog_name", "", "Destination Catalog")
dbutils.widgets.text("schema_name", "", "Destination Schema")
dbutils.widgets.dropdown("output_mode", "tag", ["tag", "knowledge_base"], "Output Mode")
dbutils.widgets.text("model", "databricks-meta-llama-3-3-70b-instruct", "LLM Endpoint")
dbutils.widgets.dropdown("include_deterministic_pi", "true", ["true", "false"], "Include Presidio Pre-scan")

# COMMAND ----------

table_names = dbutils.widgets.get("table_names")
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
output_mode = dbutils.widgets.get("output_mode")
model = dbutils.widgets.get("model")
include_deterministic_pi = dbutils.widgets.get("include_deterministic_pi").lower() == "true"

assert table_names, "table_names is required (e.g. my_catalog.my_schema.* or catalog.schema.table1,catalog.schema.table2)"
assert catalog_name, "catalog_name is required"
assert schema_name, "schema_name is required"

print(f"Running PI detection on: {table_names}")
print(f"Output mode: {output_mode}")
print(f"Model: {model}")
print(f"Deterministic PI (Presidio): {include_deterministic_pi}")

# COMMAND ----------

from dbxmetagen.main import main

config_kwargs = {
    "mode": "pi",
    "catalog_name": catalog_name,
    "schema_name": schema_name,
    "table_names": table_names,
    "table_names_source": "parameter",
    "model": model,
    "include_deterministic_pi": include_deterministic_pi,
    "apply_ddl": output_mode == "tag",
    "tag_none_fields": False,
    "incremental": True,
}

main(config_kwargs)

# COMMAND ----------

if output_mode == "tag":
    print("PI tags applied directly to Unity Catalog tables/columns.")
    print("Tags: data_classification, data_subclassification")
else:
    print(f"PI results written to {catalog_name}.{schema_name}.metadata_generation_log")
    print("Run the dbxmetagen KB build pipeline to ingest these into the knowledge base.")
