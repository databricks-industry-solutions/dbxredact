# Databricks notebook source
# MAGIC %md
# MAGIC # Grant App Service Principal Permissions
# MAGIC
# MAGIC Grants the app's service principal access to the catalog, schema, and tables
# MAGIC it needs. Called automatically by `deploy.sh` after bundle deployment.

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Catalog")
dbutils.widgets.text("schema", "", "Schema")
dbutils.widgets.text("spn_name", "", "Service Principal Name")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
spn_name = dbutils.widgets.get("spn_name")
assert catalog and schema and spn_name, "All parameters are required"

fqn = f"`{catalog}`.`{schema}`"
spn = f"`{spn_name}`"

# COMMAND ----------

grants = [
    f"GRANT USE CATALOG ON CATALOG `{catalog}` TO {spn}",
    f"GRANT USE SCHEMA ON SCHEMA {fqn} TO {spn}",
    f"GRANT CREATE TABLE ON SCHEMA {fqn} TO {spn}",
    f"GRANT SELECT ON SCHEMA {fqn} TO {spn}",
    f"GRANT MODIFY ON SCHEMA {fqn} TO {spn}",
]

for stmt in grants:
    print(f"Running: {stmt}")
    spark.sql(stmt)
    print("  OK")

# COMMAND ----------

print(f"All permissions granted to {spn_name} on {fqn}")
