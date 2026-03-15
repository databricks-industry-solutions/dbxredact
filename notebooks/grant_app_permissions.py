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

import re
_SAFE_ID = re.compile(r"^[a-zA-Z0-9_.\-@]+$")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
spn_name = dbutils.widgets.get("spn_name")
assert catalog and schema and spn_name, "All parameters are required"
if not re.match(r"^[a-zA-Z0-9_]+$", catalog) or not re.match(r"^[a-zA-Z0-9_]+$", schema):
    raise ValueError(f"Invalid catalog or schema name: {catalog!r}, {schema!r}")
if not _SAFE_ID.match(spn_name):
    raise ValueError(f"Invalid service principal name: {spn_name!r}")

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
