# Databricks notebook source
# MAGIC %md
# MAGIC # Setup App Tables
# MAGIC
# MAGIC Creates the Unity Catalog tables required by the dbxredact management app.
# MAGIC Run this once before deploying the app.

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Catalog")
dbutils.widgets.text("schema", "", "Schema")

# COMMAND ----------

import re
_SAFE_ID = re.compile(r"^[a-zA-Z0-9_]+$")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
assert catalog and schema, "Both catalog and schema are required"
if not _SAFE_ID.match(catalog) or not _SAFE_ID.match(schema):
    raise ValueError(f"Invalid catalog or schema name: {catalog!r}, {schema!r}")
prefix = f"`{catalog}`.`{schema}`"

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {prefix}.redact_config (
    config_id STRING NOT NULL,
    name STRING NOT NULL,
    use_presidio BOOLEAN DEFAULT true,
    use_ai_query BOOLEAN DEFAULT true,
    use_gliner BOOLEAN DEFAULT false,
    endpoint STRING,
    score_threshold DOUBLE DEFAULT 0.5,
    gliner_model STRING DEFAULT 'nvidia/gliner-PII',
    gliner_threshold DOUBLE DEFAULT 0.2,
    redaction_strategy STRING DEFAULT 'typed',
    alignment_mode STRING DEFAULT 'union',
    extra_params STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
""")
print(f"Created {prefix}.redact_config")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {prefix}.redact_block_list (
    entry_id STRING NOT NULL,
    value STRING NOT NULL,
    is_pattern BOOLEAN DEFAULT false,
    entity_type STRING,
    notes STRING,
    created_at TIMESTAMP
)
""")
print(f"Created {prefix}.redact_block_list")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {prefix}.redact_safe_list (
    entry_id STRING NOT NULL,
    value STRING NOT NULL,
    is_pattern BOOLEAN DEFAULT false,
    entity_type STRING,
    notes STRING,
    created_at TIMESTAMP
)
""")
print(f"Created {prefix}.redact_safe_list")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {prefix}.redact_annotations (
    annotation_id STRING NOT NULL,
    doc_id STRING NOT NULL,
    source_table STRING NOT NULL,
    workflow STRING,
    entity_text STRING,
    entity_type STRING,
    start INT,
    end_pos INT,
    action STRING,
    corrected_type STRING,
    corrected_value STRING,
    detection_method STRING,
    created_at TIMESTAMP
)
""")
print(f"Created {prefix}.redact_annotations")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {prefix}.redact_job_history (
    run_id BIGINT NOT NULL,
    config_id STRING NOT NULL,
    source_table STRING NOT NULL,
    output_table STRING NOT NULL,
    status STRING,
    cost_estimate_usd DOUBLE,
    started_at TIMESTAMP,
    completed_at TIMESTAMP
)
""")
print(f"Created {prefix}.redact_job_history")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {prefix}.redact_ab_tests (
    test_id STRING NOT NULL,
    name STRING NOT NULL,
    config_a_id STRING NOT NULL,
    config_b_id STRING NOT NULL,
    source_table STRING NOT NULL,
    sample_size INT DEFAULT 100,
    status STRING DEFAULT 'created',
    metrics_a STRING,
    metrics_b STRING,
    winner STRING,
    created_at TIMESTAMP,
    completed_at TIMESTAMP
)
""")
print(f"Created {prefix}.redact_ab_tests")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {prefix}.redact_active_learn_queue (
    entry_id STRING NOT NULL,
    doc_id STRING NOT NULL,
    source_table STRING NOT NULL,
    priority_score DOUBLE,
    status STRING DEFAULT 'pending',
    assigned_to STRING,
    created_at TIMESTAMP,
    reviewed_at TIMESTAMP
)
""")
print(f"Created {prefix}.redact_active_learn_queue")

# COMMAND ----------

print("All app tables created successfully.")
