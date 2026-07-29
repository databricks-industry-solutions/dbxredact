# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test 14: Entity Filter (Safe List)
# MAGIC
# MAGIC Creates an entity filter safe-list table, runs the pipeline, and verifies
# MAGIC that safe-listed entities are NOT redacted.

# COMMAND ----------

# MAGIC %md
# MAGIC Install the spaCy model Presidio needs (en_core_web_lg, MIT). The analyzer
# MAGIC falls back trf -> lg -> md -> sm, so lg satisfies the default. Restart so
# MAGIC the freshly installed model is importable.

# COMMAND ----------

# MAGIC %pip install https://github.com/explosion/spacy-models/releases/download/en_core_web_lg-3.8.0/en_core_web_lg-3.8.0-py3-none-any.whl -q
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ./test_utils

# COMMAND ----------

dbutils.widgets.text("test_schema", "dbxredact_tests", "Test Schema")
dbutils.widgets.text("test_catalog", "", "Test Catalog (blank = current)")

_tc = dbutils.widgets.get("test_catalog").strip()
test_catalog = _tc or spark.sql("SELECT current_catalog()").first()[0]
test_schema = dbutils.widgets.get("test_schema")

# COMMAND ----------

print_test_header("Test 14: Entity Filter")

spark = SparkSession.builder.getOrCreate()
tu = IntegrationTestUtils(spark, test_catalog, test_schema)

test_passed = False
error_message = None

try:
    tu.setup_test_environment()

    # Create source table that mentions "Springfield" (a safe-listed location)
    source_fqn = tu._fqn("test_14_pii")
    spark.sql(f"DROP TABLE IF EXISTS {source_fqn}")
    spark.sql(f"CREATE TABLE {source_fqn} (doc_id STRING, text STRING) USING DELTA")
    spark.sql(f"""
        INSERT INTO {source_fqn} VALUES
        ('d1', 'John Smith lives in Springfield at 742 Evergreen Terrace'),
        ('d2', 'Acme Corp hired Jane Doe SSN 457-55-5462'),
        ('d3', 'Clean text with no PII here')
    """)
    tu.track_table(source_fqn)

    filter_table = tu.create_filter_table("test_14_safe_list")
    output_table = tu._fqn("test_14_output")
    tu.track_table(output_table)

    from dbxredact import run_redaction_pipeline, load_filter_from_table

    entity_filter = load_filter_from_table(spark, filter_table)

    run_redaction_pipeline(
        spark=spark,
        source_table=source_fqn,
        text_column="text",
        output_table=output_table,
        doc_id_column="doc_id",
        use_presidio=True,
        use_ai_query=False,
        use_gliner=False,
        output_mode="separate",
        entity_filter=entity_filter,
        output_strategy="validation",
        confirm_validation_output=True,
    )

    out_df = spark.table(output_table)

    # "Springfield" is safe-listed -- it should NOT be redacted
    redacted_texts = [r["text_redacted"] for r in out_df.select("text_redacted").collect()]
    all_redacted = " ".join(redacted_texts)

    # Springfield should still appear (safe-listed)
    tu.assert_contains(all_redacted, "Springfield",
                        "Safe-listed 'Springfield' preserved in redacted output")

    # But SSNs should still be redacted (not safe-listed)
    tu.assert_false("457-55-5462" in all_redacted,
                     "SSN still redacted despite entity filter")

    test_passed = True
    print_test_result("Test 14: Entity Filter", True)

except TestFailure as e:
    error_message = str(e)
    print_test_result("Test 14: Entity Filter", False, error_message)
except Exception as e:
    error_message = f"Unexpected error: {e}"
    import traceback; traceback.print_exc()
    print_test_result("Test 14: Entity Filter", False, error_message)
finally:
    tu.cleanup_test_artifacts()

# COMMAND ----------

if not test_passed:
    raise Exception(f"Test failed: {error_message}")
dbutils.notebook.exit(json.dumps({"passed": test_passed, "error": error_message}))
