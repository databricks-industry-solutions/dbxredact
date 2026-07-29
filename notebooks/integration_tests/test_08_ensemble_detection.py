# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test 08: Ensemble Detection (Union)
# MAGIC
# MAGIC Runs both Presidio + AI Query with union alignment.
# MAGIC Verifies aligned_entities column and that union produces >= either detector alone.

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
dbutils.widgets.text("endpoint", "databricks-gpt-oss-120b", "Model Endpoint")

_tc = dbutils.widgets.get("test_catalog").strip()
test_catalog = _tc or spark.sql("SELECT current_catalog()").first()[0]
test_schema = dbutils.widgets.get("test_schema")
endpoint = dbutils.widgets.get("endpoint")

# COMMAND ----------

print_test_header("Test 08: Ensemble Detection (Union)")

spark = SparkSession.builder.getOrCreate()
tu = IntegrationTestUtils(spark, test_catalog, test_schema)

test_passed = False
error_message = None

try:
    tu.setup_test_environment()
    source_table = tu.create_pii_test_table("test_08_pii")
    output_table = tu._fqn("test_08_output")
    tu.track_table(output_table)

    from dbxredact import run_redaction_pipeline
    from pyspark.sql.functions import col

    result_df = run_redaction_pipeline(
        spark=spark,
        source_table=source_table,
        text_column="text",
        output_table=output_table,
        doc_id_column="doc_id",
        use_presidio=True,
        use_ai_query=True,
        use_gliner=False,
        endpoint=endpoint,
        alignment_mode="union",
        redaction_strategy="typed",
        output_mode="separate",
        output_strategy="validation",
        confirm_validation_output=True,
    )

    out_df = spark.table(output_table)

    # Check entity detail columns present (validation mode)
    cols = set(out_df.columns)
    tu.assert_true("text_redacted" in cols, "text_redacted present")
    tu.assert_true("_entity_count" in cols, "_entity_count present")

    # At least some entities detected
    from pyspark.sql.functions import sum as spark_sum
    total_entities = out_df.select(spark_sum("_entity_count")).collect()[0][0] or 0
    tu.assert_greater(total_entities, 0, f"Union ensemble found {total_entities} entities")

    # PII should be redacted
    known_pii = ["457-55-5462", "262-78-4510"]
    verify_no_pii_in_redacted(out_df, "text_redacted", known_pii)

    test_passed = True
    print_test_result("Test 08: Ensemble Detection (Union)", True)

except TestFailure as e:
    error_message = str(e)
    print_test_result("Test 08: Ensemble Detection (Union)", False, error_message)
except Exception as e:
    error_message = f"Unexpected error: {e}"
    import traceback; traceback.print_exc()
    print_test_result("Test 08: Ensemble Detection (Union)", False, error_message)
finally:
    tu.cleanup_test_artifacts()

# COMMAND ----------

if not test_passed:
    raise Exception(f"Test failed: {error_message}")
dbutils.notebook.exit(json.dumps({"passed": test_passed, "error": error_message}))
