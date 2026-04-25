# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test 05: Presidio Redaction (Generic)
# MAGIC
# MAGIC Same as test_04 but with `redaction_strategy="generic"`.
# MAGIC Verifies `[REDACTED]` markers instead of typed entity markers.

# COMMAND ----------

# MAGIC %run ./test_utils

# COMMAND ----------

dbutils.widgets.text("test_schema", "dbxredact_tests", "Test Schema")

test_catalog = spark.sql("SELECT current_catalog()").first()[0]
test_schema = dbutils.widgets.get("test_schema")

# COMMAND ----------

print_test_header("Test 05: Presidio Redaction (Generic)")

spark = SparkSession.builder.getOrCreate()
tu = IntegrationTestUtils(spark, test_catalog, test_schema)

test_passed = False
error_message = None

try:
    tu.setup_test_environment()
    source_table = tu.create_pii_test_table("test_05_pii")
    output_table = tu._fqn("test_05_output")
    tu.track_table(output_table)

    from dbxredact import run_redaction_pipeline

    run_redaction_pipeline(
        spark=spark,
        source_table=source_table,
        text_column="text",
        output_table=output_table,
        doc_id_column="doc_id",
        use_presidio=True,
        use_ai_query=False,
        use_gliner=False,
        redaction_strategy="generic",
        output_mode="separate",
    )

    out_df = verify_output_table(spark, output_table)
    verify_redaction_markers(out_df, "text_redacted", ["[REDACTED]"])

    known_pii = ["123-45-6789", "987-65-4321", "jane.doe@example.com"]
    verify_no_pii_in_redacted(out_df, "text_redacted", known_pii)

    test_passed = True
    print_test_result("Test 05: Presidio Redaction (Generic)", True)

except TestFailure as e:
    error_message = str(e)
    print_test_result("Test 05: Presidio Redaction (Generic)", False, error_message)
except Exception as e:
    error_message = f"Unexpected error: {e}"
    import traceback; traceback.print_exc()
    print_test_result("Test 05: Presidio Redaction (Generic)", False, error_message)
finally:
    tu.cleanup_test_artifacts()

# COMMAND ----------

if not test_passed:
    raise Exception(f"Test failed: {error_message}")
dbutils.notebook.exit(json.dumps({"passed": test_passed, "error": error_message}))
