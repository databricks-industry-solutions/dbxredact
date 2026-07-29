# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test 10: max_rows Truncation
# MAGIC
# MAGIC Creates a 100-row table and runs the pipeline with `max_rows=10`.
# MAGIC Verifies output is capped at the specified limit.

# COMMAND ----------

# MAGIC %run ./test_utils

# COMMAND ----------

dbutils.widgets.text("test_schema", "dbxredact_tests", "Test Schema")
dbutils.widgets.text("test_catalog", "", "Test Catalog (blank = current)")

_tc = dbutils.widgets.get("test_catalog").strip()
test_catalog = _tc or spark.sql("SELECT current_catalog()").first()[0]
test_schema = dbutils.widgets.get("test_schema")

# COMMAND ----------

print_test_header("Test 10: max_rows Truncation")

spark = SparkSession.builder.getOrCreate()
tu = IntegrationTestUtils(spark, test_catalog, test_schema)

test_passed = False
error_message = None

try:
    tu.setup_test_environment()
    source_table = tu.create_large_test_table("test_10_large", num_rows=100)
    output_table = tu._fqn("test_10_output")
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
        max_rows=10,
        output_mode="separate",
        presidio_pattern_only=True,
    )

    out_count = spark.table(output_table).count()
    tu.assert_less_equal(out_count, 10, f"Output capped at {out_count} rows (max_rows=10)")
    tu.assert_greater(out_count, 0, "Output is not empty")

    test_passed = True
    print_test_result("Test 10: max_rows", True)

except TestFailure as e:
    error_message = str(e)
    print_test_result("Test 10: max_rows", False, error_message)
except Exception as e:
    error_message = f"Unexpected error: {e}"
    import traceback; traceback.print_exc()
    print_test_result("Test 10: max_rows", False, error_message)
finally:
    tu.cleanup_test_artifacts()

# COMMAND ----------

if not test_passed:
    raise Exception(f"Test failed: {error_message}")
dbutils.notebook.exit(json.dumps({"passed": test_passed, "error": error_message}))
