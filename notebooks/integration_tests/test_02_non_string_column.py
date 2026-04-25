# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test 02: Non-String Column Handling
# MAGIC
# MAGIC Validates the TypeError fix: running the pipeline against a non-string column
# MAGIC should succeed (with a warning log about casting) rather than crashing.

# COMMAND ----------

# MAGIC %run ./test_utils

# COMMAND ----------

dbutils.widgets.text("test_schema", "dbxredact_tests", "Test Schema")

test_catalog = spark.sql("SELECT current_catalog()").first()[0]
test_schema = dbutils.widgets.get("test_schema")

# COMMAND ----------

print_test_header("Test 02: Non-String Column Handling")

spark = SparkSession.builder.getOrCreate()
tu = IntegrationTestUtils(spark, test_catalog, test_schema)

test_passed = False
error_message = None

try:
    tu.setup_test_environment()
    source_table = tu.create_non_string_test_table("test_02_nonstring")
    output_table = tu._fqn("test_02_nonstring_output")
    tu.track_table(output_table)

    from dbxredact import run_redaction_pipeline

    # Presidio-only (pattern), no LLM needed
    result_df = run_redaction_pipeline(
        spark=spark,
        source_table=source_table,
        text_column="text_col",
        output_table=output_table,
        doc_id_column="doc_id",
        use_presidio=True,
        use_ai_query=False,
        use_gliner=False,
        output_mode="separate",
        presidio_pattern_only=True,
    )

    tu.assert_not_none(result_df, "Pipeline returned a DataFrame")
    count = spark.table(output_table).count()
    tu.assert_greater(count, 0, f"Output table has {count} rows")

    test_passed = True
    print_test_result("Test 02: Non-String Column", True)

except TestFailure as e:
    error_message = str(e)
    print_test_result("Test 02: Non-String Column", False, error_message)
except Exception as e:
    error_message = f"Unexpected error: {e}"
    import traceback; traceback.print_exc()
    print_test_result("Test 02: Non-String Column", False, error_message)
finally:
    tu.cleanup_test_artifacts()

# COMMAND ----------

if not test_passed:
    raise Exception(f"Test failed: {error_message}")
dbutils.notebook.exit(json.dumps({"passed": test_passed, "error": error_message}))
