# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test 07: AI Query Detection
# MAGIC
# MAGIC Runs AI Query-only detection against a model serving endpoint.
# MAGIC Requires a foundation model endpoint (configured via widget).

# COMMAND ----------

# MAGIC %run ./test_utils

# COMMAND ----------

dbutils.widgets.text("test_schema", "dbxredact_tests", "Test Schema")
dbutils.widgets.text("endpoint", "databricks-gpt-oss-120b", "Model Endpoint")

test_catalog = spark.sql("SELECT current_catalog()").first()[0]
test_schema = dbutils.widgets.get("test_schema")
endpoint = dbutils.widgets.get("endpoint")

# COMMAND ----------

print_test_header("Test 07: AI Query Detection")

spark = SparkSession.builder.getOrCreate()
tu = IntegrationTestUtils(spark, test_catalog, test_schema)

test_passed = False
error_message = None

try:
    tu.setup_test_environment()
    source_table = tu.create_pii_test_table("test_07_pii")

    from dbxredact import run_detection_pipeline

    source_df = spark.table(source_table)
    det_df = run_detection_pipeline(
        spark=spark,
        source_df=source_df,
        doc_id_column="doc_id",
        text_column="text",
        use_presidio=False,
        use_ai_query=True,
        use_gliner=False,
        endpoint=endpoint,
        align_results=False,
    )

    tu.assert_true("ai_results_struct" in det_df.columns,
                    "ai_results_struct column present")

    total = verify_entities_detected(det_df, "ai_results_struct", min_total=1)
    print(f"  Total AI Query entities: {total}")

    test_passed = True
    print_test_result("Test 07: AI Query Detection", True)

except TestFailure as e:
    error_message = str(e)
    print_test_result("Test 07: AI Query Detection", False, error_message)
except Exception as e:
    error_message = f"Unexpected error: {e}"
    import traceback; traceback.print_exc()
    print_test_result("Test 07: AI Query Detection", False, error_message)
finally:
    tu.cleanup_test_artifacts()

# COMMAND ----------

if not test_passed:
    raise Exception(f"Test failed: {error_message}")
dbutils.notebook.exit(json.dumps({"passed": test_passed, "error": error_message}))
