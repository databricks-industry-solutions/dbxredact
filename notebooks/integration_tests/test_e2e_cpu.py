# Databricks notebook source
# MAGIC %md
# MAGIC # E2E Integration Test: CPU Cluster
# MAGIC
# MAGIC Full end-to-end redaction pipeline on a CPU cluster with Presidio + AI Query.
# MAGIC Creates test data, runs detection + redaction, and validates output quality.

# COMMAND ----------

# MAGIC %run ./test_utils

# COMMAND ----------

dbutils.widgets.text("test_schema", "dbxredact_tests", "Test Schema")
dbutils.widgets.text("endpoint", "databricks-gpt-oss-120b", "Model Endpoint")

test_catalog = spark.sql("SELECT current_catalog()").first()[0]
test_schema = dbutils.widgets.get("test_schema")
endpoint = dbutils.widgets.get("endpoint")

# COMMAND ----------

print_test_header("E2E Test: CPU Cluster (Presidio + AI Query)")

spark = SparkSession.builder.getOrCreate()
tu = IntegrationTestUtils(spark, test_catalog, test_schema)

test_passed = False
error_message = None

try:
    tu.setup_test_environment()
    source_table = tu.create_pii_test_table("test_e2e_cpu_src")
    output_table = tu._fqn("test_e2e_cpu_out")
    tu.track_table(output_table)

    from dbxredact import run_redaction_pipeline
    from pyspark.sql.functions import col, sum as spark_sum

    # --- Full pipeline ---
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
        output_strategy="production",
    )

    out_df = spark.table(output_table)

    # --- Validate structure ---
    expected_cols = ["doc_id", "text_redacted", "_entity_count", "_detection_status"]
    verify_output_table(spark, output_table, expected_columns=expected_cols)

    # --- Validate PII removal ---
    known_pii = [
        "123-45-6789", "987-65-4321", "jane.doe@example.com",
        "John Smith", "Alice Johnson", "Bob Williams", "Mary Garcia",
    ]
    verify_no_pii_in_redacted(out_df, "text_redacted", known_pii)

    # --- Validate redaction markers ---
    verify_redaction_markers(out_df, "text_redacted", ["[PERSON]", "[US_SSN]"])

    # --- Validate entity counts ---
    total = out_df.select(spark_sum("_entity_count")).collect()[0][0] or 0
    tu.assert_greater(total, 5, f"E2E detected {total} total entities (expected > 5)")

    # --- Validate detection statuses ---
    statuses = {r[0] for r in out_df.select("_detection_status").distinct().collect()}
    tu.assert_true("ok" in statuses, "'ok' status present")

    # Row d4 and d10 have no PII -- should have no_entities or low count
    clean_row = out_df.filter(col("doc_id") == "d4").first()
    if clean_row:
        tu.assert_equals(clean_row["_entity_count"], 0,
                          "Clean row d4 has 0 entities")
        tu.assert_equals(clean_row["_detection_status"], "no_entities",
                          "Clean row d4 has 'no_entities' status")

    test_passed = True
    print_test_result("E2E CPU", True)

except TestFailure as e:
    error_message = str(e)
    print_test_result("E2E CPU", False, error_message)
except Exception as e:
    error_message = f"Unexpected error: {e}"
    import traceback; traceback.print_exc()
    print_test_result("E2E CPU", False, error_message)
finally:
    tu.cleanup_test_artifacts()

# COMMAND ----------

if not test_passed:
    raise Exception(f"Test failed: {error_message}")
dbutils.notebook.exit(json.dumps({"passed": test_passed, "error": error_message}))
