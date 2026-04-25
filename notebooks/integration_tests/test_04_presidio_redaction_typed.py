# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test 04: Presidio Redaction (Typed, Separate)
# MAGIC
# MAGIC Full `run_redaction_pipeline` with Presidio only, typed redaction strategy,
# MAGIC separate output mode. Validates output table structure and PII removal.

# COMMAND ----------

# MAGIC %run ./test_utils

# COMMAND ----------

dbutils.widgets.text("test_schema", "dbxredact_tests", "Test Schema")

test_catalog = spark.sql("SELECT current_catalog()").first()[0]
test_schema = dbutils.widgets.get("test_schema")

# COMMAND ----------

print_test_header("Test 04: Presidio Redaction (Typed)")

spark = SparkSession.builder.getOrCreate()
tu = IntegrationTestUtils(spark, test_catalog, test_schema)

test_passed = False
error_message = None

try:
    tu.setup_test_environment()
    source_table = tu.create_pii_test_table("test_04_pii")
    output_table = tu._fqn("test_04_output")
    tu.track_table(output_table)

    from dbxredact import run_redaction_pipeline

    result_df = run_redaction_pipeline(
        spark=spark,
        source_table=source_table,
        text_column="text",
        output_table=output_table,
        doc_id_column="doc_id",
        use_presidio=True,
        use_ai_query=False,
        use_gliner=False,
        redaction_strategy="typed",
        output_mode="separate",
        output_strategy="production",
    )

    # Verify output table structure
    out_df = verify_output_table(spark, output_table,
                                  expected_columns=["doc_id", "text_redacted",
                                                    "_entity_count", "_detection_status"])

    # Verify PII is removed
    known_pii = ["123-45-6789", "jane.doe@example.com", "987-65-4321",
                  "John Smith", "Alice Johnson"]
    verify_no_pii_in_redacted(out_df, "text_redacted", known_pii)

    # Verify typed markers present
    verify_redaction_markers(out_df, "text_redacted", ["[PERSON]", "[US_SSN]"])

    # Verify entity counts
    from pyspark.sql.functions import col
    pii_rows = out_df.filter(col("_entity_count") > 0).count()
    tu.assert_greater(pii_rows, 0, f"{pii_rows} rows have detected entities")

    # Verify detection status
    statuses = [r["_detection_status"] for r in out_df.select("_detection_status").distinct().collect()]
    tu.assert_true("ok" in statuses, "'ok' status present for rows with entities")

    test_passed = True
    print_test_result("Test 04: Presidio Redaction (Typed)", True)

except TestFailure as e:
    error_message = str(e)
    print_test_result("Test 04: Presidio Redaction (Typed)", False, error_message)
except Exception as e:
    error_message = f"Unexpected error: {e}"
    import traceback; traceback.print_exc()
    print_test_result("Test 04: Presidio Redaction (Typed)", False, error_message)
finally:
    tu.cleanup_test_artifacts()

# COMMAND ----------

if not test_passed:
    raise Exception(f"Test failed: {error_message}")
dbutils.notebook.exit(json.dumps({"passed": test_passed, "error": error_message}))
