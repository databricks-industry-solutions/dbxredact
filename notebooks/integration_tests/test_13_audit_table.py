# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test 13: Audit Table
# MAGIC
# MAGIC Runs the pipeline with an `audit_table` specified and verifies that
# MAGIC the audit log is created with entity type counts and no raw PII.

# COMMAND ----------

# MAGIC %run ./test_utils

# COMMAND ----------

dbutils.widgets.text("test_schema", "dbxredact_tests", "Test Schema")
dbutils.widgets.text("test_catalog", "", "Test Catalog (blank = current)")

_tc = dbutils.widgets.get("test_catalog").strip()
test_catalog = _tc or spark.sql("SELECT current_catalog()").first()[0]
test_schema = dbutils.widgets.get("test_schema")

# COMMAND ----------

print_test_header("Test 13: Audit Table")

spark = SparkSession.builder.getOrCreate()
tu = IntegrationTestUtils(spark, test_catalog, test_schema)

test_passed = False
error_message = None

try:
    tu.setup_test_environment()
    source_table = tu.create_pii_test_table("test_13_pii")
    output_table = tu._fqn("test_13_output")
    audit_table = tu._fqn("test_13_audit")
    tu.track_table(output_table)
    tu.track_table(audit_table)

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
        output_mode="separate",
        audit_table=audit_table,
        presidio_pattern_only=True,
    )

    # Verify audit table exists and has data
    audit_df = spark.table(audit_table)
    audit_count = audit_df.count()
    tu.assert_greater(audit_count, 0, f"Audit table has {audit_count} rows")

    # Audit should have entity_type counts, not raw PII text
    audit_cols = set(audit_df.columns)
    tu.assert_true("doc_id" in audit_cols or "entity_type" in audit_cols,
                    "Audit table has expected columns")

    # Spot-check: raw PII text should NOT be in audit table
    audit_text = " ".join(str(row.asDict()) for row in audit_df.collect())
    for pii in ["457-55-5462", "jane.doe@example.com"]:
        tu.assert_false(pii in audit_text, f"Raw PII '{pii}' not in audit table")

    test_passed = True
    print_test_result("Test 13: Audit Table", True)

except TestFailure as e:
    error_message = str(e)
    print_test_result("Test 13: Audit Table", False, error_message)
except Exception as e:
    error_message = f"Unexpected error: {e}"
    import traceback; traceback.print_exc()
    print_test_result("Test 13: Audit Table", False, error_message)
finally:
    tu.cleanup_test_artifacts()

# COMMAND ----------

if not test_passed:
    raise Exception(f"Test failed: {error_message}")
dbutils.notebook.exit(json.dumps({"passed": test_passed, "error": error_message}))
