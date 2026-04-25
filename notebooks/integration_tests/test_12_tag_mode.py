# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test 12: Tag Mode
# MAGIC
# MAGIC Creates a table with UC column tags, then runs `run_redaction_pipeline_by_tag`
# MAGIC to verify automatic column discovery and redaction via tags.

# COMMAND ----------

# MAGIC %run ./test_utils

# COMMAND ----------

dbutils.widgets.text("test_schema", "dbxredact_tests", "Test Schema")

test_catalog = spark.sql("SELECT current_catalog()").first()[0]
test_schema = dbutils.widgets.get("test_schema")

# COMMAND ----------

print_test_header("Test 12: Tag Mode")

spark = SparkSession.builder.getOrCreate()
tu = IntegrationTestUtils(spark, test_catalog, test_schema)

test_passed = False
error_message = None

try:
    tu.setup_test_environment()
    source_table = tu.create_tagged_test_table(
        "test_12_tagged",
        tag_name="data_classification",
        tag_value="protected",
    )
    output_table = tu._fqn("test_12_tag_output")
    tu.track_table(output_table)

    from dbxredact import run_redaction_pipeline_by_tag

    result_df = run_redaction_pipeline_by_tag(
        spark=spark,
        source_table=source_table,
        output_table=output_table,
        tag_name="data_classification",
        tag_value="protected",
        doc_id_column="doc_id",
        use_presidio=True,
        use_ai_query=False,
        use_gliner=False,
        presidio_pattern_only=True,
    )

    tu.assert_not_none(result_df, "Tag mode returned a result")

    # Output should exist (may be output_table or output_table_text depending on column count)
    try:
        out_df = spark.table(output_table)
    except Exception:
        out_df = spark.table(f"{output_table}_text")
        tu.track_table(f"{output_table}_text")

    count = out_df.count()
    tu.assert_greater(count, 0, f"Tag mode output has {count} rows")
    tu.assert_true("text_redacted" in out_df.columns, "text_redacted present in tag mode output")

    known_pii = ["123-45-6789", "jane.doe@example.com"]
    verify_no_pii_in_redacted(out_df, "text_redacted", known_pii)

    test_passed = True
    print_test_result("Test 12: Tag Mode", True)

except TestFailure as e:
    error_message = str(e)
    print_test_result("Test 12: Tag Mode", False, error_message)
except Exception as e:
    error_message = f"Unexpected error: {e}"
    import traceback; traceback.print_exc()
    print_test_result("Test 12: Tag Mode", False, error_message)
finally:
    tu.cleanup_test_artifacts()

# COMMAND ----------

if not test_passed:
    raise Exception(f"Test failed: {error_message}")
dbutils.notebook.exit(json.dumps({"passed": test_passed, "error": error_message}))
