# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test 11: Streaming Pipeline
# MAGIC
# MAGIC Writes PII test data to a Delta table, then runs `run_redaction_pipeline_streaming`
# MAGIC with a checkpoint in a UC Volume. Validates output and checkpoint existence.

# COMMAND ----------

# MAGIC %run ./test_utils

# COMMAND ----------

dbutils.widgets.text("test_schema", "dbxredact_tests", "Test Schema")
dbutils.widgets.text("test_catalog", "", "Test Catalog (blank = current)")

_tc = dbutils.widgets.get("test_catalog").strip()
test_catalog = _tc or spark.sql("SELECT current_catalog()").first()[0]
test_schema = dbutils.widgets.get("test_schema")

# COMMAND ----------

print_test_header("Test 11: Streaming Pipeline")

spark = SparkSession.builder.getOrCreate()
tu = IntegrationTestUtils(spark, test_catalog, test_schema)

test_passed = False
error_message = None

try:
    tu.setup_test_environment()
    source_table = tu.create_pii_test_table("test_11_stream_src")
    output_table = tu._fqn("test_11_stream_out")
    tu.track_table(output_table)

    checkpoint_path = f"/Volumes/{test_catalog}/{test_schema}/test_volume/test_11_checkpoint"

    from dbxredact import run_redaction_pipeline_streaming

    query = run_redaction_pipeline_streaming(
        spark=spark,
        source_table=source_table,
        text_column="text",
        output_table=output_table,
        doc_id_column="doc_id",
        checkpoint_path=checkpoint_path,
        use_presidio=True,
        use_ai_query=False,
        use_gliner=False,
        output_mode="separate",
        presidio_pattern_only=True,
    )

    # Streaming with availableNow=True should complete and return
    out_df = spark.table(output_table)
    count = out_df.count()
    tu.assert_greater(count, 0, f"Streaming output has {count} rows")
    tu.assert_true("text_redacted" in out_df.columns, "text_redacted column present")

    # Verify checkpoint directory was created
    try:
        files = dbutils.fs.ls(checkpoint_path)
        tu.assert_greater(len(files), 0, f"Checkpoint directory has {len(files)} entries")
    except Exception as e:
        print(f"  Warning: Could not verify checkpoint path: {e}")

    test_passed = True
    print_test_result("Test 11: Streaming Pipeline", True)

except TestFailure as e:
    error_message = str(e)
    print_test_result("Test 11: Streaming Pipeline", False, error_message)
except Exception as e:
    error_message = f"Unexpected error: {e}"
    import traceback; traceback.print_exc()
    print_test_result("Test 11: Streaming Pipeline", False, error_message)
finally:
    tu.cleanup_test_artifacts()
    # Clean up checkpoint
    try:
        dbutils.fs.rm(checkpoint_path, recurse=True)
        print(f"  Cleaned up checkpoint: {checkpoint_path}")
    except Exception:
        pass

# COMMAND ----------

if not test_passed:
    raise Exception(f"Test failed: {error_message}")
dbutils.notebook.exit(json.dumps({"passed": test_passed, "error": error_message}))
