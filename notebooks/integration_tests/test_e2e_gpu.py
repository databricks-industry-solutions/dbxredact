# Databricks notebook source
# MAGIC %md
# MAGIC # E2E Integration Test: GPU Cluster (with GLiNER)
# MAGIC
# MAGIC Full end-to-end pipeline on a GPU cluster with all three detectors enabled:
# MAGIC Presidio + AI Query + GLiNER. Validates that GLiNER results appear.

# COMMAND ----------

# MAGIC %md
# MAGIC Install the spaCy model (Presidio) and GLiNER (optional extra, not in the
# MAGIC base wheel). Restart so both are importable.

# COMMAND ----------

# MAGIC %pip install https://github.com/explosion/spacy-models/releases/download/en_core_web_lg-3.8.0/en_core_web_lg-3.8.0-py3-none-any.whl "gliner>=0.2.0,<1.0.0" -q
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

print_test_header("E2E Test: GPU Cluster (Presidio + AI Query + GLiNER)")

spark = SparkSession.builder.getOrCreate()
tu = IntegrationTestUtils(spark, test_catalog, test_schema)

test_passed = False
error_message = None

try:
    tu.setup_test_environment()
    source_table = tu.create_pii_test_table("test_e2e_gpu_src")
    output_table = tu._fqn("test_e2e_gpu_out")
    tu.track_table(output_table)

    from dbxredact import run_redaction_pipeline, run_detection_pipeline
    from pyspark.sql.functions import sum as spark_sum

    # --- First: verify GLiNER detection produces results ---
    source_df = spark.table(source_table)
    det_df = run_detection_pipeline(
        spark=spark,
        source_df=source_df,
        doc_id_column="doc_id",
        text_column="text",
        use_presidio=True,
        use_ai_query=True,
        use_gliner=True,
        endpoint=endpoint,
        align_results=False,
    )

    tu.assert_true("gliner_results_struct" in det_df.columns,
                    "gliner_results_struct column present")

    gliner_total = verify_entities_detected(det_df, "gliner_results_struct", min_total=1)
    print(f"  GLiNER detected {gliner_total} entities")

    # --- Full pipeline with all detectors ---
    run_redaction_pipeline(
        spark=spark,
        source_table=source_table,
        text_column="text",
        output_table=output_table,
        doc_id_column="doc_id",
        use_presidio=True,
        use_ai_query=True,
        use_gliner=True,
        endpoint=endpoint,
        alignment_mode="union",
        redaction_strategy="typed",
        output_mode="separate",
    )

    out_df = spark.table(output_table)
    total = out_df.select(spark_sum("_entity_count")).collect()[0][0] or 0
    tu.assert_greater(total, 5, f"3-detector ensemble found {total} entities")

    known_pii = ["457-55-5462", "262-78-4510", "jane.doe@example.com"]
    verify_no_pii_in_redacted(out_df, "text_redacted", known_pii)

    test_passed = True
    print_test_result("E2E GPU", True)

except TestFailure as e:
    error_message = str(e)
    print_test_result("E2E GPU", False, error_message)
except Exception as e:
    error_message = f"Unexpected error: {e}"
    import traceback; traceback.print_exc()
    print_test_result("E2E GPU", False, error_message)
finally:
    tu.cleanup_test_artifacts()

# COMMAND ----------

if not test_passed:
    raise Exception(f"Test failed: {error_message}")
dbutils.notebook.exit(json.dumps({"passed": test_passed, "error": error_message}))
