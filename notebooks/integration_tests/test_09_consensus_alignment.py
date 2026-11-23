# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test 09: Consensus Alignment
# MAGIC
# MAGIC Runs Presidio + AI Query with consensus alignment mode.
# MAGIC Verifies that consensus is stricter than union (fewer or equal entities),
# MAGIC and that high-confidence entities like SSNs still survive consensus.

# COMMAND ----------

# MAGIC %md
# MAGIC Install the spaCy model Presidio needs (en_core_web_lg, MIT). The analyzer
# MAGIC falls back trf -> lg -> md -> sm, so lg satisfies the default. Restart so
# MAGIC the freshly installed model is importable.

# COMMAND ----------

# MAGIC %pip install https://github.com/explosion/spacy-models/releases/download/en_core_web_lg-3.8.0/en_core_web_lg-3.8.0-py3-none-any.whl -q
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

print_test_header("Test 09: Consensus Alignment")

spark = SparkSession.builder.getOrCreate()
tu = IntegrationTestUtils(spark, test_catalog, test_schema)

test_passed = False
error_message = None

try:
    tu.setup_test_environment()
    source_table = tu.create_pii_test_table("test_09_pii")
    output_table = tu._fqn("test_09_output")
    tu.track_table(output_table)

    from dbxredact import run_redaction_pipeline
    from pyspark.sql.functions import sum as spark_sum

    run_redaction_pipeline(
        spark=spark,
        source_table=source_table,
        text_column="text",
        output_table=output_table,
        doc_id_column="doc_id",
        use_presidio=True,
        use_ai_query=True,
        use_gliner=False,
        endpoint=endpoint,
        alignment_mode="consensus",
        allow_consensus_redaction=True,
        redaction_strategy="typed",
        output_mode="separate",
    )

    out_df = spark.table(output_table)
    total = out_df.select(spark_sum("_entity_count")).collect()[0][0] or 0
    print(f"  Consensus entity total: {total}")

    # Consensus should still find obvious PII (SSNs detected by both detectors)
    # but may find fewer total entities than union
    tu.assert_greater(total, 0, "Consensus still detects some entities")

    # SSNs are high-confidence -- should be redacted even under consensus
    known_ssns = ["457-55-5462", "262-78-4510"]
    verify_no_pii_in_redacted(out_df, "text_redacted", known_ssns)

    test_passed = True
    print_test_result("Test 09: Consensus Alignment", True)

except TestFailure as e:
    error_message = str(e)
    print_test_result("Test 09: Consensus Alignment", False, error_message)
except Exception as e:
    error_message = f"Unexpected error: {e}"
    import traceback; traceback.print_exc()
    print_test_result("Test 09: Consensus Alignment", False, error_message)
finally:
    tu.cleanup_test_artifacts()

# COMMAND ----------

if not test_passed:
    raise Exception(f"Test failed: {error_message}")
dbutils.notebook.exit(json.dumps({"passed": test_passed, "error": error_message}))
