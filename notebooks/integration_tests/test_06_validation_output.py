# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test 06: Validation Output Strategy
# MAGIC
# MAGIC Runs the pipeline with `output_strategy="validation"` and verifies that
# MAGIC the output retains raw text and entity detail columns alongside redacted output.

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

_tc = dbutils.widgets.get("test_catalog").strip()
test_catalog = _tc or spark.sql("SELECT current_catalog()").first()[0]
test_schema = dbutils.widgets.get("test_schema")

# COMMAND ----------

print_test_header("Test 06: Validation Output Strategy")

spark = SparkSession.builder.getOrCreate()
tu = IntegrationTestUtils(spark, test_catalog, test_schema)

test_passed = False
error_message = None

try:
    tu.setup_test_environment()
    source_table = tu.create_pii_test_table("test_06_pii")
    output_table = tu._fqn("test_06_output")
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
        redaction_strategy="typed",
        output_mode="separate",
        output_strategy="validation",
        confirm_validation_output=True,
    )

    out_df = spark.table(output_table)
    cols = set(out_df.columns)

    # Validation mode keeps raw text and entity columns
    tu.assert_true("text" in cols, "Original 'text' column retained in validation mode")
    tu.assert_true("text_redacted" in cols, "'text_redacted' column present")
    tu.assert_true("_entity_count" in cols, "'_entity_count' column present")

    # Should have either aligned_entities or a detector-specific column
    has_entities_col = any(c for c in cols if "entities" in c.lower() or "results_struct" in c.lower())
    tu.assert_true(has_entities_col, "Entity detail column present in validation output")

    count = out_df.count()
    tu.assert_greater(count, 0, f"Output has {count} rows")

    test_passed = True
    print_test_result("Test 06: Validation Output", True)

except TestFailure as e:
    error_message = str(e)
    print_test_result("Test 06: Validation Output", False, error_message)
except Exception as e:
    error_message = f"Unexpected error: {e}"
    import traceback; traceback.print_exc()
    print_test_result("Test 06: Validation Output", False, error_message)
finally:
    tu.cleanup_test_artifacts()

# COMMAND ----------

if not test_passed:
    raise Exception(f"Test failed: {error_message}")
dbutils.notebook.exit(json.dumps({"passed": test_passed, "error": error_message}))
