# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test 03: Presidio Detection
# MAGIC
# MAGIC Runs Presidio-only detection on a table with known PII and validates that
# MAGIC entities are found with correct types and valid text offsets.

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

print_test_header("Test 03: Presidio Detection")

spark = SparkSession.builder.getOrCreate()
tu = IntegrationTestUtils(spark, test_catalog, test_schema)

test_passed = False
error_message = None

try:
    tu.setup_test_environment()
    source_table = tu.create_pii_test_table("test_03_pii")

    from dbxredact import run_detection_pipeline

    source_df = spark.table(source_table)
    det_df = run_detection_pipeline(
        spark=spark,
        source_df=source_df,
        doc_id_column="doc_id",
        text_column="text",
        use_presidio=True,
        use_ai_query=False,
        use_gliner=False,
        align_results=False,
    )

    # Presidio results column exists
    tu.assert_true("presidio_results_struct" in det_df.columns,
                    "presidio_results_struct column present")

    # Entities were detected
    total = verify_entities_detected(det_df, "presidio_results_struct", min_total=3)
    print(f"  Total Presidio entities: {total}")

    # Validate entity structure: check first non-empty row
    from pyspark.sql.functions import size, col
    sample = (det_df.filter(size("presidio_results_struct") > 0)
              .select("doc_id", "text", "presidio_results_struct")
              .first())
    tu.assert_not_none(sample, "Found a row with detected entities")

    ent = sample["presidio_results_struct"][0]
    tu.assert_true("entity_type" in ent.asDict(), "Entity has entity_type field")
    tu.assert_true("start" in ent.asDict(), "Entity has start field")
    tu.assert_true("end" in ent.asDict(), "Entity has end field")
    tu.assert_true(ent["start"] >= 0 and ent["end"] > ent["start"],
                    f"Valid offsets: start={ent['start']}, end={ent['end']}")

    test_passed = True
    print_test_result("Test 03: Presidio Detection", True)

except TestFailure as e:
    error_message = str(e)
    print_test_result("Test 03: Presidio Detection", False, error_message)
except Exception as e:
    error_message = f"Unexpected error: {e}"
    import traceback; traceback.print_exc()
    print_test_result("Test 03: Presidio Detection", False, error_message)
finally:
    tu.cleanup_test_artifacts()

# COMMAND ----------

if not test_passed:
    raise Exception(f"Test failed: {error_message}")
dbutils.notebook.exit(json.dumps({"passed": test_passed, "error": error_message}))
