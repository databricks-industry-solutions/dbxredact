# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test 01: Imports and Config
# MAGIC
# MAGIC Verifies the dbxredact wheel is importable and that `RedactionConfig` +
# MAGIC governance guards work correctly. No LLM calls or detection -- fast.

# COMMAND ----------

# MAGIC %run ./test_utils

# COMMAND ----------

dbutils.widgets.text("test_schema", "dbxredact_tests", "Test Schema")
dbutils.widgets.text("test_catalog", "", "Test Catalog (blank = current)")

_tc = dbutils.widgets.get("test_catalog").strip()
test_catalog = _tc or spark.sql("SELECT current_catalog()").first()[0]
test_schema = dbutils.widgets.get("test_schema")

# COMMAND ----------

print_test_header("Test 01: Imports and Config")

spark = SparkSession.builder.getOrCreate()
test_utils = IntegrationTestUtils(spark, test_catalog, test_schema)

test_passed = False
error_message = None

try:
    # --- 1. Public API imports ---
    print("\n1. Verifying public API imports")
    import dbxredact

    public_names = [
        "RedactionConfig", "run_redaction_pipeline", "run_detection_pipeline",
        "run_redaction_pipeline_streaming", "run_redaction_pipeline_by_tag",
        "run_presidio_detection", "run_ai_query_detection", "run_detection",
        "redact_text", "create_redaction_udf", "EntityFilter",
        "get_columns_by_tag", "get_protected_columns",
        "OutputStrategy", "OutputMode", "AlignmentMode",
    ]
    missing = [n for n in public_names if not hasattr(dbxredact, n)]
    test_utils.assert_true(len(missing) == 0, f"All public names importable (missing: {missing})")

    # --- 2. RedactionConfig instantiation ---
    print("\n2. Verifying RedactionConfig")
    from dbxredact import RedactionConfig

    cfg = RedactionConfig(use_presidio=True, use_ai_query=False)
    test_utils.assert_true(cfg.use_presidio is True, "use_presidio=True")
    test_utils.assert_true(cfg.use_ai_query is False, "use_ai_query=False")
    test_utils.assert_equals(cfg.redaction_strategy, "generic", "default redaction_strategy is generic")
    test_utils.assert_equals(cfg.alignment_mode, "union", "default alignment_mode is union")

    cfg2 = RedactionConfig(
        use_presidio=True, use_ai_query=True,
        alignment_mode="consensus", allow_consensus_redaction=True,
        redaction_strategy="typed", output_strategy="validation",
    )
    test_utils.assert_equals(cfg2.alignment_mode, "consensus", "consensus config accepted")

    # --- 3. Governance guards ---
    print("\n3. Verifying governance guards")
    from dbxredact import run_redaction_pipeline

    # No detectors
    try:
        run_redaction_pipeline(spark=spark, source_table="a.b.c", text_column="t",
                               output_table="a.b.d", use_presidio=False,
                               use_ai_query=False, use_gliner=False)
        test_utils.assert_true(False, "Should have raised for no detectors")
    except ValueError as e:
        test_utils.assert_contains(str(e), "detection method", "No-detector guard fires")

    # Destructive without confirm
    try:
        run_redaction_pipeline(spark=spark, source_table="a.b.c", text_column="t",
                               output_mode="in_place", confirm_destructive=False,
                               use_presidio=True, use_ai_query=False, use_gliner=False)
        test_utils.assert_true(False, "Should have raised for destructive")
    except ValueError as e:
        test_utils.assert_contains(str(e), "destructive", "Destructive guard fires")

    # Consensus without opt-in
    try:
        run_redaction_pipeline(spark=spark, source_table="a.b.c", text_column="t",
                               output_table="a.b.d", alignment_mode="consensus",
                               allow_consensus_redaction=False,
                               use_presidio=True, use_ai_query=False, use_gliner=False)
        test_utils.assert_true(False, "Should have raised for consensus")
    except ValueError as e:
        test_utils.assert_contains(str(e), "unsafe", "Consensus guard fires")

    # Missing output_table for separate mode
    try:
        run_redaction_pipeline(spark=spark, source_table="a.b.c", text_column="t",
                               output_table=None, output_mode="separate",
                               use_presidio=True, use_ai_query=False, use_gliner=False)
        test_utils.assert_true(False, "Should have raised for missing output_table")
    except ValueError as e:
        test_utils.assert_contains(str(e), "output_table", "output_table guard fires")

    test_passed = True
    print_test_result("Test 01: Imports and Config", True)

except TestFailure as e:
    error_message = str(e)
    print_test_result("Test 01: Imports and Config", False, error_message)
except Exception as e:
    error_message = f"Unexpected error: {e}"
    import traceback; traceback.print_exc()
    print_test_result("Test 01: Imports and Config", False, error_message)

# COMMAND ----------

if not test_passed:
    raise Exception(f"Test failed: {error_message}")
dbutils.notebook.exit(json.dumps({"passed": test_passed, "error": error_message}))
