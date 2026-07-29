# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test 15: Structured Column Masking
# MAGIC
# MAGIC Exercises `run_table_redaction` structured masking on known PII columns using
# MAGIC all three strategies -- `mask`, `hash`, `encrypt` -- and verifies the
# MAGIC encrypt-without-key governance guard. No LLM needed (Presidio pattern-only /
# MAGIC structured masking is deterministic).

# COMMAND ----------

# MAGIC %run ./test_utils

# COMMAND ----------

dbutils.widgets.text("test_schema", "dbxredact_tests", "Test Schema")
dbutils.widgets.text("test_catalog", "", "Test Catalog (blank = current)")

_tc = dbutils.widgets.get("test_catalog").strip()
test_catalog = _tc or spark.sql("SELECT current_catalog()").first()[0]
test_schema = dbutils.widgets.get("test_schema")

# COMMAND ----------

print_test_header("Test 15: Structured Column Masking")

spark = SparkSession.builder.getOrCreate()
tu = IntegrationTestUtils(spark, test_catalog, test_schema)

test_passed = False
error_message = None

# The structured PII columns to mask and their pii_type mapping.
STRUCTURED_COLUMNS = {"name": "name", "ssn": "ssn", "email": "email", "phone": "phone"}
# Known raw PII values that must NOT survive in the masked output.
KNOWN_PII = ["John Smith", "457-55-5462", "john.smith@example.com", "555-867-5309"]

try:
    tu.setup_test_environment()
    source_table = tu.create_structured_pii_test_table("test_15_masking_src")

    from dbxredact import run_table_redaction

    def _run(strategy, out_name, encryption_key=None):
        out_fqn = tu._fqn(out_name)
        tu.track_table(out_fqn)
        # No text columns: structured-only masking. Detectors off (deterministic).
        run_table_redaction(
            spark=spark,
            source_table=source_table,
            output_table=out_fqn,
            masking_strategy=strategy,
            encryption_key=encryption_key,
            doc_id_column="doc_id",
            use_presidio=False,
            use_ai_query=False,
            use_gliner=False,
            text_columns=[],
            structured_columns=STRUCTURED_COLUMNS,
            max_rows=None,
        )
        return verify_output_table(spark, out_fqn, expected_columns=list(STRUCTURED_COLUMNS))

    # --- 1. mask strategy ---
    print("\n1. masking_strategy='mask'")
    mask_df = _run("mask", "test_15_out_mask")
    verify_no_pii_in_redacted(mask_df, "ssn", KNOWN_PII)
    verify_no_pii_in_redacted(mask_df, "email", KNOWN_PII)

    # --- 2. hash strategy (deterministic, no salt override -> per-run salt) ---
    print("\n2. masking_strategy='hash'")
    hash_df = _run("hash", "test_15_out_hash")
    verify_no_pii_in_redacted(hash_df, "ssn", KNOWN_PII)
    hash_rows = {r["doc_id"]: r["ssn"] for r in hash_df.select("doc_id", "ssn").collect()}
    # d1 and d2 share the same raw SSN -> same hash within a run.
    tu.assert_equals(hash_rows["d1"], hash_rows["d2"],
                     "hash is deterministic for identical inputs (d1 == d2)")
    tu.assert_true(hash_rows["d1"] != hash_rows["d3"],
                   "hash differs for different inputs (d1 != d3)")

    # --- 3. encrypt strategy ---
    print("\n3. masking_strategy='encrypt'")
    enc_df = _run("encrypt", "test_15_out_encrypt", encryption_key="test-key-1234567")
    verify_no_pii_in_redacted(enc_df, "ssn", KNOWN_PII)
    enc_ssn = {r["doc_id"]: r["ssn"] for r in enc_df.select("doc_id", "ssn").collect()}
    tu.assert_true(enc_ssn["d1"] != hash_rows["d1"],
                   "encrypt output differs from hash output")

    # --- 4. governance: encrypt without key must raise ---
    print("\n4. Governance: encrypt without encryption_key")
    try:
        run_table_redaction(
            spark=spark,
            source_table=source_table,
            output_table=tu._fqn("test_15_out_should_fail"),
            masking_strategy="encrypt",
            encryption_key=None,
            doc_id_column="doc_id",
            use_presidio=False, use_ai_query=False, use_gliner=False,
            text_columns=[],
            structured_columns=STRUCTURED_COLUMNS,
            max_rows=None,
        )
        tu.assert_true(False, "Should have raised for encrypt without key")
    except ValueError as e:
        tu.assert_contains(str(e), "encryption_key", "encrypt-without-key guard fires")

    test_passed = True
    print_test_result("Test 15: Structured Column Masking", True)

except TestFailure as e:
    error_message = str(e)
    print_test_result("Test 15: Structured Column Masking", False, error_message)
except Exception as e:
    error_message = f"Unexpected error: {e}"
    import traceback; traceback.print_exc()
    print_test_result("Test 15: Structured Column Masking", False, error_message)
finally:
    tu.cleanup_test_artifacts()

# COMMAND ----------

if not test_passed:
    raise Exception(f"Test failed: {error_message}")
dbutils.notebook.exit(json.dumps({"passed": test_passed, "error": error_message}))
