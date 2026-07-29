# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test 16: Ingestion Pipeline File Formats
# MAGIC
# MAGIC Exercises `run_ingestion_redaction_pipeline` (Auto Loader from a UC Volume) across
# MAGIC every supported file format -- json / csv / parquet / text -- and both multi-column
# MAGIC strategies (`concat`, `sequential`). Detection is Presidio-only for the format matrix
# MAGIC (cheap/deterministic); one run turns AI Query on to prove the ingestion path wires it.

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

print_test_header("Test 16: Ingestion Pipeline File Formats")

spark = SparkSession.builder.getOrCreate()
tu = IntegrationTestUtils(spark, test_catalog, test_schema)

test_passed = False
error_message = None

# Pattern-detectable PII present in the sample data (Presidio pattern-only catches
# these without spaCy NER): SSNs, emails, phone numbers.
KNOWN_PII = [
    "457-55-5462", "pat1@example.com", "555-867-5309",
    "jane.doe@example.com", "262-78-4510", "555-111-2222",
    "222-33-4444", "bob@example.com", "carol@example.com",
]
VOL = f"/Volumes/{test_catalog}/{test_schema}/test_volume"
_paths_to_clean = []


def _vol(sub):
    p = f"{VOL}/test_16_{sub}"
    _paths_to_clean.append(p)
    return p


try:
    tu.setup_test_environment()
    from dbxredact import run_ingestion_redaction_pipeline

    def _ingest(source_path, file_format, out_name, *, text_columns,
                multi_column_strategy="concat", use_ai_query=False):
        out_fqn = tu._fqn(out_name)
        tu.track_table(out_fqn)
        checkpoint = _vol(f"{out_name}_ckpt")
        schema_loc = _vol(f"{out_name}_schema")
        run_ingestion_redaction_pipeline(
            spark=spark,
            source_path=source_path,
            file_format=file_format,
            output_table=out_fqn,
            text_columns=list(text_columns),
            multi_column_strategy=multi_column_strategy,
            doc_id_column="doc_id",
            checkpoint_path=checkpoint,
            schema_location=schema_loc,
            max_files_per_trigger=10,
            use_presidio=True,
            use_ai_query=use_ai_query,
            use_gliner=False,
            presidio_pattern_only=True,
            # Presidio scores bare phone numbers at 0.4; lower the threshold so
            # phone PII is caught by pattern-only detection in this test.
            score_threshold=0.35,
            redaction_strategy="generic",
            endpoint=endpoint,
        )  # availableNow -> blocks until complete
        return verify_output_table(spark, out_fqn)

    # --- 1. Single-column ingestion across all four file formats ---
    for fmt in ["json", "csv", "parquet", "text"]:
        print(f"\n1.{fmt}: single-column ingestion")
        src = _vol(f"src_{fmt}")
        tu.write_sample_files(src, fmt, text_columns=["text"])
        out_df = _ingest(src, fmt, f"out_{fmt}", text_columns=["text"])
        tu.assert_true("text" in out_df.columns, f"{fmt}: redacted 'text' column present")
        verify_no_pii_in_redacted(out_df, "text", KNOWN_PII)

    # --- 2. Multi-column: concat vs sequential (json, two text columns) ---
    print("\n2. Multi-column strategies (json, two text columns)")
    src_multi = _vol("src_multi")
    tu.write_sample_files(src_multi, "json", text_columns=["notes", "contact"])

    concat_df = _ingest(src_multi, "json", "out_concat",
                        text_columns=["notes", "contact"], multi_column_strategy="concat")
    tu.assert_true("notes" in concat_df.columns and "contact" in concat_df.columns,
                   "concat: both text columns present in output")
    verify_no_pii_in_redacted(concat_df, "notes", KNOWN_PII)
    verify_no_pii_in_redacted(concat_df, "contact", KNOWN_PII)

    seq_df = _ingest(src_multi, "json", "out_sequential",
                    text_columns=["notes", "contact"], multi_column_strategy="sequential")
    tu.assert_true("notes" in seq_df.columns and "contact" in seq_df.columns,
                   "sequential: both text columns present in output")
    verify_no_pii_in_redacted(seq_df, "notes", KNOWN_PII)
    verify_no_pii_in_redacted(seq_df, "contact", KNOWN_PII)

    # --- 3. AI Query wired through ingestion (one run, proves the path) ---
    print("\n3. AI Query enabled through ingestion (json)")
    src_ai = _vol("src_ai")
    tu.write_sample_files(src_ai, "json", text_columns=["text"])
    ai_df = _ingest(src_ai, "json", "out_ai", text_columns=["text"], use_ai_query=True)
    verify_no_pii_in_redacted(ai_df, "text", KNOWN_PII)

    test_passed = True
    print_test_result("Test 16: Ingestion Pipeline File Formats", True)

except TestFailure as e:
    error_message = str(e)
    print_test_result("Test 16: Ingestion Pipeline File Formats", False, error_message)
except Exception as e:
    error_message = f"Unexpected error: {e}"
    import traceback; traceback.print_exc()
    print_test_result("Test 16: Ingestion Pipeline File Formats", False, error_message)
finally:
    tu.cleanup_test_artifacts()
    for p in _paths_to_clean:
        try:
            dbutils.fs.rm(p, recurse=True)
        except Exception:
            pass
    print(f"  Cleaned up {len(_paths_to_clean)} volume path(s)")

# COMMAND ----------

if not test_passed:
    raise Exception(f"Test failed: {error_message}")
dbutils.notebook.exit(json.dumps({"passed": test_passed, "error": error_message}))
