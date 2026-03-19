# Databricks notebook source
# MAGIC %md
# MAGIC # End-to-End Redaction Pipeline
# MAGIC
# MAGIC This notebook provides a complete end-to-end pipeline for PII/PHI detection and redaction.
# MAGIC
# MAGIC **Input Modes:**
# MAGIC - **Table + Column**: Specify table name and text column directly
# MAGIC - **Table + Tag**: Query Unity Catalog for columns with specific classification tags
# MAGIC
# MAGIC **Refresh Approaches:**
# MAGIC - **full**: Batch processing with overwrite (existing behavior)
# MAGIC - **incremental**: Structured Streaming with append mode
# MAGIC
# MAGIC **Output Strategies:**
# MAGIC - **validation**: Include all detection columns for debugging
# MAGIC - **production**: Only doc_id and redacted text columns

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install dbxredact
# MAGIC
# MAGIC When running via a Databricks Asset Bundle job, the wheel is attached as a cluster library automatically.
# MAGIC For interactive use, uncomment and update the `%pip install` line below.
# MAGIC Uncomment the spaCy model lines if using Presidio detection.

# COMMAND ----------

# MAGIC %pip install https://github.com/explosion/spacy-models/releases/download/en_core_web_trf-3.8.0/en_core_web_trf-3.8.0-py3-none-any.whl
# MAGIC # For faster CPU inference at the cost of lower NER accuracy:
# MAGIC # %pip install https://github.com/explosion/spacy-models/releases/download/en_core_web_lg-3.8.0/en_core_web_lg-3.8.0-py3-none-any.whl
# MAGIC # %pip install https://github.com/explosion/spacy-models/releases/download/es_core_news_lg-3.8.0/es_core_news_lg-3.8.0-py3-none-any.whl
# MAGIC # For interactive use (not running via DAB job), also uncomment one of the following:
# MAGIC # %pip install /Workspace/<path-to-bundle>/artifacts/dbxredact-0.2.0-py3-none-any.whl
# MAGIC # %pip install git+https://github.com/databricks-industry-solutions/dbxredact.git
# MAGIC %restart_python

# COMMAND ----------

import os

from dbxredact import (
    run_redaction_pipeline,
    run_redaction_pipeline_streaming,
    run_redaction_pipeline_by_tag,
    get_columns_by_tag,
    load_filter_from_table,
    EntityFilter,
    RedactionConfig,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget Configuration

# COMMAND ----------

dbutils.widgets.dropdown(
    name="detection_profile",
    defaultValue="fast",
    choices=["fast", "deep", "custom"],
    label="0. Detection Profile",
)
dbutils.widgets.dropdown(
    name="input_mode",
    defaultValue="table_column",
    choices=["table_column", "table_tag"],
    label="1. Input Mode",
)
dbutils.widgets.text(
    name="source_table",
    defaultValue="your_catalog.your_schema.jsl_benchmark_source",
    label="2. Source Table (fully qualified)",
)
dbutils.widgets.text(
    name="text_column",
    defaultValue="text",
    label="3a. Text Column (for table_column mode)",
)
dbutils.widgets.text(
    name="tag_name",
    defaultValue="data_classification",
    label="3b. Tag Name (for table_tag mode)",
)
dbutils.widgets.text(
    name="tag_value",
    defaultValue="protected",
    label="3c. Tag Value (for table_tag mode)",
)
dbutils.widgets.text(
    name="doc_id_column", defaultValue="doc_id", label="4. Document ID Column"
)
dbutils.widgets.dropdown(
    name="use_presidio",
    defaultValue="true",
    choices=["true", "false"],
    label="5. Use Presidio Detection",
)
dbutils.widgets.dropdown(
    name="use_ai_query",
    defaultValue="true",
    choices=["true", "false"],
    label="6. Use AI Query Detection",
)
dbutils.widgets.dropdown(
    name="use_gliner",
    defaultValue="true",
    choices=["true", "false"],
    label="7. Use GLiNER Detection",
)
dbutils.widgets.dropdown(
    name="redaction_strategy",
    defaultValue="typed",
    choices=["generic", "typed"],
    label="8. Redaction Strategy",
)
dbutils.widgets.text(
    name="endpoint",
    defaultValue="databricks-gpt-oss-120b",
    label="9. AI Endpoint (for AI Query method)",
)
dbutils.widgets.text(
    name="score_threshold",
    defaultValue="0.5",
    label="10. Score Threshold (Presidio minimum confidence)",
)
dbutils.widgets.text(name="num_cores", defaultValue="0", label="11. Number of Cores (0=auto)")
dbutils.widgets.text(
    name="output_table",
    defaultValue="",
    label="12. Output Table (leave blank for auto-suffix)",
)
dbutils.widgets.dropdown(
    name="refresh_approach",
    defaultValue="full",
    choices=["full", "incremental"],
    label="13. Refresh Approach",
)
dbutils.widgets.dropdown(
    name="output_strategy",
    defaultValue="production",
    choices=["validation", "production"],
    label="14. Output Strategy (validation = debug, includes raw PII)",
)
dbutils.widgets.dropdown(
    name="confirm_validation_output",
    defaultValue="false",
    choices=["true", "false"],
    label="14b. Confirm Validation Output (required if output_strategy=validation)",
)
dbutils.widgets.text(
    name="checkpoint_path",
    defaultValue="",
    label="15. Checkpoint Path (for incremental, leave blank for auto)",
)
dbutils.widgets.text(
    name="max_rows",
    defaultValue="10000",
    label="16. Max Rows (0 for unlimited)",
)
dbutils.widgets.dropdown(
    name="alignment_mode",
    defaultValue="union",
    choices=["union", "consensus"],
    label="17. Alignment Mode (union=recall, consensus=precision)",
)
dbutils.widgets.text(
    name="max_files_per_trigger",
    defaultValue="10",
    label="18. Max Files Per Trigger (incremental only, 0 for unlimited)",
)
dbutils.widgets.dropdown(
    name="reasoning_effort",
    defaultValue="low",
    choices=["low", "medium", "high"],
    label="19. Reasoning Effort (AI Query)",
)
dbutils.widgets.text(
    name="gliner_max_words",
    defaultValue="256",
    label="20. GLiNER Max Words (chunk size)",
)
dbutils.widgets.text(
    name="safe_list_table",
    defaultValue="",
    label="21. Safe List Table (optional, fully qualified)",
)
dbutils.widgets.text(
    name="block_list_table",
    defaultValue="",
    label="22. Block List Table (optional, fully qualified)",
)
dbutils.widgets.dropdown(
    name="output_mode",
    defaultValue="separate",
    choices=["separate", "in_place"],
    label="23. Output Mode (separate=new table, in_place=overwrite source column)",
)
dbutils.widgets.dropdown(
    name="confirm_destructive",
    defaultValue="false",
    choices=["true", "false"],
    label="24. Confirm Destructive (required for in_place)",
)
dbutils.widgets.dropdown(
    name="presidio_pattern_only",
    defaultValue="false",
    choices=["true", "false"],
    label="25. Presidio Pattern Only (regex-only, no spaCy NER)",
)
dbutils.widgets.text(
    name="presidio_model_size",
    defaultValue="trf",
    label="26. Presidio Model Size (trf, lg, or md)",
)
dbutils.widgets.text(
    name="gliner_threshold",
    defaultValue="0.2",
    label="27. GLiNER Confidence Threshold",
)
dbutils.widgets.text(
    name="gliner_model",
    defaultValue="nvidia/gliner-PII",
    label="28. GLiNER Model Name",
)

# COMMAND ----------

detection_profile = dbutils.widgets.get("detection_profile")
input_mode = dbutils.widgets.get("input_mode")
source_table = dbutils.widgets.get("source_table")

if "your_catalog" in source_table or "your_schema" in source_table:
    raise ValueError(
        "Please update the 'source_table' widget with your actual catalog and schema names. "
        "The defaults (your_catalog.your_schema) are placeholders."
    )
text_column = dbutils.widgets.get("text_column")
tag_name = dbutils.widgets.get("tag_name")
tag_value = dbutils.widgets.get("tag_value")
doc_id_column = dbutils.widgets.get("doc_id_column")
use_presidio = dbutils.widgets.get("use_presidio") == "true"
use_ai_query = dbutils.widgets.get("use_ai_query") == "true"
use_gliner = dbutils.widgets.get("use_gliner") == "true"
redaction_strategy = dbutils.widgets.get("redaction_strategy")
endpoint = dbutils.widgets.get("endpoint")
score_threshold = float(dbutils.widgets.get("score_threshold"))
num_cores = int(dbutils.widgets.get("num_cores"))
if num_cores <= 0:
    try:
        num_cores = sc.defaultParallelism
    except Exception:
        num_cores = 8
    print(f"Auto-detected {num_cores} task slots")
output_table = dbutils.widgets.get("output_table")
refresh_approach = dbutils.widgets.get("refresh_approach")
output_strategy = dbutils.widgets.get("output_strategy")
checkpoint_path = dbutils.widgets.get("checkpoint_path")
max_rows_str = dbutils.widgets.get("max_rows")
max_rows = None if max_rows_str == "0" else int(max_rows_str)
alignment_mode = dbutils.widgets.get("alignment_mode")
max_files_str = dbutils.widgets.get("max_files_per_trigger")
max_files_per_trigger = None if max_files_str == "0" else int(max_files_str)
reasoning_effort = dbutils.widgets.get("reasoning_effort")
gliner_max_words = int(dbutils.widgets.get("gliner_max_words"))
safe_list_table = dbutils.widgets.get("safe_list_table").strip()
block_list_table = dbutils.widgets.get("block_list_table").strip()
output_mode = dbutils.widgets.get("output_mode")
confirm_destructive = dbutils.widgets.get("confirm_destructive") == "true"
confirm_validation_output = dbutils.widgets.get("confirm_validation_output") == "true"
presidio_pattern_only = dbutils.widgets.get("presidio_pattern_only") == "true"
presidio_model_size = dbutils.widgets.get("presidio_model_size").strip() or None
gliner_threshold = float(dbutils.widgets.get("gliner_threshold"))
gliner_model = dbutils.widgets.get("gliner_model").strip()

# Profile overrides (fast/deep force specific settings; custom uses widget values as-is)
if detection_profile == "fast":
    use_presidio, use_ai_query, use_gliner = True, True, True
    reasoning_effort, gliner_max_words = "low", 256
    presidio_pattern_only = True
    print("Profile: Fast Mode -- AI Query + GLiNER + Presidio (pattern-only), reasoning=low, max_words=256")
elif detection_profile == "deep":
    use_presidio, use_ai_query, use_gliner = True, True, True
    reasoning_effort, gliner_max_words = "medium", 256
    presidio_pattern_only = False
    print("Profile: Deep Search -- all detectors, reasoning=medium, max_words=256")

entity_filter = None
if safe_list_table or block_list_table:
    ef = EntityFilter()
    if safe_list_table:
        safe_ef = load_filter_from_table(spark, safe_list_table, list_type="safe")
        ef.safe_list = safe_ef.safe_list
        ef.safe_patterns = safe_ef.safe_patterns
        ef._safe_set = safe_ef._safe_set
        ef._safe_re = safe_ef._safe_re
        print(f"Loaded safe list: {len(ef.safe_list)} exact, {len(ef.safe_patterns)} patterns")
    if block_list_table:
        block_ef = load_filter_from_table(spark, block_list_table, list_type="block")
        ef.block_list = block_ef.block_list
        ef.block_patterns = block_ef.block_patterns
        ef._block_set = block_ef._block_set
        ef._block_re = block_ef._block_re
        print(f"Loaded block list: {len(ef.block_list)} exact, {len(ef.block_patterns)} patterns")
    entity_filter = ef

config = RedactionConfig(
    use_presidio=use_presidio,
    use_ai_query=use_ai_query,
    use_gliner=use_gliner,
    endpoint=endpoint if use_ai_query else None,
    score_threshold=score_threshold,
    gliner_model=gliner_model,
    gliner_threshold=gliner_threshold,
    gliner_max_words=gliner_max_words,
    num_cores=num_cores,
    redaction_strategy=redaction_strategy,
    output_strategy=output_strategy,
    output_mode=output_mode,
    confirm_destructive=confirm_destructive,
    confirm_validation_output=confirm_validation_output,
    max_rows=max_rows,
    alignment_mode=alignment_mode,
    reasoning_effort=reasoning_effort,
    presidio_model_size=presidio_model_size,
    presidio_pattern_only=presidio_pattern_only,
    entity_filter=entity_filter,
)

if not any([use_presidio, use_ai_query, use_gliner]):
    raise ValueError("At least one detection method must be enabled.")
if not 0.0 <= score_threshold <= 1.0:
    raise ValueError(f"Presidio score threshold must be in [0.0, 1.0], got {score_threshold}")
if num_cores < 1:
    raise ValueError(f"Number of cores must be a positive integer, got {num_cores}")

if input_mode == "table_column":
    _source_columns = [c.name for c in spark.table(source_table).schema]
    for _col in [doc_id_column, text_column]:
        if _col not in _source_columns:
            raise ValueError(f"Column '{_col}' not found in {source_table}. Available: {_source_columns}")

if output_mode == "in_place":
    if not confirm_destructive:
        raise ValueError(
            "In-place redaction will permanently overwrite the text column in the source table. "
            "Set 'Confirm Destructive' to 'true' to proceed."
        )
    print(f"WARNING: In-place mode will PERMANENTLY overwrite '{text_column}' in {source_table}.")
    output_table = None
elif not output_table:
    output_table = f"{source_table}_redacted"

# Auto-generate checkpoint path if not provided
if not checkpoint_path and refresh_approach == "incremental":
    _ckpt_ref = source_table if output_mode == "in_place" else output_table
    table_name_only = _ckpt_ref.split(".")[-1]
    catalog = _ckpt_ref.split(".")[0] if "." in _ckpt_ref else "main"
    schema = _ckpt_ref.split(".")[1] if _ckpt_ref.count(".") >= 1 else "default"
    checkpoint_path = f"/Volumes/{catalog}/{schema}/checkpoints/{table_name_only}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbr_version = os.environ.get("DATABRICKS_RUNTIME_VERSION", "")
if "client" not in dbr_version:
    spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", 100)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Input Mode

# COMMAND ----------

if input_mode == "table_tag":
    print(f"Input Mode: Table + Tag")
    print(f"Searching for columns with {tag_name}='{tag_value}' in {source_table}")

    protected_columns = get_columns_by_tag(
        spark=spark, table_name=source_table, tag_name=tag_name, tag_value=tag_value
    )

    if not protected_columns:
        raise ValueError(
            f"No columns found with {tag_name}='{tag_value}' in {source_table}"
        )

    print(f"Found {len(protected_columns)} protected column(s): {protected_columns}")
    text_column = protected_columns[0]
    print(f"Using first column for redaction: {text_column}")
else:
    print(f"Input Mode: Table + Column")
    print(f"Using specified column: {text_column}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Redaction Pipeline

# COMMAND ----------

print("=" * 80)
print("STARTING REDACTION PIPELINE")
print("=" * 80)
print(f"Detection Profile: {detection_profile}")
print(f"Source Table: {source_table}")
print(f"Text Column: {text_column}")
print(f"Use Presidio: {use_presidio}")
print(f"Use AI Query: {use_ai_query}")
print(f"Use GLiNER: {use_gliner}")
print(f"Reasoning Effort: {reasoning_effort}")
print(f"GLiNER Max Words: {gliner_max_words}")
print(f"Redaction Strategy: {redaction_strategy}")
print(f"Output Mode: {output_mode}")
if output_mode == "in_place":
    print(f"  -> DESTRUCTIVE: overwriting {text_column} in {source_table}")
else:
    print(f"Output Table: {output_table}")
print(f"Refresh Approach: {refresh_approach}")
print(f"Output Strategy: {output_strategy}")
print(f"Alignment Mode: {alignment_mode}")
if entity_filter:
    print(f"Entity Filter: safe={len(entity_filter.safe_list)}+{len(entity_filter.safe_patterns)}pat, block={len(entity_filter.block_list)}+{len(entity_filter.block_patterns)}pat")
if refresh_approach == "incremental":
    print(f"Checkpoint Path: {checkpoint_path}")
print("=" * 80)

# COMMAND ----------

if refresh_approach == "incremental":
    # Streaming approach - incremental processing
    # TIP: For tables over 100k rows, streaming (incremental) is recommended for
    # lower memory pressure, no row-count limits, and incremental updates.
    print("Using INCREMENTAL (streaming) approach...")

    query = run_redaction_pipeline_streaming(
        spark=spark,
        source_table=source_table,
        text_column=text_column,
        output_table=output_table,
        checkpoint_path=checkpoint_path,
        doc_id_column=doc_id_column,
        max_files_per_trigger=max_files_per_trigger,
        config=config,
    )

    # Wait for completion (availableNow trigger processes all then stops)
    query.awaitTermination()

    # Load result for display
    result_df = spark.table(output_table)

else:
    # Batch approach - full refresh
    print("Using FULL (batch) approach...")

    if input_mode == "table_tag":
        result_df = run_redaction_pipeline_by_tag(
            spark=spark,
            source_table=source_table,
            output_table=output_table,
            tag_name=tag_name,
            tag_value=tag_value,
            doc_id_column=doc_id_column,
            config=config,
        )
    else:
        result_df = run_redaction_pipeline(
            spark=spark,
            source_table=source_table,
            text_column=text_column,
            output_table=output_table,
            doc_id_column=doc_id_column,
            config=config,
        )

# COMMAND ----------

print("=" * 80)
print("PIPELINE COMPLETE")
print("=" * 80)
if output_mode == "in_place":
    print(f"Source table updated in-place: {source_table}")
else:
    print(f"Redacted table saved to: {output_table}")

# COMMAND ----------

# Read from saved table to avoid re-executing the lazy pipeline (which would re-run AI Query, doubling token costs)
_result_table = source_table if output_mode == "in_place" else output_table
result_df = spark.table(_result_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Results

# COMMAND ----------

display(result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results Summary

# COMMAND ----------

redacted_col_name = f"{text_column}_redacted"

# Check which columns are available based on output_strategy
if redacted_col_name in result_df.columns and text_column in result_df.columns:
    summary_df = result_df.selectExpr(
        "COUNT(*) as total_documents",
        f"AVG(LENGTH({text_column})) as avg_original_length",
        f"AVG(LENGTH({redacted_col_name})) as avg_redacted_length",
    )
    display(summary_df)
elif redacted_col_name in result_df.columns:
    # Production mode - only redacted column available
    summary_df = result_df.selectExpr(
        "COUNT(*) as total_documents",
        f"AVG(LENGTH({redacted_col_name})) as avg_redacted_length",
    )
    display(summary_df)
else:
    print(f"Output table has {result_df.count()} documents")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Comparisons (Validation Mode Only)

# COMMAND ----------

if text_column in result_df.columns and redacted_col_name in result_df.columns:
    comparison_df = result_df.select(doc_id_column, text_column, redacted_col_name)
    display(comparison_df.limit(10))
else:
    print("Sample comparisons only available in validation output mode")
    display(result_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Entity Statistics (Validation Mode Only)

# COMMAND ----------

if output_strategy == "validation":
    if use_presidio and "presidio_results_struct" in result_df.columns:
        print("=== Presidio Detection Stats ===")
        presidio_stats = result_df.selectExpr(
            "COUNT(*) as total_docs",
            "SUM(SIZE(presidio_results_struct)) as total_entities",
            "AVG(SIZE(presidio_results_struct)) as avg_entities_per_doc",
        )
        display(presidio_stats)

# COMMAND ----------

if output_strategy == "validation":
    if use_ai_query and "ai_results_struct" in result_df.columns:
        print("=== AI Query Detection Stats ===")
        ai_stats = result_df.selectExpr(
            "COUNT(*) as total_docs",
            "SUM(SIZE(ai_results_struct)) as total_entities",
            "AVG(SIZE(ai_results_struct)) as avg_entities_per_doc",
        )
        display(ai_stats)

# COMMAND ----------

if output_strategy == "validation":
    if use_gliner and "gliner_results_struct" in result_df.columns:
        print("=== GLiNER Detection Stats ===")
        gliner_stats = result_df.selectExpr(
            "COUNT(*) as total_docs",
            "SUM(SIZE(gliner_results_struct)) as total_entities",
            "AVG(SIZE(gliner_results_struct)) as avg_entities_per_doc",
        )
        display(gliner_stats)

# COMMAND ----------

if output_strategy == "validation":
    if "aligned_entities" in result_df.columns:
        print("=== Aligned Detection Stats ===")
        aligned_stats = result_df.selectExpr(
            "COUNT(*) as total_docs",
            "SUM(SIZE(aligned_entities)) as total_entities",
            "AVG(SIZE(aligned_entities)) as avg_entities_per_doc",
        )
        display(aligned_stats)
