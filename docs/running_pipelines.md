# Running Redaction Pipelines

This guide covers how to run dbxredact redaction pipelines **without** the web application -- via Databricks notebooks, the CLI/bundle, or the Python library directly.

---

## 1. Interactive Notebook

Open `notebooks/4_redaction_pipeline.py` in your Databricks workspace. The notebook exposes all configuration through widgets (parameters) grouped into sections.

### Minimum required parameters

| Parameter | Description |
|-----------|-------------|
| `source_table` | Fully qualified table name (`catalog.schema.table`) |
| `text_column` | Column containing text to scan for PII |
| `output_table` | Where to write results (leave empty to auto-generate) |

### Detection profiles

The `detection_profile` widget controls which detectors run:

| Profile | Detectors | Speed | Use when |
|---------|-----------|-------|----------|
| **fast** | AI Query + GLiNER + Presidio (pattern-only) | Fastest | Routine redaction, large tables |
| **deep** | AI Query + GLiNER + Presidio (full NER) | Slower | Compliance-critical data (HIPAA, GDPR) |
| **custom** | You choose | Varies | Fine-tuning detection behavior |

With `fast` or `deep`, most settings are locked to profile defaults. Switch to `custom` to override individual detectors.

### Input modes

| Mode | Description |
|------|-------------|
| `table_column` | Scan a single text column (default) |
| `table_tag` | Scan all tables with a specific Unity Catalog tag |
| `full_table` | Apply structured masking (hash/encrypt/mask) to all PII-tagged columns |

### Output modes

| Mode | Description |
|------|-------------|
| `separate` | Write results to a new output table (default, safe) |
| `in_place` | Overwrite the source column. Requires `confirm_destructive=true` |

### Key widget reference

**Detectors:**
- `use_presidio` / `use_ai_query` -- toggle Presidio regex and LLM detection
- `ner_backend` -- `gliner`, `privacy_filter`, or `none`
- `endpoint` -- model serving endpoint for AI Query (default: `databricks-gpt-oss-120b`)
- `reasoning_effort` -- `low` / `medium` / `high` (higher = more thorough, slower, costlier)

**Redaction:**
- `redaction_strategy` -- `typed` replaces with `[PERSON]`, `[EMAIL]`, etc.; `generic` uses `[REDACTED]`
- `alignment_mode` -- `union` (any detector flags it) or `consensus` (majority agreement)
- `masking_strategy` -- `mask` (text replacement), `hash` (SHA-256), `encrypt` (AES, requires `encryption_secret`)

**Scale:**
- `max_rows` -- row limit per run (0 = unlimited, default 10000)
- `num_cores` -- parallelism for Presidio/NER UDFs (0 = auto-detect)

**Language:**
- `language` -- `en` or `es` (affects AI Query prompts; Presidio/GLiNER are English-only)
- `translate_to` -- `en` or `none` (post-redaction translation, adds ~3x AI Query cost)

### Example: interactive run

1. Attach the notebook to an ML Runtime cluster (GPU required for `privacy_filter` backend)
2. Set `source_table` to your table, `text_column` to your column
3. Leave `detection_profile` as `fast` for the first run
4. Run all cells
5. Review results in the output table

---

## 2. Databricks CLI / Bundle Jobs

The bundle defines six pipeline job variants in `resources/jobs.yml`:

| Job | Cluster | Workers |
|-----|---------|---------|
| `redaction_pipeline_cpu_small` | ML Runtime, CPU | 2 |
| `redaction_pipeline_cpu_medium` | ML Runtime, CPU | 5 |
| `redaction_pipeline_cpu_large` | ML Runtime, CPU | 10 |
| `redaction_pipeline_gpu_small` | GPU ML Runtime | 2 |
| `redaction_pipeline_gpu_medium` | GPU ML Runtime | 5 |
| `redaction_pipeline_gpu_large` | GPU ML Runtime | 10 |

Use GPU variants when `ner_backend=privacy_filter`. CPU variants are sufficient for GLiNER and Presidio-only detection.

### Running a job

```bash
# Deploy the bundle first (builds wheel, syncs to workspace)
./deploy.sh dev

# Run with default parameters
databricks bundle run redaction_pipeline_cpu_small -t dev -p DEFAULT

# Override parameters
databricks bundle run redaction_pipeline_cpu_small -t dev -p DEFAULT \
  --params source_table='my_catalog.my_schema.clinical_notes',text_column='note_text',output_table='my_catalog.my_schema.clinical_notes_redacted',max_rows='0'
```

### Overriding bundle variables

Parameters that reference `${var.*}` can be overridden at deploy or run time:

```bash
# Via environment variable
export BUNDLE_VAR_language=es
export BUNDLE_VAR_translate_to=en
databricks bundle run redaction_pipeline_gpu_small -t dev -p DEFAULT

# Via --var flag
databricks bundle run redaction_pipeline_cpu_small -t dev -p DEFAULT \
  --var language=es --var translate_to=en
```

Available bundle variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `detection_profile` | `fast` | `fast`, `deep`, or `custom` |
| `max_rows` | `10000` | Row limit (0 = unlimited) |
| `reasoning_effort` | `low` | AI Query reasoning: `low`, `medium`, `high` |
| `gliner_model` | `nvidia/gliner-PII` | NER model name |
| `gliner_max_words` | `256` | NER chunk size in words |
| `language` | `en` | Detection language: `en`, `es` |
| `translate_to` | `none` | Post-redaction translation target |

### Passing arbitrary notebook parameters

Any parameter accepted by the notebook widget can be passed directly:

```bash
databricks bundle run redaction_pipeline_cpu_small -t dev -p DEFAULT \
  --params ner_backend='privacy_filter',language='es',translate_to='en',score_threshold='0.3'
```

Parameters not declared in `base_parameters` are still passed through to the notebook via `notebook_params`.

---

## 3. Python Library (from a notebook or script)

Install the wheel, then call the pipeline functions directly.

### Using `RedactionConfig` (recommended)

```python
from dbxredact.config import RedactionConfig
from dbxredact.pipeline import run_redaction_pipeline

config = RedactionConfig(
    use_presidio=True,
    use_ai_query=True,
    ner_backend="gliner",        # "gliner", "privacy_filter", or "none"
    endpoint="databricks-gpt-oss-120b",
    redaction_strategy="typed",
    alignment_mode="union",
    reasoning_effort="low",
    max_rows=10000,
    language="es",               # Spanish detection prompts
    translate_to="en",           # Translate redacted output to English
)

result_df = run_redaction_pipeline(
    spark=spark,
    source_table="my_catalog.my_schema.clinical_notes",
    text_column="note_text",
    output_table="my_catalog.my_schema.clinical_notes_redacted",
    config=config,
)

display(result_df)
```

When `config=` is passed, its fields override any individual keyword arguments.

### Using keyword arguments

```python
from dbxredact.pipeline import run_redaction_pipeline

result_df = run_redaction_pipeline(
    spark=spark,
    source_table="my_catalog.my_schema.pii_data",
    text_column="text",
    output_table="my_catalog.my_schema.pii_data_redacted",
    use_presidio=True,
    use_ai_query=True,
    use_gliner=True,
    redaction_strategy="typed",
    endpoint="databricks-gpt-oss-120b",
    max_rows=0,  # no limit
)
```

### Tag-based scanning

Scan all tables with a Unity Catalog tag:

```python
from dbxredact.pipeline import run_redaction_pipeline_by_tag

run_redaction_pipeline_by_tag(
    spark=spark,
    tag_name="data_classification",
    tag_value="protected",
    text_column="text",
    config=config,
)
```

### Full-table structured masking

Mask all PII-tagged columns in a table using hash, encrypt, or mask strategies:

```python
from dbxredact.pipeline import run_table_redaction

run_table_redaction(
    spark=spark,
    source_table="my_catalog.my_schema.patient_records",
    masking_strategy="hash",         # "mask", "hash", or "encrypt"
    # encryption_secret="my-scope/aes-key",  # required for "encrypt"
)
```

### Streaming ingestion

For continuous redaction on new files arriving in a volume or table:

```python
from dbxredact.pipeline import run_redaction_pipeline_streaming

run_redaction_pipeline_streaming(
    spark=spark,
    source_path="/Volumes/my_catalog/my_schema/incoming/",
    text_column="content",
    output_table="my_catalog.my_schema.redacted_stream",
    checkpoint_path="/Volumes/my_catalog/my_schema/checkpoints/redact/",
    config=config,
)
```

Note: streaming mode uses Presidio and AI Query only. GLiNER, Privacy Filter, and AI Query text chunking are not available in streaming pipelines.

---

## 4. Entity Filters (safe/block lists)

Control which entities are detected or ignored:

```python
from dbxredact.config import RedactionConfig
from dbxredact.entity_filter import EntityFilter

entity_filter = EntityFilter(
    safe_list_table="my_catalog.my_schema.redact_safe_list",
    block_list_table="my_catalog.my_schema.redact_block_list",
)

config = RedactionConfig(
    entity_filter=entity_filter,
    # ... other settings ...
)
```

- **Safe list**: values that should never be flagged as PII (e.g., company names, product names)
- **Block list**: values that should always be flagged as PII regardless of detector output

Both tables can be managed through the app UI or populated directly via SQL.

---

## 5. Cluster Requirements

| Backend | Runtime | GPU | Notes |
|---------|---------|-----|-------|
| Presidio (pattern-only) | Any ML Runtime | No | Regex only, fastest |
| Presidio (full NER) | ML Runtime | No | Loads spaCy `en_core_web_trf` or `en_core_web_lg` |
| GLiNER | ML Runtime | No | Runs on CPU, moderate speed |
| Privacy Filter | ML Runtime | Yes | HuggingFace transformer model, GPU strongly recommended |
| AI Query | Any | No | Uses model serving endpoint, no local compute needed |

All pipeline jobs use ML Runtime (`17.3.x-scala2.13` for CPU, `17.3.x-gpu-ml-scala2.13` for GPU).
