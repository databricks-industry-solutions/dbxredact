# CLAUDE.md

Guidance for AI assistants working with this repository.

## What is dbxredact?

dbxredact is a Python library + Databricks App for PII/PHI detection and redaction on Unity Catalog tables. It uses an ensemble of three detectors (Presidio, AI Query via LLM, GLiNER NER), aligns their results, and redacts/masks text. Includes benchmarking, evaluation, LLM judge, active learning, and a FastAPI+React dashboard app deployed via Databricks Asset Bundles.

## Build & Test Commands

```bash
# Install dependencies
poetry install --with dev

# Build wheel
poetry build

# Run library unit tests (excludes integration tests)
poetry run pytest tests/ -x -q --ignore=tests/integration

# Run integration tests (requires PySpark)
poetry run pytest tests/integration/ -x -q

# Run app API tests
poetry run pytest apps/dbxredact-app/tests/ -x -q

# Lint
ruff check src/ apps/dbxredact-app/
ruff format --check src/ apps/dbxredact-app/

# Frontend
cd apps/dbxredact-app && npm ci && npm run build
```

**Important:** Library unit tests and integration tests must be run separately. Integration tests use real local PySpark and are in a separate CI job. Never combine them in a single pytest invocation.

### Frontend (React App)

```bash
cd apps/dbxredact-app
npm ci
npm run build   # Output: dist/ (committed to repo)
npm run dev     # Local dev server
```

The built `dist/` directory is committed and synced to Databricks via DAB. Rebuild after any frontend changes.

## Deployment

```bash
# Deploy to Databricks
./deploy.sh dev        # with interactive confirms
./deploy.sh dev -y     # skip confirms

# Validate only (no deploy)
./deploy.sh dev --validate-only
```

Deployment requires a `dev.env` file (copy from `example.env`). Required vars: `DATABRICKS_HOST`, `CATALOG`, `SCHEMA`, `WAREHOUSE_ID`.

`deploy.sh` generates `databricks.yml` from `databricks.yml.template` by stamping env vars -- do not edit `databricks.yml` directly. The script builds the wheel, uploads it, runs `databricks bundle deploy`, and optionally grants UC permissions for the app service principal.

## Architecture

### Core Library (`src/dbxredact/`)

Entry point: `pipeline.py` -> `run_redaction_pipeline()` orchestrates detection, alignment, redaction, and output.

**Key modules:**
- `detection.py` -- Unified entry: `run_detection()` dispatches to enabled detectors
- `presidio.py` / `analyzer.py` -- Presidio NER with spaCy, batch UDFs
- `ai_detector.py` -- LLM-based detection via Databricks AI Query
- `gliner_detector.py` -- GLiNER NER model detection
- `alignment.py` -- Union/consensus alignment across detector outputs
- `redaction.py` -- Text redaction strategies (generic, typed), UDFs
- `masking.py` -- Structured column masking (hash, encrypt, mask)
- `config.py` -- Entity types, `RedactionConfig`, thresholds, GLiNER presets
- `entity_filter.py` -- Safe/block list filtering from YAML or UC tables
- `evaluation.py` -- Precision/recall/F1 metrics, error diagnosis
- `judge.py` -- LLM judge for redaction quality grading
- `active_learning.py` -- Uncertainty scoring, review queue building
- `calibration.py` -- Isotonic regression confidence calibration
- `cost.py` -- Token estimation and cost projection
- `metadata.py` -- UC tag operations, column discovery
- `utils.py` -- Overlap detection, fuzzy matching helpers

### Notebooks (`notebooks/`)

Numbered pipeline: `0_load_benchmark_data` -> `1_benchmarking_detection` -> `2_benchmarking_evaluation` -> `3_benchmarking_redaction` -> `4_redaction_pipeline` (main E2E) -> `5_benchmarking_judge` -> `6_benchmarking_audit` -> `7_benchmarking_next_actions` -> `9_gliner_fine_tuning` -> `10_setup_app_tables`.

### Web Dashboard (`apps/dbxredact-app/`)

**Backend:** FastAPI (`api/`) with route modules: config, pipeline, benchmark, review, metrics, lists, labels, ab_test, active_learn, catalog, admin. Services layer (`api/services/`) wraps Databricks SDK for job triggering and SQL warehouse for DB operations.

**Frontend:** React 19 + Vite + Tailwind CSS. Pages: Home, Config, Run, Benchmark, Review, Metrics, Lists, Labels, ABTest (preview), ActiveLearn (preview), Admin.

### Databricks Asset Bundles

- `databricks.yml.template` -- Bundle config with targets (dev, demo, prod)
- `variables.yml` -- Runtime parameters (catalog, schema, detection settings, cluster types)
- `resources/jobs.yml` -- Benchmark job + 6 pipeline variants (CPU/GPU x small/medium/large)
- `resources/app.yml` -- App manifest with job resources, warehouse, permissions

## Key Design Patterns

- **Pipeline jobs use `notebook_params`**: Extra keys pass through as widget values. Any parameter can be sent without declaring it in `jobs.yml` `base_parameters`.
- **Benchmark jobs use `job_parameters`**: Only keys declared in the job's `parameters` block are accepted. **Undeclared parameters are silently dropped.** This is a critical distinction -- always declare new benchmark params in both `_config_to_job_params()` in `benchmark.py` AND the `parameters` block in `jobs.yml`.
- **Config-driven**: Detection configs live in a UC table (`redact_config`). The ConfigPage creates them, RunPage/BenchmarkPage reference them by `config_id`. The route handler fetches the config and maps it to notebook/job params.
- **Ensemble detection**: Three detectors run independently, then `alignment.py` merges results via union (any detector) or consensus (majority agreement).
- **Governance gates**: `pipeline.py` enforces `confirm_destructive` for in-place writes and `allow_consensus_redaction` for consensus mode.

## Known Pitfalls

1. **`job_parameters` vs `notebook_params`**: Benchmark jobs use `job_parameters=` which silently drops undeclared params. Pipeline jobs use `notebook_params=` which accepts everything. When adding a new config field, you must add it to: (a) the Pydantic schema, (b) `_config_to_job_params()` in `benchmark.py`, (c) the benchmark job `parameters` block in `jobs.yml`, (d) the `run_detection` task `base_parameters` in `jobs.yml`.

2. **`WAREHOUSE_ID` vs `DATABRICKS_WAREHOUSE_ID`**: `example.env` uses `WAREHOUSE_ID` for deploy script validation. The running app reads `DATABRICKS_WAREHOUSE_ID` (set by the bundle in `resources/app.yml`). For local development, set `DATABRICKS_WAREHOUSE_ID`.

3. **A/B testing is incomplete**: The backend starts two benchmark jobs but never polls completion or writes metrics. The Run button is disabled with "Coming Soon" in the UI.

4. **Active learning review is queue-only**: "Mark Reviewed" removes docs from the queue but does not capture entity corrections. Full correction UI is not yet implemented.

5. **Test isolation**: App tests (`apps/dbxredact-app/tests/`) must be run separately from library tests. Integration tests (`tests/integration/`) use real PySpark and are in their own CI job.

## Test Conventions

- Library tests use local PySpark via `conftest.py` session-scoped `spark` fixture
- App tests use FastAPI `TestClient` with mocked `WorkspaceClient` and DB layer
- Mock helpers (`_mock_fetch_all`, `_mock_fetch_one`, `_mock_execute`) dispatch by SQL table name
- CI enforces >= 40% library coverage and runs ruff lint/format checks

## Version

Version is tracked in `pyproject.toml` only.
