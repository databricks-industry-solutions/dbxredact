# dbxredact Examples

These notebooks demonstrate using **dbxredact as a pip-installable library** in standalone Databricks notebooks. They are intended for:

- Embedding dbxredact into your own projects or pipelines
- One-off ad-hoc PII/PHI detection and redaction without deploying the full application
- Quick evaluation of dbxredact capabilities

Each notebook installs dbxredact directly from GitHub via `%pip install git+...` and does **not** require cloning the repo or running `deploy.sh`.

## Full Deployment

For the full-featured experience -- including the interactive dashboard, benchmarking jobs, active learning, and the review workflow -- use the main repo's `deploy.sh` workflow as documented in the [top-level README](../README.md).

## Notebooks

| Notebook | Description |
|----------|-------------|
| `01_quickstart_redaction.py` | Detect and redact PII/PHI in an existing table with Presidio + AI Query |
| `02_unstructured_data_ingestion.py` | End-to-end: ingest files from a UC Volume, redact, and inspect results |

## Prerequisites

- Databricks workspace with **Unity Catalog** enabled
- A **model serving endpoint** for AI Query detection (e.g. `databricks-gpt-oss-120b` or any foundation model)
- For Presidio detection: the spaCy model is installed automatically by the notebooks
- Source data: an existing table (notebook 01) or files in a UC Volume (notebook 02)

## Pinning for Production

The examples install from `@main` for convenience. In production, **pin to a git tag** matching the version you tested:

```
%pip install git+https://github.com/databricks-industry-solutions/dbxredact.git@v0.1.2
```
