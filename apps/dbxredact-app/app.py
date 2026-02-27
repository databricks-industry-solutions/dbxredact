"""FastAPI entry point -- serves API routes and React SPA static files."""

import logging
import os
import uuid

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from api.routes import api_router
from api.services.db import execute, fetch_one, _table, CATALOG, SCHEMA

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="dbxredact", version="0.0.2")
app.include_router(api_router, prefix="/api")


_DEBUG = os.environ.get("DEBUG", "").lower() in ("1", "true", "yes")


@app.exception_handler(Exception)
async def global_error_handler(request: Request, exc: Exception):
    logger.error("Unhandled error on %s: %s", request.url.path, exc, exc_info=True)
    detail = str(exc) if _DEBUG else "Internal server error"
    return JSONResponse(status_code=500, content={"error": detail})


# ---------------------------------------------------------------------------
# Startup: auto-create tables + seed default config
# ---------------------------------------------------------------------------

TABLE_DDLS = [
    f"""CREATE TABLE IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.redact_config (
        config_id STRING, name STRING, use_presidio BOOLEAN, use_ai_query BOOLEAN,
        use_gliner BOOLEAN, endpoint STRING, score_threshold DOUBLE,
        gliner_model STRING, gliner_threshold DOUBLE, redaction_strategy STRING,
        alignment_mode STRING, extra_params STRING, created_at TIMESTAMP, updated_at TIMESTAMP
    )""",
    f"""CREATE TABLE IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.redact_block_list (
        entry_id STRING, value STRING, is_pattern BOOLEAN, entity_type STRING,
        notes STRING, created_at TIMESTAMP
    )""",
    f"""CREATE TABLE IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.redact_safe_list (
        entry_id STRING, value STRING, is_pattern BOOLEAN, entity_type STRING,
        notes STRING, created_at TIMESTAMP
    )""",
    f"""CREATE TABLE IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.redact_annotations (
        annotation_id STRING, doc_id STRING, source_table STRING,
        workflow STRING, entity_text STRING, entity_type STRING,
        start INT, end_pos INT, action STRING, corrected_type STRING,
        corrected_value STRING, detection_method STRING, created_at TIMESTAMP
    )""",
    f"""CREATE TABLE IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.redact_job_history (
        run_id BIGINT, config_id STRING, source_table STRING, output_table STRING,
        status STRING, cost_estimate_usd DOUBLE, started_at TIMESTAMP, completed_at TIMESTAMP
    )""",
    f"""CREATE TABLE IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.redact_ab_tests (
        test_id STRING, name STRING, config_a_id STRING, config_b_id STRING,
        source_table STRING, sample_size INT, status STRING, metrics_a STRING,
        metrics_b STRING, winner STRING, created_at TIMESTAMP, completed_at TIMESTAMP
    )""",
    f"""CREATE TABLE IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.redact_active_learn_queue (
        entry_id STRING, doc_id STRING, source_table STRING, priority_score DOUBLE,
        status STRING, assigned_to STRING, created_at TIMESTAMP, reviewed_at TIMESTAMP
    )""",
]

DEFAULT_CONFIG = {
    "config_id": str(uuid.uuid4()),
    "name": "default",
    "use_presidio": True,
    "use_ai_query": True,
    "use_gliner": False,
    "endpoint": "databricks-gpt-oss-120b",
    "score_threshold": 0.5,
    "gliner_model": "nvidia/gliner-PII",
    "gliner_threshold": 0.2,
    "redaction_strategy": "typed",
    "alignment_mode": "union",
}


@app.on_event("startup")
async def on_startup():
    if not os.environ.get("DATABRICKS_WAREHOUSE_ID"):
        logger.warning("DATABRICKS_WAREHOUSE_ID not set -- skipping table setup")
        return
    for ddl in TABLE_DDLS:
        try:
            execute(ddl)
        except Exception as e:
            logger.error("Failed to create table: %s", e)
    try:
        row = fetch_one(f"SELECT count(*) as cnt FROM {_table('redact_config')}")
        if row and int(row.get("cnt", 0)) == 0:
            execute(
                f"""INSERT INTO {_table('redact_config')}
                (config_id, name, use_presidio, use_ai_query, use_gliner, endpoint,
                 score_threshold, gliner_model, gliner_threshold, redaction_strategy,
                 alignment_mode, extra_params, created_at, updated_at)
                VALUES (%(config_id)s, %(name)s, %(use_presidio)s, %(use_ai_query)s,
                        %(use_gliner)s, %(endpoint)s, %(score_threshold)s, %(gliner_model)s,
                        %(gliner_threshold)s, %(redaction_strategy)s, %(alignment_mode)s,
                        NULL, current_timestamp(), current_timestamp())""",
                DEFAULT_CONFIG,
            )
            logger.info("Seeded default config")
    except Exception as e:
        logger.error("Failed to seed default config: %s", e)


# ---------------------------------------------------------------------------
# SPA static file serving
# ---------------------------------------------------------------------------

build_dir = os.path.join(os.path.dirname(__file__), "dist")
if os.path.isdir(build_dir):
    assets_dir = os.path.join(build_dir, "assets")
    if os.path.isdir(assets_dir):
        app.mount("/assets", StaticFiles(directory=assets_dir), name="assets")

    @app.get("/{path:path}")
    async def serve_spa(path: str):
        file_path = os.path.join(build_dir, path)
        if os.path.isfile(file_path):
            return FileResponse(file_path)
        return FileResponse(os.path.join(build_dir, "index.html"))
