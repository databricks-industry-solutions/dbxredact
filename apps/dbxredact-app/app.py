"""FastAPI entry point -- serves API routes and React SPA static files."""

import logging
import os
import uuid

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from api.routes import api_router
from api.services.db import execute, fetch_one, _table, CATALOG, SCHEMA, WAREHOUSE_ID, DatabaseError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="dbxredact", version="0.2.0")
app.include_router(api_router, prefix="/api")


_DEBUG = os.environ.get("DEBUG", "").lower() in ("1", "true", "yes")


@app.exception_handler(DatabaseError)
async def database_error_handler(request: Request, exc: DatabaseError):
    logger.warning("Database error on %s: %s", request.url.path, exc.user_message)
    return JSONResponse(status_code=exc.status_code, content={"error": exc.user_message})


@app.exception_handler(Exception)
async def global_error_handler(request: Request, exc: Exception):
    logger.error("Unhandled error on %s: %s", request.url.path, exc, exc_info=True)
    detail = str(exc) if _DEBUG else "Internal server error"
    return JSONResponse(status_code=500, content={"error": detail})


@app.get("/api/health")
async def health_check():
    try:
        fetch_one("SELECT 1 AS ok")
        return {"status": "ok", "warehouse_id": WAREHOUSE_ID}
    except Exception as exc:
        return JSONResponse(
            status_code=503,
            content={"status": "error", "message": str(exc), "warehouse_id": WAREHOUSE_ID},
        )


# ---------------------------------------------------------------------------
# Startup: auto-create tables + seed default config
# ---------------------------------------------------------------------------

TABLE_DDLS = [
    f"""CREATE TABLE IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.redact_config (
        config_id STRING, name STRING, detection_profile STRING, use_presidio BOOLEAN,
        use_ai_query BOOLEAN, use_gliner BOOLEAN, endpoint STRING, score_threshold DOUBLE,
        gliner_model STRING, gliner_threshold DOUBLE, redaction_strategy STRING,
        alignment_mode STRING, reasoning_effort STRING, gliner_max_words INT,
        presidio_model_size STRING, extra_params STRING, created_at TIMESTAMP, updated_at TIMESTAMP
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
    f"""CREATE TABLE IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.redact_ground_truths (
        doc_id STRING, source_table STRING, entity_text STRING, entity_type STRING,
        start INT, end_pos INT, created_at TIMESTAMP
    )""",
]

DEFAULT_CONFIG = {
    "config_id": str(uuid.uuid4()),
    "name": "default",
    "detection_profile": "fast",
    "use_presidio": False,
    "use_ai_query": True,
    "use_gliner": True,
    "endpoint": "databricks-gpt-oss-120b",
    "score_threshold": 0.5,
    "gliner_model": "nvidia/gliner-PII",
    "gliner_threshold": 0.2,
    "redaction_strategy": "typed",
    "alignment_mode": "union",
    "reasoning_effort": "low",
    "gliner_max_words": 512,
    "presidio_model_size": "trf",
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
    # Migrate pre-existing tables that may lack newer columns
    for col_ddl in [
        f"ALTER TABLE {_table('redact_job_history')} ADD COLUMNS (cost_estimate_usd DOUBLE)",
        f"ALTER TABLE {_table('redact_config')} ADD COLUMNS (detection_profile STRING)",
        f"ALTER TABLE {_table('redact_config')} ADD COLUMNS (reasoning_effort STRING)",
        f"ALTER TABLE {_table('redact_config')} ADD COLUMNS (gliner_max_words INT)",
        f"ALTER TABLE {_table('redact_config')} ADD COLUMNS (presidio_model_size STRING)",
    ]:
        try:
            execute(col_ddl)
        except Exception:
            pass  # column already exists
    try:
        row = fetch_one(f"SELECT count(*) as cnt FROM {_table('redact_config')}")
        if row and int(row.get("cnt", 0)) == 0:
            execute(
                f"""INSERT INTO {_table('redact_config')}
                (config_id, name, detection_profile, use_presidio, use_ai_query, use_gliner,
                 endpoint, score_threshold, gliner_model, gliner_threshold, redaction_strategy,
                 alignment_mode, reasoning_effort, gliner_max_words, presidio_model_size,
                 extra_params, created_at, updated_at)
                VALUES (%(config_id)s, %(name)s, %(detection_profile)s, %(use_presidio)s,
                        %(use_ai_query)s, %(use_gliner)s, %(endpoint)s, %(score_threshold)s,
                        %(gliner_model)s, %(gliner_threshold)s, %(redaction_strategy)s,
                        %(alignment_mode)s, %(reasoning_effort)s, %(gliner_max_words)s,
                        %(presidio_model_size)s, NULL, current_timestamp(), current_timestamp())""",
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
        file_path = os.path.realpath(os.path.join(build_dir, path))
        if not file_path.startswith(os.path.realpath(build_dir)):
            return FileResponse(os.path.join(build_dir, "index.html"))
        if os.path.isfile(file_path):
            return FileResponse(file_path)
        return FileResponse(os.path.join(build_dir, "index.html"))
