"""Pipeline job triggering and status routes."""

import logging
from typing import List
from fastapi import APIRouter, HTTPException
from api.models.schemas import PipelineRunRequest, RunStatusResponse, JobHistoryItem
from api.services.db import fetch_all, fetch_one, execute, _table, quote_table
from api.services.jobs import trigger_pipeline_run, get_run_status, cancel_run

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post("/run", response_model=RunStatusResponse)
async def run_pipeline(body: PipelineRunRequest):
    config = fetch_one(
        f"SELECT * FROM {_table('redact_config')} WHERE config_id = %(config_id)s",
        {"config_id": body.config_id},
    )
    if not config:
        raise HTTPException(404, "Config not found")

    output_table = body.output_table or f"{body.source_table}_redacted"
    notebook_params = {
        "source_table": body.source_table,
        "text_column": body.text_column,
        "doc_id_column": body.doc_id_column,
        "output_table": output_table,
        "use_presidio": str(config.get("use_presidio", True)),
        "use_ai_query": str(config.get("use_ai_query", True)),
        "use_gliner": str(config.get("use_gliner", False)),
        "endpoint": config.get("endpoint", ""),
        "score_threshold": str(config.get("score_threshold", 0.5)),
        "redaction_strategy": config.get("redaction_strategy", "typed"),
        "max_rows": str(body.max_rows or 10000),
    }

    run_id = trigger_pipeline_run(notebook_params)

    execute(
        f"""INSERT INTO {_table('redact_job_history')}
        (run_id, config_id, source_table, output_table, status, started_at)
        VALUES (%(run_id)s, %(config_id)s, %(source_table)s, %(output_table)s, 'RUNNING', current_timestamp())""",
        {"run_id": run_id, "config_id": body.config_id,
         "source_table": body.source_table, "output_table": output_table},
    )

    return get_run_status(run_id)


@router.get("/status/{run_id}", response_model=RunStatusResponse)
async def pipeline_status(run_id: int):
    return get_run_status(run_id)


@router.post("/cancel/{run_id}", status_code=204)
async def cancel_pipeline(run_id: int):
    cancel_run(run_id)
    execute(
        f"UPDATE {_table('redact_job_history')} SET status='CANCELLED', completed_at=current_timestamp() WHERE run_id = %(run_id)s",
        {"run_id": run_id},
    )


@router.get("/history", response_model=List[JobHistoryItem])
async def pipeline_history():
    return fetch_all(f"SELECT * FROM {_table('redact_job_history')} ORDER BY started_at DESC LIMIT 50")


@router.get("/table-info")
async def table_info(table: str):
    """Return column names and row count for a UC table."""
    qualified = quote_table(table)
    cols = fetch_all(f"DESCRIBE TABLE {qualified}")
    column_names = [c["col_name"] for c in cols if not c["col_name"].startswith("#")]
    row = fetch_one(f"SELECT count(*) as cnt FROM {qualified}")
    row_count = int(row["cnt"]) if row else 0
    return {"columns": column_names, "row_count": row_count}
