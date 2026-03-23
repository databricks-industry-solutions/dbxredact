"""Benchmark job triggering routes."""

import logging
from typing import List, Optional
from pydantic import BaseModel
from fastapi import APIRouter, HTTPException
from api.models.schemas import JobHistoryItem
from api.services.jobs import trigger_benchmark_run, get_run_status, cancel_run
from api.services.db import fetch_all, fetch_one, execute, _table

logger = logging.getLogger(__name__)
router = APIRouter()

TERMINAL_STATES = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}


class BenchmarkRequest(BaseModel):
    source_table: Optional[str] = None
    config_id: Optional[str] = None


def _config_to_job_params(config: dict) -> dict[str, str]:
    """Extract job parameters from a config row (DB returns all values as strings)."""
    return {
        "endpoint": config.get("endpoint") or "databricks-gpt-oss-120b",
        "alignment_mode": config.get("alignment_mode") or "union",
        "use_presidio": str(config.get("use_presidio", "true")).lower(),
        "use_ai_query": str(config.get("use_ai_query", "true")).lower(),
        "use_gliner": str(config.get("use_gliner", "false")).lower(),
        "score_threshold": str(config.get("score_threshold", "0.5")),
        "detection_profile": config.get("detection_profile") or "fast",
        "reasoning_effort": config.get("reasoning_effort") or "low",
        "gliner_max_words": str(config.get("gliner_max_words", 256)),
        "presidio_model_size": config.get("presidio_model_size") or "trf",
        "presidio_pattern_only": str(config.get("presidio_pattern_only", "false")).lower(),
    }


def _fetch_config(config_id: str) -> dict:
    config = fetch_one(
        f"SELECT * FROM {_table('redact_config')} WHERE config_id = %(cid)s",
        {"cid": config_id},
    )
    if not config:
        raise HTTPException(404, "Config not found")
    return config


@router.post("/run")
async def run_benchmark(body: BenchmarkRequest):
    params: dict[str, str] = {}

    if body.source_table:
        params["source_table"] = body.source_table

    if body.config_id:
        config = _fetch_config(body.config_id)
        params.update(_config_to_job_params(config))

    logger.info("Benchmark job_parameters: %s", params)
    run_id = trigger_benchmark_run(params or None)
    status = get_run_status(run_id)

    output_table = f"{body.source_table}_detection_results" if body.source_table else ""
    execute(
        f"""INSERT INTO {_table('redact_job_history')}
        (run_id, config_id, source_table, output_table, status, started_at, run_page_url, job_type)
        VALUES (%(run_id)s, %(config_id)s, %(source_table)s, %(output_table)s, 'RUNNING', current_timestamp(), %(url)s, 'benchmark')""",
        {"run_id": run_id, "config_id": body.config_id or "",
         "source_table": body.source_table or "", "output_table": output_table,
         "url": status.get("run_page_url")},
    )

    return status


@router.get("/status/{run_id}")
async def benchmark_status(run_id: int):
    status = get_run_status(run_id)
    if status.get("state") in TERMINAL_STATES:
        result_val = status.get("result_state", "") or status.get("state", "")
        execute(
            f"""UPDATE {_table('redact_job_history')}
            SET status = %(status)s, completed_at = current_timestamp(),
                run_page_url = COALESCE(run_page_url, %(url)s)
            WHERE run_id = %(run_id)s AND completed_at IS NULL""",
            {"run_id": run_id, "status": result_val, "url": status.get("run_page_url")},
        )
    return status


@router.post("/cancel/{run_id}", status_code=204)
async def cancel_benchmark(run_id: int):
    cancel_run(run_id)
    execute(
        f"UPDATE {_table('redact_job_history')} SET status='CANCELLED', completed_at=current_timestamp() WHERE run_id = %(run_id)s",
        {"run_id": run_id},
    )


@router.get("/history", response_model=List[JobHistoryItem])
async def benchmark_history(limit: int = 50, offset: int = 0):
    limit = max(1, min(limit, 500))
    offset = max(0, offset)
    rows = fetch_all(
        f"""SELECT * FROM {_table('redact_job_history')}
        WHERE job_type = 'benchmark'
        ORDER BY started_at DESC LIMIT %(limit)s OFFSET %(offset)s""",
        {"limit": limit, "offset": offset},
    )
    for row in rows:
        if row.get("status") != "RUNNING":
            continue
        try:
            live = get_run_status(int(row["run_id"]))
            state = live.get("state", "")
            if state not in TERMINAL_STATES:
                if live.get("run_page_url") and not row.get("run_page_url"):
                    row["run_page_url"] = live["run_page_url"]
                continue
            result = live.get("result_state", "") or state
            row["status"] = result
            row["run_page_url"] = row.get("run_page_url") or live.get("run_page_url")
            execute(
                f"""UPDATE {_table('redact_job_history')}
                SET status = %(status)s, completed_at = current_timestamp(),
                    run_page_url = COALESCE(run_page_url, %(url)s)
                WHERE run_id = %(run_id)s AND completed_at IS NULL""",
                {"run_id": row["run_id"], "status": result, "url": live.get("run_page_url")},
            )
        except Exception as exc:
            logger.warning("Failed to reconcile benchmark run %s: %s", row.get("run_id"), exc)
    return rows
