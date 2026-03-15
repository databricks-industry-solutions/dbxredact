"""Benchmark job triggering routes."""

import logging
from pydantic import BaseModel
from typing import Optional
from fastapi import APIRouter, HTTPException
from api.services.jobs import trigger_benchmark_run, get_run_status
from api.services.db import fetch_one, _table

logger = logging.getLogger(__name__)
router = APIRouter()


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
    return get_run_status(run_id)


@router.get("/status/{run_id}")
async def benchmark_status(run_id: int):
    return get_run_status(run_id)
