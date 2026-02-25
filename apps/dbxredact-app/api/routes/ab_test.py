"""A/B test management routes."""

import logging
import uuid
from typing import List
from fastapi import APIRouter, HTTPException
from api.models.schemas import ABTestCreate, ABTestResponse
from api.services.db import fetch_all, fetch_one, execute, _table
from api.services.jobs import trigger_benchmark_run
from api.routes.benchmark import _config_to_job_params

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/", response_model=List[ABTestResponse])
async def list_ab_tests():
    return fetch_all(f"SELECT * FROM {_table('redact_ab_tests')} ORDER BY created_at DESC")


@router.get("/{test_id}", response_model=ABTestResponse)
async def get_ab_test(test_id: str):
    row = fetch_one(
        f"SELECT * FROM {_table('redact_ab_tests')} WHERE test_id = %(test_id)s",
        {"test_id": test_id},
    )
    if not row:
        raise HTTPException(404, "A/B test not found")
    return row


@router.post("/", response_model=ABTestResponse, status_code=201)
async def create_ab_test(body: ABTestCreate):
    for cid in [body.config_a_id, body.config_b_id]:
        cfg = fetch_one(
            f"SELECT config_id FROM {_table('redact_config')} WHERE config_id = %(cid)s",
            {"cid": cid},
        )
        if not cfg:
            raise HTTPException(400, f"Config {cid} not found")

    test_id = str(uuid.uuid4())
    execute(
        f"""INSERT INTO {_table('redact_ab_tests')}
        (test_id, name, config_a_id, config_b_id, source_table, sample_size,
         status, created_at)
        VALUES (%(test_id)s, %(name)s, %(config_a_id)s, %(config_b_id)s,
                %(source_table)s, %(sample_size)s, 'created', current_timestamp())""",
        {"test_id": test_id, **body.model_dump()},
    )
    return {**body.model_dump(), "test_id": test_id, "status": "created"}


@router.post("/{test_id}/run")
async def run_ab_test(test_id: str):
    """Trigger two benchmark runs -- one per config variant."""
    test = fetch_one(
        f"SELECT * FROM {_table('redact_ab_tests')} WHERE test_id = %(test_id)s",
        {"test_id": test_id},
    )
    if not test:
        raise HTTPException(404, "A/B test not found")

    results = {}
    for variant, config_id in [("a", test["config_a_id"]), ("b", test["config_b_id"])]:
        config = fetch_one(
            f"SELECT * FROM {_table('redact_config')} WHERE config_id = %(cid)s",
            {"cid": config_id},
        )
        if not config:
            raise HTTPException(400, f"Config {config_id} not found")

        params = _config_to_job_params(config)
        params["source_table"] = test["source_table"]
        logger.info("A/B test %s variant %s job_parameters: %s", test_id[:8], variant, params)

        run_id = trigger_benchmark_run(params)
        results[variant] = {"run_id": run_id}

    execute(
        f"UPDATE {_table('redact_ab_tests')} SET status='running' WHERE test_id = %(test_id)s",
        {"test_id": test_id},
    )

    return {"test_id": test_id, "status": "running", "runs": results}
