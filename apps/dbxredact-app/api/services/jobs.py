"""Databricks SDK Jobs API wrapper."""

import os
import logging
from typing import Any, Dict, List, Optional
from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)

_client: Optional[WorkspaceClient] = None
_pipeline_job_id: Optional[int] = None

PIPELINE_JOB_NAME = os.environ.get("PIPELINE_JOB_NAME", "dbxredact - Redaction Pipeline")
BENCHMARK_JOB_NAME = os.environ.get("BENCHMARK_JOB_NAME", "dbxredact - Redaction Benchmark")


def _get_client() -> WorkspaceClient:
    global _client
    if _client is None:
        _client = WorkspaceClient()
    return _client


def _get_pipeline_job_id() -> int:
    """Find the pipeline job ID by partial name match. Cached after first lookup."""
    global _pipeline_job_id
    if _pipeline_job_id is not None:
        return _pipeline_job_id

    w = _get_client()
    for job in w.jobs.list():
        if job.settings and job.settings.name and PIPELINE_JOB_NAME in job.settings.name:
            _pipeline_job_id = job.job_id
            logger.info("Found pipeline job: %s (id=%d)", job.settings.name, _pipeline_job_id)
            return _pipeline_job_id

    raise RuntimeError(f"No job matching '{PIPELINE_JOB_NAME}' found")


_benchmark_job_id: Optional[int] = None


def _get_benchmark_job_id() -> int:
    global _benchmark_job_id
    if _benchmark_job_id is not None:
        return _benchmark_job_id

    w = _get_client()
    for job in w.jobs.list():
        if job.settings and job.settings.name and BENCHMARK_JOB_NAME in job.settings.name:
            _benchmark_job_id = job.job_id
            logger.info("Found benchmark job: %s (id=%d)", job.settings.name, _benchmark_job_id)
            return _benchmark_job_id

    raise RuntimeError(f"No job matching '{BENCHMARK_JOB_NAME}' found")


def trigger_pipeline_run(notebook_params: Optional[Dict[str, str]] = None) -> int:
    job_id = _get_pipeline_job_id()
    w = _get_client()
    response = w.jobs.run_now(job_id=job_id, notebook_params=notebook_params or {})
    return response.run_id


def trigger_benchmark_run(job_parameters: Optional[Dict[str, str]] = None) -> int:
    job_id = _get_benchmark_job_id()
    w = _get_client()
    response = w.jobs.run_now(job_id=job_id, job_parameters=job_parameters or {})
    return response.run_id


def get_run_status(run_id: int) -> Dict[str, Any]:
    w = _get_client()
    run = w.jobs.get_run(run_id=run_id)
    return {
        "run_id": run.run_id,
        "state": run.state.life_cycle_state.value if run.state else None,
        "result_state": run.state.result_state.value if run.state and run.state.result_state else None,
        "start_time": run.start_time,
        "end_time": run.end_time,
        "run_page_url": run.run_page_url,
    }


def cancel_run(run_id: int) -> None:
    w = _get_client()
    w.jobs.cancel_run(run_id=run_id)
