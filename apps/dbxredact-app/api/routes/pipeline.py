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
    # Pre-run cost guardrail
    if body.max_cost_usd is not None and str(config.get("use_ai_query", "false")).lower() == "true":
        est = await cost_estimate(
            table=body.source_table,
            text_column=body.text_column,
            endpoint=config.get("endpoint", "databricks-gpt-oss-120b"),
            max_rows=body.max_rows or 10000,
        )
        if est["estimated_cost_usd"] > body.max_cost_usd:
            raise HTTPException(
                400,
                f"Estimated cost ${est['estimated_cost_usd']:.4f} exceeds limit ${body.max_cost_usd:.4f}",
            )

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

    run_id = trigger_pipeline_run(notebook_params, cluster_profile=body.cluster_profile)

    execute(
        f"""INSERT INTO {_table('redact_job_history')}
        (run_id, config_id, source_table, output_table, status, started_at)
        VALUES (%(run_id)s, %(config_id)s, %(source_table)s, %(output_table)s, 'RUNNING', current_timestamp())""",
        {"run_id": run_id, "config_id": body.config_id,
         "source_table": body.source_table, "output_table": output_table},
    )

    return get_run_status(run_id)


TERMINAL_STATES = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}


def _compute_post_run_cost(run_id: int) -> float | None:
    """Estimate cost for a completed run based on its source table and config."""
    row = fetch_one(
        f"SELECT source_table, config_id FROM {_table('redact_job_history')} WHERE run_id = %(run_id)s",
        {"run_id": run_id},
    )
    if not row or not row.get("source_table"):
        return None
    cfg = fetch_one(
        f"SELECT endpoint, use_ai_query FROM {_table('redact_config')} WHERE config_id = %(cid)s",
        {"cid": row["config_id"]},
    )
    if not cfg or str(cfg.get("use_ai_query", "false")).lower() != "true":
        return 0.0
    try:
        qualified = quote_table(row["source_table"])
        chars_row = fetch_one(f"SELECT COALESCE(sum(length(*)), 0) as total_chars, count(*) as cnt FROM {qualified}")
        total_chars = int(chars_row.get("total_chars") or 0) if chars_row else 0
        row_count = int(chars_row.get("cnt") or 0) if chars_row else 0
        endpoint = cfg.get("endpoint", "databricks-gpt-oss-120b")
        input_chars = total_chars + (row_count * _COST_PROMPT_OVERHEAD)
        input_tokens = int(input_chars * _COST_TOKENS_PER_CHAR)
        output_tokens = int(input_tokens * _COST_OUTPUT_RATIO)
        in_cost = _COST_INPUT.get(endpoint, 0.001)
        out_cost = _COST_OUTPUT.get(endpoint, 0.002)
        return round((input_tokens / 1000 * in_cost) + (output_tokens / 1000 * out_cost), 4)
    except Exception as exc:
        logger.warning("Post-run cost estimate failed for run %s: %s", run_id, exc)
        return None


@router.get("/status/{run_id}", response_model=RunStatusResponse)
async def pipeline_status(run_id: int):
    status = get_run_status(run_id)
    if status.get("state") in TERMINAL_STATES or (hasattr(status, "state") and status.state in TERMINAL_STATES):
        state_val = status.get("state") if isinstance(status, dict) else status.state
        result_val = status.get("result_state", "") if isinstance(status, dict) else (status.result_state or "")
        cost = _compute_post_run_cost(run_id)
        execute(
            f"""UPDATE {_table('redact_job_history')}
            SET status = %(status)s, cost_estimate_usd = %(cost)s, completed_at = current_timestamp()
            WHERE run_id = %(run_id)s AND completed_at IS NULL""",
            {"run_id": run_id, "status": result_val or state_val, "cost": cost},
        )
    return status


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


# Cost constants -- canonical source is src/dbxredact/cost.py. Keep in sync.
# Token prices per 1K tokens. Not fetched live -- no API exists.
# Source: https://www.databricks.com/product/pricing/foundation-model-training
# LAST_UPDATED: 2025-06-01
_COST_TOKENS_PER_CHAR = 0.25
_COST_INPUT = {
    "databricks-gpt-oss-120b": 0.001,
    "databricks-meta-llama-3-3-70b-instruct": 0.001,
    "databricks-claude-sonnet-4": 0.003,
    "databricks-gpt-4o-mini": 0.00015,
}
_COST_OUTPUT = {
    "databricks-gpt-oss-120b": 0.002,
    "databricks-meta-llama-3-3-70b-instruct": 0.002,
    "databricks-claude-sonnet-4": 0.015,
    "databricks-gpt-4o-mini": 0.0006,
}
_COST_PROMPT_OVERHEAD = 5500
_COST_OUTPUT_RATIO = 0.3
_CLUSTER_STARTUP_MINUTES = 5

# Compute cost profiles: DBU/hr rate and estimated rows/min throughput.
# Throughput values are initial heuristics -- calibrate with real benchmarks.
COMPUTE_PROFILES = {
    "cpu_small":  {"instance": "i3.xlarge",  "workers": 2,  "dbu_per_hr": 1.5,  "rows_per_min_ai": 15,  "rows_per_min_all": 5},
    "cpu_medium": {"instance": "i3.xlarge",  "workers": 5,  "dbu_per_hr": 3.0,  "rows_per_min_ai": 35,  "rows_per_min_all": 12},
    "cpu_large":  {"instance": "i3.xlarge",  "workers": 10, "dbu_per_hr": 5.5,  "rows_per_min_ai": 60,  "rows_per_min_all": 20},
    "gpu_small":  {"instance": "g5.xlarge",  "workers": 2,  "dbu_per_hr": 4.0,  "rows_per_min_ai": 15,  "rows_per_min_all": 30},
    "gpu_medium": {"instance": "g5.xlarge",  "workers": 5,  "dbu_per_hr": 9.0,  "rows_per_min_ai": 35,  "rows_per_min_all": 70},
    "gpu_large":  {"instance": "g5.xlarge",  "workers": 10, "dbu_per_hr": 17.0, "rows_per_min_ai": 60,  "rows_per_min_all": 130},
}


@router.get("/cost-estimate")
async def cost_estimate(
    table: str,
    text_column: str = "text",
    endpoint: str = "databricks-gpt-oss-120b",
    max_rows: int = 10000,
    cluster_profile: str = "cpu_small",
    use_gliner: bool = False,
):
    qualified = quote_table(table)
    row = fetch_one(
        f"SELECT sum(length({text_column})) as total_chars, count(*) as cnt "
        f"FROM (SELECT {text_column} FROM {qualified} LIMIT {int(max_rows)})"
    )
    total_chars = int(row.get("total_chars") or 0) if row else 0
    row_count = int(row.get("cnt") or 0) if row else 0
    input_chars = total_chars + (row_count * _COST_PROMPT_OVERHEAD)
    input_tokens = int(input_chars * _COST_TOKENS_PER_CHAR)
    output_tokens = int(input_tokens * _COST_OUTPUT_RATIO)
    in_cost = _COST_INPUT.get(endpoint, 0.001)
    out_cost = _COST_OUTPUT.get(endpoint, 0.002)
    ai_query_cost = (input_tokens / 1000 * in_cost) + (output_tokens / 1000 * out_cost)

    profile = COMPUTE_PROFILES.get(cluster_profile, COMPUTE_PROFILES["cpu_small"])
    throughput = profile["rows_per_min_all"] if use_gliner else profile["rows_per_min_ai"]
    est_minutes = (row_count / max(throughput, 1)) + _CLUSTER_STARTUP_MINUTES
    compute_cost = profile["dbu_per_hr"] * (est_minutes / 60)

    return {
        "row_count": row_count,
        "total_chars": total_chars,
        "estimated_input_tokens": input_tokens,
        "estimated_output_tokens": output_tokens,
        "ai_query_cost_usd": round(ai_query_cost, 4),
        "compute_cost_usd": round(compute_cost, 4),
        "estimated_cost_usd": round(ai_query_cost + compute_cost, 4),
        "estimated_minutes": round(est_minutes, 1),
        "endpoint": endpoint,
        "cluster_profile": cluster_profile,
    }


@router.get("/table-info")
async def table_info(table: str):
    """Return column names and row count for a UC table."""
    qualified = quote_table(table)
    cols = fetch_all(f"DESCRIBE TABLE {qualified}")
    column_names = [c["col_name"] for c in cols if not c["col_name"].startswith("#")]
    row = fetch_one(f"SELECT count(*) as cnt FROM {qualified}")
    row_count = int(row["cnt"]) if row else 0
    return {"columns": column_names, "row_count": row_count}
