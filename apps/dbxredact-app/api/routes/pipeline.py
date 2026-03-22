"""Pipeline job triggering and status routes."""

import json
import logging
from typing import List, Optional
from fastapi import APIRouter, HTTPException
from api.models.schemas import PipelineRunRequest, RunStatusResponse, JobHistoryItem
from api.services.db import fetch_all, fetch_one, execute, _table, quote_table, validate_identifier, CATALOG, SCHEMA
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

    is_in_place = body.output_mode == "in_place"
    output_table = "" if is_in_place else (body.output_table or f"{body.source_table}_redacted")
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
        "detection_profile": config.get("detection_profile", "fast"),
        "use_presidio": str(config.get("use_presidio", True)).lower(),
        "use_ai_query": str(config.get("use_ai_query", True)).lower(),
        "use_gliner": str(config.get("use_gliner", False)).lower(),
        "endpoint": config.get("endpoint", ""),
        "score_threshold": str(config.get("score_threshold", 0.5)),
        "redaction_strategy": config.get("redaction_strategy", "typed"),
        "alignment_mode": config.get("alignment_mode", "union"),
        "reasoning_effort": config.get("reasoning_effort", "low"),
        "gliner_model": config.get("gliner_model", "nvidia/gliner-PII"),
        "gliner_max_words": str(config.get("gliner_max_words", 256)),
        "gliner_threshold": str(config.get("gliner_threshold", 0.2)),
        "presidio_model_size": config.get("presidio_model_size", "trf"),
        "presidio_pattern_only": str(config.get("presidio_pattern_only", False)).lower(),
        "max_rows": str(body.max_rows or 10000),
        "refresh_approach": body.refresh_approach,
        "safe_list_table": f"{CATALOG}.{SCHEMA}.redact_safe_list",
        "block_list_table": f"{CATALOG}.{SCHEMA}.redact_block_list",
        "output_mode": body.output_mode,
        "confirm_destructive": "true" if is_in_place else "false",
        "allow_consensus_redaction": "true" if config.get("alignment_mode", "union") == "consensus" else "false",
        "audit_table": f"{CATALOG}.{SCHEMA}.redact_audit_log",
        "redaction_scope": body.redaction_scope,
        "masking_strategy": body.masking_strategy or "mask",
    }

    if body.redaction_scope == "full_table":
        notebook_params["redaction_scope"] = "all_tagged"
        extra = {}
        if body.text_columns:
            extra["text_columns"] = body.text_columns
        if body.structured_columns:
            extra["structured_columns"] = body.structured_columns
        if extra:
            notebook_params["extra_params"] = json.dumps(extra)

    ep = config.get("extra_params")
    if ep and "extra_params" not in notebook_params:
        notebook_params["extra_params"] = json.dumps(ep) if not isinstance(ep, str) else ep

    run_id = trigger_pipeline_run(notebook_params, cluster_profile=body.cluster_profile)

    history_output = f"{body.source_table} (in-place)" if is_in_place else output_table
    execute(
        f"""INSERT INTO {_table('redact_job_history')}
        (run_id, config_id, source_table, output_table, status, started_at)
        VALUES (%(run_id)s, %(config_id)s, %(source_table)s, %(output_table)s, 'RUNNING', current_timestamp())""",
        {"run_id": run_id, "config_id": body.config_id,
         "source_table": body.source_table, "output_table": history_output},
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
        cols = fetch_all(f"DESCRIBE TABLE {qualified}")
        text_col = next(
            (c["col_name"] for c in cols
             if c.get("data_type", "").upper() == "STRING"
             and c["col_name"] in ("text", "note", "content", "body", "message")),
            None,
        )
        if not text_col:
            text_col = next(
                (c["col_name"] for c in cols if c.get("data_type", "").upper() == "STRING"),
                None,
            )
        if not text_col:
            return None
        validate_identifier(text_col)
        chars_row = fetch_one(f"SELECT COALESCE(sum(length(`{text_col}`)), 0) as total_chars, count(*) as cnt FROM {qualified}")
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
async def pipeline_history(limit: int = 50, offset: int = 0):
    limit = max(1, min(limit, 500))
    offset = max(0, offset)
    return fetch_all(
        f"SELECT * FROM {_table('redact_job_history')} ORDER BY started_at DESC LIMIT %(limit)s OFFSET %(offset)s",
        {"limit": limit, "offset": offset},
    )


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

# Empirical cost model: est_min = startup + overhead + total_chars / chars_per_min
# Derived from real benchmark runs with ensemble detection (Presidio + AI Query + GLiNER).
# Settings: en_core_web_lg, reasoning_effort=low, spark.task.resource.gpu.amount=0.25.
# Doc sizes: ~7.5k chars/doc. LAST_CALIBRATED: 2026-03-09.
# gpu_small (2x g5.xlarge, 8 partitions) benchmarked:
#   384r=14.85m, 768r=24.46m -> marginal cpm=300k, overhead=5.2m (detection floor+align+redact).
# cpu profiles scaled proportionally from gpu_small (~4.8x vs prior en_core_web_trf calibration).
# Medium/large extrapolated by worker ratio with 0.8x/0.6x sub-linear scaling
# (AI_QUERY throughput is bounded by endpoint capacity at high concurrency).
COMPUTE_PROFILES = {
    "cpu_small":  {"dbu_per_hr": 1.5,  "startup": 8,  "overhead": 7.0, "chars_per_min": 145_000},
    "cpu_medium": {"dbu_per_hr": 3.0,  "startup": 8,  "overhead": 5.5, "chars_per_min": 360_000},
    "cpu_large":  {"dbu_per_hr": 5.5,  "startup": 8,  "overhead": 4.5, "chars_per_min": 650_000},
    "gpu_small":  {"dbu_per_hr": 4.0,  "startup": 11, "overhead": 5.2, "chars_per_min": 300_000},
    "gpu_medium": {"dbu_per_hr": 9.0,  "startup": 11, "overhead": 4.0, "chars_per_min": 600_000},
    "gpu_large":  {"dbu_per_hr": 17.0, "startup": 11, "overhead": 3.0, "chars_per_min": 900_000},
}


# Fast mode uses pattern-only Presidio (no spaCy) and low reasoning -- ~1.4x faster from benchmarks.
_PROFILE_SPEED_FACTOR = {"fast": 1.4, "deep": 1.0, "custom": 1.0}


@router.get("/cost-estimate")
async def cost_estimate(
    table: str,
    text_column: str = "text",
    text_columns: Optional[str] = None,
    endpoint: str = "databricks-gpt-oss-120b",
    max_rows: int = 10000,
    cluster_profile: str = "cpu_small",
    use_gliner: bool = False,
    use_ai_query: bool = True,
    detection_profile: str = "fast",
):
    columns = [c.strip() for c in text_columns.split(",") if c.strip()] if text_columns else [text_column]
    for c in columns:
        validate_identifier(c)
    qualified = quote_table(table)

    speed_factor = _PROFILE_SPEED_FACTOR.get(detection_profile, 1.0)
    profile = COMPUTE_PROFILES.get(cluster_profile, COMPUTE_PROFILES["cpu_small"])
    effective_cpm = profile["chars_per_min"] * speed_factor
    in_cost = _COST_INPUT.get(endpoint, 0.001)
    out_cost = _COST_OUTPUT.get(endpoint, 0.002)

    per_column = []
    agg_chars = 0
    agg_ai_cost = 0.0
    agg_input_tokens = 0
    agg_output_tokens = 0
    row_count = 0

    for tc in columns:
        row = fetch_one(
            f"SELECT sum(length(`{tc}`)) as total_chars, count(*) as cnt "
            f"FROM (SELECT `{tc}` FROM {qualified} LIMIT {int(max_rows)})"
        )
        chars = int(row.get("total_chars") or 0) if row else 0
        cnt = int(row.get("cnt") or 0) if row else 0
        row_count = max(row_count, cnt)
        agg_chars += chars

        if use_ai_query:
            inp_chars = chars + (cnt * _COST_PROMPT_OVERHEAD)
            inp_tok = int(inp_chars * _COST_TOKENS_PER_CHAR)
            out_tok = int(inp_tok * _COST_OUTPUT_RATIO)
            col_ai_cost = (inp_tok / 1000 * in_cost) + (out_tok / 1000 * out_cost)
        else:
            inp_tok = out_tok = 0
            col_ai_cost = 0.0

        col_minutes = chars / max(effective_cpm, 1)
        agg_ai_cost += col_ai_cost
        agg_input_tokens += inp_tok
        agg_output_tokens += out_tok

        per_column.append({
            "column": tc,
            "total_chars": chars,
            "ai_query_cost_usd": round(col_ai_cost, 4),
            "estimated_minutes": round(col_minutes, 1),
        })

    est_minutes = profile["startup"] + profile["overhead"] + agg_chars / max(effective_cpm, 1)
    compute_cost = profile["dbu_per_hr"] * (est_minutes / 60)

    result = {
        "row_count": row_count,
        "total_chars": agg_chars,
        "estimated_input_tokens": agg_input_tokens,
        "estimated_output_tokens": agg_output_tokens,
        "ai_query_cost_usd": round(agg_ai_cost, 4),
        "compute_cost_usd": round(compute_cost, 4),
        "estimated_cost_usd": round(agg_ai_cost + compute_cost, 4),
        "estimated_minutes": round(est_minutes, 1),
        "endpoint": endpoint,
        "cluster_profile": cluster_profile,
        "detection_profile": detection_profile,
        "use_ai_query": use_ai_query,
    }
    if len(columns) > 1:
        result["per_column"] = per_column
    return result


@router.get("/discover-columns")
async def discover_columns(table: str):
    """Discover PII columns via UC tags for full-table redaction mode."""
    qualified = quote_table(table)
    cols = fetch_all(f"DESCRIBE TABLE {qualified}")
    col_meta = {}
    for c in cols:
        name = c["col_name"]
        if name.startswith("#"):
            continue
        col_meta[name] = {"type": c.get("data_type", ""), "tags": {}}

    # Fetch all tags for this table
    parts = table.split(".")
    if len(parts) == 3:
        tag_rows = fetch_all(
            f"""SELECT column_name, tag_name, tag_value
                FROM system.information_schema.column_tags
                WHERE table_catalog = %(catalog)s
                  AND table_schema = %(schema)s
                  AND table_name = %(table)s""",
            {"catalog": parts[0], "schema": parts[1], "table": parts[2]},
        )
        for row in tag_rows:
            cn = row["column_name"]
            if cn in col_meta:
                col_meta[cn]["tags"][row["tag_name"]] = row["tag_value"]

    text_columns = []
    structured_columns = {}
    untagged_columns = []
    doc_id_candidates = []
    warnings = []

    for col_name, info in col_meta.items():
        tags = info["tags"]
        col_type = info["type"].upper()
        is_protected = tags.get("data_classification") == "protected"
        pii_type = tags.get("pii_type")

        if is_protected:
            if pii_type and pii_type != "free_text":
                structured_columns[col_name] = pii_type
            elif col_type == "STRING":
                text_columns.append(col_name)
            else:
                warnings.append(
                    f"Column '{col_name}' is tagged protected but has no pii_type "
                    f"and is {col_type} type -- skipped"
                )
        else:
            untagged_columns.append(col_name)
            if col_name.endswith("_id") or col_name in ("id", "doc_id", "patient_id", "encounter_id"):
                doc_id_candidates.append(col_name)

    return {
        "text_columns": text_columns,
        "structured_columns": structured_columns,
        "untagged_columns": untagged_columns,
        "doc_id_candidates": doc_id_candidates,
        "warnings": warnings,
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
