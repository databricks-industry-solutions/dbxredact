"""Admin routes for governance operations (purge, audit)."""

import logging
import os
from typing import Optional
from fastapi import APIRouter, Query
from api.services.db import execute, fetch_one, fetch_all, _table, validate_identifier

logger = logging.getLogger(__name__)
router = APIRouter()

RETENTION_DAYS = int(os.environ.get("RETENTION_DAYS", "90"))


@router.post("/purge-annotations", status_code=200)
async def purge_annotations(retention_days: int = Query(default=None)):
    """Delete annotations and ground truths older than the retention period.

    These tables contain raw PII (entity_text) and should be purged regularly.
    """
    days = retention_days if retention_days is not None else RETENTION_DAYS
    results = {}
    for table_name in ("redact_annotations", "redact_ground_truths"):
        before = fetch_one(
            f"SELECT count(*) as cnt FROM {_table(table_name)} "
            f"WHERE created_at < current_timestamp() - INTERVAL {int(days)} DAY"
        )
        count = int(before.get("cnt", 0)) if before else 0
        if count > 0:
            execute(
                f"DELETE FROM {_table(table_name)} "
                f"WHERE created_at < current_timestamp() - INTERVAL {int(days)} DAY"
            )
            logger.info("Purged %d rows from %s (older than %d days)", count, table_name, days)
        results[table_name] = {"purged": count}
    return {"retention_days": days, "tables": results}


@router.get("/retention-status")
async def retention_status():
    """Check how many rows in PII-bearing tables exceed the retention period."""
    results = {}
    for table_name in ("redact_annotations", "redact_ground_truths"):
        row = fetch_one(
            f"SELECT count(*) as cnt, min(created_at) as oldest, max(created_at) as newest "
            f"FROM {_table(table_name)}"
        )
        stale = fetch_one(
            f"SELECT count(*) as cnt FROM {_table(table_name)} "
            f"WHERE created_at < current_timestamp() - INTERVAL {RETENTION_DAYS} DAY"
        )
        results[table_name] = {
            "total_rows": int(row.get("cnt", 0)) if row else 0,
            "oldest": str(row.get("oldest", "")) if row else "",
            "newest": str(row.get("newest", "")) if row else "",
            "stale_rows": int(stale.get("cnt", 0)) if stale else 0,
        }
    return {"retention_days": RETENTION_DAYS, "tables": results}


@router.get("/audit-log")
async def get_audit_log(
    run_id: Optional[str] = None,
    doc_id: Optional[str] = None,
    entity_type: Optional[str] = None,
    days: int = Query(30, ge=1, le=365),
    limit: int = Query(100, le=1000),
):
    """Query the entity-level redaction audit log.

    Returns entity type counts per document -- no raw PII is stored in this table.
    """
    where_clauses = [f"created_at >= current_timestamp() - INTERVAL {int(days)} DAY"]
    params = {"limit": limit}
    if run_id:
        where_clauses.append("run_id = %(run_id)s")
        params["run_id"] = run_id
    if doc_id:
        where_clauses.append("doc_id = %(doc_id)s")
        params["doc_id"] = doc_id
    if entity_type:
        where_clauses.append("entity_type = %(entity_type)s")
        params["entity_type"] = entity_type

    where = "WHERE " + " AND ".join(where_clauses)
    rows = fetch_all(
        f"SELECT * FROM {_table('redact_audit_log')} {where} "
        f"ORDER BY created_at DESC LIMIT %(limit)s",
        params,
    )
    return {"rows": rows, "count": len(rows)}


@router.get("/audit-summary")
async def audit_summary(
    run_id: Optional[str] = None,
    days: int = Query(30, ge=1, le=365),
):
    """Aggregated audit summary: total docs, entity counts by type."""
    where_clauses = [f"created_at >= current_timestamp() - INTERVAL {int(days)} DAY"]
    params = {}
    if run_id:
        where_clauses.append("run_id = %(run_id)s")
        params["run_id"] = run_id
    where = "WHERE " + " AND ".join(where_clauses)

    summary = fetch_all(
        f"SELECT entity_type, sum(entity_count) as total_entities, "
        f"count(DISTINCT doc_id) as doc_count "
        f"FROM {_table('redact_audit_log')} {where} "
        f"GROUP BY entity_type ORDER BY total_entities DESC",
        params,
    )
    total_row = fetch_one(
        f"SELECT count(DISTINCT doc_id) as total_docs, count(DISTINCT run_id) as total_runs "
        f"FROM {_table('redact_audit_log')} {where}",
        params,
    )
    return {
        "total_docs": int(total_row.get("total_docs", 0)) if total_row else 0,
        "total_runs": int(total_row.get("total_runs", 0)) if total_row else 0,
        "by_entity_type": summary,
    }
