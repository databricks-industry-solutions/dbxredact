"""Active learning queue routes."""

import json
import uuid
from typing import List, Optional
from fastapi import APIRouter, HTTPException, Query
from api.models.schemas import (
    ActiveLearnQueueItem, BuildQueueRequest, ReviewRequest, ActiveLearnStats,
)
from api.services.db import fetch_all, fetch_one, execute, _table, quote_table, validate_identifier

router = APIRouter()


@router.post("/build-queue")
async def build_queue(body: BuildQueueRequest):
    """Compute uncertainty scores and populate the review queue from a detection table."""
    validate_identifier(body.entities_column)
    rows = fetch_all(
        f"""SELECT doc_id,
            avg(ent.score) as avg_score,
            min(ent.score) as min_score,
            count(*) as entity_count,
            sum(CASE WHEN ent.score < 0.5 THEN 1 ELSE 0 END) as low_conf
        FROM {quote_table(body.detection_table)}
        LATERAL VIEW explode({body.entities_column}) t AS ent
        GROUP BY doc_id
        ORDER BY avg_score ASC
        LIMIT %(top_k)s""",
        {"top_k": body.top_k},
    )

    inserted = 0
    for row in rows:
        priority = 1.0 - (row.get("avg_score") or 0.5)
        entry_id = str(uuid.uuid4())
        execute(
            f"""INSERT INTO {_table('redact_active_learn_queue')}
            (entry_id, doc_id, source_table, priority_score, status, created_at)
            VALUES (%(entry_id)s, %(doc_id)s, %(source_table)s, %(priority)s, 'pending', current_timestamp())""",
            {
                "entry_id": entry_id,
                "doc_id": row["doc_id"],
                "source_table": body.detection_table,
                "priority": priority,
            },
        )
        inserted += 1

    return {"queued": inserted}


@router.get("/queue", response_model=List[ActiveLearnQueueItem])
async def get_queue(
    status: Optional[str] = Query(None),
    limit: int = Query(50, le=200),
    offset: int = Query(0),
):
    where = "WHERE status = %(status)s" if status else ""
    params = {"limit": limit, "offset": offset}
    if status:
        params["status"] = status
    return fetch_all(
        f"""SELECT * FROM {_table('redact_active_learn_queue')}
        {where}
        ORDER BY priority_score DESC
        LIMIT %(limit)s OFFSET %(offset)s""",
        params,
    )


@router.post("/queue/{doc_id}/review")
async def review_document(doc_id: str, body: ReviewRequest):
    """Mark a document as reviewed and save corrections."""
    for correction in body.corrections:
        correction_id = str(uuid.uuid4())
        execute(
            f"""INSERT INTO {_table('redact_corrections')}
            (correction_id, doc_id, source_table, entity_text, entity_type,
             start, end, action, corrected_type, corrected_text, created_at)
            VALUES (%(correction_id)s, %(doc_id)s, %(source_table)s, %(entity_text)s,
                    %(entity_type)s, %(start)s, %(end)s, %(action)s, %(corrected_type)s,
                    %(corrected_text)s, current_timestamp())""",
            {"correction_id": correction_id, **correction.model_dump()},
        )

    execute(
        f"""UPDATE {_table('redact_active_learn_queue')}
        SET status='reviewed', reviewed_at=current_timestamp()
        WHERE doc_id = %(doc_id)s""",
        {"doc_id": doc_id},
    )

    # Write reviewed entities to ground truth table
    entities_json = json.dumps([c.model_dump() for c in body.corrections]) if body.corrections else "[]"
    queue_row = fetch_one(
        f"""SELECT source_table FROM {_table('redact_active_learn_queue')}
        WHERE doc_id = %(doc_id)s LIMIT 1""",
        {"doc_id": doc_id},
    )
    source_table = queue_row.get("source_table", "") if queue_row else ""
    execute(
        f"""INSERT INTO {_table('redact_ground_truth')}
        (ground_truth_id, doc_id, source_table, entities, created_at)
        VALUES (%(gt_id)s, %(doc_id)s, %(source_table)s, %(entities)s, current_timestamp())""",
        {
            "gt_id": str(uuid.uuid4()),
            "doc_id": doc_id,
            "source_table": source_table,
            "entities": entities_json,
        },
    )

    return {"doc_id": doc_id, "status": "reviewed", "corrections_saved": len(body.corrections)}


@router.get("/stats", response_model=ActiveLearnStats)
async def queue_stats():
    row = fetch_one(
        f"""SELECT
            count(*) as total_queued,
            sum(CASE WHEN status='reviewed' THEN 1 ELSE 0 END) as reviewed,
            sum(CASE WHEN status='pending' THEN 1 ELSE 0 END) as pending,
            sum(CASE WHEN status='skipped' THEN 1 ELSE 0 END) as skipped,
            avg(priority_score) as avg_priority
        FROM {_table('redact_active_learn_queue')}"""
    )
    return row or {"total_queued": 0, "reviewed": 0, "pending": 0, "skipped": 0}
