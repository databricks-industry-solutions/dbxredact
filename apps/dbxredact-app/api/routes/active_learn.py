"""Active learning queue routes."""

import uuid
from typing import List, Optional
from fastapi import APIRouter, Query
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
    queue_row = fetch_one(
        f"""SELECT source_table FROM {_table('redact_active_learn_queue')}
        WHERE doc_id = %(doc_id)s LIMIT 1""",
        {"doc_id": doc_id},
    )
    source_table = queue_row.get("source_table", "") if queue_row else ""

    for c in body.corrections:
        annotation_id = str(uuid.uuid4())
        execute(
            f"""INSERT INTO {_table('redact_annotations')}
            (annotation_id, doc_id, source_table, workflow, entity_text, entity_type,
             start, end_pos, action, corrected_type, corrected_value, detection_method, created_at)
            VALUES (%(annotation_id)s, %(doc_id)s, %(source_table)s, 'active_learn',
                    %(entity_text)s, %(entity_type)s, %(start)s, %(end_pos)s,
                    %(action)s, %(corrected_type)s, %(corrected_value)s, %(detection_method)s,
                    current_timestamp())""",
            {"annotation_id": annotation_id, **c.model_dump()},
        )

    execute(
        f"""UPDATE {_table('redact_active_learn_queue')}
        SET status='reviewed', reviewed_at=current_timestamp()
        WHERE doc_id = %(doc_id)s""",
        {"doc_id": doc_id},
    )

    return {"doc_id": doc_id, "status": "reviewed", "annotations_saved": len(body.corrections)}


@router.get("/stats", response_model=ActiveLearnStats)
async def queue_stats():
    row = fetch_one(
        f"""SELECT
            count(*) as total_queued,
            COALESCE(sum(CASE WHEN status='reviewed' THEN 1 ELSE 0 END), 0) as reviewed,
            COALESCE(sum(CASE WHEN status='pending' THEN 1 ELSE 0 END), 0) as pending,
            COALESCE(sum(CASE WHEN status='skipped' THEN 1 ELSE 0 END), 0) as skipped,
            avg(priority_score) as avg_priority
        FROM {_table('redact_active_learn_queue')}"""
    )
    return row or {"total_queued": 0, "reviewed": 0, "pending": 0, "skipped": 0}
