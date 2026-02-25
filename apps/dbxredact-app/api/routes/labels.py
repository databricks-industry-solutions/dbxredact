"""Labeling/annotation routes for ground truth creation."""

import uuid
from typing import List, Optional
from fastapi import APIRouter, HTTPException, Query
from api.services.db import fetch_all, fetch_one, execute, _table, quote_table

router = APIRouter()


@router.get("/documents")
async def list_unlabeled_documents(
    source_table: str = Query(...),
    limit: int = Query(20, le=100),
):
    """Return documents that don't yet have corrections."""
    rows = fetch_all(
        f"""SELECT s.doc_id, s.text
        FROM {quote_table(source_table)} s
        LEFT ANTI JOIN {_table('redact_corrections')} c
            ON s.doc_id = c.doc_id AND c.source_table = %(source_table)s
        LIMIT %(limit)s""",
        {"source_table": source_table, "limit": limit},
    )
    return rows


@router.post("/batch")
async def batch_label(
    doc_id: str,
    source_table: str,
    labels: List[dict],
):
    """Save a batch of entity labels for a document."""
    for label in labels:
        correction_id = str(uuid.uuid4())
        execute(
            f"""INSERT INTO {_table('redact_corrections')}
            (correction_id, doc_id, source_table, entity_text, entity_type,
             start, end, action, corrected_type, corrected_text, created_at)
            VALUES (%(correction_id)s, %(doc_id)s, %(source_table)s, %(entity_text)s,
                    %(entity_type)s, %(start)s, %(end)s, 'label', %(entity_type)s,
                    %(entity_text)s, current_timestamp())""",
            {
                "correction_id": correction_id,
                "doc_id": doc_id,
                "source_table": source_table,
                "entity_text": label["entity_text"],
                "entity_type": label["entity_type"],
                "start": label["start"],
                "end": label["end"],
            },
        )
    return {"labeled": len(labels)}


@router.get("/stats")
async def labeling_stats(source_table: str = Query(...)):
    """How many documents have been labeled."""
    row = fetch_one(
        f"""SELECT
            (SELECT count(DISTINCT doc_id) FROM {quote_table(source_table)}) as total_docs,
            (SELECT count(DISTINCT doc_id) FROM {_table('redact_corrections')}
             WHERE source_table = %(source_table)s) as labeled_docs""",
        {"source_table": source_table},
    )
    return row or {}
