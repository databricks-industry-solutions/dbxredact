"""Labeling/annotation routes for ground truth creation."""

from typing import List
from fastapi import APIRouter, Query
from api.services.db import fetch_all, fetch_one, execute, _table, quote_table, validate_identifier

router = APIRouter()


@router.get("/documents")
async def list_unlabeled_documents(
    source_table: str = Query(...),
    text_column: str = Query("text"),
    doc_id_column: str = Query("doc_id"),
    limit: int = Query(20, le=100),
):
    validate_identifier(text_column)
    validate_identifier(doc_id_column)
    rows = fetch_all(
        f"""SELECT s.`{doc_id_column}` AS doc_id, s.`{text_column}` AS text
        FROM {quote_table(source_table)} s
        LEFT ANTI JOIN {_table('redact_ground_truths')} g
            ON CAST(s.`{doc_id_column}` AS STRING) = g.doc_id AND g.source_table = %(source_table)s
        LIMIT %(limit)s""",
        {"source_table": source_table, "limit": limit},
    )
    return rows


@router.get("/documents-with-labels")
async def list_documents_with_labels(
    source_table: str = Query(...),
    text_column: str = Query("text"),
    doc_id_column: str = Query("doc_id"),
    entity_text_column: str = Query("entity_text"),
    entity_type_column: str = Query("entity_type"),
    start_column: str = Query("start"),
    end_column: str = Query("end"),
    limit: int = Query(20, le=100),
    offset: int = Query(0),
):
    """Fetch documents that already have denormalized label columns."""
    for col in [text_column, doc_id_column, entity_text_column, entity_type_column, start_column, end_column]:
        validate_identifier(col)
    qt = quote_table(source_table)
    rows = fetch_all(
        f"""SELECT `{doc_id_column}` AS doc_id, `{text_column}` AS text,
                   `{entity_text_column}` AS entity_text, `{entity_type_column}` AS entity_type,
                   CAST(`{start_column}` AS INT) AS start, CAST(`{end_column}` AS INT) AS end
            FROM {qt}
            ORDER BY `{doc_id_column}`, `{start_column}`
            LIMIT %(limit)s OFFSET %(offset)s""",
        {"limit": limit, "offset": offset},
    )
    # Group rows by doc_id
    docs: dict = {}
    for r in rows:
        did = str(r["doc_id"])
        if did not in docs:
            docs[did] = {"doc_id": did, "text": r["text"], "labels": []}
        docs[did]["labels"].append({
            "entity_text": r["entity_text"],
            "entity_type": r["entity_type"],
            "start": int(r["start"]) if r["start"] is not None else 0,
            "end": int(r["end"]) if r["end"] is not None else 0,
        })
    return list(docs.values())


@router.post("/batch")
async def batch_label(doc_id: str, source_table: str, labels: List[dict]):
    for label in labels:
        execute(
            f"""INSERT INTO {_table('redact_ground_truths')}
            (doc_id, source_table, entity_text, entity_type, start, end_pos, created_at)
            VALUES (%(doc_id)s, %(source_table)s,
                    %(entity_text)s, %(entity_type)s, %(start)s, %(end_pos)s,
                    current_timestamp())""",
            {
                "doc_id": doc_id,
                "source_table": source_table,
                "entity_text": label["entity_text"],
                "entity_type": label["entity_type"],
                "start": label["start"],
                "end_pos": label["end"],
            },
        )
    return {"labeled": len(labels)}


@router.get("/stats")
async def labeling_stats(source_table: str = Query(...)):
    row = fetch_one(
        f"""SELECT
            (SELECT count(DISTINCT doc_id) FROM {quote_table(source_table)}) as total_docs,
            (SELECT count(DISTINCT doc_id) FROM {_table('redact_ground_truths')}
             WHERE source_table = %(source_table)s) as labeled_docs""",
        {"source_table": source_table},
    )
    return row or {}
