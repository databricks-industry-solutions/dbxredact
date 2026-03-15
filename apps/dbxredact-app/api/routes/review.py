"""Document review and annotation routes."""

import uuid
from typing import List, Optional
from fastapi import APIRouter, HTTPException, Query
from api.models.schemas import AnnotationCreate, AnnotationResponse
from api.services.db import fetch_all, fetch_one, execute, _table, quote_table, validate_identifier

router = APIRouter()


@router.get("/compare")
async def compare_documents(
    source_table: str = Query(...),
    source_column: str = Query("text"),
    output_table: str = Query(...),
    output_column: str = Query("redacted_text"),
    doc_id_column: str = Query("doc_id"),
    limit: int = Query(1),
    offset: int = Query(0),
):
    """Side-by-side original vs redacted text, joined on doc_id."""
    src = quote_table(source_table)
    out = quote_table(output_table)
    validate_identifier(source_column)
    validate_identifier(output_column)
    validate_identifier(doc_id_column)
    rows = fetch_all(
        f"""SELECT s.`{doc_id_column}` AS doc_id,
                   s.`{source_column}` AS original_text,
                   o.`{output_column}` AS redacted_text
            FROM {src} s
            JOIN {out} o ON s.`{doc_id_column}` = o.`{doc_id_column}`
            ORDER BY s.`{doc_id_column}`
            LIMIT %(limit)s OFFSET %(offset)s""",
        {"limit": limit, "offset": offset},
    )
    count_row = fetch_one(
        f"""SELECT COUNT(*) AS cnt
            FROM {src} s
            JOIN {out} o ON s.`{doc_id_column}` = o.`{doc_id_column}`"""
    )
    total = int(count_row["cnt"]) if count_row else 0
    return {"rows": rows, "total": total}


@router.get("/documents")
async def list_documents(
    source_table: str = Query(...),
    limit: int = Query(50, le=200),
    offset: int = Query(0),
):
    rows = fetch_all(
        f"SELECT * FROM {quote_table(source_table)} LIMIT %(limit)s OFFSET %(offset)s",
        {"limit": limit, "offset": offset},
    )
    return rows


@router.get("/documents/{doc_id}")
async def get_document(doc_id: str, source_table: str = Query(...)):
    row = fetch_one(
        f"SELECT * FROM {quote_table(source_table)} WHERE doc_id = %(doc_id)s",
        {"doc_id": doc_id},
    )
    if not row:
        raise HTTPException(404, "Document not found")
    return row


@router.post("/annotations", response_model=AnnotationResponse, status_code=201)
async def create_annotation(body: AnnotationCreate):
    annotation_id = str(uuid.uuid4())
    execute(
        f"""INSERT INTO {_table('redact_annotations')}
        (annotation_id, doc_id, source_table, workflow, entity_text, entity_type,
         start, end_pos, action, corrected_type, corrected_value, detection_method, created_at)
        VALUES (%(annotation_id)s, %(doc_id)s, %(source_table)s, %(workflow)s,
                %(entity_text)s, %(entity_type)s, %(start)s, %(end_pos)s,
                %(action)s, %(corrected_type)s, %(corrected_value)s, %(detection_method)s,
                current_timestamp())""",
        {"annotation_id": annotation_id, **body.model_dump()},
    )
    return {**body.model_dump(), "annotation_id": annotation_id}


@router.get("/annotations", response_model=List[AnnotationResponse])
async def list_annotations(
    doc_id: Optional[str] = None,
    source_table: Optional[str] = None,
    workflow: Optional[str] = None,
    limit: int = Query(100, le=500),
):
    where_clauses = []
    params = {"limit": limit}
    if doc_id:
        where_clauses.append("doc_id = %(doc_id)s")
        params["doc_id"] = doc_id
    if source_table:
        where_clauses.append("source_table = %(source_table)s")
        params["source_table"] = source_table
    if workflow:
        where_clauses.append("workflow = %(workflow)s")
        params["workflow"] = workflow

    where = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
    return fetch_all(
        f"SELECT * FROM {_table('redact_annotations')} {where} ORDER BY created_at DESC LIMIT %(limit)s",
        params,
    )
