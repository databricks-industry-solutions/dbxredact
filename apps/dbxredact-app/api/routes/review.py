"""Document review and correction routes."""

import uuid
from typing import List, Optional
from fastapi import APIRouter, HTTPException, Query
from api.models.schemas import CorrectionCreate, CorrectionResponse
from api.services.db import fetch_all, fetch_one, execute, _table, quote_table

router = APIRouter()


@router.get("/documents")
async def list_documents(
    source_table: str = Query(...),
    limit: int = Query(50, le=200),
    offset: int = Query(0),
):
    """List documents from a detection results table for review."""
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


@router.post("/corrections", response_model=CorrectionResponse, status_code=201)
async def create_correction(body: CorrectionCreate):
    correction_id = str(uuid.uuid4())
    execute(
        f"""INSERT INTO {_table('redact_corrections')}
        (correction_id, doc_id, source_table, entity_text, entity_type,
         start, end, action, corrected_type, corrected_text, created_at)
        VALUES (%(correction_id)s, %(doc_id)s, %(source_table)s, %(entity_text)s,
                %(entity_type)s, %(start)s, %(end)s, %(action)s, %(corrected_type)s,
                %(corrected_text)s, current_timestamp())""",
        {"correction_id": correction_id, **body.model_dump()},
    )
    return {**body.model_dump(), "correction_id": correction_id}


@router.get("/corrections", response_model=List[CorrectionResponse])
async def list_corrections(
    doc_id: Optional[str] = None,
    source_table: Optional[str] = None,
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

    where = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
    return fetch_all(
        f"SELECT * FROM {_table('redact_corrections')} {where} ORDER BY created_at DESC LIMIT %(limit)s",
        params,
    )
