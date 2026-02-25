"""Deny/Allow list management routes."""

import uuid
from typing import List
from fastapi import APIRouter, Query
from api.models.schemas import ListEntryCreate, ListEntryResponse
from api.services.db import fetch_all, execute, _table

router = APIRouter()


@router.get("/deny", response_model=List[ListEntryResponse])
async def list_deny_entries(limit: int = Query(200, le=1000)):
    return fetch_all(
        f"SELECT *, 'deny' as list_type FROM {_table('redact_deny_list')} ORDER BY created_at DESC LIMIT %(limit)s",
        {"limit": limit},
    )


@router.post("/deny", response_model=ListEntryResponse, status_code=201)
async def add_deny_entry(body: ListEntryCreate):
    entry_id = str(uuid.uuid4())
    execute(
        f"""INSERT INTO {_table('redact_deny_list')}
        (entry_id, value, is_pattern, entity_type, notes, created_at)
        VALUES (%(entry_id)s, %(value)s, %(is_pattern)s, %(entity_type)s, %(notes)s, current_timestamp())""",
        {"entry_id": entry_id, **body.model_dump()},
    )
    return {**body.model_dump(), "entry_id": entry_id, "list_type": "deny"}


@router.delete("/deny/{entry_id}", status_code=204)
async def delete_deny_entry(entry_id: str):
    execute(f"DELETE FROM {_table('redact_deny_list')} WHERE entry_id = %(entry_id)s",
            {"entry_id": entry_id})


@router.get("/allow", response_model=List[ListEntryResponse])
async def list_allow_entries(limit: int = Query(200, le=1000)):
    return fetch_all(
        f"SELECT *, 'allow' as list_type FROM {_table('redact_allow_list')} ORDER BY created_at DESC LIMIT %(limit)s",
        {"limit": limit},
    )


@router.post("/allow", response_model=ListEntryResponse, status_code=201)
async def add_allow_entry(body: ListEntryCreate):
    entry_id = str(uuid.uuid4())
    execute(
        f"""INSERT INTO {_table('redact_allow_list')}
        (entry_id, value, is_pattern, entity_type, notes, created_at)
        VALUES (%(entry_id)s, %(value)s, %(is_pattern)s, %(entity_type)s, %(notes)s, current_timestamp())""",
        {"entry_id": entry_id, **body.model_dump()},
    )
    return {**body.model_dump(), "entry_id": entry_id, "list_type": "allow"}


@router.delete("/allow/{entry_id}", status_code=204)
async def delete_allow_entry(entry_id: str):
    execute(f"DELETE FROM {_table('redact_allow_list')} WHERE entry_id = %(entry_id)s",
            {"entry_id": entry_id})
