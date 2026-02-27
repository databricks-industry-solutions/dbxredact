"""Block/Safe list management routes."""

import uuid
from typing import List
from fastapi import APIRouter, Query
from api.models.schemas import ListEntryCreate, ListEntryResponse
from api.services.db import fetch_all, execute, _table

router = APIRouter()


@router.get("/block", response_model=List[ListEntryResponse])
async def list_block_entries(limit: int = Query(200, le=1000)):
    return fetch_all(
        f"SELECT *, 'block' as list_type FROM {_table('redact_block_list')} ORDER BY created_at DESC LIMIT %(limit)s",
        {"limit": limit},
    )


@router.post("/block", response_model=ListEntryResponse, status_code=201)
async def add_block_entry(body: ListEntryCreate):
    entry_id = str(uuid.uuid4())
    execute(
        f"""INSERT INTO {_table('redact_block_list')}
        (entry_id, value, is_pattern, entity_type, notes, created_at)
        VALUES (%(entry_id)s, %(value)s, %(is_pattern)s, %(entity_type)s, %(notes)s, current_timestamp())""",
        {"entry_id": entry_id, **body.model_dump()},
    )
    return {**body.model_dump(), "entry_id": entry_id, "list_type": "block"}


@router.delete("/block/{entry_id}", status_code=204)
async def delete_block_entry(entry_id: str):
    execute(f"DELETE FROM {_table('redact_block_list')} WHERE entry_id = %(entry_id)s",
            {"entry_id": entry_id})


@router.get("/safe", response_model=List[ListEntryResponse])
async def list_safe_entries(limit: int = Query(200, le=1000)):
    return fetch_all(
        f"SELECT *, 'safe' as list_type FROM {_table('redact_safe_list')} ORDER BY created_at DESC LIMIT %(limit)s",
        {"limit": limit},
    )


@router.post("/safe", response_model=ListEntryResponse, status_code=201)
async def add_safe_entry(body: ListEntryCreate):
    entry_id = str(uuid.uuid4())
    execute(
        f"""INSERT INTO {_table('redact_safe_list')}
        (entry_id, value, is_pattern, entity_type, notes, created_at)
        VALUES (%(entry_id)s, %(value)s, %(is_pattern)s, %(entity_type)s, %(notes)s, current_timestamp())""",
        {"entry_id": entry_id, **body.model_dump()},
    )
    return {**body.model_dump(), "entry_id": entry_id, "list_type": "safe"}


@router.delete("/safe/{entry_id}", status_code=204)
async def delete_safe_entry(entry_id: str):
    execute(f"DELETE FROM {_table('redact_safe_list')} WHERE entry_id = %(entry_id)s",
            {"entry_id": entry_id})
