"""CRUD routes for redact_config table."""

import json
import logging
import uuid
from typing import List
from fastapi import APIRouter, HTTPException
from api.models.schemas import ConfigCreate, ConfigResponse
from api.services.db import fetch_all, fetch_one, execute, _table

logger = logging.getLogger(__name__)
router = APIRouter()


def _serialize_params(body: ConfigCreate) -> dict:
    d = body.model_dump()
    ep = d.get("extra_params")
    d["extra_params"] = json.dumps(ep) if ep else None
    return d


@router.get("/", response_model=List[ConfigResponse])
async def list_configs():
    rows = fetch_all(f"SELECT * FROM {_table('redact_config')} ORDER BY created_at DESC")
    return rows


@router.get("/{config_id}", response_model=ConfigResponse)
async def get_config(config_id: str):
    row = fetch_one(f"SELECT * FROM {_table('redact_config')} WHERE config_id = %(config_id)s",
                    {"config_id": config_id})
    if not row:
        raise HTTPException(404, "Config not found")
    return row


@router.post("/", response_model=ConfigResponse, status_code=201)
async def create_config(body: ConfigCreate):
    params = _serialize_params(body)
    existing = fetch_one(
        f"SELECT config_id FROM {_table('redact_config')} WHERE name = %(name)s",
        {"name": body.name},
    )
    if existing:
        config_id = existing["config_id"]
        execute(
            f"""UPDATE {_table('redact_config')} SET
                detection_profile=%(detection_profile)s,
                use_presidio=%(use_presidio)s, use_ai_query=%(use_ai_query)s,
                use_gliner=%(use_gliner)s, endpoint=%(endpoint)s, score_threshold=%(score_threshold)s,
                gliner_model=%(gliner_model)s, gliner_threshold=%(gliner_threshold)s,
                redaction_strategy=%(redaction_strategy)s, alignment_mode=%(alignment_mode)s,
                reasoning_effort=%(reasoning_effort)s, gliner_max_words=%(gliner_max_words)s,
                presidio_model_size=%(presidio_model_size)s,
                extra_params=%(extra_params)s, updated_at=current_timestamp()
            WHERE config_id = %(config_id)s""",
            {"config_id": config_id, **params},
        )
    else:
        config_id = str(uuid.uuid4())
        execute(
            f"""INSERT INTO {_table('redact_config')}
            (config_id, name, detection_profile, use_presidio, use_ai_query, use_gliner,
             endpoint, score_threshold, gliner_model, gliner_threshold, redaction_strategy,
             alignment_mode, reasoning_effort, gliner_max_words, presidio_model_size,
             extra_params, created_at, updated_at)
            VALUES (%(config_id)s, %(name)s, %(detection_profile)s, %(use_presidio)s,
                    %(use_ai_query)s, %(use_gliner)s, %(endpoint)s, %(score_threshold)s,
                    %(gliner_model)s, %(gliner_threshold)s, %(redaction_strategy)s,
                    %(alignment_mode)s, %(reasoning_effort)s, %(gliner_max_words)s,
                    %(presidio_model_size)s, %(extra_params)s,
                    current_timestamp(), current_timestamp())""",
            {"config_id": config_id, **params},
        )
    return {**body.model_dump(), "config_id": config_id}


@router.put("/{config_id}", response_model=ConfigResponse)
async def update_config(config_id: str, body: ConfigCreate):
    existing = fetch_one(f"SELECT * FROM {_table('redact_config')} WHERE config_id = %(config_id)s",
                         {"config_id": config_id})
    if not existing:
        raise HTTPException(404, "Config not found")
    params = _serialize_params(body)
    execute(
        f"""UPDATE {_table('redact_config')} SET
            name=%(name)s, detection_profile=%(detection_profile)s,
            use_presidio=%(use_presidio)s, use_ai_query=%(use_ai_query)s,
            use_gliner=%(use_gliner)s, endpoint=%(endpoint)s, score_threshold=%(score_threshold)s,
            gliner_model=%(gliner_model)s, gliner_threshold=%(gliner_threshold)s,
            redaction_strategy=%(redaction_strategy)s, alignment_mode=%(alignment_mode)s,
            reasoning_effort=%(reasoning_effort)s, gliner_max_words=%(gliner_max_words)s,
            presidio_model_size=%(presidio_model_size)s,
            extra_params=%(extra_params)s, updated_at=current_timestamp()
        WHERE config_id = %(config_id)s""",
        {"config_id": config_id, **params},
    )
    return {**body.model_dump(), "config_id": config_id}


@router.delete("/{config_id}", status_code=204)
async def delete_config(config_id: str):
    execute(f"DELETE FROM {_table('redact_config')} WHERE config_id = %(config_id)s",
            {"config_id": config_id})
