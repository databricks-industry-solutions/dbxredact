"""CRUD routes for redact_config table."""

import json
import logging
import uuid
from typing import List
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from api.models.schemas import ConfigCreate
from api.services.db import fetch_all, fetch_one, execute, _table

logger = logging.getLogger(__name__)
router = APIRouter()


def _serialize_params(body: ConfigCreate) -> dict:
    d = body.model_dump()
    ep = d.get("extra_params")
    d["extra_params"] = json.dumps(ep) if ep else None
    return d


def _to_bool(v) -> bool:
    if isinstance(v, str):
        return v.lower() in ("true", "1", "yes")
    return bool(v) if v is not None else False


def _clean_config(row: dict) -> dict:
    """Normalize DB row types so the frontend gets consistent JSON."""
    if not row:
        return row
    for k in ("use_presidio", "use_ai_query", "use_gliner", "presidio_pattern_only"):
        if k in row:
            row[k] = _to_bool(row[k])
    for k in ("score_threshold", "gliner_threshold"):
        if k in row and row[k] is not None:
            row[k] = float(row[k])
    if "gliner_max_words" in row and row["gliner_max_words"] is not None:
        row["gliner_max_words"] = int(row["gliner_max_words"])
    if not row.get("ner_backend"):
        row["ner_backend"] = "gliner"
    if not row.get("language"):
        row["language"] = "en"
    if not row.get("translate_to"):
        row["translate_to"] = "none"
    ep = row.get("extra_params")
    if isinstance(ep, str):
        row["extra_params"] = json.loads(ep) if ep else None
    return row


@router.get("/")
async def list_configs():
    rows = fetch_all(f"SELECT * FROM {_table('redact_config')} ORDER BY created_at DESC")
    return [_clean_config(r) for r in rows]


@router.get("/{config_id}")
async def get_config(config_id: str):
    row = fetch_one(f"SELECT * FROM {_table('redact_config')} WHERE config_id = %(config_id)s",
                    {"config_id": config_id})
    if not row:
        raise HTTPException(404, "Config not found")
    return _clean_config(row)


@router.post("/", status_code=201)
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
                use_gliner=%(use_gliner)s, ner_backend=%(ner_backend)s,
                endpoint=%(endpoint)s, score_threshold=%(score_threshold)s,
                gliner_model=%(gliner_model)s, gliner_threshold=%(gliner_threshold)s,
                redaction_strategy=%(redaction_strategy)s, alignment_mode=%(alignment_mode)s,
                reasoning_effort=%(reasoning_effort)s, gliner_max_words=%(gliner_max_words)s,
                presidio_model_size=%(presidio_model_size)s,
                presidio_pattern_only=%(presidio_pattern_only)s,
                language=%(language)s, translate_to=%(translate_to)s,
                extra_params=%(extra_params)s, updated_at=current_timestamp()
            WHERE config_id = %(config_id)s""",
            {"config_id": config_id, **params},
        )
    else:
        config_id = str(uuid.uuid4())
        execute(
            f"""INSERT INTO {_table('redact_config')}
            (config_id, name, detection_profile, use_presidio, use_ai_query, use_gliner,
             ner_backend, endpoint, score_threshold, gliner_model, gliner_threshold,
             redaction_strategy, alignment_mode, reasoning_effort, gliner_max_words,
             presidio_model_size, presidio_pattern_only, language, translate_to,
             extra_params, created_at, updated_at)
            VALUES (%(config_id)s, %(name)s, %(detection_profile)s, %(use_presidio)s,
                    %(use_ai_query)s, %(use_gliner)s, %(ner_backend)s, %(endpoint)s,
                    %(score_threshold)s, %(gliner_model)s, %(gliner_threshold)s,
                    %(redaction_strategy)s, %(alignment_mode)s, %(reasoning_effort)s,
                    %(gliner_max_words)s, %(presidio_model_size)s, %(presidio_pattern_only)s,
                    %(language)s, %(translate_to)s,
                    %(extra_params)s, current_timestamp(), current_timestamp())""",
            {"config_id": config_id, **params},
        )
    return {**body.model_dump(), "config_id": config_id}


@router.put("/{config_id}")
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
            use_gliner=%(use_gliner)s, ner_backend=%(ner_backend)s,
            endpoint=%(endpoint)s, score_threshold=%(score_threshold)s,
            gliner_model=%(gliner_model)s, gliner_threshold=%(gliner_threshold)s,
            redaction_strategy=%(redaction_strategy)s, alignment_mode=%(alignment_mode)s,
            reasoning_effort=%(reasoning_effort)s, gliner_max_words=%(gliner_max_words)s,
            presidio_model_size=%(presidio_model_size)s,
            presidio_pattern_only=%(presidio_pattern_only)s,
            language=%(language)s, translate_to=%(translate_to)s,
            extra_params=%(extra_params)s, updated_at=current_timestamp()
        WHERE config_id = %(config_id)s""",
        {"config_id": config_id, **params},
    )
    return {**body.model_dump(), "config_id": config_id}


@router.delete("/{config_id}", status_code=204)
async def delete_config(config_id: str):
    execute(f"DELETE FROM {_table('redact_config')} WHERE config_id = %(config_id)s",
            {"config_id": config_id})
