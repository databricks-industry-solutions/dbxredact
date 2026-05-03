"""Pydantic models for API request/response schemas."""

import json
from datetime import datetime
from typing import Any, Dict, List, Literal, Optional
from pydantic import BaseModel, Field, field_validator


class ConfigCreate(BaseModel):
    name: str
    detection_profile: Optional[str] = "fast"
    use_presidio: bool = True
    use_ai_query: bool = True
    use_gliner: bool = False
    ner_backend: Literal["none", "gliner", "privacy_filter"] = "none"
    endpoint: str = "databricks-gpt-oss-120b"
    score_threshold: float = Field(0.5, ge=0.1, le=1.0)
    gliner_model: str = "nvidia/gliner-PII"
    gliner_threshold: Optional[float] = 0.2
    redaction_strategy: str = "typed"
    alignment_mode: str = "union"
    reasoning_effort: Optional[str] = "low"
    gliner_max_words: Optional[int] = 256
    presidio_model_size: Optional[str] = "trf"
    presidio_pattern_only: Optional[bool] = False
    language: str = "en"
    translate_to: Optional[str] = "none"
    encryption_secret: Optional[str] = ""
    extra_params: Optional[Dict[str, Any]] = None

    @field_validator("gliner_threshold", mode="before")
    @classmethod
    def coerce_gliner_threshold(cls, v):
        if v is None or v == "":
            return 0.2
        v = float(v)
        return v if 0.05 <= v <= 1.0 else 0.2


class ConfigResponse(ConfigCreate):
    model_config = {"from_attributes": True}
    config_id: str
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    @field_validator("ner_backend", mode="before")
    @classmethod
    def coerce_ner_backend(cls, v):
        return v if v else "none"

    @field_validator("use_presidio", "use_ai_query", "use_gliner", "presidio_pattern_only", mode="before")
    @classmethod
    def coerce_bool(cls, v):
        if isinstance(v, str):
            return v.lower() in ("true", "1", "yes")
        return bool(v) if v is not None else False

    @field_validator("score_threshold", mode="before")
    @classmethod
    def coerce_score_threshold(cls, v):
        if v is None:
            return 0.5
        return float(v)

    @field_validator("gliner_threshold", mode="before")
    @classmethod
    def coerce_gliner_threshold(cls, v):
        if v is None:
            return 0.2
        return float(v)

    @field_validator("gliner_max_words", mode="before")
    @classmethod
    def coerce_optional_int(cls, v):
        if v is None or v == "":
            return None
        return int(v)

    @field_validator("extra_params", mode="before")
    @classmethod
    def parse_extra_params(cls, v):
        if isinstance(v, str):
            return json.loads(v) if v else None
        return v


class PipelineRunRequest(BaseModel):
    config_id: str
    source_table: str
    redaction_scope: Literal["single_column", "tagged_columns", "full_table"] = "single_column"
    text_column: str = "text"
    doc_id_column: str = "doc_id"
    output_table: Optional[str] = None
    max_rows: Optional[int] = Field(default=10000, le=1_000_000)
    max_cost_usd: Optional[float] = None
    cluster_profile: str = "cpu_small"
    refresh_approach: str = "full"
    output_mode: Literal["separate", "in_place"] = "separate"
    text_columns: Optional[List[str]] = None
    structured_columns: Optional[Dict[str, str]] = None
    masking_strategy: Optional[str] = "mask"
    output_strategy: Literal["production", "validation"] = "production"


class RunStatusResponse(BaseModel):
    run_id: int
    state: Optional[str] = None
    result_state: Optional[str] = None
    start_time: Optional[int] = None
    end_time: Optional[int] = None
    run_page_url: Optional[str] = None


class JobHistoryItem(BaseModel):
    run_id: int
    config_id: str
    config_name: Optional[str] = None
    source_table: str
    output_table: str
    status: str
    cost_estimate_usd: Optional[float] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    run_page_url: Optional[str] = None
    job_type: Optional[str] = None


class AnnotationCreate(BaseModel):
    doc_id: str
    source_table: str
    entity_text: str
    entity_type: str
    start: int
    end_pos: int
    action: str  # "accept", "reject", "retype"
    corrected_type: Optional[str] = None
    corrected_value: Optional[str] = None
    detection_method: Optional[str] = None
    workflow: str = "review"


class AnnotationResponse(AnnotationCreate):
    annotation_id: str
    created_at: Optional[str] = None


class ListEntryCreate(BaseModel):
    value: str
    is_pattern: bool = False
    entity_type: Optional[str] = None
    notes: Optional[str] = None


class ListEntryResponse(ListEntryCreate):
    entry_id: str
    list_type: str
    created_at: Optional[str] = None


class ABTestCreate(BaseModel):
    name: str
    config_a_id: str
    config_b_id: str
    source_table: str
    sample_size: int = 100


class ABTestResponse(BaseModel):
    test_id: str
    name: str
    config_a_id: str
    config_b_id: str
    source_table: str
    sample_size: int
    status: str
    metrics_a: Optional[Dict[str, Any]] = None
    metrics_b: Optional[Dict[str, Any]] = None
    winner: Optional[str] = None
    created_at: Optional[str] = None
    completed_at: Optional[str] = None

    @field_validator("metrics_a", "metrics_b", mode="before")
    @classmethod
    def parse_metrics(cls, v):
        if isinstance(v, str):
            return json.loads(v) if v else None
        return v


class ActiveLearnQueueItem(BaseModel):
    doc_id: str
    source_table: str
    priority_score: float
    status: str = "pending"
    assigned_to: Optional[str] = None
    created_at: Optional[str] = None
    reviewed_at: Optional[str] = None


class BuildQueueRequest(BaseModel):
    detection_table: str
    doc_id_column: str = "doc_id"
    entities_column: str = "aligned_entities"
    top_k: int = Field(default=100, le=10_000)


class ReviewRequest(BaseModel):
    corrections: List[AnnotationCreate]


class ActiveLearnStats(BaseModel):
    total_queued: int
    reviewed: int
    pending: int
    skipped: int
    avg_priority: Optional[float] = None


class LabelCreate(BaseModel):
    entity_text: str
    entity_type: str
    start: int = Field(ge=0)
    end_pos: int = Field(ge=0)


class BatchLabelRequest(BaseModel):
    doc_id: str
    source_table: str
    labels: List[LabelCreate]
