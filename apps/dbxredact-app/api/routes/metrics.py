"""Evaluation metrics and audit routes."""

import logging
from fastapi import APIRouter, Query
from api.services.db import fetch_all, fetch_one, _table, quote_table

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/summary")
async def metrics_summary(output_table: str = Query(...)):
    """Aggregated metrics from a detection results table."""
    row = fetch_one(
        f"""SELECT
            count(*) as total_documents,
            avg(size(aligned_entities)) as avg_entities_per_doc,
            sum(size(aligned_entities)) as total_entities
        FROM {quote_table(output_table)}
        WHERE aligned_entities IS NOT NULL"""
    )
    return row or {}


@router.get("/by-type")
async def metrics_by_entity_type(output_table: str = Query(...)):
    rows = fetch_all(
        f"""SELECT ent.entity_type, count(*) as count
        FROM {quote_table(output_table)}
        LATERAL VIEW explode(aligned_entities) t AS ent
        GROUP BY ent.entity_type
        ORDER BY count DESC"""
    )
    return rows


@router.get("/confidence-distribution")
async def confidence_distribution(output_table: str = Query(...)):
    rows = fetch_all(
        f"""SELECT
            CASE
                WHEN ent.score >= 0.9 THEN '0.9-1.0'
                WHEN ent.score >= 0.7 THEN '0.7-0.9'
                WHEN ent.score >= 0.5 THEN '0.5-0.7'
                WHEN ent.score >= 0.3 THEN '0.3-0.5'
                ELSE '0.0-0.3'
            END as bucket,
            count(*) as count
        FROM {quote_table(output_table)}
        LATERAL VIEW explode(aligned_entities) t AS ent
        GROUP BY bucket
        ORDER BY bucket"""
    )
    return rows


@router.get("/examples")
async def entity_examples(output_table: str = Query(...), limit: int = Query(50, le=200)):
    """Sample of detected entities with text, type, and confidence."""
    rows = fetch_all(
        f"""SELECT ent.entity, ent.entity_type, ent.confidence
        FROM {quote_table(output_table)}
        LATERAL VIEW explode(aligned_entities) t AS ent
        LIMIT {int(limit)}"""
    )
    return rows


@router.get("/evaluation")
async def evaluation_metrics(eval_table: str = Query(...)):
    """Precision/Recall/F1 from the long-format evaluation results table."""
    rows = fetch_all(
        f"""SELECT method_name, metric_name, metric_value, match_mode
        FROM {quote_table(eval_table)}
        WHERE metric_name IN ('precision', 'recall', 'f1_score', 'accuracy',
                              'true_positives', 'false_positives', 'false_negatives', 'true_negatives')
        ORDER BY method_name, metric_name"""
    )
    return rows


@router.get("/judge")
async def judge_grades(judge_table: str = Query(...)):
    """Grade distribution from the judge results table."""
    rows = fetch_all(
        f"""SELECT method, grade, count(*) as count
        FROM {quote_table(judge_table)}
        GROUP BY method, grade
        ORDER BY method, grade"""
    )
    return rows


@router.get("/judge-examples")
async def judge_examples(
    judge_table: str = Query(...),
    grade: str = Query("FAIL"),
    limit: int = Query(5, le=20),
):
    """Example documents for a specific judge grade."""
    rows = fetch_all(
        f"""SELECT doc_id, method, grade, findings
        FROM {quote_table(judge_table)}
        WHERE grade = %(grade)s
        LIMIT {int(limit)}""",
        {"grade": grade},
    )
    return rows


@router.get("/audit")
async def audit_summary(audit_table: str = Query(...)):
    """Audit rows: method-level P/R/F1 and judge rates per run."""
    rows = fetch_all(
        f"""SELECT method, match_mode, precision, recall, f1_score,
            judge_pass_rate, judge_partial_rate, judge_fail_rate,
            top_missed_entities, timestamp
        FROM {quote_table(audit_table)}
        ORDER BY timestamp DESC"""
    )
    return rows


@router.get("/recommendations")
async def recommendations(recs_table: str = Query(...)):
    """Next-best-action recommendations."""
    rows = fetch_all(
        f"""SELECT priority, method, action, rationale
        FROM {quote_table(recs_table)}
        ORDER BY priority"""
    )
    return rows


@router.get("/recommendations-for-lists")
async def recommendations_for_lists(recs_table: str = Query(...)):
    """Filter recommendations relevant to block/safe list changes."""
    rows = fetch_all(
        f"""SELECT priority, method, action, rationale
        FROM {quote_table(recs_table)}
        WHERE lower(action) LIKE '%block%'
           OR lower(action) LIKE '%safe%'
           OR lower(action) LIKE '%deny%'
           OR lower(action) LIKE '%allow%'
           OR lower(action) LIKE '%whitelist%'
           OR lower(action) LIKE '%blacklist%'
           OR lower(action) LIKE '%false positive%'
           OR lower(action) LIKE '%suppress%'
           OR lower(action) LIKE '%add to%'
           OR lower(rationale) LIKE '%block list%'
           OR lower(rationale) LIKE '%safe list%'
           OR lower(rationale) LIKE '%false positive%'
        ORDER BY priority"""
    )
    return rows


@router.get("/job-history")
async def job_metrics():
    return fetch_all(
        f"""SELECT config_id, status, count(*) as run_count,
            avg(timestampdiff(SECOND, started_at, completed_at)) as avg_duration_sec
        FROM {_table('redact_job_history')}
        WHERE started_at IS NOT NULL
        GROUP BY config_id, status
        ORDER BY run_count DESC"""
    )
