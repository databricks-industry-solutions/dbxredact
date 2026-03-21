"""Tests for API routes using mocked DB layer.

Tests verify INTENDED behavior: correct status codes, response structure,
parameter validation, and that the right SQL operations are invoked.
"""

import sys
import os
from unittest.mock import patch, MagicMock, call

APP_DIR = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, APP_DIR)

import pytest

_mock_ws = MagicMock()
with patch.dict(os.environ, {
    "DATABRICKS_WAREHOUSE_ID": "test-warehouse",
    "CATALOG": "test_catalog",
    "SCHEMA": "test_schema",
}):
    with patch("databricks.sdk.WorkspaceClient", return_value=_mock_ws):
        from fastapi.testclient import TestClient
        from app import app

client = TestClient(app, raise_server_exceptions=False)

# ---------------------------------------------------------------------------
# Shared mock helpers
# ---------------------------------------------------------------------------

_CONFIG_ROW = {
    "config_id": "cfg-1",
    "name": "default",
    "detection_profile": "fast",
    "use_presidio": True,
    "use_ai_query": True,
    "use_gliner": False,
    "endpoint": "databricks-gpt-oss-120b",
    "score_threshold": 0.5,
    "gliner_model": "nvidia/gliner-PII",
    "gliner_threshold": 0.2,
    "gliner_max_words": 256,
    "redaction_strategy": "typed",
    "alignment_mode": "union",
    "reasoning_effort": "low",
    "presidio_model_size": "trf",
    "presidio_pattern_only": False,
    "extra_params": None,
    "created_at": "2025-01-01T00:00:00",
    "updated_at": None,
}

_BLOCK_ENTRY = {
    "entry_id": "blk-1", "value": "SSN-\\d+", "is_pattern": True,
    "entity_type": "US_SSN", "notes": "test", "list_type": "block",
    "created_at": "2025-01-01T00:00:00",
}

_SAFE_ENTRY = {
    "entry_id": "safe-1", "value": "Dr. House", "is_pattern": False,
    "entity_type": "PERSON", "notes": None, "list_type": "safe",
    "created_at": "2025-01-01T00:00:00",
}

_ANNOTATION = {
    "annotation_id": "ann-1", "doc_id": "d1", "source_table": "cat.sch.tbl",
    "entity_text": "John", "entity_type": "PERSON", "start": 0, "end_pos": 4,
    "action": "accept", "corrected_type": None, "corrected_value": None,
    "detection_method": "presidio", "workflow": "review",
    "created_at": "2025-01-01T00:00:00",
}

_JOB_HISTORY = {
    "run_id": 123, "config_id": "cfg-1", "source_table": "cat.sch.tbl",
    "output_table": "cat.sch.tbl_redacted", "status": "RUNNING",
    "cost_estimate_usd": None, "started_at": "2025-01-01T00:00:00",
    "completed_at": None,
}

_QUEUE_ITEM = {
    "doc_id": "d1", "source_table": "cat.sch.det", "priority_score": 0.8,
    "status": "pending", "assigned_to": None,
    "created_at": "2025-01-01T00:00:00", "reviewed_at": None,
}


def _mock_fetch_all(sql, params=None):
    sql_lower = sql.lower()
    if "redact_block_list" in sql_lower:
        return [_BLOCK_ENTRY]
    if "redact_safe_list" in sql_lower:
        return [_SAFE_ENTRY]
    if "redact_config" in sql_lower:
        return [_CONFIG_ROW]
    if "redact_job_history" in sql_lower:
        return [_JOB_HISTORY]
    if "redact_annotations" in sql_lower:
        return [_ANNOTATION]
    if "redact_active_learn_queue" in sql_lower:
        return [_QUEUE_ITEM]
    if "redact_audit_log" in sql_lower and "group by" in sql_lower:
        return [{"entity_type": "PERSON", "total_entities": 10, "doc_count": 5}]
    if "redact_audit_log" in sql_lower:
        return [{"run_id": "r1", "doc_id": "d1", "entity_type": "PERSON",
                 "entity_count": 3, "created_at": "2025-01-01T00:00:00"}]
    if "show catalogs" in sql_lower:
        return [{"catalog": "main"}]
    if "show schemas" in sql_lower:
        return [{"databaseName": "default"}]
    if "show tables" in sql_lower:
        return [{"tableName": "my_table"}]
    return []


def _mock_fetch_one(sql, params=None):
    sql_lower = sql.lower()
    if "redact_active_learn_queue" in sql_lower and "total_queued" in sql_lower:
        return {"total_queued": 10, "reviewed": 3, "pending": 6, "skipped": 1, "avg_priority": 0.65}
    if "redact_active_learn_queue" in sql_lower:
        return _QUEUE_ITEM
    if "redact_audit_log" in sql_lower and "total_docs" in sql_lower:
        return {"total_docs": 5, "total_runs": 2, "total_entities": 50}
    if "redact_config" in sql_lower:
        return _CONFIG_ROW
    if "redact_annotations" in sql_lower or "redact_ground_truths" in sql_lower:
        return {"cnt": 0, "oldest": "", "newest": ""}
    rows = _mock_fetch_all(sql, params)
    return rows[0] if rows else None


def _mock_execute(sql, params=None):
    pass


# ===================================================================
# Config routes
# ===================================================================

class TestConfigRoutes:

    @patch("api.routes.config.fetch_all", side_effect=_mock_fetch_all)
    def test_list_configs_returns_list(self, _):
        resp = client.get("/api/config/")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert data[0]["config_id"] == "cfg-1"

    @patch("api.routes.config.fetch_one", side_effect=_mock_fetch_one)
    def test_get_config_returns_config(self, _):
        resp = client.get("/api/config/cfg-1")
        assert resp.status_code == 200
        assert resp.json()["name"] == "default"

    @patch("api.routes.config.fetch_one", return_value=None)
    def test_get_config_404_when_missing(self, _):
        resp = client.get("/api/config/nonexistent")
        assert resp.status_code == 404

    @patch("api.routes.config.execute", side_effect=_mock_execute)
    @patch("api.routes.config.fetch_one", return_value=None)
    def test_create_config(self, _, __):
        resp = client.post("/api/config/", json={"name": "new-config"})
        assert resp.status_code == 201
        assert "config_id" in resp.json()

    @patch("api.routes.config.execute", side_effect=_mock_execute)
    @patch("api.routes.config.fetch_one", side_effect=_mock_fetch_one)
    def test_update_config(self, _, __):
        resp = client.put("/api/config/cfg-1", json={"name": "updated"})
        assert resp.status_code == 200

    @patch("api.routes.config.execute", side_effect=_mock_execute)
    def test_delete_config(self, _):
        resp = client.delete("/api/config/cfg-1")
        assert resp.status_code == 204


# ===================================================================
# Catalog routes
# ===================================================================

class TestCatalogRoutes:

    @patch("api.routes.catalog.fetch_all", side_effect=_mock_fetch_all)
    def test_list_catalogs(self, _):
        resp = client.get("/api/catalog/catalogs")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    @patch("api.routes.catalog.fetch_all", side_effect=_mock_fetch_all)
    def test_list_schemas(self, _):
        resp = client.get("/api/catalog/schemas?catalog=main")
        assert resp.status_code == 200

    @patch("api.routes.catalog.fetch_all", side_effect=_mock_fetch_all)
    def test_list_tables(self, _):
        resp = client.get("/api/catalog/tables?catalog=main&schema=default")
        assert resp.status_code == 200


# ===================================================================
# Lists routes (block / safe)
# ===================================================================

class TestListsRoutes:

    @patch("api.routes.lists.fetch_all", side_effect=_mock_fetch_all)
    def test_list_block_entries(self, _):
        resp = client.get("/api/lists/block")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert data[0]["list_type"] == "block"

    @patch("api.routes.lists.execute", side_effect=_mock_execute)
    def test_add_block_entry_returns_201(self, mock_exec):
        resp = client.post("/api/lists/block", json={
            "value": "SSN-\\d+", "is_pattern": True, "entity_type": "US_SSN",
        })
        assert resp.status_code == 201
        body = resp.json()
        assert "entry_id" in body
        assert body["value"] == "SSN-\\d+"
        assert body["list_type"] == "block"
        mock_exec.assert_called_once()

    @patch("api.routes.lists.execute", side_effect=_mock_execute)
    def test_delete_block_entry(self, mock_exec):
        resp = client.delete("/api/lists/block/blk-1")
        assert resp.status_code == 204
        mock_exec.assert_called_once()

    @patch("api.routes.lists.fetch_all", side_effect=_mock_fetch_all)
    def test_list_safe_entries(self, _):
        resp = client.get("/api/lists/safe")
        assert resp.status_code == 200
        data = resp.json()
        assert data[0]["list_type"] == "safe"

    @patch("api.routes.lists.execute", side_effect=_mock_execute)
    def test_add_safe_entry_returns_201(self, mock_exec):
        resp = client.post("/api/lists/safe", json={
            "value": "Dr. House", "is_pattern": False,
        })
        assert resp.status_code == 201
        assert resp.json()["list_type"] == "safe"

    @patch("api.routes.lists.execute", side_effect=_mock_execute)
    def test_delete_safe_entry(self, mock_exec):
        resp = client.delete("/api/lists/safe/safe-1")
        assert resp.status_code == 204


# ===================================================================
# Labels routes
# ===================================================================

class TestLabelsRoutes:

    @patch("api.routes.labels.fetch_all", side_effect=_mock_fetch_all)
    def test_list_unlabeled_documents(self, _):
        resp = client.get("/api/labels/documents?source_table=cat.sch.tbl")
        assert resp.status_code == 200

    @patch("api.routes.labels.execute", side_effect=_mock_execute)
    @patch("api.routes.labels.fetch_all", return_value=[])
    def test_batch_label_saves_and_returns_count(self, _, mock_exec):
        resp = client.post("/api/labels/batch", json={
            "doc_id": "d1",
            "source_table": "cat.sch.tbl",
            "labels": [
                {"entity_text": "John", "entity_type": "PERSON", "start": 0, "end_pos": 4},
                {"entity_text": "555-1234", "entity_type": "PHONE", "start": 10, "end_pos": 18},
            ],
        })
        assert resp.status_code == 200
        assert resp.json()["labeled"] == 2

    @patch("api.routes.labels.execute", side_effect=_mock_execute)
    def test_delete_labels(self, mock_exec):
        resp = client.delete("/api/labels/d1?source_table=cat.sch.tbl")
        assert resp.status_code == 200
        assert resp.json()["deleted"] is True

    @patch("api.routes.labels.fetch_one", return_value={"total_docs": 100, "labeled_docs": 25})
    def test_labeling_stats(self, _):
        resp = client.get("/api/labels/stats?source_table=cat.sch.tbl")
        assert resp.status_code == 200
        data = resp.json()
        assert "total_docs" in data
        assert "labeled_docs" in data

    def test_list_unlabeled_rejects_sql_injection_column(self):
        resp = client.get("/api/labels/documents?source_table=cat.sch.tbl&text_column=bad;col")
        assert resp.status_code >= 400


# ===================================================================
# Review / Annotations routes
# ===================================================================

class TestReviewRoutes:

    @patch("api.routes.review.fetch_one", side_effect=_mock_fetch_one)
    @patch("api.routes.review.fetch_all", return_value=[
        {"doc_id": "d1", "original_text": "John Smith", "redacted_text": "[PERSON]"}
    ])
    def test_compare_returns_rows_and_total(self, _, __):
        resp = client.get(
            "/api/review/compare?source_table=cat.sch.src&output_table=cat.sch.out"
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "rows" in data
        assert "total" in data

    @patch("api.routes.review.fetch_all", return_value=[{"doc_id": "d1", "text": "hello"}])
    def test_list_documents(self, _):
        resp = client.get("/api/review/documents?source_table=cat.sch.tbl")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    @patch("api.routes.review.fetch_one", return_value={"doc_id": "d1", "text": "hello"})
    def test_get_document(self, _):
        resp = client.get("/api/review/documents/d1?source_table=cat.sch.tbl")
        assert resp.status_code == 200

    @patch("api.routes.review.fetch_one", return_value=None)
    def test_get_document_404(self, _):
        resp = client.get("/api/review/documents/missing?source_table=cat.sch.tbl")
        assert resp.status_code == 404

    @patch("api.routes.review.execute", side_effect=_mock_execute)
    def test_create_annotation(self, mock_exec):
        resp = client.post("/api/review/annotations", json={
            "doc_id": "d1", "source_table": "cat.sch.tbl",
            "entity_text": "John", "entity_type": "PERSON",
            "start": 0, "end_pos": 4, "action": "accept",
        })
        assert resp.status_code == 201
        body = resp.json()
        assert "annotation_id" in body
        assert body["action"] == "accept"

    @patch("api.routes.review.fetch_all", side_effect=_mock_fetch_all)
    def test_list_annotations(self, _):
        resp = client.get("/api/review/annotations")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_compare_rejects_sql_injection(self):
        resp = client.get(
            "/api/review/compare?source_table=cat.sch.src&output_table=cat.sch.out"
            "&source_column=bad;col"
        )
        assert resp.status_code >= 400


# ===================================================================
# Pipeline routes
# ===================================================================

class TestPipelineRoutes:

    @patch("api.routes.pipeline.fetch_all", side_effect=_mock_fetch_all)
    def test_pipeline_history(self, _):
        resp = client.get("/api/pipeline/history")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    @patch("api.routes.pipeline.trigger_pipeline_run", return_value=999)
    @patch("api.routes.pipeline.get_run_status", return_value={
        "run_id": 999, "state": "RUNNING", "result_state": None,
        "start_time": 1000, "end_time": None, "run_page_url": "https://example.com",
    })
    @patch("api.routes.pipeline.execute", side_effect=_mock_execute)
    @patch("api.routes.pipeline.fetch_one", side_effect=_mock_fetch_one)
    def test_run_pipeline_returns_status(self, _, __, ___, mock_trigger):
        resp = client.post("/api/pipeline/run", json={
            "config_id": "cfg-1",
            "source_table": "cat.sch.tbl",
            "text_column": "text",
        })
        assert resp.status_code == 200
        body = resp.json()
        assert body["run_id"] == 999
        assert body["state"] == "RUNNING"

        params = mock_trigger.call_args[0][0]
        assert params["source_table"] == "cat.sch.tbl"
        assert params["text_column"] == "text"
        assert params["use_presidio"] == "true"
        assert params["use_ai_query"] == "true"
        assert params["alignment_mode"] == "union"
        assert params["confirm_destructive"] == "false"
        assert params["allow_consensus_redaction"] == "false"
        assert "audit_table" in params

    @patch("api.routes.pipeline.fetch_one", return_value=None)
    def test_run_pipeline_404_when_config_missing(self, _):
        resp = client.post("/api/pipeline/run", json={
            "config_id": "nonexistent",
            "source_table": "cat.sch.tbl",
            "text_column": "text",
        })
        assert resp.status_code == 404

    @patch("api.routes.pipeline.trigger_pipeline_run", return_value=999)
    @patch("api.routes.pipeline.get_run_status", return_value={
        "run_id": 999, "state": "RUNNING", "result_state": None,
        "start_time": 1000, "end_time": None, "run_page_url": None,
    })
    @patch("api.routes.pipeline.execute", side_effect=_mock_execute)
    @patch("api.routes.pipeline.fetch_one", side_effect=_mock_fetch_one)
    def test_run_in_place_sets_confirm_destructive(self, _, mock_exec, __, mock_trigger):
        resp = client.post("/api/pipeline/run", json={
            "config_id": "cfg-1",
            "source_table": "cat.sch.tbl",
            "text_column": "text",
            "output_mode": "in_place",
        })
        assert resp.status_code == 200
        insert_call = [c for c in mock_exec.call_args_list if "INSERT" in str(c)]
        assert len(insert_call) >= 1

        params = mock_trigger.call_args[0][0]
        assert params["confirm_destructive"] == "true"
        assert params["output_mode"] == "in_place"

    @patch("api.routes.pipeline.trigger_pipeline_run", return_value=999)
    @patch("api.routes.pipeline.get_run_status", return_value={
        "run_id": 999, "state": "RUNNING", "result_state": None,
        "start_time": 1000, "end_time": None, "run_page_url": None,
    })
    @patch("api.routes.pipeline.execute", side_effect=_mock_execute)
    @patch("api.routes.pipeline.fetch_one", return_value={**_CONFIG_ROW, "alignment_mode": "consensus"})
    def test_consensus_alignment_sets_allow_flag(self, _, __, ___, mock_trigger):
        resp = client.post("/api/pipeline/run", json={
            "config_id": "cfg-1",
            "source_table": "cat.sch.tbl",
            "text_column": "text",
        })
        assert resp.status_code == 200
        params = mock_trigger.call_args[0][0]
        assert params["allow_consensus_redaction"] == "true"

    @patch("api.routes.pipeline.get_run_status", return_value={
        "run_id": 123, "state": "TERMINATED", "result_state": "SUCCESS",
        "start_time": 1000, "end_time": 2000, "run_page_url": None,
    })
    @patch("api.routes.pipeline.execute", side_effect=_mock_execute)
    @patch("api.routes.pipeline.fetch_one", side_effect=_mock_fetch_one)
    @patch("api.routes.pipeline.fetch_all", return_value=[])
    def test_status_updates_history_on_terminal(self, _, __, mock_exec, ___):
        resp = client.get("/api/pipeline/status/123")
        assert resp.status_code == 200
        update_calls = [c for c in mock_exec.call_args_list if "UPDATE" in str(c)]
        assert len(update_calls) >= 1

    @patch("api.routes.pipeline.cancel_run")
    @patch("api.routes.pipeline.execute", side_effect=_mock_execute)
    def test_cancel_pipeline(self, _, mock_cancel):
        resp = client.post("/api/pipeline/cancel/123")
        assert resp.status_code == 204
        mock_cancel.assert_called_once_with(123)

    @patch("api.routes.pipeline.fetch_one", return_value={
        "total_chars": 50000, "cnt": 100,
    })
    def test_cost_estimate_returns_breakdown(self, _):
        resp = client.get(
            "/api/pipeline/cost-estimate?table=cat.sch.tbl&text_column=text"
        )
        assert resp.status_code == 200
        body = resp.json()
        for key in ("row_count", "total_chars", "ai_query_cost_usd",
                     "compute_cost_usd", "estimated_cost_usd", "estimated_minutes"):
            assert key in body
        assert body["estimated_cost_usd"] >= 0

    @patch("api.routes.pipeline.fetch_one", return_value={"total_chars": 50000, "cnt": 100})
    def test_cost_estimate_zero_without_ai(self, _):
        resp = client.get(
            "/api/pipeline/cost-estimate?table=cat.sch.tbl&text_column=text&use_ai_query=false"
        )
        assert resp.status_code == 200
        assert resp.json()["ai_query_cost_usd"] == 0.0


# ===================================================================
# Admin routes
# ===================================================================

class TestAdminRoutes:

    @patch("api.routes.admin.fetch_one", side_effect=_mock_fetch_one)
    @patch("api.routes.admin.execute", side_effect=_mock_execute)
    def test_purge_annotations(self, _, __):
        resp = client.post("/api/admin/purge-annotations?retention_days=30")
        assert resp.status_code == 200
        body = resp.json()
        assert "retention_days" in body
        assert body["retention_days"] == 30

    @patch("api.routes.admin.fetch_one", side_effect=_mock_fetch_one)
    def test_retention_status(self, _):
        resp = client.get("/api/admin/retention-status")
        assert resp.status_code == 200
        body = resp.json()
        assert "retention_days" in body
        assert "tables" in body

    @patch("api.routes.admin.fetch_all", side_effect=_mock_fetch_all)
    def test_audit_log(self, _):
        resp = client.get("/api/admin/audit-log")
        assert resp.status_code == 200
        body = resp.json()
        assert "rows" in body
        assert "count" in body

    @patch("api.routes.admin.fetch_one", side_effect=_mock_fetch_one)
    @patch("api.routes.admin.fetch_all", side_effect=_mock_fetch_all)
    def test_audit_summary(self, _, __):
        resp = client.get("/api/admin/audit-summary")
        assert resp.status_code == 200
        body = resp.json()
        assert "total_docs" in body
        assert "by_entity_type" in body


# ===================================================================
# Active Learning routes
# ===================================================================

class TestActiveLearningRoutes:

    @patch("api.routes.active_learn.fetch_all", side_effect=_mock_fetch_all)
    def test_get_queue(self, _):
        resp = client.get("/api/active-learn/queue")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    @patch("api.routes.active_learn.fetch_one", side_effect=_mock_fetch_one)
    def test_queue_stats(self, _):
        resp = client.get("/api/active-learn/stats")
        assert resp.status_code == 200
        body = resp.json()
        assert body["total_queued"] == 10
        assert body["pending"] == 6

    @patch("api.routes.active_learn.execute", side_effect=_mock_execute)
    @patch("api.routes.active_learn.fetch_one", side_effect=_mock_fetch_one)
    def test_review_document(self, _, __):
        resp = client.post("/api/active-learn/queue/d1/review", json={
            "corrections": [{
                "doc_id": "d1", "source_table": "cat.sch.tbl",
                "entity_text": "John", "entity_type": "PERSON",
                "start": 0, "end_pos": 4, "action": "accept",
            }],
        })
        assert resp.status_code == 200
        assert resp.json()["status"] == "reviewed"
        assert resp.json()["annotations_saved"] == 1

    @patch("api.routes.active_learn.fetch_one", return_value=None)
    def test_review_document_404_when_not_queued(self, _):
        resp = client.post("/api/active-learn/queue/missing/review", json={
            "corrections": [],
        })
        assert resp.status_code == 404


# ===================================================================
# Pydantic schema validation
# ===================================================================

class TestSchemaValidation:

    def test_config_create_score_threshold_min(self):
        resp = client.post("/api/config/", json={
            "name": "bad", "score_threshold": 0.05,
        })
        assert resp.status_code == 422

    def test_config_create_score_threshold_max(self):
        resp = client.post("/api/config/", json={
            "name": "bad", "score_threshold": 1.5,
        })
        assert resp.status_code == 422

    def test_config_create_gliner_threshold_min(self):
        resp = client.post("/api/config/", json={
            "name": "bad", "gliner_threshold": 0.01,
        })
        assert resp.status_code == 422

    def test_pipeline_run_requires_config_id(self):
        resp = client.post("/api/pipeline/run", json={
            "source_table": "cat.sch.tbl",
        })
        assert resp.status_code == 422

    def test_pipeline_run_requires_source_table(self):
        resp = client.post("/api/pipeline/run", json={
            "config_id": "cfg-1",
        })
        assert resp.status_code == 422

    def test_pipeline_run_max_rows_limit(self):
        resp = client.post("/api/pipeline/run", json={
            "config_id": "cfg-1", "source_table": "cat.sch.tbl",
            "max_rows": 2_000_000,
        })
        assert resp.status_code == 422

    def test_label_create_start_non_negative(self):
        resp = client.post("/api/labels/batch", json={
            "doc_id": "d1", "source_table": "cat.sch.tbl",
            "labels": [{"entity_text": "John", "entity_type": "PERSON", "start": -1, "end_pos": 4}],
        })
        assert resp.status_code == 422


# ===================================================================
# Health check
# ===================================================================

class TestHealthCheck:

    def test_root_returns_html_or_json(self):
        resp = client.get("/")
        assert resp.status_code in (200, 404)
