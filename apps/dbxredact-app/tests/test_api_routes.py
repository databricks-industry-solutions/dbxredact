"""Smoke tests for critical API routes using mocked DB layer."""

import sys
import os
from unittest.mock import patch, MagicMock

# Ensure the app directory is on the path
APP_DIR = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, APP_DIR)

import pytest

# Mock the Databricks SDK before importing the app
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


def _mock_fetch_all(sql, params=None):
    """Return plausible data for common queries."""
    sql_lower = sql.lower()
    if "redact_config" in sql_lower:
        return [{
            "config_id": "cfg-1",
            "name": "default",
            "use_presidio": True,
            "use_ai_query": True,
            "use_gliner": False,
            "endpoint": "databricks-gpt-oss-120b",
            "score_threshold": 0.5,
            "gliner_model": "nvidia/gliner-PII",
            "gliner_threshold": 0.2,
            "redaction_strategy": "typed",
            "alignment_mode": "union",
            "extra_params": None,
            "created_at": "2025-01-01T00:00:00",
            "updated_at": None,
        }]
    if "redact_job_history" in sql_lower:
        return []
    if "show catalogs" in sql_lower:
        return [{"catalog": "main"}]
    if "show schemas" in sql_lower:
        return [{"databaseName": "default"}]
    if "show tables" in sql_lower:
        return [{"tableName": "my_table"}]
    return []


def _mock_fetch_one(sql, params=None):
    rows = _mock_fetch_all(sql, params)
    return rows[0] if rows else None


def _mock_execute(sql, params=None):
    pass


class TestConfigRoutes:

    @patch("api.routes.config.fetch_all", side_effect=_mock_fetch_all)
    def test_list_configs(self, mock_fa):
        resp = client.get("/api/config/")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)

    @patch("api.routes.config.fetch_one", side_effect=_mock_fetch_one)
    def test_get_config(self, mock_fo):
        resp = client.get("/api/config/cfg-1")
        assert resp.status_code in (200, 404)


class TestCatalogRoutes:

    @patch("api.routes.catalog.fetch_all", side_effect=_mock_fetch_all)
    def test_list_catalogs(self, mock_fa):
        resp = client.get("/api/catalog/catalogs")
        assert resp.status_code == 200

    @patch("api.routes.catalog.fetch_all", side_effect=_mock_fetch_all)
    def test_list_schemas(self, mock_fa):
        resp = client.get("/api/catalog/schemas?catalog=main")
        assert resp.status_code == 200

    @patch("api.routes.catalog.fetch_all", side_effect=_mock_fetch_all)
    def test_list_tables(self, mock_fa):
        resp = client.get("/api/catalog/tables?catalog=main&schema=default")
        assert resp.status_code == 200


class TestPipelineRoutes:

    @patch("api.routes.pipeline.fetch_all", side_effect=_mock_fetch_all)
    def test_pipeline_history(self, mock_fa):
        resp = client.get("/api/pipeline/history")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)


class TestHealthCheck:

    def test_root_returns_html_or_json(self):
        resp = client.get("/")
        assert resp.status_code in (200, 404)
