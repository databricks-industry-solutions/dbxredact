"""Databricks SDK wrapper for Unity Catalog table access via statement execution."""

import os
import re
import time
import logging
from typing import Any, Dict, List, Optional

from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)


class DatabaseError(Exception):
    """Raised for classifiable database/infrastructure failures."""

    def __init__(self, user_message: str, status_code: int = 500):
        super().__init__(user_message)
        self.user_message = user_message
        self.status_code = status_code

_MAX_RETRIES = 3
_INITIAL_BACKOFF_S = 1.0

_client: Optional[WorkspaceClient] = None

WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
CATALOG = os.environ.get("CATALOG", "main")
SCHEMA = os.environ.get("SCHEMA", "default")


def _get_client() -> WorkspaceClient:
    global _client
    if _client is None:
        _client = WorkspaceClient()
    return _client


_IDENTIFIER_RE = re.compile(r"^[a-zA-Z0-9_\-]+$")


def validate_identifier(name: str) -> str:
    """Validate a SQL identifier (catalog, schema, table, column name).

    Rejects anything that isn't alphanumeric, underscore, or hyphen to prevent
    SQL injection through identifier interpolation.
    """
    if not name or not _IDENTIFIER_RE.match(name):
        raise ValueError(f"Invalid SQL identifier: {name!r}")
    return name


def _table(name: str) -> str:
    validate_identifier(name)
    return f"`{CATALOG}`.`{SCHEMA}`.`{name}`"


def quote_table(qualified_name: str) -> str:
    """Backtick-quote each part of a qualified table name (catalog.schema.table)."""
    parts = qualified_name.split(".")
    for p in parts:
        validate_identifier(p)
    return ".".join(f"`{p}`" for p in parts)


def _parse_result(result) -> List[Dict[str, Any]]:
    """Parse statement execution result into list of dicts."""
    if not result.result or not result.result.data_array:
        return []
    columns = [col.name for col in result.manifest.schema.columns]
    return [dict(zip(columns, row)) for row in result.result.data_array]


def _classify_error(exc: Exception) -> DatabaseError:
    """Convert known exceptions into user-friendly DatabaseError instances."""
    msg = str(exc)
    msg_lower = msg.lower()
    if "not a valid endpoint id" in msg_lower or "invalid warehouse" in msg_lower:
        return DatabaseError(
            "SQL Warehouse is misconfigured or unavailable. "
            "Check the DATABRICKS_WAREHOUSE_ID setting.", 503,
        )
    if "table_or_view_not_found" in msg_lower:
        return DatabaseError(f"Table not found. {msg}", 404)
    if "permission_denied" in msg_lower or "access_denied" in msg_lower:
        return DatabaseError(
            f"Permission denied. Check Unity Catalog grants. {msg}", 403,
        )
    if "warehouse" in msg_lower and ("stopped" in msg_lower or "not running" in msg_lower):
        return DatabaseError("SQL Warehouse is stopped. Start it and retry.", 503)
    return DatabaseError(msg, 500)


def _execute_with_retry(statement: str):
    """Execute a statement with exponential backoff on transient failures."""
    last_exc = None
    for attempt in range(_MAX_RETRIES):
        try:
            result = _get_client().statement_execution.execute_statement(
                warehouse_id=WAREHOUSE_ID, statement=statement, wait_timeout="30s",
            )
            if result.status and result.status.error:
                raise RuntimeError(f"SQL error: {result.status.error.message}")
            return result
        except DatabaseError:
            raise
        except Exception as exc:
            last_exc = exc
            msg = str(exc).lower()
            is_transient = any(
                kw in msg for kw in ("temporarily unavailable", "timeout", "throttl", "429", "503")
            )
            if not is_transient or attempt == _MAX_RETRIES - 1:
                raise _classify_error(exc) from exc
            wait = _INITIAL_BACKOFF_S * (2 ** attempt)
            logger.warning("Transient SQL error (attempt %d/%d), retrying in %.1fs: %s",
                           attempt + 1, _MAX_RETRIES, wait, exc)
            time.sleep(wait)
    raise _classify_error(last_exc) from last_exc


def execute(sql: str, params: Optional[Dict[str, Any]] = None) -> None:
    """Execute a SQL statement (INSERT/UPDATE/DELETE). Params are interpolated manually."""
    statement = _interpolate(sql, params) if params else sql
    logger.debug("execute: %s", statement[:200])
    _execute_with_retry(statement)


def fetch_all(sql: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    statement = _interpolate(sql, params) if params else sql
    logger.debug("fetch_all: %s", statement[:200])
    result = _execute_with_retry(statement)
    return _parse_result(result)


def fetch_one(sql: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    rows = fetch_all(sql, params)
    return rows[0] if rows else None


def _interpolate(sql: str, params: Dict[str, Any]) -> str:
    """Replace %(key)s placeholders with escaped values.

    The Databricks statement execution API doesn't support parameterized queries
    in the same way as DB-API cursors, so we do safe interpolation here.
    """
    escaped = {}
    for k, v in params.items():
        if v is None:
            escaped[k] = "NULL"
        elif isinstance(v, bool):
            escaped[k] = "true" if v else "false"
        elif isinstance(v, (int, float)):
            escaped[k] = str(v)
        else:
            escaped[k] = "'" + str(v).replace("'", "''") + "'"
    return sql % escaped
