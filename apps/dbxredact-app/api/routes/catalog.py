"""UC catalog/schema/table browsing routes."""

from fastapi import APIRouter, HTTPException
from api.services.db import fetch_all, validate_identifier

router = APIRouter()


@router.get("/catalogs")
async def list_catalogs():
    rows = fetch_all("SHOW CATALOGS")
    return [r.get("catalog") or r.get("catalog_name") or list(r.values())[0] for r in rows]


@router.get("/schemas")
async def list_schemas(catalog: str):
    try:
        validate_identifier(catalog)
    except ValueError:
        raise HTTPException(400, "Invalid catalog name")
    rows = fetch_all(f"SHOW SCHEMAS IN `{catalog}`")
    return [r.get("databaseName") or r.get("namespace") or r.get("schema_name") or list(r.values())[0] for r in rows]


@router.get("/tables")
async def list_tables(catalog: str, schema: str):
    try:
        validate_identifier(catalog)
        validate_identifier(schema)
    except ValueError:
        raise HTTPException(400, "Invalid catalog or schema name")
    rows = fetch_all(f"SHOW TABLES IN `{catalog}`.`{schema}`")
    return [r.get("tableName") or r.get("table_name") or next(iter(r.values())) for r in rows]
