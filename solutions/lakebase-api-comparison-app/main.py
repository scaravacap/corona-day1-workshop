"""Corona API comparison app.

Compares low-latency reads through the Lakebase Data API with analytical reads
through the Databricks SQL Statement Execution API.
"""

import asyncio
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from fastapi import FastAPI, Query, Request
from fastapi.staticfiles import StaticFiles

ROOT = Path(__file__).parent
ALLOWED_PLANTS = {"Sabaneta", "Sopó", "Girardota"}
SELECTED_COLUMNS = [
    "lote_id",
    "fecha_produccion",
    "planta",
    "linea",
    "turno",
    "familia_producto",
    "sku",
    "toneladas_producidas",
    "tasa_defectos",
    "energia_kwh_ton",
    "estado_calidad",
]

CATALOG = os.getenv("LAKEHOUSE_CATALOG", "corona_workshop")
SCHEMA = os.getenv("LAKEHOUSE_SCHEMA", "operaciones")
TABLE = os.getenv("LAKEHOUSE_TABLE", "produccion_calidad")
LAKEBASE_SCHEMA = os.getenv("LAKEBASE_SCHEMA", "public")
LAKEBASE_TABLE = os.getenv("LAKEBASE_TABLE", "produccion_calidad")
WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "")
DATA_API_URL = os.getenv("LAKEBASE_DATA_API_URL", "")
USE_MOCK_DATA = os.getenv("USE_MOCK_DATA", "false").lower() == "true"

app = FastAPI(title="Corona Lakehouse vs Lakebase", version="1.0.0")


class ApiCallError(RuntimeError):
    def __init__(self, message: str, technical: dict[str, Any]):
        super().__init__(message)
        self.technical = technical


def _workspace_client() -> WorkspaceClient:
    return WorkspaceClient()


def _auth_headers() -> dict[str, str]:
    return dict(Config().authenticate())


def _data_api_base_url() -> str:
    value = DATA_API_URL.rstrip("/")
    if value and "REPLACE" not in value.upper():
        return value

    host = os.getenv("PGHOST", "").strip()
    workspace_id = os.getenv("DATABRICKS_WORKSPACE_ID", "").strip()
    database = os.getenv("PGDATABASE", "").strip()
    if host and workspace_id and database:
        return (
            f"https://{host}/api/2.0/workspace/{workspace_id}/rest/{database}"
        )

    raise RuntimeError(
        "No pude construir la URL de Data API. Adjunta el recurso Postgres o "
        "configura LAKEBASE_DATA_API_URL desde la página Lakebase > Data API."
    )


def _statement_rows(response: dict[str, Any]) -> list[dict[str, Any]]:
    state = response.get("status", {}).get("state")
    if state != "SUCCEEDED":
        message = response.get("status", {}).get("error", {}).get(
            "message", f"Statement terminó con estado {state}"
        )
        raise RuntimeError(message)

    columns = [
        column["name"]
        for column in response.get("manifest", {})
        .get("schema", {})
        .get("columns", [])
    ]
    values = response.get("result", {}).get("data_array", []) or []
    return [dict(zip(columns, row)) for row in values]


def _run_statement_api(planta: str, limit: int) -> dict[str, Any]:
    if not WAREHOUSE_ID:
        raise RuntimeError(
            "DATABRICKS_WAREHOUSE_ID no está configurado. Agrega un recurso "
            "SQL warehouse con la clave sql-warehouse."
        )

    statement = f"""
        SELECT {", ".join(SELECTED_COLUMNS)}
        FROM {CATALOG}.{SCHEMA}.{TABLE}
        WHERE planta = :planta
        ORDER BY fecha_evento DESC, lote_id ASC
    """
    payload = {
        "warehouse_id": WAREHOUSE_ID,
        "catalog": CATALOG,
        "schema": SCHEMA,
        "statement": statement,
        "parameters": [{"name": "planta", "value": planta, "type": "STRING"}],
        "format": "JSON_ARRAY",
        "disposition": "INLINE",
        "wait_timeout": "50s",
        "on_wait_timeout": "CONTINUE",
        "row_limit": limit,
        "query_tags": [
            {"key": "workshop", "value": "corona-day1"},
            {"key": "path", "value": "statement-execution-api"},
        ],
    }

    client = _workspace_client()
    started = time.perf_counter()
    response = client.api_client.do(
        method="POST",
        path="/api/2.0/sql/statements",
        body=payload,
    )

    deadline = time.monotonic() + 55
    while response.get("status", {}).get("state") in {"PENDING", "RUNNING"}:
        if time.monotonic() >= deadline:
            raise TimeoutError("Statement Execution API superó 55 segundos.")
        time.sleep(0.35)
        response = client.api_client.do(
            method="GET",
            path=f"/api/2.0/sql/statements/{response['statement_id']}",
        )

    technical = {
        "request": {
            "method": "POST",
            "url": "/api/2.0/sql/statements",
            "headers": {
                "Authorization": "Bearer [REDACTED]",
                "Content-Type": "application/json",
            },
            "body": payload,
        },
        "response": {
            "status_code": 200,
            "headers": {"Content-Type": "application/json"},
            "body": response,
        },
    }
    try:
        rows = _statement_rows(response)
    except RuntimeError as error:
        raise ApiCallError(str(error), technical) from error

    elapsed_ms = round((time.perf_counter() - started) * 1000, 1)
    return {
        "source": "Statement Execution API",
        "elapsed_ms": elapsed_ms,
        "rows": rows,
        "row_count": len(rows),
        "request_id": response.get("statement_id"),
        "server_timing": None,
        "freshness": f"{CATALOG}.{SCHEMA}.{TABLE}",
        "technical": technical,
    }


async def _run_data_api(planta: str, limit: int) -> dict[str, Any]:
    base_url = _data_api_base_url()
    headers = await asyncio.to_thread(_auth_headers)
    headers.update({"Accept": "application/json", "Prefer": "count=exact"})
    params = {
        "select": ",".join(SELECTED_COLUMNS),
        "planta": f"eq.{planta}",
        "order": "fecha_evento.desc,lote_id.asc",
        "limit": str(limit),
    }

    started = time.perf_counter()
    async with httpx.AsyncClient(timeout=55.0) as client:
        response = await client.get(
            f"{base_url}/{LAKEBASE_SCHEMA}/{LAKEBASE_TABLE}",
            headers=headers,
            params=params,
        )
    elapsed_ms = round((time.perf_counter() - started) * 1000, 1)
    try:
        body = response.json()
    except ValueError:
        body = {"raw_text": response.text}

    technical = {
        "request": {
            "method": "GET",
            "url": str(response.request.url),
            "headers": {
                "Authorization": "Bearer [REDACTED]",
                "Accept": "application/json",
                "Prefer": "count=exact",
            },
            "query_parameters": params,
        },
        "response": {
            "status_code": response.status_code,
            "headers": {
                key: value
                for key, value in {
                    "Content-Type": response.headers.get("content-type"),
                    "Content-Range": response.headers.get("content-range"),
                    "Server-Timing": response.headers.get("server-timing"),
                    "X-Request-Id": response.headers.get("x-request-id"),
                }.items()
                if value is not None
            },
            "body": body,
        },
    }
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as error:
        raise ApiCallError(
            f"Data API devolvió HTTP {response.status_code}.", technical
        ) from error

    rows = body
    if not isinstance(rows, list):
        raise ApiCallError("Data API devolvió un payload inesperado.", technical)

    return {
        "source": "Lakebase Data API",
        "elapsed_ms": elapsed_ms,
        "rows": rows,
        "row_count": len(rows),
        "request_id": response.headers.get("x-request-id"),
        "server_timing": response.headers.get("server-timing"),
        "freshness": f"{LAKEBASE_SCHEMA}.{LAKEBASE_TABLE} (synced table)",
        "technical": technical,
    }


def _mock_result(source: str, planta: str, limit: int) -> dict[str, Any]:
    rows = [
        {
            "lote_id": f"MOCK-{index:04d}",
            "fecha_produccion": "2026-09-10",
            "planta": planta,
            "linea": "Horno-2",
            "turno": "A",
            "familia_producto": "Pisos",
            "sku": "PISO-ANDINO-60",
            "toneladas_producidas": 34.5,
            "tasa_defectos": 0.2963,
            "energia_kwh_ton": 959.1,
            "estado_calidad": "Revisar",
        }
        for index in range(1, min(limit, 10) + 1)
    ]
    is_data_api = source == "Lakebase Data API"
    request = (
        {
            "method": "GET",
            "url": (
                "https://lakebase.example/api/2.0/workspace/123/rest/"
                f"databricks_postgres/public/produccion_calidad?planta=eq.{planta}"
            ),
            "headers": {
                "Authorization": "Bearer [REDACTED]",
                "Accept": "application/json",
                "Prefer": "count=exact",
            },
            "query_parameters": {
                "planta": f"eq.{planta}",
                "order": "fecha_evento.desc,lote_id.asc",
                "limit": str(limit),
            },
        }
        if is_data_api
        else {
            "method": "POST",
            "url": "/api/2.0/sql/statements",
            "headers": {
                "Authorization": "Bearer [REDACTED]",
                "Content-Type": "application/json",
            },
            "body": {
                "warehouse_id": "[valueFrom: sql-warehouse]",
                "statement": "SELECT ... WHERE planta = :planta",
                "parameters": [
                    {"name": "planta", "value": planta, "type": "STRING"}
                ],
                "row_limit": limit,
            },
        }
    )
    response = (
        {
            "status_code": 206,
            "headers": {
                "Content-Type": "application/json",
                "Content-Range": f"0-{len(rows) - 1}/{len(rows)}",
                "Server-Timing": "db;dur=18.2",
            },
            "body": rows,
        }
        if is_data_api
        else {
            "status_code": 200,
            "headers": {"Content-Type": "application/json"},
            "body": {
                "statement_id": f"mock-{uuid.uuid4()}",
                "status": {"state": "SUCCEEDED"},
                "result": {"data_array": rows},
            },
        }
    )
    return {
        "source": source,
        "elapsed_ms": 42.7 if is_data_api else 812.4,
        "rows": rows,
        "row_count": len(rows),
        "request_id": f"mock-{uuid.uuid4()}",
        "server_timing": "db;dur=18.2" if is_data_api else None,
        "freshness": "Datos mock para desarrollo local",
        "technical": {"request": request, "response": response},
    }


def _error_result(source: str, error: Exception) -> dict[str, Any]:
    return {
        "source": source,
        "error": str(error),
        "elapsed_ms": None,
        "rows": [],
        "row_count": 0,
        "request_id": None,
        "server_timing": None,
        "technical": getattr(error, "technical", None),
    }


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/config")
def config(request: Request) -> dict[str, Any]:
    return {
        "mock": USE_MOCK_DATA,
        "warehouse_configured": bool(WAREHOUSE_ID),
        "data_api_configured": bool(DATA_API_URL)
        and "REPLACE" not in DATA_API_URL.upper()
        or all(
            os.getenv(name)
            for name in ("PGHOST", "PGDATABASE", "DATABRICKS_WORKSPACE_ID")
        ),
        "lakehouse_table": f"{CATALOG}.{SCHEMA}.{TABLE}",
        "lakebase_table": f"{LAKEBASE_SCHEMA}.{LAKEBASE_TABLE}",
        "identity": request.headers.get(
            "x-forwarded-email",
            request.headers.get("x-forwarded-user", "service principal de la app"),
        ),
    }


@app.get("/api/query/data-api")
async def query_data_api(
    planta: str = Query("Sabaneta"),
    limit: int = Query(25, ge=1, le=100),
) -> dict[str, Any]:
    if planta not in ALLOWED_PLANTS:
        return _error_result("Lakebase Data API", ValueError("Planta inválida."))
    if USE_MOCK_DATA:
        return _mock_result("Lakebase Data API", planta, limit)
    try:
        return await _run_data_api(planta, limit)
    except Exception as error:
        return _error_result("Lakebase Data API", error)


@app.get("/api/query/statement-api")
async def query_statement_api(
    planta: str = Query("Sabaneta"),
    limit: int = Query(25, ge=1, le=100),
) -> dict[str, Any]:
    if planta not in ALLOWED_PLANTS:
        return _error_result(
            "Statement Execution API", ValueError("Planta inválida.")
        )
    if USE_MOCK_DATA:
        return _mock_result("Statement Execution API", planta, limit)
    try:
        return await asyncio.to_thread(_run_statement_api, planta, limit)
    except Exception as error:
        return _error_result("Statement Execution API", error)


@app.get("/api/compare")
async def compare(
    planta: str = Query("Sabaneta"),
    limit: int = Query(25, ge=1, le=100),
) -> dict[str, Any]:
    if planta not in ALLOWED_PLANTS:
        return {"error": "Planta inválida."}

    if USE_MOCK_DATA:
        data_api = _mock_result("Lakebase Data API", planta, limit)
        statement_api = _mock_result("Statement Execution API", planta, limit)
    else:
        data_outcome, statement_outcome = await asyncio.gather(
            _run_data_api(planta, limit),
            asyncio.to_thread(_run_statement_api, planta, limit),
            return_exceptions=True,
        )
        data_api = (
            _error_result("Lakebase Data API", data_outcome)
            if isinstance(data_outcome, Exception)
            else data_outcome
        )
        statement_api = (
            _error_result("Statement Execution API", statement_outcome)
            if isinstance(statement_outcome, Exception)
            else statement_outcome
        )

    data_ids = {str(row.get("lote_id")) for row in data_api.get("rows", [])}
    statement_ids = {
        str(row.get("lote_id")) for row in statement_api.get("rows", [])
    }
    return {
        "comparison_id": str(uuid.uuid4()),
        "measured_at": datetime.now(timezone.utc).isoformat(),
        "filters": {"planta": planta, "limit": limit},
        "data_api": data_api,
        "statement_api": statement_api,
        "parity": {
            "same_lote_ids": data_ids == statement_ids and bool(data_ids),
            "data_api_only": sorted(data_ids - statement_ids),
            "statement_api_only": sorted(statement_ids - data_ids),
        },
        "notice": (
            "Esta medición compara latencia percibida por la app. No es un benchmark "
            "de motores: cada API sirve una copia, compute y patrón de consulta distinto."
        ),
    }


app.mount(
    "/",
    StaticFiles(directory=ROOT / "frontend", html=True),
    name="frontend",
)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.environ.get("DATABRICKS_APP_PORT", "8000")),
    )
