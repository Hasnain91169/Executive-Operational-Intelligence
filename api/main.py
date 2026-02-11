from __future__ import annotations

from pathlib import Path
from uuid import uuid4

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from api.db import record_api_usage, resolve_db_path
from api.routes import anomalies, auth, automations, explain, governance, kpis
from api.routes.auth import role_from_api_key

app = FastAPI(title="Operational Intelligence Copilot API", version="0.1.0")

app.include_router(auth.router)
app.include_router(kpis.router)
app.include_router(anomalies.router)
app.include_router(explain.router)
app.include_router(governance.router)
app.include_router(governance.feedback_router)
app.include_router(automations.router)


@app.middleware("http")
async def api_usage_middleware(request: Request, call_next):
    request_id = str(uuid4())
    request.state.request_id = request_id

    role = role_from_api_key(request.headers.get("x-api-key"))
    request.state.role = role

    try:
        response = await call_next(request)
    except Exception:
        response = JSONResponse(
            status_code=500,
            content={"detail": "Internal server error", "request_id": request_id},
        )

    response.headers["x-request-id"] = request_id

    if not request.url.path.startswith("/health"):
        record_api_usage(
            endpoint=request.url.path,
            method=request.method,
            role=role,
            status_code=response.status_code,
            request_id=request_id,
        )

    return response


@app.get("/health")
def health() -> dict:
    db_path = resolve_db_path()
    return {
        "status": "ok",
        "db_path": str(db_path),
        "db_exists": Path(db_path).exists(),
    }
