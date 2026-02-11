from __future__ import annotations

from datetime import date
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Request

from ai.explain import ask_ops_data, explain_kpi
from api.db import get_connection
from api.models import AskRequest, ExplainRequest
from api.routes.auth import kpi_allowed_for_role, require_role

router = APIRouter(tags=["explain"])


@router.post("/explain")
def explain_endpoint(payload: ExplainRequest, request: Request, role: str = Depends(require_role)) -> dict:
    if not kpi_allowed_for_role(role, payload.kpi_name):
        raise HTTPException(status_code=403, detail="Role does not have access to this KPI")
    try:
        date.fromisoformat(payload.date)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="date must be YYYY-MM-DD") from exc

    request_id = getattr(request.state, "request_id", str(uuid4()))
    with get_connection() as conn:
        try:
            result = explain_kpi(conn, kpi_name=payload.kpi_name, target_date=payload.date, request_id=request_id)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    result["role"] = role
    return result


@router.post("/explain/ask")
def ask_ops(payload: AskRequest, role: str = Depends(require_role)) -> dict:
    if payload.kpi_name and not kpi_allowed_for_role(role, payload.kpi_name):
        raise HTTPException(status_code=403, detail="Role does not have access to this KPI")

    with get_connection() as conn:
        response = ask_ops_data(conn, question=payload.question, kpi_name=payload.kpi_name, target_date=payload.date)

    return {"role": role, **response}
