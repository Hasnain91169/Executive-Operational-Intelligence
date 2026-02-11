from __future__ import annotations

from fastapi import APIRouter, Body, Depends, HTTPException, Query

from ai.anomaly import recompute_anomalies
from api.db import get_connection
from api.models import AnomalyRunRequest
from api.routes.auth import enforce_roles, kpi_allowed_for_role, require_role

router = APIRouter(tags=["anomalies"])


@router.get("/anomalies")
def list_anomalies(
    status: str = Query(default="open"),
    role: str = Depends(require_role),
) -> dict:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT id, kpi_name, date, value, baseline, score, status, scenario_tag, created_at
            FROM anomalies
            WHERE status = ?
            ORDER BY score DESC, date DESC
            """,
            (status,),
        ).fetchall()

    items = [dict(row) for row in rows if kpi_allowed_for_role(role, row["kpi_name"])]
    return {"role": role, "status": status, "count": len(items), "items": items}


@router.post("/anomalies/run")
def run_anomalies(
    payload: AnomalyRunRequest | None = Body(default=None),
    role: str = Depends(require_role),
) -> dict:
    enforce_roles(role, {"exec", "ops", "finance"})
    payload = payload or AnomalyRunRequest()

    with get_connection() as conn:
        anomalies = recompute_anomalies(
            conn,
            threshold=payload.threshold,
            window_days=payload.window_days,
            min_history=max(8, payload.window_days),
        )

        from api.routes.automations import trigger_automations_for_anomalies

        runs = trigger_automations_for_anomalies(conn, anomalies)

    visible = [item for item in anomalies if kpi_allowed_for_role(role, item["kpi_name"])]
    return {
        "role": role,
        "detected_count": len(anomalies),
        "visible_count": len(visible),
        "anomalies": visible,
        "automation_runs_triggered": runs,
    }
