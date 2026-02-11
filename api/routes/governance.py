from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import json

from fastapi import APIRouter, Depends, HTTPException

from api.db import get_connection
from api.models import FeedbackRequest
from api.routes.auth import enforce_roles, require_role
from governance.metric_store import framework_adoption_proxy

ROOT = Path(__file__).resolve().parents[1]

router = APIRouter(prefix="/governance", tags=["governance"])


@router.get("/scorecard")
def governance_scorecard(role: str = Depends(require_role)) -> dict:
    enforce_roles(role, {"exec", "ops", "finance"})

    with get_connection() as conn:
        checks = [dict(r) for r in conn.execute("SELECT check_name, table_name, status, score, details FROM data_quality_results").fetchall()]
        latest_dq = conn.execute(
            """
            SELECT date, value
            FROM fact_kpi_daily
            WHERE kpi_name = 'data_quality_score'
            ORDER BY date DESC
            LIMIT 1
            """
        ).fetchone()

        api_usage_rows = conn.execute(
            """
            SELECT endpoint, COUNT(*) AS request_count
            FROM api_usage_log
            GROUP BY endpoint
            ORDER BY request_count DESC
            """
        ).fetchall()

        avg_feedback_row = conn.execute("SELECT AVG(rating) AS avg_rating, COUNT(*) AS n FROM feedback").fetchone()

        framework_adoption = framework_adoption_proxy(conn)

    markdown_path = ROOT / "governance" / "scorecard.md"
    markdown = markdown_path.read_text(encoding="utf-8") if markdown_path.exists() else "Scorecard not generated yet."

    metrics = {
        "framework_adoption_proxy_pct": framework_adoption,
        "latest_data_quality_score": float(latest_dq["value"]) if latest_dq else None,
        "bi_utilisation_proxy": {
            "total_requests": int(sum(row["request_count"] for row in api_usage_rows)),
            "by_endpoint": [dict(row) for row in api_usage_rows],
        },
        "stakeholder_satisfaction_proxy": {
            "average_rating": float(avg_feedback_row["avg_rating"] or 0.0),
            "feedback_count": int(avg_feedback_row["n"] or 0),
        },
    }

    return {
        "role": role,
        "markdown": markdown,
        "checks": checks,
        "metrics": metrics,
    }


@router.get("/contracts")
def governance_contracts(role: str = Depends(require_role)) -> dict:
    enforce_roles(role, {"exec", "ops", "finance"})

    path = ROOT / "governance" / "data_contracts.md"
    markdown = path.read_text(encoding="utf-8") if path.exists() else "No data contracts document found."

    with get_connection() as conn:
        logs = [
            dict(row)
            for row in conn.execute(
                """
                SELECT run_timestamp, table_name, status, details
                FROM contract_validation_log
                ORDER BY id DESC
                LIMIT 100
                """
            ).fetchall()
        ]

    return {"role": role, "markdown": markdown, "validation_log": logs}


feedback_router = APIRouter(tags=["feedback"])


@feedback_router.post("/feedback")
def submit_feedback(payload: FeedbackRequest, role: str = Depends(require_role)) -> dict:
    enforce_roles(role, {"exec", "ops", "finance"})

    with get_connection() as conn:
        exists = conn.execute("SELECT id FROM explain_audit_log WHERE id = ?", (payload.audit_id,)).fetchone()
        if not exists:
            raise HTTPException(status_code=404, detail=f"Audit id {payload.audit_id} not found")

        now = datetime.now(timezone.utc).isoformat()
        cursor = conn.execute(
            """
            INSERT INTO feedback (audit_id, rating, notes, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (payload.audit_id, payload.rating, payload.notes, now),
        )

        existing_feedback = conn.execute(
            "SELECT user_feedback FROM explain_audit_log WHERE id = ?",
            (payload.audit_id,),
        ).fetchone()["user_feedback"]

        feedback_blob = []
        if existing_feedback:
            try:
                feedback_blob = json.loads(existing_feedback)
            except json.JSONDecodeError:
                feedback_blob = []

        feedback_blob.append({"rating": payload.rating, "notes": payload.notes, "timestamp": now})

        conn.execute(
            "UPDATE explain_audit_log SET user_feedback = ? WHERE id = ?",
            (json.dumps(feedback_blob), payload.audit_id),
        )
        conn.commit()

    return {
        "id": cursor.lastrowid,
        "audit_id": payload.audit_id,
        "rating": payload.rating,
        "notes": payload.notes,
    }
