from __future__ import annotations

from datetime import datetime, timezone
import json
import sqlite3
from typing import Any

import requests
from fastapi import APIRouter, Depends, HTTPException

from ai.drivers import compute_top_drivers
from api.db import get_connection
from api.models import AutomationRegisterRequest, AutomationTestRequest
from api.routes.auth import enforce_roles, require_role

router = APIRouter(prefix="/automations", tags=["automations"])


def _parse_condition(condition_json: str) -> dict[str, Any]:
    try:
        return json.loads(condition_json)
    except json.JSONDecodeError:
        return {}


def _threshold_match(value: float, rule: Any) -> bool:
    if isinstance(rule, dict):
        operator = rule.get("operator", ">")
        threshold = float(rule.get("value", 0.0))
    else:
        operator = ">"
        threshold = float(rule)

    if operator == ">":
        return value > threshold
    if operator == ">=":
        return value >= threshold
    if operator == "<":
        return value < threshold
    if operator == "<=":
        return value <= threshold
    if operator == "==":
        return value == threshold
    return value > threshold


def _segment_filters_match(conn: sqlite3.Connection, anomaly: dict[str, Any], filters: dict[str, Any]) -> bool:
    if not filters:
        return True

    drivers = compute_top_drivers(
        conn,
        kpi_name=anomaly["kpi_name"],
        target_date=anomaly["date"],
        baseline_days=14,
        top_n=15,
    ).get("drivers", [])

    by_dimension: dict[str, set[str]] = {}
    for item in drivers:
        by_dimension.setdefault(str(item.get("dimension", "")).lower(), set()).add(str(item.get("segment", "")).lower())

    alias = {
        "site": "site",
        "category": "category",
        "carrier": "carrier",
        "product": "product_family",
        "product_family": "product_family",
        "customer": "customer_tier",
        "customer_tier": "customer_tier",
    }

    for key, expected in filters.items():
        dim = alias.get(str(key).lower(), str(key).lower())
        expected_str = str(expected).lower()
        if expected_str not in by_dimension.get(dim, set()):
            return False

    return True


def _condition_match(conn: sqlite3.Connection, anomaly: dict[str, Any], condition: dict[str, Any]) -> bool:
    threshold_rule = condition.get("threshold")
    anomaly_score_rule = condition.get("anomaly_score")
    segment_filters = condition.get("segment_filters") or {}

    if threshold_rule is not None and not _threshold_match(float(anomaly["value"]), threshold_rule):
        return False

    if anomaly_score_rule is not None and not _threshold_match(float(anomaly["score"]), anomaly_score_rule):
        return False

    if not _segment_filters_match(conn, anomaly, segment_filters):
        return False

    return True


def trigger_automations_for_anomalies(conn: sqlite3.Connection, anomalies: list[dict[str, Any]]) -> int:
    if not anomalies:
        return 0

    automations = conn.execute(
        """
        SELECT id, name, trigger_kpi, condition_json, webhook_url, enabled
        FROM automations
        WHERE enabled = 1
        """
    ).fetchall()

    triggered = 0

    for anomaly in anomalies:
        for automation in automations:
            if automation["trigger_kpi"] != anomaly["kpi_name"]:
                continue

            condition = _parse_condition(automation["condition_json"])
            if not _condition_match(conn, anomaly, condition):
                continue

            payload = {
                "automation_name": automation["name"],
                "kpi_name": anomaly["kpi_name"],
                "date": anomaly["date"],
                "value": anomaly["value"],
                "baseline": anomaly["baseline"],
                "anomaly_score": anomaly["score"],
                "scenario_tag": anomaly.get("scenario_tag"),
                "condition": condition,
                "triggered_at": datetime.now(timezone.utc).isoformat(),
            }

            status = "failed"
            response_code = None
            response_body = None
            try:
                response = requests.post(automation["webhook_url"], json=payload, timeout=7)
                response_code = response.status_code
                response_body = response.text[:500]
                status = "success" if 200 <= response.status_code < 300 else "failed"
            except requests.RequestException as exc:
                response_body = str(exc)

            conn.execute(
                """
                INSERT INTO automation_runs (
                    automation_id, name, kpi_name, date, payload_json, status,
                    response_code, response_body, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    automation["id"],
                    automation["name"],
                    anomaly["kpi_name"],
                    anomaly["date"],
                    json.dumps(payload),
                    status,
                    response_code,
                    response_body,
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
            conn.commit()
            triggered += 1

    return triggered


@router.post("/register")
def register_automation(payload: AutomationRegisterRequest, role: str = Depends(require_role)) -> dict:
    enforce_roles(role, {"exec", "ops"})

    with get_connection() as conn:
        kpi_exists = conn.execute(
            "SELECT 1 FROM kpi_definitions WHERE kpi_name = ?",
            (payload.trigger_kpi,),
        ).fetchone()
        if not kpi_exists:
            raise HTTPException(status_code=404, detail=f"Unknown KPI: {payload.trigger_kpi}")

        try:
            cursor = conn.execute(
                """
                INSERT INTO automations (name, trigger_kpi, condition_json, webhook_url, enabled, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    payload.name,
                    payload.trigger_kpi,
                    json.dumps(payload.condition_json),
                    str(payload.webhook_url),
                    1 if payload.enabled else 0,
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
            conn.commit()
        except sqlite3.IntegrityError as exc:
            raise HTTPException(status_code=409, detail="Automation name already exists") from exc

    return {"id": cursor.lastrowid, "name": payload.name, "enabled": payload.enabled}


@router.post("/test")
def test_automation(payload: AutomationTestRequest, role: str = Depends(require_role)) -> dict:
    enforce_roles(role, {"exec", "ops", "finance"})

    body = payload.payload or {
        "event": "automation_test",
        "name": payload.name,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    try:
        response = requests.post(str(payload.webhook_url), json=body, timeout=7)
        return {
            "name": payload.name,
            "webhook_url": str(payload.webhook_url),
            "status": "success" if 200 <= response.status_code < 300 else "failed",
            "response_code": response.status_code,
            "response_body": response.text[:500],
        }
    except requests.RequestException as exc:
        return {
            "name": payload.name,
            "webhook_url": str(payload.webhook_url),
            "status": "failed",
            "response_code": None,
            "error": str(exc),
        }


@router.get("")
def list_automations(role: str = Depends(require_role)) -> dict:
    enforce_roles(role, {"exec", "ops", "finance"})
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT id, name, trigger_kpi, condition_json, webhook_url, enabled, created_at
            FROM automations
            ORDER BY created_at DESC
            """
        ).fetchall()

    items = []
    for row in rows:
        item = dict(row)
        item["condition_json"] = _parse_condition(item["condition_json"])
        items.append(item)
    return {"count": len(items), "items": items}


@router.get("/runs")
def list_automation_runs(role: str = Depends(require_role)) -> dict:
    enforce_roles(role, {"exec", "ops", "finance"})

    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT id, automation_id, name, kpi_name, date, payload_json, status,
                   response_code, response_body, created_at
            FROM automation_runs
            ORDER BY created_at DESC
            LIMIT 200
            """
        ).fetchall()

    items = [dict(row) for row in rows]
    for item in items:
        try:
            item["payload_json"] = json.loads(item["payload_json"])
        except Exception:
            pass

    return {"count": len(items), "items": items}
