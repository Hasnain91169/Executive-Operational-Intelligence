from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import json
import os
import sqlite3
from typing import Any
from uuid import uuid4

import pandas as pd

from ai.drivers import compute_top_drivers


def _get_kpi_snapshot(conn: sqlite3.Connection, kpi_name: str, target_date: str) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT value
        FROM fact_kpi_daily
        WHERE kpi_name = ? AND date = ?
        """,
        (kpi_name, target_date),
    ).fetchone()
    if not row:
        raise ValueError(f"No KPI value found for {kpi_name} on {target_date}")

    target = date.fromisoformat(target_date)
    baseline_start = (target - timedelta(days=14)).isoformat()
    baseline_end = (target - timedelta(days=1)).isoformat()

    baseline_row = conn.execute(
        """
        SELECT
            AVG(value) AS baseline_mean,
            MIN(value) AS baseline_min,
            MAX(value) AS baseline_max,
            COUNT(*) AS observations
        FROM fact_kpi_daily
        WHERE kpi_name = ?
          AND date BETWEEN ? AND ?
        """,
        (kpi_name, baseline_start, baseline_end),
    ).fetchone()

    anomaly_row = conn.execute(
        """
        SELECT score, baseline, status, scenario_tag
        FROM anomalies
        WHERE kpi_name = ? AND date = ?
        ORDER BY score DESC
        LIMIT 1
        """,
        (kpi_name, target_date),
    ).fetchone()
    if not anomaly_row:
        # Ensure explain output includes anomaly metadata even if anomaly run has not been called yet.
        from ai.anomaly import recompute_anomalies

        recompute_anomalies(conn, kpi_names=[kpi_name])
        anomaly_row = conn.execute(
            """
            SELECT score, baseline, status, scenario_tag
            FROM anomalies
            WHERE kpi_name = ? AND date = ?
            ORDER BY score DESC
            LIMIT 1
            """,
            (kpi_name, target_date),
        ).fetchone()

    return {
        "kpi_name": kpi_name,
        "date": target_date,
        "value": float(row["value"]),
        "baseline_mean": float(baseline_row["baseline_mean"] or 0.0),
        "baseline_min": float(baseline_row["baseline_min"] or 0.0),
        "baseline_max": float(baseline_row["baseline_max"] or 0.0),
        "baseline_observations": int(baseline_row["observations"] or 0),
        "anomaly_score": float(anomaly_row["score"]) if anomaly_row else None,
        "anomaly_baseline": float(anomaly_row["baseline"]) if anomaly_row else None,
        "anomaly_status": anomaly_row["status"] if anomaly_row else None,
        "scenario_tag": anomaly_row["scenario_tag"] if anomaly_row else None,
    }


def _expected_impact_from_driver(driver: dict[str, Any]) -> dict[str, float]:
    magnitude = abs(float(driver.get("delta_abs", 0.0)))
    contribution = abs(float(driver.get("contribution_share", 0.0)))

    pct_reduction = max(5.0, min(45.0, (magnitude * 18.0) + (contribution * 28.0)))
    est_hours = round(max(2.0, pct_reduction * 0.25), 2)
    est_gbp = round(est_hours * 42.0, 2)
    return {
        "expected_kpi_improvement_pct": round(pct_reduction, 2),
        "expected_hours_saved": est_hours,
        "expected_gbp_saved": est_gbp,
    }


def _recommended_action_for_driver(kpi_name: str, driver: dict[str, Any]) -> dict[str, Any]:
    dimension = driver.get("dimension", "unknown")
    segment = driver.get("segment", "unknown")

    if dimension == "carrier":
        action = f"Launch carrier performance escalation for {segment}; enforce ETA update cadence and temporary fallback allocation."
    elif dimension == "product_family":
        action = f"Run targeted planning huddle for {segment}; increase buffer stock and supplier confirmation controls."
    elif dimension == "incident_type":
        action = f"Create mitigation playbook for incident type '{segment}' with owner and same-day triage SLA."
    elif dimension == "category":
        action = f"Apply auto-routing and templated response workflow for communication category '{segment}'."
    elif dimension == "customer_tier":
        action = f"Prioritize proactive comms for {segment} accounts with threshold-based alerting and escalation path."
    elif dimension == "site":
        action = f"Open site-level corrective action at {segment}; assign data/process owner and monitor daily recovery KPI."
    else:
        action = f"Investigate and mitigate driver in {dimension}={segment} with owner accountability and daily checkpoint."

    impact = _expected_impact_from_driver(driver)
    return {
        "driver": {"dimension": dimension, "segment": segment},
        "action": action,
        "expected_impact": impact,
        "kpi_name": kpi_name,
    }


def _maybe_rephrase_with_openai(payload: dict[str, Any]) -> str | None:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return None

    try:
        from openai import OpenAI

        client = OpenAI(api_key=api_key)
        prompt = (
            "Rephrase this KPI anomaly explanation for an operations executive. "
            "Keep all numbers and entities unchanged. "
            f"Payload: {json.dumps(payload, default=str)}"
        )
        response = client.responses.create(
            model="gpt-4.1-mini",
            input=prompt,
            max_output_tokens=220,
        )
        text = getattr(response, "output_text", None)
        return text.strip() if text else None
    except Exception:
        return None


def explain_kpi(
    conn: sqlite3.Connection,
    kpi_name: str,
    target_date: str,
    request_id: str | None = None,
) -> dict[str, Any]:
    request_id = request_id or str(uuid4())
    snapshot = _get_kpi_snapshot(conn, kpi_name, target_date)

    driver_result = compute_top_drivers(conn, kpi_name=kpi_name, target_date=target_date, baseline_days=14, top_n=3)
    drivers = driver_result.get("drivers", [])
    evidence = driver_result.get("evidence", [])
    attribution_note = driver_result.get("attribution_note")

    recommended_actions = [_recommended_action_for_driver(kpi_name, driver) for driver in drivers]

    summary = {
        "kpi_name": kpi_name,
        "date": target_date,
        "value": snapshot["value"],
        "baseline_mean": snapshot["baseline_mean"],
        "anomaly_score": snapshot["anomaly_score"],
        "scenario_tag": snapshot["scenario_tag"],
        "delta_vs_baseline": round(snapshot["value"] - snapshot["baseline_mean"], 4),
    }

    output = {
        "request_id": request_id,
        "summary": summary,
        "drivers": drivers,
        "recommended_actions": recommended_actions,
        "evidence": evidence,
    }
    if attribution_note:
        output["attribution_note"] = attribution_note

    rephrased = _maybe_rephrase_with_openai(output)
    if rephrased:
        output["narrative_rephrased"] = rephrased

    audit_sql_used = json.dumps(evidence, default=str)
    slices_returned = json.dumps(drivers, default=str)

    ts = datetime.now(timezone.utc).isoformat()
    cursor = conn.execute(
        """
        INSERT INTO explain_audit_log (timestamp, kpi_name, date, sql_used, slices_returned, user_feedback, request_id)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (ts, kpi_name, target_date, audit_sql_used, slices_returned, None, request_id),
    )
    conn.commit()

    output["audit_id"] = cursor.lastrowid
    return output


def route_intent(question: str) -> str:
    q = question.lower()
    if "why" in q:
        return "why_anomaly"
    if "top driver" in q or "driver" in q:
        return "top_drivers"
    if "worst" in q:
        return "worst_segments"
    if "trend" in q:
        return "trend"
    if "what changed" in q or "changed" in q:
        return "what_changed"
    return "top_drivers"


def _latest_date_for_kpi(conn: sqlite3.Connection, kpi_name: str) -> str:
    row = conn.execute(
        "SELECT MAX(date) AS d FROM fact_kpi_daily WHERE kpi_name = ?",
        (kpi_name,),
    ).fetchone()
    if not row or not row["d"]:
        raise ValueError(f"No values found for KPI {kpi_name}")
    return str(row["d"])


def ask_ops_data(
    conn: sqlite3.Connection,
    question: str,
    kpi_name: str | None = None,
    target_date: str | None = None,
) -> dict[str, Any]:
    intent = route_intent(question)
    kpi_name = kpi_name or "sla_breach_rate_pct"
    target_date = target_date or _latest_date_for_kpi(conn, kpi_name)

    if intent == "why_anomaly":
        return {"intent": intent, "response": explain_kpi(conn, kpi_name, target_date)}

    if intent == "top_drivers":
        return {
            "intent": intent,
            "kpi_name": kpi_name,
            "date": target_date,
            "result": compute_top_drivers(conn, kpi_name=kpi_name, target_date=target_date, baseline_days=14, top_n=5),
        }

    if intent == "worst_segments":
        if kpi_name == "sla_breach_rate_pct":
            sql = """
                SELECT
                    dd.date,
                    ds.site_name,
                    dc.tier AS customer_tier,
                    100.0 * SUM(CASE WHEN fc.sla_sensitive_bool = 1 AND fc.breached_bool = 1 THEN 1 ELSE 0 END)
                        / NULLIF(SUM(CASE WHEN fc.sla_sensitive_bool = 1 THEN 1 ELSE 0 END), 0) AS breach_rate_pct,
                    COUNT(*) AS comm_count
                FROM fact_comms fc
                JOIN dim_date dd ON dd.date_key = fc.date_key
                JOIN dim_site ds ON ds.site_id = fc.site_id
                JOIN dim_customer dc ON dc.customer_id = fc.customer_id
                WHERE dd.date = ?
                GROUP BY dd.date, ds.site_name, dc.tier
                HAVING comm_count > 5
                ORDER BY breach_rate_pct DESC
                LIMIT 10
            """
            rows = [dict(r) for r in conn.execute(sql, (target_date,)).fetchall()]
        else:
            sql = """
                SELECT
                    dd.date,
                    ds.site_name,
                    dp.product_family,
                    100.0 * COUNT(DISTINCT fi.incident_id) / NULLIF(COUNT(DISTINCT fj.job_id), 0) AS exception_rate_pct,
                    COUNT(DISTINCT fj.job_id) AS jobs
                FROM fact_jobs fj
                JOIN dim_date dd ON dd.date_key = fj.date_key
                JOIN dim_site ds ON ds.site_id = fj.site_id
                JOIN dim_product dp ON dp.product_id = fj.product_id
                LEFT JOIN fact_incidents fi ON fi.job_id = fj.job_id
                WHERE dd.date = ?
                GROUP BY dd.date, ds.site_name, dp.product_family
                ORDER BY exception_rate_pct DESC
                LIMIT 10
            """
            rows = [dict(r) for r in conn.execute(sql, (target_date,)).fetchall()]

        return {"intent": intent, "kpi_name": kpi_name, "date": target_date, "rows": rows}

    if intent == "trend":
        start = (date.fromisoformat(target_date) - timedelta(days=29)).isoformat()
        sql = """
            SELECT date, value
            FROM fact_kpi_daily
            WHERE kpi_name = ?
              AND date BETWEEN ? AND ?
            ORDER BY date
        """
        rows = [dict(r) for r in conn.execute(sql, (kpi_name, start, target_date)).fetchall()]
        return {"intent": intent, "kpi_name": kpi_name, "from": start, "to": target_date, "timeseries": rows}

    if intent == "what_changed":
        t = date.fromisoformat(target_date)
        baseline_start = (t - timedelta(days=14)).isoformat()
        baseline_end = (t - timedelta(days=1)).isoformat()

        row = conn.execute(
            """
            SELECT
                (SELECT value FROM fact_kpi_daily WHERE kpi_name = ? AND date = ?) AS current_value,
                (SELECT AVG(value) FROM fact_kpi_daily WHERE kpi_name = ? AND date BETWEEN ? AND ?) AS baseline
            """,
            (kpi_name, target_date, kpi_name, baseline_start, baseline_end),
        ).fetchone()

        current_value = float(row["current_value"] or 0.0)
        baseline = float(row["baseline"] or 0.0)
        delta = current_value - baseline
        delta_pct = (delta / baseline * 100.0) if baseline else None

        return {
            "intent": intent,
            "kpi_name": kpi_name,
            "date": target_date,
            "current_value": current_value,
            "baseline": baseline,
            "delta": round(delta, 4),
            "delta_pct": round(delta_pct, 4) if delta_pct is not None else None,
        }

    return {"intent": intent, "message": "No matching intent handler."}
