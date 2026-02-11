from __future__ import annotations

import sqlite3
from typing import Any


def fetch_kpi_definitions(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT kpi_name, description, formula_text, grain, owner_role, threshold_good,
               threshold_bad, refresh_cadence, business_value, leading_indicator_bool
        FROM kpi_definitions
        ORDER BY kpi_name
        """
    ).fetchall()

    return [dict(row) for row in rows]


def framework_adoption_proxy(conn: sqlite3.Connection) -> float:
    total = conn.execute("SELECT COUNT(*) AS c FROM kpi_definitions").fetchone()["c"]
    if total == 0:
        return 0.0

    populated = conn.execute(
        """
        SELECT COUNT(*) AS c
        FROM kpi_definitions
        WHERE owner_role IS NOT NULL
          AND TRIM(owner_role) <> ''
          AND threshold_good IS NOT NULL
          AND threshold_bad IS NOT NULL
          AND refresh_cadence IS NOT NULL
          AND TRIM(refresh_cadence) <> ''
        """
    ).fetchone()["c"]

    return round((populated / total) * 100, 2)


def metric_by_role(definitions: list[dict[str, Any]]) -> dict[str, list[str]]:
    mapping: dict[str, list[str]] = {"exec": [], "ops": [], "finance": []}

    finance_kpis = {
        "cost_leakage_estimate_gbp",
        "automation_impact_hours_weekly",
        "automation_impact_gbp_weekly",
        "automation_impact_gbp_cumulative",
        "data_quality_score",
        "framework_adoption_proxy_pct",
        "bi_utilisation_proxy_requests",
        "stakeholder_satisfaction_proxy_rating",
    }
    ops_kpis = {
        "on_time_delivery_pct",
        "sla_breach_rate_pct",
        "exception_rate_per_100_jobs",
        "manual_workload_hours_weekly",
        "data_quality_score",
        "framework_adoption_proxy_pct",
        "bi_utilisation_proxy_requests",
        "stakeholder_satisfaction_proxy_rating",
    }

    for item in definitions:
        kpi_name = item["kpi_name"]
        mapping["exec"].append(kpi_name)
        if kpi_name in ops_kpis:
            mapping["ops"].append(kpi_name)
        if kpi_name in finance_kpis:
            mapping["finance"].append(kpi_name)

    return mapping
