from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import sqlite3

import pandas as pd

from etl.validate_contracts import log_results, validate_clean_contracts
from governance.data_quality import evaluate_data_quality, render_scorecard_markdown, serialize_quality_json
from governance.metric_store import framework_adoption_proxy

ROOT = Path(__file__).resolve().parents[1]
CLEAN_DIR = ROOT / "data" / "clean"
MART_DIR = ROOT / "data" / "mart"
DB_PATH = MART_DIR / "ops_copilot.db"

SCHEMA_SQL = ROOT / "sql" / "schema.sql"
VIEWS_SQL = ROOT / "sql" / "views.sql"
KPI_DEFINITIONS_SQL = ROOT / "sql" / "kpi_definitions.sql"
RBAC_SQL = ROOT / "sql" / "rbac_views.sql"

BLENDED_RATE_GBP_PER_HOUR = 42.0


def execute_sql_file(conn: sqlite3.Connection, path: Path) -> None:
    conn.executescript(path.read_text(encoding="utf-8"))


def load_table(conn: sqlite3.Connection, table_name: str, csv_path: Path) -> None:
    frame = pd.read_csv(csv_path)
    frame.to_sql(table_name, conn, if_exists="append", index=False)


def load_clean_data(conn: sqlite3.Connection) -> None:
    dimensions = ["dim_date", "dim_site", "dim_customer", "dim_team", "dim_category", "dim_carrier", "dim_product"]
    facts = ["fact_jobs", "fact_incidents", "fact_comms", "fact_costs", "fact_automation_events"]

    for table in dimensions + facts + ["scenario_registry"]:
        load_table(conn, table, CLEAN_DIR / f"{table}.csv")


def _status_for_kpi(kpi_name: str, value: float, good: float | None, bad: float | None) -> str:
    if good is None or bad is None:
        return "no_threshold"

    lower_is_better = {
        "sla_breach_rate_pct",
        "exception_rate_per_100_jobs",
        "manual_workload_hours_weekly",
        "cost_leakage_estimate_gbp",
    }

    if kpi_name in lower_is_better:
        if value <= good:
            return "good"
        if value <= bad:
            return "watch"
        return "bad"

    if value >= good:
        return "good"
    if value >= bad:
        return "watch"
    return "bad"


def compute_kpis(conn: sqlite3.Connection, quality_result: dict) -> None:
    definitions = pd.read_sql_query("SELECT * FROM kpi_definitions", conn)
    definition_map = {row["kpi_name"]: row for _, row in definitions.iterrows()}

    jobs = pd.read_sql_query(
        """
        SELECT dd.date, fj.*
        FROM fact_jobs fj
        JOIN dim_date dd ON dd.date_key = fj.date_key
        """,
        conn,
    )
    comms = pd.read_sql_query(
        """
        SELECT dd.date, fc.*
        FROM fact_comms fc
        JOIN dim_date dd ON dd.date_key = fc.date_key
        """,
        conn,
    )
    incidents = pd.read_sql_query(
        """
        SELECT dd.date, fi.*
        FROM fact_incidents fi
        JOIN dim_date dd ON dd.date_key = fi.date_key
        """,
        conn,
    )
    automation = pd.read_sql_query(
        """
        SELECT dd.date, fa.*
        FROM fact_automation_events fa
        JOIN dim_date dd ON dd.date_key = fa.date_key
        """,
        conn,
    )
    date_keys = pd.read_sql_query(
        """
        SELECT DISTINCT dd.date, dd.date_key
        FROM dim_date dd
        JOIN fact_jobs fj ON fj.date_key = dd.date_key
        ORDER BY dd.date
        """,
        conn,
    )
    api_usage = pd.read_sql_query("SELECT DATE(requested_at) AS date, COUNT(*) AS request_count FROM api_usage_log GROUP BY DATE(requested_at)", conn)
    feedback = pd.read_sql_query("SELECT DATE(created_at) AS date, AVG(rating) AS avg_rating FROM feedback GROUP BY DATE(created_at)", conn)

    quality_daily = quality_result.get("daily_scores", {})
    framework_adoption = framework_adoption_proxy(conn)

    api_usage_map = {row["date"]: float(row["request_count"]) for _, row in api_usage.iterrows()}
    feedback_map = {row["date"]: float(row["avg_rating"]) for _, row in feedback.iterrows()}

    jobs["date"] = pd.to_datetime(jobs["date"]).dt.date
    comms["date"] = pd.to_datetime(comms["date"]).dt.date
    incidents["date"] = pd.to_datetime(incidents["date"]).dt.date
    automation["date"] = pd.to_datetime(automation["date"]).dt.date
    date_keys["date"] = pd.to_datetime(date_keys["date"]).dt.date

    rows_to_insert: list[tuple] = []
    computed_at = datetime.now(timezone.utc).isoformat()

    for _, date_row in date_keys.iterrows():
        current_date = date_row["date"]
        current_date_str = current_date.isoformat()
        current_key = int(date_row["date_key"])

        jobs_day = jobs[jobs["date"] == current_date]
        comms_day = comms[comms["date"] == current_date]
        incidents_day = incidents[incidents["date"] == current_date]

        delivered = jobs_day[jobs_day["delivered_date_key"].notna()]
        if delivered.empty:
            on_time = 100.0
        else:
            on_time = float((delivered["delivered_date_key"] <= delivered["promised_date_key"]).mean() * 100)

        sla_sensitive = comms_day[comms_day["sla_sensitive_bool"] == 1]
        sla_breach = 0.0 if sla_sensitive.empty else float((sla_sensitive["breached_bool"].sum() / len(sla_sensitive)) * 100)

        exception_rate = float((len(incidents_day) / max(1, len(jobs_day))) * 100)

        week_start = current_date - timedelta(days=6)
        comms_week = comms[(comms["date"] >= week_start) & (comms["date"] <= current_date)]
        incidents_week = incidents[(incidents["date"] >= week_start) & (incidents["date"] <= current_date)]
        automation_week = automation[(automation["date"] >= week_start) & (automation["date"] <= current_date)]

        manual_hours_weekly = float(comms_week["minutes_spent"].sum() / 60.0)
        cost_leakage = float(((comms_day["minutes_spent"].sum() + incidents_day["minutes_lost"].sum()) / 60.0) * BLENDED_RATE_GBP_PER_HOUR)

        dq_score = float(quality_daily.get(current_date_str, quality_result.get("overall_score", 0.0)))

        automation_hours_weekly = float(automation_week["hours_saved"].sum())
        automation_gbp_weekly = float(automation_week["gbp_saved"].sum())
        automation_gbp_cumulative = float(automation[automation["date"] <= current_date]["gbp_saved"].sum())

        bi_util = float(api_usage_map.get(current_date_str, 0.0))
        stakeholder_rating = float(feedback_map.get(current_date_str, 0.0))

        kpi_values = {
            "on_time_delivery_pct": on_time,
            "sla_breach_rate_pct": sla_breach,
            "exception_rate_per_100_jobs": exception_rate,
            "manual_workload_hours_weekly": manual_hours_weekly,
            "cost_leakage_estimate_gbp": cost_leakage,
            "data_quality_score": dq_score,
            "automation_impact_hours_weekly": automation_hours_weekly,
            "automation_impact_gbp_weekly": automation_gbp_weekly,
            "automation_impact_gbp_cumulative": automation_gbp_cumulative,
            "framework_adoption_proxy_pct": framework_adoption,
            "bi_utilisation_proxy_requests": bi_util,
            "stakeholder_satisfaction_proxy_rating": stakeholder_rating,
        }

        for kpi_name, value in kpi_values.items():
            if kpi_name not in definition_map:
                continue
            definition = definition_map[kpi_name]
            target_good = float(definition["threshold_good"]) if pd.notna(definition["threshold_good"]) else None
            target_bad = float(definition["threshold_bad"]) if pd.notna(definition["threshold_bad"]) else None
            owner_role = definition["owner_role"] if pd.notna(definition["owner_role"]) else None
            status = _status_for_kpi(kpi_name, float(value), target_good, target_bad)

            rows_to_insert.append(
                (
                    current_date_str,
                    current_key,
                    kpi_name,
                    round(float(value), 4),
                    target_good,
                    target_bad,
                    owner_role,
                    status,
                    computed_at,
                )
            )

    conn.execute("DELETE FROM fact_kpi_daily")
    conn.executemany(
        """
        INSERT INTO fact_kpi_daily (
            date, date_key, kpi_name, value, target_good, target_bad, owner_role, status, computed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows_to_insert,
    )
    conn.commit()


def export_mart_csvs(conn: sqlite3.Connection) -> None:
    MART_DIR.mkdir(parents=True, exist_ok=True)

    export_tables = [
        "dim_date",
        "dim_site",
        "dim_customer",
        "dim_team",
        "dim_category",
        "dim_carrier",
        "dim_product",
        "fact_jobs",
        "fact_incidents",
        "fact_comms",
        "fact_costs",
        "fact_automation_events",
        "kpi_definitions",
        "fact_kpi_daily",
        "anomalies",
        "api_usage_log",
        "explain_audit_log",
        "automation_runs",
        "feedback",
        "scenario_registry",
        "data_quality_results",
    ]

    for table in export_tables:
        frame = pd.read_sql_query(f"SELECT * FROM {table}", conn)
        frame.to_csv(MART_DIR / f"{table}.csv", index=False)


def main() -> None:
    MART_DIR.mkdir(parents=True, exist_ok=True)
    if DB_PATH.exists():
        DB_PATH.unlink()

    validation_results = validate_clean_contracts()
    failures = [r for r in validation_results if r.status != "passed"]
    if failures:
        for item in validation_results:
            print(f"[{item.status.upper()}] {item.table_name}: {item.details}")
        raise SystemExit("Contract validation failed. Resolve clean layer issues before loading mart.")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    execute_sql_file(conn, SCHEMA_SQL)
    log_results(conn, validation_results)

    load_clean_data(conn)

    execute_sql_file(conn, KPI_DEFINITIONS_SQL)
    execute_sql_file(conn, VIEWS_SQL)
    execute_sql_file(conn, RBAC_SQL)

    quality_result = evaluate_data_quality(conn)
    scorecard_md = render_scorecard_markdown(quality_result)
    (ROOT / "governance" / "scorecard.md").write_text(scorecard_md, encoding="utf-8")
    (MART_DIR / "data_quality_scorecard.json").write_text(serialize_quality_json(quality_result), encoding="utf-8")

    compute_kpis(conn, quality_result)

    export_mart_csvs(conn)

    conn.close()
    print(f"Mart loaded to {DB_PATH}")


if __name__ == "__main__":
    main()
