from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
import json
import sqlite3
from typing import Any

import pandas as pd

EXPECTED_SCHEMA: dict[str, set[str]] = {
    "fact_jobs": {
        "job_id",
        "date_key",
        "site_id",
        "customer_id",
        "team_id",
        "carrier_id",
        "product_id",
        "value_gbp",
        "promised_date_key",
        "delivered_date_key",
        "status",
        "priority",
        "duplicate_flag",
        "source_batch",
    },
    "fact_comms": {
        "comm_id",
        "date_key",
        "site_id",
        "customer_id",
        "category_id",
        "channel",
        "minutes_spent",
        "sla_sensitive_bool",
        "response_minutes",
        "breached_bool",
        "job_id",
        "carrier_id",
        "product_id",
    },
    "fact_incidents": {
        "incident_id",
        "date_key",
        "site_id",
        "job_id",
        "incident_type",
        "severity",
        "minutes_lost",
        "product_id",
    },
}


@dataclass
class QualityCheck:
    check_name: str
    table_name: str
    status: str
    score: float
    details: str


LOWER_BETTER_WARN = 70.0
LOWER_BETTER_FAIL = 45.0


def _status_from_score(score: float) -> str:
    if score >= LOWER_BETTER_WARN:
        return "pass"
    if score >= LOWER_BETTER_FAIL:
        return "warn"
    return "fail"


def _schema_drift_score(conn: sqlite3.Connection) -> tuple[float, str]:
    penalties = 0.0
    missing_fragments: list[str] = []

    for table, expected_cols in EXPECTED_SCHEMA.items():
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        present = {row[1] for row in rows}
        missing = sorted(expected_cols - present)
        if missing:
            penalties += min(100.0, len(missing) * 15)
            missing_fragments.append(f"{table}: missing {', '.join(missing)}")

    score = max(0.0, 100.0 - penalties)
    details = "No schema drift detected" if not missing_fragments else "; ".join(missing_fragments)
    return score, details


def _freshness_score(conn: sqlite3.Connection) -> tuple[float, str, int]:
    row = conn.execute("SELECT MAX(date) AS max_date FROM dim_date").fetchone()
    if not row or not row["max_date"]:
        return 0.0, "dim_date empty", 999

    max_date = date.fromisoformat(row["max_date"])
    lag_days = (date.today() - max_date).days
    score = max(0.0, 100.0 - max(0, lag_days - 1) * 10.0)
    details = f"Latest data date {max_date.isoformat()} (lag {lag_days} day(s))"
    return score, details, lag_days


def evaluate_data_quality(conn: sqlite3.Connection) -> dict[str, Any]:
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

    unique_dates = sorted(jobs["date"].dropna().unique().tolist())

    daily_scores: dict[str, float] = {}
    daily_components: list[dict[str, Any]] = []

    completeness_scores = []
    duplicate_scores = []
    null_scores = []
    range_scores = []

    for d in unique_dates:
        jobs_day = jobs[jobs["date"] == d]
        comms_day = comms[comms["date"] == d]
        incidents_day = incidents[incidents["date"] == d]

        delivered = jobs_day[jobs_day["status"] == "Delivered"]
        missing_delivered = int(delivered["delivered_date_key"].isna().sum()) if not delivered.empty else 0
        completeness = 100.0 if delivered.empty else 100.0 * (1.0 - (missing_delivered / len(delivered)))
        completeness_scores.append(completeness)

        duplicate_rate = float(jobs_day["duplicate_flag"].fillna(0).mean() * 100) if not jobs_day.empty else 0.0
        duplicate_penalty = min(60.0, duplicate_rate * 5.0)
        duplicate_score = max(0.0, 100.0 - duplicate_penalty)
        duplicate_scores.append(duplicate_score)

        key_nulls = 0
        key_total = 0
        for frame, cols in [
            (jobs_day, ["job_id", "date_key", "site_id", "customer_id", "team_id", "carrier_id", "product_id"]),
            (comms_day, ["comm_id", "date_key", "site_id", "customer_id", "category_id"]),
            (incidents_day, ["incident_id", "date_key", "site_id", "incident_type"]),
        ]:
            if frame.empty:
                continue
            key_total += len(frame) * len(cols)
            key_nulls += int(frame[cols].isna().sum().sum())
        null_rate = (key_nulls / key_total) if key_total else 0.0
        null_score = max(0.0, 100.0 - min(100.0, null_rate * 1400.0))
        null_scores.append(null_score)

        invalid_count = 0
        count_base = max(1, len(jobs_day) + len(comms_day) + len(incidents_day))
        if not jobs_day.empty:
            invalid_count += int((jobs_day["value_gbp"] <= 0).sum())
        if not comms_day.empty:
            invalid_count += int((comms_day["response_minutes"] < 0).sum())
            invalid_count += int((comms_day["response_minutes"] > 720).sum())
            invalid_count += int((comms_day["minutes_spent"] < 0).sum())
            invalid_count += int((comms_day["breached_bool"] > 1).sum())
        if not incidents_day.empty:
            invalid_count += int((incidents_day["minutes_lost"] < 0).sum())
            invalid_count += int((incidents_day["minutes_lost"] > 720).sum())

        range_rate = invalid_count / count_base
        range_score = max(0.0, 100.0 - min(100.0, range_rate * 800.0))
        range_scores.append(range_score)

        overall = (
            0.35 * completeness
            + 0.25 * duplicate_score
            + 0.2 * null_score
            + 0.2 * range_score
        )
        daily_scores[d] = round(overall, 2)
        daily_components.append(
            {
                "date": d,
                "completeness": round(completeness, 2),
                "duplicate_score": round(duplicate_score, 2),
                "null_score": round(null_score, 2),
                "range_score": round(range_score, 2),
                "overall": round(overall, 2),
                "missing_delivered": missing_delivered,
            }
        )

    freshness_score, freshness_details, freshness_lag = _freshness_score(conn)
    schema_score, schema_details = _schema_drift_score(conn)

    checks = [
        QualityCheck(
            check_name="completeness",
            table_name="fact_jobs",
            status=_status_from_score(float(sum(completeness_scores) / max(1, len(completeness_scores)))),
            score=round(float(sum(completeness_scores) / max(1, len(completeness_scores))), 2),
            details="Delivered jobs with missing delivered_date_key penalized.",
        ),
        QualityCheck(
            check_name="freshness_lag",
            table_name="dim_date",
            status=_status_from_score(freshness_score),
            score=round(freshness_score, 2),
            details=freshness_details,
        ),
        QualityCheck(
            check_name="duplicates",
            table_name="fact_jobs",
            status=_status_from_score(float(sum(duplicate_scores) / max(1, len(duplicate_scores)))),
            score=round(float(sum(duplicate_scores) / max(1, len(duplicate_scores))), 2),
            details="Duplicate penalty derived from duplicate_flag prevalence.",
        ),
        QualityCheck(
            check_name="null_key_fields",
            table_name="fact_jobs|fact_comms|fact_incidents",
            status=_status_from_score(float(sum(null_scores) / max(1, len(null_scores)))),
            score=round(float(sum(null_scores) / max(1, len(null_scores))), 2),
            details="Nulls in critical key fields across operational facts.",
        ),
        QualityCheck(
            check_name="schema_drift",
            table_name="warehouse",
            status=_status_from_score(schema_score),
            score=round(schema_score, 2),
            details=schema_details,
        ),
        QualityCheck(
            check_name="out_of_range_values",
            table_name="fact_jobs|fact_comms|fact_incidents",
            status=_status_from_score(float(sum(range_scores) / max(1, len(range_scores)))),
            score=round(float(sum(range_scores) / max(1, len(range_scores))), 2),
            details="Negative / impossible values in key numeric columns.",
        ),
    ]

    quality_boost = (freshness_score * 0.5 + schema_score * 0.5)
    overall_score = round(
        (
            (sum(daily_scores.values()) / max(1, len(daily_scores))) * 0.82
            + quality_boost * 0.18
        ),
        2,
    )

    run_timestamp = datetime.now(timezone.utc).isoformat()
    conn.execute("DELETE FROM data_quality_results")
    conn.executemany(
        """
        INSERT INTO data_quality_results (run_timestamp, check_name, table_name, status, score, details)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [(run_timestamp, c.check_name, c.table_name, c.status, c.score, c.details) for c in checks],
    )
    conn.commit()

    issues = [
        item
        for item in daily_components
        if item["missing_delivered"] > 0 or item["duplicate_score"] < 95
    ]

    return {
        "run_timestamp": run_timestamp,
        "overall_score": overall_score,
        "freshness_lag_days": freshness_lag,
        "daily_scores": daily_scores,
        "checks": [c.__dict__ for c in checks],
        "issues": issues,
        "components": daily_components,
    }


def render_scorecard_markdown(result: dict[str, Any]) -> str:
    lines = [
        "# Data Quality Scorecard",
        "",
        f"- Run timestamp (UTC): {result['run_timestamp']}",
        f"- Overall score: **{result['overall_score']} / 100**",
        f"- Freshness lag (days): {result['freshness_lag_days']}",
        "",
        "## Check Results",
    ]

    for check in result["checks"]:
        lines.append(
            f"- {check['check_name']} ({check['table_name']}): {check['status'].upper()} | score={check['score']} | {check['details']}"
        )

    lines.append("")
    lines.append("## Notable Daily Issues")
    issue_rows = result.get("issues", [])
    if not issue_rows:
        lines.append("- None detected.")
    else:
        for issue in issue_rows[:10]:
            lines.append(
                f"- {issue['date']}: missing_delivered={issue['missing_delivered']}, duplicate_score={issue['duplicate_score']}, overall={issue['overall']}"
            )

    return "\n".join(lines)


def serialize_quality_json(result: dict[str, Any]) -> str:
    return json.dumps(result, indent=2)
