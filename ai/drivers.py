from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
import sqlite3
from typing import Any

import pandas as pd


@dataclass
class DimensionQueryConfig:
    dimension: str
    segment_expr: str
    from_clause: str
    numerator_expr: str
    denominator_expr: str
    extra_where: str = "1=1"


def _safe_rate(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return float(numerator / denominator)


def _run_df(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...]) -> pd.DataFrame:
    return pd.read_sql_query(sql, conn, params=params)


def _build_driver_rows(
    dimension: str,
    anomaly_df: pd.DataFrame,
    baseline_df: pd.DataFrame,
    top_n: int = 5,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if anomaly_df.empty and baseline_df.empty:
        return [], []

    if anomaly_df.empty:
        anomaly_df = pd.DataFrame(columns=["segment", "numerator", "denominator"])
    if baseline_df.empty:
        baseline_df = pd.DataFrame(columns=["segment", "numerator", "denominator"])

    anomaly_df = anomaly_df.copy()
    baseline_df = baseline_df.copy()

    anomaly_df["segment"] = anomaly_df["segment"].fillna("Unknown").astype(str)
    baseline_df["segment"] = baseline_df["segment"].fillna("Unknown").astype(str)

    baseline_grouped = baseline_df.groupby("segment", as_index=False)[["numerator", "denominator"]].mean()

    merged = anomaly_df.merge(
        baseline_grouped,
        on="segment",
        how="outer",
        suffixes=("_anomaly", "_baseline"),
    ).fillna(0.0)

    total_anomaly_num = float(merged["numerator_anomaly"].sum())
    total_baseline_num = float(merged["numerator_baseline"].sum())
    total_num_delta = total_anomaly_num - total_baseline_num

    rows: list[dict[str, Any]] = []
    evidence_rows: list[dict[str, Any]] = []

    for _, row in merged.iterrows():
        a_num = float(row["numerator_anomaly"])
        a_den = float(row["denominator_anomaly"])
        b_num = float(row["numerator_baseline"])
        b_den = float(row["denominator_baseline"])

        anomaly_rate = _safe_rate(a_num, a_den)
        baseline_rate = _safe_rate(b_num, b_den)
        delta_abs = anomaly_rate - baseline_rate
        delta_pct = (delta_abs / baseline_rate * 100.0) if baseline_rate > 0 else None

        numerator_delta = a_num - b_num
        contribution_share = (numerator_delta / total_num_delta) if total_num_delta != 0 else 0.0

        rows.append(
            {
                "dimension": dimension,
                "segment": str(row["segment"]),
                "delta_abs": round(delta_abs, 6),
                "delta_pct": round(delta_pct, 4) if delta_pct is not None else None,
                "contribution_share": round(contribution_share, 6),
                "supporting_stats": {
                    "anomaly_numerator": round(a_num, 4),
                    "anomaly_denominator": round(a_den, 4),
                    "baseline_numerator": round(b_num, 4),
                    "baseline_denominator": round(b_den, 4),
                    "anomaly_rate": round(anomaly_rate, 6),
                    "baseline_rate": round(baseline_rate, 6),
                },
                "driver_score": abs(numerator_delta) * max(0.01, abs(delta_abs)),
            }
        )

    evidence_rows = (
        pd.DataFrame(rows)
        .sort_values(by=["driver_score", "contribution_share"], ascending=False)
        .head(top_n)
        .drop(columns=["driver_score"])
        .to_dict(orient="records")
    )

    return rows, evidence_rows


def _dimension_configs_for_kpi(kpi_name: str) -> list[DimensionQueryConfig]:
    if kpi_name == "sla_breach_rate_pct":
        from_clause = """
            fact_comms fc
            JOIN dim_date dd ON dd.date_key = fc.date_key
            JOIN dim_site ds ON ds.site_id = fc.site_id
            JOIN dim_customer dc ON dc.customer_id = fc.customer_id
            JOIN dim_category dcat ON dcat.category_id = fc.category_id
            LEFT JOIN fact_incidents fi ON fi.job_id = fc.job_id
            LEFT JOIN dim_carrier dcar ON dcar.carrier_id = fc.carrier_id
            LEFT JOIN dim_product dp ON dp.product_id = fc.product_id
        """
        numerator = "SUM(CASE WHEN fc.sla_sensitive_bool = 1 AND fc.breached_bool = 1 THEN 1 ELSE 0 END)"
        denominator = "SUM(CASE WHEN fc.sla_sensitive_bool = 1 THEN 1 ELSE 0 END)"
        return [
            DimensionQueryConfig("site", "ds.site_name", from_clause, numerator, denominator),
            DimensionQueryConfig("customer_tier", "dc.tier", from_clause, numerator, denominator),
            DimensionQueryConfig("category", "dcat.category_name", from_clause, numerator, denominator),
            DimensionQueryConfig("incident_type", "COALESCE(fi.incident_type, 'No incident')", from_clause, numerator, denominator),
            DimensionQueryConfig("carrier", "COALESCE(dcar.carrier_name, 'Unknown carrier')", from_clause, numerator, denominator),
            DimensionQueryConfig("product_family", "COALESCE(dp.product_family, 'Unknown product')", from_clause, numerator, denominator),
        ]

    if kpi_name == "exception_rate_per_100_jobs":
        from_clause = """
            fact_jobs fj
            JOIN dim_date dd ON dd.date_key = fj.date_key
            JOIN dim_site ds ON ds.site_id = fj.site_id
            JOIN dim_customer dc ON dc.customer_id = fj.customer_id
            LEFT JOIN fact_incidents fi ON fi.job_id = fj.job_id
            LEFT JOIN fact_comms fc ON fc.job_id = fj.job_id AND fc.date_key = fj.date_key
            LEFT JOIN dim_category dcat ON dcat.category_id = fc.category_id
            JOIN dim_carrier dcar ON dcar.carrier_id = fj.carrier_id
            JOIN dim_product dp ON dp.product_id = fj.product_id
        """
        numerator = "COUNT(DISTINCT fi.incident_id)"
        denominator = "COUNT(DISTINCT fj.job_id)"
        return [
            DimensionQueryConfig("site", "ds.site_name", from_clause, numerator, denominator),
            DimensionQueryConfig("customer_tier", "dc.tier", from_clause, numerator, denominator),
            DimensionQueryConfig("category", "COALESCE(dcat.category_name, 'No comm category')", from_clause, numerator, denominator),
            DimensionQueryConfig("incident_type", "COALESCE(fi.incident_type, 'No incident')", from_clause, numerator, denominator),
            DimensionQueryConfig("carrier", "dcar.carrier_name", from_clause, numerator, denominator),
            DimensionQueryConfig("product_family", "dp.product_family", from_clause, numerator, denominator),
        ]

    return []


def _analyze_rate_kpi(
    conn: sqlite3.Connection,
    kpi_name: str,
    target_date: str,
    baseline_days: int,
    top_n: int,
) -> dict[str, Any]:
    target = date.fromisoformat(target_date)
    baseline_start = (target - timedelta(days=baseline_days)).isoformat()
    baseline_end = (target - timedelta(days=1)).isoformat()

    all_rows: list[dict[str, Any]] = []
    evidence: list[dict[str, Any]] = []

    for cfg in _dimension_configs_for_kpi(kpi_name):
        anomaly_sql = f"""
            SELECT {cfg.segment_expr} AS segment,
                   {cfg.numerator_expr} AS numerator,
                   {cfg.denominator_expr} AS denominator
            FROM {cfg.from_clause}
            WHERE {cfg.extra_where}
              AND dd.date = ?
            GROUP BY {cfg.segment_expr}
        """
        baseline_sql = f"""
            SELECT dd.date AS baseline_date,
                   {cfg.segment_expr} AS segment,
                   {cfg.numerator_expr} AS numerator,
                   {cfg.denominator_expr} AS denominator
            FROM {cfg.from_clause}
            WHERE {cfg.extra_where}
              AND dd.date BETWEEN ? AND ?
            GROUP BY dd.date, {cfg.segment_expr}
        """

        anomaly_df = _run_df(conn, anomaly_sql, (target_date,))
        baseline_df = _run_df(conn, baseline_sql, (baseline_start, baseline_end))

        rows, top_rows = _build_driver_rows(cfg.dimension, anomaly_df, baseline_df)
        all_rows.extend(rows)

        evidence.append(
            {
                "dimension": cfg.dimension,
                "anomaly_sql": anomaly_sql.strip(),
                "anomaly_params": [target_date],
                "baseline_sql": baseline_sql.strip(),
                "baseline_params": [baseline_start, baseline_end],
                "top_rows": top_rows,
            }
        )

    if not all_rows:
        return {"drivers": [], "evidence": evidence}

    drivers_frame = pd.DataFrame(all_rows)

    # Prefer positive delta and contribution for spike explanations; fallback to strongest absolute movement.
    positive = drivers_frame[(drivers_frame["delta_abs"] > 0) & (drivers_frame["contribution_share"] > 0)]
    ranked = positive if not positive.empty else drivers_frame
    ranked = ranked.assign(rank_score=ranked["contribution_share"].abs() * ranked["delta_abs"].abs())
    ranked = ranked.sort_values(by=["rank_score", "contribution_share", "delta_abs"], ascending=False)

    top_drivers = ranked.head(top_n).drop(columns=["rank_score"]).to_dict(orient="records")
    return {"drivers": top_drivers, "evidence": evidence}


def _analyze_data_quality(
    conn: sqlite3.Connection,
    target_date: str,
    baseline_days: int,
    top_n: int,
) -> dict[str, Any]:
    target = date.fromisoformat(target_date)
    baseline_start = (target - timedelta(days=baseline_days)).isoformat()
    baseline_end = (target - timedelta(days=1)).isoformat()

    anomaly_sql = """
        SELECT
            ds.site_name AS segment,
            SUM(CASE WHEN fj.status = 'Delivered' AND fj.delivered_date_key IS NULL THEN 1 ELSE 0 END) AS missing_delivered,
            SUM(CASE WHEN fj.status = 'Delivered' THEN 1 ELSE 0 END) AS delivered_jobs,
            AVG(fj.duplicate_flag) AS duplicate_rate
        FROM fact_jobs fj
        JOIN dim_date dd ON dd.date_key = fj.date_key
        JOIN dim_site ds ON ds.site_id = fj.site_id
        WHERE dd.date = ?
        GROUP BY ds.site_name
    """

    baseline_sql = """
        SELECT
            dd.date AS baseline_date,
            ds.site_name AS segment,
            SUM(CASE WHEN fj.status = 'Delivered' AND fj.delivered_date_key IS NULL THEN 1 ELSE 0 END) AS missing_delivered,
            SUM(CASE WHEN fj.status = 'Delivered' THEN 1 ELSE 0 END) AS delivered_jobs,
            AVG(fj.duplicate_flag) AS duplicate_rate
        FROM fact_jobs fj
        JOIN dim_date dd ON dd.date_key = fj.date_key
        JOIN dim_site ds ON ds.site_id = fj.site_id
        WHERE dd.date BETWEEN ? AND ?
        GROUP BY dd.date, ds.site_name
    """

    anomaly_df = _run_df(conn, anomaly_sql, (target_date,))
    baseline_df = _run_df(conn, baseline_sql, (baseline_start, baseline_end))

    baseline_grouped = baseline_df.groupby("segment", as_index=False)[["missing_delivered", "delivered_jobs", "duplicate_rate"]].mean()
    merged = anomaly_df.merge(baseline_grouped, on="segment", how="outer", suffixes=("_anomaly", "_baseline")).fillna(0.0)

    rows: list[dict[str, Any]] = []
    for _, row in merged.iterrows():
        a_missing = float(row["missing_delivered_anomaly"])
        a_delivered = float(row["delivered_jobs_anomaly"])
        b_missing = float(row["missing_delivered_baseline"])
        b_delivered = float(row["delivered_jobs_baseline"])
        a_dup = float(row["duplicate_rate_anomaly"])
        b_dup = float(row["duplicate_rate_baseline"])

        a_missing_rate = _safe_rate(a_missing, a_delivered)
        b_missing_rate = _safe_rate(b_missing, b_delivered)

        anomaly_penalty = (a_missing_rate * 70.0) + (a_dup * 30.0)
        baseline_penalty = (b_missing_rate * 70.0) + (b_dup * 30.0)
        delta_abs = anomaly_penalty - baseline_penalty
        delta_pct = (delta_abs / baseline_penalty * 100.0) if baseline_penalty > 0 else None

        rows.append(
            {
                "dimension": "site",
                "segment": str(row["segment"]),
                "delta_abs": round(delta_abs, 6),
                "delta_pct": round(delta_pct, 4) if delta_pct is not None else None,
                "contribution_share": 0.0,
                "supporting_stats": {
                    "anomaly_missing_delivered": round(a_missing, 2),
                    "anomaly_delivered_jobs": round(a_delivered, 2),
                    "baseline_missing_delivered": round(b_missing, 2),
                    "baseline_delivered_jobs": round(b_delivered, 2),
                    "anomaly_duplicate_rate": round(a_dup, 6),
                    "baseline_duplicate_rate": round(b_dup, 6),
                    "anomaly_penalty": round(anomaly_penalty, 6),
                    "baseline_penalty": round(baseline_penalty, 6),
                },
            }
        )

    if rows:
        total_delta = sum(max(0.0, r["delta_abs"]) for r in rows) or 1.0
        for item in rows:
            item["contribution_share"] = round(max(0.0, item["delta_abs"]) / total_delta, 6)

    ranked = pd.DataFrame(rows)
    if not ranked.empty:
        ranked = ranked.sort_values(by=["contribution_share", "delta_abs"], ascending=False)
    drivers = ranked.head(top_n).to_dict(orient="records") if not ranked.empty else []

    evidence = [
        {
            "dimension": "site",
            "anomaly_sql": anomaly_sql.strip(),
            "anomaly_params": [target_date],
            "baseline_sql": baseline_sql.strip(),
            "baseline_params": [baseline_start, baseline_end],
            "top_rows": drivers,
        }
    ]
    return {"drivers": drivers, "evidence": evidence}


def compute_top_drivers(
    conn: sqlite3.Connection,
    kpi_name: str,
    target_date: str,
    baseline_days: int = 14,
    top_n: int = 3,
) -> dict[str, Any]:
    if kpi_name in {"sla_breach_rate_pct", "exception_rate_per_100_jobs"}:
        return _analyze_rate_kpi(conn, kpi_name, target_date, baseline_days, top_n)

    if kpi_name == "data_quality_score":
        return _analyze_data_quality(conn, target_date, baseline_days, top_n)

    return {"drivers": [], "evidence": []}
