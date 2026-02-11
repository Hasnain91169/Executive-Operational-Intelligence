from __future__ import annotations

from datetime import date, timedelta

from fastapi import APIRouter, Depends, HTTPException, Query

from api.db import get_connection
from api.routes.auth import kpi_allowed_for_role, require_role

router = APIRouter(prefix="/kpis", tags=["kpis"])


def _parse_range(from_date: str | None, to_date: str | None) -> tuple[str, str]:
    try:
        end = date.fromisoformat(to_date) if to_date else date.today()
        start = date.fromisoformat(from_date) if from_date else (end - timedelta(days=29))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Dates must be in YYYY-MM-DD format") from exc
    if start > end:
        raise HTTPException(status_code=400, detail="'from' must be on or before 'to'")
    return start.isoformat(), end.isoformat()


@router.get("/summary")
def kpi_summary(
    from_date: str | None = Query(default=None, alias="from"),
    to_date: str | None = Query(default=None, alias="to"),
    role: str = Depends(require_role),
) -> dict:
    start, end = _parse_range(from_date, to_date)

    with get_connection() as conn:
        rows = conn.execute(
            """
            WITH scoped AS (
                SELECT *
                FROM fact_kpi_daily
                WHERE date BETWEEN ? AND ?
            ),
            latest AS (
                SELECT kpi_name, MAX(date) AS max_date
                FROM scoped
                GROUP BY kpi_name
            )
            SELECT
                s.kpi_name,
                ROUND(AVG(s.value), 4) AS avg_value,
                ROUND(MIN(s.value), 4) AS min_value,
                ROUND(MAX(s.value), 4) AS max_value,
                ROUND(lv.value, 4) AS latest_value,
                lv.date AS latest_date,
                s.target_good,
                s.target_bad,
                s.owner_role
            FROM scoped s
            JOIN latest l
              ON l.kpi_name = s.kpi_name
            JOIN fact_kpi_daily lv
              ON lv.kpi_name = l.kpi_name
             AND lv.date = l.max_date
            GROUP BY s.kpi_name, lv.value, lv.date, s.target_good, s.target_bad, s.owner_role
            ORDER BY s.kpi_name
            """,
            (start, end),
        ).fetchall()

        filtered = [dict(row) for row in rows if kpi_allowed_for_role(role, row["kpi_name"])]
        return {"from": start, "to": end, "role": role, "kpis": filtered}


@router.get("/timeseries")
def kpi_timeseries(
    kpi_name: str,
    from_date: str | None = Query(default=None, alias="from"),
    to_date: str | None = Query(default=None, alias="to"),
    role: str = Depends(require_role),
) -> dict:
    if not kpi_allowed_for_role(role, kpi_name):
        raise HTTPException(status_code=403, detail="Role does not have access to this KPI")

    start, end = _parse_range(from_date, to_date)
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT date, value, status, target_good, target_bad
            FROM fact_kpi_daily
            WHERE kpi_name = ?
              AND date BETWEEN ? AND ?
            ORDER BY date
            """,
            (kpi_name, start, end),
        ).fetchall()

    return {
        "kpi_name": kpi_name,
        "from": start,
        "to": end,
        "role": role,
        "points": [dict(row) for row in rows],
    }


@router.get("/definitions")
def kpi_definitions(role: str = Depends(require_role)) -> dict:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT kpi_name, description, formula_text, grain, owner_role, threshold_good,
                   threshold_bad, refresh_cadence, business_value, leading_indicator_bool
            FROM kpi_definitions
            ORDER BY kpi_name
            """
        ).fetchall()

    definitions = [dict(row) for row in rows if kpi_allowed_for_role(role, row["kpi_name"])]
    return {"role": role, "definitions": definitions}
