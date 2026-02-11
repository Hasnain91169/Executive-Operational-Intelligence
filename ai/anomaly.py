from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import os
import sqlite3
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = ROOT / "data" / "mart" / "ops_copilot.db"


def _load_scenario_lookup(conn: sqlite3.Connection) -> dict[tuple[str, str], str]:
    rows = conn.execute("SELECT scenario_tag, scenario_date, kpi_name FROM scenario_registry").fetchall()
    mapping: dict[tuple[str, str], str] = {}
    for row in rows:
        if row["kpi_name"]:
            mapping[(row["kpi_name"], row["scenario_date"])] = row["scenario_tag"]
    return mapping


def recompute_anomalies(
    conn: sqlite3.Connection,
    threshold: float = 3.0,
    window_days: int = 14,
    min_history: int = 14,
    kpi_names: list[str] | None = None,
) -> list[dict[str, Any]]:
    query = "SELECT kpi_name, date, value FROM fact_kpi_daily"
    params: list[Any] = []
    if kpi_names:
        placeholders = ",".join("?" for _ in kpi_names)
        query += f" WHERE kpi_name IN ({placeholders})"
        params.extend(kpi_names)
    query += " ORDER BY kpi_name, date"

    frame = pd.read_sql_query(query, conn, params=params)
    if frame.empty:
        return []

    frame["date"] = pd.to_datetime(frame["date"]) 
    scenario_lookup = _load_scenario_lookup(conn)

    anomaly_rows: list[tuple] = []
    out: list[dict[str, Any]] = []
    created_at = datetime.now(timezone.utc).isoformat()

    for kpi_name, group in frame.groupby("kpi_name"):
        values = group.sort_values("date").reset_index(drop=True)

        for idx in range(len(values)):
            if idx < min_history:
                continue

            current = values.iloc[idx]
            history = values.iloc[max(0, idx - window_days):idx]
            if len(history) < min_history:
                continue

            baseline = float(history["value"].median())
            mad = float(np.median(np.abs(history["value"] - baseline)))

            if mad == 0:
                std = float(history["value"].std(ddof=0))
                denom = std if std > 0 else 1e-9
                score = abs(float(current["value"]) - baseline) / denom
            else:
                score = abs(float(current["value"]) - baseline) / (1.4826 * mad)

            if score > threshold:
                current_date = pd.Timestamp(current["date"]).date().isoformat()
                scenario_tag = scenario_lookup.get((kpi_name, current_date))

                record = {
                    "kpi_name": kpi_name,
                    "date": current_date,
                    "value": round(float(current["value"]), 4),
                    "baseline": round(baseline, 4),
                    "score": round(float(score), 4),
                    "status": "open",
                    "scenario_tag": scenario_tag,
                    "created_at": created_at,
                }
                out.append(record)
                anomaly_rows.append(
                    (
                        record["kpi_name"],
                        record["date"],
                        record["value"],
                        record["baseline"],
                        record["score"],
                        record["status"],
                        record["scenario_tag"],
                        record["created_at"],
                    )
                )

    conn.execute("DELETE FROM anomalies")
    if anomaly_rows:
        conn.executemany(
            """
            INSERT INTO anomalies (kpi_name, date, value, baseline, score, status, scenario_tag, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            anomaly_rows,
        )
    conn.commit()
    return out


def main() -> None:
    db_path = Path(os.getenv("OPS_COPILOT_DB_PATH", str(DEFAULT_DB)))
    if not db_path.exists():
        raise SystemExit(f"Database not found at {db_path}")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    anomalies = recompute_anomalies(conn)
    conn.close()

    print(f"Anomalies detected: {len(anomalies)}")
    for anomaly in anomalies[:20]:
        print(
            f"{anomaly['date']} | {anomaly['kpi_name']} | value={anomaly['value']} baseline={anomaly['baseline']} score={anomaly['score']}"
        )


if __name__ == "__main__":
    main()
