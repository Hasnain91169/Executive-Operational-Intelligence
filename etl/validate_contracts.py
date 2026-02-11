from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import sqlite3
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
CLEAN_DIR = ROOT / "data" / "clean"


@dataclass
class ContractResult:
    table_name: str
    status: str
    details: str


CONTRACTS: dict[str, dict[str, Any]] = {
    "dim_date": {
        "columns": {
            "date_key": "int",
            "date": "str",
            "week": "int",
            "month": "int",
            "quarter": "int",
            "day_name": "str",
        },
        "nullable": [],
    },
    "dim_site": {
        "columns": {"site_id": "int", "site_name": "str"},
        "nullable": [],
    },
    "dim_customer": {
        "columns": {"customer_id": "int", "customer_name": "str", "tier": "str"},
        "nullable": [],
    },
    "dim_team": {
        "columns": {"team_id": "int", "team_name": "str"},
        "nullable": [],
    },
    "dim_category": {
        "columns": {"category_id": "int", "category_name": "str"},
        "nullable": [],
    },
    "dim_carrier": {
        "columns": {"carrier_id": "int", "carrier_name": "str"},
        "nullable": [],
    },
    "dim_product": {
        "columns": {"product_id": "int", "product_family": "str"},
        "nullable": [],
    },
    "fact_jobs": {
        "columns": {
            "job_id": "str",
            "date_key": "int",
            "site_id": "int",
            "customer_id": "int",
            "team_id": "int",
            "carrier_id": "int",
            "product_id": "int",
            "value_gbp": "float",
            "promised_date_key": "int",
            "delivered_date_key": "int",
            "status": "str",
            "priority": "str",
            "duplicate_flag": "int",
            "source_batch": "str",
        },
        "nullable": ["delivered_date_key"],
    },
    "fact_incidents": {
        "columns": {
            "incident_id": "str",
            "date_key": "int",
            "site_id": "int",
            "job_id": "str",
            "incident_type": "str",
            "severity": "str",
            "minutes_lost": "int",
            "product_id": "int",
        },
        "nullable": [],
    },
    "fact_comms": {
        "columns": {
            "comm_id": "str",
            "date_key": "int",
            "site_id": "int",
            "customer_id": "int",
            "category_id": "int",
            "channel": "str",
            "minutes_spent": "int",
            "sla_sensitive_bool": "int",
            "response_minutes": "int",
            "breached_bool": "int",
            "job_id": "str",
            "carrier_id": "int",
            "product_id": "int",
        },
        "nullable": [],
    },
    "fact_costs": {
        "columns": {
            "cost_id": "str",
            "date_key": "int",
            "site_id": "int",
            "cost_type": "str",
            "amount_gbp": "float",
        },
        "nullable": [],
    },
    "fact_automation_events": {
        "columns": {
            "event_id": "str",
            "date_key": "int",
            "site_id": "int",
            "event_type": "str",
            "hours_saved": "float",
            "gbp_saved": "float",
            "notes": "str",
        },
        "nullable": ["notes"],
    },
    "scenario_registry": {
        "columns": {
            "scenario_tag": "str",
            "scenario_date": "str",
            "description": "str",
            "kpi_name": "str",
            "expected_driver_dimension": "str",
            "expected_driver_value": "str",
        },
        "nullable": [],
    },
}


def _check_series_type(series: pd.Series, expected: str) -> bool:
    non_null = series.dropna()
    if expected == "str":
        return True
    if expected == "int":
        coerced = pd.to_numeric(non_null, errors="coerce")
        return coerced.notna().all()
    if expected == "float":
        coerced = pd.to_numeric(non_null, errors="coerce")
        return coerced.notna().all()
    return False


def validate_clean_contracts() -> list[ContractResult]:
    results: list[ContractResult] = []

    for table_name, contract in CONTRACTS.items():
        path = CLEAN_DIR / f"{table_name}.csv"
        if not path.exists():
            results.append(ContractResult(table_name=table_name, status="failed", details=f"Missing file: {path.name}"))
            continue

        frame = pd.read_csv(path)
        required_cols = contract["columns"]
        nullable = set(contract.get("nullable", []))

        missing_columns = sorted(set(required_cols.keys()) - set(frame.columns))
        if missing_columns:
            results.append(
                ContractResult(
                    table_name=table_name,
                    status="failed",
                    details=f"Missing columns: {', '.join(missing_columns)}",
                )
            )
            continue

        errors: list[str] = []
        for col, expected_type in required_cols.items():
            series = frame[col]
            if not _check_series_type(series, expected_type):
                errors.append(f"{col} type mismatch expected {expected_type}")
            if col not in nullable and series.isna().any():
                null_count = int(series.isna().sum())
                errors.append(f"{col} has {null_count} null(s)")

        if errors:
            results.append(ContractResult(table_name=table_name, status="failed", details="; ".join(errors)))
        else:
            results.append(ContractResult(table_name=table_name, status="passed", details="All required checks passed"))

    return results


def log_results(conn: sqlite3.Connection, results: list[ContractResult]) -> None:
    ts = datetime.now(timezone.utc).isoformat()
    rows = [(ts, r.table_name, r.status, r.details) for r in results]
    conn.executemany(
        """
        INSERT INTO contract_validation_log (run_timestamp, table_name, status, details)
        VALUES (?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()


def main() -> None:
    results = validate_clean_contracts()
    failures = [r for r in results if r.status != "passed"]
    for result in results:
        print(f"[{result.status.upper()}] {result.table_name}: {result.details}")

    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
