from __future__ import annotations

from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
CLEAN_DIR = ROOT / "data" / "clean"


def load_csv(name: str) -> pd.DataFrame:
    path = RAW_DIR / f"{name}.csv"
    if not path.exists():
        raise FileNotFoundError(f"Missing raw input: {path}")
    return pd.read_csv(path)


def build_dim_date(jobs: pd.DataFrame, comms: pd.DataFrame, incidents: pd.DataFrame, costs: pd.DataFrame, automation_events: pd.DataFrame) -> pd.DataFrame:
    date_cols = []
    for col in ["date", "promised_date", "delivered_date"]:
        if col in jobs.columns:
            date_cols.append(pd.to_datetime(jobs[col], errors="coerce"))

    for frame in [comms, incidents, costs, automation_events]:
        if "date" in frame.columns:
            date_cols.append(pd.to_datetime(frame["date"], errors="coerce"))

    all_dates = pd.concat(date_cols, axis=0).dropna().drop_duplicates().sort_values()
    dim_date = pd.DataFrame({"date": all_dates.dt.date.astype(str)})
    dim_date["date_key"] = pd.to_datetime(dim_date["date"]).dt.strftime("%Y%m%d").astype(int)
    iso = pd.to_datetime(dim_date["date"]).dt.isocalendar()
    dim_date["week"] = iso.week.astype(int)
    dim_date["month"] = pd.to_datetime(dim_date["date"]).dt.month.astype(int)
    dim_date["quarter"] = pd.to_datetime(dim_date["date"]).dt.quarter.astype(int)
    dim_date["day_name"] = pd.to_datetime(dim_date["date"]).dt.day_name()
    return dim_date[["date_key", "date", "week", "month", "quarter", "day_name"]]


def transform() -> None:
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)

    dim_site = load_csv("dim_site")
    dim_customer = load_csv("dim_customer")
    dim_team = load_csv("dim_team")
    dim_category = load_csv("dim_category")
    dim_carrier = load_csv("dim_carrier")
    dim_product = load_csv("dim_product")

    fact_jobs = load_csv("fact_jobs")
    fact_incidents = load_csv("fact_incidents")
    fact_comms = load_csv("fact_comms")
    fact_costs = load_csv("fact_costs")
    fact_automation_events = load_csv("fact_automation_events")
    scenario_registry = load_csv("scenario_registry")

    fact_jobs["date"] = pd.to_datetime(fact_jobs["date"]).dt.date.astype(str)
    fact_jobs["promised_date"] = pd.to_datetime(fact_jobs["promised_date"]).dt.date.astype(str)
    fact_jobs["delivered_date"] = pd.to_datetime(fact_jobs["delivered_date"], errors="coerce").dt.date.astype(str)

    for col in ["date_key", "site_id", "customer_id", "team_id", "carrier_id", "product_id", "promised_date_key", "duplicate_flag"]:
        fact_jobs[col] = pd.to_numeric(fact_jobs[col], errors="coerce").astype("Int64")
    fact_jobs["delivered_date_key"] = pd.to_numeric(fact_jobs["delivered_date_key"], errors="coerce").astype("Int64")
    fact_jobs["value_gbp"] = pd.to_numeric(fact_jobs["value_gbp"], errors="coerce").astype(float)

    fact_incidents["date"] = pd.to_datetime(fact_incidents["date"]).dt.date.astype(str)
    for col in ["date_key", "site_id", "minutes_lost", "product_id"]:
        fact_incidents[col] = pd.to_numeric(fact_incidents[col], errors="coerce").astype("Int64")

    fact_comms["date"] = pd.to_datetime(fact_comms["date"]).dt.date.astype(str)
    for col in [
        "date_key",
        "site_id",
        "customer_id",
        "category_id",
        "minutes_spent",
        "sla_sensitive_bool",
        "response_minutes",
        "breached_bool",
        "carrier_id",
        "product_id",
    ]:
        fact_comms[col] = pd.to_numeric(fact_comms[col], errors="coerce").astype("Int64")

    fact_costs["date"] = pd.to_datetime(fact_costs["date"]).dt.date.astype(str)
    for col in ["date_key", "site_id"]:
        fact_costs[col] = pd.to_numeric(fact_costs[col], errors="coerce").astype("Int64")
    fact_costs["amount_gbp"] = pd.to_numeric(fact_costs["amount_gbp"], errors="coerce").astype(float)

    fact_automation_events["date"] = pd.to_datetime(fact_automation_events["date"]).dt.date.astype(str)
    for col in ["date_key", "site_id"]:
        fact_automation_events[col] = pd.to_numeric(fact_automation_events[col], errors="coerce").astype("Int64")
    fact_automation_events["hours_saved"] = pd.to_numeric(fact_automation_events["hours_saved"], errors="coerce").astype(float)
    fact_automation_events["gbp_saved"] = pd.to_numeric(fact_automation_events["gbp_saved"], errors="coerce").astype(float)

    dim_date = build_dim_date(fact_jobs, fact_comms, fact_incidents, fact_costs, fact_automation_events)

    outputs = {
        "dim_site": dim_site,
        "dim_customer": dim_customer,
        "dim_team": dim_team,
        "dim_category": dim_category,
        "dim_carrier": dim_carrier,
        "dim_product": dim_product,
        "dim_date": dim_date,
        "fact_jobs": fact_jobs[
            [
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
            ]
        ],
        "fact_incidents": fact_incidents[
            ["incident_id", "date_key", "site_id", "job_id", "incident_type", "severity", "minutes_lost", "product_id"]
        ],
        "fact_comms": fact_comms[
            [
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
            ]
        ],
        "fact_costs": fact_costs[["cost_id", "date_key", "site_id", "cost_type", "amount_gbp"]],
        "fact_automation_events": fact_automation_events[
            ["event_id", "date_key", "site_id", "event_type", "hours_saved", "gbp_saved", "notes"]
        ],
        "scenario_registry": scenario_registry,
    }

    for name, frame in outputs.items():
        frame.to_csv(CLEAN_DIR / f"{name}.csv", index=False)

    print("Transformed clean data in data/clean")


if __name__ == "__main__":
    transform()
