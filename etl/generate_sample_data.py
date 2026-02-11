from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

SEED = 42
LOOKBACK_DAYS = 120

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"


@dataclass(frozen=True)
class ScenarioConfig:
    scenario_a_date: date
    scenario_b_date: date
    scenario_c_dates: tuple[date, date]


def date_key(d: date) -> int:
    return int(d.strftime("%Y%m%d"))


def init_dimensions() -> dict[str, pd.DataFrame]:
    dim_site = pd.DataFrame(
        [
            {"site_id": 1, "site_name": "Birmingham"},
            {"site_id": 2, "site_name": "Coventry"},
            {"site_id": 3, "site_name": "Leeds"},
        ]
    )

    dim_customer = pd.DataFrame(
        [
            {"customer_id": idx + 1, "customer_name": f"Customer {idx + 1:02d}", "tier": tier}
            for idx, tier in enumerate(
                [
                    "Tier A",
                    "Tier A",
                    "Tier A",
                    "Tier B",
                    "Tier B",
                    "Tier B",
                    "Tier B",
                    "Tier C",
                    "Tier C",
                    "Tier C",
                    "Tier C",
                    "Tier B",
                ]
            )
        ]
    )

    dim_team = pd.DataFrame(
        [
            {"team_id": 1, "team_name": "Ops"},
            {"team_id": 2, "team_name": "Planning"},
            {"team_id": 3, "team_name": "Customer Service"},
            {"team_id": 4, "team_name": "Finance"},
        ]
    )

    dim_category = pd.DataFrame(
        [
            {"category_id": 1, "category_name": "Tracking/ETA"},
            {"category_id": 2, "category_name": "Exception/Delay"},
            {"category_id": 3, "category_name": "Documentation"},
            {"category_id": 4, "category_name": "Pricing"},
            {"category_id": 5, "category_name": "Ad-hoc"},
        ]
    )

    dim_carrier = pd.DataFrame(
        [
            {"carrier_id": 1, "carrier_name": "NorthHaul"},
            {"carrier_id": 2, "carrier_name": "SwiftFreight"},
            {"carrier_id": 3, "carrier_name": "AtlasMove"},
            {"carrier_id": 4, "carrier_name": "CrownTransit"},
            {"carrier_id": 5, "carrier_name": "RapidLynx"},
        ]
    )

    dim_product = pd.DataFrame(
        [
            {"product_id": 1, "product_family": "Walls"},
            {"product_id": 2, "product_family": "Ceilings"},
            {"product_id": 3, "product_family": "Partitions"},
            {"product_id": 4, "product_family": "Fit-Out"},
        ]
    )

    return {
        "dim_site": dim_site,
        "dim_customer": dim_customer,
        "dim_team": dim_team,
        "dim_category": dim_category,
        "dim_carrier": dim_carrier,
        "dim_product": dim_product,
    }


def choose_anchor_end_date() -> date:
    override = os.getenv("OPS_COPILOT_END_DATE")
    if override:
        return date.fromisoformat(override)
    return date.today() - timedelta(days=1)


def init_scenarios(date_list: list[date]) -> ScenarioConfig:
    return ScenarioConfig(
        scenario_a_date=date_list[84],
        scenario_b_date=date_list[97],
        scenario_c_dates=(date_list[108], date_list[109]),
    )


def generate_operational_facts(
    dimensions: dict[str, pd.DataFrame], date_list: list[date], scenarios: ScenarioConfig
) -> dict[str, pd.DataFrame]:
    rng = np.random.default_rng(SEED)

    customer_ids = dimensions["dim_customer"]["customer_id"].to_numpy()
    customer_weights = np.array([0.16, 0.14, 0.12, 0.1, 0.1, 0.09, 0.08, 0.06, 0.05, 0.04, 0.03, 0.03])
    customer_weights = customer_weights / customer_weights.sum()

    team_ids = np.array([1, 2, 3, 4])
    team_weights = np.array([0.45, 0.22, 0.23, 0.1])

    carrier_ids = np.array([1, 2, 3, 4, 5])
    carrier_weights = np.array([0.28, 0.2, 0.18, 0.17, 0.17])

    product_ids = np.array([1, 2, 3, 4])
    product_weights = np.array([0.33, 0.24, 0.22, 0.21])

    incident_types = ["Late materials", "Vehicle issue", "Damage", "Reschedule", "Wrong spec"]
    incident_weights = np.array([0.26, 0.2, 0.18, 0.18, 0.18])

    jobs: list[dict] = []
    incidents: list[dict] = []
    comms: list[dict] = []
    costs: list[dict] = []
    automation_events: list[dict] = []

    job_counter = 1
    incident_counter = 1
    comm_counter = 1
    cost_counter = 1
    auto_counter = 1

    end_date = date_list[-1]
    scenario_c_date_set = set(scenarios.scenario_c_dates)

    for d in date_list:
        for site_id in [1, 2, 3]:
            site_multiplier = {1: 1.1, 2: 1.0, 3: 0.9}[site_id]
            base_jobs = max(10, int(rng.normal(28 * site_multiplier, 4)))

            for _ in range(base_jobs):
                customer_id = int(rng.choice(customer_ids, p=customer_weights))
                team_id = int(rng.choice(team_ids, p=team_weights))
                carrier_id = int(rng.choice(carrier_ids, p=carrier_weights))
                product_id = int(rng.choice(product_ids, p=product_weights))

                promised_date = d + timedelta(days=int(rng.integers(2, 9)))
                delivery_shift = int(rng.choice([-1, 0, 0, 1, 1, 2, 3], p=[0.1, 0.28, 0.24, 0.18, 0.1, 0.06, 0.04]))
                delivered_date = promised_date + timedelta(days=delivery_shift)

                missing_delivery = site_id == 3 and d in scenario_c_date_set and rng.random() < 0.58
                if missing_delivery:
                    delivered_date = None

                status = "Delivered" if delivered_date and delivered_date <= end_date else "In Progress"
                if missing_delivery:
                    status = "Delivered"

                value_gbp = float(np.clip(rng.normal(6200, 1400), 1800, 14000))
                priority = str(rng.choice(["Low", "Medium", "High"], p=[0.35, 0.5, 0.15]))

                job_id = f"JOB-{job_counter:07d}"
                job_counter += 1

                job_row = {
                    "job_id": job_id,
                    "date": d.isoformat(),
                    "date_key": date_key(d),
                    "site_id": site_id,
                    "customer_id": customer_id,
                    "team_id": team_id,
                    "carrier_id": carrier_id,
                    "product_id": product_id,
                    "value_gbp": round(value_gbp, 2),
                    "promised_date": promised_date.isoformat(),
                    "promised_date_key": date_key(promised_date),
                    "delivered_date": delivered_date.isoformat() if delivered_date else None,
                    "delivered_date_key": date_key(delivered_date) if delivered_date else None,
                    "status": status,
                    "priority": priority,
                    "duplicate_flag": 0,
                    "source_batch": "baseline",
                }
                jobs.append(job_row)

                # Scenario C: duplicate jobs at Leeds across two days.
                if site_id == 3 and d in scenario_c_date_set and rng.random() < 0.13:
                    duplicate = job_row.copy()
                    duplicate["job_id"] = f"{job_id}-DUP"
                    duplicate["duplicate_flag"] = 1
                    duplicate["source_batch"] = "scenario_c_duplicate"
                    jobs.append(duplicate)

                comm_count = int(rng.choice([0, 1, 2, 3], p=[0.2, 0.38, 0.28, 0.14]))
                in_scenario_a = d == scenarios.scenario_a_date and site_id == 1 and carrier_id == 1
                if in_scenario_a:
                    comm_count += int(rng.integers(4, 8))

                for _ in range(comm_count):
                    category_id = int(
                        rng.choice(
                            [1, 2, 3, 4, 5],
                            p=[0.48, 0.25, 0.12, 0.07, 0.08] if in_scenario_a else [0.34, 0.2, 0.17, 0.14, 0.15],
                        )
                    )
                    sla_sensitive = 1 if category_id in (1, 2) else int(rng.random() < 0.18)

                    if in_scenario_a:
                        response_minutes = int(np.clip(rng.normal(225, 40), 80, 420))
                        minutes_spent = int(np.clip(rng.normal(26, 7), 8, 80))
                    else:
                        mean = 78 if sla_sensitive else 42
                        spread = 35 if sla_sensitive else 18
                        response_minutes = int(np.clip(rng.normal(mean, spread), 4, 300))
                        minutes_spent = int(np.clip(rng.normal(14 if sla_sensitive else 10, 4), 3, 55))

                    breached = 1 if sla_sensitive and response_minutes > 90 else 0
                    channel = str(rng.choice(["email", "call"], p=[0.62, 0.38]))

                    comms.append(
                        {
                            "comm_id": f"COM-{comm_counter:08d}",
                            "date": d.isoformat(),
                            "date_key": date_key(d),
                            "site_id": site_id,
                            "customer_id": customer_id,
                            "category_id": category_id,
                            "channel": channel,
                            "minutes_spent": minutes_spent,
                            "sla_sensitive_bool": sla_sensitive,
                            "response_minutes": response_minutes,
                            "breached_bool": breached,
                            "job_id": job_id,
                            "carrier_id": carrier_id,
                            "product_id": product_id,
                        }
                    )
                    comm_counter += 1

                incident_probability = 0.1
                in_scenario_b = d == scenarios.scenario_b_date and site_id == 2 and product_id == 3
                if in_scenario_b:
                    incident_probability = 0.88

                if rng.random() < incident_probability:
                    if in_scenario_b:
                        incident_type = "Late materials"
                        minutes_lost = int(np.clip(rng.normal(165, 45), 60, 360))
                    else:
                        incident_type = str(rng.choice(incident_types, p=incident_weights))
                        minutes_lost = int(np.clip(rng.normal(72, 32), 12, 260))

                    severity = "High" if minutes_lost >= 150 else "Medium" if minutes_lost >= 70 else "Low"
                    incidents.append(
                        {
                            "incident_id": f"INC-{incident_counter:08d}",
                            "date": d.isoformat(),
                            "date_key": date_key(d),
                            "site_id": site_id,
                            "job_id": job_id,
                            "incident_type": incident_type,
                            "severity": severity,
                            "minutes_lost": minutes_lost,
                            "product_id": product_id,
                        }
                    )
                    incident_counter += 1

            cost_types = ["Overtime", "Rework", "Expedite", "Claims"]
            for cost_type in cost_types:
                if rng.random() < 0.82:
                    amount = float(np.clip(rng.normal(850, 280), 80, 2600))
                    if d == scenarios.scenario_a_date and site_id == 1 and cost_type in ("Overtime", "Expedite"):
                        amount *= 1.7
                    if d == scenarios.scenario_b_date and site_id == 2 and cost_type == "Expedite":
                        amount *= 2.1
                    if d in scenario_c_date_set and site_id == 3 and cost_type == "Rework":
                        amount *= 1.5

                    costs.append(
                        {
                            "cost_id": f"CST-{cost_counter:08d}",
                            "date": d.isoformat(),
                            "date_key": date_key(d),
                            "site_id": site_id,
                            "cost_type": cost_type,
                            "amount_gbp": round(amount, 2),
                        }
                    )
                    cost_counter += 1

            if rng.random() < 0.38:
                events = int(rng.integers(1, 3))
                for _ in range(events):
                    hours_saved = float(np.clip(rng.normal(2.4, 0.8), 0.5, 5.8))
                    gbp_saved = round(hours_saved * float(np.clip(rng.normal(48, 8), 24, 75)), 2)
                    notes = str(
                        rng.choice(
                            [
                                "Auto-routed SLA alerts",
                                "Template-based comm reply",
                                "Invoice validation bot",
                                "Exception triage workflow",
                            ]
                        )
                    )
                    automation_events.append(
                        {
                            "event_id": f"AUT-{auto_counter:08d}",
                            "date": d.isoformat(),
                            "date_key": date_key(d),
                            "site_id": site_id,
                            "event_type": "workflow_run",
                            "hours_saved": round(hours_saved, 2),
                            "gbp_saved": gbp_saved,
                            "notes": notes,
                        }
                    )
                    auto_counter += 1

    return {
        "fact_jobs": pd.DataFrame(jobs),
        "fact_incidents": pd.DataFrame(incidents),
        "fact_comms": pd.DataFrame(comms),
        "fact_costs": pd.DataFrame(costs),
        "fact_automation_events": pd.DataFrame(automation_events),
    }


def build_scenario_registry(scenarios: ScenarioConfig) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "scenario_tag": "Scenario A",
                "scenario_date": scenarios.scenario_a_date.isoformat(),
                "description": "SLA spike driven by Birmingham + NorthHaul comm surge.",
                "kpi_name": "sla_breach_rate_pct",
                "expected_driver_dimension": "carrier",
                "expected_driver_value": "NorthHaul",
            },
            {
                "scenario_tag": "Scenario B",
                "scenario_date": scenarios.scenario_b_date.isoformat(),
                "description": "Exception spike in Coventry for Partitions due to late materials.",
                "kpi_name": "exception_rate_per_100_jobs",
                "expected_driver_dimension": "product_family",
                "expected_driver_value": "Partitions",
            },
            {
                "scenario_tag": "Scenario C",
                "scenario_date": scenarios.scenario_c_dates[0].isoformat(),
                "description": "Data quality issue at Leeds with duplicates and missing delivered date for two days.",
                "kpi_name": "data_quality_score",
                "expected_driver_dimension": "site",
                "expected_driver_value": "Leeds",
            },
        ]
    )


def main() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    end_date = choose_anchor_end_date()
    start_date = end_date - timedelta(days=LOOKBACK_DAYS - 1)
    date_list = [d.date() for d in pd.date_range(start_date, end_date, freq="D")]

    scenarios = init_scenarios(date_list)
    dimensions = init_dimensions()
    facts = generate_operational_facts(dimensions, date_list, scenarios)
    scenario_registry = build_scenario_registry(scenarios)

    for table_name, df in dimensions.items():
        df.to_csv(RAW_DIR / f"{table_name}.csv", index=False)

    for table_name, df in facts.items():
        df.to_csv(RAW_DIR / f"{table_name}.csv", index=False)

    scenario_registry.to_csv(RAW_DIR / "scenario_registry.csv", index=False)

    metadata = {
        "seed": SEED,
        "lookback_days": LOOKBACK_DAYS,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "scenario_a_date": scenarios.scenario_a_date.isoformat(),
        "scenario_b_date": scenarios.scenario_b_date.isoformat(),
        "scenario_c_date_1": scenarios.scenario_c_dates[0].isoformat(),
        "scenario_c_date_2": scenarios.scenario_c_dates[1].isoformat(),
    }
    (RAW_DIR / "metadata.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")

    print("Generated raw data in data/raw")
    print(json.dumps(metadata, indent=2))


if __name__ == "__main__":
    main()
