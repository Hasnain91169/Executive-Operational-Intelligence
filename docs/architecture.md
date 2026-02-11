# Architecture

## Overview

Operational Intelligence Copilot is a local BI + AI MVP that implements:

- Deterministic ops data generation (120 days)
- Star-schema mart for Power BI Desktop
- FastAPI endpoints for KPI boards and drill-through
- Robust anomaly detection (median/MAD)
- Evidence-grounded explain engine with SQL traceability
- RPA-style webhook automation triggers
- Governance layer (contracts, quality checks, ownership, usage, audit)

## Logical Flow

```text
Raw Generator (etl/generate_sample_data.py)
        |
        v
Clean Transform (etl/transform.py) --> Contract Validation (etl/validate_contracts.py)
        |
        v
Mart Loader (etl/load_mart.py) --> SQLite Mart + CSV Exports (data/mart/*.csv)
        |                                   |
        |                                   +--> Power BI Desktop Import
        |
        +--> Data Quality Scorecard (governance/scorecard.md)

FastAPI (api/main.py)
  |- /kpis/*
  |- /anomalies/*  --> ai/anomaly.py
  |- /explain      --> ai/explain.py + ai/drivers.py + explain_audit_log
  |- /automations/* --> webhook POST + automation_runs
  |- /governance/* --> contracts + scorecard + feedback

Webhook Receiver (automation/webhook_receiver.py)
```

## Data Layer

- Warehouse DB: `data/mart/ops_copilot.db`
- SQL contracts:
  - `sql/schema.sql`
  - `sql/kpi_definitions.sql`
  - `sql/views.sql`
  - `sql/rbac_views.sql`
- Star schema dimensions:
  - `dim_date`, `dim_site`, `dim_customer`, `dim_team`, `dim_category`, `dim_carrier`, `dim_product`
- Facts:
  - `fact_jobs`, `fact_incidents`, `fact_comms`, `fact_costs`, `fact_automation_events`, `fact_kpi_daily`

## Governance Features

- Data contracts validation log: `contract_validation_log`
- Data quality checks: completeness, freshness, duplicates, nulls, schema drift, out-of-range
- Metric store governance: ownership, thresholds, cadence completeness
- Auditability:
  - Explanation logs: `explain_audit_log`
  - API usage: `api_usage_log`
  - Feedback proxy: `feedback`

## Security and Role Scoping (RBAC-lite)

Header: `x-api-key`

- `exec-local-key` -> `exec` (all KPIs)
- `ops-local-key` -> `ops` (delivery/workload/service KPIs)
- `finance-local-key` -> `finance` (cost/automation/value KPIs)

Role filters are enforced in API logic and mirrored by SQL role views in `sql/rbac_views.sql`.
