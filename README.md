# Operational Intelligence Copilot (BI x AI)

Operational Intelligence Copilot is a local, portfolio-grade MVP for a **Business Intelligence & Automation Lead** role.

It demonstrates enterprise BI execution end-to-end:

- Dimensional modelling + KPI metric store for Power BI Desktop
- API-first analytics service for dashboards and demos
- Robust anomaly detection (median/MAD)
- Evidence-grounded root-cause explanations (SQL-backed, auditable)
- RPA-style webhook automations with run tracking
- Governance layer (contracts, data quality scorecard, ownership, usage, feedback)

## Implementation Plan

1. Scaffold project + contracts + API skeleton
2. Deterministic 120-day data with injected scenario anomalies
3. Star-schema mart in SQLite + KPI computation + CSV export
4. KPI/anomaly/explain/governance/automation API endpoints
5. Automated anomaly + explanation tests
6. Power BI Desktop and demo docs

## Why This Is BI-Lead Ready

- Uses formal **metric definitions** (`kpi_definitions`) with ownership, thresholds, cadence, and business value.
- Produces a clean **star schema** for local Power BI Desktop.
- Treats AI as accountable analytics: all explanation outputs include **SQL evidence** and are logged.
- Includes **automation orchestration** with trigger conditions and audit trails.
- Includes governance artifacts normally expected in enterprise BI operations.

## Architecture

```text
Raw Ops Simulator (120 days, deterministic)
  -> Clean Layer + Data Contracts Validation
  -> SQLite Mart (Star Schema + KPI Store)
  -> CSV Exports for Power BI Desktop

FastAPI Service
  /kpis       -> board metrics
  /anomalies  -> median/MAD detection
  /explain    -> grounded drivers + evidence + audit
  /automations-> webhook orchestration + runs log
  /governance -> contracts + scorecard + feedback

Webhook Receiver (local n8n/Power Automate equivalent sink)
```

See `docs/architecture.md` for details.

## Repository Layout

```text
data/
  raw/
  clean/
  mart/
sql/
  schema.sql
  views.sql
  kpi_definitions.sql
  rbac_views.sql
etl/
  generate_sample_data.py
  transform.py
  load_mart.py
  validate_contracts.py
ai/
  anomaly.py
  explain.py
  drivers.py
api/
  main.py
  db.py
  models.py
  routes/
    kpis.py
    anomalies.py
    explain.py
    governance.py
    automations.py
    auth.py
governance/
  data_quality.py
  metric_store.py
  scorecard.md
  data_contracts.md
automation/
  webhook_receiver.py
  examples.md
docs/
  architecture.md
  demo_script.md
  powerbi_desktop_guide.md
tests/
```

## Quickstart (One Command Chain)

```bash
pip install -r requirements.txt
python etl/generate_sample_data.py && python etl/transform.py && python etl/load_mart.py
uvicorn api.main:app --reload
```

If `python` is unavailable on Windows, use `py -3.13` (or your installed version).

## Local Auth (RBAC-lite)

Pass `x-api-key` header:

- `exec-local-key` -> exec (all KPIs)
- `ops-local-key` -> ops workload/service KPIs
- `finance-local-key` -> finance/value KPIs

## Core Endpoints

- `GET /health`
- `GET /kpis/summary?from=YYYY-MM-DD&to=YYYY-MM-DD`
- `GET /kpis/timeseries?kpi_name=...&from=...&to=...`
- `GET /kpis/definitions`
- `GET /anomalies?status=open`
- `POST /anomalies/run`
- `POST /explain`
- `POST /explain/ask`
- `GET /governance/scorecard`
- `GET /governance/contracts`
- `POST /feedback`
- `POST /automations/register`
- `POST /automations/test`
- `GET /automations`
- `GET /automations/runs`

## Injected Deterministic Scenarios

- **Scenario A (SLA spike):** Birmingham + NorthHaul comm surge and SLA breaches.
- **Scenario B (Exceptions spike):** Coventry Partitions jobs spike in `Late materials` incidents.
- **Scenario C (Data quality issue):** Leeds duplicate jobs + missing delivered date over two days.

Scenario metadata is persisted in `scenario_registry`.

## KPI Set

Required KPIs are defined in `kpi_definitions` and computed into `fact_kpi_daily`:

- On-time delivery %
- SLA breach rate %
- Exception rate per 100 jobs
- Manual workload hours (weekly rollup)
- Cost leakage estimate (£)
- Data quality score (0-100)
- Automation impact (hours + GBP weekly and cumulative)
- Framework adoption proxy
- BI utilisation proxy
- Stakeholder satisfaction proxy

## Power BI Desktop (No Tenant Required)

Use exported CSVs from `data/mart/` and rebuild relationships in model view.

Full guide: `docs/powerbi_desktop_guide.md`

## 2-Minute Demo

Use `docs/demo_script.md`.

Flow:

1. Run ETL + API
2. Show KPI summary
3. Run anomalies
4. Explain Scenario A with SQL evidence
5. Trigger automation webhook
6. Show governance scorecard
7. Close with "measure -> explain -> act -> prove impact"

## Tests

```bash
pytest -q
```

Included tests:

- anomaly detection flags injected scenarios
- explanation ranking returns expected top drivers for Scenario A/B

## Screenshot Checklist (Paste in after running report)

- `[Screenshot: Power BI Exec KPI Board]`
- `[Screenshot: Scenario A anomaly + explanation API output]`
- `[Screenshot: Automation run log + webhook receiver event]`
- `[Screenshot: Governance scorecard endpoint output]`
- `[Screenshot: Power BI drill-through by site/carrier/product]`

## CV Bullet Suggestions (Copy/Paste)

- Built a local Operational Intelligence Copilot MVP combining dimensional BI modelling, anomaly detection, and explainable insight APIs (FastAPI + SQLite + Python).
- Designed and implemented a governed KPI metric store with ownership, thresholds, refresh cadence, and auditability across operational and financial metrics.
- Delivered evidence-grounded root-cause analytics (top driver decomposition + SQL evidence trace) and integrated webhook-triggered automations with run logs.
- Implemented enterprise governance controls: data contracts, quality scorecard, RBAC-lite, API usage telemetry, and stakeholder feedback capture.
- Produced Power BI Desktop-ready mart exports and DAX modelling guidance for no-tenant local analytics delivery.
