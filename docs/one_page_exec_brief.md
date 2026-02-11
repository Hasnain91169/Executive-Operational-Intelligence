# One-Page Executive Brief

## What This Tool Is

Operational Intelligence Copilot is a local BI + AI operating layer for manufacturing/project-delivery operations. It turns fragmented operational data into governed KPIs, anomaly alerts, evidence-based root-cause analysis, and automated follow-up actions.

## KPIs Tracked

- On-time delivery %
- SLA breach rate %
- Exception rate per 100 jobs
- Manual workload hours (weekly)
- Cost leakage estimate (GBP)
- Data quality score (0-100)
- Automation impact (hours and GBP weekly + cumulative)
- Framework adoption proxy
- BI utilisation proxy
- Stakeholder satisfaction proxy

## How Anomalies Are Handled

- Daily KPI values are evaluated using a robust rolling baseline (median + MAD).
- Score threshold: anomaly if score > 3.0.
- Anomalies are stored with `kpi_name`, `date`, `value`, `baseline`, `score`, `status`, and `scenario_tag`.
- Explain API compares anomaly date vs previous 14 days and returns top drivers with SQL evidence.
- Attribution note is included because cross-dimensional driver slices overlap.

## How Governance Works

- Data contracts enforce required columns and type checks before mart load.
- Data quality checks cover completeness, freshness, duplicates, key nulls, schema drift, and out-of-range values.
- KPI definitions are governed in a metric store with owners, thresholds, cadence, and business value.
- Auditability:
  - Explanation audit log with SQL used and slices returned
  - API usage logs for BI utilisation proxy
  - Feedback log for stakeholder satisfaction proxy
- RBAC-lite roles via API key: `exec`, `ops`, `finance`.

## What We Automated

- Automation rules are registered against KPI/anomaly conditions.
- When anomalies are detected, matching rules trigger webhooks (n8n/Power Automate equivalent).
- Each trigger is logged with payload, response code, and status in `automation_runs`.

## 30/60/90-Day Rollout Plan

### 30 Days

- Productionize daily ETL schedule and anomaly refresh.
- Baseline KPI ownership with named accountable roles.
- Stand up exec/ops/finance Power BI Desktop templates.

### 60 Days

- Integrate webhook actions with ticketing + collaboration channels.
- Add SLA for insight delivery and feedback capture loop.
- Expand data quality checks with source-system freshness monitors.

### 90 Days

- Introduce controlled self-serve insight queries for business teams.
- Benchmark automation savings vs baseline run-rate.
- Formalize monthly governance review: KPI quality, utilisation, and business impact.
