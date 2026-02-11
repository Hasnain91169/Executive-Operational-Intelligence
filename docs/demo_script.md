# 2-Minute Demo Script

## 0) Prep (before audience joins)

```bash
pip install -r requirements.txt
python etl/generate_sample_data.py
python etl/transform.py
python etl/load_mart.py
uvicorn api.main:app --reload
```

Optional webhook sink in second terminal:

```bash
uvicorn automation.webhook_receiver:app --port 8010 --reload
```

## 1) KPI Summary (Exec Board)

```bash
curl -H "x-api-key: exec-local-key" "http://127.0.0.1:8000/kpis/summary?from=2025-10-15&to=2026-02-10"
```

Talk track: "This is the daily KPI layer built from a governed metric store, ready for board views."

## 2) Show Detected Anomalies

```bash
curl -X POST http://127.0.0.1:8000/anomalies/run \
  -H "Content-Type: application/json" \
  -H "x-api-key: exec-local-key" \
  -d '{"threshold": 3.0, "window_days": 14}'

curl -H "x-api-key: exec-local-key" "http://127.0.0.1:8000/anomalies?status=open"
```

Talk track: "Median/MAD catches SLA, exception, and data quality shocks without manual tuning."

## 3) Explain Scenario A (Grounded Root Cause)

First, get Scenario A date from registry:

```bash
sqlite3 data/mart/ops_copilot.db "select scenario_date from scenario_registry where scenario_tag='Scenario A';"
```

Then explain:

```bash
curl -X POST http://127.0.0.1:8000/explain \
  -H "Content-Type: application/json" \
  -H "x-api-key: exec-local-key" \
  -d '{"kpi_name":"sla_breach_rate_pct","date":"<SCENARIO_A_DATE>"}'
```

Talk track: "Top drivers, deltas, contribution shares, and exact SQL evidence are returned; every explain call is audited."

## 4) Trigger Automation (Act)

Register:

```bash
curl -X POST http://127.0.0.1:8000/automations/register \
  -H "Content-Type: application/json" \
  -H "x-api-key: exec-local-key" \
  -d '{
    "name":"sla-spike-escalation",
    "trigger_kpi":"sla_breach_rate_pct",
    "condition_json":{"threshold":{"operator":">","value":12},"anomaly_score":{"operator":">","value":3.0}},
    "webhook_url":"http://127.0.0.1:8010/webhook",
    "enabled":true
  }'
```

Re-run anomalies and inspect runs:

```bash
curl -X POST http://127.0.0.1:8000/anomalies/run -H "Content-Type: application/json" -H "x-api-key: exec-local-key" -d '{}'
curl -H "x-api-key: exec-local-key" http://127.0.0.1:8000/automations/runs
```

## 5) Governance Scorecard (Prove)

```bash
curl -H "x-api-key: exec-local-key" http://127.0.0.1:8000/governance/scorecard
curl -H "x-api-key: exec-local-key" http://127.0.0.1:8000/governance/contracts
```

Talk track: "This closes the loop from measure -> explain -> act -> prove impact with governance evidence."

## Closing Line

"Operational Intelligence Copilot turns BI from reporting into an operating system: **measure -> explain -> act -> prove impact**."
