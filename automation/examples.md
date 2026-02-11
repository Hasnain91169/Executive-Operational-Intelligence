# Automation Webhook Examples

## Run Local Receiver

```bash
uvicorn automation.webhook_receiver:app --port 8010 --reload
```

Webhook URL used by automation rules:

```text
http://127.0.0.1:8010/webhook
```

## Register Automation Rule

```bash
curl -X POST http://127.0.0.1:8000/automations/register \
  -H "Content-Type: application/json" \
  -H "x-api-key: exec-local-key" \
  -d '{
    "name": "sla-spike-escalation",
    "trigger_kpi": "sla_breach_rate_pct",
    "condition_json": {
      "threshold": {"operator": ">", "value": 12},
      "anomaly_score": {"operator": ">", "value": 3.0},
      "segment_filters": {"site": "Birmingham", "carrier": "NorthHaul"}
    },
    "webhook_url": "http://127.0.0.1:8010/webhook",
    "enabled": true
  }'
```

## Trigger Automations by Re-running Anomaly Detection

```bash
curl -X POST http://127.0.0.1:8000/anomalies/run \
  -H "Content-Type: application/json" \
  -H "x-api-key: exec-local-key" \
  -d '{"threshold": 3.0, "window_days": 14}'
```

## Check Automation Runs

```bash
curl -H "x-api-key: exec-local-key" http://127.0.0.1:8000/automations/runs
```

## Check Receiver Events

```bash
curl http://127.0.0.1:8010/events
```

## n8n / Power Automate Conceptual Mapping

- **Trigger node**: HTTP Webhook listening on anomaly payload.
- **Filter node**: Condition on `kpi_name`, `anomaly_score`, and `scenario_tag`.
- **Action node**: Send Teams/Email, create ticket, or open planning task.
- **Audit**: Use API `GET /automations/runs` as run ledger.
