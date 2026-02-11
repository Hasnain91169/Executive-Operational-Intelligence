from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import json

from fastapi import FastAPI, Request

ROOT = Path(__file__).resolve().parents[1]
EVENT_LOG = ROOT / "automation" / "webhook_events.jsonl"

app = FastAPI(title="Local Webhook Receiver", version="0.1.0")
memory_events: list[dict] = []


@app.post("/webhook")
async def receive_webhook(request: Request) -> dict:
    payload = await request.json()
    event = {
        "received_at": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
    }
    memory_events.append(event)
    EVENT_LOG.parent.mkdir(parents=True, exist_ok=True)
    with EVENT_LOG.open("a", encoding="utf-8") as f:
        f.write(json.dumps(event) + "\n")
    return {"status": "received", "events_in_memory": len(memory_events)}


@app.get("/events")
def list_events() -> dict:
    return {"count": len(memory_events), "events": memory_events[-50:]}


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "event_log": str(EVENT_LOG)}
