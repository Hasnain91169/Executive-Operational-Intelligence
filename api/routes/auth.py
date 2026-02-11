from __future__ import annotations

import json
import os
from typing import Iterable

from fastapi import APIRouter, Header, HTTPException, Request, status

router = APIRouter(prefix="/auth", tags=["auth"])

DEFAULT_KEYS = {
    "exec-local-key": "exec",
    "ops-local-key": "ops",
    "finance-local-key": "finance",
}

OPS_ALLOWED_KPIS = {
    "on_time_delivery_pct",
    "sla_breach_rate_pct",
    "exception_rate_per_100_jobs",
    "manual_workload_hours_weekly",
    "data_quality_score",
    "framework_adoption_proxy_pct",
    "bi_utilisation_proxy_requests",
    "stakeholder_satisfaction_proxy_rating",
}

FINANCE_ALLOWED_KPIS = {
    "cost_leakage_estimate_gbp",
    "automation_impact_hours_weekly",
    "automation_impact_gbp_weekly",
    "automation_impact_gbp_cumulative",
    "data_quality_score",
    "framework_adoption_proxy_pct",
    "bi_utilisation_proxy_requests",
    "stakeholder_satisfaction_proxy_rating",
}


def resolve_api_key_map() -> dict[str, str]:
    raw = os.getenv("OPS_COPILOT_API_KEYS_JSON")
    if not raw:
        return DEFAULT_KEYS
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            return {str(k): str(v) for k, v in parsed.items()}
    except json.JSONDecodeError:
        pass
    return DEFAULT_KEYS


def role_from_api_key(api_key: str | None) -> str:
    if not api_key:
        return "anonymous"
    mapping = resolve_api_key_map()
    return mapping.get(api_key, "anonymous")


async def require_role(request: Request, x_api_key: str | None = Header(default=None)) -> str:
    role = role_from_api_key(x_api_key)
    request.state.role = role
    if role == "anonymous":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing x-api-key. Use exec-local-key, ops-local-key, or finance-local-key.",
        )
    return role


def enforce_roles(role: str, allowed: Iterable[str]) -> None:
    if role not in set(allowed):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Role not allowed for this endpoint")


def kpi_allowed_for_role(role: str, kpi_name: str) -> bool:
    if role == "exec":
        return True
    if role == "ops":
        return kpi_name in OPS_ALLOWED_KPIS
    if role == "finance":
        return kpi_name in FINANCE_ALLOWED_KPIS
    return False


@router.get("/whoami")
async def whoami(x_api_key: str | None = Header(default=None)) -> dict[str, str]:
    return {
        "resolved_role": role_from_api_key(x_api_key),
        "message": "Use x-api-key header (exec-local-key, ops-local-key, finance-local-key).",
    }
