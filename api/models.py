from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, HttpUrl, field_validator


class ExplainRequest(BaseModel):
    kpi_name: str = Field(..., min_length=2)
    date: str = Field(..., description="YYYY-MM-DD")


class AskRequest(BaseModel):
    question: str = Field(..., min_length=3)
    kpi_name: str | None = None
    date: str | None = Field(default=None, description="YYYY-MM-DD")


class FeedbackRequest(BaseModel):
    audit_id: int
    rating: int = Field(..., ge=1, le=5)
    notes: str | None = None


class AutomationRegisterRequest(BaseModel):
    name: str = Field(..., min_length=2)
    trigger_kpi: str = Field(..., min_length=2)
    condition_json: dict[str, Any]
    webhook_url: HttpUrl
    enabled: bool = True


class AutomationTestRequest(BaseModel):
    name: str = Field(default="test-automation")
    webhook_url: HttpUrl
    payload: dict[str, Any] | None = None


class AutomationToggleRequest(BaseModel):
    enabled: bool


class AnomalyRunRequest(BaseModel):
    threshold: float = Field(default=3.0, ge=0.1)
    window_days: int = Field(default=14, ge=7, le=60)

    @field_validator("threshold")
    @classmethod
    def validate_threshold(cls, v: float) -> float:
        return round(v, 4)
