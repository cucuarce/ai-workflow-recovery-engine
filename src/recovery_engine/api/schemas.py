"""Contratos HTTP (Pydantic)."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from recovery_engine.domain.models import AttemptKind, AttemptOutcome, OperationStatus
from recovery_engine.external.contracts import Scenario


class OperationCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    operation_type: str = Field(min_length=1, max_length=100, examples=["issue_invoice"])
    payload: dict[str, Any] = Field(
        default_factory=dict, examples=[{"customer_id": "c-001", "amount_cents": 15000}]
    )


class ExecuteRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    scenario: Scenario | None = Field(
        default=None,
        description="Fallo forzado para este intento. Sin valor: plan de fallos del simulador "
        "o éxito.",
    )


class _FromDomain(BaseModel):
    model_config = ConfigDict(from_attributes=True)


class OperationOut(_FromDomain):
    id: str
    idempotency_key: str
    operation_type: str
    payload: dict[str, Any]
    status: OperationStatus
    attempt_count: int
    external_effect_id: str | None
    last_error: str | None
    created_at: str
    updated_at: str
    next_attempt_at: str | None
    lease_owner: str | None
    lease_expires_at: str | None
    lease_token: int


class AttemptOut(_FromDomain):
    id: int
    attempt_number: int
    scenario: str
    outcome: AttemptOutcome | None
    http_status: int | None
    error_code: str | None
    error_message: str | None
    external_effect_id: str | None
    deduplicated_by_receiver: bool | None
    started_at: str
    finished_at: str | None
    kind: AttemptKind
    lease_token: int | None
    worker_id: str | None
    retry_after: str | None
    retry_decision: str | None
    next_retry_at: str | None


class TransitionOut(_FromDomain):
    from_status: OperationStatus | None
    to_status: OperationStatus
    reason: str
    at: str
    lease_token: int | None
    actor: str | None


class OperationDetailOut(OperationOut):
    attempts: list[AttemptOut]
    transitions: list[TransitionOut]


class ExecutionOut(BaseModel):
    operation: OperationOut
    attempt: AttemptOut


class ErrorOut(BaseModel):
    error: str
    detail: str
