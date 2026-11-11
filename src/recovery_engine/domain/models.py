"""Entidades y enumeraciones del dominio."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class OperationStatus(StrEnum):
    PENDING = "pending"
    """Registrada, todavía sin intentos."""
    IN_PROGRESS = "in_progress"
    """Reclamada por un dueño (lease). Puede tener un intento en vuelo."""
    SUCCEEDED = "succeeded"
    """El servicio externo confirmó el efecto. Terminal."""
    FAILED_RETRYABLE = "failed_retryable"
    """El servicio externo rechazó sin procesar (503/429). Reintento programado."""
    OUTCOME_UNKNOWN = "outcome_unknown"
    """No se sabe si el efecto ocurrió. Requiere reconciliación antes de reenviar."""
    FAILED_PERMANENT = "failed_permanent"
    """Rechazo definitivo. Terminal."""
    DEAD_LETTER = "dead_letter"
    """Recuperación automática agotada o insegura. Requiere intervención explícita."""


class AttemptOutcome(StrEnum):
    # Intentos de envío (kind = submit)
    SUCCESS = "success"
    RETRYABLE_ERROR = "retryable_error"
    RATE_LIMITED = "rate_limited"
    TIMEOUT = "timeout"
    PERMANENT_ERROR = "permanent_error"
    ABANDONED = "abandoned"
    """Intento cuyo resultado nunca se registró (caída / lease vencido)."""
    # Consultas de reconciliación (kind = reconcile)
    EFFECT_FOUND = "effect_found"
    EFFECT_NOT_FOUND = "effect_not_found"
    LOOKUP_FAILED = "lookup_failed"


class AttemptKind(StrEnum):
    SUBMIT = "submit"
    RECONCILE = "reconcile"


@dataclass(frozen=True, slots=True)
class Operation:
    id: str
    idempotency_key: str
    request_hash: str
    operation_type: str
    payload: dict[str, Any]
    status: OperationStatus
    attempt_count: int
    external_effect_id: str | None
    last_error: str | None
    created_at: str
    updated_at: str
    next_attempt_at: str | None = None
    lease_owner: str | None = None
    lease_expires_at: str | None = None
    lease_token: int = 0


@dataclass(frozen=True, slots=True)
class AttemptResult:
    """Resultado clasificado de una llamada al servicio externo."""

    outcome: AttemptOutcome
    http_status: int | None
    error_code: str | None = None
    error_message: str | None = None
    external_effect_id: str | None = None
    deduplicated_by_receiver: bool | None = None
    retry_after: str | None = None


@dataclass(frozen=True, slots=True)
class Attempt:
    id: int
    operation_id: str
    attempt_number: int
    scenario: str
    started_at: str
    outcome: AttemptOutcome | None = None
    http_status: int | None = None
    error_code: str | None = None
    error_message: str | None = None
    external_effect_id: str | None = None
    deduplicated_by_receiver: bool | None = None
    finished_at: str | None = None
    kind: AttemptKind = AttemptKind.SUBMIT
    lease_token: int | None = None
    worker_id: str | None = None
    retry_after: str | None = None
    retry_decision: str | None = None
    next_retry_at: str | None = None


@dataclass(frozen=True, slots=True)
class StatusTransition:
    operation_id: str
    from_status: OperationStatus | None
    to_status: OperationStatus
    reason: str
    at: str
    lease_token: int | None = None
    actor: str | None = None
