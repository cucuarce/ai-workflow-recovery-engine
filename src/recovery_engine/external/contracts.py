"""Contrato entre el motor y un servicio externo.

El motor depende solo de este contrato. Hoy lo implementa el simulador en proceso;
más adelante puede implementarlo un cliente HTTP (o un webhook de n8n) sin tocar el servicio.

Capacidades opcionales que habilitan la recuperación segura de resultados inciertos:
- ``lookup(idempotency_key)``: consulta el estado persistido del proveedor (reconciliación).
- ``supports_idempotency``: el receptor deduplica por clave, así que un reenvío con la
  misma clave no puede duplicar el efecto. Si no se declara, se asume ``False``.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol


class Scenario(StrEnum):
    """Inyección de fallos determinística (equivalente a un header de fault injection)."""

    SUCCESS = "success"
    HTTP_503 = "http_503"
    HTTP_429 = "http_429"
    TIMEOUT_BEFORE_PROCESSING = "timeout_before_processing"
    TIMEOUT_AFTER_PROCESSING = "timeout_after_processing"
    PERMANENT_ERROR = "permanent_error"


@dataclass(frozen=True, slots=True)
class ExternalRequest:
    idempotency_key: str
    operation_ref: str
    operation_type: str
    payload: dict[str, Any]
    scenario: Scenario | None = None
    """Fallo forzado. ``None``: comportamiento por defecto del proveedor."""


@dataclass(frozen=True, slots=True)
class ExternalResponse:
    status_code: int
    effect_id: str | None = None
    deduplicated: bool = False
    error_code: str | None = None
    message: str | None = None
    retry_after_seconds: int | None = None
    retry_after: str | None = None
    """Valor crudo del header Retry-After (segundos o HTTP-date), si vino."""


@dataclass(frozen=True, slots=True)
class ExternalLookup:
    found: bool
    effect_id: str | None = None


class ExternalTimeoutError(Exception):
    """El cliente no recibió respuesta. NO implica que la operación no se haya procesado."""


class ExternalServiceClient(Protocol):
    def submit(self, request: ExternalRequest) -> ExternalResponse:
        """Envía el pedido. Lanza :class:`ExternalTimeoutError` si no hay respuesta."""
        ...


class ReconcilableServiceClient(ExternalServiceClient, Protocol):
    supports_idempotency: bool

    def lookup(self, idempotency_key: str) -> ExternalLookup:
        """Estado persistido del proveedor para la clave. Puede lanzar ExternalTimeoutError."""
        ...
