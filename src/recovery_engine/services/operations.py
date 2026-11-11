"""Casos de uso: registrar, consultar y ejecutar el primer intento de una operación.

Los reintentos y la recuperación los hace el worker (``services/worker.py``); ambos
comparten el mismo procesador con lease y fencing (``services/processing.py``).
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Any

from recovery_engine.clock import Clock, IdFactory, new_operation_id, utc_now
from recovery_engine.domain.errors import (
    IdempotencyConflictError,
    OperationNotExecutableError,
    OperationNotFoundError,
)
from recovery_engine.domain.hashing import request_fingerprint
from recovery_engine.domain.models import (
    Attempt,
    Operation,
    OperationStatus,
    StatusTransition,
)
from recovery_engine.domain.retry import EqualJitter, Jitter, RetryPolicy
from recovery_engine.external.contracts import ExternalServiceClient, Scenario
from recovery_engine.persistence.db import transaction
from recovery_engine.persistence.repositories import (
    AttemptRepository,
    OperationRepository,
    TransitionRepository,
)
from recovery_engine.services.processing import OperationProcessor

# ``execute`` solo despacha el primer intento. Reintentos y recuperación: worker.
EXECUTABLE_STATUSES = frozenset({OperationStatus.PENDING})
API_LEASE_OWNER = "api"
DEFAULT_LEASE_TTL_SECONDS = 30.0


@dataclass(frozen=True, slots=True)
class CreateResult:
    operation: Operation
    created: bool


@dataclass(frozen=True, slots=True)
class OperationDetail:
    operation: Operation
    attempts: list[Attempt]
    transitions: list[StatusTransition]


@dataclass(frozen=True, slots=True)
class ExecutionResult:
    operation: Operation
    attempt: Attempt


class OperationService:
    def __init__(
        self,
        conn: sqlite3.Connection,
        external: ExternalServiceClient,
        *,
        clock: Clock = utc_now,
        id_factory: IdFactory = new_operation_id,
        policy: RetryPolicy | None = None,
        jitter: Jitter | None = None,
        lease_ttl_seconds: float = DEFAULT_LEASE_TTL_SECONDS,
    ) -> None:
        self._conn = conn
        self._external = external
        self._clock = clock
        self._id_factory = id_factory
        self._processor = OperationProcessor(
            conn,
            external,
            policy=policy or RetryPolicy(),
            clock=clock,
            jitter=jitter or EqualJitter(),
            lease_ttl_seconds=lease_ttl_seconds,
        )
        self._operations = OperationRepository(conn)
        self._attempts = AttemptRepository(conn)
        self._transitions = TransitionRepository(conn)

    # ------------------------------------------------------------- registro

    def create(
        self, *, idempotency_key: str, operation_type: str, payload: dict[str, Any]
    ) -> CreateResult:
        """Registra una operación de forma idempotente.

        - Clave nueva: crea la operación en ``pending``.
        - Clave existente con el mismo pedido: devuelve la operación existente.
        - Clave existente con otro pedido: :class:`IdempotencyConflictError`.

        ``BEGIN IMMEDIATE`` serializa a los escritores concurrentes, así que la
        lectura y la inserción no tienen carrera; ``UNIQUE(idempotency_key)`` actúa
        como garantía adicional a nivel de esquema.
        """
        request_hash = request_fingerprint(operation_type, payload)
        with transaction(self._conn):
            existing = self._operations.get_by_idempotency_key(idempotency_key)
            if existing is not None:
                if existing.request_hash != request_hash:
                    raise IdempotencyConflictError(idempotency_key, existing.id)
                return CreateResult(existing, created=False)

            now = self._clock()
            operation = Operation(
                id=self._id_factory(),
                idempotency_key=idempotency_key,
                request_hash=request_hash,
                operation_type=operation_type,
                payload=payload,
                status=OperationStatus.PENDING,
                attempt_count=0,
                external_effect_id=None,
                last_error=None,
                created_at=now,
                updated_at=now,
            )
            self._operations.insert(operation)
            self._transitions.record(operation.id, None, OperationStatus.PENDING, "created", now)
            return CreateResult(operation, created=True)

    # ------------------------------------------------------------- consulta

    def get(self, operation_id: str) -> Operation:
        return self._require(operation_id)

    def get_detail(self, operation_id: str) -> OperationDetail:
        return OperationDetail(
            operation=self._require(operation_id),
            attempts=self._attempts.list_for_operation(operation_id),
            transitions=self._transitions.list_for_operation(operation_id),
        )

    def list(self, status: OperationStatus | None = None, limit: int = 100) -> list[Operation]:
        return self._operations.list(status=status, limit=limit)

    # ------------------------------------------------------------ ejecución

    def execute(self, operation_id: str, scenario: Scenario | None = None) -> ExecutionResult:
        """Despacha el PRIMER intento de una operación ``pending``.

        Usa el mismo camino que el worker: claim con lease (``pending -> in_progress``),
        intento reservado y confirmado antes de la llamada externa, llamada fuera de toda
        transacción y registro con fencing. Si el proceso muere en medio, la operación
        queda ``in_progress`` con lease y el worker la recupera cuando el lease vence.
        """
        lease = self._processor.claim(operation_id, API_LEASE_OWNER, allowed=EXECUTABLE_STATUSES)
        if lease is None:
            current = self._require(operation_id)
            raise OperationNotExecutableError(current.id, current.status.value)
        result = self._processor.process(lease, scenario)
        attempt = result.submit_attempt
        assert attempt is not None  # una operación pending nunca requiere reconciliación
        return ExecutionResult(operation=result.operation, attempt=attempt)

    # ------------------------------------------------------------- internos

    def _require(self, operation_id: str) -> Operation:
        operation = self._operations.get(operation_id)
        if operation is None:
            raise OperationNotFoundError(operation_id)
        return operation
