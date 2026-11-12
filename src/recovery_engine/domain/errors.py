"""Errores de dominio. La capa API los traduce a códigos HTTP."""

from __future__ import annotations


class DomainError(Exception):
    code = "domain_error"


class OperationNotFoundError(DomainError):
    code = "operation_not_found"

    def __init__(self, operation_id: str) -> None:
        super().__init__(f"Operación {operation_id} no encontrada")
        self.operation_id = operation_id


class IdempotencyConflictError(DomainError):
    code = "idempotency_conflict"

    def __init__(self, idempotency_key: str, existing_operation_id: str) -> None:
        super().__init__(
            f"La Idempotency-Key '{idempotency_key}' ya fue usada con un payload distinto "
            f"(operación {existing_operation_id})"
        )
        self.idempotency_key = idempotency_key
        self.existing_operation_id = existing_operation_id


class InvalidTransitionError(DomainError):
    code = "invalid_transition"

    def __init__(self, from_status: str, to_status: str) -> None:
        super().__init__(f"Transición no permitida: {from_status} -> {to_status}")
        self.from_status = from_status
        self.to_status = to_status


class OperationNotExecutableError(DomainError):
    code = "operation_not_executable"

    def __init__(self, operation_id: str, status: str) -> None:
        super().__init__(
            f"La operación {operation_id} está en estado '{status}' y no puede ejecutarse "
            "(execute solo despacha el primer intento de operaciones 'pending'; "
            "los reintentos y la recuperación los realiza el worker)"
        )
        self.operation_id = operation_id
        self.status = status


class ConcurrentModificationError(DomainError):
    code = "concurrent_modification"

    def __init__(self, operation_id: str, expected_status: str) -> None:
        super().__init__(
            f"La operación {operation_id} cambió de estado concurrentemente "
            f"(se esperaba '{expected_status}')"
        )
        self.operation_id = operation_id


class FaultInjectionDisabledError(DomainError):
    """Se pidió un escenario de fallo en un entorno que no permite elegirlo en el pedido."""

    code = "fault_injection_disabled"

    def __init__(self) -> None:
        super().__init__(
            "Este entorno no acepta 'scenario' en execute. En demo, los fallos se configuran "
            "antes con PUT /simulator/fault-plans (token administrativo)"
        )


class LeaseLostError(DomainError):
    """Fencing: otro dueño reclamó la operación (el token de lease ya no coincide)."""

    code = "lease_lost"

    def __init__(self, operation_id: str, token: int) -> None:
        super().__init__(
            f"El lease con token {token} sobre la operación {operation_id} ya no es válido; "
            "la escritura se descartó"
        )
        self.operation_id = operation_id
        self.token = token
