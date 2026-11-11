"""Máquina de estados de una operación.

Recuperación (Etapa 2):
- ``failed_retryable -> in_progress``: el worker reclama un reintento vencido.
- ``outcome_unknown -> in_progress``: el worker reclama para reconciliar (y reenviar con la
  misma clave solo si es seguro).
- ``in_progress -> outcome_unknown``: un lease vencido convierte el intento en incierto.
- ``in_progress -> dead_letter``: intentos agotados o resultado incierto sin garantías.
- ``dead_letter -> outcome_unknown``: reservado para un redrive manual (siempre vuelve a
  pasar por reconciliación). Ningún caso de uso de la Etapa 2 lo dispara.

Invariante: ``outcome_unknown`` nunca pasa directo a un estado de fallo.
"""

from __future__ import annotations

from recovery_engine.domain.errors import InvalidTransitionError
from recovery_engine.domain.models import AttemptOutcome, OperationStatus

S = OperationStatus

ALLOWED_TRANSITIONS: dict[OperationStatus, frozenset[OperationStatus]] = {
    S.PENDING: frozenset({S.IN_PROGRESS}),
    S.IN_PROGRESS: frozenset(
        {S.SUCCEEDED, S.FAILED_RETRYABLE, S.OUTCOME_UNKNOWN, S.FAILED_PERMANENT, S.DEAD_LETTER}
    ),
    S.FAILED_RETRYABLE: frozenset({S.IN_PROGRESS}),
    S.OUTCOME_UNKNOWN: frozenset({S.IN_PROGRESS, S.SUCCEEDED}),
    # Redrive manual reservado: no es terminal, pero nada lo dispara automáticamente.
    S.DEAD_LETTER: frozenset({S.OUTCOME_UNKNOWN}),
    S.SUCCEEDED: frozenset(),
    S.FAILED_PERMANENT: frozenset(),
}

TERMINAL_STATUSES: frozenset[OperationStatus] = frozenset(
    status for status, targets in ALLOWED_TRANSITIONS.items() if not targets
)

# Estados que el worker puede reclamar (si están vencidos).
RECOVERABLE_STATUSES: frozenset[OperationStatus] = frozenset(
    {S.FAILED_RETRYABLE, S.OUTCOME_UNKNOWN, S.IN_PROGRESS}
)

STATUS_AFTER_OUTCOME: dict[AttemptOutcome, OperationStatus] = {
    AttemptOutcome.SUCCESS: S.SUCCEEDED,
    AttemptOutcome.RETRYABLE_ERROR: S.FAILED_RETRYABLE,
    AttemptOutcome.RATE_LIMITED: S.FAILED_RETRYABLE,
    # Un timeout NO implica que la operación no ocurrió.
    AttemptOutcome.TIMEOUT: S.OUTCOME_UNKNOWN,
    AttemptOutcome.PERMANENT_ERROR: S.FAILED_PERMANENT,
}


def can_transition(source: OperationStatus, target: OperationStatus) -> bool:
    return target in ALLOWED_TRANSITIONS[source]


def ensure_transition(source: OperationStatus, target: OperationStatus) -> None:
    if not can_transition(source, target):
        raise InvalidTransitionError(source.value, target.value)


def status_after(outcome: AttemptOutcome) -> OperationStatus:
    return STATUS_AFTER_OUTCOME[outcome]
