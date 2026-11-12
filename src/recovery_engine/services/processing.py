"""Procesamiento de operaciones: lease, fencing, reintentos y reconciliación.

Flujo de una operación reclamada (todas las transacciones son cortas y ninguna
abarca una llamada externa):

1. ``claim`` (tx): toma un lease ``(owner, expires_at)`` e incrementa ``lease_token``.
   Si la operación estaba ``in_progress`` con lease vencido, cierra sus intentos abiertos
   como ``abandoned`` y la pasa por ``outcome_unknown`` antes de reclamarla.
2. Si el resultado previo es incierto: ``lookup`` al proveedor (fuera de tx) y registro (tx).
   - efecto encontrado -> ``succeeded`` sin reenviar;
   - no encontrado -> reenvío con la MISMA clave solo si el receptor garantiza idempotencia
     y quedan intentos; si no, ``dead_letter`` (requiere intervención).
3. Reserva del envío (tx): ``attempt_count + 1`` e intento abierto.
4. Envío (fuera de tx).
5. Registro del resultado (tx) con fencing: solo se aplica si ``lease_token`` sigue igual.

Fencing: cada escritura posterior al claim exige ``status = in_progress AND
lease_token = <token del claim>``. Un worker cuyo lease venció y fue re-reclamado por otro
no puede sobrescribir nada: su transacción falla con :class:`LeaseLostError` y se revierte.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, replace
from typing import Any

from recovery_engine.clock import Clock, add_seconds, normalize_timestamp, parse_timestamp
from recovery_engine.domain.errors import (
    ConcurrentModificationError,
    LeaseLostError,
    OperationNotFoundError,
)
from recovery_engine.domain.models import (
    Attempt,
    AttemptKind,
    AttemptOutcome,
    AttemptResult,
    Operation,
    OperationStatus,
)
from recovery_engine.domain.outcomes import classify_http_status
from recovery_engine.domain.retry import Jitter, RetryPolicy, backoff_delay, decide_retry
from recovery_engine.domain.state_machine import ensure_transition
from recovery_engine.external.contracts import (
    ExternalRequest,
    ExternalServiceClient,
    ExternalTimeoutError,
    Scenario,
)
from recovery_engine.persistence.db import transaction
from recovery_engine.persistence.repositories import (
    AttemptRepository,
    OperationRepository,
    TransitionRepository,
)

S = OperationStatus

# Estados desde los que se reclama sin saber si el proveedor aplicó el efecto.
UNCERTAIN_STATUSES = frozenset({S.OUTCOME_UNKNOWN, S.IN_PROGRESS})
DEFAULT_SCENARIO_LABEL = "provider_default"


@dataclass(frozen=True, slots=True)
class Lease:
    operation: Operation
    token: int
    owner: str
    claimed_from: OperationStatus

    @property
    def needs_reconciliation(self) -> bool:
        return self.claimed_from in UNCERTAIN_STATUSES


@dataclass(frozen=True, slots=True)
class ProcessingResult:
    operation: Operation
    attempts: list[Attempt]
    """Intentos creados en este procesamiento (reconcile y/o submit), en orden."""
    action: str

    @property
    def submit_attempt(self) -> Attempt | None:
        submits = [a for a in self.attempts if a.kind is AttemptKind.SUBMIT]
        return submits[-1] if submits else None


@dataclass(frozen=True, slots=True)
class _LookupOutcome:
    outcome: AttemptOutcome
    effect_id: str | None
    detail: str


class OperationProcessor:
    def __init__(
        self,
        conn: sqlite3.Connection,
        external: ExternalServiceClient,
        *,
        policy: RetryPolicy,
        clock: Clock,
        jitter: Jitter,
        lease_ttl_seconds: float,
    ) -> None:
        if lease_ttl_seconds <= 0:
            raise ValueError("lease_ttl_seconds debe ser > 0")
        self._conn = conn
        self._external = external
        self._policy = policy
        self._clock = clock
        self._jitter = jitter
        self._lease_ttl = lease_ttl_seconds
        self._operations = OperationRepository(conn)
        self._attempts = AttemptRepository(conn)
        self._transitions = TransitionRepository(conn)

    # ---------------------------------------------------------------- claim

    def claim(
        self,
        operation_id: str,
        owner: str,
        *,
        allowed: frozenset[OperationStatus] | None = None,
    ) -> Lease | None:
        """Reclama una operación concreta si está vencida (y en ``allowed``, si se indica)."""
        with transaction(self._conn):
            operation = self._operations.get(operation_id)
            if operation is None:
                raise OperationNotFoundError(operation_id)
            return self._claim_locked(operation, owner, allowed)

    def claim_next(
        self, owner: str, exclude: frozenset[str] | set[str] = frozenset()
    ) -> Lease | None:
        """Reclama la próxima operación vencida en orden determinístico (created_at, id)."""
        with transaction(self._conn):
            now = normalize_timestamp(self._clock())
            for operation_id in self._operations.find_claimable_ids(now, 1, exclude):
                operation = self._operations.get(operation_id)
                assert operation is not None
                return self._claim_locked(operation, owner, None)
        return None

    def _is_due(self, operation: Operation, now: str) -> bool:
        if operation.status in (S.PENDING, S.FAILED_RETRYABLE, S.OUTCOME_UNKNOWN):
            return operation.next_attempt_at is None or operation.next_attempt_at <= now
        if operation.status is S.IN_PROGRESS:
            return operation.lease_expires_at is None or operation.lease_expires_at <= now
        return False

    def _claim_locked(
        self,
        operation: Operation,
        owner: str,
        allowed: frozenset[OperationStatus] | None,
    ) -> Lease | None:
        now_raw = self._clock()
        now = normalize_timestamp(now_raw)
        if allowed is not None and operation.status not in allowed:
            return None
        if not self._is_due(operation, now):
            return None

        claimed_from = operation.status
        if operation.status is S.IN_PROGRESS:
            # Lease vencido: el dueño anterior pudo haber enviado (o no) el pedido.
            reason = f"lease expired (owner={operation.lease_owner}, token={operation.lease_token})"
            abandoned = self._attempts.abandon_open(operation.id, now_raw, reason)
            self._move(
                operation.id,
                S.IN_PROGRESS,
                S.OUTCOME_UNKNOWN,
                now_raw,
                reason=f"{reason}; {abandoned} open attempt(s) abandoned",
                fields={"lease_owner": None, "lease_expires_at": None},
                expected_token=operation.lease_token,
                actor=owner,
            )
            operation = replace(operation, status=S.OUTCOME_UNKNOWN)

        token = operation.lease_token + 1
        expires_at = add_seconds(now, self._lease_ttl)
        self._move(
            operation.id,
            operation.status,
            S.IN_PROGRESS,
            now_raw,
            reason=f"claimed by {owner} (lease token {token}, expires {expires_at})",
            fields={"lease_owner": owner, "lease_expires_at": expires_at, "next_attempt_at": None},
            expected_token=operation.lease_token,
            bump_token=True,
            actor=owner,
            transition_token=token,
        )
        leased = replace(
            operation,
            status=S.IN_PROGRESS,
            lease_owner=owner,
            lease_expires_at=expires_at,
            lease_token=token,
            next_attempt_at=None,
            updated_at=now_raw,
        )
        return Lease(operation=leased, token=token, owner=owner, claimed_from=claimed_from)

    # -------------------------------------------------------------- process

    def process(self, lease: Lease, scenario: Scenario | None = None) -> ProcessingResult:
        created: list[Attempt] = []

        if lease.needs_reconciliation:
            lookup = self._lookup(lease.operation)  # fuera de transacción
            with transaction(self._conn):
                operation = self._ensure_lease(lease)
                created.append(self._record_lookup(lease, lookup))
                if lookup.outcome is AttemptOutcome.EFFECT_FOUND:
                    final = self._finish(
                        lease,
                        S.SUCCEEDED,
                        reason=f"reconciled: provider has effect {lookup.effect_id}",
                        fields={"external_effect_id": lookup.effect_id, "last_error": None},
                    )
                    return ProcessingResult(final, created, "reconciled")
                blocker = self._resend_blocker(operation, lookup)
                if blocker is not None:
                    final = self._finish(
                        lease, S.DEAD_LETTER, reason=blocker, fields={"last_error": blocker}
                    )
                    return ProcessingResult(final, created, "dead_lettered")
                submit = self._start_submit(lease, operation, scenario)
        else:
            with transaction(self._conn):
                operation = self._ensure_lease(lease)
                if operation.attempt_count >= self._policy.max_attempts:
                    reason = f"max_attempts_exhausted ({operation.attempt_count})"
                    final = self._finish(
                        lease, S.DEAD_LETTER, reason=reason, fields={"last_error": reason}
                    )
                    return ProcessingResult(final, created, "dead_lettered")
                submit = self._start_submit(lease, operation, scenario)
        created.append(submit)

        result = self._call_external(lease.operation, scenario)  # fuera de transacción

        with transaction(self._conn):
            final, closed = self._record_submit(lease, submit, result)
        created[-1] = closed
        return ProcessingResult(final, created, f"submitted:{result.outcome.value}")

    # ------------------------------------------------------- reconciliación

    def _lookup(self, operation: Operation) -> _LookupOutcome:
        lookup = getattr(self._external, "lookup", None)
        if lookup is None:
            return _LookupOutcome(AttemptOutcome.LOOKUP_FAILED, None, "lookup_unsupported")
        try:
            found = lookup(operation.id)
        except ExternalTimeoutError as exc:
            return _LookupOutcome(AttemptOutcome.LOOKUP_FAILED, None, f"lookup_timeout: {exc}")
        if found.found:
            return _LookupOutcome(AttemptOutcome.EFFECT_FOUND, found.effect_id, "effect_found")
        return _LookupOutcome(AttemptOutcome.EFFECT_NOT_FOUND, None, "effect_not_found")

    def _record_lookup(self, lease: Lease, lookup: _LookupOutcome) -> Attempt:
        now = self._clock()
        attempt = self._attempts.start(
            lease.operation.id,
            self._attempts.next_attempt_number(lease.operation.id),
            "reconciliation",
            now,
            kind=AttemptKind.RECONCILE,
            lease_token=lease.token,
            worker_id=lease.owner,
        )
        self._attempts.finish(
            attempt.id,
            AttemptResult(
                outcome=lookup.outcome,
                http_status=None,
                error_code=None if lookup.effect_id else lookup.detail,
                external_effect_id=lookup.effect_id,
            ),
            now,
        )
        stored = self._attempts.get(attempt.id)
        assert stored is not None
        return stored

    def _resend_blocker(self, operation: Operation, lookup: _LookupOutcome) -> str | None:
        """Motivo por el que NO es seguro/posible reenviar tras un resultado incierto."""
        if operation.attempt_count >= self._policy.max_attempts:
            return (
                f"max_attempts_exhausted ({operation.attempt_count}); "
                f"outcome unresolved after reconciliation ({lookup.detail})"
            )
        if not getattr(self._external, "supports_idempotency", False):
            return (
                "uncertain_outcome_requires_manual_reconciliation: receiver does not guarantee "
                f"idempotency and reconciliation was inconclusive ({lookup.detail})"
            )
        return None

    # --------------------------------------------------------------- envío

    def _start_submit(
        self, lease: Lease, operation: Operation, scenario: Scenario | None
    ) -> Attempt:
        now = self._clock()
        count = operation.attempt_count + 1
        if not self._operations.update_leased(
            operation.id, token=lease.token, updated_at=now, fields={"attempt_count": count}
        ):
            raise LeaseLostError(operation.id, lease.token)
        return self._attempts.start(
            operation.id,
            self._attempts.next_attempt_number(operation.id),
            scenario.value if scenario is not None else DEFAULT_SCENARIO_LABEL,
            now,
            kind=AttemptKind.SUBMIT,
            lease_token=lease.token,
            worker_id=lease.owner,
        )

    def _call_external(self, operation: Operation, scenario: Scenario | None) -> AttemptResult:
        request = ExternalRequest(
            # Clave estable por operación lógica: TODOS los envíos usan la misma.
            idempotency_key=operation.id,
            operation_ref=operation.id,
            operation_type=operation.operation_type,
            payload=operation.payload,
            scenario=scenario,
            client_reference=operation.idempotency_key,
        )
        try:
            response = self._external.submit(request)
        except ExternalTimeoutError as exc:
            return AttemptResult(
                outcome=AttemptOutcome.TIMEOUT,
                http_status=None,
                error_code="timeout",
                error_message=str(exc),
            )
        retry_after = response.retry_after
        if retry_after is None and response.retry_after_seconds is not None:
            retry_after = str(response.retry_after_seconds)
        return AttemptResult(
            outcome=classify_http_status(response.status_code),
            http_status=response.status_code,
            error_code=response.error_code,
            error_message=response.message,
            external_effect_id=response.effect_id,
            deduplicated_by_receiver=response.deduplicated,
            retry_after=retry_after,
        )

    def _record_submit(
        self, lease: Lease, attempt: Attempt, result: AttemptResult
    ) -> tuple[Operation, Attempt]:
        now_raw = self._clock()
        now = normalize_timestamp(now_raw)
        operation = self._ensure_lease(lease)
        attempts_made = operation.attempt_count
        label = f"attempt {attempt.attempt_number}: {result.outcome.value}"
        next_at: str | None = None
        fields: dict[str, Any]

        if result.outcome is AttemptOutcome.SUCCESS:
            target, decision = S.SUCCEEDED, "completed"
            fields = {"external_effect_id": result.external_effect_id, "last_error": None}
        elif result.outcome is AttemptOutcome.PERMANENT_ERROR:
            target, decision = S.FAILED_PERMANENT, "no_retry: permanent_error"
            fields = {"last_error": f"{result.error_code}: {result.error_message}"}
        elif result.outcome is AttemptOutcome.TIMEOUT:
            # No sabemos si ocurrió: se programa una reconciliación, nunca un reenvío ciego.
            delay = backoff_delay(attempts_made, self._policy, self._jitter)
            next_at = add_seconds(now, delay)
            target, decision = S.OUTCOME_UNKNOWN, f"reconcile in {delay:.3f}s (outcome unknown)"
            fields = {
                "last_error": f"{result.error_code}: {result.error_message}",
                "next_attempt_at": next_at,
            }
        else:
            retry = decide_retry(
                result.outcome,
                attempts_made=attempts_made,
                policy=self._policy,
                now=parse_timestamp(now),
                retry_after=result.retry_after,
                jitter=self._jitter,
            )
            decision = retry.describe()
            error = f"{result.error_code}: {result.error_message}"
            if retry.retry:
                assert retry.delay_seconds is not None
                next_at = add_seconds(now, retry.delay_seconds)
                target = S.FAILED_RETRYABLE
                fields = {"last_error": error, "next_attempt_at": next_at}
            else:
                target = S.DEAD_LETTER
                fields = {"last_error": f"{error} ({retry.reason})"}

        final = self._finish(lease, target, reason=f"{label}; {decision}", fields=fields)
        if not self._attempts.finish(
            attempt.id, result, now_raw, retry_decision=decision, next_retry_at=next_at
        ):
            # El intento fue cerrado por quien reclamó después: nuestro resultado es obsoleto.
            raise LeaseLostError(lease.operation.id, lease.token)
        closed = self._attempts.get(attempt.id)
        assert closed is not None
        return final, closed

    # ------------------------------------------------------------- internos

    def _ensure_lease(self, lease: Lease) -> Operation:
        operation = self._operations.get(lease.operation.id)
        if (
            operation is None
            or operation.status is not S.IN_PROGRESS
            or operation.lease_token != lease.token
        ):
            raise LeaseLostError(lease.operation.id, lease.token)
        return operation

    def _finish(
        self,
        lease: Lease,
        target: OperationStatus,
        *,
        reason: str,
        fields: dict[str, Any],
    ) -> Operation:
        """Transición final con fencing y liberación del lease."""
        now = self._clock()
        self._move(
            lease.operation.id,
            S.IN_PROGRESS,
            target,
            now,
            reason=reason,
            fields={**fields, "lease_owner": None, "lease_expires_at": None},
            expected_token=lease.token,
            actor=lease.owner,
        )
        final = self._operations.get(lease.operation.id)
        assert final is not None
        return final

    def _move(
        self,
        operation_id: str,
        source: OperationStatus,
        target: OperationStatus,
        now: str,
        *,
        reason: str,
        fields: dict[str, Any] | None = None,
        expected_token: int | None = None,
        bump_token: bool = False,
        actor: str | None = None,
        transition_token: int | None = None,
    ) -> None:
        ensure_transition(source, target)
        applied = self._operations.compare_and_set_status(
            operation_id,
            expected=source,
            new=target,
            updated_at=now,
            fields=fields,
            expected_token=expected_token,
            bump_token=bump_token,
        )
        if not applied:
            if source is S.IN_PROGRESS and expected_token is not None:
                raise LeaseLostError(operation_id, expected_token)
            raise ConcurrentModificationError(operation_id, source.value)
        self._transitions.record(
            operation_id,
            source,
            target,
            reason,
            now,
            lease_token=transition_token if transition_token is not None else expected_token,
            actor=actor,
        )
