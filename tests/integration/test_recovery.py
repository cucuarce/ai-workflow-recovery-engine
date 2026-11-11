"""Etapa 2: reintentos, reconciliación, lease/fencing y dead-letter.

Todos los tests son determinísticos: el reloj solo avanza con ``clock.advance`` (sin
sleeps), el jitter está desactivado y el proveedor es el simulador en proceso.
"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import timedelta
from email.utils import format_datetime

import pytest

from recovery_engine.clock import add_seconds
from recovery_engine.domain.errors import IdempotencyConflictError, LeaseLostError
from recovery_engine.domain.models import AttemptKind, AttemptOutcome, OperationStatus
from recovery_engine.domain.retry import RetryPolicy, no_jitter
from recovery_engine.external.contracts import Scenario
from recovery_engine.external.simulator import ExternalServiceSimulator, FaultSpec
from recovery_engine.services.operations import OperationService
from support import (
    LEASE_TTL,
    POLICY,
    CrashingClient,
    HookClient,
    SimulatedCrash,
    SubmitOnlyClient,
)

S = OperationStatus
O = AttemptOutcome  # noqa: E741


# ----------------------------------------------------------------- helpers


def submits(detail):
    return [a for a in detail.attempts if a.kind is AttemptKind.SUBMIT]


def outcomes(detail):
    return [(a.kind.value, a.outcome) for a in detail.attempts]


def assert_single_effect(simulator, operation_id):
    """Un solo efecto de negocio y TODOS los envíos con la misma clave externa."""
    effects = simulator.list_effects(operation_id)
    assert len(effects) == 1, effects
    assert simulator.find_duplicate_effects() == []
    keys = {r.idempotency_key for r in simulator.list_requests(operation_id)}
    assert keys == {operation_id}
    return effects[0]


def plan(simulator, operation_id, *scenarios, retry_after=None):
    simulator.set_fault_plan(
        operation_id,
        [FaultSpec(s, retry_after if s is Scenario.HTTP_429 else None) for s in scenarios],
    )


# ------------------------------------------------- 1. 503 y recuperación


def test_503_then_successful_recovery(service, new_operation, simulator, make_worker, clock):
    op = new_operation()
    plan(simulator, op.id, Scenario.HTTP_503)
    worker = make_worker()

    first = worker.run_once()
    assert [(i.claimed_from, i.final_status) for i in first.items] == [
        ("pending", "failed_retryable")
    ]
    scheduled = service.get(op.id)
    assert scheduled.next_attempt_at == add_seconds(clock(), 2.0)
    assert scheduled.lease_owner is None
    assert scheduled.lease_expires_at is None

    # El backoff se respeta sin dormir: el reloj no avanzó lo suficiente.
    assert worker.run_once().processed == 0
    clock.advance(1.999)
    assert worker.run_once().processed == 0
    clock.advance(0.001)

    second = worker.run_once()
    assert [i.final_status for i in second.items] == ["succeeded"]
    detail = service.get_detail(op.id)
    failed, ok = submits(detail)
    assert failed.outcome is O.RETRYABLE_ERROR
    assert failed.http_status == 503
    assert failed.retry_decision == "retry in 2.000s (exponential_backoff)"
    assert failed.next_retry_at == scheduled.next_attempt_at
    assert ok.outcome is O.SUCCESS
    assert detail.operation.attempt_count == 2
    effect = assert_single_effect(simulator, op.id)
    assert detail.operation.external_effect_id == effect.effect_id


def test_backoff_is_exponential(service, new_operation, simulator, make_worker, clock):
    op = new_operation()
    plan(simulator, op.id, Scenario.HTTP_503, Scenario.HTTP_503, Scenario.HTTP_503)
    worker = make_worker()

    delays = []
    worker.run_once()
    for _ in range(3):
        current = service.get(op.id)
        delays.append(current.next_attempt_at)
        clock.advance(60)
        worker.run_once()

    decisions = [a.retry_decision for a in submits(service.get_detail(op.id))]
    assert decisions == [
        "retry in 2.000s (exponential_backoff)",
        "retry in 4.000s (exponential_backoff)",
        "retry in 8.000s (exponential_backoff)",
        "completed",
    ]
    assert service.get(op.id).status is S.SUCCEEDED


def test_backoff_is_capped(engine_db, simulator, clock, new_operation, make_worker):
    policy = RetryPolicy(max_attempts=10, base_delay_seconds=2.0, max_delay_seconds=5.0)
    op = new_operation()
    plan(simulator, op.id, *[Scenario.HTTP_503] * 4)
    worker = make_worker(policy=policy)
    for _ in range(5):
        worker.run_once()
        clock.advance(5)
    with closing(engine_db.connect()) as conn:
        detail = OperationService(conn, simulator).get_detail(op.id)
    assert [a.retry_decision for a in submits(detail)][:4] == [
        "retry in 2.000s (exponential_backoff)",
        "retry in 4.000s (exponential_backoff)",
        "retry in 5.000s (exponential_backoff)",
        "retry in 5.000s (exponential_backoff)",
    ]


# ------------------------------------------------- 2. 429 y Retry-After


def test_429_respects_retry_after_seconds(service, new_operation, simulator, make_worker, clock):
    op = new_operation()
    plan(simulator, op.id, Scenario.HTTP_429, retry_after="120")
    worker = make_worker()

    worker.run_once()
    scheduled = service.get(op.id)
    assert scheduled.status is S.FAILED_RETRYABLE
    assert scheduled.next_attempt_at == add_seconds(clock(), 120)
    [attempt] = service.get_detail(op.id).attempts
    assert attempt.retry_after == "120"
    assert attempt.retry_decision == "retry in 120.000s (retry_after)"

    clock.advance(119.999)
    assert worker.run_once().processed == 0
    clock.advance(0.001)
    assert worker.run_once().items[0].final_status == "succeeded"
    assert_single_effect(simulator, op.id)


def test_429_respects_retry_after_http_date(service, new_operation, simulator, make_worker, clock):
    op = new_operation()
    header = format_datetime(clock.now + timedelta(seconds=90), usegmt=True)
    plan(simulator, op.id, Scenario.HTTP_429, retry_after=header)

    make_worker().run_once()

    assert service.get(op.id).next_attempt_at == add_seconds(clock(), 90)
    [attempt] = service.get_detail(op.id).attempts
    assert attempt.retry_after == header
    assert attempt.retry_decision == "retry in 90.000s (retry_after)"


@pytest.mark.parametrize("header", ["soon", "-5", "1.5", "", "Mon, 99 Foo 2026"])
def test_429_with_invalid_retry_after_falls_back_to_backoff(
    service, new_operation, simulator, make_worker, clock, header
):
    op = new_operation()
    plan(simulator, op.id, Scenario.HTTP_429, retry_after=header)

    make_worker().run_once()

    assert service.get(op.id).next_attempt_at == add_seconds(clock(), 2.0)
    [attempt] = service.get_detail(op.id).attempts
    assert attempt.retry_decision == (
        "retry in 2.000s (exponential_backoff; retry-after missing or invalid)"
    )


def test_429_retry_after_shorter_than_backoff_uses_backoff(
    service, new_operation, simulator, make_worker, clock
):
    op = new_operation()
    plan(simulator, op.id, Scenario.HTTP_429, retry_after="0")
    make_worker().run_once()
    assert service.get(op.id).next_attempt_at == add_seconds(clock(), 2.0)


def test_429_huge_retry_after_is_capped(service, new_operation, simulator, make_worker, clock):
    op = new_operation()
    plan(simulator, op.id, Scenario.HTTP_429, retry_after="999999")
    make_worker().run_once()
    assert service.get(op.id).next_attempt_at == add_seconds(
        clock(), POLICY.max_retry_after_seconds
    )
    assert service.get_detail(op.id).attempts[0].retry_decision.endswith("(retry_after (capped))")


# ------------------------------------------------- 3. timeout antes del efecto


def test_timeout_before_effect_reconciles_then_resends_with_same_key(
    service, new_operation, simulator, make_worker, clock
):
    op = new_operation()
    plan(simulator, op.id, Scenario.TIMEOUT_BEFORE_PROCESSING)
    worker = make_worker()

    worker.run_once()
    uncertain = service.get(op.id)
    assert uncertain.status is S.OUTCOME_UNKNOWN
    assert uncertain.next_attempt_at == add_seconds(clock(), 2.0)
    assert simulator.list_effects(op.id) == []

    clock.advance(2)
    [item] = worker.run_once().items
    assert (item.claimed_from, item.final_status) == ("outcome_unknown", "succeeded")

    detail = service.get_detail(op.id)
    assert outcomes(detail) == [
        ("submit", O.TIMEOUT),
        ("reconcile", O.EFFECT_NOT_FOUND),
        ("submit", O.SUCCESS),
    ]
    assert [lk.found for lk in simulator.list_lookups(op.id)] == [False]
    assert len(simulator.list_requests(op.id)) == 2
    assert_single_effect(simulator, op.id)


# ------------------------------------------------- 4. timeout después del efecto


def test_timeout_after_effect_is_reconciled_without_resend(
    service, new_operation, simulator, make_worker, clock
):
    op = new_operation()
    plan(simulator, op.id, Scenario.TIMEOUT_AFTER_PROCESSING)
    worker = make_worker()

    worker.run_once()
    assert service.get(op.id).status is S.OUTCOME_UNKNOWN
    [effect] = simulator.list_effects(op.id)  # el proveedor sí lo aplicó

    clock.advance(2)
    [item] = worker.run_once().items
    assert item.action == "reconciled"

    detail = service.get_detail(op.id)
    assert detail.operation.status is S.SUCCEEDED
    assert detail.operation.external_effect_id == effect.effect_id
    assert outcomes(detail) == [("submit", O.TIMEOUT), ("reconcile", O.EFFECT_FOUND)]
    assert len(simulator.list_requests(op.id)) == 1  # no hubo reenvío
    assert assert_single_effect(simulator, op.id) == effect


def test_timeout_after_effect_without_lookup_resends_and_receiver_deduplicates(
    service, new_operation, simulator, make_worker, clock
):
    client = SubmitOnlyClient(simulator, supports_idempotency=True)
    op = new_operation()
    plan(simulator, op.id, Scenario.TIMEOUT_AFTER_PROCESSING)
    worker = make_worker(client=client)

    worker.run_once()
    clock.advance(2)
    worker.run_once()

    detail = service.get_detail(op.id)
    assert detail.operation.status is S.SUCCEEDED
    reconcile, resend = detail.attempts[1:]
    assert reconcile.outcome is O.LOOKUP_FAILED
    assert reconcile.error_code == "lookup_unsupported"
    assert resend.deduplicated_by_receiver is True
    first, second = simulator.list_requests(op.id)
    assert first.effect_applied and second.deduplicated
    effect = assert_single_effect(simulator, op.id)
    assert detail.operation.external_effect_id == effect.effect_id


def test_uncertain_outcome_without_idempotency_or_lookup_goes_to_dead_letter(
    service, new_operation, simulator_db, make_worker, clock
):
    unsafe = ExternalServiceSimulator(simulator_db, honor_idempotency=False)
    client = SubmitOnlyClient(unsafe, supports_idempotency=False)
    op = new_operation()
    plan(unsafe, op.id, Scenario.TIMEOUT_AFTER_PROCESSING)
    worker = make_worker(client=client)

    worker.run_once()
    clock.advance(2)
    [item] = worker.run_once().items

    assert item.final_status == "dead_letter"
    operation = service.get(op.id)
    assert "requires_manual_reconciliation" in operation.last_error
    # Nunca se reenvió a ciegas: un solo pedido y un solo efecto.
    assert len(unsafe.list_requests(op.id)) == 1
    assert len(unsafe.list_effects(op.id)) == 1
    clock.advance(10_000)
    assert worker.run_once().processed == 0


def test_lookup_resolves_uncertainty_even_without_receiver_idempotency(
    service, new_operation, simulator_db, make_worker, clock
):
    unsafe = ExternalServiceSimulator(simulator_db, honor_idempotency=False)
    op = new_operation()
    plan(unsafe, op.id, Scenario.TIMEOUT_AFTER_PROCESSING)
    worker = make_worker(client=unsafe)

    worker.run_once()
    clock.advance(2)
    worker.run_once()

    assert service.get(op.id).status is S.SUCCEEDED
    assert len(unsafe.list_requests(op.id)) == 1
    assert unsafe.find_duplicate_effects() == []


def test_not_found_without_receiver_idempotency_is_not_resent(
    service, new_operation, simulator_db, make_worker, clock
):
    unsafe = ExternalServiceSimulator(simulator_db, honor_idempotency=False)
    op = new_operation()
    plan(unsafe, op.id, Scenario.TIMEOUT_BEFORE_PROCESSING)
    worker = make_worker(client=unsafe)

    worker.run_once()
    clock.advance(2)
    worker.run_once()

    # Un "no encontrado" sin idempotencia del receptor no alcanza: un pedido demorado
    # aún podría procesarse. Se exige intervención en lugar de arriesgar un duplicado.
    assert service.get(op.id).status is S.DEAD_LETTER
    assert len(unsafe.list_requests(op.id)) == 1
    assert unsafe.list_effects(op.id) == []


# ------------------------------------------------- 5. webhook duplicado


def test_duplicate_webhook_while_retry_is_scheduled(service, simulator, make_worker, clock):
    payload = {"customer_id": "c-001", "amount_cents": 1500}
    original = service.create(
        idempotency_key="webhook-1", operation_type="issue_invoice", payload=payload
    ).operation
    plan(simulator, original.id, Scenario.HTTP_503)
    worker = make_worker()
    worker.run_once()

    duplicate = service.create(
        idempotency_key="webhook-1", operation_type="issue_invoice", payload=payload
    )
    assert duplicate.created is False
    assert duplicate.operation.id == original.id
    assert duplicate.operation.status is S.FAILED_RETRYABLE
    assert duplicate.operation.attempt_count == 1
    with pytest.raises(IdempotencyConflictError):
        service.create(
            idempotency_key="webhook-1", operation_type="issue_invoice", payload={"x": 1}
        )

    clock.advance(2)
    worker.run_once()
    late = service.create(
        idempotency_key="webhook-1", operation_type="issue_invoice", payload=payload
    )
    assert late.operation.status is S.SUCCEEDED
    assert len(service.list()) == 1
    assert len(simulator.list_requests(original.id)) == 2
    assert_single_effect(simulator, original.id)


def test_duplicate_webhook_and_second_worker_while_attempt_in_flight(
    engine_db, service, simulator, make_worker, clock
):
    payload = {"amount_cents": 1500}
    op = service.create(
        idempotency_key="webhook-2", operation_type="issue_invoice", payload=payload
    ).operation
    observed = {}

    def during_call():
        # Corre mientras worker-a tiene el lease y el pedido está en vuelo. Si el motor
        # mantuviera un lock SQLite durante la llamada externa, esto se bloquearía.
        with closing(engine_db.connect()) as conn:
            other = OperationService(conn, simulator, clock=clock)
            observed["duplicate"] = other.create(
                idempotency_key="webhook-2", operation_type="issue_invoice", payload=payload
            )
        observed["worker_b"] = make_worker("worker-b").run_once()

    make_worker("worker-a", client=HookClient(simulator, before_submit=during_call)).run_once()

    assert observed["duplicate"].created is False
    assert observed["duplicate"].operation.status is S.IN_PROGRESS
    assert observed["duplicate"].operation.lease_owner == "worker-a"
    assert observed["worker_b"].processed == 0
    assert service.get(op.id).status is S.SUCCEEDED
    assert len(simulator.list_requests(op.id)) == 1
    assert_single_effect(simulator, op.id)


# ------------------------------------------------- 6. claims concurrentes


def test_two_processors_cannot_claim_the_same_operation(new_operation, make_processor):
    op = new_operation()
    first, second = make_processor(), make_processor()

    lease = first.claim_next("worker-a")
    assert lease is not None and lease.token == 1
    assert second.claim_next("worker-b") is None
    assert second.claim(op.id, "worker-b") is None


def test_concurrent_workers_process_each_operation_exactly_once(
    service, new_operation, simulator, make_worker
):
    operations = [new_operation() for _ in range(6)]
    workers = 4
    barrier = threading.Barrier(workers)

    def run(index):
        worker = make_worker(f"worker-{index}")
        barrier.wait()
        return worker.run_once(max_operations=10)

    with ThreadPoolExecutor(workers) as pool:
        reports = list(pool.map(run, range(workers)))

    processed = sorted(item.operation_id for r in reports for item in r.items)
    assert processed == sorted(op.id for op in operations)
    assert all(service.get(op.id).status is S.SUCCEEDED for op in operations)
    assert len(simulator.list_requests()) == 6
    assert len(simulator.list_effects()) == 6
    assert simulator.find_duplicate_effects() == []


# ------------------------------------------------- 7-9. caídas y lease vencido


def test_crash_after_reserving_attempt_before_calling_provider(
    service, new_operation, simulator, make_worker, clock
):
    op = new_operation()
    with pytest.raises(SimulatedCrash):
        make_worker("worker-a", client=CrashingClient(simulator, crash="before_submit")).run_once()

    stuck = service.get_detail(op.id)
    assert stuck.operation.status is S.IN_PROGRESS
    assert stuck.operation.lease_owner == "worker-a"
    [open_attempt] = stuck.attempts
    assert open_attempt.finished_at is None
    assert simulator.list_requests(op.id) == []

    # Mientras el lease está vigente, nadie más la toca.
    assert make_worker("worker-b").run_once().processed == 0

    clock.advance(LEASE_TTL)
    [item] = make_worker("worker-b").run_once().items
    assert (item.claimed_from, item.final_status) == ("in_progress", "succeeded")

    detail = service.get_detail(op.id)
    assert outcomes(detail) == [
        ("submit", O.ABANDONED),
        ("reconcile", O.EFFECT_NOT_FOUND),
        ("submit", O.SUCCESS),
    ]
    assert [(a.worker_id, a.lease_token) for a in detail.attempts] == [
        ("worker-a", 1),
        ("worker-b", 2),
        ("worker-b", 2),
    ]
    assert len(simulator.list_requests(op.id)) == 1
    assert_single_effect(simulator, op.id)


def test_crash_after_provider_effect_before_recording_result(
    service, new_operation, simulator, make_worker, clock
):
    op = new_operation()
    with pytest.raises(SimulatedCrash):
        make_worker("worker-a", client=CrashingClient(simulator, crash="after_submit")).run_once()

    [effect] = simulator.list_effects(op.id)
    assert service.get(op.id).status is S.IN_PROGRESS  # el motor no llegó a registrarlo

    clock.advance(LEASE_TTL)
    [item] = make_worker("worker-b").run_once().items
    assert item.action == "reconciled"

    detail = service.get_detail(op.id)
    assert detail.operation.status is S.SUCCEEDED
    assert detail.operation.external_effect_id == effect.effect_id
    assert outcomes(detail) == [("submit", O.ABANDONED), ("reconcile", O.EFFECT_FOUND)]
    assert len(simulator.list_requests(op.id)) == 1
    assert_single_effect(simulator, op.id)


def test_expired_lease_is_recovered_by_another_worker(
    service, new_operation, simulator, make_worker, clock
):
    op = new_operation()
    with pytest.raises(SimulatedCrash):
        make_worker("worker-a", client=CrashingClient(simulator, crash="before_submit")).run_once()
    lease_expires_at = service.get(op.id).lease_expires_at
    assert lease_expires_at == add_seconds(clock(), LEASE_TTL)

    clock.advance(LEASE_TTL - 0.001)
    assert make_worker("worker-b").run_once().processed == 0
    clock.advance(0.001)  # vence exactamente ahora
    report = make_worker("worker-b").run_once()
    assert report.items[0].lease_token == 2

    transitions = service.get_detail(op.id).transitions
    takeover, reclaim = transitions[2], transitions[3]
    assert (takeover.from_status, takeover.to_status) == (S.IN_PROGRESS, S.OUTCOME_UNKNOWN)
    assert takeover.actor == "worker-b"
    assert "lease expired (owner=worker-a, token=1)" in takeover.reason
    assert (reclaim.from_status, reclaim.to_status, reclaim.lease_token) == (
        S.OUTCOME_UNKNOWN,
        S.IN_PROGRESS,
        2,
    )


# ------------------------------------------------- 10. fencing


def test_stale_worker_cannot_persist_after_losing_lease(
    service, new_operation, simulator, make_worker, clock
):
    op = new_operation()
    observed = {}

    def lose_lease():
        # worker-a quedó "colgado" tras el envío; su lease vence y worker-b toma el control.
        clock.advance(LEASE_TTL + 1)
        observed["worker_b"] = make_worker("worker-b").run_once()

    report_a = make_worker(
        "worker-a", client=HookClient(simulator, after_submit=lose_lease)
    ).run_once()

    [item_a] = report_a.items
    assert item_a.action == "lease_lost"
    [item_b] = observed["worker_b"].items
    assert (item_b.action, item_b.final_status) == ("reconciled", "succeeded")

    detail = service.get_detail(op.id)
    assert detail.operation.status is S.SUCCEEDED
    assert detail.operation.lease_token == 2
    assert detail.operation.lease_owner is None
    # El resultado tardío de worker-a se descartó: su intento quedó como lo cerró worker-b.
    assert outcomes(detail) == [("submit", O.ABANDONED), ("reconcile", O.EFFECT_FOUND)]
    assert detail.transitions[-1].actor == "worker-b"
    assert len(simulator.list_requests(op.id)) == 1
    assert_single_effect(simulator, op.id)


def test_stale_lease_never_calls_the_provider(
    service, new_operation, simulator, make_processor, clock
):
    op = new_operation()
    processor_a, processor_b = make_processor(), make_processor()
    lease_a = processor_a.claim(op.id, "worker-a")
    clock.advance(LEASE_TTL)
    lease_b = processor_b.claim(op.id, "worker-b")
    assert (lease_a.token, lease_b.token) == (1, 2)

    with pytest.raises(LeaseLostError):
        processor_a.process(lease_a)
    assert simulator.list_requests(op.id) == []

    processor_b.process(lease_b)
    assert service.get(op.id).status is S.SUCCEEDED
    assert_single_effect(simulator, op.id)


# ------------------------------------------------- 11-12. permanente y dead-letter


def test_permanent_error_is_not_retried(service, new_operation, simulator, make_worker, clock):
    op = new_operation()
    plan(simulator, op.id, Scenario.PERMANENT_ERROR)
    worker = make_worker()

    worker.run_once()
    clock.advance(10_000)
    assert worker.run_once().processed == 0

    detail = service.get_detail(op.id)
    assert detail.operation.status is S.FAILED_PERMANENT
    assert detail.operation.next_attempt_at is None
    [attempt] = detail.attempts
    assert attempt.retry_decision == "no_retry: permanent_error"
    assert len(simulator.list_requests(op.id)) == 1


def test_exhausted_retries_go_to_dead_letter(service, new_operation, simulator, make_worker, clock):
    op = new_operation()
    plan(simulator, op.id, *[Scenario.HTTP_503] * POLICY.max_attempts)
    worker = make_worker()

    for _ in range(POLICY.max_attempts):
        worker.run_once()
        clock.advance(60)

    detail = service.get_detail(op.id)
    assert detail.operation.status is S.DEAD_LETTER
    assert detail.operation.next_attempt_at is None
    assert "max_attempts_exhausted (4)" in detail.operation.last_error
    assert [a.retry_decision for a in detail.attempts] == [
        "retry in 2.000s (exponential_backoff)",
        "retry in 4.000s (exponential_backoff)",
        "retry in 8.000s (exponential_backoff)",
        "no_retry: max_attempts_exhausted (4)",
    ]
    clock.advance(1_000_000)
    assert worker.run_once().processed == 0
    assert len(simulator.list_requests(op.id)) == POLICY.max_attempts
    assert simulator.list_effects(op.id) == []


def test_repeated_timeouts_exhaust_into_dead_letter_after_reconciling(
    service, new_operation, simulator, make_worker, clock
):
    op = new_operation()
    plan(simulator, op.id, *[Scenario.TIMEOUT_BEFORE_PROCESSING] * POLICY.max_attempts)
    worker = make_worker()

    for _ in range(POLICY.max_attempts + 1):
        worker.run_once()
        clock.advance(60)

    detail = service.get_detail(op.id)
    assert detail.operation.status is S.DEAD_LETTER
    assert "outcome unresolved after reconciliation" in detail.operation.last_error
    assert len(simulator.list_requests(op.id)) == POLICY.max_attempts
    assert len(simulator.list_lookups(op.id)) == POLICY.max_attempts
    assert simulator.list_effects(op.id) == []


def test_execute_with_single_attempt_budget_dead_letters_immediately(
    engine_db, simulator, clock, new_operation
):
    op = new_operation()
    with closing(engine_db.connect()) as conn:
        service = OperationService(
            conn, simulator, clock=clock, policy=RetryPolicy(max_attempts=1), jitter=no_jitter
        )
        result = service.execute(op.id, Scenario.HTTP_503)
    assert result.operation.status is S.DEAD_LETTER
    assert result.attempt.retry_decision == "no_retry: max_attempts_exhausted (1)"


# ------------------------------------------------- 13. corridas sin trabajo


def test_worker_rerun_without_eligible_operations_has_no_effects(
    engine_db, service, new_operation, simulator, make_worker, clock
):
    worker = make_worker()
    assert worker.run_once().processed == 0  # base vacía

    done, permanent, waiting = new_operation(), new_operation(), new_operation()
    plan(simulator, permanent.id, Scenario.PERMANENT_ERROR)
    plan(simulator, waiting.id, Scenario.HTTP_429, retry_after="300")
    worker.run_once()

    def snapshot():
        with closing(engine_db.connect()) as conn:
            counts = {
                table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                for table in ("operations", "attempts", "operation_transitions")
            }
        counts["requests"] = len(simulator.list_requests())
        counts["effects"] = len(simulator.list_effects())
        counts["lookups"] = len(simulator.list_lookups())
        return counts

    before = snapshot()
    for _ in range(3):
        assert worker.run_once().processed == 0
        clock.advance(10)
    assert snapshot() == before
    assert service.get(done.id).status is S.SUCCEEDED


def test_worker_processes_each_operation_at_most_once_per_run(
    service, new_operation, simulator, engine_db, clock
):
    """Aunque una operación quede re-programada para "ahora", la corrida está acotada."""
    from recovery_engine.services.worker import RecoveryWorker

    zero_delay = RetryPolicy(max_attempts=10, base_delay_seconds=0.0, max_delay_seconds=0.0)
    op = new_operation()
    plan(simulator, op.id, *[Scenario.HTTP_503] * 5)
    worker = RecoveryWorker(
        engine_db, simulator, worker_id="w", policy=zero_delay, clock=clock, jitter=no_jitter
    )

    report = worker.run_once(max_operations=10)

    assert report.processed == 1
    assert len(simulator.list_requests(op.id)) == 1


# ------------------------------------------------- 14. historial y trazabilidad


def test_full_history_is_preserved_and_traceable(
    service, new_operation, simulator, make_worker, clock
):
    op = new_operation()
    plan(simulator, op.id, Scenario.HTTP_503, Scenario.TIMEOUT_AFTER_PROCESSING)
    worker = make_worker()
    for _ in range(3):
        worker.run_once()
        clock.advance(60)

    detail = service.get_detail(op.id)
    assert detail.operation.status is S.SUCCEEDED

    # Intentos: numeración contigua, tipo, resultado, dueño y token de cada uno.
    assert [a.attempt_number for a in detail.attempts] == [1, 2, 3]
    assert outcomes(detail) == [
        ("submit", O.RETRYABLE_ERROR),
        ("submit", O.TIMEOUT),
        ("reconcile", O.EFFECT_FOUND),
    ]
    assert [a.lease_token for a in detail.attempts] == [1, 2, 3]
    assert {a.worker_id for a in detail.attempts} == {"worker-a"}
    assert all(a.finished_at is not None for a in detail.attempts)
    assert detail.attempts[1].retry_decision == "reconcile in 4.000s (outcome unknown)"

    # Transiciones: historia completa, append-only, con el token de cada claim.
    assert [(t.from_status, t.to_status) for t in detail.transitions] == [
        (None, S.PENDING),
        (S.PENDING, S.IN_PROGRESS),
        (S.IN_PROGRESS, S.FAILED_RETRYABLE),
        (S.FAILED_RETRYABLE, S.IN_PROGRESS),
        (S.IN_PROGRESS, S.OUTCOME_UNKNOWN),
        (S.OUTCOME_UNKNOWN, S.IN_PROGRESS),
        (S.IN_PROGRESS, S.SUCCEEDED),
    ]
    claims = [t for t in detail.transitions if t.to_status is S.IN_PROGRESS]
    assert [t.lease_token for t in claims] == [1, 2, 3]
    assert all(t.actor == "worker-a" for t in detail.transitions[1:])

    # Proveedor: todos los envíos con la misma clave y un solo efecto.
    assert_single_effect(simulator, op.id)
    assert [r.scenario for r in simulator.list_requests(op.id)] == [
        "http_503",
        "timeout_after_processing",
    ]
