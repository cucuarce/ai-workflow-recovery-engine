from contextlib import closing

import pytest

from recovery_engine.domain.errors import (
    IdempotencyConflictError,
    OperationNotExecutableError,
    OperationNotFoundError,
)
from recovery_engine.domain.models import AttemptOutcome, OperationStatus
from recovery_engine.external.contracts import ExternalRequest, ExternalResponse, Scenario
from recovery_engine.services.operations import OperationService

PAYLOAD = {"customer_id": "c-001", "amount_cents": 1500}


def test_create_is_idempotent(make_service):
    service = make_service()
    first = service.create(idempotency_key="k1", operation_type="issue_invoice", payload=PAYLOAD)
    second = service.create(idempotency_key="k1", operation_type="issue_invoice", payload=PAYLOAD)

    assert first.created
    assert not second.created
    assert second.operation.id == first.operation.id
    assert len(service.list()) == 1


def test_create_conflicts_on_payload_mismatch(make_service):
    service = make_service()
    created = service.create(idempotency_key="k1", operation_type="issue_invoice", payload=PAYLOAD)

    with pytest.raises(IdempotencyConflictError) as excinfo:
        service.create(
            idempotency_key="k1", operation_type="issue_invoice", payload={"amount_cents": 1}
        )
    assert excinfo.value.existing_operation_id == created.operation.id


def test_deterministic_ids_and_clock_are_injectable(engine_db, simulator):
    with closing(engine_db.connect()) as conn:
        service = OperationService(
            conn, simulator, clock=lambda: "2026-01-01T00:00:00+00:00", id_factory=lambda: "op_x"
        )
        result = service.create(idempotency_key="k", operation_type="t", payload={})
    assert result.operation.id == "op_x"
    assert result.operation.created_at == "2026-01-01T00:00:00+00:00"


def test_execute_unknown_operation(make_service):
    with pytest.raises(OperationNotFoundError):
        make_service().execute("op_missing")


def test_execute_only_from_pending_in_stage_1(make_service):
    service = make_service()
    op = service.create(idempotency_key="k", operation_type="t", payload={}).operation
    service.execute(op.id, Scenario.HTTP_503)

    assert service.get(op.id).status is OperationStatus.FAILED_RETRYABLE
    with pytest.raises(OperationNotExecutableError):
        service.execute(op.id, Scenario.SUCCESS)


class ExplodingClient:
    """Simula una caída del proceso durante la llamada externa."""

    def __init__(self) -> None:
        self.calls: list[ExternalRequest] = []

    def submit(self, request: ExternalRequest) -> ExternalResponse:
        self.calls.append(request)
        raise RuntimeError("process crashed mid-call")


def test_crash_during_external_call_leaves_persistent_evidence(engine_db):
    client = ExplodingClient()
    conn = engine_db.connect()
    try:
        service = OperationService(conn, client)
        op = service.create(idempotency_key="k", operation_type="t", payload={}).operation

        with pytest.raises(RuntimeError):
            service.execute(op.id)

        detail = service.get_detail(op.id)
    finally:
        conn.close()

    # El intento quedó reservado (in_progress + intento abierto): la Etapa 2 podrá detectarlo.
    assert len(client.calls) == 1
    assert detail.operation.status is OperationStatus.IN_PROGRESS
    [attempt] = detail.attempts
    assert attempt.outcome is None
    assert attempt.finished_at is None


def test_external_idempotency_key_is_the_operation_id(engine_db):
    class RecordingClient:
        def __init__(self) -> None:
            self.requests: list[ExternalRequest] = []

        def submit(self, request: ExternalRequest) -> ExternalResponse:
            self.requests.append(request)
            return ExternalResponse(status_code=201, effect_id="eff_1")

    client = RecordingClient()
    conn = engine_db.connect()
    try:
        service = OperationService(conn, client)
        op = service.create(idempotency_key="client-key", operation_type="t", payload={}).operation
        result = service.execute(op.id)
    finally:
        conn.close()

    [request] = client.requests
    assert request.idempotency_key == op.id
    assert result.attempt.outcome is AttemptOutcome.SUCCESS
    assert result.operation.external_effect_id == "eff_1"
