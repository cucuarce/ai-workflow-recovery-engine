import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from recovery_engine.external.contracts import (
    ExternalRequest,
    ExternalTimeoutError,
    Scenario,
)
from recovery_engine.external.simulator import RETRY_AFTER_SECONDS, ExternalServiceSimulator


def make_request(
    scenario: Scenario = Scenario.SUCCESS, key: str = "op_1", payload: dict | None = None
) -> ExternalRequest:
    return ExternalRequest(
        idempotency_key=key,
        operation_ref=key,
        operation_type="issue_invoice",
        payload=payload if payload is not None else {"amount_cents": 1500},
        scenario=scenario,
    )


def test_success_applies_exactly_one_effect(simulator):
    response = simulator.submit(make_request())

    assert response.status_code == 201
    assert response.effect_id is not None
    assert not response.deduplicated
    [effect] = simulator.list_effects("op_1")
    assert effect.effect_id == response.effect_id
    assert effect.payload == {"amount_cents": 1500}
    [record] = simulator.list_requests("op_1")
    assert record.effect_applied
    assert record.delivered_to_client
    assert record.server_status == 201


@pytest.mark.parametrize(
    ("scenario", "status", "code"),
    [
        (Scenario.HTTP_503, 503, "service_unavailable"),
        (Scenario.HTTP_429, 429, "rate_limited"),
        (Scenario.PERMANENT_ERROR, 422, "invalid_request"),
    ],
)
def test_rejections_do_not_apply_effects(simulator, scenario, status, code):
    response = simulator.submit(make_request(scenario))

    assert response.status_code == status
    assert response.error_code == code
    assert response.effect_id is None
    assert simulator.list_effects() == []
    [record] = simulator.list_requests()
    assert record.server_status == status
    assert not record.effect_applied
    assert record.delivered_to_client


def test_429_includes_retry_after(simulator):
    response = simulator.submit(make_request(Scenario.HTTP_429))
    assert response.retry_after_seconds == RETRY_AFTER_SECONDS


def test_timeout_before_processing_applies_nothing(simulator):
    with pytest.raises(ExternalTimeoutError):
        simulator.submit(make_request(Scenario.TIMEOUT_BEFORE_PROCESSING))

    assert simulator.list_effects() == []
    [record] = simulator.list_requests()
    assert record.server_status is None
    assert not record.effect_applied
    assert not record.delivered_to_client


def test_timeout_after_processing_commits_effect_but_client_sees_timeout(simulator):
    with pytest.raises(ExternalTimeoutError):
        simulator.submit(make_request(Scenario.TIMEOUT_AFTER_PROCESSING))

    [effect] = simulator.list_effects("op_1")
    [record] = simulator.list_requests("op_1")
    assert record.effect_applied
    assert record.effect_id == effect.effect_id
    assert record.server_status == 201
    assert not record.delivered_to_client


def test_receiver_idempotency_deduplicates_resend_after_timeout(simulator):
    with pytest.raises(ExternalTimeoutError):
        simulator.submit(make_request(Scenario.TIMEOUT_AFTER_PROCESSING))

    response = simulator.submit(make_request(Scenario.SUCCESS))

    [effect] = simulator.list_effects("op_1")
    assert response.status_code == 200
    assert response.deduplicated
    assert response.effect_id == effect.effect_id
    assert simulator.find_duplicate_effects() == []
    first, second = simulator.list_requests("op_1")
    assert first.effect_applied and not first.deduplicated
    assert second.deduplicated and not second.effect_applied


def test_receiver_rejects_same_key_with_different_payload(simulator):
    simulator.submit(make_request(payload={"amount_cents": 1500}))
    response = simulator.submit(make_request(payload={"amount_cents": 9999}))

    assert response.status_code == 409
    assert response.error_code == "idempotency_key_mismatch"
    assert len(simulator.list_effects()) == 1


def test_without_receiver_idempotency_duplicates_are_detectable(simulator_db):
    simulator = ExternalServiceSimulator(simulator_db, honor_idempotency=False)
    simulator.initialize()

    with pytest.raises(ExternalTimeoutError):
        simulator.submit(make_request(Scenario.TIMEOUT_AFTER_PROCESSING))
    simulator.submit(make_request(Scenario.SUCCESS))

    effects = simulator.list_effects("op_1")
    [report] = simulator.find_duplicate_effects()
    assert len(effects) == 2
    assert report.operation_ref == "op_1"
    assert report.effect_count == 2
    assert report.effect_ids == sorted(e.effect_id for e in effects)


def test_concurrent_submissions_with_same_key_produce_one_effect(simulator):
    workers = 12
    barrier = threading.Barrier(workers)

    def submit():
        barrier.wait()
        return simulator.submit(make_request())

    with ThreadPoolExecutor(workers) as pool:
        responses = list(pool.map(lambda _: submit(), range(workers)))

    assert len(simulator.list_effects()) == 1
    assert sorted(r.status_code for r in responses) == [200] * (workers - 1) + [201]
    assert len({r.effect_id for r in responses}) == 1


def test_deterministic_effect_ids_with_injected_factory(simulator_db):
    ids = iter(["eff_a", "eff_b"])
    simulator = ExternalServiceSimulator(simulator_db, effect_id_factory=lambda: next(ids))
    simulator.initialize()

    assert simulator.submit(make_request(key="op_1")).effect_id == "eff_a"
    assert simulator.submit(make_request(key="op_2")).effect_id == "eff_b"
