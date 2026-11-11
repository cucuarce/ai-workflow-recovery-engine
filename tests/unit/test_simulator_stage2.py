import pytest

from recovery_engine.external.contracts import ExternalRequest, ExternalTimeoutError, Scenario
from recovery_engine.external.simulator import FaultSpec


def request(scenario=None, key="op_1"):
    return ExternalRequest(
        idempotency_key=key,
        operation_ref=key,
        operation_type="issue_invoice",
        payload={"amount_cents": 1500},
        scenario=scenario,
    )


def test_fault_plan_is_consumed_in_order_then_defaults_to_success(simulator):
    simulator.set_fault_plan(
        "op_1", [FaultSpec(Scenario.HTTP_503), FaultSpec(Scenario.HTTP_429, retry_after="15")]
    )

    assert simulator.submit(request()).status_code == 503
    rate_limited = simulator.submit(request())
    assert rate_limited.status_code == 429
    assert (rate_limited.retry_after, rate_limited.retry_after_seconds) == ("15", 15)
    assert simulator.submit(request()).status_code == 201
    assert simulator.pending_faults("op_1") == []
    assert [r.scenario for r in simulator.list_requests("op_1")] == [
        "http_503",
        "http_429",
        "success",
    ]


def test_explicit_scenario_overrides_plan_without_consuming_it(simulator):
    simulator.set_fault_plan("op_1", [FaultSpec(Scenario.HTTP_503)])
    with pytest.raises(ExternalTimeoutError):
        simulator.submit(request(Scenario.TIMEOUT_BEFORE_PROCESSING))
    assert simulator.pending_faults("op_1") == [FaultSpec(Scenario.HTTP_503)]


def test_fault_plans_are_isolated_per_operation(simulator):
    simulator.set_fault_plan("op_1", [FaultSpec(Scenario.PERMANENT_ERROR)])
    assert simulator.submit(request(key="op_2")).status_code == 201
    assert simulator.submit(request(key="op_1")).status_code == 422


def test_set_fault_plan_replaces_only_pending_entries(simulator):
    simulator.set_fault_plan("op_1", [FaultSpec(Scenario.HTTP_503), FaultSpec(Scenario.HTTP_503)])
    simulator.submit(request())
    simulator.set_fault_plan("op_1", [FaultSpec(Scenario.HTTP_429)])
    assert simulator.pending_faults("op_1") == [FaultSpec(Scenario.HTTP_429)]
    assert len(simulator.list_requests("op_1")) == 1


def test_http_date_retry_after_is_passed_through_raw(simulator):
    header = "Wed, 21 Oct 2026 07:28:00 GMT"
    simulator.set_fault_plan("op_1", [FaultSpec(Scenario.HTTP_429, retry_after=header)])
    response = simulator.submit(request())
    assert (response.retry_after, response.retry_after_seconds) == (header, None)


def test_lookup_reports_persisted_state_and_is_logged(simulator):
    assert simulator.lookup("op_1").found is False
    with pytest.raises(ExternalTimeoutError):
        simulator.submit(request(Scenario.TIMEOUT_AFTER_PROCESSING))

    found = simulator.lookup("op_1")

    [effect] = simulator.list_effects("op_1")
    assert (found.found, found.effect_id) == (True, effect.effect_id)
    assert [(lk.found, lk.effect_id) for lk in simulator.list_lookups("op_1")] == [
        (False, None),
        (True, effect.effect_id),
    ]
    assert len(simulator.list_effects()) == 1  # consultar no crea efectos


def test_supports_idempotency_reflects_configuration(simulator, simulator_db):
    from recovery_engine.external.simulator import ExternalServiceSimulator

    assert simulator.supports_idempotency is True
    assert (
        ExternalServiceSimulator(simulator_db, honor_idempotency=False).supports_idempotency
        is False
    )
