"""Planes de fallos cargados por la referencia del cliente (antes de que exista la operación)."""

from recovery_engine.external.contracts import ExternalRequest, ExternalTimeoutError, Scenario
from recovery_engine.external.simulator import FaultSpec


def request(op_id="op_1", client_reference="client-key"):
    return ExternalRequest(
        idempotency_key=op_id,
        operation_ref=op_id,
        operation_type="issue_invoice",
        payload={"amount_cents": 1500},
        client_reference=client_reference,
    )


def test_client_plan_applies_to_the_operation_created_later(simulator):
    simulator.set_client_fault_plan("client-key", [FaultSpec(Scenario.HTTP_503)])
    assert simulator.submit(request()).status_code == 503
    assert simulator.submit(request()).status_code == 201  # plan consumido


def test_operation_plan_takes_precedence_over_client_plan(simulator):
    simulator.set_client_fault_plan("client-key", [FaultSpec(Scenario.PERMANENT_ERROR)])
    simulator.set_fault_plan("op_1", [FaultSpec(Scenario.TIMEOUT_AFTER_PROCESSING)])
    try:
        simulator.submit(request())
    except ExternalTimeoutError:
        pass
    else:  # pragma: no cover
        raise AssertionError("se esperaba el plan propio de la operación")
    assert simulator.submit(request()).status_code == 200  # dedup: el efecto ya existe


def test_client_plan_does_not_leak_to_other_requests(simulator):
    simulator.set_client_fault_plan("client-key", [FaultSpec(Scenario.HTTP_503)])
    assert simulator.submit(request("op_2", client_reference="other")).status_code == 201
    assert simulator.submit(request("op_3", client_reference=None)).status_code == 201
