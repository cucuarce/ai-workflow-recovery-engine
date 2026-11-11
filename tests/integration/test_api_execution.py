import pytest

PAYLOAD = {"customer_id": "c-001", "amount_cents": 1500}


def create_operation(client, key="key-1") -> str:
    response = client.post(
        "/operations",
        json={"operation_type": "issue_invoice", "payload": PAYLOAD},
        headers={"Idempotency-Key": key},
    )
    return response.json()["id"]


def execute(client, op_id, scenario=None):
    body = None if scenario is None else {"scenario": scenario}
    return client.post(f"/operations/{op_id}/execute", json=body)


@pytest.mark.parametrize(
    ("scenario", "status", "outcome", "http_status", "effects"),
    [
        ("success", "succeeded", "success", 201, 1),
        ("http_503", "failed_retryable", "retryable_error", 503, 0),
        ("http_429", "failed_retryable", "rate_limited", 429, 0),
        ("timeout_before_processing", "outcome_unknown", "timeout", None, 0),
        ("timeout_after_processing", "outcome_unknown", "timeout", None, 1),
        ("permanent_error", "failed_permanent", "permanent_error", 422, 0),
    ],
)
def test_execute_scenarios(client, scenario, status, outcome, http_status, effects):
    op_id = create_operation(client)

    response = execute(client, op_id, scenario)

    assert response.status_code == 200
    body = response.json()
    assert body["operation"]["status"] == status
    assert body["operation"]["attempt_count"] == 1
    assert body["attempt"]["attempt_number"] == 1
    assert body["attempt"]["scenario"] == scenario
    assert body["attempt"]["outcome"] == outcome
    assert body["attempt"]["http_status"] == http_status
    assert body["attempt"]["finished_at"] is not None

    actual_effects = client.get("/simulator/effects", params={"operation_ref": op_id}).json()
    assert len(actual_effects) == effects
    requests = client.get("/simulator/requests", params={"operation_ref": op_id}).json()
    assert len(requests) == 1


def test_default_scenario_is_success(client):
    op_id = create_operation(client)
    assert execute(client, op_id).json()["operation"]["status"] == "succeeded"


def test_success_links_external_effect(client):
    op_id = create_operation(client)
    body = execute(client, op_id, "success").json()

    [effect] = client.get("/simulator/effects", params={"operation_ref": op_id}).json()
    assert body["operation"]["external_effect_id"] == effect["effect_id"]
    assert body["attempt"]["external_effect_id"] == effect["effect_id"]
    assert body["operation"]["last_error"] is None
    assert effect["payload"] == PAYLOAD


def test_timeout_after_processing_is_not_assumed_to_be_a_failure(client):
    op_id = create_operation(client)
    body = execute(client, op_id, "timeout_after_processing").json()

    # El motor no sabe qué pasó: no lo da por fallido ni por exitoso.
    assert body["operation"]["status"] == "outcome_unknown"
    assert body["operation"]["external_effect_id"] is None
    assert body["operation"]["last_error"].startswith("timeout")

    # El simulador registra la verdad: el efecto se aplicó pero la respuesta no llegó.
    [record] = client.get("/simulator/requests", params={"operation_ref": op_id}).json()
    assert record["effect_applied"] is True
    assert record["delivered_to_client"] is False
    assert record["server_status"] == 201


def test_timeout_before_processing_truth_is_observable(client):
    op_id = create_operation(client)
    execute(client, op_id, "timeout_before_processing")

    [record] = client.get("/simulator/requests", params={"operation_ref": op_id}).json()
    assert record["effect_applied"] is False
    assert record["server_status"] is None
    assert record["delivered_to_client"] is False


def test_failure_is_recorded_on_operation_and_attempt(client):
    op_id = create_operation(client)
    execute(client, op_id, "http_429")

    detail = client.get(f"/operations/{op_id}").json()
    assert detail["last_error"].startswith("rate_limited")
    [attempt] = detail["attempts"]
    assert attempt["error_code"] == "rate_limited"
    assert attempt["outcome"] == "rate_limited"


def test_transitions_are_recorded(client):
    op_id = create_operation(client)
    execute(client, op_id, "success")

    transitions = client.get(f"/operations/{op_id}").json()["transitions"]
    assert [(t["from_status"], t["to_status"]) for t in transitions] == [
        (None, "pending"),
        ("pending", "in_progress"),
        ("in_progress", "succeeded"),
    ]


def test_second_execution_is_rejected_in_stage_1(client):
    op_id = create_operation(client)
    execute(client, op_id, "http_503")

    response = execute(client, op_id, "success")

    assert response.status_code == 409
    assert response.json()["error"] == "operation_not_executable"
    assert len(client.get("/simulator/requests", params={"operation_ref": op_id}).json()) == 1


def test_execute_unknown_operation_returns_404(client):
    assert execute(client, "op_missing", "success").status_code == 404


def test_invalid_scenario_is_rejected(client):
    op_id = create_operation(client)
    assert execute(client, op_id, "explode").status_code == 422
    assert client.get(f"/operations/{op_id}").json()["status"] == "pending"


def test_no_duplicates_across_many_operations(client):
    for i, scenario in enumerate(["success", "timeout_after_processing", "success", "http_503"]):
        execute(client, create_operation(client, key=f"k-{i}"), scenario)

    assert client.get("/simulator/duplicates").json() == []
    assert len(client.get("/simulator/effects").json()) == 3
