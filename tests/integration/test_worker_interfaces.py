"""Interfaces de disparo del worker: endpoint administrativo (opt-in) y CLI."""

from __future__ import annotations

import json
from dataclasses import replace

import pytest
from fastapi.testclient import TestClient

from recovery_engine.api.app import create_app
from recovery_engine.cli import main
from recovery_engine.domain.retry import no_jitter
from recovery_engine.runtime import build_runtime
from support import TEST_ADMIN_TOKEN


def create(client, key="k-1"):
    response = client.post(
        "/operations",
        json={"operation_type": "issue_invoice", "payload": {"amount_cents": 1500}},
        headers={"Idempotency-Key": key},
    )
    assert response.status_code == 201
    return response.json()["id"]


@pytest.fixture
def admin_client(settings, clock):
    admin = replace(settings, enable_admin_endpoints=True, admin_api_token=TEST_ADMIN_TOKEN)
    with TestClient(
        create_app(admin, clock=clock, jitter=no_jitter),
        headers={"Authorization": f"Bearer {TEST_ADMIN_TOKEN}"},
    ) as client:
        yield client


def test_admin_endpoints_are_disabled_by_default(client):
    op_id = create(client)
    assert client.post("/worker/run").status_code == 404
    assert client.put(f"/simulator/fault-plans/{op_id}", json={"faults": []}).status_code == 404


def test_worker_endpoint_recovers_operations(admin_client, clock):
    op_id = create(admin_client)
    planned = admin_client.put(
        f"/simulator/fault-plans/{op_id}", json={"faults": [{"scenario": "http_503"}]}
    )
    assert planned.status_code == 200

    first = admin_client.post("/worker/run", params={"worker_id": "n8n"}).json()
    assert first["processed"] == 1
    assert first["items"][0]["final_status"] == "failed_retryable"

    operation = admin_client.get(f"/operations/{op_id}").json()
    assert operation["next_attempt_at"] is not None
    [attempt] = operation["attempts"]
    assert attempt["kind"] == "submit"
    assert attempt["worker_id"] == "n8n"
    assert attempt["retry_decision"].startswith("retry in ")

    assert admin_client.post("/worker/run").json()["processed"] == 0  # todavía no vence
    clock.advance(3600)
    second = admin_client.post("/worker/run").json()
    assert second["items"][0]["final_status"] == "succeeded"
    assert admin_client.get("/simulator/duplicates").json() == []


def test_execute_then_worker_reconciles_timeout_after_effect(admin_client, clock):
    op_id = create(admin_client)
    executed = admin_client.post(
        f"/operations/{op_id}/execute", json={"scenario": "timeout_after_processing"}
    ).json()
    assert executed["operation"]["status"] == "outcome_unknown"
    assert executed["attempt"]["worker_id"] == "api"

    clock.advance(3600)
    report = admin_client.post("/worker/run").json()
    assert report["items"][0]["action"] == "reconciled"

    final = admin_client.get(f"/operations/{op_id}").json()
    [effect] = admin_client.get("/simulator/effects", params={"operation_ref": op_id}).json()
    assert final["status"] == "succeeded"
    assert final["external_effect_id"] == effect["effect_id"]
    [lookup] = admin_client.get("/simulator/lookups", params={"idempotency_key": op_id}).json()
    assert lookup["found"] is True


@pytest.mark.parametrize("params", [{"max_operations": 0}, {"max_operations": 101}])
def test_worker_endpoint_validates_bounds(admin_client, params):
    assert admin_client.post("/worker/run", params=params).status_code == 422


def test_fault_plan_endpoint_rejects_unknown_scenarios(admin_client):
    response = admin_client.put("/simulator/fault-plans/op_x", json={"faults": [{"scenario": "x"}]})
    assert response.status_code == 422


def test_cli_fault_plan_worker_and_show(settings, clock, capsys, tmp_path):
    runtime = build_runtime(settings, clock=clock, jitter=no_jitter)
    with TestClient(create_app(settings, clock=clock, jitter=no_jitter)) as client:
        op_id = create(client)

    assert main(["fault-plan", op_id, "http_429@15", "http_503"], runtime=runtime) == 0
    planned = json.loads(capsys.readouterr().out)
    assert planned["pending_faults"] == [
        {"scenario": "http_429", "retry_after": "15"},
        {"scenario": "http_503", "retry_after": None},
    ]

    assert main(["worker", "--worker-id", "cli"], runtime=runtime) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["processed"] == 1
    assert report["items"][0]["final_status"] == "failed_retryable"

    assert main(["show", op_id], runtime=runtime) == 0
    shown = json.loads(capsys.readouterr().out)
    assert shown["operation"]["status"] == "failed_retryable"
    assert shown["attempts"][0]["retry_after"] == "15"
    assert len(shown["provider"]["requests"]) == 1


def test_cli_rejects_invalid_scenario(settings, capsys):
    runtime = build_runtime(settings)
    with pytest.raises(SystemExit) as excinfo:
        main(["fault-plan", "op_x", "explode"], runtime=runtime)
    assert excinfo.value.code == 2
    assert "escenario inválido" in capsys.readouterr().err
