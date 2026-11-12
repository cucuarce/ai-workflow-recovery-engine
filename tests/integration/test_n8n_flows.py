"""Etapa 3: los caminos HTTP que siguen los workflows de n8n, con la configuración de demo.

Workflow 1 (intake):   POST /operations -> (si pending) POST /execute -> GET /operations/{id}
Workflow 2 (recovery): POST /worker/run
Operador (demo):       PUT /simulator/fault-plans/by-idempotency-key/{key} (antes del pedido)

Toda la lógica de recuperación está en el motor; estos tests verifican que n8n solo
necesita orquestar esas llamadas y que los fallos de orquestación no pierden ni duplican.
"""

from __future__ import annotations

import threading
from contextlib import closing

import pytest
from fastapi.testclient import TestClient

from recovery_engine.api.app import create_app
from recovery_engine.config import AppEnv, Settings
from recovery_engine.domain.errors import OperationNotExecutableError
from recovery_engine.domain.retry import no_jitter
from recovery_engine.services.operations import OperationService
from support import LEASE_TTL, POLICY, TEST_ADMIN_TOKEN, TEST_ENGINE_TOKEN

API = {"Authorization": f"Bearer {TEST_ENGINE_TOKEN}"}
ADMIN = {"Authorization": f"Bearer {TEST_ADMIN_TOKEN}"}
BODY = {"operation_type": "issue_invoice", "payload": {"amount_cents": 1500}}


@pytest.fixture
def demo_settings(tmp_path) -> Settings:
    return Settings(
        engine_db_path=tmp_path / "engine.db",
        simulator_db_path=tmp_path / "external.db",
        app_env=AppEnv.DEMO,
        engine_api_token=TEST_ENGINE_TOKEN,
        admin_api_token=TEST_ADMIN_TOKEN,
        enable_admin_endpoints=True,
        fault_injection_enabled=True,
        retry_policy=POLICY,
        retry_jitter=False,
        lease_ttl_seconds=LEASE_TTL,
    )


@pytest.fixture
def api(demo_settings, clock):
    with TestClient(create_app(demo_settings, clock=clock, jitter=no_jitter)) as client:
        yield client


def register(api, key, body=BODY):
    return api.post("/operations", json=body, headers={**API, "Idempotency-Key": key})


def execute(api, op_id):
    return api.post(f"/operations/{op_id}/execute", headers=API)


def run_worker(api, worker_id="n8n-recovery"):
    response = api.post("/worker/run", params={"worker_id": worker_id}, headers=ADMIN)
    assert response.status_code == 200
    return response.json()


def operation(api, op_id):
    return api.get(f"/operations/{op_id}", headers=API).json()


def provider(api, op_id):
    def get(path, **params):
        return api.get(f"/simulator/{path}", params=params, headers=ADMIN).json()

    return {
        "requests": get("requests", operation_ref=op_id),
        "effects": get("effects", operation_ref=op_id),
        "lookups": get("lookups", idempotency_key=op_id),
        "duplicates": get("duplicates"),
    }


def intake(api, key, body=BODY):
    """Lo que hace el Workflow 1, nodo por nodo."""
    created = register(api, key, body)
    assert created.status_code in (200, 201)
    op = created.json()
    if op["status"] == "pending":
        execute(api, op["id"])  # 200, o 409 si otro (worker / webhook duplicado) lo tomó
    return created.status_code, operation(api, op["id"])


# ------------------------------------------- 3. operaciones pending sin primer execute


def test_pending_operation_abandoned_before_execute_is_recovered_by_worker(api):
    # n8n registra la operación y muere antes de "Ejecutar primer intento".
    op_id = register(api, "crash-before-execute").json()["id"]
    assert operation(api, op_id)["status"] == "pending"
    assert provider(api, op_id)["requests"] == []

    report = run_worker(api)

    [item] = report["items"]
    assert (item["operation_id"], item["claimed_from"], item["final_status"]) == (
        op_id,
        "pending",
        "succeeded",
    )
    evidence = provider(api, op_id)
    assert len(evidence["requests"]) == 1
    assert len(evidence["effects"]) == 1
    assert [a["worker_id"] for a in operation(api, op_id)["attempts"]] == ["n8n-recovery"]


def test_duplicate_webhook_after_recovery_replays_without_a_new_effect(api):
    op_id = register(api, "dup-after-recovery").json()["id"]
    run_worker(api)

    status, final = intake(api, "dup-after-recovery")

    assert status == 200  # replay idempotente: Workflow 1 no vuelve a ejecutar
    assert (final["id"], final["status"]) == (op_id, "succeeded")
    assert execute(api, op_id).status_code == 409
    assert len(provider(api, op_id)["requests"]) == 1
    assert len(provider(api, op_id)["effects"]) == 1


def test_duplicate_webhook_while_pending_dispatches_a_single_attempt(api):
    first = register(api, "dup-pending").json()
    replay = register(api, "dup-pending")
    assert replay.status_code == 200
    assert replay.headers["Idempotent-Replayed"] == "true"
    assert replay.json()["id"] == first["id"]

    # Ambas ejecuciones del Workflow 1 ven 'pending' e intentan el primer execute.
    assert execute(api, first["id"]).status_code == 200
    second = execute(api, first["id"])
    assert second.status_code == 409
    assert second.json()["error"] == "operation_not_executable"

    assert len(provider(api, first["id"])["requests"]) == 1
    assert run_worker(api)["processed"] == 0


def test_worker_wins_the_race_and_intake_execute_is_rejected(api):
    op_id = register(api, "race").json()["id"]
    run_worker(api)  # el Schedule llega antes que el nodo de execute

    late = execute(api, op_id)

    assert late.status_code == 409
    assert operation(api, op_id)["status"] == "succeeded"
    assert len(provider(api, op_id)["requests"]) == 1


def test_concurrent_intake_and_worker_on_pending_operations(
    engine_db, simulator, make_worker, clock
):
    with closing(engine_db.connect()) as conn:
        service = OperationService(conn, simulator)
        ids = [
            service.create(
                idempotency_key=f"conc-{i}", operation_type="issue_invoice", payload={"i": i}
            ).operation.id
            for i in range(20)
        ]
    barrier = threading.Barrier(3)
    rejected: list[str] = []
    errors: list[BaseException] = []

    def intake_thread(op_ids):
        try:
            with closing(engine_db.connect()) as own:
                svc = OperationService(
                    own,
                    simulator,
                    clock=clock,
                    policy=POLICY,
                    jitter=no_jitter,
                    lease_ttl_seconds=LEASE_TTL,
                )
                barrier.wait()
                for op_id in op_ids:
                    try:
                        svc.execute(op_id)
                    except OperationNotExecutableError:
                        rejected.append(op_id)
        except BaseException as exc:  # pragma: no cover - se reporta abajo
            errors.append(exc)

    def worker_thread():
        try:
            barrier.wait()
            for _ in range(5):
                make_worker("n8n-recovery").run_once(len(ids))
        except BaseException as exc:  # pragma: no cover
            errors.append(exc)

    threads = [
        threading.Thread(target=intake_thread, args=(ids,)),
        threading.Thread(target=intake_thread, args=(list(reversed(ids)),)),
        threading.Thread(target=worker_thread),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)

    assert errors == []
    assert simulator.find_duplicate_effects() == []
    for op_id in ids:
        assert len(simulator.list_requests(op_id)) == 1
    assert len(rejected) >= len(ids)  # cada op: un ganador, el resto recibe 409/excepción
    with closing(engine_db.connect()) as conn:
        statuses = {OperationService(conn, simulator).get(i).status.value for i in ids}
    assert statuses == {"succeeded"}


# --------------------------------------------- 4. evidencia de reconciliación (demo)


def test_timeout_after_effect_produces_single_effect_and_no_resend(api, clock):
    key = "demo-timeout"
    planned = api.put(
        f"/simulator/fault-plans/by-idempotency-key/{key}",
        json={"faults": [{"scenario": "timeout_after_processing"}]},
        headers=ADMIN,
    )
    assert planned.status_code == 200

    status, after_intake = intake(api, key)

    assert status == 201
    assert after_intake["status"] == "outcome_unknown"
    op_id = after_intake["id"]
    before = provider(api, op_id)
    [original] = before["requests"]
    assert original["scenario"] == "timeout_after_processing"
    assert original["effect_applied"] is True
    assert original["delivered_to_client"] is False
    assert len(before["effects"]) == 1
    assert before["lookups"] == []

    clock.advance(60)  # vence el backoff; el Schedule dispara el Workflow 2
    [item] = run_worker(api)["items"]
    assert (item["action"], item["final_status"]) == ("reconciled", "succeeded")

    final = operation(api, op_id)
    after = provider(api, op_id)
    [effect] = after["effects"]
    # 1) una solicitud original; 3) ningún reenvío durante la reconciliación
    assert [r["id"] for r in after["requests"]] == [original["id"]]
    # 2) un único efecto
    assert effect["effect_id"] == original["effect_id"]
    # la reconciliación es una consulta de solo lectura que encontró el efecto
    assert [(lk["found"], lk["effect_id"]) for lk in after["lookups"]] == [
        (True, effect["effect_id"])
    ]
    # 4) cero duplicados
    assert after["duplicates"] == []
    # 5) estado final succeeded, enlazado al efecto del proveedor
    assert final["status"] == "succeeded"
    assert final["external_effect_id"] == effect["effect_id"]
    assert [(a["kind"], a["outcome"], a["worker_id"]) for a in final["attempts"]] == [
        ("submit", "timeout", "api"),
        ("reconcile", "effect_found", "n8n-recovery"),
    ]

    # Un nuevo webhook con la misma clave no cambia nada.
    status, replayed = intake(api, key)
    assert (status, replayed["id"], replayed["status"]) == (200, op_id, "succeeded")
    assert len(provider(api, op_id)["requests"]) == 1


def test_fault_plan_by_key_applies_only_to_that_logical_request(api):
    api.put(
        "/simulator/fault-plans/by-idempotency-key/target",
        json={"faults": [{"scenario": "http_503"}]},
        headers=ADMIN,
    )
    _, other = intake(api, "other", {**BODY, "payload": {"amount_cents": 1}})
    _, target = intake(api, "target")
    assert other["status"] == "succeeded"
    assert target["status"] == "failed_retryable"


def test_webhook_caller_cannot_choose_scenarios_in_demo(api):
    op_id = register(api, "chooser").json()["id"]
    response = api.post(
        f"/operations/{op_id}/execute", json={"scenario": "timeout_after_processing"}, headers=API
    )
    assert response.status_code == 403
    assert provider(api, op_id)["requests"] == []


def test_dead_letter_is_terminal_for_the_intake_and_keeps_its_reason(api, clock):
    """El intake responde 409 ante ``dead_letter`` (ver Responder estado): aquí se verifica
    que el motor entrega lo que esa respuesta necesita y que repetir el webhook no reabre
    la recuperación automática."""
    key = "demo-dead-letter"
    api.put(
        f"/simulator/fault-plans/by-idempotency-key/{key}",
        json={"faults": [{"scenario": "http_503"}] * POLICY.max_attempts},
        headers=ADMIN,
    )
    _, op = intake(api, key)
    assert op["status"] == "failed_retryable"
    while op["status"] == "failed_retryable":
        clock.advance(POLICY.max_delay_seconds)
        run_worker(api)
        op = operation(api, op["id"])

    assert op["status"] == "dead_letter"
    assert op["attempt_count"] == POLICY.max_attempts
    assert op["last_error"]  # el motivo que el intake devuelve como "reason"
    requests_before = len(provider(api, op["id"])["requests"])

    status, replayed = intake(api, key)
    assert (status, replayed["id"], replayed["status"]) == (200, op["id"], "dead_letter")
    assert replayed["last_error"] == op["last_error"]
    assert len(provider(api, op["id"])["requests"]) == requests_before
    clock.advance(3600)
    assert run_worker(api)["items"] == []
