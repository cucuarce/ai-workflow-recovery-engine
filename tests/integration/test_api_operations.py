from fastapi.testclient import TestClient

from recovery_engine.api.app import create_app

PAYLOAD = {"customer_id": "c-001", "amount_cents": 1500}


def create(client, key="key-1", payload=None, operation_type="issue_invoice"):
    return client.post(
        "/operations",
        json={"operation_type": operation_type, "payload": PAYLOAD if payload is None else payload},
        headers={"Idempotency-Key": key},
    )


def test_health(client):
    assert client.get("/health").json()["status"] == "ok"


def test_create_operation(client):
    response = create(client)

    assert response.status_code == 201
    body = response.json()
    assert body["id"].startswith("op_")
    assert body["idempotency_key"] == "key-1"
    assert body["status"] == "pending"
    assert body["attempt_count"] == 0
    assert body["payload"] == PAYLOAD
    assert "Idempotent-Replayed" not in response.headers


def test_get_operation_detail(client):
    op_id = create(client).json()["id"]

    response = client.get(f"/operations/{op_id}")

    assert response.status_code == 200
    body = response.json()
    assert body["id"] == op_id
    assert body["attempts"] == []
    [transition] = body["transitions"]
    assert transition["from_status"] is None
    assert transition["to_status"] == "pending"


def test_get_unknown_operation_returns_404(client):
    response = client.get("/operations/op_missing")
    assert response.status_code == 404
    assert response.json()["error"] == "operation_not_found"


def test_same_key_same_payload_reuses_operation(client):
    first = create(client)
    second = create(client)

    assert first.status_code == 201
    assert second.status_code == 200
    assert second.headers["Idempotent-Replayed"] == "true"
    assert second.json()["id"] == first.json()["id"]
    assert len(client.get("/operations").json()) == 1


def test_payload_key_order_does_not_matter(client):
    first = create(client, payload={"a": 1, "b": {"x": 1, "y": 2}})
    second = create(client, payload={"b": {"y": 2, "x": 1}, "a": 1})
    assert second.status_code == 200
    assert second.json()["id"] == first.json()["id"]


def test_same_key_different_payload_conflicts(client):
    original = create(client)
    response = create(client, payload={"customer_id": "c-001", "amount_cents": 9999})

    assert response.status_code == 409
    body = response.json()
    assert body["error"] == "idempotency_conflict"
    assert original.json()["id"] in body["detail"]
    assert len(client.get("/operations").json()) == 1


def test_same_key_different_operation_type_conflicts(client):
    create(client)
    assert create(client, operation_type="refund").status_code == 409


def test_different_keys_create_different_operations(client):
    a = create(client, key="k-a").json()["id"]
    b = create(client, key="k-b").json()["id"]
    assert a != b


def test_missing_idempotency_key_is_rejected(client):
    response = client.post("/operations", json={"operation_type": "t", "payload": {}})
    assert response.status_code == 422


def test_empty_idempotency_key_is_rejected(client):
    response = client.post(
        "/operations", json={"operation_type": "t", "payload": {}}, headers={"Idempotency-Key": ""}
    )
    assert response.status_code == 422


def test_unknown_body_fields_are_rejected(client):
    response = client.post(
        "/operations",
        json={"operation_type": "t", "payload": {}, "status": "succeeded"},
        headers={"Idempotency-Key": "k"},
    )
    assert response.status_code == 422


def test_list_filters_by_status(client):
    pending = create(client, key="k-1").json()["id"]
    executed = create(client, key="k-2").json()["id"]
    client.post(f"/operations/{executed}/execute", json={"scenario": "success"})

    pending_ids = [
        op["id"] for op in client.get("/operations", params={"status": "pending"}).json()
    ]
    succeeded_ids = [
        op["id"] for op in client.get("/operations", params={"status": "succeeded"}).json()
    ]
    assert pending_ids == [pending]
    assert succeeded_ids == [executed]


def test_operations_persist_across_restarts(settings):
    with TestClient(create_app(settings)) as first_app:
        op_id = create(first_app).json()["id"]

    with TestClient(create_app(settings)) as second_app:
        assert second_app.get(f"/operations/{op_id}").status_code == 200
        # La idempotencia también sobrevive al reinicio.
        replay = create(second_app)
        assert replay.status_code == 200
        assert replay.json()["id"] == op_id
