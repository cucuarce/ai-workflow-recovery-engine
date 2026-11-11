"""Concurrencia real: hilos con conexiones SQLite independientes."""

import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing

from recovery_engine.domain.errors import IdempotencyConflictError, OperationNotExecutableError
from recovery_engine.services.operations import OperationService

WORKERS = 16
PAYLOAD = {"customer_id": "c-001", "amount_cents": 1500}


def run_concurrently(fn, workers: int = WORKERS) -> list:
    """Ejecuta ``fn(i)`` en paralelo y devuelve el resultado o la excepción de cada hilo."""
    barrier = threading.Barrier(workers)

    def wrapped(i):
        barrier.wait()
        try:
            return fn(i)
        except Exception as exc:
            return exc

    with ThreadPoolExecutor(workers) as pool:
        return list(pool.map(wrapped, range(workers)))


def count_operations(engine_db) -> int:
    with closing(engine_db.connect()) as conn:
        return conn.execute("SELECT COUNT(*) FROM operations").fetchone()[0]


def test_concurrent_creates_with_same_key_create_one_operation(engine_db, simulator):
    def create(_):
        with closing(engine_db.connect()) as conn:
            service = OperationService(conn, simulator)
            return service.create(idempotency_key="k", operation_type="t", payload=PAYLOAD)

    results = run_concurrently(create)

    assert all(not isinstance(r, Exception) for r in results), results
    assert sum(r.created for r in results) == 1
    assert len({r.operation.id for r in results}) == 1
    assert count_operations(engine_db) == 1


def test_concurrent_creates_with_same_key_and_different_payloads(engine_db, simulator):
    def create(i):
        with closing(engine_db.connect()) as conn:
            service = OperationService(conn, simulator)
            return service.create(idempotency_key="k", operation_type="t", payload={"v": i % 2})

    results = run_concurrently(create)

    successes = [r for r in results if not isinstance(r, Exception)]
    conflicts = [r for r in results if isinstance(r, IdempotencyConflictError)]
    assert len(successes) + len(conflicts) == WORKERS
    assert sum(r.created for r in successes) == 1
    winner = next(r.operation for r in successes if r.created)
    # Quien no ganó solo puede reutilizar si su payload coincide con el del ganador.
    assert all(r.operation.id == winner.id for r in successes)
    assert all(r.operation.payload == winner.payload for r in successes)
    assert len(conflicts) == WORKERS // 2
    assert count_operations(engine_db) == 1


def test_concurrent_executions_dispatch_a_single_attempt(engine_db, simulator, make_service):
    op_id = make_service().create(idempotency_key="k", operation_type="t", payload=PAYLOAD)
    op_id = op_id.operation.id

    def execute(_):
        with closing(engine_db.connect()) as conn:
            return OperationService(conn, simulator).execute(op_id)

    results = run_concurrently(execute)

    executed = [r for r in results if not isinstance(r, Exception)]
    rejected = [r for r in results if isinstance(r, OperationNotExecutableError)]
    assert len(executed) == 1
    assert len(rejected) == WORKERS - 1
    detail = make_service().get_detail(op_id)
    assert len(detail.attempts) == 1
    assert len(simulator.list_requests(op_id)) == 1
    assert len(simulator.list_effects(op_id)) == 1


def test_api_concurrent_requests_with_same_key(client):
    def post(_):
        return client.post(
            "/operations",
            json={"operation_type": "issue_invoice", "payload": PAYLOAD},
            headers={"Idempotency-Key": "api-key"},
        )

    responses = run_concurrently(post, workers=10)

    assert sorted(r.status_code for r in responses) == [200] * 9 + [201]
    assert len({r.json()["id"] for r in responses}) == 1
    assert len(client.get("/operations").json()) == 1
