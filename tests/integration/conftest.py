"""Fixtures de la Etapa 2: reloj controlable, política fija, workers y procesadores."""

from __future__ import annotations

import itertools
from collections.abc import Callable, Iterator
from contextlib import ExitStack, closing

import pytest

from recovery_engine.domain.models import Operation
from recovery_engine.domain.retry import RetryPolicy, no_jitter
from recovery_engine.external.contracts import ExternalServiceClient
from recovery_engine.services.operations import OperationService
from recovery_engine.services.processing import OperationProcessor
from recovery_engine.services.worker import RecoveryWorker
from support import LEASE_TTL, POLICY, FakeClock


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def service(engine_db, simulator, clock) -> Iterator[OperationService]:
    with closing(engine_db.connect()) as conn:
        yield OperationService(
            conn,
            simulator,
            clock=clock,
            policy=POLICY,
            jitter=no_jitter,
            lease_ttl_seconds=LEASE_TTL,
        )


@pytest.fixture
def new_operation(service) -> Callable[..., Operation]:
    counter = itertools.count(1)

    def factory(payload: dict | None = None) -> Operation:
        return service.create(
            idempotency_key=f"key-{next(counter)}",
            operation_type="issue_invoice",
            payload=payload if payload is not None else {"amount_cents": 1500},
        ).operation

    return factory


@pytest.fixture
def make_worker(engine_db, simulator, clock) -> Callable[..., RecoveryWorker]:
    def factory(
        worker_id: str = "worker-a",
        client: ExternalServiceClient | None = None,
        policy: RetryPolicy = POLICY,
    ) -> RecoveryWorker:
        return RecoveryWorker(
            engine_db,
            client or simulator,
            worker_id=worker_id,
            policy=policy,
            clock=clock,
            jitter=no_jitter,
            lease_ttl_seconds=LEASE_TTL,
        )

    return factory


@pytest.fixture
def make_processor(engine_db, simulator, clock) -> Iterator[Callable[..., OperationProcessor]]:
    with ExitStack() as stack:

        def factory(client: ExternalServiceClient | None = None) -> OperationProcessor:
            conn = stack.enter_context(closing(engine_db.connect()))
            return OperationProcessor(
                conn,
                client or simulator,
                policy=POLICY,
                clock=clock,
                jitter=no_jitter,
                lease_ttl_seconds=LEASE_TTL,
            )

        yield factory
