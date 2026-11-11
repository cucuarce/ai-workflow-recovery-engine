"""Dependencias de FastAPI: una conexión SQLite por request."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Annotated

from fastapi import Depends, Request

from recovery_engine.external.simulator import ExternalServiceSimulator
from recovery_engine.runtime import Runtime
from recovery_engine.services.operations import OperationService


def get_runtime(request: Request) -> Runtime:
    return request.app.state.runtime


def get_simulator(request: Request) -> ExternalServiceSimulator:
    return request.app.state.simulator


def get_operation_service(request: Request) -> Iterator[OperationService]:
    runtime: Runtime = request.app.state.runtime
    conn = runtime.engine_db.connect()
    try:
        yield OperationService(
            conn,
            runtime.simulator,
            clock=runtime.clock,
            policy=runtime.settings.retry_policy,
            jitter=runtime.jitter,
            lease_ttl_seconds=runtime.settings.lease_ttl_seconds,
        )
    finally:
        conn.close()


ServiceDep = Annotated[OperationService, Depends(get_operation_service)]
SimulatorDep = Annotated[ExternalServiceSimulator, Depends(get_simulator)]
RuntimeDep = Annotated[Runtime, Depends(get_runtime)]
