"""Endpoints administrativos (deshabilitados por defecto).

ADVERTENCIA: no tienen autenticación ni autorización. Solo se montan con
``ENABLE_ADMIN_ENDPOINTS=true`` y están pensados para uso local (uvicorn escucha en
127.0.0.1 por defecto) y para que n8n dispare el worker en la Etapa 3. No exponerlos a una
red no confiable: permiten forzar procesamiento y reescribir el guion de fallos del simulador.
"""

from __future__ import annotations

from dataclasses import asdict
from typing import Annotated

from fastapi import APIRouter, Query
from pydantic import BaseModel, ConfigDict, Field

from recovery_engine.api.deps import RuntimeDep, SimulatorDep
from recovery_engine.external.contracts import Scenario
from recovery_engine.external.simulator import FaultSpec
from recovery_engine.services.worker import WorkerReport

router = APIRouter(tags=["admin (local only)"])


class WorkerRunOut(BaseModel):
    worker_id: str
    started_at: str
    finished_at: str
    max_operations: int
    processed: int
    items: list[dict]

    @classmethod
    def from_report(cls, report: WorkerReport) -> WorkerRunOut:

        return cls(
            worker_id=report.worker_id,
            started_at=report.started_at,
            finished_at=report.finished_at,
            max_operations=report.max_operations,
            processed=report.processed,
            items=[asdict(item) for item in report.items],
        )


class FaultIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    scenario: Scenario
    retry_after: str | None = Field(
        default=None, description="Valor crudo de Retry-After para http_429."
    )


class FaultPlanIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    faults: list[FaultIn] = Field(max_length=50)


@router.post("/worker/run", response_model=WorkerRunOut)
def run_worker(
    runtime: RuntimeDep,
    max_operations: Annotated[int, Query(ge=1, le=100)] = 10,
    worker_id: Annotated[str, Query(min_length=1, max_length=64)] = "http-worker",
) -> WorkerRunOut:
    """Una corrida acotada del worker. Seguro ante llamadas repetidas o concurrentes."""
    return WorkerRunOut.from_report(runtime.worker(worker_id).run_once(max_operations))


@router.put("/simulator/fault-plans/{operation_ref}", response_model=list[FaultIn])
def set_fault_plan(operation_ref: str, body: FaultPlanIn, simulator: SimulatorDep) -> list[FaultIn]:
    """Reemplaza el guion de fallos pendiente de una operación (solo simulador)."""
    simulator.set_fault_plan(
        operation_ref, [FaultSpec(f.scenario, f.retry_after) for f in body.faults]
    )
    return [FaultIn(scenario=f.scenario, retry_after=f.retry_after) for f in body.faults]
