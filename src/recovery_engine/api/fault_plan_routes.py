"""Planes de fallos del simulador (solo demo/local).

Se montan solo con ``FAULT_INJECTION_ENABLED=true`` y ``ENABLE_ADMIN_ENDPOINTS=true``
(prohibido en producción) y exigen ``ADMIN_API_TOKEN``. Así el operador configura el
fallo ANTES y el pedido se ejecuta después de forma normal: quien llama al webhook de
n8n no puede elegir escenarios.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Path
from pydantic import BaseModel, ConfigDict, Field

from recovery_engine.api.deps import SimulatorDep
from recovery_engine.external.contracts import Scenario
from recovery_engine.external.simulator import FaultSpec

router = APIRouter(prefix="/simulator/fault-plans", tags=["admin: fault injection"])


class FaultIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    scenario: Scenario
    retry_after: str | None = Field(
        default=None, max_length=64, description="Valor crudo de Retry-After para http_429."
    )


class FaultPlanIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    faults: list[FaultIn] = Field(max_length=50)


def _specs(body: FaultPlanIn) -> list[FaultSpec]:
    return [FaultSpec(f.scenario, f.retry_after) for f in body.faults]


def _echo(body: FaultPlanIn) -> list[FaultIn]:
    return [FaultIn(scenario=f.scenario, retry_after=f.retry_after) for f in body.faults]


@router.put("/by-idempotency-key/{idempotency_key}", response_model=list[FaultIn])
def set_fault_plan_for_idempotency_key(
    idempotency_key: Annotated[str, Path(min_length=1, max_length=255)],
    body: FaultPlanIn,
    simulator: SimulatorDep,
) -> list[FaultIn]:
    """Guion de fallos para el pedido lógico identificado por la ``Idempotency-Key`` del cliente.

    Se puede cargar antes de que la operación exista: se aplica a sus envíos al proveedor.
    """
    simulator.set_client_fault_plan(idempotency_key, _specs(body))
    return _echo(body)


@router.put("/{operation_ref}", response_model=list[FaultIn])
def set_fault_plan(operation_ref: str, body: FaultPlanIn, simulator: SimulatorDep) -> list[FaultIn]:
    """Reemplaza el guion de fallos pendiente de una operación ya registrada."""
    simulator.set_fault_plan(operation_ref, _specs(body))
    return _echo(body)
