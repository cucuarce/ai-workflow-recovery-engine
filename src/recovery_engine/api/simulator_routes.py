"""Endpoints de observación del servicio externo simulado (solo lectura)."""

from __future__ import annotations

from fastapi import APIRouter

from recovery_engine.api.deps import SimulatorDep
from recovery_engine.external.simulator import (
    DuplicateEffectReport,
    ExternalEffect,
    ExternalLookupRecord,
    ExternalRequestRecord,
)

router = APIRouter(prefix="/simulator", tags=["simulator"])


@router.get("/requests", response_model=list[ExternalRequestRecord])
def list_requests(
    simulator: SimulatorDep, operation_ref: str | None = None
) -> list[ExternalRequestRecord]:
    """Todo pedido recibido por el servicio externo y lo que realmente ocurrió con él."""
    return simulator.list_requests(operation_ref)


@router.get("/effects", response_model=list[ExternalEffect])
def list_effects(simulator: SimulatorDep, operation_ref: str | None = None) -> list[ExternalEffect]:
    """Efectos de negocio aplicados por el servicio externo."""
    return simulator.list_effects(operation_ref)


@router.get("/duplicates", response_model=list[DuplicateEffectReport])
def list_duplicates(simulator: SimulatorDep) -> list[DuplicateEffectReport]:
    """Operaciones lógicas con más de un efecto aplicado. Vacío = sin duplicaciones."""
    return simulator.find_duplicate_effects()


@router.get("/lookups", response_model=list[ExternalLookupRecord])
def list_lookups(
    simulator: SimulatorDep, idempotency_key: str | None = None
) -> list[ExternalLookupRecord]:
    """Consultas de reconciliación recibidas por el proveedor (no modifican efectos)."""
    return simulator.list_lookups(idempotency_key)
