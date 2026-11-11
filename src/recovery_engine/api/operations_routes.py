"""Endpoints de operaciones."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Body, Header, Query, Response, status

from recovery_engine.api.deps import ServiceDep
from recovery_engine.api.schemas import (
    AttemptOut,
    ErrorOut,
    ExecuteRequest,
    ExecutionOut,
    OperationCreate,
    OperationDetailOut,
    OperationOut,
    TransitionOut,
)
from recovery_engine.domain.models import OperationStatus

router = APIRouter(prefix="/operations", tags=["operations"])

IdempotencyKey = Annotated[
    str,
    Header(
        alias="Idempotency-Key",
        min_length=1,
        max_length=255,
        description="Clave provista por el cliente que identifica el pedido lógico.",
    ),
]


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    response_model=OperationOut,
    responses={
        200: {"model": OperationOut, "description": "Pedido repetido: se reutiliza la operación."},
        409: {"model": ErrorOut, "description": "Clave reutilizada con un payload distinto."},
    },
)
def create_operation(
    body: OperationCreate,
    response: Response,
    service: ServiceDep,
    idempotency_key: IdempotencyKey,
) -> OperationOut:
    result = service.create(
        idempotency_key=idempotency_key,
        operation_type=body.operation_type,
        payload=body.payload,
    )
    if not result.created:
        response.status_code = status.HTTP_200_OK
        response.headers["Idempotent-Replayed"] = "true"
    return OperationOut.model_validate(result.operation)


@router.get("", response_model=list[OperationOut])
def list_operations(
    service: ServiceDep,
    status_filter: Annotated[OperationStatus | None, Query(alias="status")] = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
) -> list[OperationOut]:
    return [OperationOut.model_validate(op) for op in service.list(status_filter, limit)]


@router.get(
    "/{operation_id}",
    response_model=OperationDetailOut,
    responses={404: {"model": ErrorOut}},
)
def get_operation(operation_id: str, service: ServiceDep) -> OperationDetailOut:
    detail = service.get_detail(operation_id)
    return OperationDetailOut(
        **OperationOut.model_validate(detail.operation).model_dump(),
        attempts=[AttemptOut.model_validate(a) for a in detail.attempts],
        transitions=[TransitionOut.model_validate(t) for t in detail.transitions],
    )


@router.post(
    "/{operation_id}/execute",
    response_model=ExecutionOut,
    responses={404: {"model": ErrorOut}, 409: {"model": ErrorOut}},
)
def execute_operation(
    operation_id: str,
    service: ServiceDep,
    body: Annotated[ExecuteRequest | None, Body()] = None,
) -> ExecutionOut:
    """Ejecuta un intento. Un fallo externo NO es un error HTTP: queda registrado en el intento."""
    request = body or ExecuteRequest()
    result = service.execute(operation_id, request.scenario)
    return ExecutionOut(
        operation=OperationOut.model_validate(result.operation),
        attempt=AttemptOut.model_validate(result.attempt),
    )
