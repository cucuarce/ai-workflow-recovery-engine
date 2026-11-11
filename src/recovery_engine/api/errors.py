"""Traducción de errores de dominio a respuestas HTTP."""

from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from recovery_engine.domain.errors import (
    ConcurrentModificationError,
    DomainError,
    IdempotencyConflictError,
    InvalidTransitionError,
    OperationNotExecutableError,
    OperationNotFoundError,
)

_HTTP_STATUS: dict[type[DomainError], int] = {
    OperationNotFoundError: 404,
    IdempotencyConflictError: 409,
    OperationNotExecutableError: 409,
    InvalidTransitionError: 409,
    ConcurrentModificationError: 409,
}


def _status_for(exc: DomainError) -> int:
    for error_type, status in _HTTP_STATUS.items():
        if isinstance(exc, error_type):
            return status
    return 400


async def _handle_domain_error(_: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, DomainError)
    return JSONResponse(
        status_code=_status_for(exc), content={"error": exc.code, "detail": str(exc)}
    )


def register_exception_handlers(app: FastAPI) -> None:
    app.add_exception_handler(DomainError, _handle_domain_error)
