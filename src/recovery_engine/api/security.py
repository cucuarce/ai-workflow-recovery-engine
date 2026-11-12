"""Autenticación Bearer con dos credenciales separadas.

- ``ENGINE_API_TOKEN``: ``/operations*``. Es lo único que necesita el Workflow 1 de n8n.
- ``ADMIN_API_TOKEN``: worker, planes de fallos y observación del simulador (Workflow 2,
  operador local). Las rutas del simulador solo existen en ``demo`` y ``local``.

Un token no sirve para la superficie del otro. Si el token de una superficie no está
configurado, ``Settings`` ya garantizó que el entorno es ``local``; en cualquier otro
entorno la aplicación no arranca sin él.

Los valores se comparan en tiempo constante y nunca se loguean ni se devuelven.
"""

from __future__ import annotations

import hmac

from fastapi import HTTPException, Request, status

from recovery_engine.config import Settings

_CHALLENGE = {"WWW-Authenticate": "Bearer"}


def _presented_token(request: Request) -> str | None:
    header = request.headers.get("Authorization")
    if not header:
        return None
    scheme, _, value = header.partition(" ")
    if scheme.lower() != "bearer" or not value.strip():
        return None
    return value.strip()


def _check(request: Request, expected: str | None) -> None:
    if expected is None:
        return  # solo posible con APP_ENV=local (validado en Settings)
    presented = _presented_token(request)
    if presented is None:
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED,
            detail={"error": "unauthorized", "detail": "Falta el token Bearer"},
            headers=_CHALLENGE,
        )
    if not hmac.compare_digest(presented.encode(), expected.encode()):
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            detail={"error": "forbidden", "detail": "Token inválido para este recurso"},
        )


def _settings(request: Request) -> Settings:
    return request.app.state.settings


def require_engine_token(request: Request) -> None:
    _check(request, _settings(request).engine_api_token)


def require_admin_token(request: Request) -> None:
    _check(request, _settings(request).admin_api_token)
