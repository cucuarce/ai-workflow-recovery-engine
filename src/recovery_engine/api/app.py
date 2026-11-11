"""Fábrica de la aplicación FastAPI.

Ejecutar con: ``uv run uvicorn recovery_engine.api.app:create_app --factory``
"""

from __future__ import annotations

from fastapi import FastAPI

from recovery_engine import __version__
from recovery_engine.api import admin_routes, operations_routes, simulator_routes
from recovery_engine.api.errors import register_exception_handlers
from recovery_engine.clock import Clock
from recovery_engine.config import Settings
from recovery_engine.domain.retry import Jitter
from recovery_engine.runtime import build_runtime


def create_app(
    settings: Settings | None = None,
    *,
    clock: Clock | None = None,
    jitter: Jitter | None = None,
) -> FastAPI:
    settings = settings or Settings.from_env()
    runtime = build_runtime(settings, clock=clock, jitter=jitter)

    app = FastAPI(
        title="AI Workflow Recovery Engine",
        version=__version__,
        description=(
            "Registro idempotente, simulación de fallos, reintentos con backoff, "
            "lease/fencing, reconciliación y dead-letter."
        ),
    )
    app.state.settings = settings
    app.state.runtime = runtime
    app.state.engine_db = runtime.engine_db
    app.state.simulator = runtime.simulator

    app.include_router(operations_routes.router)
    app.include_router(simulator_routes.router)
    if settings.enable_admin_endpoints:
        # Sin autenticación: pensado solo para uso local y para el disparo desde n8n (Etapa 3).
        app.include_router(admin_routes.router)
    register_exception_handlers(app)

    @app.get("/health", tags=["meta"])
    def health() -> dict[str, str]:
        return {"status": "ok", "version": __version__}

    return app
