"""Fábrica de la aplicación FastAPI.

Ejecutar con: ``uv run uvicorn recovery_engine.api.app:create_app --factory``
"""

from __future__ import annotations

from fastapi import Depends, FastAPI

from recovery_engine import __version__
from recovery_engine.api import (
    admin_routes,
    fault_plan_routes,
    operations_routes,
    simulator_routes,
)
from recovery_engine.api.errors import register_exception_handlers
from recovery_engine.api.security import require_admin_token, require_engine_token
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
    # Settings valida sus combinaciones al construirse: una configuración insegura
    # (p. ej. producción sin token o con inyección de fallos) no llega hasta aquí.
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

    engine_auth = [Depends(require_engine_token)]
    admin_auth = [Depends(require_admin_token)]
    app.include_router(operations_routes.router, dependencies=engine_auth)
    if settings.observability_routes_enabled:
        app.include_router(simulator_routes.router, dependencies=admin_auth)
    if settings.enable_admin_endpoints:
        app.include_router(admin_routes.router, dependencies=admin_auth)
    if settings.fault_plan_routes_enabled:
        app.include_router(fault_plan_routes.router, dependencies=admin_auth)
    register_exception_handlers(app)

    @app.get("/health", tags=["meta"])
    def health() -> dict[str, str]:
        return {"status": "ok", "version": __version__}

    return app
