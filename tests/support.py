"""Dobles de prueba compartidos: reloj controlable y clientes que simulan caídas."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Literal

from recovery_engine.clock import format_timestamp, parse_timestamp
from recovery_engine.config import AppEnv
from recovery_engine.domain.retry import RetryPolicy
from recovery_engine.external.contracts import ExternalLookup, ExternalRequest, ExternalResponse
from recovery_engine.external.simulator import ExternalServiceSimulator

START = "2026-01-01T00:00:00.000000+00:00"

# Configuración explícita de los tests de Etapas 1 y 2: entorno local (sin token de API) con
# inyección de fallos habilitada. Settings() por defecto es "production" y no la permite.
LOCAL_TEST_CONFIG = {"app_env": AppEnv.LOCAL, "fault_injection_enabled": True}

# Valores ficticios exclusivos de tests (no son credenciales).
TEST_ENGINE_TOKEN = "test-engine-token-0000000000000000"
TEST_ADMIN_TOKEN = "test-admin-token-11111111111111111"

# Parámetros fijos de los tests de recuperación.
LEASE_TTL = 30.0
POLICY = RetryPolicy(
    max_attempts=4,
    base_delay_seconds=2.0,
    max_delay_seconds=30.0,
    max_retry_after_seconds=600.0,
)


class FakeClock:
    """Reloj determinístico: solo avanza cuando el test lo pide (sin sleeps)."""

    def __init__(self, start: str = START) -> None:
        self._now = parse_timestamp(start)

    def __call__(self) -> str:
        return format_timestamp(self._now)

    @property
    def now(self) -> datetime:
        return self._now

    def advance(self, seconds: float) -> None:
        self._now += timedelta(seconds=seconds)


class SimulatedCrash(BaseException):
    """Muerte del proceso. Hereda de BaseException: ningún ``except Exception`` la atrapa."""


class CrashingClient:
    """Delega en el simulador pero "mata el proceso" antes o después del envío."""

    def __init__(
        self,
        inner: ExternalServiceSimulator,
        *,
        crash: Literal["before_submit", "after_submit"],
    ) -> None:
        self._inner = inner
        self._crash = crash

    @property
    def supports_idempotency(self) -> bool:
        return self._inner.supports_idempotency

    def lookup(self, idempotency_key: str) -> ExternalLookup:
        return self._inner.lookup(idempotency_key)

    def submit(self, request: ExternalRequest) -> ExternalResponse:
        if self._crash == "before_submit":
            raise SimulatedCrash("crash after reserving the attempt, before calling provider")
        try:
            self._inner.submit(request)
        finally:
            raise SimulatedCrash("crash after provider effect, before recording the result")


class SubmitOnlyClient:
    """Proveedor sin endpoint de consulta (no hay reconciliación por lookup)."""

    def __init__(self, inner: ExternalServiceSimulator, *, supports_idempotency: bool) -> None:
        self._inner = inner
        self.supports_idempotency = supports_idempotency

    def submit(self, request: ExternalRequest) -> ExternalResponse:
        return self._inner.submit(request)


class HookClient:
    """Ejecuta callbacks alrededor del envío (para intercalar otros actores en vuelo)."""

    def __init__(
        self,
        inner: ExternalServiceSimulator,
        *,
        before_submit: Callable[[], None] | None = None,
        after_submit: Callable[[], None] | None = None,
    ) -> None:
        self._inner = inner
        self._before = before_submit
        self._after = after_submit

    @property
    def supports_idempotency(self) -> bool:
        return self._inner.supports_idempotency

    def lookup(self, idempotency_key: str) -> ExternalLookup:
        return self._inner.lookup(idempotency_key)

    def submit(self, request: ExternalRequest) -> ExternalResponse:
        if self._before:
            self._before()
        response = self._inner.submit(request)
        if self._after:
            self._after()
        return response
