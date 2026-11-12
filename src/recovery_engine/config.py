"""Configuración de la aplicación (segura por defecto).

``Settings`` valida sus combinaciones al construirse: una configuración inválida no puede
existir, así que la API y la CLI fallan al arrancar en vez de quedar expuestas.

Entornos (``APP_ENV``):

- ``production`` (por defecto): exige ``ENGINE_API_TOKEN``; prohíbe la inyección de fallos
  y no monta las rutas ``/simulator/*``.
- ``demo``: entorno Docker con n8n. Exige ``ENGINE_API_TOKEN``; permite planes de fallos
  solo a través de endpoints administrativos con ``ADMIN_API_TOKEN``.
- ``local``: desarrollo y tests en la máquina propia. Es el único entorno que permite
  operar sin token de API y enviar ``scenario`` en ``execute``. Debe escuchar solo en
  127.0.0.1.

En cualquier entorno, habilitar los endpoints administrativos exige ``ADMIN_API_TOKEN``,
y ese token debe ser distinto de ``ENGINE_API_TOKEN``.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path

from recovery_engine.domain.retry import RetryPolicy

MIN_TOKEN_LENGTH = 32


class AppEnv(StrEnum):
    PRODUCTION = "production"
    DEMO = "demo"
    LOCAL = "local"


class ConfigurationError(ValueError):
    """Combinación de configuración insegura o inválida. La aplicación no debe arrancar."""


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ConfigurationError(f"{name}: valor booleano inválido {value!r}")


def _env_secret(name: str) -> str | None:
    value = os.getenv(name)
    return value.strip() if value and value.strip() else None


@dataclass(frozen=True, slots=True)
class Settings:
    engine_db_path: Path
    simulator_db_path: Path
    app_env: AppEnv = AppEnv.PRODUCTION
    engine_api_token: str | None = field(default=None, repr=False)
    """Bearer para ``/operations*`` (lo usa n8n en el Workflow 1)."""
    admin_api_token: str | None = field(default=None, repr=False)
    """Bearer para el worker, los planes de fallos y la observación del simulador."""
    enable_admin_endpoints: bool = False
    """Monta ``POST /worker/run`` (y los planes de fallos si la inyección está habilitada)."""
    fault_injection_enabled: bool = False
    """Habilita la simulación de fallos a pedido. Prohibida en ``production``."""
    simulator_honor_idempotency: bool = True
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)
    retry_jitter: bool = True
    lease_ttl_seconds: float = 30.0

    def __post_init__(self) -> None:
        object.__setattr__(self, "app_env", AppEnv(self.app_env))
        errors = self._validation_errors()
        if errors:
            raise ConfigurationError("Configuración inválida: " + "; ".join(errors))

    def _validation_errors(self) -> list[str]:
        errors: list[str] = []
        for name in ("engine_api_token", "admin_api_token"):
            token = getattr(self, name)
            if token is not None and len(token) < MIN_TOKEN_LENGTH:
                errors.append(f"{name.upper()} debe tener al menos {MIN_TOKEN_LENGTH} caracteres")
        if self.engine_api_token is not None and self.engine_api_token == self.admin_api_token:
            errors.append("ENGINE_API_TOKEN y ADMIN_API_TOKEN deben ser distintos")
        if self.app_env is not AppEnv.LOCAL and self.engine_api_token is None:
            errors.append(f"APP_ENV={self.app_env.value} exige ENGINE_API_TOKEN")
        if self.enable_admin_endpoints and self.admin_api_token is None:
            errors.append("ENABLE_ADMIN_ENDPOINTS=true exige ADMIN_API_TOKEN")
        if self.fault_injection_enabled and self.app_env is AppEnv.PRODUCTION:
            errors.append("la inyección de fallos está prohibida con APP_ENV=production")
        if (
            self.app_env is not AppEnv.LOCAL
            and self.observability_routes_enabled
            and self.admin_api_token is None
        ):
            errors.append("la observación del simulador fuera de local exige ADMIN_API_TOKEN")
        if self.lease_ttl_seconds <= 0:
            errors.append("LEASE_TTL_SECONDS debe ser positivo")
        return errors

    # ------------------------------------------------------- superficie derivada

    @property
    def inline_scenarios_allowed(self) -> bool:
        """``scenario`` en el cuerpo de ``execute``: solo en ``local`` con inyección habilitada.

        En ``demo`` los fallos se configuran antes, por la interfaz administrativa, y el
        pedido se ejecuta de forma normal: quien llama al webhook no elige el fallo.
        """
        return self.fault_injection_enabled and self.app_env is AppEnv.LOCAL

    @property
    def fault_plan_routes_enabled(self) -> bool:
        return self.fault_injection_enabled and self.enable_admin_endpoints

    @property
    def observability_routes_enabled(self) -> bool:
        """``GET /simulator/*``: evidencia del lado del proveedor simulado.

        Nunca se monta con ``APP_ENV=production``, aun con credenciales administrativas:
        el simulador es una herramienta de demo y de pruebas, no parte del producto.
        """
        if self.app_env is AppEnv.PRODUCTION:
            return False
        return self.fault_injection_enabled or self.enable_admin_endpoints

    # ------------------------------------------------------------------ entorno

    @classmethod
    def from_env(cls) -> Settings:
        data_dir = Path(os.getenv("RECOVERY_DATA_DIR", "data"))
        raw_env = os.getenv("APP_ENV", AppEnv.PRODUCTION.value).strip().lower()
        try:
            app_env = AppEnv(raw_env)
        except ValueError as exc:
            valid = ", ".join(e.value for e in AppEnv)
            raise ConfigurationError(f"APP_ENV inválido {raw_env!r} (válidos: {valid})") from exc
        return cls(
            engine_db_path=Path(os.getenv("RECOVERY_ENGINE_DB", str(data_dir / "engine.db"))),
            simulator_db_path=Path(
                os.getenv("RECOVERY_SIMULATOR_DB", str(data_dir / "external_simulator.db"))
            ),
            app_env=app_env,
            engine_api_token=_env_secret("ENGINE_API_TOKEN"),
            admin_api_token=_env_secret("ADMIN_API_TOKEN"),
            enable_admin_endpoints=_env_bool("ENABLE_ADMIN_ENDPOINTS", False),
            fault_injection_enabled=_env_bool("FAULT_INJECTION_ENABLED", False),
            simulator_honor_idempotency=_env_bool("SIMULATOR_HONOR_IDEMPOTENCY", True),
            retry_policy=RetryPolicy(
                max_attempts=int(os.getenv("RETRY_MAX_ATTEMPTS", "5")),
                base_delay_seconds=float(os.getenv("RETRY_BASE_DELAY_SECONDS", "1")),
                max_delay_seconds=float(os.getenv("RETRY_MAX_DELAY_SECONDS", "300")),
                max_retry_after_seconds=float(os.getenv("RETRY_AFTER_MAX_SECONDS", "3600")),
            ),
            retry_jitter=_env_bool("RETRY_JITTER", True),
            lease_ttl_seconds=float(os.getenv("LEASE_TTL_SECONDS", "30")),
        )
