"""Configuración de la aplicación a partir de variables de entorno."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from recovery_engine.domain.retry import RetryPolicy


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True, slots=True)
class Settings:
    engine_db_path: Path
    simulator_db_path: Path
    simulator_honor_idempotency: bool = True
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)
    retry_jitter: bool = True
    lease_ttl_seconds: float = 30.0
    enable_admin_endpoints: bool = False
    """Habilita POST /worker/run y PUT /simulator/fault-plans. Sin autenticación: solo local."""

    @classmethod
    def from_env(cls) -> Settings:
        data_dir = Path(os.getenv("RECOVERY_DATA_DIR", "data"))
        return cls(
            engine_db_path=Path(os.getenv("RECOVERY_ENGINE_DB", str(data_dir / "engine.db"))),
            simulator_db_path=Path(
                os.getenv("RECOVERY_SIMULATOR_DB", str(data_dir / "external_simulator.db"))
            ),
            simulator_honor_idempotency=(
                os.getenv("SIMULATOR_HONOR_IDEMPOTENCY", "true").strip().lower() != "false"
            ),
            retry_policy=RetryPolicy(
                max_attempts=int(os.getenv("RETRY_MAX_ATTEMPTS", "5")),
                base_delay_seconds=float(os.getenv("RETRY_BASE_DELAY_SECONDS", "1")),
                max_delay_seconds=float(os.getenv("RETRY_MAX_DELAY_SECONDS", "300")),
                max_retry_after_seconds=float(os.getenv("RETRY_AFTER_MAX_SECONDS", "3600")),
            ),
            retry_jitter=_env_bool("RETRY_JITTER", True),
            lease_ttl_seconds=float(os.getenv("LEASE_TTL_SECONDS", "30")),
            enable_admin_endpoints=_env_bool("ENABLE_ADMIN_ENDPOINTS", False),
        )
