"""Construcción de componentes a partir de ``Settings`` (compartido por API y CLI)."""

from __future__ import annotations

from dataclasses import dataclass

from recovery_engine.clock import Clock, utc_now
from recovery_engine.config import Settings
from recovery_engine.domain.retry import EqualJitter, Jitter, no_jitter
from recovery_engine.external.simulator import ExternalServiceSimulator
from recovery_engine.persistence.db import Database
from recovery_engine.persistence.schema import ENGINE_SCHEMA, migrate_engine
from recovery_engine.services.worker import RecoveryWorker


@dataclass(frozen=True, slots=True)
class Runtime:
    settings: Settings
    engine_db: Database
    simulator: ExternalServiceSimulator
    clock: Clock
    jitter: Jitter

    def worker(self, worker_id: str) -> RecoveryWorker:
        return RecoveryWorker(
            self.engine_db,
            self.simulator,
            worker_id=worker_id,
            policy=self.settings.retry_policy,
            clock=self.clock,
            jitter=self.jitter,
            lease_ttl_seconds=self.settings.lease_ttl_seconds,
        )


def build_runtime(
    settings: Settings, *, clock: Clock | None = None, jitter: Jitter | None = None
) -> Runtime:
    engine_db = Database(settings.engine_db_path)
    engine_db.initialize(ENGINE_SCHEMA, migrate=migrate_engine)

    simulator = ExternalServiceSimulator(
        Database(settings.simulator_db_path),
        honor_idempotency=settings.simulator_honor_idempotency,
        clock=clock or utc_now,
    )
    simulator.initialize()

    if jitter is None:
        jitter = EqualJitter() if settings.retry_jitter else no_jitter
    return Runtime(
        settings=settings,
        engine_db=engine_db,
        simulator=simulator,
        clock=clock or utc_now,
        jitter=jitter,
    )
