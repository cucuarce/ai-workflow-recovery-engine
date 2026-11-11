from __future__ import annotations

import sqlite3
from collections.abc import Callable, Iterator

import pytest
from fastapi.testclient import TestClient

from recovery_engine.api.app import create_app
from recovery_engine.config import Settings
from recovery_engine.external.simulator import ExternalServiceSimulator
from recovery_engine.persistence.db import Database
from recovery_engine.persistence.schema import ENGINE_SCHEMA
from recovery_engine.services.operations import OperationService


@pytest.fixture
def settings(tmp_path) -> Settings:
    return Settings(
        engine_db_path=tmp_path / "engine.db",
        simulator_db_path=tmp_path / "external.db",
    )


@pytest.fixture
def client(settings: Settings) -> Iterator[TestClient]:
    with TestClient(create_app(settings)) as test_client:
        yield test_client


@pytest.fixture
def engine_db(tmp_path) -> Database:
    db = Database(tmp_path / "engine.db")
    db.initialize(ENGINE_SCHEMA)
    return db


@pytest.fixture
def simulator_db(tmp_path) -> Database:
    return Database(tmp_path / "external.db")


@pytest.fixture
def simulator(simulator_db: Database) -> ExternalServiceSimulator:
    sim = ExternalServiceSimulator(simulator_db)
    sim.initialize()
    return sim


@pytest.fixture
def make_service(
    engine_db: Database, simulator: ExternalServiceSimulator
) -> Iterator[Callable[[], OperationService]]:
    """Crea servicios con conexiones propias (como hace la API por request)."""
    connections: list[sqlite3.Connection] = []

    def factory() -> OperationService:
        conn = engine_db.connect()
        connections.append(conn)
        return OperationService(conn, simulator)

    yield factory
    for conn in connections:
        conn.close()
