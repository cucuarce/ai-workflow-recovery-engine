"""Compatibilidad: una base creada por la Etapa 1 se actualiza al esquema v2 sin perder datos."""

from __future__ import annotations

import sqlite3
from contextlib import closing

import pytest
from fastapi.testclient import TestClient

from recovery_engine.api.app import create_app
from recovery_engine.config import Settings
from recovery_engine.domain.models import AttemptKind, OperationStatus
from recovery_engine.domain.retry import no_jitter
from recovery_engine.persistence.db import Database
from recovery_engine.persistence.schema import ENGINE_SCHEMA, SCHEMA_VERSION, migrate_engine
from recovery_engine.services.worker import RecoveryWorker

# Esquema exacto de la Etapa 1 (congelado; no derivar del código actual).
STAGE1_SCHEMA = """
CREATE TABLE IF NOT EXISTS operations (
    id                 TEXT PRIMARY KEY,
    idempotency_key    TEXT NOT NULL UNIQUE,
    request_hash       TEXT NOT NULL,
    operation_type     TEXT NOT NULL,
    payload_json       TEXT NOT NULL,
    status             TEXT NOT NULL CHECK (status IN ('pending', 'in_progress', 'succeeded',
                           'failed_retryable', 'outcome_unknown', 'failed_permanent')),
    attempt_count      INTEGER NOT NULL DEFAULT 0,
    external_effect_id TEXT,
    last_error         TEXT,
    created_at         TEXT NOT NULL,
    updated_at         TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_operations_status ON operations (status);
CREATE TABLE IF NOT EXISTS attempts (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    operation_id             TEXT NOT NULL REFERENCES operations (id),
    attempt_number           INTEGER NOT NULL,
    scenario                 TEXT NOT NULL,
    outcome                  TEXT CHECK (outcome IS NULL OR outcome IN ('success',
                                 'retryable_error', 'rate_limited', 'timeout', 'permanent_error')),
    http_status              INTEGER,
    error_code               TEXT,
    error_message            TEXT,
    external_effect_id       TEXT,
    deduplicated_by_receiver INTEGER,
    started_at               TEXT NOT NULL,
    finished_at              TEXT,
    UNIQUE (operation_id, attempt_number)
);
CREATE TABLE IF NOT EXISTS operation_transitions (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    operation_id TEXT NOT NULL REFERENCES operations (id),
    from_status  TEXT,
    to_status    TEXT NOT NULL,
    reason       TEXT NOT NULL,
    at           TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_transitions_operation ON operation_transitions (operation_id);
"""

T0 = "2025-12-31T23:00:00.000000+00:00"


def _seed_stage1_db(path) -> None:
    with closing(sqlite3.connect(path)) as conn:
        conn.executescript(STAGE1_SCHEMA)
        rows = [
            ("op_pending", "k1", "pending", 0, None),
            ("op_crashed", "k2", "in_progress", 1, None),  # caída en Etapa 1
            ("op_retry", "k3", "failed_retryable", 1, "service_unavailable: x"),
            ("op_done", "k4", "succeeded", 1, None),
        ]
        for op_id, key, status, count, error in rows:
            conn.execute(
                "INSERT INTO operations VALUES (?, ?, 'h', 'issue_invoice', '{\"a\":1}', ?, ?, "
                "NULL, ?, ?, ?)",
                (op_id, key, status, count, error, T0, T0),
            )
            conn.execute(
                "INSERT INTO operation_transitions "
                "(operation_id, from_status, to_status, reason, at) "
                "VALUES (?, NULL, 'pending', 'created', ?)",
                (op_id, T0),
            )
        conn.execute(
            "INSERT INTO attempts (operation_id, attempt_number, scenario, started_at) "
            "VALUES ('op_crashed', 1, 'success', ?)",
            (T0,),
        )
        conn.execute(
            "INSERT INTO attempts (operation_id, attempt_number, scenario, outcome, http_status, "
            "started_at, finished_at) "
            "VALUES ('op_retry', 1, 'http_503', 'retryable_error', 503, ?, ?)",
            (T0, T0),
        )
        conn.commit()


def _migrate(path) -> Database:
    db = Database(path)
    db.initialize(ENGINE_SCHEMA, migrate=migrate_engine)
    return db


def test_stage1_database_is_migrated_preserving_data(tmp_path):
    path = tmp_path / "engine.db"
    _seed_stage1_db(path)

    db = _migrate(path)

    with closing(db.connect()) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        operation_columns = {r[1] for r in conn.execute("PRAGMA table_info(operations)")}
        assert {"next_attempt_at", "lease_owner", "lease_expires_at", "lease_token"} <= (
            operation_columns
        )
        statuses = dict(conn.execute("SELECT id, status FROM operations").fetchall())
        assert statuses == {
            "op_pending": "pending",
            "op_crashed": "in_progress",
            "op_retry": "failed_retryable",
            "op_done": "succeeded",
        }
        assert conn.execute("SELECT COUNT(*) FROM attempts").fetchone()[0] == 2
        assert conn.execute("SELECT COUNT(*) FROM operation_transitions").fetchone()[0] == 4
        kinds = {r[0] for r in conn.execute("SELECT kind FROM attempts")}
        assert kinds == {AttemptKind.SUBMIT.value}
        # El nuevo CHECK admite dead_letter; el UNIQUE de idempotencia sigue vigente.
        conn.execute("UPDATE operations SET status = 'dead_letter' WHERE id = 'op_done'")
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO operations (id, idempotency_key, request_hash, operation_type, "
                "payload_json, status, created_at, updated_at) "
                "VALUES ('x', 'k1', 'h', 't', '{}', 'pending', ?, ?)",
                (T0, T0),
            )


def test_migration_is_idempotent(tmp_path):
    path = tmp_path / "engine.db"
    _seed_stage1_db(path)
    _migrate(path)
    db = _migrate(path)
    with closing(db.connect()) as conn:
        assert conn.execute("SELECT COUNT(*) FROM operations").fetchone()[0] == 4


def test_fresh_database_gets_current_version(tmp_path):
    db = _migrate(tmp_path / "fresh.db")
    with closing(db.connect()) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION


def test_legacy_operations_are_recoverable_after_migration(tmp_path, simulator):
    path = tmp_path / "engine.db"
    _seed_stage1_db(path)
    db = _migrate(path)

    report = RecoveryWorker(
        db, simulator, worker_id="w", clock=lambda: "2026-01-01T00:00:00+00:00", jitter=no_jitter
    ).run_once()

    # Sin lease registrado, un in_progress legado se trata como abandonado: se reconcilia.
    by_id = {item.operation_id: item for item in report.items}
    assert set(by_id) == {"op_pending", "op_crashed", "op_retry"}
    assert by_id["op_crashed"].claimed_from == OperationStatus.IN_PROGRESS.value
    assert all(item.final_status == "succeeded" for item in report.items)
    assert simulator.find_duplicate_effects() == []
    with closing(db.connect()) as conn:
        outcomes = [
            r[0]
            for r in conn.execute(
                "SELECT outcome FROM attempts WHERE operation_id = 'op_crashed' "
                "ORDER BY attempt_number"
            )
        ]
    assert outcomes == ["abandoned", "effect_not_found", "success"]


def test_app_starts_on_a_stage1_database(tmp_path):
    path = tmp_path / "engine.db"
    _seed_stage1_db(path)
    settings = Settings(engine_db_path=path, simulator_db_path=tmp_path / "external.db")
    with TestClient(create_app(settings)) as client:
        body = client.get("/operations/op_retry").json()
    assert body["status"] == "failed_retryable"
    assert body["lease_token"] == 0
    assert body["attempts"][0]["kind"] == "submit"
