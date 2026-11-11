"""Esquema SQLite del motor (independiente del simulador externo) y su migración.

Versiones (``PRAGMA user_version``):
- 1 (implícita, ``user_version = 0``): esquema de la Etapa 1.
- 2: lease/fencing, programación de reintentos, dead-letter y trazabilidad por intento.

Los ``CHECK`` de ``status`` y ``outcome`` cambian, y SQLite no permite alterar un
``CHECK``: la migración reconstruye ``operations`` y ``attempts`` siguiendo el
procedimiento de 12 pasos de la documentación de SQLite, preservando los datos.
"""

from __future__ import annotations

import sqlite3
from enum import StrEnum

from recovery_engine.domain.models import AttemptKind, AttemptOutcome, OperationStatus

SCHEMA_VERSION = 2


def _sql_values(enum: type[StrEnum]) -> str:
    return ", ".join(f"'{member.value}'" for member in enum)


def _operations_table(name: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {name} (
    id                 TEXT PRIMARY KEY,
    idempotency_key    TEXT NOT NULL UNIQUE,
    request_hash       TEXT NOT NULL,
    operation_type     TEXT NOT NULL,
    payload_json       TEXT NOT NULL,
    status             TEXT NOT NULL CHECK (status IN ({_sql_values(OperationStatus)})),
    attempt_count      INTEGER NOT NULL DEFAULT 0,
    external_effect_id TEXT,
    last_error         TEXT,
    created_at         TEXT NOT NULL,
    updated_at         TEXT NOT NULL,
    next_attempt_at    TEXT,
    lease_owner        TEXT,
    lease_expires_at   TEXT,
    lease_token        INTEGER NOT NULL DEFAULT 0
);
"""


def _attempts_table(name: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {name} (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    operation_id             TEXT NOT NULL REFERENCES operations (id),
    attempt_number           INTEGER NOT NULL,
    scenario                 TEXT NOT NULL,
    outcome                  TEXT CHECK (
                                 outcome IS NULL OR outcome IN ({_sql_values(AttemptOutcome)})
                             ),
    http_status              INTEGER,
    error_code               TEXT,
    error_message            TEXT,
    external_effect_id       TEXT,
    deduplicated_by_receiver INTEGER,
    started_at               TEXT NOT NULL,
    finished_at              TEXT,
    kind                     TEXT NOT NULL DEFAULT 'submit'
                             CHECK (kind IN ({_sql_values(AttemptKind)})),
    lease_token              INTEGER,
    worker_id                TEXT,
    retry_after              TEXT,
    retry_decision           TEXT,
    next_retry_at            TEXT,
    UNIQUE (operation_id, attempt_number)
);
"""


ENGINE_SCHEMA = (
    _operations_table("operations")
    + """
CREATE INDEX IF NOT EXISTS ix_operations_status ON operations (status);
CREATE INDEX IF NOT EXISTS ix_operations_due ON operations (status, next_attempt_at);
"""
    + _attempts_table("attempts")
    + """
CREATE TABLE IF NOT EXISTS operation_transitions (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    operation_id TEXT NOT NULL REFERENCES operations (id),
    from_status  TEXT,
    to_status    TEXT NOT NULL,
    reason       TEXT NOT NULL,
    at           TEXT NOT NULL,
    lease_token  INTEGER,
    actor        TEXT
);

CREATE INDEX IF NOT EXISTS ix_transitions_operation ON operation_transitions (operation_id);
"""
)

# Columna que solo existe a partir de v2, usada para detectar tablas v1.
_V2_MARKERS = {"operations": "lease_token", "attempts": "kind"}
_REBUILDERS = {"operations": _operations_table, "attempts": _attempts_table}
_ADDED_TRANSITION_COLUMNS = {"lease_token": "INTEGER", "actor": "TEXT"}


def _columns(conn: sqlite3.Connection, table: str) -> list[str]:
    return [row[1] for row in conn.execute(f"PRAGMA table_info({table})")]


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)
    ).fetchone()
    return row is not None


def _needs_rebuild(conn: sqlite3.Connection, table: str) -> bool:
    return _table_exists(conn, table) and _V2_MARKERS[table] not in _columns(conn, table)


def migrate_engine(conn: sqlite3.Connection) -> None:
    """Lleva una base existente al esquema v2. Idempotente; en bases nuevas no hace nada.

    La detección es por columnas (no solo por ``user_version``) para que sea segura aun
    si la base fue creada sin registrar versión.
    """
    pending = [table for table in ("operations", "attempts") if _needs_rebuild(conn, table)]
    transitions_missing = []
    if _table_exists(conn, "operation_transitions"):
        existing = _columns(conn, "operation_transitions")
        transitions_missing = [c for c in _ADDED_TRANSITION_COLUMNS if c not in existing]

    if pending or transitions_missing:
        conn.execute("PRAGMA foreign_keys = OFF")
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                for table in pending:
                    temp = f"{table}__v2"
                    old_columns = _columns(conn, table)
                    conn.execute(_REBUILDERS[table](temp))
                    common = ", ".join(c for c in old_columns if c in _columns(conn, temp))
                    conn.execute(f"INSERT INTO {temp} ({common}) SELECT {common} FROM {table}")
                    conn.execute(f"DROP TABLE {table}")
                    conn.execute(f"ALTER TABLE {temp} RENAME TO {table}")
                for column in transitions_missing:
                    sql_type = _ADDED_TRANSITION_COLUMNS[column]
                    conn.execute(
                        f"ALTER TABLE operation_transitions ADD COLUMN {column} {sql_type}"
                    )
                violations = conn.execute("PRAGMA foreign_key_check").fetchall()
                if violations:
                    raise RuntimeError(f"Migración con claves foráneas inválidas: {violations}")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
            conn.execute("COMMIT")
        finally:
            conn.execute("PRAGMA foreign_keys = ON")

    conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
