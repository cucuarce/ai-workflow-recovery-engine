"""Repositorios SQLite. No abren transacciones: las gestiona la capa de servicio."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Collection
from typing import Any

from recovery_engine.domain.hashing import canonical_json
from recovery_engine.domain.models import (
    Attempt,
    AttemptKind,
    AttemptOutcome,
    AttemptResult,
    Operation,
    OperationStatus,
    StatusTransition,
)

_MUTABLE_OPERATION_FIELDS = frozenset(
    {
        "attempt_count",
        "external_effect_id",
        "last_error",
        "next_attempt_at",
        "lease_owner",
        "lease_expires_at",
    }
)

# Estados que esperan su próximo turno según ``next_attempt_at``.
_SCHEDULED_STATUSES = (
    OperationStatus.PENDING.value,
    OperationStatus.FAILED_RETRYABLE.value,
    OperationStatus.OUTCOME_UNKNOWN.value,
)


def _optional_bool(value: int | None) -> bool | None:
    return None if value is None else bool(value)


def _row_to_operation(row: sqlite3.Row) -> Operation:
    return Operation(
        id=row["id"],
        idempotency_key=row["idempotency_key"],
        request_hash=row["request_hash"],
        operation_type=row["operation_type"],
        payload=json.loads(row["payload_json"]),
        status=OperationStatus(row["status"]),
        attempt_count=row["attempt_count"],
        external_effect_id=row["external_effect_id"],
        last_error=row["last_error"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        next_attempt_at=row["next_attempt_at"],
        lease_owner=row["lease_owner"],
        lease_expires_at=row["lease_expires_at"],
        lease_token=row["lease_token"],
    )


def _row_to_attempt(row: sqlite3.Row) -> Attempt:
    return Attempt(
        id=row["id"],
        operation_id=row["operation_id"],
        attempt_number=row["attempt_number"],
        scenario=row["scenario"],
        started_at=row["started_at"],
        outcome=AttemptOutcome(row["outcome"]) if row["outcome"] else None,
        http_status=row["http_status"],
        error_code=row["error_code"],
        error_message=row["error_message"],
        external_effect_id=row["external_effect_id"],
        deduplicated_by_receiver=_optional_bool(row["deduplicated_by_receiver"]),
        finished_at=row["finished_at"],
        kind=AttemptKind(row["kind"]),
        lease_token=row["lease_token"],
        worker_id=row["worker_id"],
        retry_after=row["retry_after"],
        retry_decision=row["retry_decision"],
        next_retry_at=row["next_retry_at"],
    )


class OperationRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def get(self, operation_id: str) -> Operation | None:
        row = self._conn.execute(
            "SELECT * FROM operations WHERE id = ?", (operation_id,)
        ).fetchone()
        return _row_to_operation(row) if row else None

    def get_by_idempotency_key(self, idempotency_key: str) -> Operation | None:
        row = self._conn.execute(
            "SELECT * FROM operations WHERE idempotency_key = ?", (idempotency_key,)
        ).fetchone()
        return _row_to_operation(row) if row else None

    def list(self, status: OperationStatus | None = None, limit: int = 100) -> list[Operation]:
        if status is None:
            rows = self._conn.execute(
                "SELECT * FROM operations ORDER BY created_at, id LIMIT ?", (limit,)
            )
        else:
            rows = self._conn.execute(
                "SELECT * FROM operations WHERE status = ? ORDER BY created_at, id LIMIT ?",
                (status.value, limit),
            )
        return [_row_to_operation(row) for row in rows]

    def find_claimable_ids(self, now: str, limit: int, exclude: Collection[str] = ()) -> list[str]:
        """Operaciones vencidas: programadas con ``next_attempt_at <= now`` o con lease vencido.

        Orden determinístico: ``created_at, id``. ``now`` debe estar en formato canónico.
        """
        rows = self._conn.execute(
            f"""
            SELECT id FROM operations
            WHERE (status IN ({", ".join("?" * len(_SCHEDULED_STATUSES))})
                   AND (next_attempt_at IS NULL OR next_attempt_at <= ?))
               OR (status = ? AND (lease_expires_at IS NULL OR lease_expires_at <= ?))
            ORDER BY created_at, id
            LIMIT ?
            """,
            (
                *_SCHEDULED_STATUSES,
                now,
                OperationStatus.IN_PROGRESS.value,
                now,
                limit + len(exclude),
            ),
        ).fetchall()
        ids = [row["id"] for row in rows if row["id"] not in exclude]
        return ids[:limit]

    def insert(self, operation: Operation) -> None:
        self._conn.execute(
            """
            INSERT INTO operations (
                id, idempotency_key, request_hash, operation_type, payload_json, status,
                attempt_count, external_effect_id, last_error, created_at, updated_at,
                next_attempt_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                operation.id,
                operation.idempotency_key,
                operation.request_hash,
                operation.operation_type,
                canonical_json(operation.payload),
                operation.status.value,
                operation.attempt_count,
                operation.external_effect_id,
                operation.last_error,
                operation.created_at,
                operation.updated_at,
                operation.next_attempt_at,
            ),
        )

    def compare_and_set_status(
        self,
        operation_id: str,
        *,
        expected: OperationStatus,
        new: OperationStatus,
        updated_at: str,
        fields: dict[str, Any] | None = None,
        expected_token: int | None = None,
        bump_token: bool = False,
    ) -> bool:
        """Cambia el estado solo si el actual es ``expected`` (y el token, si se indica).

        ``bump_token`` incrementa ``lease_token``: es la nueva generación de fencing.
        Devuelve si se aplicó.
        """
        fields = fields or {}
        unknown = set(fields) - _MUTABLE_OPERATION_FIELDS
        if unknown:
            raise ValueError(f"Campos no actualizables: {sorted(unknown)}")
        assignments = ["status = ?", "updated_at = ?", *(f"{column} = ?" for column in fields)]
        if bump_token:
            assignments.append("lease_token = lease_token + 1")
        params: list[Any] = [new.value, updated_at, *fields.values(), operation_id, expected.value]
        where = "id = ? AND status = ?"
        if expected_token is not None:
            where += " AND lease_token = ?"
            params.append(expected_token)
        cursor = self._conn.execute(
            f"UPDATE operations SET {', '.join(assignments)} WHERE {where}", params
        )
        return cursor.rowcount == 1

    def update_leased(
        self, operation_id: str, *, token: int, updated_at: str, fields: dict[str, Any]
    ) -> bool:
        """Actualiza campos de una operación ``in_progress`` sin cambiar estado (con fencing)."""
        unknown = set(fields) - _MUTABLE_OPERATION_FIELDS
        if unknown:
            raise ValueError(f"Campos no actualizables: {sorted(unknown)}")
        assignments = ["updated_at = ?", *(f"{column} = ?" for column in fields)]
        cursor = self._conn.execute(
            f"UPDATE operations SET {', '.join(assignments)} "
            "WHERE id = ? AND status = ? AND lease_token = ?",
            [
                updated_at,
                *fields.values(),
                operation_id,
                OperationStatus.IN_PROGRESS.value,
                token,
            ],
        )
        return cursor.rowcount == 1


class AttemptRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def next_attempt_number(self, operation_id: str) -> int:
        row = self._conn.execute(
            "SELECT COALESCE(MAX(attempt_number), 0) + 1 FROM attempts WHERE operation_id = ?",
            (operation_id,),
        ).fetchone()
        return int(row[0])

    def start(
        self,
        operation_id: str,
        attempt_number: int,
        scenario: str,
        started_at: str,
        *,
        kind: AttemptKind = AttemptKind.SUBMIT,
        lease_token: int | None = None,
        worker_id: str | None = None,
    ) -> Attempt:
        cursor = self._conn.execute(
            """
            INSERT INTO attempts (
                operation_id, attempt_number, scenario, started_at, kind, lease_token, worker_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                operation_id,
                attempt_number,
                scenario,
                started_at,
                kind.value,
                lease_token,
                worker_id,
            ),
        )
        return Attempt(
            id=int(cursor.lastrowid or 0),
            operation_id=operation_id,
            attempt_number=attempt_number,
            scenario=scenario,
            started_at=started_at,
            kind=kind,
            lease_token=lease_token,
            worker_id=worker_id,
        )

    def finish(
        self,
        attempt_id: int,
        result: AttemptResult,
        finished_at: str,
        *,
        retry_decision: str | None = None,
        next_retry_at: str | None = None,
    ) -> bool:
        """Cierra un intento abierto. Devuelve ``False`` si ya estaba cerrado."""
        dedup = result.deduplicated_by_receiver
        cursor = self._conn.execute(
            """
            UPDATE attempts SET
                outcome = ?, http_status = ?, error_code = ?, error_message = ?,
                external_effect_id = ?, deduplicated_by_receiver = ?, finished_at = ?,
                retry_after = ?, retry_decision = ?, next_retry_at = ?
            WHERE id = ? AND finished_at IS NULL
            """,
            (
                result.outcome.value,
                result.http_status,
                result.error_code,
                result.error_message,
                result.external_effect_id,
                None if dedup is None else int(dedup),
                finished_at,
                result.retry_after,
                retry_decision,
                next_retry_at,
                attempt_id,
            ),
        )
        return cursor.rowcount == 1

    def abandon_open(self, operation_id: str, finished_at: str, reason: str) -> int:
        """Cierra como ``abandoned`` los intentos que nunca registraron resultado."""
        cursor = self._conn.execute(
            """
            UPDATE attempts SET
                outcome = ?, error_code = 'lease_expired', error_message = ?, finished_at = ?,
                retry_decision = 'reconcile before any resend'
            WHERE operation_id = ? AND finished_at IS NULL
            """,
            (AttemptOutcome.ABANDONED.value, reason, finished_at, operation_id),
        )
        return cursor.rowcount

    def get(self, attempt_id: int) -> Attempt | None:
        row = self._conn.execute("SELECT * FROM attempts WHERE id = ?", (attempt_id,)).fetchone()
        return _row_to_attempt(row) if row else None

    def list_for_operation(self, operation_id: str) -> list[Attempt]:
        rows = self._conn.execute(
            "SELECT * FROM attempts WHERE operation_id = ? ORDER BY attempt_number",
            (operation_id,),
        )
        return [_row_to_attempt(row) for row in rows]


class TransitionRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def record(
        self,
        operation_id: str,
        from_status: OperationStatus | None,
        to_status: OperationStatus,
        reason: str,
        at: str,
        *,
        lease_token: int | None = None,
        actor: str | None = None,
    ) -> None:
        self._conn.execute(
            """
            INSERT INTO operation_transitions
                (operation_id, from_status, to_status, reason, at, lease_token, actor)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                operation_id,
                from_status.value if from_status else None,
                to_status.value,
                reason,
                at,
                lease_token,
                actor,
            ),
        )

    def list_for_operation(self, operation_id: str) -> list[StatusTransition]:
        rows = self._conn.execute(
            "SELECT * FROM operation_transitions WHERE operation_id = ? ORDER BY id",
            (operation_id,),
        )
        return [
            StatusTransition(
                operation_id=row["operation_id"],
                from_status=OperationStatus(row["from_status"]) if row["from_status"] else None,
                to_status=OperationStatus(row["to_status"]),
                reason=row["reason"],
                at=row["at"],
                lease_token=row["lease_token"],
                actor=row["actor"],
            )
            for row in rows
        ]
