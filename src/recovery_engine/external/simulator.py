"""Simulador de un servicio externo con fallos configurables.

Usa su propia base SQLite, separada de la del motor: el commit del efecto externo
y el registro local NUNCA son atómicos entre sí, igual que con un proveedor real.

Tablas:
- ``external_effects``: efectos de negocio aplicados (uno por fila). Sin restricción
  UNIQUE por operación, para que una duplicación sea observable si ocurre.
- ``idempotency_registry``: idempotencia del lado receptor (clave -> efecto).
- ``external_requests``: log de cada pedido recibido con lo que ocurrió realmente,
  incluso si el cliente nunca recibió la respuesta.
- ``fault_plan``: guion de fallos por ``operation_ref``; cada envío sin escenario forzado
  consume la próxima entrada (si no queda ninguna, se procesa con éxito). Un plan cargado
  por referencia del cliente (``set_client_fault_plan``) se guarda bajo
  ``client-ref:<clave>`` y se usa cuando la operación no tiene plan propio.
- ``external_lookups``: log de consultas de reconciliación (solo lectura del estado).
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from typing import Any

from recovery_engine.clock import Clock, IdFactory, new_effect_id, utc_now
from recovery_engine.domain.hashing import canonical_json, request_fingerprint
from recovery_engine.external.contracts import (
    ExternalLookup,
    ExternalRequest,
    ExternalResponse,
    ExternalTimeoutError,
    Scenario,
)
from recovery_engine.persistence.db import Database, transaction

SIMULATOR_SCHEMA = """
CREATE TABLE IF NOT EXISTS external_effects (
    effect_id       TEXT PRIMARY KEY,
    operation_ref   TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    payload_hash    TEXT NOT NULL,
    payload_json    TEXT NOT NULL,
    created_at      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_effects_operation_ref ON external_effects (operation_ref);

CREATE TABLE IF NOT EXISTS idempotency_registry (
    idempotency_key TEXT PRIMARY KEY,
    payload_hash    TEXT NOT NULL,
    effect_id       TEXT NOT NULL REFERENCES external_effects (effect_id),
    created_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS external_requests (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    idempotency_key     TEXT NOT NULL,
    operation_ref       TEXT NOT NULL,
    payload_hash        TEXT NOT NULL,
    scenario            TEXT NOT NULL,
    server_status       INTEGER,            -- NULL si el pedido se perdió antes de procesarse
    effect_applied      INTEGER NOT NULL,   -- 1 si ESTE pedido creó un efecto nuevo
    deduplicated        INTEGER NOT NULL,   -- 1 si se devolvió un efecto preexistente
    effect_id           TEXT,
    delivered_to_client INTEGER NOT NULL,   -- 0 en timeouts: el cliente no vio la respuesta
    received_at         TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_requests_operation_ref ON external_requests (operation_ref);

CREATE TABLE IF NOT EXISTS fault_plan (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    operation_ref TEXT NOT NULL,
    scenario      TEXT NOT NULL,
    retry_after   TEXT,                     -- valor crudo de Retry-After para http_429
    consumed_at   TEXT
);

CREATE INDEX IF NOT EXISTS ix_fault_plan_ref ON fault_plan (operation_ref, consumed_at);

CREATE TABLE IF NOT EXISTS external_lookups (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    idempotency_key TEXT NOT NULL,
    found           INTEGER NOT NULL,
    effect_id       TEXT,
    looked_up_at    TEXT NOT NULL
);
"""

RETRY_AFTER_SECONDS = 30
CLIENT_REF_PREFIX = "client-ref:"

# Fallos que ocurren antes de tocar el estado de negocio del receptor.
_REJECTIONS: dict[Scenario, tuple[int, str, str]] = {
    Scenario.HTTP_503: (503, "service_unavailable", "Servicio temporalmente no disponible"),
    Scenario.HTTP_429: (429, "rate_limited", "Límite de solicitudes excedido"),
    Scenario.PERMANENT_ERROR: (422, "invalid_request", "Solicitud rechazada de forma permanente"),
}

_TIMEOUT_SCENARIOS = frozenset(
    {Scenario.TIMEOUT_BEFORE_PROCESSING, Scenario.TIMEOUT_AFTER_PROCESSING}
)


@dataclass(frozen=True, slots=True)
class ExternalRequestRecord:
    id: int
    idempotency_key: str
    operation_ref: str
    payload_hash: str
    scenario: str
    server_status: int | None
    effect_applied: bool
    deduplicated: bool
    effect_id: str | None
    delivered_to_client: bool
    received_at: str


@dataclass(frozen=True, slots=True)
class ExternalEffect:
    effect_id: str
    operation_ref: str
    idempotency_key: str
    payload_hash: str
    payload: dict[str, Any]
    created_at: str


@dataclass(frozen=True, slots=True)
class FaultSpec:
    scenario: Scenario
    retry_after: str | None = None


@dataclass(frozen=True, slots=True)
class ExternalLookupRecord:
    id: int
    idempotency_key: str
    found: bool
    effect_id: str | None
    looked_up_at: str


@dataclass(frozen=True, slots=True)
class DuplicateEffectReport:
    operation_ref: str
    effect_count: int
    effect_ids: list[str]


class ExternalServiceSimulator:
    def __init__(
        self,
        db: Database,
        *,
        honor_idempotency: bool = True,
        clock: Clock = utc_now,
        effect_id_factory: IdFactory = new_effect_id,
    ) -> None:
        self._db = db
        self.honor_idempotency = honor_idempotency
        self._clock = clock
        self._effect_id_factory = effect_id_factory

    def initialize(self) -> None:
        self._db.initialize(SIMULATOR_SCHEMA)

    @property
    def supports_idempotency(self) -> bool:
        """Capacidad declarada al cliente: deduplica reenvíos con la misma clave."""
        return self.honor_idempotency

    # ------------------------------------------------------------------ envío

    def submit(self, request: ExternalRequest) -> ExternalResponse:
        payload_hash = request_fingerprint(request.operation_type, request.payload)
        now = self._clock()
        with closing(self._db.connect()) as conn, transaction(conn):
            fault = self._resolve_fault(conn, request, now)
            response = self._handle(conn, request, fault, payload_hash, now)
            delivered = fault.scenario not in _TIMEOUT_SCENARIOS
            self._log_request(conn, request, fault, payload_hash, response, delivered, now)
        # El commit del receptor ya ocurrió: un timeout posterior no lo deshace.
        if not delivered:
            raise ExternalTimeoutError(
                f"Sin respuesta del servicio externo (escenario: {fault.scenario.value})"
            )
        assert response is not None
        return response

    @staticmethod
    def _resolve_fault(conn: sqlite3.Connection, request: ExternalRequest, now: str) -> FaultSpec:
        """Escenario forzado en el pedido > próxima entrada del plan > éxito."""
        if request.scenario is not None:
            return FaultSpec(request.scenario)
        # Un plan propio de la operación (aunque ya esté consumido) reemplaza al plan
        # cargado por la referencia del cliente.
        ref = request.operation_ref
        has_own_plan = conn.execute(
            "SELECT 1 FROM fault_plan WHERE operation_ref = ? LIMIT 1", (ref,)
        ).fetchone()
        if has_own_plan is None and request.client_reference is not None:
            ref = CLIENT_REF_PREFIX + request.client_reference
        row = conn.execute(
            "SELECT id, scenario, retry_after FROM fault_plan "
            "WHERE operation_ref = ? AND consumed_at IS NULL ORDER BY id LIMIT 1",
            (ref,),
        ).fetchone()
        if row is None:
            return FaultSpec(Scenario.SUCCESS)
        conn.execute("UPDATE fault_plan SET consumed_at = ? WHERE id = ?", (now, row["id"]))
        return FaultSpec(Scenario(row["scenario"]), row["retry_after"])

    def _handle(
        self,
        conn: sqlite3.Connection,
        request: ExternalRequest,
        fault: FaultSpec,
        payload_hash: str,
        now: str,
    ) -> ExternalResponse | None:
        if fault.scenario is Scenario.TIMEOUT_BEFORE_PROCESSING:
            return None  # el pedido se pierde antes de llegar a procesarse
        rejection = _REJECTIONS.get(fault.scenario)
        if rejection is not None:
            status, code, message = rejection
            retry_after = None
            if status == 429:
                retry_after = (
                    fault.retry_after if fault.retry_after is not None else str(RETRY_AFTER_SECONDS)
                )
            return ExternalResponse(
                status_code=status,
                error_code=code,
                message=message,
                retry_after_seconds=(
                    int(retry_after) if retry_after and retry_after.isdecimal() else None
                ),
                retry_after=retry_after,
            )
        # SUCCESS y TIMEOUT_AFTER_PROCESSING procesan normalmente.
        return self._apply_effect(conn, request, payload_hash, now)

    def _apply_effect(
        self, conn: sqlite3.Connection, request: ExternalRequest, payload_hash: str, now: str
    ) -> ExternalResponse:
        if self.honor_idempotency:
            existing = conn.execute(
                "SELECT payload_hash, effect_id FROM idempotency_registry "
                "WHERE idempotency_key = ?",
                (request.idempotency_key,),
            ).fetchone()
            if existing is not None:
                if existing["payload_hash"] != payload_hash:
                    return ExternalResponse(
                        status_code=409,
                        error_code="idempotency_key_mismatch",
                        message="La clave idempotente ya se usó con otro payload",
                    )
                return ExternalResponse(
                    status_code=200, effect_id=existing["effect_id"], deduplicated=True
                )

        effect_id = self._effect_id_factory()
        conn.execute(
            """
            INSERT INTO external_effects
                (effect_id, operation_ref, idempotency_key, payload_hash, payload_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                effect_id,
                request.operation_ref,
                request.idempotency_key,
                payload_hash,
                canonical_json(request.payload),
                now,
            ),
        )
        if self.honor_idempotency:
            conn.execute(
                """
                INSERT INTO idempotency_registry
                    (idempotency_key, payload_hash, effect_id, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (request.idempotency_key, payload_hash, effect_id, now),
            )
        return ExternalResponse(status_code=201, effect_id=effect_id)

    @staticmethod
    def _log_request(
        conn: sqlite3.Connection,
        request: ExternalRequest,
        fault: FaultSpec,
        payload_hash: str,
        response: ExternalResponse | None,
        delivered: bool,
        now: str,
    ) -> None:
        effect_id = response.effect_id if response else None
        deduplicated = bool(response and response.deduplicated)
        conn.execute(
            """
            INSERT INTO external_requests (
                idempotency_key, operation_ref, payload_hash, scenario, server_status,
                effect_applied, deduplicated, effect_id, delivered_to_client, received_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                request.idempotency_key,
                request.operation_ref,
                payload_hash,
                fault.scenario.value,
                response.status_code if response else None,
                int(effect_id is not None and not deduplicated),
                int(deduplicated),
                effect_id,
                int(delivered),
                now,
            ),
        )

    # -------------------------------------------------------- reconciliación

    def lookup(self, idempotency_key: str) -> ExternalLookup:
        """Estado persistido del proveedor para una clave (no modifica efectos)."""
        now = self._clock()
        with closing(self._db.connect()) as conn, transaction(conn):
            row = conn.execute(
                "SELECT effect_id FROM external_effects WHERE idempotency_key = ? "
                "ORDER BY created_at, effect_id LIMIT 1",
                (idempotency_key,),
            ).fetchone()
            effect_id = row["effect_id"] if row else None
            conn.execute(
                "INSERT INTO external_lookups (idempotency_key, found, effect_id, looked_up_at) "
                "VALUES (?, ?, ?, ?)",
                (idempotency_key, int(effect_id is not None), effect_id, now),
            )
        return ExternalLookup(found=effect_id is not None, effect_id=effect_id)

    # ----------------------------------------------------------- fault plan

    def set_fault_plan(self, operation_ref: str, faults: list[FaultSpec]) -> None:
        """Reemplaza las entradas no consumidas del plan de fallos de ``operation_ref``."""
        with closing(self._db.connect()) as conn, transaction(conn):
            conn.execute(
                "DELETE FROM fault_plan WHERE operation_ref = ? AND consumed_at IS NULL",
                (operation_ref,),
            )
            conn.executemany(
                "INSERT INTO fault_plan (operation_ref, scenario, retry_after) VALUES (?, ?, ?)",
                [(operation_ref, f.scenario.value, f.retry_after) for f in faults],
            )

    def set_client_fault_plan(self, client_reference: str, faults: list[FaultSpec]) -> None:
        """Plan para el pedido lógico del cliente; puede cargarse antes de crear la operación."""
        self.set_fault_plan(CLIENT_REF_PREFIX + client_reference, faults)

    def pending_faults(self, operation_ref: str) -> list[FaultSpec]:
        with closing(self._db.connect()) as conn:
            rows = conn.execute(
                "SELECT scenario, retry_after FROM fault_plan "
                "WHERE operation_ref = ? AND consumed_at IS NULL ORDER BY id",
                (operation_ref,),
            ).fetchall()
        return [FaultSpec(Scenario(r["scenario"]), r["retry_after"]) for r in rows]

    # ----------------------------------------------------------- observación

    def list_lookups(self, idempotency_key: str | None = None) -> list[ExternalLookupRecord]:
        query = "SELECT * FROM external_lookups"
        params: tuple[str, ...] = ()
        if idempotency_key is not None:
            query += " WHERE idempotency_key = ?"
            params = (idempotency_key,)
        with closing(self._db.connect()) as conn:
            rows = conn.execute(query + " ORDER BY id", params).fetchall()
        return [
            ExternalLookupRecord(
                id=r["id"],
                idempotency_key=r["idempotency_key"],
                found=bool(r["found"]),
                effect_id=r["effect_id"],
                looked_up_at=r["looked_up_at"],
            )
            for r in rows
        ]

    def list_requests(self, operation_ref: str | None = None) -> list[ExternalRequestRecord]:
        query = "SELECT * FROM external_requests"
        params: tuple[str, ...] = ()
        if operation_ref is not None:
            query += " WHERE operation_ref = ?"
            params = (operation_ref,)
        with closing(self._db.connect()) as conn:
            rows = conn.execute(query + " ORDER BY id", params).fetchall()
        return [
            ExternalRequestRecord(
                id=row["id"],
                idempotency_key=row["idempotency_key"],
                operation_ref=row["operation_ref"],
                payload_hash=row["payload_hash"],
                scenario=row["scenario"],
                server_status=row["server_status"],
                effect_applied=bool(row["effect_applied"]),
                deduplicated=bool(row["deduplicated"]),
                effect_id=row["effect_id"],
                delivered_to_client=bool(row["delivered_to_client"]),
                received_at=row["received_at"],
            )
            for row in rows
        ]

    def list_effects(self, operation_ref: str | None = None) -> list[ExternalEffect]:
        query = "SELECT * FROM external_effects"
        params: tuple[str, ...] = ()
        if operation_ref is not None:
            query += " WHERE operation_ref = ?"
            params = (operation_ref,)
        with closing(self._db.connect()) as conn:
            rows = conn.execute(query + " ORDER BY created_at, effect_id", params).fetchall()
        return [
            ExternalEffect(
                effect_id=row["effect_id"],
                operation_ref=row["operation_ref"],
                idempotency_key=row["idempotency_key"],
                payload_hash=row["payload_hash"],
                payload=json.loads(row["payload_json"]),
                created_at=row["created_at"],
            )
            for row in rows
        ]

    def find_duplicate_effects(self) -> list[DuplicateEffectReport]:
        """Operaciones lógicas con más de un efecto de negocio aplicado."""
        with closing(self._db.connect()) as conn:
            rows = conn.execute(
                """
                SELECT operation_ref, COUNT(*) AS effect_count, GROUP_CONCAT(effect_id) AS ids
                FROM external_effects
                GROUP BY operation_ref
                HAVING COUNT(*) > 1
                ORDER BY operation_ref
                """
            ).fetchall()
        return [
            DuplicateEffectReport(
                operation_ref=row["operation_ref"],
                effect_count=row["effect_count"],
                effect_ids=sorted(row["ids"].split(",")),
            )
            for row in rows
        ]
