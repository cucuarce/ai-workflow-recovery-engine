"""Worker de recuperación: procesa operaciones vencidas de forma acotada y determinística.

Una corrida (``run_once``):
- reclama operaciones vencidas una por una, en orden ``created_at, id``;
- procesa cada operación como máximo una vez por corrida (aunque quede re-programada para
  "ahora"), hasta ``max_operations``;
- no duerme ni programa nada: quien lo dispara (CLI, endpoint, n8n en Etapa 3) decide cuándo.

Es seguro ejecutarlo de forma repetida o concurrente: el claim es atómico (``BEGIN
IMMEDIATE`` + compare-and-set) y toda escritura posterior está protegida por fencing.
"""

from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass, field

from recovery_engine.clock import Clock, utc_now
from recovery_engine.domain.errors import LeaseLostError
from recovery_engine.domain.retry import EqualJitter, Jitter, RetryPolicy
from recovery_engine.external.contracts import ExternalServiceClient
from recovery_engine.persistence.db import Database
from recovery_engine.services.processing import OperationProcessor

DEFAULT_MAX_OPERATIONS = 10


@dataclass(frozen=True, slots=True)
class WorkerItem:
    operation_id: str
    claimed_from: str
    lease_token: int
    action: str
    final_status: str | None
    error: str | None = None


@dataclass(frozen=True, slots=True)
class WorkerReport:
    worker_id: str
    started_at: str
    finished_at: str
    max_operations: int
    items: list[WorkerItem] = field(default_factory=list)

    @property
    def processed(self) -> int:
        return len(self.items)


class RecoveryWorker:
    def __init__(
        self,
        db: Database,
        external: ExternalServiceClient,
        *,
        worker_id: str,
        policy: RetryPolicy | None = None,
        clock: Clock = utc_now,
        jitter: Jitter | None = None,
        lease_ttl_seconds: float = 30.0,
    ) -> None:
        self._db = db
        self._external = external
        self.worker_id = worker_id
        self._policy = policy or RetryPolicy()
        self._clock = clock
        self._jitter = jitter or EqualJitter()
        self._lease_ttl = lease_ttl_seconds

    def run_once(self, max_operations: int = DEFAULT_MAX_OPERATIONS) -> WorkerReport:
        if max_operations < 1:
            raise ValueError("max_operations debe ser >= 1")
        started_at = self._clock()
        items: list[WorkerItem] = []
        seen: set[str] = set()
        with closing(self._db.connect()) as conn:
            processor = OperationProcessor(
                conn,
                self._external,
                policy=self._policy,
                clock=self._clock,
                jitter=self._jitter,
                lease_ttl_seconds=self._lease_ttl,
            )
            while len(items) < max_operations:
                lease = processor.claim_next(self.worker_id, exclude=seen)
                if lease is None:
                    break
                operation_id = lease.operation.id
                seen.add(operation_id)
                try:
                    result = processor.process(lease)
                except LeaseLostError as exc:
                    # Otro worker re-reclamó la operación: nuestra escritura se descartó.
                    items.append(self._item(lease, "lease_lost", None, str(exc)))
                except Exception as exc:
                    # Error inesperado del proveedor o del cliente: la operación queda
                    # in_progress con lease y se recupera (reconciliando) cuando venza.
                    error = f"{type(exc).__name__}: {exc}"
                    items.append(self._item(lease, "error_left_for_lease_expiry", None, error))
                else:
                    items.append(self._item(lease, result.action, result.operation.status.value))
        return WorkerReport(
            worker_id=self.worker_id,
            started_at=started_at,
            finished_at=self._clock(),
            max_operations=max_operations,
            items=items,
        )

    @staticmethod
    def _item(lease, action: str, final_status: str | None, error: str | None = None) -> WorkerItem:
        return WorkerItem(
            operation_id=lease.operation.id,
            claimed_from=lease.claimed_from.value,
            lease_token=lease.token,
            action=action,
            final_status=final_status,
            error=error,
        )
