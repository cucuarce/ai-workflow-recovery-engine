"""Endpoint administrativo del worker (deshabilitado por defecto).

Se monta solo con ``ENABLE_ADMIN_ENDPOINTS=true`` y exige ``ADMIN_API_TOKEN`` (Bearer);
``Settings`` impide habilitarlo sin token. Lo usa el Workflow 2 de n8n. No exponerlo a una
red no confiable: permite forzar corridas del worker.
"""

from __future__ import annotations

from dataclasses import asdict
from typing import Annotated

from fastapi import APIRouter, Query
from pydantic import BaseModel

from recovery_engine.api.deps import RuntimeDep
from recovery_engine.services.worker import WorkerReport

router = APIRouter(tags=["admin"])


class WorkerRunOut(BaseModel):
    worker_id: str
    started_at: str
    finished_at: str
    max_operations: int
    processed: int
    items: list[dict]

    @classmethod
    def from_report(cls, report: WorkerReport) -> WorkerRunOut:
        return cls(
            worker_id=report.worker_id,
            started_at=report.started_at,
            finished_at=report.finished_at,
            max_operations=report.max_operations,
            processed=report.processed,
            items=[asdict(item) for item in report.items],
        )


@router.post("/worker/run", response_model=WorkerRunOut)
def run_worker(
    runtime: RuntimeDep,
    max_operations: Annotated[int, Query(ge=1, le=100)] = 10,
    worker_id: Annotated[str, Query(min_length=1, max_length=64)] = "http-worker",
) -> WorkerRunOut:
    """Una corrida acotada del worker. Seguro ante llamadas repetidas o concurrentes."""
    return WorkerRunOut.from_report(runtime.worker(worker_id).run_once(max_operations))
