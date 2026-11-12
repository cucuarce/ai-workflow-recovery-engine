"""CLI local.

    uv run recovery-engine worker [--max-operations N] [--worker-id ID]
    uv run recovery-engine fault-plan <operation_id> http_503 http_429@120 success
    uv run recovery-engine show <operation_id>

Usa las mismas variables de entorno que la API (``RECOVERY_DATA_DIR``, ``RETRY_*``, ...).
Imprime JSON en stdout. No requiere exponer endpoints administrativos.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from contextlib import closing
from dataclasses import asdict

from recovery_engine.config import Settings
from recovery_engine.external.contracts import Scenario
from recovery_engine.external.simulator import FaultSpec
from recovery_engine.runtime import Runtime, build_runtime
from recovery_engine.services.operations import OperationService


def _parse_fault(text: str) -> FaultSpec:
    scenario, _, retry_after = text.partition("@")
    try:
        return FaultSpec(Scenario(scenario), retry_after or None)
    except ValueError as exc:
        valid = ", ".join(s.value for s in Scenario)
        raise argparse.ArgumentTypeError(f"escenario inválido '{scenario}' ({valid})") from exc


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="recovery-engine", description=__doc__.split("\n")[0])
    commands = parser.add_subparsers(dest="command", required=True)

    worker = commands.add_parser("worker", help="Una corrida acotada del worker de recuperación.")
    worker.add_argument("--max-operations", type=int, default=10)
    worker.add_argument("--worker-id", default="cli-worker")

    plan = commands.add_parser(
        "fault-plan", help="Guion de fallos del simulador para una operación."
    )
    plan.add_argument("operation_id")
    plan.add_argument("faults", nargs="*", type=_parse_fault, help="escenario[@retry_after]")

    show = commands.add_parser("show", help="Estado, intentos y transiciones de una operación.")
    show.add_argument("operation_id")
    return parser


def _show(runtime: Runtime, operation_id: str) -> dict:
    with closing(runtime.engine_db.connect()) as conn:
        detail = OperationService(conn, runtime.simulator).get_detail(operation_id)
    return {
        "operation": asdict(detail.operation),
        "attempts": [asdict(a) for a in detail.attempts],
        "transitions": [asdict(t) for t in detail.transitions],
        "provider": {
            "requests": [asdict(r) for r in runtime.simulator.list_requests(operation_id)],
            "effects": [asdict(e) for e in runtime.simulator.list_effects(operation_id)],
            "lookups": [asdict(lk) for lk in runtime.simulator.list_lookups(operation_id)],
        },
    }


def main(argv: Sequence[str] | None = None, *, runtime: Runtime | None = None) -> int:
    args = _build_parser().parse_args(argv)
    runtime = runtime or build_runtime(Settings.from_env())

    if args.command == "worker":
        report = runtime.worker(args.worker_id).run_once(args.max_operations)
        output: object = {**asdict(report), "processed": report.processed}
    elif args.command == "fault-plan":
        if not runtime.settings.fault_injection_enabled:
            sys.stderr.write("fault-plan requiere FAULT_INJECTION_ENABLED=true (no production)\n")
            return 2
        runtime.simulator.set_fault_plan(args.operation_id, args.faults)
        output = {
            "operation_ref": args.operation_id,
            "pending_faults": [
                asdict(f) for f in runtime.simulator.pending_faults(args.operation_id)
            ],
        }
    else:
        output = _show(runtime, args.operation_id)

    json.dump(output, sys.stdout, indent=2, ensure_ascii=False, default=str)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
