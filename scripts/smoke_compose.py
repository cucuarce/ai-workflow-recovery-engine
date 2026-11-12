"""Smoke test del entorno Docker Compose (motor + n8n) y del escenario timeout_after_processing.

Uso (desde la raíz del repo, con Docker en ejecución y un .env válido):

    uv run python scripts/smoke_compose.py            # levanta, provisiona n8n y verifica
    uv run python scripts/smoke_compose.py --down     # además baja el stack al terminar

Qué hace:
1. ``docker compose up -d --build --wait``.
2. Provisiona n8n: credenciales (generadas en memoria desde .env y enviadas por stdin al
   contenedor; nunca se escriben en el host), importa y publica los dos workflows.
3. Verifica exposición (solo 127.0.0.1), aislamiento de n8n respecto de las bases del
   motor, autenticación y separación de tokens.
4. Demo: plan de fallos ``timeout_after_processing`` cargado por la interfaz administrativa,
   pedido normal por el webhook, reconciliación por el Workflow 2 y evidencia del proveedor.
5. Operación pending abandonada (creada sin execute) recuperada por el Workflow 2, y
   webhook duplicado.
6. Operación agotada en ``dead_letter``: el webhook repetido responde 409 con el motivo.
7. Busca los valores secretos en logs, en los workflows y credenciales exportados desde
   n8n, en la base de n8n y en los archivos del repo.

Nunca imprime secretos. Sale con código 1 si alguna verificación falla.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from contextlib import closing
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
ENGINE = "http://127.0.0.1:8000"
WEBHOOK = "http://127.0.0.1:5678/webhook/operations"
WEBHOOK_HEADER = "X-Recovery-Webhook-Token"
SECRET_KEYS = ("ENGINE_API_TOKEN", "ADMIN_API_TOKEN", "WEBHOOK_SECRET", "N8N_ENCRYPTION_KEY")
WORKFLOW_IDS = ("RecIntakeWflow01", "RecRecoveryWf001")
RECOVERY_TIMEOUT_SECONDS = 120


class SmokeFailure(Exception):
    pass


# --------------------------------------------------------------------------- utilidades


def load_env() -> dict[str, str]:
    path = ROOT / ".env"
    if not path.exists():
        raise SmokeFailure("Falta .env: copiar .env.example y completar valores aleatorios")
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            values[key.strip()] = value.strip().strip('"').strip("'")
    for key in SECRET_KEYS:
        value = values.get(key, "")
        if len(value) < 32 or value == "replace-me":
            raise SmokeFailure(f"{key} en .env debe ser un valor aleatorio de >= 32 caracteres")
    if len({values[k] for k in SECRET_KEYS}) != len(SECRET_KEYS):
        raise SmokeFailure("Los secretos de .env deben ser distintos entre sí")
    return values


def compose(*args: str, stdin: str | None = None, check: bool = True) -> str:
    result = subprocess.run(
        ["docker", "compose", *args],
        cwd=ROOT,
        input=stdin,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if check and result.returncode != 0:
        raise SmokeFailure(f"docker compose {args[0]} falló:\n{result.stderr[-2000:]}")
    return result.stdout


def http(
    method: str,
    url: str,
    *,
    token: str | None = None,
    headers: dict[str, str] | None = None,
    body: Any = None,
    timeout: float = 30,
) -> tuple[int, Any]:
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(url, data=data, method=method)
    request.add_header("Content-Type", "application/json")
    if token:
        request.add_header("Authorization", f"Bearer {token}")
    for name, value in (headers or {}).items():
        request.add_header(name, value)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            status, raw = response.status, response.read()
    except urllib.error.HTTPError as exc:
        status, raw = exc.code, exc.read()
    try:
        return status, json.loads(raw) if raw else None
    except json.JSONDecodeError:
        return status, raw.decode(errors="replace")


class Report:
    def __init__(self) -> None:
        self.failures: list[str] = []

    def check(self, label: str, ok: bool, detail: Any = "") -> bool:
        print(f"  [{'OK' if ok else 'FALLA'}] {label}" + (f" -> {detail}" if detail != "" else ""))
        if not ok:
            self.failures.append(label)
        return ok


def wait_until(predicate, timeout: float, interval: float = 2.0):
    deadline = time.monotonic() + timeout
    while True:
        value = predicate()
        if value or time.monotonic() >= deadline:
            return value
        time.sleep(interval)


# ------------------------------------------------------------------------------- pasos


def provision_n8n(env: dict[str, str]) -> None:
    credentials = [
        {
            "id": "RecWebhookCred01",
            "name": "Recovery Webhook Inbound",
            "type": "httpHeaderAuth",
            "data": {"name": WEBHOOK_HEADER, "value": env["WEBHOOK_SECRET"]},
        },
        {
            "id": "RecEngApiCred001",
            "name": "Recovery Engine API",
            "type": "httpBearerAuth",
            "data": {"token": env["ENGINE_API_TOKEN"]},
        },
        {
            "id": "RecEngAdmCred001",
            "name": "Recovery Engine Admin",
            "type": "httpBearerAuth",
            "data": {"token": env["ADMIN_API_TOKEN"]},
        },
    ]
    # El JSON viaja por stdin y vive solo en /tmp del contenedor durante la importación.
    compose(
        "exec",
        "-T",
        "n8n",
        "sh",
        "-c",
        "umask 077; cat > /tmp/creds.json; n8n import:credentials --input=/tmp/creds.json "
        ">/dev/null; rc=$?; rm -f /tmp/creds.json; exit $rc",
        stdin=json.dumps(credentials),
    )
    compose("exec", "-T", "n8n", "n8n", "import:workflow", "--separate", "--input=/workflows")
    for workflow_id in WORKFLOW_IDS:
        compose("exec", "-T", "n8n", "n8n", "publish:workflow", f"--id={workflow_id}")
    # La activación hecha por CLI se toma al reiniciar el proceso principal.
    compose("restart", "n8n")
    compose("up", "-d", "--wait", "n8n")


def webhook(env: dict[str, str], key: str | None, body: dict, secret: str | None = "env"):
    headers = {}
    if secret is not None:
        headers[WEBHOOK_HEADER] = env["WEBHOOK_SECRET"] if secret == "env" else secret
    if key is not None:
        headers["Idempotency-Key"] = key
    return http("POST", WEBHOOK, headers=headers, body=body, timeout=60)


def wait_for_webhook(env: dict[str, str]) -> None:
    def ready() -> bool:
        status, _ = webhook(env, None, {})  # sin clave: el workflow responde 400
        return status == 400

    if not wait_until(ready, timeout=90):
        raise SmokeFailure("El webhook de n8n no quedó activo (¿workflow publicado?)")


def check_exposure_and_isolation(report: Report, env: dict[str, str]) -> None:
    print("\n== Exposición y aislamiento")
    config = json.loads(compose("config", "--format", "json"))
    published = [
        (name, port.get("host_ip"), port.get("published"))
        for name, service in config["services"].items()
        for port in service.get("ports", [])
    ]
    report.check(
        "todos los puertos publicados están ligados a 127.0.0.1",
        bool(published) and all(ip == "127.0.0.1" for _, ip, _ in published),
        published,
    )
    for service, port in (("engine", "8000"), ("n8n", "5678")):
        binding = compose("port", service, port).strip()
        report.check(f"{service}:{port} publicado como {binding}", binding.startswith("127.0.0.1:"))

    n8n_id = compose("ps", "-q", "n8n").strip()
    mounts = json.loads(
        subprocess.run(
            ["docker", "inspect", "--format", "{{json .Mounts}}", n8n_id],
            capture_output=True,
            text=True,
            check=True,
        ).stdout
    )
    destinations = sorted(m["Destination"] for m in mounts)
    report.check(
        "n8n solo monta su volumen y los workflows (sin el volumen del motor)",
        destinations == ["/home/node/.n8n", "/workflows"]
        and not any("engine-data" in (m.get("Name") or "") for m in mounts),
        destinations,
    )
    probe = compose(
        "exec",
        "-T",
        "n8n",
        "sh",
        "-c",
        "ls /app/data 2>/dev/null; find / -xdev \\( -name 'engine.db*' -o "
        "-name 'external_simulator.db*' \\) 2>/dev/null | head -1",
        check=False,
    ).strip()
    report.check("n8n no ve archivos de las bases del motor", probe == "", probe or "nada")
    n8n_env = compose("exec", "-T", "n8n", "env")
    report.check(
        "n8n no recibe ENGINE_API_TOKEN ni ADMIN_API_TOKEN por entorno",
        env["ENGINE_API_TOKEN"] not in n8n_env and env["ADMIN_API_TOKEN"] not in n8n_env,
    )


def check_auth(report: Report, env: dict[str, str]) -> None:
    print("\n== Autenticación y separación de credenciales")
    api, admin = env["ENGINE_API_TOKEN"], env["ADMIN_API_TOKEN"]
    cases = [
        ("GET /health sin token", "GET", "/health", None, 200),
        ("GET /operations sin token", "GET", "/operations", None, 401),
        ("GET /operations con token admin", "GET", "/operations", admin, 403),
        ("GET /operations con token API", "GET", "/operations", api, 200),
        ("POST /worker/run sin token", "POST", "/worker/run", None, 401),
        ("POST /worker/run con token API", "POST", "/worker/run", api, 403),
        ("GET /simulator/effects con token API", "GET", "/simulator/effects", api, 403),
        ("GET /simulator/effects con token admin", "GET", "/simulator/effects", admin, 200),
    ]
    for label, method, path, token, expected in cases:
        status, _ = http(method, ENGINE + path, token=token)
        report.check(f"{label} -> {expected}", status == expected, status)
    status, body = http(
        "POST",
        f"{ENGINE}/operations/op_inexistente/execute",
        token=api,
        body={"scenario": "timeout_after_processing"},
    )
    report.check(
        "execute con 'scenario' en demo -> 403 fault_injection_disabled",
        status == 403 and body.get("error") == "fault_injection_disabled",
        status,
    )
    status, _ = webhook(env, "smoke-auth", {"operation_type": "x"}, secret=None)
    report.check("webhook sin secreto -> rechazado", status in (401, 403), status)
    status, _ = webhook(env, "smoke-auth", {"operation_type": "x"}, secret="incorrecto" * 4)
    report.check("webhook con secreto incorrecto -> rechazado", status in (401, 403), status)
    status, body = webhook(env, None, {"operation_type": "issue_invoice", "payload": {}})
    report.check("webhook sin Idempotency-Key -> 400", status == 400, status)
    key = f"smoke-inject-{time.strftime('%Y%m%d%H%M%S')}"
    status, body = webhook(
        env,
        key,
        {"operation_type": "issue_invoice", "payload": {}, "scenario": "permanent_error"},
    )
    _, listed = http("GET", f"{ENGINE}/operations", token=api)
    report.check(
        "webhook con 'scenario' -> 400 unexpected_fields, sin registrar la operación",
        status == 400
        and body.get("error") == "unexpected_fields"
        and body.get("fields") == ["scenario"]
        and not [op for op in listed or [] if op["idempotency_key"] == key],
        (status, body),
    )


def wait_for_status(admin: str, api: str, op_id: str, target: str) -> dict:
    def reached():
        _, body = http("GET", f"{ENGINE}/operations/{op_id}", token=api)
        return body if body and body.get("status") == target else None

    return (
        wait_until(reached, timeout=RECOVERY_TIMEOUT_SECONDS)
        or http("GET", f"{ENGINE}/operations/{op_id}", token=api)[1]
    )


def provider_evidence(admin: str, op_id: str) -> dict[str, Any]:
    def get(path: str, **params: str) -> Any:
        query = "&".join(f"{k}={v}" for k, v in params.items())
        return http(
            "GET", f"{ENGINE}/simulator/{path}" + (f"?{query}" if query else ""), token=admin
        )[1]

    return {
        "requests": get("requests", operation_ref=op_id),
        "effects": get("effects", operation_ref=op_id),
        "lookups": get("lookups", idempotency_key=op_id),
        "duplicates": get("duplicates"),
    }


def demo_timeout_after_processing(report: Report, env: dict[str, str], run_id: str) -> dict:
    print("\n== Demo: timeout después del efecto -> reconciliación")
    api, admin = env["ENGINE_API_TOKEN"], env["ADMIN_API_TOKEN"]
    key = f"demo-timeout-{run_id}"

    status, _ = http(
        "PUT",
        f"{ENGINE}/simulator/fault-plans/by-idempotency-key/{key}",
        token=admin,
        body={"faults": [{"scenario": "timeout_after_processing"}]},
    )
    report.check("1. plan de fallos cargado por la interfaz administrativa", status == 200, status)

    # Pedido normal: el cliente no elige el fallo (un 'scenario' se rechaza, ver check_auth).
    request_body = {
        "operation_type": "issue_invoice",
        "payload": {"customer_id": "c-demo", "amount_cents": 15000},
    }
    status, first = webhook(env, key, request_body)
    report.check(
        "2. webhook con pedido normal -> 202 outcome_unknown",
        status == 202 and first.get("status") == "outcome_unknown",
        (status, first),
    )
    op_id = first.get("operation_id")
    if not op_id:
        raise SmokeFailure("El webhook no devolvió operation_id")

    before = provider_evidence(admin, op_id)
    [original] = before["requests"] or [None]
    report.check(
        "3. el proveedor aplicó el efecto aunque el motor no recibió respuesta",
        original is not None
        and original["effect_applied"]
        and not original["delivered_to_client"]
        and len(before["effects"]) == 1,
        original
        and {k: original[k] for k in ("scenario", "effect_applied", "delivered_to_client")},
    )

    final = wait_for_status(admin, api, op_id, "succeeded")
    report.check(
        "4. el Workflow 2 reconcilia: estado final succeeded",
        final["status"] == "succeeded",
        final["status"],
    )

    after = provider_evidence(admin, op_id)
    attempts = [(a["kind"], a["outcome"], a["worker_id"]) for a in final["attempts"]]
    [effect] = after["effects"] or [None]
    report.check(
        "5. una sola solicitud original al proveedor (sin reenvío)",
        len(after["requests"]) == 1,
        len(after["requests"]),
    )
    report.check("6. un único efecto externo", len(after["effects"]) == 1, len(after["effects"]))
    report.check(
        "7. la reconciliación consultó (lookup) y encontró el efecto",
        any(
            lk["found"] and lk["effect_id"] == (effect or {}).get("effect_id")
            for lk in after["lookups"]
        ),
        [(lk["found"], lk["effect_id"]) for lk in after["lookups"]],
    )
    report.check(
        "8. intentos: submit/timeout (api) y reconcile/effect_found (n8n-recovery)",
        attempts == [("submit", "timeout", "api"), ("reconcile", "effect_found", "n8n-recovery")],
        attempts,
    )
    report.check(
        "9. el motor enlaza el efecto del proveedor",
        effect is not None and final["external_effect_id"] == effect["effect_id"],
    )
    report.check(
        "10. cero duplicados en el proveedor", after["duplicates"] == [], after["duplicates"]
    )

    status, replay = webhook(env, key, request_body)
    again = provider_evidence(admin, op_id)
    report.check(
        "11. webhook repetido -> 200, misma operación, sin nuevas solicitudes ni efectos",
        status == 200
        and replay.get("operation_id") == op_id
        and replay.get("replayed") is True
        and len(again["requests"]) == 1
        and len(again["effects"]) == 1,
        (status, replay.get("status"), replay.get("replayed")),
    )
    return {
        "operation_id": op_id,
        "idempotency_key": key,
        "attempts": attempts,
        "provider_requests": len(after["requests"]),
        "provider_effects": len(after["effects"]),
        "lookups": [(lk["found"], lk["effect_id"]) for lk in after["lookups"]],
        "duplicates": after["duplicates"],
        "final_status": final["status"],
    }


def pending_recovery(report: Report, env: dict[str, str], run_id: str) -> None:
    print("\n== Operación pending abandonada (creada sin execute) + webhook duplicado")
    api, admin = env["ENGINE_API_TOKEN"], env["ADMIN_API_TOKEN"]
    key = f"demo-pending-{run_id}"
    body = {"operation_type": "issue_invoice", "payload": {"amount_cents": 990}}
    # Simula que n8n murió entre "Registrar operación" y "Ejecutar primer intento".
    status, created = http(
        "POST", f"{ENGINE}/operations", token=api, body=body, headers={"Idempotency-Key": key}
    )
    report.check(
        "operación registrada y abandonada en pending",
        status == 201 and created["status"] == "pending",
        status,
    )
    op_id = created["id"]
    final = wait_for_status(admin, api, op_id, "succeeded")
    report.check(
        "el Workflow 2 la recupera: succeeded",
        final["status"] == "succeeded",
        [(a["kind"], a["outcome"], a["worker_id"]) for a in final["attempts"]],
    )
    status, replay = webhook(env, key, body)
    evidence = provider_evidence(admin, op_id)
    report.check(
        "webhook duplicado -> 200 misma operación; 1 solicitud y 1 efecto",
        status == 200
        and replay.get("operation_id") == op_id
        and len(evidence["requests"]) == 1
        and len(evidence["effects"]) == 1,
        (status, len(evidence["requests"]), len(evidence["effects"])),
    )


def dead_letter_response(report: Report, env: dict[str, str], run_id: str) -> None:
    print("\n== dead_letter: el intake no lo presenta como recuperación en curso")
    api, admin = env["ENGINE_API_TOKEN"], env["ADMIN_API_TOKEN"]
    key = f"demo-dead-letter-{run_id}"
    # RETRY_MAX_ATTEMPTS no se define en compose: vale 5. Un 503 por intento lo agota.
    http(
        "PUT",
        f"{ENGINE}/simulator/fault-plans/by-idempotency-key/{key}",
        token=admin,
        body={"faults": [{"scenario": "http_503"}] * 5},
    )
    body = {"operation_type": "issue_invoice", "payload": {"amount_cents": 4040}}
    status, first = webhook(env, key, body)
    report.check(
        "primer intento con 503 -> 202 failed_retryable",
        status == 202 and first.get("status") == "failed_retryable",
        (status, first.get("status")),
    )
    op_id = first.get("operation_id")

    def exhausted():
        # El operador acelera los reintentos llamando al worker (el Workflow 2 también corre).
        http("POST", f"{ENGINE}/worker/run?worker_id=smoke-operator", token=admin)
        _, op = http("GET", f"{ENGINE}/operations/{op_id}", token=api)
        return op if op and op.get("status") == "dead_letter" else None

    final = wait_until(exhausted, timeout=RECOVERY_TIMEOUT_SECONDS)
    report.check("la operación queda en dead_letter tras 5 intentos", bool(final))
    status, replay = webhook(env, key, body)
    report.check(
        "webhook repetido -> 409 con operation_id, status=dead_letter y reason",
        status == 409
        and replay.get("operation_id") == op_id
        and replay.get("status") == "dead_letter"
        and bool(replay.get("reason")),
        (status, {k: replay.get(k) for k in ("status", "reason")}),
    )


def n8n_database_scan(secrets: dict[str, str]) -> tuple[set[str], list[tuple[str, str, str]]]:
    """Copia la base de n8n y devuelve (secretos en bytes crudos, filas que los contienen)."""
    with tempfile.TemporaryDirectory() as tmp:
        for name in ("database.sqlite", "database.sqlite-wal", "database.sqlite-shm"):
            subprocess.run(
                ["docker", "compose", "cp", f"n8n:/home/node/.n8n/{name}", str(Path(tmp) / name)],
                cwd=ROOT,
                capture_output=True,
                text=True,
            )
        db_path = Path(tmp) / "database.sqlite"
        if not db_path.exists():
            raise SmokeFailure("No se pudo copiar la base de n8n para inspeccionarla")
        content = b"".join(p.read_bytes() for p in Path(tmp).iterdir())
        raw = {k for k, v in secrets.items() if v.encode() in content}
        rows: list[tuple[str, str, str]] = []
        with closing(sqlite3.connect(db_path)) as conn:
            tables = [
                r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            ]
            for table in tables:
                for column in [r[1] for r in conn.execute(f'PRAGMA table_info("{table}")')]:
                    for key, value in secrets.items():
                        found = conn.execute(
                            f'SELECT 1 FROM "{table}" WHERE instr(CAST("{column}" AS TEXT), ?) > 0 '
                            "LIMIT 1",
                            (value,),
                        ).fetchone()
                        if found:
                            rows.append((key, table, column))
    return raw, rows


def scan_for_secrets(report: Report, env: dict[str, str]) -> None:
    print("\n== Búsqueda de secretos")
    secrets = {key: env[key] for key in SECRET_KEYS}

    def leaked_in(text: str) -> list[str]:
        return [key for key, value in secrets.items() if value in text]

    logs = compose("logs", "--no-color", "engine", "n8n")
    report.check("logs de engine y n8n sin secretos", not leaked_in(logs), leaked_in(logs) or "")

    # Lo que devolvería una exportación desde n8n (credenciales sin --decrypted: cifradas).
    exported = compose("exec", "-T", "n8n", "n8n", "export:workflow", "--all") + compose(
        "exec", "-T", "n8n", "n8n", "export:credentials", "--all"
    )
    report.check(
        "workflows y credenciales exportados desde n8n sin secretos",
        "RecIntakeWflow01" in exported and not leaked_in(exported),
        leaked_in(exported) or "",
    )

    # Tokens del motor y clave de cifrado: n8n solo los guarda cifrados. No deben aparecer
    # en la base ni siquiera como bytes residuales.
    never_in_clear = ("ENGINE_API_TOKEN", "ADMIN_API_TOKEN", "N8N_ENCRYPTION_KEY")
    raw, rows = n8n_database_scan(secrets)
    raw_leaks = [k for k in never_in_clear if k in raw]
    report.check(
        "base de n8n: tokens del motor y clave de cifrado nunca en claro",
        not raw_leaks and not [r for r in rows if r[0] in never_in_clear],
        raw_leaks or "",
    )
    # El secreto del webhook llega en los headers del pedido y n8n lo incluye en los datos de
    # ejecución. El intake no guarda ejecuciones (borrado lógico inmediato) y compose fuerza
    # el borrado físico en ~1 minuto: se verifica que ninguna fila lo conserve.
    started = time.monotonic()
    purged = wait_until(lambda: not n8n_database_scan(secrets)[1], timeout=180, interval=15)
    report.check(
        "base de n8n: secreto del webhook purgado de los datos de ejecución",
        bool(purged),
        f"en {time.monotonic() - started:.0f} s" if purged else n8n_database_scan(secrets)[1],
    )

    files = subprocess.run(
        ["git", "ls-files", "-co", "--exclude-standard"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.split()
    leaked_files = []
    for name in files:
        path = ROOT / name
        if path.is_file() and path.name != ".env":
            text = path.read_text(encoding="utf-8", errors="ignore")
            leaked_files += [f"{name}:{k}" for k in leaked_in(text)]
    report.check(
        "archivos versionables del repo sin secretos", not leaked_files, leaked_files or ""
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--skip-provision", action="store_true", help="no reimportar en n8n")
    parser.add_argument("--down", action="store_true", help="bajar el stack al terminar")
    args = parser.parse_args()

    report = Report()
    try:
        env = load_env()
        print("== Levantando el stack (docker compose up --build --wait)")
        compose("up", "-d", "--build", "--wait")
        if not args.skip_provision:
            print("== Provisionando n8n (credenciales, workflows, publicación)")
            provision_n8n(env)
        wait_for_webhook(env)
        run_id = time.strftime("%Y%m%d%H%M%S")
        check_exposure_and_isolation(report, env)
        check_auth(report, env)
        evidence = demo_timeout_after_processing(report, env, run_id)
        pending_recovery(report, env, run_id)
        dead_letter_response(report, env, run_id)
        scan_for_secrets(report, env)
        print("\n== Evidencia de reconciliación")
        print(json.dumps(evidence, indent=2, ensure_ascii=False))
    except SmokeFailure as exc:
        print(f"\nERROR: {exc}")
        return 1
    finally:
        if args.down:
            compose("down", check=False)

    print(
        f"\n{'SMOKE OK' if not report.failures else 'SMOKE CON FALLAS'}: "
        f"{len(report.failures)} verificaciones fallidas"
    )
    for failure in report.failures:
        print(f"  - {failure}")
    return 0 if not report.failures else 1


if __name__ == "__main__":
    sys.exit(main())
