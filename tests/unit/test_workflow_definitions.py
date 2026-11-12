"""Validación estática de los workflows de n8n y del entorno Docker Compose.

No requiere n8n ni Docker: protege las propiedades de seguridad e idempotencia que la
integración depende de mantener cuando alguien edita y reexporta los archivos.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS = ROOT / "n8n" / "workflows"
ENGINE_URL = re.compile(r"^=?http://engine:8000/")
UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
# Generadores de valores no determinísticos: usarlos como Idempotency-Key rompería la
# deduplicación ante reintentos del cliente.
NON_DETERMINISTIC = ("$now", "$today", "uuid", "Math.random", "$randomString", "Date.now",
                     "$execution.id", "$runIndex")  # fmt: skip
ALLOWED_NODE_TYPES = {
    "n8n-nodes-base.webhook",
    "n8n-nodes-base.if",
    "n8n-nodes-base.httpRequest",
    "n8n-nodes-base.respondToWebhook",
    "n8n-nodes-base.scheduleTrigger",
    "n8n-nodes-base.stopAndError",
    "n8n-nodes-base.noOp",
}


def load(name: str) -> dict:
    return json.loads((WORKFLOWS / name).read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def intake() -> dict:
    return load("01-intake.json")


@pytest.fixture(scope="module")
def recovery() -> dict:
    return load("02-recovery.json")


def nodes_by_name(workflow: dict) -> dict[str, dict]:
    return {node["name"]: node for node in workflow["nodes"]}


def strings(value) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        return [s for v in value.values() for s in strings(v)]
    if isinstance(value, list):
        return [s for v in value for s in strings(v)]
    return []


def credential_names(workflow: dict) -> set[str]:
    return {
        ref["name"] for node in workflow["nodes"] for ref in node.get("credentials", {}).values()
    }


# ------------------------------------------------------------------- comunes


@pytest.mark.parametrize("name", ["01-intake.json", "02-recovery.json"])
def test_workflow_is_importable_shape(name):
    workflow = load(name)
    assert {"id", "name", "nodes", "connections", "settings"} <= set(workflow)
    names = nodes_by_name(workflow)
    assert len(names) == len(workflow["nodes"]), "nombres de nodo duplicados"
    for source, outputs in workflow["connections"].items():
        assert source in names
        for branch in outputs["main"]:
            for link in branch:
                assert link["node"] in names
    assert workflow["active"] is False  # se activa después de asignar credenciales


@pytest.mark.parametrize("name", ["01-intake.json", "02-recovery.json"])
def test_only_orchestration_nodes_no_code_or_commands(name):
    types = {node["type"] for node in load(name)["nodes"]}
    assert types <= ALLOWED_NODE_TYPES, types - ALLOWED_NODE_TYPES


@pytest.mark.parametrize("name", ["01-intake.json", "02-recovery.json"])
def test_http_nodes_only_call_the_engine_over_the_internal_network(name):
    http_nodes = [n for n in load(name)["nodes"] if n["type"] == "n8n-nodes-base.httpRequest"]
    assert http_nodes
    for node in http_nodes:
        params = node["parameters"]
        assert ENGINE_URL.match(params["url"]), params["url"]
        assert params["authentication"] == "genericCredentialType"
        assert params["genericAuthType"] == "httpBearerAuth"
        assert "headerParameters" not in params or all(
            h["name"].lower() != "authorization" for h in params["headerParameters"]["parameters"]
        )
        assert params["options"]["timeout"] > 0


@pytest.mark.parametrize("name", ["01-intake.json", "02-recovery.json"])
def test_no_embedded_secrets(name):
    workflow = load(name)
    for node in workflow["nodes"]:
        for ref in node.get("credentials", {}).values():
            assert set(ref) == {"id", "name"}, "las credenciales se referencian, no se embeben"
    text = json.dumps(workflow)
    assert "Bearer " not in text
    for value in strings(workflow):
        if value.startswith("="):
            continue  # expresiones
        for token in re.findall(r"[A-Za-z0-9_\-]{32,}", value):
            assert UUID.match(token), f"posible secreto embebido en {name}"


# -------------------------------------------------------------------- intake


def test_intake_uses_only_the_engine_api_credential(intake):
    assert credential_names(intake) == {"Recovery Webhook Inbound", "Recovery Engine API"}
    text = json.dumps(intake)
    assert "Recovery Engine Admin" not in text
    assert "/worker/run" not in text
    assert "/simulator" not in text


def test_intake_never_forwards_fault_scenarios(intake):
    assert "scenario" not in json.dumps(intake)
    execute = nodes_by_name(intake)["Ejecutar primer intento"]["parameters"]
    assert not execute.get("sendBody")


ALLOWED_FIELDS = "['operation_type', 'payload']"
UNEXPECTED = f"Object.keys($json.body ?? {{}}).filter(k => !{ALLOWED_FIELDS}.includes(k))"


def test_intake_rejects_unexpected_fields_explicitly(intake):
    """Un campo como ``scenario`` se rechaza con 400 antes de llamar al motor."""
    nodes = nodes_by_name(intake)
    conditions = nodes["¿Pedido válido?"]["parameters"]["conditions"]["conditions"]
    assert {
        "leftValue": f"={{{{ {UNEXPECTED}.length === 0 }}}}",
        "operator": {"type": "boolean", "operation": "true", "singleValue": True},
    }.items() <= next(c for c in conditions if "Object.keys" in c["leftValue"]).items()
    reject = nodes["Responder pedido inválido"]["parameters"]
    assert reject["options"]["responseCode"] == 400
    assert "error: 'unexpected_fields'" in reject["responseBody"]
    assert f"fields: {UNEXPECTED}" in reject["responseBody"]
    [_, false_branch] = intake["connections"]["¿Pedido válido?"]["main"]
    assert false_branch[0]["node"] == "Responder pedido inválido"


def test_intake_answers_dead_letter_with_conflict_and_reason(intake):
    respond = nodes_by_name(intake)["Responder estado"]["parameters"]
    code = respond["options"]["responseCode"]
    assert "{ succeeded: 200, failed_permanent: 422, dead_letter: 409 }" in code
    assert "?? 202" in code  # solo los estados que siguen en recuperación automática
    body = respond["responseBody"]
    for field in ("operation_id: $json.body.id", "status: $json.body.status"):
        assert field in body
    assert (
        "reason: $json.body.status === 'dead_letter' "
        "? ($json.body.last_error ?? 'automatic_recovery_exhausted') : undefined"
    ) in body


def test_intake_webhook_is_authenticated_and_answers_explicitly(intake):
    webhook = nodes_by_name(intake)["Webhook entrante"]
    assert webhook["parameters"]["authentication"] == "headerAuth"
    assert webhook["parameters"]["responseMode"] == "responseNode"
    assert set(webhook["credentials"]) == {"httpHeaderAuth"}


def test_idempotency_key_is_forwarded_from_the_caller_never_generated(intake):
    register = nodes_by_name(intake)["Registrar operación"]["parameters"]
    [header] = register["headerParameters"]["parameters"]
    assert header == {
        "name": "Idempotency-Key",
        "value": "={{ $json.headers['idempotency-key'] }}",
    }
    text = json.dumps(intake)
    for generator in NON_DETERMINISTIC:
        assert generator not in text, generator


def test_intake_only_forwards_operation_type_and_payload(intake):
    body = nodes_by_name(intake)["Registrar operación"]["parameters"]["jsonBody"]
    assert "operation_type: $json.body.operation_type" in body
    assert "payload: $json.body.payload" in body
    assert "...$json.body" not in body


def test_intake_validates_key_and_operation_type_before_calling_the_engine(intake):
    conditions = nodes_by_name(intake)["¿Pedido válido?"]["parameters"]["conditions"]
    left = {c["leftValue"] for c in conditions["conditions"]}
    assert {"={{ $json.headers['idempotency-key'] }}", "={{ $json.body.operation_type }}"} <= left
    assert len(left) == 3  # + campos no permitidos (test_intake_rejects_unexpected_fields...)
    assert conditions["combinator"] == "and"
    first = intake["connections"]["Webhook entrante"]["main"][0][0]["node"]
    assert first == "¿Pedido válido?"


def test_intake_executes_only_pending_operations(intake):
    check = nodes_by_name(intake)["¿Primer intento pendiente?"]["parameters"]["conditions"]
    assert check["conditions"][0]["leftValue"] == "={{ $json.body.status === 'pending' }}"
    [true_branch, false_branch] = intake["connections"]["¿Primer intento pendiente?"]["main"]
    assert true_branch[0]["node"] == "Ejecutar primer intento"
    assert false_branch[0]["node"] == "Consultar estado"


def test_intake_does_not_retry_execute_and_does_not_store_executions(intake):
    nodes = nodes_by_name(intake)
    assert not nodes["Ejecutar primer intento"].get("retryOnFail")
    # Los datos de ejecución incluyen los headers del webhook (con su secreto).
    settings = intake["settings"]
    assert settings["saveDataSuccessExecution"] == "none"
    assert settings["saveDataErrorExecution"] == "none"
    assert settings["saveManualExecutions"] is False


# ------------------------------------------------------------------ recovery


def test_recovery_calls_the_bounded_worker_with_the_admin_credential(recovery):
    nodes = nodes_by_name(recovery)
    worker = nodes["Ejecutar worker"]
    assert credential_names(recovery) == {"Recovery Engine Admin"}
    assert worker["parameters"]["url"] == "http://engine:8000/worker/run"
    query = {p["name"]: p["value"] for p in worker["parameters"]["queryParameters"]["parameters"]}
    assert query == {"max_operations": "25", "worker_id": "n8n-recovery"}
    assert not worker.get("retryOnFail")
    assert "onError" not in worker  # un fallo del motor queda visible como ejecución fallida


def test_recovery_is_scheduled_and_flags_operations_needing_attention(recovery):
    nodes = nodes_by_name(recovery)
    [interval] = nodes["Cada 30 segundos"]["parameters"]["rule"]["interval"]
    assert interval == {"field": "seconds", "secondsInterval": 30}
    condition = nodes["¿Requiere atención?"]["parameters"]["conditions"]["conditions"][0]
    for marker in ("dead_letter", "lease_lost", "error_left_for_lease_expiry"):
        assert marker in condition["leftValue"]
    [alert, quiet] = recovery["connections"]["¿Requiere atención?"]["main"]
    assert nodes[alert[0]["node"]]["type"] == "n8n-nodes-base.stopAndError"
    assert nodes[quiet[0]["node"]]["type"] == "n8n-nodes-base.noOp"


# ---------------------------------------------------------- compose y plantillas


def service_block(compose: str, name: str) -> str:
    match = re.search(rf"^  {name}:\n(.*?)(?=^  \w[\w-]*:\n|^\S)", compose, re.M | re.S)
    assert match, name
    return match.group(1)


@pytest.fixture(scope="module")
def compose() -> str:
    return (ROOT / "compose.yaml").read_text(encoding="utf-8")


def test_compose_publishes_ports_only_on_localhost(compose):
    mappings = re.findall(r"^\s+- \"([^\"]+:\d+)\"\s*$", compose, re.M)
    port_mappings = [m for m in mappings if re.fullmatch(r"[\d.]*:?\d+:\d+", m)]
    assert sorted(port_mappings) == ["127.0.0.1:5678:5678", "127.0.0.1:8000:8000"]


def test_compose_has_exactly_two_services(compose):
    services = compose.split("\nservices:\n", 1)[1].split("\nvolumes:\n", 1)[0]
    assert re.findall(r"^  (\w[\w-]*):\n", services, re.M) == ["engine", "n8n"]


def test_n8n_has_no_access_to_engine_databases_or_tokens(compose):
    n8n = service_block(compose, "n8n")
    assert "engine-data" not in n8n
    assert "/app/data" not in n8n
    assert "ENGINE_API_TOKEN" not in n8n
    assert "ADMIN_API_TOKEN" not in n8n
    assert "./n8n/workflows:/workflows:ro" in n8n
    assert "n8n-nodes-base.executeCommand" in n8n  # excluido explícitamente
    assert 'N8N_BLOCK_ENV_ACCESS_IN_NODE: "true"' in n8n


def test_compose_pins_n8n_and_runs_engine_in_demo_mode(compose):
    n8n = service_block(compose, "n8n")
    [image] = re.findall(r"image: (\S+)", n8n)
    assert re.fullmatch(r"docker\.n8n\.io/n8nio/n8n:\d+\.\d+\.\d+", image)
    engine = service_block(compose, "engine")
    assert "APP_ENV: demo" in engine
    assert "${ENGINE_API_TOKEN:?" in engine
    assert "${ADMIN_API_TOKEN:?" in engine
    assert "read_only: true" in engine


def test_env_example_only_has_placeholders():
    values = dict(
        line.split("=", 1)
        for line in (ROOT / ".env.example").read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#")
    )
    for key in ("ENGINE_API_TOKEN", "ADMIN_API_TOKEN", "WEBHOOK_SECRET", "N8N_ENCRYPTION_KEY"):
        assert values[key] == "replace-me"


def test_env_files_are_ignored_and_not_copied_into_the_image():
    gitignore = (ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()
    assert ".env" in gitignore
    assert "!.env.example" in gitignore
    dockerignore = (ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()
    assert ".env" in dockerignore
    assert "data/" in dockerignore
