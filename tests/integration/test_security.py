"""Etapa 3: configuración segura por defecto, autenticación y superficie expuesta."""

from __future__ import annotations

from dataclasses import replace

import pytest
from fastapi.testclient import TestClient

from recovery_engine.api.app import create_app
from recovery_engine.api.errors import _status_for
from recovery_engine.cli import main
from recovery_engine.config import AppEnv, ConfigurationError, Settings
from recovery_engine.domain.errors import LeaseLostError
from recovery_engine.runtime import build_runtime
from support import TEST_ADMIN_TOKEN, TEST_ENGINE_TOKEN

API = {"Authorization": f"Bearer {TEST_ENGINE_TOKEN}"}
ADMIN = {"Authorization": f"Bearer {TEST_ADMIN_TOKEN}"}
BODY = {"operation_type": "issue_invoice", "payload": {"amount_cents": 1500}}


def paths(tmp_path) -> dict:
    return {"engine_db_path": tmp_path / "e.db", "simulator_db_path": tmp_path / "s.db"}


def demo_settings(tmp_path, **overrides) -> Settings:
    values = {
        **paths(tmp_path),
        "app_env": AppEnv.DEMO,
        "engine_api_token": TEST_ENGINE_TOKEN,
        "admin_api_token": TEST_ADMIN_TOKEN,
        "enable_admin_endpoints": True,
        "fault_injection_enabled": True,
    }
    return Settings(**(values | overrides))


def production_settings(tmp_path, **overrides) -> Settings:
    values = {**paths(tmp_path), "engine_api_token": TEST_ENGINE_TOKEN}
    return Settings(**(values | overrides))


# ------------------------------------------------------------ validación de Settings


def test_default_settings_are_production_and_refuse_to_start_without_token(tmp_path):
    with pytest.raises(ConfigurationError, match="ENGINE_API_TOKEN"):
        Settings(**paths(tmp_path))


def test_secure_defaults(tmp_path):
    settings = production_settings(tmp_path)
    assert settings.app_env is AppEnv.PRODUCTION
    assert not settings.enable_admin_endpoints
    assert not settings.fault_injection_enabled
    assert not settings.inline_scenarios_allowed
    assert not settings.observability_routes_enabled


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"fault_injection_enabled": True}, "prohibida con APP_ENV=production"),
        ({"enable_admin_endpoints": True}, "exige ADMIN_API_TOKEN"),
        ({"engine_api_token": "corto"}, "al menos 32"),
        ({"admin_api_token": TEST_ENGINE_TOKEN}, "deben ser distintos"),
        ({"lease_ttl_seconds": 0}, "LEASE_TTL_SECONDS"),
    ],
)
def test_invalid_production_combinations_fail_closed(tmp_path, overrides, message):
    with pytest.raises(ConfigurationError, match=message):
        production_settings(tmp_path, **overrides)


def test_demo_requires_engine_token(tmp_path):
    with pytest.raises(ConfigurationError, match="APP_ENV=demo exige ENGINE_API_TOKEN"):
        demo_settings(tmp_path, engine_api_token=None)


def test_demo_fault_injection_requires_admin_token(tmp_path):
    with pytest.raises(ConfigurationError, match="ADMIN_API_TOKEN"):
        demo_settings(tmp_path, enable_admin_endpoints=False, admin_api_token=None)


def test_admin_endpoints_require_admin_token_even_locally(tmp_path):
    with pytest.raises(ConfigurationError, match="ENABLE_ADMIN_ENDPOINTS=true exige"):
        Settings(**paths(tmp_path), app_env=AppEnv.LOCAL, enable_admin_endpoints=True)


def test_replace_cannot_bypass_validation(tmp_path):
    with pytest.raises(ConfigurationError):
        replace(production_settings(tmp_path), fault_injection_enabled=True)


def test_tokens_are_not_exposed_in_repr(tmp_path):
    text = repr(demo_settings(tmp_path))
    assert TEST_ENGINE_TOKEN not in text
    assert TEST_ADMIN_TOKEN not in text


ENV_VARS = (
    "APP_ENV",
    "ENGINE_API_TOKEN",
    "ADMIN_API_TOKEN",
    "ENABLE_ADMIN_ENDPOINTS",
    "FAULT_INJECTION_ENABLED",
)


@pytest.fixture
def clean_env(monkeypatch, tmp_path):
    for name in ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("RECOVERY_DATA_DIR", str(tmp_path))
    return monkeypatch


def test_from_env_defaults_to_production_and_fails_without_token(clean_env):
    with pytest.raises(ConfigurationError, match="APP_ENV=production exige ENGINE_API_TOKEN"):
        Settings.from_env()


def test_from_env_with_token_is_secure(clean_env):
    clean_env.setenv("ENGINE_API_TOKEN", TEST_ENGINE_TOKEN)
    settings = Settings.from_env()
    assert settings.app_env is AppEnv.PRODUCTION
    assert not settings.fault_injection_enabled
    assert not settings.enable_admin_endpoints


@pytest.mark.parametrize(
    ("name", "value", "message"),
    [
        ("APP_ENV", "staging", "APP_ENV inválido"),
        ("FAULT_INJECTION_ENABLED", "tal-vez", "valor booleano inválido"),
    ],
)
def test_from_env_rejects_invalid_values(clean_env, name, value, message):
    clean_env.setenv("ENGINE_API_TOKEN", TEST_ENGINE_TOKEN)
    clean_env.setenv(name, value)
    with pytest.raises(ConfigurationError, match=message):
        Settings.from_env()


def test_from_env_refuses_fault_injection_in_production(clean_env):
    clean_env.setenv("ENGINE_API_TOKEN", TEST_ENGINE_TOKEN)
    clean_env.setenv("FAULT_INJECTION_ENABLED", "true")
    with pytest.raises(ConfigurationError, match="prohibida"):
        Settings.from_env()


# ------------------------------------------------------------------- autenticación


@pytest.fixture
def demo_client(tmp_path):
    with TestClient(create_app(demo_settings(tmp_path))) as client:
        yield client


@pytest.mark.parametrize(
    ("method", "path", "headers", "expected"),
    [
        ("GET", "/health", {}, 200),
        ("GET", "/operations", {}, 401),
        ("GET", "/operations", {"Authorization": "Bearer otro-token"}, 403),
        ("GET", "/operations", {"Authorization": f"Basic {TEST_ENGINE_TOKEN}"}, 401),
        ("GET", "/operations", ADMIN, 403),
        ("GET", "/operations", API, 200),
        ("GET", "/operations", {"Authorization": f"bearer {TEST_ENGINE_TOKEN}"}, 200),
        ("POST", "/worker/run", {}, 401),
        ("POST", "/worker/run", API, 403),
        ("POST", "/worker/run", ADMIN, 200),
        ("GET", "/simulator/effects", API, 403),
        ("GET", "/simulator/effects", ADMIN, 200),
        ("PUT", "/simulator/fault-plans/op_x", API, 403),
        ("PUT", "/simulator/fault-plans/by-idempotency-key/k", API, 403),
        ("PUT", "/simulator/fault-plans/by-idempotency-key/k", ADMIN, 200),
    ],
)
def test_token_separation(demo_client, method, path, headers, expected):
    kwargs = {"json": {"faults": []}} if method == "PUT" else {}
    response = demo_client.request(method, path, headers=headers, **kwargs)
    assert response.status_code == expected
    if expected == 401:
        assert response.headers["WWW-Authenticate"] == "Bearer"


def test_authentication_errors_do_not_echo_tokens(demo_client):
    response = demo_client.get("/operations", headers={"Authorization": "Bearer secreto-x"})
    assert "secreto-x" not in response.text
    assert TEST_ENGINE_TOKEN not in response.text


def test_inline_scenario_is_rejected_in_demo_before_touching_the_operation(demo_client):
    op = demo_client.post("/operations", json=BODY, headers={**API, "Idempotency-Key": "k"}).json()

    response = demo_client.post(
        f"/operations/{op['id']}/execute", json={"scenario": "http_503"}, headers=API
    )

    assert response.status_code == 403
    assert response.json()["error"] == "fault_injection_disabled"
    assert demo_client.get(f"/operations/{op['id']}", headers=API).json()["status"] == "pending"
    assert demo_client.get("/simulator/requests", headers=ADMIN).json() == []


def test_production_exposes_only_operations_and_health(tmp_path):
    settings = production_settings(tmp_path)
    app = create_app(settings)
    assert set(app.openapi()["paths"]) == {
        "/operations",
        "/operations/{operation_id}",
        "/operations/{operation_id}/execute",
        "/health",
    }
    with TestClient(app) as client:
        assert client.get("/simulator/effects", headers=API).status_code == 404
        assert client.post("/worker/run", headers=API).status_code == 404
        response = client.post(
            "/operations/op_x/execute", json={"scenario": "http_503"}, headers=API
        )
        assert response.status_code == 403


def test_production_admin_mode_mounts_worker_but_never_the_simulator(tmp_path):
    settings = production_settings(
        tmp_path, enable_admin_endpoints=True, admin_api_token=TEST_ADMIN_TOKEN
    )
    app = create_app(settings)
    assert not settings.observability_routes_enabled
    assert not [p for p in app.openapi()["paths"] if p.startswith("/simulator")]
    with TestClient(app) as client:
        assert client.post("/worker/run", headers=ADMIN).status_code == 200
        assert client.get("/simulator/duplicates", headers=ADMIN).status_code == 404
        planned = client.put("/simulator/fault-plans/op_x", json={"faults": []}, headers=ADMIN)
        assert planned.status_code == 404


SIMULATOR_READ_PATHS = {
    "/simulator/requests",
    "/simulator/effects",
    "/simulator/lookups",
    "/simulator/duplicates",
}
FAULT_PLAN_PATHS = {
    "/simulator/fault-plans/{operation_ref}",
    "/simulator/fault-plans/by-idempotency-key/{idempotency_key}",
}


@pytest.mark.parametrize(
    ("env", "overrides", "expected"),
    [
        (AppEnv.PRODUCTION, {}, set()),
        (
            AppEnv.PRODUCTION,
            {"enable_admin_endpoints": True, "admin_api_token": TEST_ADMIN_TOKEN},
            set(),
        ),
        (AppEnv.DEMO, {}, SIMULATOR_READ_PATHS | FAULT_PLAN_PATHS),
        (AppEnv.DEMO, {"fault_injection_enabled": False}, SIMULATOR_READ_PATHS),
        (
            AppEnv.LOCAL,
            {"engine_api_token": None, "enable_admin_endpoints": False},
            SIMULATOR_READ_PATHS,
        ),
        (AppEnv.LOCAL, {}, SIMULATOR_READ_PATHS | FAULT_PLAN_PATHS),
    ],
    ids=["prod", "prod-admin", "demo", "demo-sin-fallos", "local-sin-admin", "local-admin"],
)
def test_simulator_routes_by_environment(tmp_path, env, overrides, expected):
    if env is AppEnv.PRODUCTION:
        settings = production_settings(tmp_path, **overrides)
    else:
        settings = demo_settings(tmp_path, app_env=env, **overrides)
    app = create_app(settings)
    mounted = {p for p in app.openapi()["paths"] if p.startswith("/simulator")}
    assert mounted == expected
    with TestClient(app) as client:
        status = client.get("/simulator/effects", headers=ADMIN).status_code
        assert status == (200 if expected else 404)


def test_simulator_routes_keep_admin_auth_in_demo(demo_client):
    assert demo_client.get("/simulator/effects").status_code == 401
    assert demo_client.get("/simulator/effects", headers=API).status_code == 403
    assert demo_client.get("/simulator/effects", headers=ADMIN).status_code == 200


def test_operations_reject_injection_fields_explicitly(demo_client):
    """Ni el registro ni el primer intento aceptan campos que controlen al proveedor."""
    created = demo_client.post(
        "/operations",
        json={**BODY, "scenario": "timeout_after_processing"},
        headers={**API, "Idempotency-Key": "inject"},
    )
    assert created.status_code == 422
    assert "scenario" in created.text
    assert demo_client.get("/operations", headers=API).json() == []


def test_cli_fault_plan_is_refused_without_fault_injection(tmp_path, capsys):
    runtime = build_runtime(production_settings(tmp_path))
    assert main(["fault-plan", "op_x", "http_503"], runtime=runtime) == 2
    assert "FAULT_INJECTION_ENABLED" in capsys.readouterr().err
    assert runtime.simulator.pending_faults("op_x") == []


def test_lease_lost_maps_to_conflict():
    assert _status_for(LeaseLostError("op_x", 3)) == 409
