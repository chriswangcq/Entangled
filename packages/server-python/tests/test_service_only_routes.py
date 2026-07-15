import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from entangled.app import auth
from entangled.app.schema import router as schema_router
from entangled.app.state_transitions import router as state_transitions_router
from entangled.app.subagent_state import router as subagent_state_router


def _dependency_names(router, path, method):
    route = next(
        route
        for route in router.routes
        if route.path == path and method in route.methods
    )
    return [dependency.call.__name__ for dependency in route.dependant.dependencies]


@pytest.mark.parametrize(
    ("router", "path", "method"),
    [
        (schema_router, "/v1/schema/register", "POST"),
        (
            subagent_state_router,
            "/v1/subagents/{agent_id}/{subagent_id}/transition",
            "POST",
        ),
        (subagent_state_router, "/v1/subagents/states", "GET"),
        (
            state_transitions_router,
            "/v1/state_transitions/subagent/{subagent_id}",
            "GET",
        ),
    ],
)
def test_control_plane_routes_require_service_identity(router, path, method):
    assert "verify_service_token" in _dependency_names(router, path, method)
    assert "verify_service_or_user" not in _dependency_names(router, path, method)


def test_service_only_dependency_rejects_missing_service_header():
    dependency = getattr(auth, "verify_service_token", None)
    assert dependency is not None

    auth.configure_auth(jwt_secret="jwt-secret", service_token="service-secret")
    with pytest.raises(HTTPException) as exc:
        dependency(x_service_token=None, x_user_id=None)

    assert exc.value.status_code == 401


def test_service_only_dependency_accepts_configured_service_header():
    dependency = getattr(auth, "verify_service_token", None)
    assert dependency is not None

    auth.configure_auth(jwt_secret="jwt-secret", service_token="service-secret")
    assert dependency(
        x_service_token="service-secret",
        x_user_id="tenant-user",
    ) == "tenant-user"


def test_user_bearer_cannot_register_schema():
    auth.configure_auth(jwt_secret="jwt-secret", service_token="service-secret")
    app = FastAPI()
    app.include_router(schema_router)

    response = TestClient(app).post(
        "/v1/schema/register",
        json={"entities": []},
        headers={"Authorization": "Bearer otherwise-valid-user-jwt"},
    )

    assert response.status_code == 401
    assert response.json() == {"detail": "Missing service token"}
