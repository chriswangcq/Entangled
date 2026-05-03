"""Entangled liveness and readiness contract."""

from fastapi import HTTPException


class _Store:
    def __init__(self, entities):
        self.entities = entities


def test_health_is_liveness_even_before_schema(monkeypatch):
    from entangled.app import health as health_module

    monkeypatch.setattr(health_module, "get_store", lambda: _Store([]))

    result = health_module.health()

    assert result["status"] == "ok"
    assert result["entities"] == 0


def test_ready_fails_before_any_schema(monkeypatch):
    from entangled.app import health as health_module

    monkeypatch.setattr(health_module, "get_store", lambda: _Store([]))

    try:
        health_module.ready(required="")
    except HTTPException as exc:
        assert exc.status_code == 503
        assert exc.detail["status"] == "not_ready"
    else:
        raise AssertionError("readiness should fail before schema registration")


def test_ready_fails_when_required_schema_missing(monkeypatch):
    from entangled.app import health as health_module

    monkeypatch.setattr(health_module, "get_store", lambda: _Store(["agents"]))

    try:
        health_module.ready(required="agents,messages")
    except HTTPException as exc:
        assert exc.status_code == 503
        assert exc.detail["missing"] == ["messages"]
    else:
        raise AssertionError("readiness should fail when required schema is missing")


def test_ready_succeeds_with_required_schema(monkeypatch):
    from entangled.app import health as health_module

    monkeypatch.setattr(health_module, "get_store", lambda: _Store(["agents", "messages"]))

    result = health_module.ready(required="agents,messages")

    assert result["status"] == "ready"
    assert result["missing"] == []
