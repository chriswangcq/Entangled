import asyncio
import json
import urllib.request

from entangled.sql.entity_store import SqlEntityStore


class _FakeResponse:
    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def read(self):
        return json.dumps({"success": True, "data": {"ok": True}}).encode("utf-8")


def test_action_hook_sends_internal_service_identity(monkeypatch):
    captured = {}

    def fake_urlopen(request, timeout):
        captured["timeout"] = timeout
        captured["headers"] = dict(request.header_items())
        return _FakeResponse()

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    store = SqlEntityStore(db=object())
    store._service_token = "service-secret"

    result = asyncio.run(
        store._call_action_hook(
            "http://business/internal/entities/chat/action/send",
            "chat",
            "send",
            "user-1",
            {"device_id": "device-1"},
            {"text": "hello"},
        )
    )

    assert result == {"ok": True}
    assert captured["timeout"] == 30
    assert captured["headers"]["X-service-token"] == "service-secret"
    assert captured["headers"]["X-internal-service"] == "entangled"
    assert captured["headers"]["Authorization"] == "Bearer service-secret"
