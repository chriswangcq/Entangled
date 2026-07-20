import asyncio
import json

import pytest
from starlette.websockets import WebSocketDisconnect

from entangled.app.auth import SessionPrincipal
from entangled.app.connection_registry import (
    AuthenticatedConnection,
    AuthenticatedConnectionRegistry,
)
from entangled.app.revocation import (
    AuthorityWatermark,
    GatewaySessionAuthority,
    RevocationEvent,
    RevocationStreamConsumer,
    StreamRecord,
    StreamSnapshot,
)


def _principal(user: str, sid: str, epoch: int = 1, exp: int = 1000):
    return SessionPrincipal(user, sid, epoch, exp, "staging")


def _event(kind: str, user: str, *, sid=None, epoch=1):
    return RevocationEvent(
        event_id=f"event-{kind}-{user}-{sid or epoch}",
        sequence=1,
        namespace="staging",
        kind=kind,
        user_id=user,
        session_id=sid,
        auth_epoch=epoch,
        occurred_at="2026-07-18T00:00:00Z",
        reason="test",
    )


async def _append(target, value):
    target.append(value)


def test_registry_revokes_only_matching_session_and_user_epoch():
    async def scenario():
        registry = AuthenticatedConnectionRegistry()
        closed = []

        async def add(connection_id, principal):
            async def close(code, reason):
                closed.append((connection_id, code, reason))

            await registry.register(
                AuthenticatedConnection(connection_id, principal, close)
            )

        await add("u1-s1", _principal("user-1", "sid-1"))
        await add("u1-s2", _principal("user-1", "sid-2"))
        await add("u2-s1", _principal("user-2", "sid-1"))

        assert await registry.apply(
            _event("session_revoked", "user-1", sid="sid-1")
        ) == 1
        assert [item[0] for item in closed] == ["u1-s1"]

        assert await registry.apply(
            _event("user_epoch_advanced", "user-1", epoch=2)
        ) == 1
        assert [item[0] for item in closed] == ["u1-s1", "u1-s2"]
        assert await registry.count() == 1

    asyncio.run(scenario())


def test_old_epoch_event_does_not_close_newer_connection_and_block_does():
    async def scenario():
        registry = AuthenticatedConnectionRegistry()
        closed = []

        async def close(code, reason):
            closed.append((code, reason))

        await registry.register(
            AuthenticatedConnection("new", _principal("user-1", "sid-new", 4), close)
        )
        assert await registry.apply(
            _event("user_epoch_advanced", "user-1", epoch=4)
        ) == 0
        assert closed == []
        assert await registry.apply(
            _event("account_blocked", "user-1", epoch=5)
        ) == 1
        assert closed == [(4403, "Account disabled")]

    asyncio.run(scenario())


def test_registry_admission_gate_is_atomic_with_fail_closed():
    async def scenario():
        registry = AuthenticatedConnectionRegistry(available=False)

        async def close(_code, _reason):
            return None

        connection = AuthenticatedConnection(
            "connection-1", _principal("user-1", "sid-1"), close
        )
        assert await registry.register(connection) is False
        await registry.mark_available()
        assert await registry.register(connection) is True
        assert await registry.close_everything("test outage") == 1
        assert await registry.register(connection) is False

    asyncio.run(scenario())


class _FakeStream:
    def __init__(self, snapshots, records=(), stop=None):
        self.snapshots = list(snapshots)
        self.records = list(records)
        self.stop = stop
        self.closed = False

    async def snapshot(self):
        if len(self.snapshots) > 1:
            return self.snapshots.pop(0)
        return self.snapshots[0]

    async def read_after(self, stream_id, *, block_milliseconds):
        records, self.records = self.records, []
        if self.stop is not None:
            self.stop.set()
        return records

    async def close(self):
        self.closed = True


class _FakeAuthority:
    def __init__(self, results, current=True):
        self.results = list(results)
        self.current = current
        self.closed = False
        self.resync_calls = 0

    async def resync(self):
        self.resync_calls += 1
        if len(self.results) > 1:
            result = self.results.pop(0)
        else:
            result = self.results[0]
        if isinstance(result, Exception):
            raise result
        return result

    async def principal_is_current(self, principal):
        return self.current

    async def close(self):
        self.closed = True


def _snapshot(run_id="redis-1", first="1-0", last="10-0"):
    return StreamSnapshot(run_id, first, last)


def _watermark(stream_id="10-0", revision=10):
    return AuthorityWatermark("gateway-db-1", revision, stream_id, 100.0)


def test_stream_first_start_resyncs_before_it_becomes_ready():
    async def scenario():
        authority = _FakeAuthority([_watermark()])
        consumer = RevocationStreamConsumer(
            namespace="staging",
            stream=_FakeStream([_snapshot()]),
            authority=authority,
            on_event=lambda event: _append([], event),
            on_unavailable=lambda reason: _append([], reason),
        )

        assert consumer.ready is False
        await consumer._inspect()
        assert consumer.ready is True
        assert authority.resync_calls == 1

    asyncio.run(scenario())


def test_stream_run_delivers_event_after_authority_watermark():
    async def scenario():
        stop = asyncio.Event()
        seen, failed = [], []
        payload = {
            "version": 1,
            "event_id": "event-1",
            "sequence": 9,
            "namespace": "staging",
            "kind": "session_revoked",
            "user_id": "user-1",
            "sid": "sid-1",
            "auth_epoch": 3,
            "occurred_at": "2026-07-18T00:00:00Z",
            "reason": "logout",
        }
        stream = _FakeStream(
            [_snapshot(last="11-0")],
            [StreamRecord("11-0", {"event": json.dumps(payload)})],
            stop,
        )
        authority = _FakeAuthority([_watermark("10-0")])
        consumer = RevocationStreamConsumer(
            namespace="staging",
            stream=stream,
            authority=authority,
            on_event=lambda event: _append(seen, event),
            on_unavailable=lambda reason: _append(failed, reason),
        )

        await consumer.run(stop)

        assert [event.event_id for event in seen] == ["event-1"]
        assert failed == []
        assert stream.closed is True
        assert authority.closed is True
        assert "token" not in repr(seen).lower()

    asyncio.run(scenario())


def test_redis_restart_stays_unready_when_authority_resync_is_unavailable():
    async def scenario():
        failed = []
        authority = _FakeAuthority([_watermark(), RuntimeError("down")])
        consumer = RevocationStreamConsumer(
            namespace="staging",
            stream=_FakeStream(
                [
                    _snapshot(run_id="redis-1"),
                    _snapshot(run_id="redis-2", first="20-0", last="20-0"),
                ]
            ),
            authority=authority,
            on_event=lambda event: _append([], event),
            on_unavailable=lambda reason: _append(failed, reason),
        )
        await consumer._inspect()

        try:
            await consumer._inspect()
        except RuntimeError:
            pass
        else:
            raise AssertionError("unavailable authority resync must fail")

        assert failed == ["redis_restart"]
        assert consumer.ready is False
        assert authority.resync_calls == 2

    asyncio.run(scenario())


def test_trim_gap_closes_connections_then_resyncs_to_authority_watermark():
    async def scenario():
        failed = []
        authority = _FakeAuthority([_watermark(), _watermark("20-0", 20)])
        consumer = RevocationStreamConsumer(
            namespace="staging",
            stream=_FakeStream(
                [
                    _snapshot(first="1-0", last="10-0"),
                    _snapshot(first="20-0", last="20-0"),
                ]
            ),
            authority=authority,
            on_event=lambda event: _append([], event),
            on_unavailable=lambda reason: _append(failed, reason),
        )
        await consumer._inspect()
        await consumer._inspect()

        assert failed == ["stream_gap"]
        assert consumer.ready is True
        assert authority.resync_calls == 2

    asyncio.run(scenario())


def test_authority_generation_change_closes_connections_without_stream_gap():
    async def scenario():
        failed = []
        authority = _FakeAuthority(
            [
                _watermark(),
                AuthorityWatermark("gateway-db-2", 20, "20-0", 100.0),
            ]
        )
        consumer = RevocationStreamConsumer(
            namespace="staging",
            stream=_FakeStream(
                [
                    _snapshot(first="1-0", last="10-0"),
                    _snapshot(first="1-0", last="20-0"),
                ]
            ),
            authority=authority,
            on_event=lambda event: _append([], event),
            on_unavailable=lambda reason: _append(failed, reason),
        )
        await consumer._inspect()
        await consumer._inspect()

        assert failed == ["authority_generation_changed"]
        assert consumer.ready is True

    asyncio.run(scenario())


def _authority_response(**overrides):
    value = {
        "version": 1,
        "namespace": "staging",
        "outbox_caught_up": True,
        "checked_at": 100.0,
        "generation": "gateway-db-1",
        "state_revision": 8,
        "stream_id": "10-0",
    }
    value.update(overrides)
    return value


class _Response:
    status_code = 200

    def __init__(self, value):
        self.value = value

    def json(self):
        return self.value


class _AuthorityHttp:
    def __init__(self, *, active=True, echo=True):
        self.active = active
        self.echo = echo
        self.requests = []

    async def get(self, path, *, params):
        self.requests.append(("GET", path, params))
        return _Response(_authority_response())

    async def post(self, path, *, json):
        self.requests.append(("POST", path, json))
        echoed = json if self.echo else {**json, "sid": "different"}
        return _Response(
            _authority_response(
                active=self.active,
                principal=echoed,
            )
        )


def _http_authority(http):
    return GatewaySessionAuthority(
        base_url="https://gateway.internal",
        service_token="service-secret",
        namespace="staging",
        response_max_age_seconds=10,
        clock=lambda: 100.0,
        challenge_factory=lambda: "challenge-0123456789",
        http_client=http,
    )


def test_gateway_authority_checks_exact_principal_and_old_session_result():
    async def scenario():
        http = _AuthorityHttp(active=False)
        authority = _http_authority(http)
        principal = _principal("user-1", "sid-1", 2)

        assert await authority.principal_is_current(principal) is False
        assert http.requests == [
            (
                "POST",
                "/internal/auth/session-authority/introspect",
                {
                    "version": 1,
                    "namespace": "staging",
                    "user_id": "user-1",
                    "sid": "sid-1",
                    "auth_epoch": 2,
                    "token_exp": 1000,
                    "challenge": "challenge-0123456789",
                },
            )
        ]

    asyncio.run(scenario())


def test_gateway_authority_rejects_mismatched_principal_echo():
    async def scenario():
        authority = _http_authority(_AuthorityHttp(echo=False))
        with pytest.raises(Exception, match="echo mismatch"):
            await authority.principal_is_current(
                _principal("user-1", "sid-1", 2)
            )

    asyncio.run(scenario())


class _Socket:
    query_params = {"token": "redacted"}
    headers = {}

    def __init__(self):
        self.accepted = False
        self.closed = None
        self.closed_event = asyncio.Event()

    async def accept(self):
        self.accepted = True

    async def close(self, code, reason):
        self.closed = (code, reason)
        self.closed_event.set()

    async def receive_json(self):
        await self.closed_event.wait()
        raise WebSocketDisconnect(1000)

    async def send_json(self, data):
        return None


class _Store:
    def get_schema(self):
        return {}


def _configure_ws(monkeypatch, *, principal_current, checker=None):
    from entangled.app import ws as ws_module

    registry = AuthenticatedConnectionRegistry()

    async def current(_principal):
        return principal_current

    ws_module.configure_connection_security(
        registry=registry,
        revocation_ready=lambda: True,
        principal_is_current=current,
    )
    ws_module.set_user_existence_checker(checker)
    monkeypatch.setattr(
        ws_module,
        "decode_principal_from_raw",
        lambda _token: _principal("user-1", "sid-1", exp=101),
    )
    monkeypatch.setattr(ws_module, "get_store", lambda: _Store())
    return ws_module, registry


def test_old_token_is_rejected_on_first_connection_after_restart(monkeypatch):
    async def scenario():
        ws_module, _registry = _configure_ws(
            monkeypatch, principal_current=False
        )
        socket = _Socket()
        await ws_module.ws_sync_handler(socket)
        assert socket.accepted is True
        assert socket.closed == (4401, "Session is no longer active")

    asyncio.run(scenario())


def test_revoke_racing_authority_introspection_cannot_escape_admission(monkeypatch):
    from entangled.app import ws as ws_module

    async def scenario():
        registry = AuthenticatedConnectionRegistry()

        async def revoke_while_checking(_principal):
            await registry.apply(
                _event("session_revoked", "user-1", sid="sid-1")
            )
            return True

        ws_module.configure_connection_security(
            registry=registry,
            revocation_ready=lambda: True,
            principal_is_current=revoke_while_checking,
        )
        ws_module.set_user_existence_checker(None)
        monkeypatch.setattr(
            ws_module,
            "decode_principal_from_raw",
            lambda _token: _principal("user-1", "sid-1", exp=101),
        )
        socket = _Socket()
        await ws_module.ws_sync_handler(socket)

        assert socket.accepted is True
        assert socket.closed == (4401, "Session revoked")
        assert await registry.count() == 0

    asyncio.run(scenario())


def test_access_expiry_closes_socket_without_client_traffic(monkeypatch):
    async def scenario():
        ws_module, registry = _configure_ws(
            monkeypatch, principal_current=True
        )
        socket = _Socket()

        async def immediate_sleep(_delay):
            return None

        async def current(_principal):
            return True

        ws_module.configure_connection_security(
            registry=registry,
            revocation_ready=lambda: True,
            principal_is_current=current,
            wall_clock=lambda: 100.0,
            sleep=immediate_sleep,
        )
        monkeypatch.setattr(ws_module, "register_client", lambda *args: None)
        monkeypatch.setattr(ws_module, "unregister_client", lambda *args: None)
        await ws_module.ws_sync_handler(socket)
        assert socket.accepted is True
        assert socket.closed == (4401, "Access token expired")

    asyncio.run(scenario())
