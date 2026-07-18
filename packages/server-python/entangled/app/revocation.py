"""Authoritative session state plus durable revocation delivery.

Gateway Postgres remains the only account authority. A service-authenticated
Gateway HTTP endpoint validates each principal and returns the durable outbox
watermark that has been published to the Redis Stream.

The stream is only the low-latency delivery plane. Every process consumes it
with its own XREAD cursor. Startup, Redis restart, stream trim gaps, stale
authority responses, and malformed events all fail closed and require a fresh
authority resync before readiness can recover. Every new WebSocket principal
is also checked against Gateway's current user and session state, so an old JWT
cannot reconnect merely because its revocation event predates this process.
"""

from __future__ import annotations

import asyncio
import json
import math
import secrets
import time
from dataclasses import dataclass
from typing import Awaitable, Callable, Mapping, Optional, Protocol, Sequence

from .auth import SessionPrincipal


REVOCATION_EVENT_VERSION = 1
AUTHORITY_API_VERSION = 1
REVOCATION_KINDS = frozenset(
    {"session_revoked", "user_epoch_advanced", "account_blocked"}
)


class RevocationContractError(ValueError):
    """A stream event or materialized authority record is invalid."""


def _canonical_text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise RevocationContractError(f"{field} must be canonical text")
    return value


def _nonnegative_integer(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise RevocationContractError(f"{field} must be a non-negative integer")
    return value


def _numeric_timestamp(value: object, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RevocationContractError(f"{field} must be a numeric timestamp")
    parsed = float(value)
    if not math.isfinite(parsed):
        raise RevocationContractError(f"{field} must be finite")
    return parsed


def _stream_id_tuple(value: str) -> tuple[int, int]:
    try:
        major, minor = value.split("-", 1)
        parsed = int(major), int(minor)
    except (AttributeError, TypeError, ValueError) as exc:
        raise RevocationContractError("invalid Redis Stream id") from exc
    if parsed[0] < 0 or parsed[1] < 0 or value != f"{parsed[0]}-{parsed[1]}":
        raise RevocationContractError("invalid Redis Stream id")
    return parsed


def _reject_duplicate_object_pairs(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise RevocationContractError(f"duplicate JSON field: {key}")
        value[key] = item
    return value


def _json_mapping(raw: str, field: str) -> Mapping[str, object]:
    try:
        value = json.loads(raw, object_pairs_hook=_reject_duplicate_object_pairs)
    except RevocationContractError:
        raise
    except (TypeError, json.JSONDecodeError) as exc:
        raise RevocationContractError(f"{field} is not valid JSON") from exc
    if not isinstance(value, dict):
        raise RevocationContractError(f"{field} must be a JSON object")
    return value


@dataclass(frozen=True)
class RevocationEvent:
    event_id: str
    sequence: int
    namespace: str
    kind: str
    user_id: str
    session_id: Optional[str]
    auth_epoch: int
    occurred_at: str
    reason: str

    @classmethod
    def from_mapping(
        cls, raw: Mapping[str, object], *, expected_namespace: str
    ) -> "RevocationEvent":
        if _nonnegative_integer(raw.get("version"), "version") != REVOCATION_EVENT_VERSION:
            raise RevocationContractError("unsupported revocation event version")
        namespace = _canonical_text(raw.get("namespace"), "namespace")
        if namespace != expected_namespace:
            raise RevocationContractError("revocation namespace mismatch")
        kind = _canonical_text(raw.get("kind"), "kind")
        if kind not in REVOCATION_KINDS:
            raise RevocationContractError("unsupported revocation kind")
        sid_raw = raw.get("sid")
        session_id = None if sid_raw is None else _canonical_text(sid_raw, "sid")
        if kind == "session_revoked" and session_id is None:
            raise RevocationContractError("session_revoked requires sid")
        if kind != "session_revoked" and session_id is not None:
            raise RevocationContractError("user-wide revocation must not carry sid")
        return cls(
            event_id=_canonical_text(raw.get("event_id"), "event_id"),
            sequence=_nonnegative_integer(raw.get("sequence"), "sequence"),
            namespace=namespace,
            kind=kind,
            user_id=_canonical_text(raw.get("user_id"), "user_id"),
            session_id=session_id,
            auth_epoch=_nonnegative_integer(raw.get("auth_epoch"), "auth_epoch"),
            occurred_at=_canonical_text(raw.get("occurred_at"), "occurred_at"),
            reason=_canonical_text(raw.get("reason"), "reason"),
        )


@dataclass(frozen=True)
class AuthorityWatermark:
    generation: str
    state_revision: int
    stream_id: str
    checked_at: float

    @property
    def consistency_token(self) -> tuple[str, int, str]:
        return self.generation, self.state_revision, self.stream_id


@dataclass(frozen=True)
class StreamRecord:
    stream_id: str
    values: Mapping[str, str]


@dataclass(frozen=True)
class StreamSnapshot:
    run_id: str
    first_id: Optional[str]
    last_id: str


class RevocationStreamPort(Protocol):
    async def snapshot(self) -> StreamSnapshot: ...

    async def read_after(
        self, stream_id: str, *, block_milliseconds: int
    ) -> Sequence[StreamRecord]: ...

    async def close(self) -> None: ...


class SessionAuthorityPort(Protocol):
    async def resync(self) -> AuthorityWatermark: ...

    async def principal_is_current(self, principal: SessionPrincipal) -> bool: ...

    async def close(self) -> None: ...


class RedisRevocationStream:
    """Thin redis.asyncio stream adapter."""

    def __init__(self, *, redis_url: str, stream_key: str, redis_client=None) -> None:
        if not redis_url or not stream_key:
            raise ValueError("revocation Redis URL and stream key are required")
        if redis_client is None:
            from redis.asyncio import Redis

            redis_client = Redis.from_url(redis_url, decode_responses=True)
        self._redis = redis_client
        self._stream_key = stream_key

    async def snapshot(self) -> StreamSnapshot:
        server = await self._redis.info(section="server")
        run_id = _canonical_text(server.get("run_id"), "redis run_id")
        try:
            info = await self._redis.xinfo_stream(self._stream_key)
        except Exception as exc:
            if "no such key" not in str(exc).lower():
                raise
            info = {}
        first_entry = info.get("first-entry")
        first_id = str(first_entry[0]) if first_entry else None
        last_id = str(info.get("last-generated-id") or "0-0")
        if first_id is not None:
            _stream_id_tuple(first_id)
        _stream_id_tuple(last_id)
        return StreamSnapshot(run_id, first_id, last_id)

    async def read_after(
        self, stream_id: str, *, block_milliseconds: int
    ) -> Sequence[StreamRecord]:
        batches = await self._redis.xread(
            {self._stream_key: stream_id},
            block=block_milliseconds,
            count=100,
        )
        return [
            StreamRecord(str(item_id), values)
            for _key, entries in batches
            for item_id, values in entries
        ]

    async def close(self) -> None:
        await self._redis.aclose()


class GatewaySessionAuthority:
    """Service-authenticated HTTP adapter to Gateway's Postgres authority."""

    def __init__(
        self,
        *,
        base_url: str,
        service_token: str,
        namespace: str,
        response_max_age_seconds: float,
        clock: Callable[[], float] = time.time,
        challenge_factory: Callable[[], str] = lambda: secrets.token_urlsafe(18),
        http_client=None,
    ) -> None:
        if not isinstance(base_url, str) or not base_url.strip():
            raise ValueError("Gateway authority URL is required")
        if not isinstance(service_token, str) or not service_token.strip():
            raise ValueError("Gateway authority service token is required")
        if response_max_age_seconds <= 0:
            raise ValueError("authority response max age must be positive")
        self._namespace = _canonical_text(namespace, "namespace")
        self._response_max_age_seconds = response_max_age_seconds
        self._clock = clock
        self._challenge_factory = challenge_factory
        self._owns_client = http_client is None
        if http_client is None:
            import httpx

            http_client = httpx.AsyncClient(
                base_url=base_url.rstrip("/"),
                headers={"Authorization": f"Bearer {service_token}"},
                timeout=5.0,
            )
        self._client = http_client

    def _parse_watermark(self, value: Mapping[str, object]) -> AuthorityWatermark:
        if _nonnegative_integer(value.get("version"), "version") != AUTHORITY_API_VERSION:
            raise RevocationContractError("unsupported authority API version")
        if _canonical_text(value.get("namespace"), "namespace") != self._namespace:
            raise RevocationContractError("authority namespace mismatch")
        if value.get("outbox_caught_up") is not True:
            raise RevocationContractError("authority outbox is not caught up")
        checked_at = _numeric_timestamp(value.get("checked_at"), "checked_at")
        age = self._clock() - checked_at
        if age < -2 or age > self._response_max_age_seconds:
            raise RevocationContractError("authority response is stale")
        stream_id = _canonical_text(value.get("stream_id"), "stream_id")
        _stream_id_tuple(stream_id)
        return AuthorityWatermark(
            generation=_canonical_text(value.get("generation"), "generation"),
            state_revision=_nonnegative_integer(
                value.get("state_revision"), "state_revision"
            ),
            stream_id=stream_id,
            checked_at=checked_at,
        )

    @staticmethod
    def _response_mapping(response) -> Mapping[str, object]:
        if getattr(response, "status_code", None) != 200:
            raise RevocationContractError("Gateway session authority is unavailable")
        value = response.json()
        if not isinstance(value, dict):
            raise RevocationContractError("Gateway authority response must be an object")
        return value

    async def resync(self) -> AuthorityWatermark:
        response = await self._client.get(
            "/internal/auth/session-authority/watermark",
            params={"namespace": self._namespace},
        )
        return self._parse_watermark(self._response_mapping(response))

    async def principal_is_current(self, principal: SessionPrincipal) -> bool:
        request = {
            "version": AUTHORITY_API_VERSION,
            "namespace": principal.namespace,
            "user_id": principal.user_id,
            "sid": principal.session_id,
            "auth_epoch": principal.auth_epoch,
            "token_exp": principal.expires_at,
            "challenge": _canonical_text(
                self._challenge_factory(), "authority challenge"
            ),
        }
        response = await self._client.post(
            "/internal/auth/session-authority/introspect",
            json=request,
        )
        value = self._response_mapping(response)
        self._parse_watermark(value)
        active = value.get("active")
        if not isinstance(active, bool):
            raise RevocationContractError("authority active result must be boolean")
        echoed = value.get("principal")
        if not isinstance(echoed, dict) or echoed != request:
            raise RevocationContractError("authority principal echo mismatch")
        return active

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()


class RevocationStreamConsumer:
    """Per-process broadcast consumer gated by authority resynchronization."""

    def __init__(
        self,
        *,
        namespace: str,
        stream: RevocationStreamPort,
        authority: SessionAuthorityPort,
        on_event: Callable[[RevocationEvent], Awaitable[None]],
        on_unavailable: Callable[[str], Awaitable[None]],
        on_available: Optional[Callable[[], Awaitable[None]]] = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        block_milliseconds: int = 2000,
        retry_seconds: float = 1.0,
    ) -> None:
        self._namespace = _canonical_text(namespace, "namespace")
        self._stream = stream
        self._authority = authority
        self._on_event = on_event
        self._on_unavailable = on_unavailable
        self._on_available = on_available
        self._sleep = sleep
        self._block_milliseconds = block_milliseconds
        self._retry_seconds = retry_seconds
        self._ready = False
        self._cursor: Optional[str] = None
        self._run_id: Optional[str] = None
        self._outage_reported = False
        self._needs_resync = True
        self._authority_generation: Optional[str] = None
        self._authority_revision: Optional[int] = None

    @property
    def ready(self) -> bool:
        return self._ready

    async def _fail_closed(self, reason: str) -> None:
        self._ready = False
        self._needs_resync = True
        if not self._outage_reported:
            self._outage_reported = True
            await self._on_unavailable(reason)

    async def _inspect(self) -> None:
        snapshot = await self._stream.snapshot()
        if self._run_id is not None and self._run_id != snapshot.run_id:
            await self._fail_closed("redis_restart")
        elif (
            self._cursor not in (None, "0-0")
            and snapshot.first_id is not None
            and _stream_id_tuple(snapshot.first_id) > _stream_id_tuple(self._cursor)
        ):
            await self._fail_closed("stream_gap")

        watermark = await self._authority.resync()
        if (
            self._authority_generation is not None
            and watermark.generation != self._authority_generation
        ):
            await self._fail_closed("authority_generation_changed")
        elif (
            self._authority_revision is not None
            and watermark.state_revision < self._authority_revision
        ):
            await self._fail_closed("authority_revision_rollback")
        if _stream_id_tuple(watermark.stream_id) > _stream_id_tuple(snapshot.last_id):
            raise RevocationContractError(
                "authority watermark is ahead of the revocation stream"
            )
        if self._needs_resync:
            self._cursor = watermark.stream_id
            self._run_id = snapshot.run_id
            self._needs_resync = False

        self._authority_generation = watermark.generation
        self._authority_revision = watermark.state_revision
        if self._on_available is not None:
            await self._on_available()
        self._outage_reported = False
        self._ready = True

    async def run(self, stop: asyncio.Event) -> None:
        try:
            while not stop.is_set():
                try:
                    await self._inspect()
                    assert self._cursor is not None
                    records = await self._stream.read_after(
                        self._cursor,
                        block_milliseconds=self._block_milliseconds,
                    )
                    for record in records:
                        payload_raw = record.values.get("event")
                        if not payload_raw:
                            raise RevocationContractError("stream record omitted event")
                        event = RevocationEvent.from_mapping(
                            _json_mapping(payload_raw, "revocation event"),
                            expected_namespace=self._namespace,
                        )
                        await self._on_event(event)
                        self._cursor = record.stream_id
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    await self._fail_closed(type(exc).__name__)
                    await self._sleep(self._retry_seconds)
        finally:
            self._ready = False
            await self._stream.close()
            await self._authority.close()
