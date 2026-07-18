"""Process-local registry for actively revoking Entangled WebSockets."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Awaitable, Callable

from .auth import SessionPrincipal
from .revocation import RevocationEvent


CloseConnection = Callable[[int, str], Awaitable[None]]


@dataclass(frozen=True)
class AuthenticatedConnection:
    connection_id: str
    principal: SessionPrincipal
    close: CloseConnection


class AuthenticatedConnectionRegistry:
    def __init__(self, *, available: bool = True) -> None:
        self._connections: dict[str, AuthenticatedConnection] = {}
        self._lock = asyncio.Lock()
        self._available = available

    async def register(self, connection: AuthenticatedConnection) -> bool:
        async with self._lock:
            if not self._available:
                return False
            self._connections[connection.connection_id] = connection
            return True

    async def mark_available(self) -> None:
        async with self._lock:
            self._available = True

    async def is_available(self) -> bool:
        async with self._lock:
            return self._available

    async def unregister(self, connection_id: str) -> None:
        async with self._lock:
            self._connections.pop(connection_id, None)

    async def count(self) -> int:
        async with self._lock:
            return len(self._connections)

    async def count_user(self, user_id: str) -> int:
        async with self._lock:
            return sum(
                1
                for connection in self._connections.values()
                if connection.principal.user_id == user_id
            )

    async def contains(self, connection_id: str) -> bool:
        async with self._lock:
            return connection_id in self._connections

    async def _take_matching(
        self, predicate: Callable[[SessionPrincipal], bool]
    ) -> list[AuthenticatedConnection]:
        async with self._lock:
            selected = [
                connection
                for connection in self._connections.values()
                if predicate(connection.principal)
            ]
            for connection in selected:
                self._connections.pop(connection.connection_id, None)
            return selected

    @staticmethod
    async def _close_all(
        connections: list[AuthenticatedConnection], *, code: int, reason: str
    ) -> int:
        for connection in connections:
            try:
                await connection.close(code, reason)
            except Exception:
                pass
        return len(connections)

    async def close_connection(
        self, connection_id: str, *, code: int, reason: str
    ) -> bool:
        async with self._lock:
            connection = self._connections.pop(connection_id, None)
        if connection is None:
            return False
        await self._close_all([connection], code=code, reason=reason)
        return True

    async def close_user(self, user_id: str) -> int:
        """Atomically detach and close every socket owned by one account."""

        selected = await self._take_matching(
            lambda principal: principal.user_id == user_id
        )
        return await self._close_all(
            selected, code=4403, reason="Account deleted"
        )

    async def apply(self, event: RevocationEvent) -> int:
        if event.kind == "session_revoked":
            selected = await self._take_matching(
                lambda principal: (
                    principal.namespace == event.namespace
                    and principal.user_id == event.user_id
                    and principal.session_id == event.session_id
                )
            )
            return await self._close_all(selected, code=4401, reason="Session revoked")
        if event.kind == "user_epoch_advanced":
            selected = await self._take_matching(
                lambda principal: (
                    principal.namespace == event.namespace
                    and principal.user_id == event.user_id
                    and principal.auth_epoch < event.auth_epoch
                )
            )
            return await self._close_all(selected, code=4401, reason="Session superseded")
        selected = await self._take_matching(
            lambda principal: (
                principal.namespace == event.namespace
                and principal.user_id == event.user_id
            )
        )
        return await self._close_all(selected, code=4403, reason="Account disabled")

    async def close_everything(self, reason: str = "Revocation plane unavailable") -> int:
        async with self._lock:
            self._available = False
            selected = list(self._connections.values())
            self._connections.clear()
        return await self._close_all(selected, code=1013, reason=reason)
