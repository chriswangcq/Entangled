"""Lifecycle binding for the sealed Entangled Staging fixture socket."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from common.account_deletion_fixture import (
    FIXTURE_ENVIRONMENT,
    FixtureCategory,
    FixtureOwner,
    FixtureReplayLedger,
)
from common.account_deletion_fixture_ipc import (
    OwnerFixtureIpcError,
    OwnerFixtureIpcServer,
    load_owner_fixture_runtime,
)
from entangled.sql.entity_store import SqlEntityStore
from entangled.staging_fixture.relational_store import (
    EntangledRelationalFixtureStore,
)


class _DeferredRelationalStore:
    """Bind the canonical schema at request time, not before registration."""

    __slots__ = (
        "__capability_secret",
        "__derivation_secret",
        "__replay_ledger",
        "__store",
    )

    def __init__(
        self,
        *,
        store: SqlEntityStore,
        capability_secret: bytes,
        derivation_secret: bytes,
        replay_ledger: FixtureReplayLedger,
    ) -> None:
        self.__store = store
        self.__capability_secret = capability_secret
        self.__derivation_secret = derivation_secret
        self.__replay_ledger = replay_ledger

    def execute(self, payload: Mapping[str, Any]):
        exact = EntangledRelationalFixtureStore(
            namespace=FIXTURE_ENVIRONMENT,
            store=self.__store,
            capability_secret=self.__capability_secret,
            derivation_secret=self.__derivation_secret,
            replay_ledger=self.__replay_ledger,
        )
        return exact.execute(payload)


@dataclass(slots=True)
class EntangledOwnerFixtureRuntime:
    server: OwnerFixtureIpcServer

    async def start(self) -> None:
        await self.server.start()

    async def close(self) -> None:
        await self.server.close()


def _configured_paths(
    *, socket_dir: str, secret_dir: str, state_dir: str
) -> tuple[Path, Path, Path] | None:
    values = (socket_dir.strip(), secret_dir.strip(), state_dir.strip())
    if not any(values):
        return None
    if not all(values):
        raise OwnerFixtureIpcError("fixture runtime unavailable")
    paths = tuple(Path(value) for value in values)
    if any(not path.is_absolute() or ".." in path.parts for path in paths):
        raise OwnerFixtureIpcError("fixture runtime unavailable")
    return paths  # type: ignore[return-value]


def build_entangled_owner_fixture_runtime(
    *,
    namespace: str,
    socket_dir: str,
    secret_dir: str,
    state_dir: str,
    store: SqlEntityStore,
) -> EntangledOwnerFixtureRuntime | None:
    """Build from the live store; Production returns before path access."""

    paths = _configured_paths(
        socket_dir=socket_dir,
        secret_dir=secret_dir,
        state_dir=state_dir,
    )
    if namespace != FIXTURE_ENVIRONMENT:
        if paths is not None:
            raise OwnerFixtureIpcError("fixture runtime unavailable")
        return None
    if paths is None:
        return None
    if type(store) is not SqlEntityStore:
        raise OwnerFixtureIpcError("fixture runtime unavailable")
    loaded = load_owner_fixture_runtime(
        namespace=namespace,
        owner=FixtureOwner.ENTANGLED,
        socket_dir=paths[0],
        secret_dir=paths[1],
        state_dir=paths[2],
    )
    replay_ledger = FixtureReplayLedger(
        loaded.replay_ledger_path,
        owner=FixtureOwner.ENTANGLED,
    )
    return EntangledOwnerFixtureRuntime(
        server=OwnerFixtureIpcServer(
            namespace=namespace,
            owner=FixtureOwner.ENTANGLED,
            socket_path=loaded.socket_path,
            stores={
                FixtureCategory.RELATIONAL_ROWS: _DeferredRelationalStore(
                    store=store,
                    capability_secret=loaded.capability_secret,
                    derivation_secret=loaded.derivation_secret,
                    replay_ledger=replay_ledger,
                )
            },
        )
    )


__all__ = [
    "EntangledOwnerFixtureRuntime",
    "build_entangled_owner_fixture_runtime",
]
