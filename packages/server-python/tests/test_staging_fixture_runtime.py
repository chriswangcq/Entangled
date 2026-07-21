from __future__ import annotations

import asyncio
from pathlib import Path
import tempfile

import pytest

from common.account_deletion_fixture_ipc import OwnerFixtureIpcError
from entangled.sql.entity_store import SqlEntityStore
from entangled.staging_fixture.runtime import build_entangled_owner_fixture_runtime


def _directories(root: Path) -> tuple[Path, Path, Path]:
    socket_dir = root / "run"
    secret_dir = root / "secrets"
    state_dir = root / "state"
    for directory in (socket_dir, secret_dir, state_dir):
        directory.mkdir(mode=0o700)
        directory.chmod(0o700)
    for name, value in (
        ("capability_secret", b"c" * 32),
        ("derivation_secret", b"d" * 32),
    ):
        path = secret_dir / name
        path.write_bytes(value)
        path.chmod(0o600)
    return socket_dir, secret_dir, state_dir


def test_runtime_starts_before_dynamic_agents_schema_and_owns_socket() -> None:
    with tempfile.TemporaryDirectory(dir="/tmp", prefix="ent-fixture-") as root:
        socket_dir, secret_dir, state_dir = _directories(Path(root))
        store = SqlEntityStore(db=object())
        with pytest.raises(KeyError):
            store.get_def("agents")
        runtime = build_entangled_owner_fixture_runtime(
            namespace="staging",
            socket_dir=str(socket_dir),
            secret_dir=str(secret_dir),
            state_dir=str(state_dir),
            store=store,
        )
        assert runtime is not None

        async def exercise() -> None:
            await runtime.start()
            assert (socket_dir / "entangled.sock").is_socket()
            await runtime.close()
            assert not (socket_dir / "entangled.sock").exists()

        asyncio.run(exercise())


def test_production_returns_before_fixture_path_access(tmp_path: Path) -> None:
    store = SqlEntityStore(db=object())
    assert build_entangled_owner_fixture_runtime(
        namespace="prod",
        socket_dir="",
        secret_dir="",
        state_dir="",
        store=store,
    ) is None
    missing = str(tmp_path / "missing")
    with pytest.raises(OwnerFixtureIpcError, match="fixture runtime unavailable"):
        build_entangled_owner_fixture_runtime(
            namespace="prod",
            socket_dir=missing,
            secret_dir=missing,
            state_dir=missing,
            store=store,
        )


def test_staging_partial_fixture_configuration_fails_closed(tmp_path: Path) -> None:
    with pytest.raises(OwnerFixtureIpcError, match="fixture runtime unavailable"):
        build_entangled_owner_fixture_runtime(
            namespace="staging",
            socket_dir=str(tmp_path),
            secret_dir="",
            state_dir="",
            store=SqlEntityStore(db=object()),
        )
