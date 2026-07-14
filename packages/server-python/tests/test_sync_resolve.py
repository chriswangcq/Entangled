"""Tests for entangled.server.sync resolve_sync (stream head_n + op-log gap)."""

from __future__ import annotations

from entangled.server.sync import (
    DEFAULT_STREAM_HEAD_DEPTH,
    MAX_STREAM_HEAD_DEPTH,
    SyncRegistry,
    SyncState,
    resolve_sync,
)


def _fetch_factory(rows: list):
    """fetch_data_fn(limit=None) returns first `limit` items (simulates SQL LIMIT)."""
    def fetch_data(limit=None):
        if limit is None:
            return list(rows)
        return list(rows[: int(limit)])

    return fetch_data


def test_stream_first_subscribe_head_n():
    state = SyncState(current_version=3)
    rows = [{"id": str(i)} for i in range(60)]
    out = resolve_sync(
        state,
        client_version=None,
        client_head=None,
        depth=50,
        fetch_data_fn=_fetch_factory(rows),
        sync_type="stream",
        default_stream_depth=50,
        exists_before_fn=lambda _oldest_id: True,
    )
    assert out["mode"] == "head_n"
    assert out["hasMore"] is True
    assert len(out["data"]) == 50


def test_stream_op_log_gap_uses_head_n_not_full_snapshot():
    """When delta cannot be replayed, stream must not call fetch_data_fn() without limit."""
    state = SyncState(current_version=5)
    # v1..4 existed but op_log only has v5 (gap for client at since=2)
    from entangled.server.sync import SyncOp

    state.op_log.clear()
    state.op_log.append(
        SyncOp(version=5, op="insert", id="x", data={}, ts=0.0)
    )
    wide = [{"id": str(i)} for i in range(10_000)]
    limits_seen: list[int | None] = []

    def fetch_data(limit=None):
        limits_seen.append(limit)
        if limit is None:
            return wide
        return wide[:limit]

    out = resolve_sync(
        state,
        client_version=2,
        client_head=None,
        depth=None,
        fetch_data_fn=fetch_data,
        sync_type="stream",
        default_stream_depth=50,
        exists_before_fn=lambda _oldest_id: True,
    )
    assert out["mode"] == "head_n"
    assert len(out["data"]) <= 50
    assert None not in limits_seen, "stream snapshot path must never omit limit"
    assert limits_seen and limits_seen[0] == DEFAULT_STREAM_HEAD_DEPTH


def test_effective_depth_prefers_client_over_entity_default():
    state = SyncState(current_version=0)
    rows = [{"id": str(i)} for i in range(12)]
    out = resolve_sync(
        state,
        None,
        None,
        depth=10,
        fetch_data_fn=_fetch_factory(rows),
        sync_type="stream",
        default_stream_depth=99,
        exists_before_fn=lambda _oldest_id: True,
    )
    assert len(out["data"]) == 10
    assert out["hasMore"] is True


def test_depth_clamped_to_max():
    state = SyncState(current_version=0)
    rows = [{"id": str(i)} for i in range(MAX_STREAM_HEAD_DEPTH + 100)]
    out = resolve_sync(
        state,
        None,
        None,
        depth=MAX_STREAM_HEAD_DEPTH + 9999,
        fetch_data_fn=_fetch_factory(rows),
        sync_type="stream",
        default_stream_depth=50,
        exists_before_fn=lambda _oldest_id: True,
    )
    assert len(out["data"]) == MAX_STREAM_HEAD_DEPTH


def test_get_ops_since_empty_op_log_when_behind_is_gap():
    """After restart, op_log is empty but current_version may be restored from DB."""
    state = SyncState(current_version=100)
    state.op_log.clear()
    assert state.get_ops_since(95) is None
    assert state.get_ops_since(100) == []


def test_list_op_log_gap_still_full_snapshot():
    state = SyncState(current_version=5)
    from entangled.server.sync import SyncOp

    state.op_log.clear()
    state.op_log.append(
        SyncOp(version=5, op="insert", id="x", data={}, ts=0.0)
    )
    limits: list[int | None] = []

    def fetch_data(limit=None):
        limits.append(limit)
        return [{"id": "a"}, {"id": "b"}]

    out = resolve_sync(
        state,
        client_version=2,
        client_head=None,
        depth=50,
        fetch_data_fn=fetch_data,
        sync_type="list",
        default_stream_depth=None,
    )
    assert out["mode"] == "snapshot"
    assert limits == [None]


def test_client_version_ahead_of_new_partition_forces_snapshot():
    """A state-key migration resets the server partition; stale clients must re-clone."""
    state = SyncState(current_version=0)

    out = resolve_sync(
        state,
        client_version=17,
        client_head=None,
        depth=None,
        fetch_data_fn=lambda limit=None: [{"id": "owned-row"}],
        sync_type="list",
    )

    assert out == {
        "mode": "snapshot",
        "version": 0,
        "data": [{"id": "owned-row"}],
    }


def test_user_partition_migration_fence_forces_snapshot_from_legacy_version():
    registry = SyncRegistry()
    registry.hydrate_versions({"messages": 17})
    state = registry.get_state("messages", user_id="user-1")

    out = resolve_sync(
        state,
        client_version=17,
        client_head=None,
        depth=None,
        fetch_data_fn=lambda limit=None: [{"id": "user-1-row"}],
        sync_type="list",
    )

    assert out == {
        "mode": "snapshot",
        "version": 18,
        "data": [{"id": "user-1-row"}],
    }
