"""SyncStateSnapshot + subscribe thread offload invariants."""

from entangled.server.sync import SyncState, snapshot_for_resolve, resolve_sync


def test_snapshot_get_ops_since_matches_live_state():
    st = SyncState.with_maxlen(100)
    st.append_op("insert", "a", {"x": 1})
    st.append_op("insert", "b", {"x": 2})
    snap = snapshot_for_resolve(st)
    assert snap.current_version == st.current_version
    assert snap.get_ops_since(0) == st.get_ops_since(0)
    st.append_op("insert", "c", None)
    assert snap.current_version == 2
    assert st.current_version == 3
    assert len(snap.get_ops_since(0) or []) == 2
    assert len(st.get_ops_since(0) or []) == 3


def test_resolve_sync_accepts_snapshot():
    st = SyncState.with_maxlen(50)
    snap = snapshot_for_resolve(st)

    def fetch():
        return [{"id": "1", "v": 1}]

    out = resolve_sync(
        snap,
        None,
        None,
        None,
        fetch,
        sync_type="list",
        id_field="id",
    )
    assert out["mode"] == "snapshot"
    assert out["version"] == 0
    assert len(out["data"]) == 1


def test_resolve_sync_snapshot_stream_head_n():
    st = SyncState.with_maxlen(50)
    snap = snapshot_for_resolve(st)

    def fetch(limit=None):
        return [
            {"id": "2", "t": 2},
            {"id": "1", "t": 1},
        ][: limit or 50]

    out = resolve_sync(
        snap,
        None,
        None,
        2,
        fetch,
        sync_type="stream",
        default_stream_depth=2,
        data_order="desc",
        id_field="id",
    )
    assert out["mode"] == "head_n"
    assert out["hasMore"] is False
    assert [r["id"] for r in out["data"]] == ["1", "2"]
