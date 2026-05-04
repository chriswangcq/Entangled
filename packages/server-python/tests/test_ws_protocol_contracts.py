from pathlib import Path

import pytest

from entangled.server.protocol import (
    build_ack_frame,
    build_error_frame,
    build_page_sync_frame,
    build_push_frame,
    build_schema_push_frame,
    parse_action_frame,
    parse_entangle_frame,
)


def test_schema_push_frame_is_materialized_in_one_contract():
    frame = build_schema_push_frame(
        [{"name": "messages", "fields": [{"name": "id", "type": "string"}]}],
        sync_contract_version=2,
    )

    assert frame["type"] == "push"
    assert frame["event"] == "schema"
    assert frame["data"]["syncContractVersion"] == 2
    assert frame["data"]["entities"][0]["name"] == "messages"
    assert len(frame["data"]["hash"]) == 12


def test_push_frame_preserves_first_class_sync_frames():
    sync = {"type": "sync", "entity": "messages", "mode": "snapshot"}

    assert build_push_frame("ignored", sync) is sync
    assert build_push_frame("schema", {"ok": True}) == {
        "type": "push",
        "event": "schema",
        "data": {"ok": True},
    }


def test_client_entangle_frame_uses_only_canonical_snake_case():
    frame = parse_entangle_frame({
        "type": "entangle",
        "entity": "messages",
        "params": {"agent_id": "a1", "none": None},
        "version": "7",
        "before_id": "m2",
        "limit": "999",
        "request_id": "req-1",
    })

    assert frame.entity == "messages"
    assert frame.params == {"agent_id": "a1"}
    assert frame.version == 7
    assert frame.before_id == "m2"
    assert frame.limit == 500
    assert frame.request_id == "req-1"


@pytest.mark.parametrize("legacy_key", ["requestId", "beforeId"])
def test_client_entangle_frame_rejects_retired_aliases(legacy_key):
    with pytest.raises(ValueError, match="retired"):
        parse_entangle_frame({"type": "entangle", "entity": "messages", legacy_key: "legacy"})


def test_client_action_frame_uses_only_canonical_snake_case():
    frame = parse_action_frame({
        "type": "action",
        "request_id": "req-2",
        "entity": "messages",
        "op": "create",
        "params": {"agent_id": "a1"},
        "data": {"text": "hi"},
    })

    assert frame.request_id == "req-2"
    assert frame.entity == "messages"
    assert frame.op == "create"
    assert frame.params == {"agent_id": "a1"}
    assert frame.payload == {"text": "hi"}


def test_client_action_frame_rejects_retired_request_id_alias():
    with pytest.raises(ValueError, match="requestId is retired"):
        parse_action_frame({
            "type": "action",
            "requestId": "legacy",
            "entity": "messages",
            "op": "create",
        })


def test_client_action_frame_rejects_non_object_payloads():
    with pytest.raises(ValueError, match="data must be an object"):
        parse_action_frame({
            "type": "action",
            "request_id": "req-3",
            "entity": "messages",
            "op": "create",
            "data": ["not", "object"],
        })


def test_page_sync_and_error_frames_use_canonical_request_id():
    page = build_page_sync_frame(
        entity="messages",
        params={"agent_id": "a1"},
        entries=[{"id": "m1"}],
        has_more=True,
        request_id="req-page",
    )
    error = build_error_frame(entity="messages", error="boom", request_id="req-err")

    assert page["request_id"] == "req-page"
    assert "requestId" not in page
    assert page["hasMore"] is True
    assert error["request_id"] == "req-err"
    assert "requestId" not in error
    assert build_ack_frame("req-ack", {"success": True})["request_id"] == "req-ack"


def test_no_legacy_client_alias_fallbacks_in_ws_handlers():
    server_root = Path(__file__).resolve().parents[1]
    repo_root = Path(__file__).resolve().parents[3]
    checked = [
        server_root / "entangled" / "app" / "ws.py",
        server_root / "entangled" / "server" / "ws_handler.py",
        server_root / "entangled" / "server" / "protocol.py",
        repo_root / "packages" / "client-rust" / "src" / "transport.rs",
    ]
    text = "\n".join(path.read_text() for path in checked)

    assert "_normalize_incoming_msg" not in text
    assert 'or msg.get("requestId"' not in text
    assert 'or_else(|| val.get("requestId"))' not in text
    assert '"requestId": request_id' not in text
