import pytest

from entangled.tools.ws_smoke import (
    parse_key_values,
    quote_identifier,
    report_contains_secret,
    summarize_frame,
)


def test_parse_key_values():
    assert parse_key_values(["agent_id=a1", "mode=test"]) == {"agent_id": "a1", "mode": "test"}


def test_parse_key_values_rejects_missing_separator():
    with pytest.raises(ValueError):
        parse_key_values(["agent_id"])


def test_summarize_schema_push_frame_redacts_schema_payload_details():
    frame = {
        "type": "push",
        "event": "schema",
        "data": {
            "hash": "abc123",
            "syncContractVersion": 2,
            "entities": [
                {"name": "rest-smoke-events", "fields": [{"name": "secretish"}]},
                {"name": "ws-smoke-stream-events"},
            ],
        },
    }

    assert summarize_frame(frame) == {
        "type": "push",
        "event": "schema",
        "entity_names": ["rest-smoke-events", "ws-smoke-stream-events"],
        "syncContractVersion": 2,
        "schema_hash": "abc123",
    }


def test_summarize_stream_sync_frame_keeps_only_order_evidence():
    frame = {
        "type": "sync",
        "entity": "ws-smoke-stream-events",
        "mode": "head_n",
        "version": 4,
        "data": [
            {"id": "a", "entangled_rowid": 1, "payload_json": {"secret": "not copied"}},
            {"id": "b", "entangled_rowid": 2, "body": "not copied"},
        ],
        "hasMore": False,
    }

    assert summarize_frame(frame) == {
        "type": "sync",
        "entity": "ws-smoke-stream-events",
        "mode": "head_n",
        "version": 4,
        "data_count": 2,
        "ops_count": None,
        "hasMore": False,
        "stream_order": [
            {"id": "a", "entangled_rowid": 1},
            {"id": "b", "entangled_rowid": 2},
        ],
    }


def test_report_contains_secret_detects_nested_values():
    report = {"secret_policy": {"raw_token_recorded": False}, "frames": [{"value": "abc"}]}
    assert report_contains_secret(report, ["abc"])
    assert not report_contains_secret(report, ["missing"])


def test_quote_identifier_rejects_unsafe_table_name():
    assert quote_identifier("ws_smoke_stream_events") == '"ws_smoke_stream_events"'
    with pytest.raises(ValueError):
        quote_identifier("ws_smoke_stream_events;drop table x")
