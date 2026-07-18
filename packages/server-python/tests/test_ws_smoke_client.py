import asyncio

from jose import jwt
import pytest

from entangled.app.auth import (
    ACCESS_TOKEN_TYPE,
    access_token_audience,
    access_token_issuer,
)
from entangled.tools.ws_smoke import (
    build_jwt,
    build_parser,
    parse_key_values,
    quote_identifier,
    report_contains_secret,
    run_smoke,
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


def test_build_jwt_uses_complete_gateway_contract():
    token = build_jwt(
        "jwt-secret",
        "user-1",
        "staging",
        now=1_800_000_000,
        ttl_seconds=60,
        jti="smoke-jti-1",
        sid="smoke-session-1",
    )
    claims = jwt.decode(
        token,
        "jwt-secret",
        algorithms=["HS256"],
        audience=access_token_audience("staging"),
        issuer=access_token_issuer("staging"),
        options={"verify_exp": False},
    )
    assert claims == {
        "typ": ACCESS_TOKEN_TYPE,
        "iss": access_token_issuer("staging"),
        "aud": access_token_audience("staging"),
        "sub": "user-1",
        "iat": 1_800_000_000,
        "exp": 1_800_000_060,
        "ns": "staging",
        "jti": "smoke-jti-1",
        "auth_version": 3,
        "sid": "smoke-session-1",
        "auth_epoch": 0,
    }


def test_smoke_parser_has_no_shared_token_file_fallback():
    args = build_parser().parse_args(
        [
            "--endpoint", "ws://127.0.0.1:19910/v1/sync",
            "--jwt-secret-file", "/tmp/jwt-secret",
            "--service-token-file", "/tmp/service-token",
            "--namespace", "staging",
            "--output", "/tmp/report.json",
        ]
    )
    assert not hasattr(args, "token_file")


def test_smoke_rejects_equal_jwt_and_service_secrets(tmp_path):
    jwt_path = tmp_path / "jwt-secret"
    service_path = tmp_path / "service-token"
    jwt_path.write_text("same-secret", encoding="utf-8")
    service_path.write_text("same-secret", encoding="utf-8")
    args = build_parser().parse_args(
        [
            "--endpoint", "ws://127.0.0.1:19910/v1/sync",
            "--jwt-secret-file", str(jwt_path),
            "--service-token-file", str(service_path),
            "--namespace", "staging",
            "--output", str(tmp_path / "report.json"),
        ]
    )

    with pytest.raises(ValueError, match="must be different"):
        asyncio.run(run_smoke(args))
