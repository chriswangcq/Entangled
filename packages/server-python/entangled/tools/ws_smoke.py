"""WebSocket smoke client for Entangled staging validation."""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import time
from pathlib import Path
from typing import Any


DEFAULT_USER_ID = "ws-smoke-user"
IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def parse_key_values(values: list[str]) -> dict[str, str]:
    params: dict[str, str] = {}
    for item in values:
        if "=" not in item:
            raise ValueError(f"expected KEY=VALUE, got {item!r}")
        key, value = item.split("=", 1)
        key = key.strip()
        if not key:
            raise ValueError(f"empty key in {item!r}")
        params[key] = value
    return params


def load_secret_file(path: str | None) -> str | None:
    if not path:
        return None
    return Path(path).read_text(encoding="utf-8").strip()


def build_jwt(secret: str, user_id: str) -> str:
    from jose import jwt

    return jwt.encode({"sub": user_id, "iat": int(time.time())}, secret, algorithm="HS256")


def _entity_names_from_schema(frame: dict[str, Any]) -> list[str]:
    data = frame.get("data")
    if not isinstance(data, dict):
        return []
    entities = data.get("entities")
    if not isinstance(entities, list):
        return []
    return [str(entity.get("name")) for entity in entities if isinstance(entity, dict) and entity.get("name")]


def _count_rows(value: Any) -> int | None:
    if isinstance(value, list):
        return len(value)
    return None


def _stream_order(data: Any, id_field: str) -> list[dict[str, Any]]:
    if not isinstance(data, list):
        return []
    evidence = []
    for row in data:
        if not isinstance(row, dict):
            continue
        item: dict[str, Any] = {}
        if id_field in row:
            item["id"] = row.get(id_field)
        if "entangled_rowid" in row:
            item["entangled_rowid"] = row.get("entangled_rowid")
        if item:
            evidence.append(item)
    return evidence


def summarize_frame(frame: dict[str, Any], *, id_field: str = "id") -> dict[str, Any]:
    summary: dict[str, Any] = {"type": frame.get("type")}
    for key in ("event", "entity", "mode", "version", "baseVersion", "request_id", "success"):
        if key in frame:
            summary[key] = frame.get(key)
    if frame.get("event") == "schema":
        summary["entity_names"] = _entity_names_from_schema(frame)
        data = frame.get("data") if isinstance(frame.get("data"), dict) else {}
        summary["syncContractVersion"] = data.get("syncContractVersion")
        summary["schema_hash"] = data.get("hash")
    if frame.get("type") == "sync":
        data = frame.get("data")
        ops = frame.get("ops")
        summary["data_count"] = _count_rows(data)
        summary["ops_count"] = _count_rows(ops)
        summary["hasMore"] = frame.get("hasMore")
        order = _stream_order(data, id_field)
        if order:
            summary["stream_order"] = order
        if isinstance(ops, list):
            summary["op_ids"] = [op.get("id") for op in ops if isinstance(op, dict)]
    if frame.get("type") == "ack":
        data = frame.get("data")
        if isinstance(data, dict):
            summary["data_keys"] = sorted(data.keys())
    if frame.get("type") == "error":
        summary["error"] = frame.get("error")
    return summary


def report_contains_secret(report: Any, secrets: list[str]) -> bool:
    redacted = [secret for secret in secrets if secret]
    if not redacted:
        return False
    payload = json.dumps(report, sort_keys=True, ensure_ascii=False)
    return any(secret in payload for secret in redacted)


def quote_identifier(identifier: str) -> str:
    if not IDENT_RE.match(identifier):
        raise ValueError(f"unsafe SQL identifier: {identifier!r}")
    return f'"{identifier}"'


async def _receive_json(ws: Any, timeout: float) -> dict[str, Any]:
    raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError("expected JSON object frame")
    return data


async def _drain_until(
    ws: Any,
    *,
    timeout: float,
    predicate,
    summaries: list[dict[str, Any]],
    id_field: str,
) -> dict[str, Any] | None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        remaining = max(0.1, deadline - time.monotonic())
        try:
            frame = await _receive_json(ws, min(remaining, timeout))
        except asyncio.TimeoutError:
            return None
        summaries.append(summarize_frame(frame, id_field=id_field))
        if predicate(frame):
            return frame
    return None


async def _append_fixture(
    *,
    http_base: str,
    service_token: str,
    user_id: str,
    entity: str,
    params: dict[str, str],
    entity_id: str,
    timeout: float,
) -> dict[str, Any]:
    import httpx

    headers = {
        "X-Service-Token": service_token,
        "X-User-ID": user_id,
        "X-Params": json.dumps(params, sort_keys=True),
    }
    body = {
        "id": entity_id,
        **params,
        "body": "ws smoke append",
        "sequence": int(time.time()),
        "payload_json": {"kind": "ws-smoke-client"},
        "is_enabled": True,
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.post(f"{http_base.rstrip('/')}/v1/entities/{entity}/append", headers=headers, json=body)
    out: dict[str, Any] = {"status_code": response.status_code, "ok": response.status_code == 200}
    try:
        payload = response.json()
    except Exception:
        payload = {}
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, dict):
            out["id"] = data.get("id")
            out["entangled_rowid"] = data.get("entangled_rowid")
    return out


def _query_postgres_evidence(dsn_file: str | None, *, stream_table: str) -> dict[str, Any] | None:
    if not dsn_file:
        return None
    import psycopg

    dsn = Path(dsn_file).read_text(encoding="utf-8").strip()
    table = quote_identifier(stream_table)
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT state_key, version FROM entangled_sync_versions ORDER BY state_key")
            versions = [{"state_key": row[0], "version": row[1]} for row in cur.fetchall()]
            cur.execute(f"SELECT count(*), min(entangled_rowid), max(entangled_rowid) FROM {table}")
            count, min_rowid, max_rowid = cur.fetchone()
    return {
        "sync_versions": versions,
        "stream_table": stream_table,
        "stream_count": count,
        "stream_entangled_rowid_min": min_rowid,
        "stream_entangled_rowid_max": max_rowid,
    }


async def run_smoke(args: argparse.Namespace) -> dict[str, Any]:
    import websockets

    if "token=" in args.endpoint:
        raise ValueError("endpoint must not contain query-string tokens; use Authorization-header JWT auth")
    jwt_secret = load_secret_file(args.jwt_secret_file or args.token_file)
    service_token = load_secret_file(args.service_token_file or args.token_file)
    if not jwt_secret:
        raise ValueError("jwt secret file or token file is required")
    ws_jwt = build_jwt(jwt_secret, args.user_id)
    params = parse_key_values(args.stream_param)
    frame_summaries: list[dict[str, Any]] = []
    request_id_prefix = f"ws-smoke-{int(time.time())}"

    async with websockets.connect(
        args.endpoint,
        additional_headers={"Authorization": f"Bearer {ws_jwt}"},
        open_timeout=args.timeout,
        close_timeout=min(args.timeout, 2.0),
    ) as ws:
        schema = await _drain_until(
            ws,
            timeout=args.timeout,
            predicate=lambda frame: frame.get("event") == "schema",
            summaries=frame_summaries,
            id_field=args.id_field,
        )
        list_sync = None
        if not args.skip_list:
            await ws.send(json.dumps({
                "type": "entangle",
                "entity": args.list_entity,
                "request_id": f"{request_id_prefix}-list",
            }))
            list_sync = await _drain_until(
                ws,
                timeout=args.timeout,
                predicate=lambda frame: frame.get("type") == "sync" and frame.get("entity") == args.list_entity,
                summaries=frame_summaries,
                id_field=args.id_field,
            )
        await ws.send(json.dumps({
            "type": "entangle",
            "entity": args.stream_entity,
            "params": params,
            "depth": args.depth,
            "request_id": f"{request_id_prefix}-stream",
        }))
        stream_sync = await _drain_until(
            ws,
            timeout=args.timeout,
            predicate=lambda frame: frame.get("type") == "sync" and frame.get("entity") == args.stream_entity,
            summaries=frame_summaries,
            id_field=args.id_field,
        )
        delta_frame = None
        append_result = None
        if args.http_base and service_token and not args.skip_write:
            append_id = args.append_id or f"ws-smoke-{int(time.time() * 1000)}"
            append_result = await _append_fixture(
                http_base=args.http_base,
                service_token=service_token,
                user_id=args.user_id,
                entity=args.stream_entity,
                params=params,
                entity_id=append_id,
                timeout=args.timeout,
            )
            delta_frame = await _drain_until(
                ws,
                timeout=args.timeout,
                predicate=lambda frame: (
                    frame.get("type") == "sync"
                    and frame.get("entity") == args.stream_entity
                    and frame.get("mode") == "delta"
                ),
                summaries=frame_summaries,
                id_field=args.id_field,
            )

    reconnect_summaries: list[dict[str, Any]] = []
    reconnect_sync = None
    if not args.skip_reconnect:
        reconnect_version = None
        for frame in (delta_frame, stream_sync):
            if isinstance(frame, dict) and frame.get("version") is not None:
                reconnect_version = frame.get("version")
                break
        async with websockets.connect(
            args.endpoint,
            additional_headers={"Authorization": f"Bearer {ws_jwt}"},
            open_timeout=args.timeout,
            close_timeout=min(args.timeout, 2.0),
        ) as ws:
            await _drain_until(
                ws,
                timeout=args.timeout,
                predicate=lambda frame: frame.get("event") == "schema",
                summaries=reconnect_summaries,
                id_field=args.id_field,
            )
            msg = {
                "type": "entangle",
                "entity": args.stream_entity,
                "params": params,
                "depth": args.depth,
                "request_id": f"{request_id_prefix}-reconnect",
            }
            if reconnect_version is not None:
                msg["version"] = reconnect_version
            await ws.send(json.dumps(msg))
            reconnect_sync = await _drain_until(
                ws,
                timeout=args.timeout,
                predicate=lambda frame: frame.get("type") == "sync" and frame.get("entity") == args.stream_entity,
                summaries=reconnect_summaries,
                id_field=args.id_field,
            )

    pg_evidence = _query_postgres_evidence(args.postgres_dsn_file, stream_table=args.stream_table)
    report: dict[str, Any] = {
        "endpoint": args.endpoint,
        "http_base": args.http_base,
        "user_id": args.user_id,
        "entities": {
            "list": args.list_entity,
            "stream": args.stream_entity,
            "stream_params": params,
        },
        "frames": frame_summaries,
        "reconnect_frames": reconnect_summaries,
        "observations": {
            "schema_seen": schema is not None,
            "list_skipped": args.skip_list,
            "list_sync_mode": list_sync.get("mode") if isinstance(list_sync, dict) else None,
            "stream_sync_mode": stream_sync.get("mode") if isinstance(stream_sync, dict) else None,
            "delta_seen": delta_frame is not None,
            "reconnect_sync_mode": reconnect_sync.get("mode") if isinstance(reconnect_sync, dict) else None,
        },
        "append_result": append_result,
        "postgres": pg_evidence,
        "secret_policy": {
            "auth_uses_authorization_header": True,
            "query_string_token_used": False,
            "raw_token_recorded": False,
            "raw_jwt_recorded": False,
            "raw_dsn_recorded": False,
        },
    }
    secrets = [jwt_secret, service_token, ws_jwt, load_secret_file(args.postgres_dsn_file)]
    report["secret_policy"]["report_contains_secret"] = report_contains_secret(report, [s for s in secrets if s])
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run an Entangled WebSocket sync smoke.")
    parser.add_argument("--endpoint", required=True, help="WebSocket endpoint, e.g. ws://127.0.0.1:19910/v1/sync")
    parser.add_argument("--http-base", default=None, help="HTTP base URL used for optional REST write, e.g. http://127.0.0.1:19910")
    parser.add_argument("--token-file", default=None, help="Shared staging secret file used for JWT signing and service-token REST auth")
    parser.add_argument("--jwt-secret-file", default=None, help="JWT signing secret file for WebSocket auth")
    parser.add_argument("--service-token-file", default=None, help="Service token file for REST writes")
    parser.add_argument("--postgres-dsn-file", default=None, help="Optional Postgres DSN file for version/rowid evidence")
    parser.add_argument("--user-id", default=DEFAULT_USER_ID)
    parser.add_argument("--list-entity", default="rest-smoke-events")
    parser.add_argument("--stream-entity", default="ws-smoke-stream-events")
    parser.add_argument("--stream-table", default="ws_smoke_stream_events")
    parser.add_argument("--stream-param", action="append", default=["agent_id=ws-agent-fixture"], help="Stream param as KEY=VALUE; repeatable")
    parser.add_argument("--id-field", default="id")
    parser.add_argument("--depth", type=int, default=10)
    parser.add_argument("--timeout", type=float, default=5.0)
    parser.add_argument("--append-id", default=None)
    parser.add_argument("--skip-list", action="store_true", help="Skip list-entity entangle; useful when staging list rows contain non-JSON-safe fields")
    parser.add_argument("--skip-write", action="store_true")
    parser.add_argument("--skip-reconnect", action="store_true")
    parser.add_argument("--output", required=True, help="JSON report output path")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    report = asyncio.run(run_smoke(args))
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps({
        "ok": not report["secret_policy"]["report_contains_secret"],
        "output": str(output),
        "schema_seen": report["observations"]["schema_seen"],
        "delta_seen": report["observations"]["delta_seen"],
        "query_string_token_used": report["secret_policy"]["query_string_token_used"],
    }, sort_keys=True))
    return 1 if report["secret_policy"]["report_contains_secret"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
