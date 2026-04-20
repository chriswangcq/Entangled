"""PR-31 (2026-04-15) — Append-only state transition log tables.

Why this module exists
----------------------
PR-21 / PR-28 / PR-29 each centralised a state machine behind a single
``transition()`` function and emitted a one-line log per transition. Log
lines are grep-able but not queryable — reconstructing "the full life of
subagent X" still meant scrolling through days of logs across half a
dozen processes.

This module owns the persistence layer for those transitions:

* ``message_state_transitions`` — one row per non-noop ``chat_messages``
  lifecycle transition (driven in-process by ``message_state.transition``).
* ``subagent_state_transitions`` — one row per non-noop ``subagents.status``
  transition (driven via HTTP from Business; see
  ``entangled/app/state_transitions.py``).

(Scope transitions live in Cortex, not Entangled — Cortex owns its own
SQLite log file next to its workspace store.)

Design choices
--------------

* **Raw tables, not registered entities.** These are ops logs, never
  user-facing — no Sync, no subscription, no User partitioning. Using
  ``SqlEntityDef`` would drag that machinery in for no gain.
* **Append-only.** No UPDATE / DELETE helpers. Each ``transition()`` call
  emits exactly zero rows (on self-loop noop) or exactly one row.
* **Co-transactional where possible.** ``append_message_transition`` is
  called from *inside* ``message_state.transition``'s ``transaction("global")``
  block — a row's state change and its history entry either both commit
  or neither does.
* **Idempotent schema.** ``CREATE TABLE IF NOT EXISTS`` so the migration
  re-runs safely on every ``ensure_schema`` pass (same pattern as
  ``_ensure_outbox_schema``).
* **Open schema for ``metadata_json``.** We intentionally don't enumerate
  the keys — callers are free to stuff whatever debug context is cheap
  ("worker_id", "trigger_type", "dedup_loser_ids", …).
"""
from __future__ import annotations

import json
import logging
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


# ── Schema ────────────────────────────────────────────────────────────────────

_CREATE_MESSAGE_TRANSITIONS = """
CREATE TABLE IF NOT EXISTS message_state_transitions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id TEXT NOT NULL,
    from_state TEXT NOT NULL,
    to_state TEXT NOT NULL,
    reason TEXT,
    actor TEXT,
    scope_id TEXT,
    metadata_json TEXT,
    created_at INTEGER NOT NULL
)
"""

_CREATE_SUBAGENT_TRANSITIONS = """
CREATE TABLE IF NOT EXISTS subagent_state_transitions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    subagent_id TEXT NOT NULL,
    agent_id TEXT,
    from_state TEXT NOT NULL,
    to_state TEXT NOT NULL,
    reason TEXT,
    actor TEXT,
    scope_id TEXT,
    metadata_json TEXT,
    created_at INTEGER NOT NULL
)
"""

# Index on (subject_id, id) so "give me the history of entity X newest-first"
# is a covering range scan. ``id`` (INTEGER PK AUTOINCREMENT) is monotonic
# per-row-insert so ordering by it matches created_at while remaining
# stable against wall-clock clock-skew on multi-writer ops.
_CREATE_MESSAGE_TRANSITIONS_IDX = (
    "CREATE INDEX IF NOT EXISTS idx_message_state_transitions_msg "
    "ON message_state_transitions (message_id, id)"
)
_CREATE_SUBAGENT_TRANSITIONS_IDX = (
    "CREATE INDEX IF NOT EXISTS idx_subagent_state_transitions_sub "
    "ON subagent_state_transitions (subagent_id, id)"
)


def ensure_state_transitions_schema(db) -> None:
    """Create the two PR-31 log tables and their indexes idempotently.

    Called from ``SqlEntityStore.ensure_all_schemas`` so the tables exist
    before any ``transition()`` has a chance to INSERT into them.
    """
    with db.transaction("global"):
        db.execute(_CREATE_MESSAGE_TRANSITIONS)
        db.execute(_CREATE_MESSAGE_TRANSITIONS_IDX)
        db.execute(_CREATE_SUBAGENT_TRANSITIONS)
        db.execute(_CREATE_SUBAGENT_TRANSITIONS_IDX)
    logger.info("[state_transitions] schema ensured (message + subagent)")


# ── Writers ───────────────────────────────────────────────────────────────────

def append_message_transition(
    db,
    *,
    message_id: str,
    from_state: str,
    to_state: str,
    reason: str = "",
    actor: str = "",
    scope_id: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
    created_at_ms: Optional[int] = None,
) -> None:
    """Append one row to ``message_state_transitions``.

    Caller is responsible for transaction scope — ``message_state.transition``
    wraps this call inside its own ``transaction("global")`` so the row
    shares atomicity with the ``UPDATE chat_messages`` that preceded it.
    """
    db.execute(
        """
        INSERT INTO message_state_transitions
            (message_id, from_state, to_state, reason, actor, scope_id,
             metadata_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            message_id,
            from_state,
            to_state,
            reason or "",
            actor or "",
            scope_id,
            json.dumps(metadata) if metadata else None,
            created_at_ms if created_at_ms is not None else int(time.time() * 1000),
        ),
    )


def append_subagent_transition(
    db,
    *,
    subagent_id: str,
    agent_id: Optional[str],
    from_state: str,
    to_state: str,
    reason: str = "",
    actor: str = "",
    scope_id: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
    created_at_ms: Optional[int] = None,
) -> None:
    """Append one row to ``subagent_state_transitions``.

    Transaction-agnostic (matches ``append_message_transition`` above):
    caller is responsible for wrapping the call in a transaction when
    co-transactional semantics are needed. PR-31b's
    ``subagent_state.transition`` calls this from inside its own
    ``transaction("global")``; the legacy HTTP shim in
    ``entangled/app/state_transitions.py:record_subagent_transition``
    wraps it in a local transaction since its caller (pre-PR-31b
    Business code) expects the endpoint to be atomic on its own.

    Previously this helper opened its own ``transaction("global")`` —
    that worked fine for the HTTP-shim caller, but deadlocked the
    PR-31b in-process caller (``subagent_state.transition`` already
    held the same global write lock). Making the helper
    transaction-agnostic is the same pattern the message_state
    pair uses and avoids nested-write-lock hangs.
    """
    db.execute(
        """
        INSERT INTO subagent_state_transitions
            (subagent_id, agent_id, from_state, to_state, reason,
             actor, scope_id, metadata_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            subagent_id,
            agent_id,
            from_state,
            to_state,
            reason or "",
            actor or "",
            scope_id,
            json.dumps(metadata) if metadata else None,
            created_at_ms if created_at_ms is not None else int(time.time() * 1000),
        ),
    )


# ── Readers ───────────────────────────────────────────────────────────────────

def list_message_transitions(db, message_id: str, *, limit: int = 50) -> List[Dict[str, Any]]:
    """Return transitions for ``message_id`` oldest-first (history order)."""
    rows = db.fetchall(
        """
        SELECT id, message_id, from_state, to_state, reason, actor,
               scope_id, metadata_json, created_at
          FROM message_state_transitions
         WHERE message_id = ?
         ORDER BY id ASC
         LIMIT ?
        """,
        (message_id, int(limit)),
    )
    return [_row_to_dict(r) for r in rows]


def list_subagent_transitions(
    db, subagent_id: str, *, limit: int = 50,
) -> List[Dict[str, Any]]:
    """Return transitions for ``subagent_id`` oldest-first (history order)."""
    rows = db.fetchall(
        """
        SELECT id, subagent_id, agent_id, from_state, to_state, reason,
               actor, scope_id, metadata_json, created_at
          FROM subagent_state_transitions
         WHERE subagent_id = ?
         ORDER BY id ASC
         LIMIT ?
        """,
        (subagent_id, int(limit)),
    )
    return [_row_to_dict(r) for r in rows]


def _row_to_dict(row) -> Dict[str, Any]:
    """SQLite Row → plain dict with ``metadata_json`` parsed if present."""
    d = {k: row[k] for k in row.keys()}
    raw = d.pop("metadata_json", None)
    d["metadata"] = json.loads(raw) if raw else None
    return d


__all__ = [
    "ensure_state_transitions_schema",
    "append_message_transition",
    "append_subagent_transition",
    "list_message_transitions",
    "list_subagent_transitions",
]
