"""PR-21 (2026-04-20) — chat_messages lifecycle state machine.

Why this module exists
----------------------
Pre-PR-21 the "where is this message in its processing?" question required
reading five columns (`read`, `processed`, `claimed_by`, `claimed_at`,
`status`) and reconstructing the intent from the combination. The
"hihi" incident (see docs/roadmap/tickets/PR-21-message-lifecycle-enum.md)
hit a state where those five columns disagreed with each other and there
was no single place to point at and say "fix the rule here". This module
is that single place.

Rules (state diagram)
---------------------
::

    pending ──▶ claimed ──▶ consumed
       │          │
       │          └──▶ orphaned ──▶ claimed   (recovery re-claim)
       │
       └──▶ deduped  (idempotency winner's duplicates)

    consumed, deduped: terminal (no outbound transitions)

Single write entrypoint
-----------------------
Business / runtime / subscriber code MUST NOT ``UPDATE chat_messages SET
lifecycle = ...`` directly. The supported surfaces are:

* In-process (tests, migrations, Entangled internals): ``transition()``
  below.
* Out-of-process (every other service): ``POST /v1/messages/{id}/transition``
  on the Entangled HTTP API, implemented by ``entangled/app/message_state.py``
  which just wraps ``transition()``.

``scripts/ci/lint_lifecycle.sh`` enforces the ban on raw UPDATEs outside
the allowlist (this module, the app-side router, tests, migrations).

Observability
-------------
Each transition logs one line
    ``message_state <id>: <from> -> <to> scope=<scope> reason=<reason>``
which is grep-able alongside every other scope_id-tagged line in the
system, so a single grep reconstructs the full lifecycle of a message.
Metrics hook is intentionally left as a TODO — Entangled has no
``metrics`` module yet (confirmed 2026-04-20); PR-26 adds one and at that
point this module is one line away from emitting
``message_transitions_total{from,to}``.

Migration
---------
Pre-PR-21 rows default to ``lifecycle='pending'`` via the column default.
The idempotent backfill query lives in ``backfill_lifecycle()`` below and
is invoked once from ``SqlEntityStore.ensure_schema`` when it notices the
``chat_messages.lifecycle`` column is freshly added. Re-running the
backfill is safe: it only touches rows where ``lifecycle='pending'`` AND
one of the legacy signals (``processed=1`` or ``claimed_by IS NOT NULL``)
is set — a fresh pending row never matches both branches.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


# ── State machine ─────────────────────────────────────────────────────────────
#
# Edit with extreme care. Adding a state almost certainly also means
# PR-22/25/26 need updates. Removing a state requires a data migration
# that touches every existing row in that state, so in practice this
# mapping is append-only.
ALLOWED_TRANSITIONS: Dict[str, set[str]] = {
    "pending":   {"claimed", "deduped"},
    "claimed":   {"consumed", "orphaned"},
    "consumed":  set(),   # terminal — message reached a scope and ran
    "orphaned":  {"claimed"},   # HealthWorker recovery can re-claim
    "deduped":   set(),   # terminal — idempotency duplicate, never dispatched
}

VALID_STATES: frozenset[str] = frozenset(ALLOWED_TRANSITIONS.keys())


class InvalidTransition(ValueError):
    """Raised when a caller requests a transition the state machine forbids.

    The FastAPI endpoint converts this to HTTP 409 so callers can tell
    "invalid state" (their bug) apart from "message not found" (race with
    GC / wrong id, 404).
    """


class MessageNotFound(ValueError):
    """Raised when the message_id does not exist in chat_messages."""


# ── Core transition ───────────────────────────────────────────────────────────

def transition(
    db,
    message_id: str,
    *,
    to: str,
    scope_id: Optional[str] = None,
    reason: str = "",
) -> Dict[str, Any]:
    """Apply a lifecycle transition under the global DB lock.

    Parameters
    ----------
    db
        Anything with ``execute(sql, params).fetchone()`` + ``transaction("global")``
        context manager semantics. In-process that's ``entangled.sql.database.Database``;
        tests pass a ``FakeDatabase`` wrapping ``sqlite3.connect(":memory:")``.
    message_id
        chat_messages.id primary key.
    to
        Target lifecycle state. Must be in ``VALID_STATES``.
    scope_id
        The scope that now owns this message. On ``claimed`` this is
        required semantically (a message without an owner scope is
        orphaned, not claimed); on other transitions it's COALESCE'd so
        you can pass ``None`` and keep whatever was already there.
    reason
        Free-text breadcrumb for the log line. Example values:
        ``"subscriber_dispatch"``, ``"health_orphan_scan"``,
        ``"idempotency_loser"``.

    Returns
    -------
    dict
        ``{"message_id", "from", "to", "scope_id", "reason"}`` — the
        transition record, suitable for returning as an HTTP response
        body.

    Raises
    ------
    MessageNotFound
        No row with that primary key.
    InvalidTransition
        The current lifecycle → ``to`` transition is not in
        ``ALLOWED_TRANSITIONS``.

    Transaction note
    ----------------
    Uses ``db.transaction("global")`` so the SELECT-for-state and UPDATE
    serialize against concurrent transitions on any message. This is the
    same lock scope the outbox claim query uses (see ``app/outbox.py``);
    chat_messages is low-traffic relative to the global lock so we
    prefer simplicity (no per-row lock) over throughput.
    """
    if to not in VALID_STATES:
        raise InvalidTransition(f"{to!r} is not a valid lifecycle state")

    now_ms = int(time.time() * 1000)
    with db.transaction("global"):
        row = db.execute(
            "SELECT lifecycle, claimed_by_scope FROM chat_messages WHERE id = ?",
            (message_id,),
        ).fetchone()
        if row is None:
            raise MessageNotFound(f"message not found: {message_id}")
        cur_state = row["lifecycle"] or "pending"
        if to not in ALLOWED_TRANSITIONS.get(cur_state, set()):
            raise InvalidTransition(
                f"{cur_state} -> {to} not allowed for message {message_id}"
            )

        # COALESCE(?, claimed_by_scope) lets callers omit scope_id on
        # transitions that don't change ownership (e.g. consumed, deduped
        # after it was already set on the earlier claim). On first claim
        # the caller MUST supply scope_id or claimed_by_scope stays NULL
        # and PR-26 orphan scan will flag the row — which is the right
        # behavior (a claim without a scope is orphaned on purpose).
        db.execute(
            """
            UPDATE chat_messages
               SET lifecycle = ?,
                   claimed_by_scope = COALESCE(?, claimed_by_scope),
                   lifecycle_updated_at = ?
             WHERE id = ?
            """,
            (to, scope_id, now_ms, message_id),
        )

    logger.info(
        "message_state %s: %s -> %s scope=%s reason=%s",
        message_id,
        cur_state,
        to,
        scope_id or "-",
        reason or "-",
    )
    return {
        "message_id": message_id,
        "from": cur_state,
        "to": to,
        "scope_id": scope_id,
        "reason": reason,
    }


# ── One-shot backfill ─────────────────────────────────────────────────────────

def backfill_lifecycle(db) -> int:
    """Set ``lifecycle`` / ``claimed_by_scope`` / ``lifecycle_updated_at`` for
    pre-PR-21 rows using legacy signals.

    Idempotent: the WHERE clause ensures only rows still showing the
    column default (``lifecycle='pending'``) AND carrying a legacy signal
    (``processed=1`` OR ``claimed_by IS NOT NULL``) are touched. A row
    written fresh after PR-21 deploy is legitimately ``pending`` with no
    legacy fields, so it's left alone.

    Returns
    -------
    int
        Rows updated (for logging).

    Called from
    -----------
    ``SqlEntityStore.ensure_schema`` — exactly once per deploy, right
    after ``ALTER TABLE chat_messages ADD COLUMN lifecycle`` would have
    run. Safe to call more than once.
    """
    now_ms = int(time.time() * 1000)
    with db.transaction("global"):
        cur = db.execute(
            """
            UPDATE chat_messages
               SET lifecycle = CASE
                       WHEN processed = 1 THEN 'consumed'
                       WHEN claimed_by IS NOT NULL THEN 'claimed'
                       ELSE lifecycle
                   END,
                   claimed_by_scope = COALESCE(claimed_by_scope, claimed_by),
                   lifecycle_updated_at = COALESCE(lifecycle_updated_at, ?)
             WHERE lifecycle = 'pending'
               AND (processed = 1 OR claimed_by IS NOT NULL)
            """,
            (now_ms,),
        )
        updated = cur.rowcount if hasattr(cur, "rowcount") else 0
    if updated:
        logger.info(
            "message_state.backfill_lifecycle migrated %s rows "
            "(processed=1 -> consumed, claimed_by set -> claimed)",
            updated,
        )
    return updated
