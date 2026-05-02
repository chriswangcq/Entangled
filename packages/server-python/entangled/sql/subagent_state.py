"""PR-31b (2026-04-15) — subagents.status state machine (server-side).

Why this module exists
----------------------
PR-28 originally put the subagent status state machine behind a Business
helper. That split validation, status writes, and transition history
across multiple process calls.

Three consequences:

* **Multiple HTTP round-trips per transition.**
* **Not co-transactional.** A crash between the status update and
  history write could silently drop a history row.
* **State rules lived in two repos.** That shape historically drifted.

This module is the server-side twin of message_state: a single
``transition()`` that runs under ``db.transaction("global")`` and does
the SELECT, UPDATE, and log append atomically. Business's helper
becomes a thin client that delegates here, which gives us:

* **1 HTTP round-trip per transition.**
* **Atomicity.** Either the ``subagents.status`` UPDATE and the
  ``subagent_state_transitions`` INSERT both commit or neither does.
* **Single source of truth for the ALLOWED matrix.** Business still
  re-exports the names for backward compatibility but the canonical
  rules live here.

Rules (state diagram, identical to PR-28's Business-side matrix)
---------------------------------------------------------------
::

    sleeping ⇄ awake
    sleeping / awake  ──▶ running / summarizing
    running  / awake  ──▶ completed / failed / cancelled
    summarizing       ──▶ sleeping / awake / completed / failed
    failed            ──▶ sleeping / awake          (recovery)
    completed, cancelled: terminal (empty out-set)

Legacy enum values (``active``, ``inactive``, ``paused``) exist in
``SubagentStatus`` for backward compatibility but no current code path
transitions into them — their out-set is empty so any attempt fails
fail-loud with ``InvalidTransition``.

Ancillary fields (``extra``)
----------------------------
Business occasionally needs to write ancillary columns atomically with
the status change (``need_rest`` on ``mark_sleeping``, ``error`` on
``mark_failed``, etc.). We accept these through an ``extra`` dict but
intersect it with ``EXTRA_ALLOWLIST`` to keep the UPDATE safe from
accidental column drift (a typo'd key would just be silently dropped
— we opt for silent drop over 400 because a forgiving API here makes
rolling the allowlist out safer; it's server-side validated and
Business's own code has been audited).
"""
from __future__ import annotations

import logging
import time
from typing import Any, Dict, Mapping, Optional

logger = logging.getLogger(__name__)


# ── State machine ────────────────────────────────────────────────────────────
#
# Keys / values are the string form of ``common.enums.SubagentStatus`` — we
# keep strings here (not the enum) so Entangled avoids a dependency on the
# shared ``common`` package. Business is responsible for passing the right
# string; we reject anything not present.

ALLOWED_TRANSITIONS: Dict[str, set[str]] = {
    "sleeping":    {"awake", "running", "summarizing", "completed", "failed", "cancelled"},
    "awake":       {"sleeping", "running", "summarizing", "completed", "failed", "cancelled"},
    "summarizing": {"sleeping", "awake", "completed", "failed"},
    "running":     {"sleeping", "awake", "summarizing", "completed", "failed", "cancelled"},
    "failed":      {"sleeping", "awake"},
    "completed":   set(),
    "cancelled":   set(),
    # Legacy enum values — no current writer transitions into these; kept as
    # empty-set entries so an accidental ``to="active"`` raises instead of
    # hitting a ``KeyError`` halfway through.
    "active":      set(),
    "inactive":    set(),
    "paused":      set(),
}

VALID_STATES: frozenset[str] = frozenset(ALLOWED_TRANSITIONS.keys())


# Columns a transition caller is permitted to set alongside ``status``. This
# mirrors the union of ``extra`` dicts built by the Business-side helpers
# (``mark_sleeping``, ``mark_failed``, …). Any other key in the caller's
# ``extra`` dict is dropped before the UPDATE — that keeps a future
# column-rename on Business from blowing up an in-flight transition.
#
# Observability note
# ------------------
# Any extra key outside this narrow product set is dropped with a WARN inside
# :func:`transition`. This intentionally rejects retired continuity fields
# such as ``historical_summary``, ``handoff_notes``, ``last_scope_id`` and
# ``last_scope_archived_at`` instead of preserving old shape as a tolerated
# write path.
EXTRA_ALLOWLIST: frozenset[str] = frozenset({
    "need_rest",
    "progress",
    "error",
    "result",
})


# ── Exceptions ───────────────────────────────────────────────────────────────

class InvalidTransition(ValueError):
    """Raised when ``current -> to`` is not in ``ALLOWED_TRANSITIONS``.

    The HTTP router maps this to 409 so Business can tell "logic bug,
    retrying won't help" apart from "target not found" (404)."""


class SubagentNotFound(LookupError):
    """Raised when ``subagent_id`` (scoped by ``agent_id``) does not exist
    in the ``subagents`` table. Mapped to HTTP 404."""


# ── Core transition ──────────────────────────────────────────────────────────

def transition(
    db,
    subagent_id: str,
    agent_id: str,
    *,
    to: str,
    reason: str = "",
    actor: str = "",
    extra: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Atomically update ``subagents.status`` + append a transition row.

    Semantics match PR-28's Business-side ``transition``:

    * ``current == to`` is a no-op (not an error). If ``extra`` was
      supplied we still apply the ancillary columns — e.g. a recovery
      worker legitimately wants to overwrite ``error`` on a subagent
      that's already ``failed`` without bumping the status again.
    * Non-noop transitions write ``status`` + any allowlisted ``extra``
      columns + ``updated_at`` in one UPDATE, then append the history
      row, all inside the same ``transaction("global")`` block.

    Parameters
    ----------
    db
        ``entangled.sql.database.Database`` (or a test double with the
        same ``execute`` / ``transaction`` shape).
    subagent_id, agent_id
        Composite key into ``subagents``.
    to
        Target status string; must be in ``VALID_STATES``.
    reason, actor
        Short breadcrumbs. ``reason`` is snake_case ("timeout", "rest",
        "spawn", "scheduled_wake"), ``actor`` is who initiated the call
        ("business", "scheduler", "worker", "recovery").
    extra
        Optional mapping of ancillary columns to write alongside
        ``status``. Only keys in ``EXTRA_ALLOWLIST`` are honored; others
        are dropped with a warning.

    Returns
    -------
    dict
        ``{"subagent_id", "agent_id", "from", "to", "reason", "actor",
        "noop"}`` — suitable for returning as a FastAPI response.

    Raises
    ------
    SubagentNotFound
        Row missing (or ``agent_id`` mismatch — we treat mismatch as
        "not found for that owner" rather than 403 because the UNIQUE
        key here is ``(subagent_id, agent_id)``).
    InvalidTransition
        ``to`` is not reachable from the current status per
        ``ALLOWED_TRANSITIONS``.
    """
    if to not in VALID_STATES:
        raise InvalidTransition(f"{to!r} is not a valid subagent status")

    # Import inline so the circular dependency between this module and
    # ``state_transitions`` (both live under entangled.sql and are wired
    # into ensure_schema paths) doesn't fire at module load.
    from .state_transitions import append_subagent_transition

    now_ms = int(time.time() * 1000)

    # Narrow ``extra`` down to the allowlist before opening the transaction
    # so a bad caller can't widen the lock scope while we deliberate.
    clean_extra: Dict[str, Any] = {}
    dropped: list[str] = []
    if extra:
        for key, value in extra.items():
            if key == "status":
                # Express status only via ``to`` — refuse to let callers
                # sneak a second status write through ``extra``.
                raise InvalidTransition("extra must not contain 'status'; use `to`")
            if key in EXTRA_ALLOWLIST:
                clean_extra[key] = value
            else:
                dropped.append(key)

    # PR-53 (2026-04-25): surface silent drops. Before PR-53 an unlisted key
    # (e.g. ``historical_summary`` before PR-45 landed) would vanish with
    # zero log output — we'd only notice when users complained about
    # broken continuity. The WARN gives on-call a single grep to find
    # "Business meant to write column X but Entangled didn't accept it"
    # regressions within one write attempt. We log once per call (keys
    # joined) to avoid log amplification in the noop-self-loop case.
    if dropped:
        logger.warning(
            "subagent_state extra_dropped subagent=%s agent=%s to=%s dropped_keys=%s "
            "reason=%s actor=%s — add to EXTRA_ALLOWLIST or use generic entity_store.update",
            subagent_id, agent_id, to,
            ",".join(sorted(dropped)),
            reason or "-", actor or "-",
        )

    with db.transaction("global"):
        row = db.execute(
            "SELECT status FROM subagents WHERE subagent_id = ? AND agent_id = ?",
            (subagent_id, agent_id),
        ).fetchone()
        if row is None:
            raise SubagentNotFound(
                f"subagent {subagent_id!r} (agent {agent_id!r}) not found"
            )
        cur_state = (row["status"] if row["status"] is not None else "sleeping")

        # Idempotent self-loop: skip the history append and the status
        # column write, but still apply ancillary extras if present. This
        # matches PR-28's Business-side contract so existing callers
        # (recovery_worker, etc.) don't see behavior change.
        if cur_state == to:
            if clean_extra:
                _apply_extras(db, subagent_id, agent_id, clean_extra, now_ms)
            logger.info(
                "subagent_state %s(agent=%s) %s -> %s noop reason=%s actor=%s",
                subagent_id, agent_id, cur_state, to, reason or "-", actor or "-",
            )
            return {
                "subagent_id": subagent_id,
                "agent_id": agent_id,
                "from": cur_state,
                "to": to,
                "reason": reason,
                "actor": actor,
                "noop": True,
            }

        if to not in ALLOWED_TRANSITIONS.get(cur_state, set()):
            raise InvalidTransition(
                f"subagent {subagent_id}: {cur_state} -> {to} is not allowed "
                f"(actor={actor or '-'}, reason={reason or '-'})"
            )

        _apply_status_and_extras(db, subagent_id, agent_id, to, clean_extra, now_ms)
        append_subagent_transition(
            db,
            subagent_id=subagent_id,
            agent_id=agent_id,
            from_state=cur_state,
            to_state=to,
            reason=reason,
            actor=actor,
            created_at_ms=now_ms,
        )

    logger.info(
        "subagent_state %s(agent=%s) %s -> %s reason=%s actor=%s",
        subagent_id, agent_id, cur_state, to, reason or "-", actor or "-",
    )
    return {
        "subagent_id": subagent_id,
        "agent_id": agent_id,
        "from": cur_state,
        "to": to,
        "reason": reason,
        "actor": actor,
        "noop": False,
    }


# ── UPDATE helpers ───────────────────────────────────────────────────────────
#
# Broken out so the self-loop branch can write just ``extras`` without
# rebuilding a SET clause that also touches ``status``.

def _apply_status_and_extras(
    db,
    subagent_id: str,
    agent_id: str,
    new_status: str,
    extras: Mapping[str, Any],
    now_ms: int,
) -> None:
    """Write ``status`` + allowlisted extras + ``updated_at`` in one UPDATE."""
    sets = ["status = ?"]
    values: list[Any] = [new_status]
    for key in sorted(extras.keys()):
        sets.append(f"{key} = ?")
        values.append(extras[key])
    sets.append("updated_at = ?")
    values.append(_now_iso_from_ms(now_ms))
    values.extend([subagent_id, agent_id])
    sql = (
        "UPDATE subagents SET "
        + ", ".join(sets)
        + " WHERE subagent_id = ? AND agent_id = ?"
    )
    db.execute(sql, tuple(values))


def _apply_extras(
    db,
    subagent_id: str,
    agent_id: str,
    extras: Mapping[str, Any],
    now_ms: int,
) -> None:
    """Self-loop variant: write only ancillary columns (no status touch)."""
    if not extras:
        return
    sets = [f"{key} = ?" for key in sorted(extras.keys())]
    values: list[Any] = [extras[key] for key in sorted(extras.keys())]
    sets.append("updated_at = ?")
    values.append(_now_iso_from_ms(now_ms))
    values.extend([subagent_id, agent_id])
    sql = (
        "UPDATE subagents SET "
        + ", ".join(sets)
        + " WHERE subagent_id = ? AND agent_id = ?"
    )
    db.execute(sql, tuple(values))


def _now_iso_from_ms(ms: int) -> str:
    """``subagents.updated_at`` is declared TEXT with a ``datetime('now')``
    default; we preserve that format so the column stays visually
    consistent with legacy rows. SQLite's ``datetime(?, 'unixepoch')``
    would work too, but doing the conversion here keeps the caller in
    Python land where it's easier to unit test."""
    from datetime import datetime, timezone
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


__all__ = [
    "ALLOWED_TRANSITIONS",
    "VALID_STATES",
    "EXTRA_ALLOWLIST",
    "InvalidTransition",
    "SubagentNotFound",
    "transition",
]
