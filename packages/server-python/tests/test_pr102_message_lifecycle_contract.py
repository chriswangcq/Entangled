"""PR-102 — Entangled message state machine follows the shared contract."""

from __future__ import annotations

import json
from pathlib import Path

from entangled.sql.message_state import (
    ALLOWED_TRANSITIONS,
    VALID_STATES,
    _PENDING_CONSUMED_REASON_ALLOWLIST,
)


def _contract() -> dict:
    repo_root = Path(__file__).resolve().parents[4]
    path = repo_root / "novaic-common/common/contracts/message_lifecycle.json"
    return json.loads(path.read_text(encoding="utf-8"))


def test_message_state_machine_matches_shared_contract():
    contract = _contract()

    assert VALID_STATES == set(contract["lifecycle_states"])
    assert {
        state: set(targets)
        for state, targets in ALLOWED_TRANSITIONS.items()
    } == {
        state: set(targets)
        for state, targets in contract["allowed_transitions"].items()
    }
    assert sorted(_PENDING_CONSUMED_REASON_ALLOWLIST) == contract[
        "pending_consumed_reason_allowlist"
    ]
