"""Phase 1.5: canonical vectors for `_state_key` (sync registry partition).

Rust `hash_params` is a u64 over sorted keys + `Value::to_string()`; it is **not** comparable
to this string. Cross-stack consistency is: same wire JSON / string params → same logical
subscription; see docs/entangled-params-canonical.md.
"""

import json

from entangled.server.sync import SyncRegistry, _state_key


def test_state_key_entity_only():
    assert _state_key("messages", None) == "messages"
    assert _state_key("messages", {}) == "messages"


def test_state_key_sorted_independent_of_insertion_order():
    p1 = {"agent_id": "uuid-1", "z": "9", "a": "1"}
    p2 = {"a": "1", "z": "9", "agent_id": "uuid-1"}
    k1 = _state_key("messages", p1)
    k2 = _state_key("messages", p2)
    assert k1 == k2
    assert k1 == 'messages:' + json.dumps(sorted(p1.items()))


def test_state_key_different_values_differ():
    assert _state_key("messages", {"agent_id": "x"}) != _state_key("messages", {"agent_id": "y"})


def test_state_key_vector_messages_agent():
    """Documented smoke shape: messages scoped by agent_id."""
    params = {"agent_id": "550e8400-e29b-41d4-a716-446655440000"}
    expected = "messages:" + json.dumps(sorted(params.items()))
    assert _state_key("messages", params) == expected


def test_state_key_partitions_user_owned_entities_by_user():
    first = _state_key("messages", {"agent_id": "a-1"}, user_id="user-1")
    second = _state_key("messages", {"agent_id": "a-1"}, user_id="user-2")

    assert first != second
    assert first == 'messages:{"params":[["agent_id","a-1"]],"user_id":"user-1"}'


def test_state_key_keeps_global_entities_shared_when_user_scope_is_absent():
    assert _state_key("models", None) == "models"


def test_empty_internal_owner_is_not_the_global_partition():
    assert _state_key("messages", None, user_id="") != _state_key("messages", None)


def test_new_user_partition_starts_beyond_legacy_shared_version():
    registry = SyncRegistry()
    registry.hydrate_versions({"messages": 17})

    state = registry.get_state("messages", user_id="user-1")

    assert state.current_version == 18
    assert list(state.op_log) == []


def test_new_user_partition_without_legacy_history_starts_at_migration_fence():
    state = SyncRegistry().get_state("messages", user_id="user-1")

    assert state.current_version == 1
    assert list(state.op_log) == []
