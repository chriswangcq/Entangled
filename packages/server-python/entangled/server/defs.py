"""
entangled/server/defs.py — Entity definitions.

An EntityDef declares what an entity is: its name, key params, CRUD functions,
custom actions, relationships, and sync strategy.

This is the ONLY place business entities are defined. The engine (store, WS handler,
push notifier, cascade) is fully generic.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class EntityRelation:
    """A pointer from one entity to another (for cascade invalidation).

    When the source entity changes, the target entity's cache is invalidated.

    Attributes:
        target:    Target entity name, e.g. "todo-items"
        param_map: Map source params to target params, e.g. {"id": "todo_id"}
        on_actions: Only cascade on specific actions. None = all actions.
    """
    target: str
    param_map: Dict[str, str] = field(default_factory=dict)
    on_actions: Optional[List[str]] = None  # None = all


# Type aliases for handler signatures
ListFn = Callable  # (store, user_id, params) -> list[dict]
GetFn = Callable   # (store, user_id, entity_id, params) -> dict | None
CreateFn = Callable  # (store, user_id, params, data) -> dict
UpdateFn = Callable  # (store, user_id, entity_id, data, params) -> dict
DeleteFn = Callable  # (store, user_id, entity_id, params) -> bool
ActionFn = Callable  # (store, user_id, params, payload) -> dict


@dataclass
class EntityDef:
    """Definition of an entity in the Entangled system.

    This is the single source of truth for an entity's schema, CRUD operations,
    custom actions, relationships, and sync strategy.

    Example:
        todos = EntityDef(
            name="todos",
            key_params=["project_id"],
            sync_type="list",
            list_fn=lambda store, uid, params: db.query(...),
            create_fn=lambda store, uid, params, data: db.insert(...),
            relations=[
                EntityRelation(target="todo-items", param_map={"id": "todo_id"}),
            ],
        )
    """
    # ── Identity ─────────────────────────────────────────────────
    name: str

    # ── Key params (scoping) ─────────────────────────────────────
    key_params: List[str] = field(default_factory=list)

    # ── Sync strategy ────────────────────────────────────────────
    sync_type: str = "list"             # "list" (mutable CRUD) | "stream" (append-only)
    # Default head_n window when the client omits ``depth``; passed to ``resolve_sync(..., default_stream_depth=...)``.
    sync_limit: Optional[int] = None    # stream only; host should set (e.g. 50)
    op_log_size: int = 1000             # max op-log entries per (entity, params)

    # ── Client subscription (declared on Gateway, exposed via get_schema()) ───
    # lazy: subscribe only when a hook mounts (default).
    # eager: also subscribe at app startup (before any hook), for global entities.
    # subscription_cascade: after subscribing to this entity, client also subscribes
    #   to these names with the same params (same keyParams scope).
    subscription_mode: str = "lazy"  # "lazy" | "eager"
    subscription_cascade: List[str] = field(default_factory=list)

    # ── CRUD handlers ────────────────────────────────────────────
    list_fn: Optional[ListFn] = None
    get_fn: Optional[GetFn] = None
    create_fn: Optional[CreateFn] = None
    update_fn: Optional[UpdateFn] = None
    delete_fn: Optional[DeleteFn] = None

    # ── Custom actions ───────────────────────────────────────────
    actions: Dict[str, ActionFn] = field(default_factory=dict)

    # ── Relations (cascade invalidation pointers) ────────────────
    # Server-side only — never pushed to clients
    relations: List[EntityRelation] = field(default_factory=list)

    # ── Push events ──────────────────────────────────────────────
    push_events: Optional[List[str]] = None

    # ── Permissions ──────────────────────────────────────────────
    check_access: Optional[Callable] = None

    def __post_init__(self):
        if self.push_events is None:
            self.push_events = [f"entity_change:{self.name}"]

    def to_schema_dict(self) -> dict:
        """Serialize to schema format for pushing to clients.

        NOTE: relations are NOT included — cascade is server-side logic.
        Clients only need name → push_events mapping.
        """
        return {
            "name": self.name,
            "keyParams": self.key_params,
            "pushEvents": self.push_events or [],
            "syncType": self.sync_type,
            "syncLimit": self.sync_limit,
            "subscriptionMode": self.subscription_mode,
            # NOTE: subscriptionCascade intentionally omitted — cascade is now
            # handled server-side in ws_handler._handle_subscribe; clients never
            # need to expand cascade targets themselves.
        }
