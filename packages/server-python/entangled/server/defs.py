"""
entangled/server/defs.py — Entity definitions.

An EntityDef declares what an entity is: its name, key params, CRUD functions,
custom actions, and sync strategy.

This is the ONLY place business entities are defined. The engine (store,
WS handler, push notifier) is fully generic — one write = one notification.
"""


import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


# Type aliases for handler signatures
ListFn = Callable          # (store, user_id, params, **kw) -> list[dict]
ListStreamFn = Callable    # (store, user_id, params, *, before_id, limit) -> list[dict]
GetFn = Callable           # (store, user_id, entity_id, params) -> dict | None
CreateFn = Callable        # (store, user_id, params, data) -> dict
UpdateFn = Callable        # (store, user_id, entity_id, data, params) -> dict
DeleteFn = Callable        # (store, user_id, entity_id, params) -> bool
UpsertFn = Callable        # (store, user_id, entity_id, data, params) -> dict
ExistsBeforeFn = Callable  # (store, user_id, entity_id, params) -> bool
ActionFn = Callable        # (store, user_id, params, payload) -> dict


@dataclass(kw_only=True)
class EntityDef:
    """Definition of an entity in the Entangled system.

    This is the single source of truth for an entity's schema, CRUD operations,
    custom actions, and sync strategy.

    Example:
        todos = EntityDef(
            name="todos",
            key_params=["project_id"],
            sync_type="list",
            list_fn=lambda store, uid, params: db.query(...),
            create_fn=lambda store, uid, params, data: db.insert(...),
        )
    """
    # ── Identity ─────────────────────────────────────────────────
    name: str

    # ── Key params (scoping) ─────────────────────────────────────
    key_params: List[str] = field(default_factory=list)

    # ── Sync strategy ────────────────────────────────────────────
    sync_type: str = "list"             # "list" (mutable CRUD) | "stream" (append-only)
    sync_limit: Optional[int] = None    # stream only; host should set (e.g. 50)
    op_log_size: int = 1000             # max entries per (entity, optional user, params)

    # ── Client entanglement (declared on server, exposed via get_schema()) ───
    # lazy: entangle only when a hook mounts (default).
    # eager: also entangle at app startup (before any hook), for global entities.
    subscription_mode: str = "lazy"  # "lazy" | "eager"

    # ── CRUD handlers ────────────────────────────────────────────
    list_fn: Optional[ListFn] = None
    get_fn: Optional[GetFn] = None
    create_fn: Optional[CreateFn] = None
    update_fn: Optional[UpdateFn] = None
    delete_fn: Optional[DeleteFn] = None
    upsert_fn: Optional[Callable] = None  # (store, user_id, entity_id, data, params) -> dict
    list_stream_fn: Optional[ListStreamFn] = None  # cursor-based backward pagination
    exists_before_fn: Optional[ExistsBeforeFn] = None  # (store, user_id, oldest_id, params) -> bool

    # Data ordering contract: declares what order fetch_data_fn / list_fn returns.
    # "desc" = newest first (typical for streams with ORDER BY timestamp DESC)
    # "asc"  = oldest first (typical for logs with ORDER BY timestamp ASC)
    # The sync engine uses this to normalize data to ASC before sending to clients.
    data_order: str = "desc"

    # ── Custom actions ───────────────────────────────────────────
    actions: Dict[str, ActionFn] = field(default_factory=dict)

    # ── Remote action hooks (action_name → callback URL) ──────
    # When an action has no local handler, Entangled will HTTP POST to the
    # registered hook URL.  Populated by Gateway during schema registration.
    action_hooks: Dict[str, str] = field(default_factory=dict)

    # ── Push events ──────────────────────────────────────────────
    push_events: Optional[List[str]] = None

    # ── Permissions ──────────────────────────────────────────────
    check_access: Optional[Callable] = None

    def __post_init__(self):
        if self.push_events is None:
            self.push_events = [f"entity_change:{self.name}"]

    def to_schema_dict(self) -> dict:
        """Serialize to schema format for pushing to clients."""
        return {
            "name": self.name,
            "keyParams": self.key_params,
            "idField": getattr(self, "id_field", "id"),
            "pushEvents": self.push_events or [],
            "syncType": self.sync_type,
            "syncLimit": self.sync_limit,
            "subscriptionMode": self.subscription_mode,
            "dataOrder": self.data_order,
            "capabilities": {
                "listStream": self.list_stream_fn is not None,
                "existsBefore": self.exists_before_fn is not None,
                "upsert": self.upsert_fn is not None,
                "actions": list(self.actions.keys()) if self.actions else [],
            },
        }
