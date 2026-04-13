"""
entangled/server/store.py — EntityStore: the runtime engine.

Holds all EntityDefs and dispatches CRUD + action operations.
Fully generic — no business logic, just routes to the handlers
defined in each EntityDef.

Architecture:
  EntityStore is a concrete class that dispatches CRUD operations
  to EntityDef handler functions (list_fn, create_fn, etc.).

  Subclasses (like a SQL-backed EntityStore) can override
  list(), list_stream(), exists_before() for storage-specific behavior.
  The base class provides:
    - Entity registration and schema management
    - CRUD dispatch to EntityDef handler functions
    - Notification routing via _notify_change()
    - Access control via _check_access()
"""


import inspect
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from .defs import EntityDef

logger = logging.getLogger(__name__)


def _primary_key_value_from_row(result: dict, defn: EntityDef) -> Optional[str]:
    """Extract string PK for sync notifications (supports non-`id` id_field, int PKs)."""
    id_field = getattr(defn, "id_field", None) or "id"
    v = result.get(id_field)
    if v is None:
        return None
    return str(v)


# ── Protocol: storage abstraction ────────────────────────────────

class EntityStoreProtocol(ABC):
    """Abstract protocol for entity storage engines.

    Defines the minimal interface that any entity store must implement.
    Concrete implementations include:
      - EntityStore (this module): fn-pointer dispatch to EntityDef handlers
      - Custom SQL-backed EntityStore: override list/get/create/update/delete with SQL queries

    Subclasses MUST call super().__init__(defs) to register entity definitions.
    """

    @abstractmethod
    def list(
        self,
        entity: str,
        user_id: str,
        *,
        params: Optional[Dict[str, str]] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """List entities for a given user and params."""
        ...

    @abstractmethod
    def get(
        self,
        entity: str,
        user_id: str,
        entity_id: str,
        *,
        params: Optional[Dict[str, str]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Get a single entity by ID."""
        ...

    @abstractmethod
    def create(
        self,
        entity: str,
        user_id: str,
        data: Dict[str, Any],
        *,
        params: Optional[Dict[str, str]] = None,
        request_id: Optional[str] = None,
        notify: bool = True,
    ) -> Dict[str, Any]:
        """Create an entity."""
        ...

    @abstractmethod
    def update(
        self,
        entity: str,
        user_id: str,
        entity_id: str,
        data: Dict[str, Any],
        *,
        params: Optional[Dict[str, str]] = None,
        request_id: Optional[str] = None,
        notify: bool = True,
    ) -> Dict[str, Any]:
        """Update an entity."""
        ...

    @abstractmethod
    def delete(
        self,
        entity: str,
        user_id: str,
        entity_id: str,
        *,
        params: Optional[Dict[str, str]] = None,
        request_id: Optional[str] = None,
        notify: bool = True,
    ) -> bool:
        """Delete an entity."""
        ...

    def list_stream(
        self,
        entity: str,
        user_id: str,
        *,
        params: Optional[Dict[str, str]] = None,
        before_id: Optional[str] = None,
        after_id: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """Cursor-based backward pagination. Default: falls back to list()."""
        return self.list(entity, user_id, params=params, limit=limit)

    def exists_before(
        self,
        entity: str,
        user_id: str,
        oldest_id: str,
        *,
        params: Optional[Dict[str, str]] = None,
    ) -> bool:
        """Check if items exist before the cursor. Default: False."""
        return False

    def upsert(
        self,
        entity: str,
        user_id: str,
        entity_id: str,
        data: Dict[str, Any],
        *,
        params: Optional[Dict[str, str]] = None,
        request_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Insert-or-update. Default: delegates to update()."""
        return self.update(entity, user_id, entity_id, data, params=params, request_id=request_id)

    async def action(
        self,
        entity: str,
        user_id: str,
        action_name: str,
        params: Dict[str, str],
        payload: Dict[str, Any],
    ) -> Any:
        """Execute a custom action. Default: raises KeyError."""
        raise KeyError(f"No action handler for '{action_name}' on '{entity}'")

    @abstractmethod
    def get_def(self, entity: str) -> EntityDef:
        """Get the EntityDef for an entity."""
        ...

    @abstractmethod
    def get_all_defs(self) -> List[EntityDef]:
        """Get all registered EntityDefs."""
        ...

    @abstractmethod
    def get_schema(self) -> List[dict]:
        """Get entity schema for Entangled protocol."""
        ...


# ── Concrete EntityStore ─────────────────────────────────────────

class EntityStore(EntityStoreProtocol):
    """Runtime entity store — dispatches operations to EntityDef handlers.

    This is the default Entangled EntityStore. It dispatches CRUD operations
    to the handler functions (list_fn, create_fn, etc.) defined in each EntityDef.

    For SQL-backed storage, subclass this and override list()/get()/create()/etc.
    or register EntityDefs with appropriate handler functions.

    Usage:
        store = EntityStore([todos_def, users_def, ...])
        items = store.list("todos", user_id, params={"project_id": "p1"})
        store.create("todos", user_id, params={...}, data={...})
    """

    def __init__(self, defs: List[EntityDef]):
        self._defs: Dict[str, EntityDef] = {}
        for d in defs:
            self.register(d)

    def register(self, defn: EntityDef) -> None:
        """Register or replace an entity definition."""
        self._defs[defn.name] = defn
        logger.info("[EntityStore] Registered entity: %s", defn.name)

    def get_def(self, entity: str) -> EntityDef:
        if entity not in self._defs:
            raise KeyError(f"Unknown entity: {entity}")
        return self._defs[entity]

    def get_all_defs(self) -> List[EntityDef]:
        return list(self._defs.values())

    def get_schema(self) -> List[dict]:
        return [d.to_schema_dict() for d in self._defs.values()]

    # ── CRUD operations ──────────────────────────────────────────

    def list(
        self,
        entity: str,
        user_id: str,
        *,
        params: Optional[Dict[str, str]] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        defn = self.get_def(entity)
        self._check_access(defn, user_id, "list", params=params)
        if not defn.list_fn:
            raise NotImplementedError(f"{entity} does not support list")
        # Try to pass limit if the list_fn supports it
        if limit is not None:
            try:
                return defn.list_fn(self, user_id, params or {}, limit=limit)
            except TypeError:
                # list_fn doesn't accept limit — fallback to full list + slice
                data = defn.list_fn(self, user_id, params or {})
                return data[:limit] if len(data) > limit else data
        return defn.list_fn(self, user_id, params or {})

    def list_stream(
        self,
        entity: str,
        user_id: str,
        *,
        params: Optional[Dict[str, str]] = None,
        before_id: Optional[str] = None,
        after_id: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """Cursor-based backward pagination for stream entities.

        If the EntityDef provides a list_stream_fn, delegates to it.
        Otherwise falls back to list() with limit (no cursor support).
        """
        defn = self.get_def(entity)
        self._check_access(defn, user_id, "list_stream", params=params)
        if defn.list_stream_fn:
            return defn.list_stream_fn(
                self, user_id, params or {},
                before_id=before_id,
                after_id=after_id,
                limit=limit,
            )
        # Fallback: no cursor, just list with limit
        return self.list(entity, user_id, params=params, limit=limit)

    def exists_before(
        self,
        entity: str,
        user_id: str,
        oldest_id: str,
        *,
        params: Optional[Dict[str, str]] = None,
    ) -> bool:
        """Check if older items exist before the given cursor ID (for hasMore).

        If the EntityDef provides exists_before_fn, delegates to it.
        Otherwise returns False (unknown — caller falls back to len-based heuristic).
        """
        defn = self.get_def(entity)
        if defn.exists_before_fn:
            return defn.exists_before_fn(self, user_id, oldest_id, params or {})
        return False


    def get(
        self,
        entity: str,
        user_id: str,
        entity_id: str,
        *,
        params: Optional[Dict[str, str]] = None,
    ) -> Optional[Dict[str, Any]]:
        defn = self.get_def(entity)
        self._check_access(defn, user_id, "get", entity_id=entity_id, params=params)
        if not defn.get_fn:
            raise NotImplementedError(f"{entity} does not support get")
        return defn.get_fn(self, user_id, entity_id, params or {})

    def create(
        self,
        entity: str,
        user_id: str,
        data: Dict[str, Any],
        *,
        params: Optional[Dict[str, str]] = None,
        request_id: Optional[str] = None,
        notify: bool = True,
    ) -> Dict[str, Any]:
        defn = self.get_def(entity)
        self._check_access(defn, user_id, "create", params=params)
        if not defn.create_fn:
            raise NotImplementedError(f"{entity} does not support create")
        result = defn.create_fn(self, user_id, params or {}, data)
        if notify:
            self._notify_change(
                entity, "created", user_id,
                entity_id=_primary_key_value_from_row(result, defn) if isinstance(result, dict) else None,
                params=params,
                data=result,
                request_id=request_id,
            )
        return result

    def update(
        self,
        entity: str,
        user_id: str,
        entity_id: str,
        data: Dict[str, Any],
        *,
        params: Optional[Dict[str, str]] = None,
        request_id: Optional[str] = None,
        notify: bool = True,
    ) -> Dict[str, Any]:
        defn = self.get_def(entity)
        self._check_access(defn, user_id, "update", entity_id=entity_id, params=params)
        if not defn.update_fn:
            raise NotImplementedError(f"{entity} does not support update")
        result = defn.update_fn(self, user_id, entity_id, data, params or {})
        if notify:
            self._notify_change(
                entity, "updated", user_id,
                entity_id=entity_id,
                params=params,
                data=result,
                request_id=request_id,
            )
        return result

    def delete(
        self,
        entity: str,
        user_id: str,
        entity_id: str,
        *,
        params: Optional[Dict[str, str]] = None,
        request_id: Optional[str] = None,
        notify: bool = True,
    ) -> bool:
        defn = self.get_def(entity)
        self._check_access(defn, user_id, "delete", entity_id=entity_id, params=params)
        if not defn.delete_fn:
            raise NotImplementedError(f"{entity} does not support delete")
        ok = defn.delete_fn(self, user_id, entity_id, params or {})
        if ok and notify:
            self._notify_change(
                entity, "deleted", user_id,
                entity_id=entity_id,
                params=params,
                request_id=request_id,
            )
        return ok

    def upsert(
        self,
        entity: str,
        user_id: str,
        entity_id: str,
        data: Dict[str, Any],
        *,
        params: Optional[Dict[str, str]] = None,
        request_id: Optional[str] = None,
        notify: bool = True,
    ) -> Dict[str, Any]:
        """Insert-or-update. Falls back to update() if no upsert_fn defined."""
        defn = self.get_def(entity)
        self._check_access(defn, user_id, "upsert", entity_id=entity_id, params=params)
        if defn.upsert_fn:
            result = defn.upsert_fn(self, user_id, entity_id, data, params or {})
        elif defn.update_fn:
            result = defn.update_fn(self, user_id, entity_id, data, params or {})
        else:
            raise NotImplementedError(f"{entity} does not support upsert or update")
        if notify:
            self._notify_change(
                entity, "updated", user_id,
                entity_id=entity_id,
                params=params,
                data=result,
                request_id=request_id,
            )
        return result

    async def action(
        self,
        entity: str,
        user_id: str,
        action_name: str,
        params: Dict[str, str],
        payload: Dict[str, Any],
    ) -> Any:
        defn = self.get_def(entity)
        self._check_access(defn, user_id, f"action:{action_name}", params=params)
        if action_name not in defn.actions:
            raise KeyError(f"Unknown action '{action_name}' on entity '{entity}'")
        handler = defn.actions[action_name]
        result = handler(self, user_id, params, payload)
        if inspect.isawaitable(result):
            result = await result
        return result

    # ── Internal ─────────────────────────────────────────────────

    def _check_access(
        self,
        defn: EntityDef,
        user_id: str,
        op: str,
        entity_id: Optional[str] = None,
        params: Optional[Dict[str, str]] = None,
    ) -> None:
        if defn.check_access:
            if not defn.check_access(user_id, op, entity_id, params or {}):
                raise PermissionError(f"Access denied: {defn.name}.{op}")

    def _notify_change(
        self,
        entity: str,
        action: str,
        user_id: str,
        entity_id: Optional[str] = None,
        params: Optional[Dict[str, str]] = None,
        data: Optional[Dict[str, Any]] = None,
        request_id: Optional[str] = None,
    ) -> None:
        """Notify entangled clients of an entity change with inline data.

        WARNING: If the entity has key_params but params is empty, the delta push
        will be sent to state_key="entity" instead of "entity:[['key','val']]",
        which means NO entangled peer will receive it (they entangle with params).
        This is almost always a bug in the caller.
        """
        # Defense: warn if entity has key_params but notification has empty params
        try:
            defn = self.get_def(entity)
            if defn.key_params and not params:
                logger.warning(
                    "[Entangled] ⚠️  _notify_change('%s', '%s') called with empty params "
                    "but entity has key_params=%s — delta push will NOT reach any entangled peer! "
                    "entity_id=%s. This is almost certainly a bug in the caller.",
                    entity, action, defn.key_params, entity_id,
                )
        except KeyError:
            pass

        from .push_port import get_sync_push_port

        get_sync_push_port().notify_entity_change(
            user_id, entity, action,
            entity_id=entity_id,
            params=params,
            data=data,
            request_id=request_id,
        )
