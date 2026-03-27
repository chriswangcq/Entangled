"""
entangled/server/store.py — EntityStore: the runtime engine.

Holds all EntityDefs and dispatches CRUD + action operations.
Fully generic — no business logic, just routes to the handlers
defined in each EntityDef.
"""

from __future__ import annotations

import inspect
import logging
from typing import Any, Dict, List, Optional

from .defs import EntityDef

logger = logging.getLogger(__name__)


class EntityStore:
    """Runtime entity store — dispatches operations to EntityDef handlers.

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
    ) -> Dict[str, Any]:
        defn = self.get_def(entity)
        self._check_access(defn, user_id, "create", params=params)
        if not defn.create_fn:
            raise NotImplementedError(f"{entity} does not support create")
        result = defn.create_fn(self, user_id, params or {}, data)
        self._notify_change(
            entity, "created", user_id,
            entity_id=result.get("id") if isinstance(result, dict) else None,
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
    ) -> Dict[str, Any]:
        defn = self.get_def(entity)
        self._check_access(defn, user_id, "update", entity_id=entity_id, params=params)
        if not defn.update_fn:
            raise NotImplementedError(f"{entity} does not support update")
        result = defn.update_fn(self, user_id, entity_id, data, params or {})
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
    ) -> bool:
        defn = self.get_def(entity)
        self._check_access(defn, user_id, "delete", entity_id=entity_id, params=params)
        if not defn.delete_fn:
            raise NotImplementedError(f"{entity} does not support delete")
        ok = defn.delete_fn(self, user_id, entity_id, params or {})
        if ok:
            self._notify_change(
                entity, "deleted", user_id,
                entity_id=entity_id,
                params=params,
                request_id=request_id,
            )
        return ok

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
        """Notify subscribed clients of an entity change with inline data."""
        from .notifier import notify_entity_change
        notify_entity_change(
            user_id, entity, action,
            entity_id=entity_id,
            params=params,
            data=data,
            request_id=request_id,
        )
