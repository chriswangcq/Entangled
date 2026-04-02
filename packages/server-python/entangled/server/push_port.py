"""
SyncPushPort — delivery abstraction for entity change notifications (ADR-6 / C.1).

Hosts may replace the default in-process port via set_sync_push_port() (e.g. bus-backed
implementation in multi-worker setups). The public entry notify_entity_change() in
notifier.py delegates here.
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Protocol, runtime_checkable

__all__ = ["SyncPushPort", "get_sync_push_port", "set_sync_push_port"]


@runtime_checkable
class SyncPushPort(Protocol):
    def notify_entity_change(
        self,
        user_id: str,
        entity: str,
        action: str,
        *,
        entity_id: Optional[str] = None,
        params: Optional[Dict[str, str]] = None,
        data: Optional[Dict[str, Any]] = None,
        request_id: Optional[str] = None,
    ) -> None: ...


_override: Optional[SyncPushPort] = None


def set_sync_push_port(port: Optional[SyncPushPort]) -> None:
    """Install a custom port, or None to use the default in-process notifier."""
    global _override
    _override = port


def get_sync_push_port() -> SyncPushPort:
    if _override is not None:
        return _override
    from .notifier import _default_inproc_push_port

    return _default_inproc_push_port
