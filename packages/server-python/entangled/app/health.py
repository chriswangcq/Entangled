"""Health check endpoint."""

from fastapi import APIRouter

from .state import get_store

router = APIRouter(tags=["health"])


@router.get("/v1/health")
def health():
    store = get_store()
    return {
        "status": "ok",
        "entities": len(store.entities),
        "entity_names": store.entities,
    }
