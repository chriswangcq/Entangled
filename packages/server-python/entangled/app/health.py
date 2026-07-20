"""Health and readiness endpoints."""

from fastapi import APIRouter, HTTPException, Query

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


@router.get("/v1/ready")
def ready(required: str = Query("", description="Comma-separated required entity names")):
    store = get_store()
    from .ws import connection_revocation_ready

    entity_names = set(store.entities)
    required_names = {name.strip() for name in required.split(",") if name.strip()}
    missing = sorted(required_names - entity_names)
    revocation_ready = connection_revocation_ready()
    if not entity_names or missing or not revocation_ready:
        raise HTTPException(
            status_code=503,
            detail={
                "status": "not_ready",
                "entities": len(entity_names),
                "entity_names": sorted(entity_names),
                "missing": missing,
                "revocation_ready": revocation_ready,
            },
        )
    return {
        "status": "ready",
        "entities": len(entity_names),
        "entity_names": sorted(entity_names),
        "missing": [],
        "revocation_ready": True,
    }
