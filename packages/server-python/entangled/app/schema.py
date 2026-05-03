"""Dynamic schema registration — receives JSON specs from upstream at startup."""

from __future__ import annotations

import logging
import hashlib
import json
from typing import List

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from ..sql.entity_def import SqlEntityDef
from ..server.notifier import notify_all
from ..server.ws_handler import SYNC_CONTRACT_VERSION
from .auth import verify_service_or_user
from .state import get_store

logger = logging.getLogger(__name__)

router = APIRouter(tags=["schema"])


class RegisterRequest(BaseModel):
    entities: List[dict]


@router.post("/v1/schema/register")
def register_schema(req: RegisterRequest, _user: str = Depends(verify_service_or_user)):
    store = get_store()
    registered = []
    errors = []

    for spec in req.entities:
        name = spec.get("name", "<unknown>")
        try:
            defn = SqlEntityDef.from_spec(spec)
            store.register(defn)
            store.ensure_schema(defn)
            registered.append(name)
            logger.info("[SchemaRegistry] Registered: %s → %s", name, defn.table)
        except Exception as e:
            logger.error("[SchemaRegistry] Failed to register %s: %s", name, e)
            errors.append({"name": name, "error": str(e)})

    if registered:
        schema = store.get_schema()
        schema_hash = hashlib.md5(
            json.dumps(schema, sort_keys=True).encode()
        ).hexdigest()[:12]
        notify_all("schema", {
            "entities": schema,
            "hash": schema_hash,
            "syncContractVersion": SYNC_CONTRACT_VERSION,
        })
        logger.info(
            "[SchemaRegistry] Broadcast schema update: entities=%d hash=%s",
            len(schema),
            schema_hash,
        )

    return {
        "registered": registered,
        "errors": errors,
        "total": len(registered),
    }
