"""Dynamic schema registration — receives JSON specs from upstream at startup."""

from __future__ import annotations

import logging
import hashlib
import json
from typing import List

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from ..sql.entity_def import SqlEntityDef
from ..sql.validation import SchemaValidationError, validate_schema_batch
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
    try:
        defs = [SqlEntityDef.from_spec(spec) for spec in req.entities]
        existing_defs = {
            defn.name: defn
            for defn in getattr(store, "get_all_defs", lambda: [])()
        }
        validate_schema_batch(defs, existing_defs=existing_defs)
    except (KeyError, ValueError, SchemaValidationError) as e:
        logger.error("[SchemaRegistry] Invalid schema batch: %s", e)
        raise HTTPException(status_code=422, detail=str(e))

    registered = [defn.name for defn in defs]
    try:
        with store.db.transaction("global"):
            for defn in defs:
                store.ensure_schema_unlocked(defn)
        for defn in defs:
            store.register(defn)
            logger.info("[SchemaRegistry] Registered: %s → %s", defn.name, defn.table)
    except Exception as e:
        logger.error("[SchemaRegistry] Failed schema registration batch: %s", e)
        raise HTTPException(status_code=500, detail=str(e))

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
        "errors": [],
        "total": len(registered),
    }
