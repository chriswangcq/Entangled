"""Sealed Entangled owner for one relational Agent fixture row."""

from __future__ import annotations

import hashlib
import hmac
import json
from typing import Any, Mapping

from common.account_deletion_fixture import (
    FIXTURE_ENVIRONMENT,
    FixtureAggregateResponse,
    FixtureCategory,
    FixtureContractError,
    FixtureOperation,
    FixtureOwner,
    FixtureReplayLedger,
    FixtureRequest,
    VerifiedFixtureRequest,
    fixture_account_user_id,
)
from entangled.sql.entity_def import SqlEntityDef
from entangled.sql.entity_store import SqlEntityStore


_AGENT_ID_DOMAIN = b"novaic.entangled.staging-fixture.agent.v1\0"
_RECEIPT_DOMAIN = b"novaic.entangled.staging-fixture.receipt.v1\0"
_AGENT_FIELDS = frozenset(
    {
        "id",
        "user_id",
        "name",
        "created_at",
        "setup_complete",
        "model_id",
        "updated_at",
    }
)
_AGENT_NAME = "ByClaw Staging Fixture"
_FIXED_TIMESTAMP = "2026-07-21T00:00:00.000Z"


class EntangledFixtureStoreError(RuntimeError):
    """The exact Entangled fixture operation failed without private detail."""


def _validated_secret(value: bytes) -> bytes:
    if type(value) is not bytes or len(value) < 32:
        raise EntangledFixtureStoreError(
            "Entangled fixture authority is unavailable"
        )
    return value


def _agent_id(secret: bytes, *, run_handle: str) -> str:
    digest = hmac.new(
        secret,
        _AGENT_ID_DOMAIN + bytes.fromhex(run_handle),
        hashlib.sha256,
    ).hexdigest()
    return f"byclaw-staging-fixture-agent-{digest}"


def _receipt_digest(
    verified: VerifiedFixtureRequest,
    *,
    count: int,
) -> str:
    projection = {
        "binding_digest": verified.binding_digest,
        "category": verified.category.value,
        "count": count,
        "operation": verified.operation.value,
        "owner": verified.owner.value,
    }
    serialized = json.dumps(
        projection,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")
    return "sha256:" + hashlib.sha256(_RECEIPT_DOMAIN + serialized).hexdigest()


def _require_agents_definition(store: SqlEntityStore) -> None:
    try:
        definition = store.get_def("agents")
    except (KeyError, RuntimeError):
        raise EntangledFixtureStoreError(
            "Entangled fixture schema is unavailable"
        ) from None
    if (
        type(definition) is not SqlEntityDef
        or definition.name != "agents"
        or definition.table != "agents"
        or definition.id_field != "id"
        or definition.user_scoped is not True
        or definition.parent is not None
        or list(definition.key_params) != []
        or frozenset(definition.field_map) != _AGENT_FIELDS
    ):
        raise EntangledFixtureStoreError("Entangled fixture schema is invalid")


class EntangledRelationalFixtureStore:
    """Own exactly one Agent row; never expose a generic entity adapter."""

    __slots__ = (
        "__capability_secret",
        "__derivation_secret",
        "__replay_ledger",
        "__store",
    )

    def __init__(
        self,
        *,
        namespace: str,
        store: SqlEntityStore,
        capability_secret: bytes,
        derivation_secret: bytes,
        replay_ledger: FixtureReplayLedger,
    ) -> None:
        if (
            namespace != FIXTURE_ENVIRONMENT
            or type(store) is not SqlEntityStore
            or type(replay_ledger) is not FixtureReplayLedger
        ):
            raise EntangledFixtureStoreError(
                "Entangled fixture authority is unavailable"
            )
        _require_agents_definition(store)
        object.__setattr__(
            self,
            "_EntangledRelationalFixtureStore__store",
            store,
        )
        object.__setattr__(
            self,
            "_EntangledRelationalFixtureStore__capability_secret",
            _validated_secret(capability_secret),
        )
        object.__setattr__(
            self,
            "_EntangledRelationalFixtureStore__derivation_secret",
            _validated_secret(derivation_secret),
        )
        object.__setattr__(
            self,
            "_EntangledRelationalFixtureStore__replay_ledger",
            replay_ledger,
        )

    def __repr__(self) -> str:
        return "EntangledRelationalFixtureStore(<redacted>)"

    def __setattr__(self, name: str, value: object) -> None:
        del name, value
        raise TypeError("Entangled fixture store is immutable")

    def __delattr__(self, name: str) -> None:
        del name
        raise TypeError("Entangled fixture store is immutable")

    def _scope(self, request: FixtureRequest) -> tuple[str, str]:
        return (
            fixture_account_user_id(
                derivation_secret=self.__derivation_secret,
                run_handle=request.run_handle,
            ),
            _agent_id(
                self.__derivation_secret,
                run_handle=request.run_handle,
            ),
        )

    def _count(self, *, user_id: str, agent_id: str) -> int:
        try:
            row = self.__store.get("agents", user_id, agent_id)
        except Exception:
            raise EntangledFixtureStoreError(
                "Entangled fixture observation is unavailable"
            ) from None
        if row is None:
            return 0
        if (
            row.get("id") != agent_id
            or row.get("user_id") != user_id
            or row.get("name") != _AGENT_NAME
            or row.get("setup_complete") is not True
        ):
            raise EntangledFixtureStoreError(
                "Entangled fixture observation is invalid"
            )
        return 1

    def _seed(self, *, user_id: str, agent_id: str) -> int:
        if self._count(user_id=user_id, agent_id=agent_id) == 0:
            try:
                self.__store.create(
                    "agents",
                    user_id,
                    {
                        "id": agent_id,
                        "name": _AGENT_NAME,
                        "created_at": _FIXED_TIMESTAMP,
                        "setup_complete": True,
                        "updated_at": _FIXED_TIMESTAMP,
                    },
                    notify=False,
                )
            except Exception:
                raise EntangledFixtureStoreError(
                    "Entangled fixture seed is unavailable"
                ) from None
        count = self._count(user_id=user_id, agent_id=agent_id)
        if count != 1:
            raise EntangledFixtureStoreError(
                "Entangled fixture seed is incomplete"
            )
        return count

    def _cleanup(self, *, user_id: str, agent_id: str) -> int:
        self._count(user_id=user_id, agent_id=agent_id)
        try:
            self.__store.delete(
                "agents",
                user_id,
                agent_id,
                notify=False,
            )
        except Exception:
            raise EntangledFixtureStoreError(
                "Entangled fixture cleanup is unavailable"
            ) from None
        count = self._count(user_id=user_id, agent_id=agent_id)
        if count != 0:
            raise EntangledFixtureStoreError(
                "Entangled fixture cleanup is incomplete"
            )
        return count

    def handle_request(
        self,
        payload: Mapping[str, Any],
    ) -> FixtureAggregateResponse:
        request = FixtureRequest.parse(payload)
        if (
            request.owner is not FixtureOwner.ENTANGLED
            or request.category is not FixtureCategory.RELATIONAL_ROWS
        ):
            raise FixtureContractError(
                "fixture request is not an Entangled relational operation"
            )
        verified = self.__replay_ledger.claim(
            payload,
            secret=self.__capability_secret,
        )
        user_id, agent_id = self._scope(request)
        if request.operation is FixtureOperation.SEED:
            count = self._seed(user_id=user_id, agent_id=agent_id)
        elif request.operation is FixtureOperation.OBSERVE:
            count = self._count(user_id=user_id, agent_id=agent_id)
        elif request.operation is FixtureOperation.CLEANUP:
            count = self._cleanup(user_id=user_id, agent_id=agent_id)
        else:  # pragma: no cover - FixtureRequest.parse owns this enum.
            raise FixtureContractError("fixture operation is invalid")
        return FixtureAggregateResponse.create(
            verified,
            count=count,
            receipt_digest=_receipt_digest(verified, count=count),
        )

    def execute(self, payload: Mapping[str, Any]) -> FixtureAggregateResponse:
        """Common owner-IPC entrypoint; retains the exact sealed contract."""

        return self.handle_request(payload)


__all__ = ["EntangledFixtureStoreError", "EntangledRelationalFixtureStore"]
