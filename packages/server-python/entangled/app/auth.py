"""JWT + Service Token authentication middleware.

Gateway signs HS256 JWTs; Entangled Service verifies them.
Service-to-service calls use a shared ENTANGLED_SERVICE_TOKEN header.
"""

from __future__ import annotations

import logging
from typing import Optional

from fastapi import Header, HTTPException

logger = logging.getLogger(__name__)

_jwt_secret: str = ""
_service_token: str = ""


def configure_auth(*, jwt_secret: str, service_token: str) -> None:
    global _jwt_secret, _service_token
    _jwt_secret = jwt_secret
    _service_token = service_token


def _decode_jwt(token: str) -> dict:
    from jose import jwt, JWTError
    if not _jwt_secret:
        raise HTTPException(status_code=503, detail="JWT_SECRET not configured on Entangled Service")
    try:
        return jwt.decode(token, _jwt_secret, algorithms=["HS256"])
    except JWTError as e:
        raise HTTPException(status_code=401, detail=f"Invalid token: {e}")


def verify_user_token(authorization: Optional[str] = Header(None)) -> str:
    """FastAPI dependency — extracts user_id from Bearer JWT."""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Bearer token")
    payload = _decode_jwt(authorization[7:])
    user_id = payload.get("sub")
    if not user_id:
        raise HTTPException(status_code=401, detail="Token missing subject")
    return user_id


def verify_service_or_user(
    authorization: Optional[str] = Header(None),
    x_service_token: Optional[str] = Header(None),
    x_user_id: Optional[str] = Header(None),
) -> str:
    """Accept either a service token (with X-User-ID) or a user JWT."""
    if x_service_token:
        if not _service_token:
            raise HTTPException(status_code=503, detail="ENTANGLED_SERVICE_TOKEN not configured")
        if x_service_token != _service_token:
            raise HTTPException(status_code=401, detail="Invalid service token")
        return x_user_id or ""

    if authorization and authorization.startswith("Bearer "):
        return verify_user_token(authorization)

    raise HTTPException(status_code=401, detail="Missing authentication")


def decode_jwt_from_raw(token: str) -> Optional[str]:
    """Decode a raw JWT string and return user_id, or None on failure."""
    try:
        payload = _decode_jwt(token)
        return payload.get("sub")
    except Exception:
        return None
