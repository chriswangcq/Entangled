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
_expected_namespace: str = ""


def configure_auth(*, jwt_secret: str, service_token: str, expected_namespace: str = "") -> None:
    global _jwt_secret, _service_token, _expected_namespace
    _jwt_secret = jwt_secret
    _service_token = service_token
    _expected_namespace = expected_namespace


def check_namespace_claim(payload: dict, expected_namespace: str) -> "str | None":
    """**纯核**:环境绑定判定(Xiaoniu 跨环境事故,2026-07-07)。

    返回 None = 放行;返回 str = 拒绝理由。规则:
    * expected 未配置 → 放行(未启用绑定);
    * token 无 ns 声明 → 放行(旧 token 兼容;跨环境已由每环境独立 jwt_secret 止血,
      本检查是纵深防御 —— 防的是将来密钥又被配成一样);
    * token 有 ns 且 != expected → 拒绝(别的环境签的 token,签名再有效也不认)。
    """
    if not expected_namespace:
        return None
    token_ns = payload.get("ns")
    if token_ns is None:
        return None
    if str(token_ns) != expected_namespace:
        return f"token namespace {token_ns!r} does not match this environment {expected_namespace!r}"
    return None


def _decode_jwt(token: str) -> dict:
    from jose import jwt, JWTError
    if not _jwt_secret:
        raise HTTPException(status_code=503, detail="JWT_SECRET not configured on Entangled Service")
    try:
        payload = jwt.decode(token, _jwt_secret, algorithms=["HS256"])
    except JWTError as e:
        raise HTTPException(status_code=401, detail=f"Invalid token: {e}")
    reason = check_namespace_claim(payload, _expected_namespace)
    if reason:
        logger.warning("[Auth] cross-namespace token rejected: %s", reason)
        raise HTTPException(status_code=401, detail=f"Invalid token: {reason}")
    return payload


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
