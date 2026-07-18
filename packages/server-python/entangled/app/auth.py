"""Strict user access JWT and independent service-token authentication.

Gateway signs HS256 user access JWTs; Entangled verifies the exact token
contract. Service-to-service calls use a separate opaque token. The two
secrets are deliberately different trust domains.
"""

from __future__ import annotations

import hmac
import logging
import time
from dataclasses import dataclass
from typing import Optional

from fastapi import Header, HTTPException

logger = logging.getLogger(__name__)

ACCESS_TOKEN_TYPE = "novaic-access+jwt"
ACCESS_TOKEN_ISSUER_PREFIX = "novaic-gateway:"
ACCESS_TOKEN_AUDIENCE_PREFIX = "novaic-api:"
ACCESS_TOKEN_ALGORITHM = "HS256"
ACCESS_TOKEN_AUTH_VERSION = 3

_REQUIRED_ACCESS_CLAIMS = frozenset(
    {
        "typ",
        "iss",
        "aud",
        "sub",
        "exp",
        "iat",
        "ns",
        "jti",
        "auth_version",
        "sid",
        "auth_epoch",
    }
)
_ALLOWED_ACCESS_CLAIMS = _REQUIRED_ACCESS_CLAIMS | {"email"}

_access_jwt_secret: str = ""
_service_token: str = ""
_expected_namespace: str = ""


class AuthConfigurationError(RuntimeError):
    """The process cannot establish independent authentication domains."""


class AccessTokenClaimsError(ValueError):
    """A signed token does not satisfy the user access-token contract."""


@dataclass(frozen=True)
class SessionPrincipal:
    """Authenticated long-lived-session identity captured at handshake.

    The raw bearer token is deliberately not retained.  Long-lived connection
    revocation needs only this immutable, namespace-bound projection.
    """

    user_id: str
    session_id: str
    auth_epoch: int
    expires_at: int
    namespace: str


def principal_from_claims(payload: dict) -> SessionPrincipal:
    """Build a token-free principal from already validated v3 claims."""

    return SessionPrincipal(
        user_id=_required_text(payload, "sub"),
        session_id=_required_text(payload, "sid"),
        auth_epoch=_nonnegative_integer(payload, "auth_epoch"),
        expires_at=_numeric_date(payload, "exp"),
        namespace=_required_text(payload, "ns"),
    )


def access_token_issuer(namespace: str) -> str:
    return f"{ACCESS_TOKEN_ISSUER_PREFIX}{namespace}"


def access_token_audience(namespace: str) -> str:
    return f"{ACCESS_TOKEN_AUDIENCE_PREFIX}{namespace}"


def deployment_auth_is_strict(namespace: str) -> bool:
    """Every explicitly named deployment uses strict authentication."""

    return isinstance(namespace, str) and bool(namespace.strip())


def validate_auth_configuration(
    *,
    access_jwt_secret: str,
    service_token: str,
    expected_namespace: str,
    strict: bool,
) -> tuple[str, str, str]:
    """Validate and normalize authentication configuration without side effects."""

    if not isinstance(access_jwt_secret, str):
        raise AuthConfigurationError("access JWT verifier secret must be a string")
    if not isinstance(service_token, str):
        raise AuthConfigurationError("service token must be a string")
    if not isinstance(expected_namespace, str):
        raise AuthConfigurationError("namespace must be a string")
    access_secret = access_jwt_secret.strip()
    internal_secret = service_token.strip()
    namespace = expected_namespace.strip()
    if namespace and namespace != expected_namespace:
        raise AuthConfigurationError("namespace must be a canonical string")

    if strict:
        missing = [
            name
            for name, value in (
                ("access JWT verifier secret", access_secret),
                ("service token", internal_secret),
                ("namespace", namespace),
            )
            if not value
        ]
        if missing:
            raise AuthConfigurationError(
                "strict auth requires non-empty " + ", ".join(missing)
            )
    if access_secret and internal_secret and hmac.compare_digest(
        access_secret.encode("utf-8"),
        internal_secret.encode("utf-8"),
    ):
        raise AuthConfigurationError(
            "access JWT verifier secret and service token must be different"
        )
    return access_secret, internal_secret, namespace


def configure_auth(
    *,
    access_jwt_secret: str,
    service_token: str,
    expected_namespace: str = "",
    strict: bool = False,
) -> None:
    global _access_jwt_secret, _service_token, _expected_namespace
    access_secret, internal_secret, namespace = validate_auth_configuration(
        access_jwt_secret=access_jwt_secret,
        service_token=service_token,
        expected_namespace=expected_namespace,
        strict=strict,
    )
    _access_jwt_secret = access_secret
    _service_token = internal_secret
    _expected_namespace = namespace


def check_namespace_claim(payload: dict, expected_namespace: str) -> "str | None":
    """Return a rejection reason unless ``ns`` exactly matches deployment."""

    if (
        not isinstance(expected_namespace, str)
        or not expected_namespace
        or expected_namespace != expected_namespace.strip()
    ):
        return "access-token namespace is not configured"
    expected = expected_namespace
    token_ns = payload.get("ns")
    if (
        not isinstance(token_ns, str)
        or not token_ns
        or token_ns != token_ns.strip()
    ):
        return "token namespace is missing or invalid"
    if token_ns != expected:
        return f"token namespace {token_ns!r} does not match this environment {expected!r}"
    return None


def _required_text(payload: dict, claim: str) -> str:
    value = payload.get(claim)
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
    ):
        raise AccessTokenClaimsError(
            f"{claim} must be a non-empty canonical string"
        )
    return value


def _numeric_date(payload: dict, claim: str) -> int:
    value = payload.get(claim)
    if isinstance(value, bool) or not isinstance(value, int):
        raise AccessTokenClaimsError(f"{claim} must be an integer NumericDate")
    return value


def _nonnegative_integer(payload: dict, claim: str) -> int:
    value = payload.get(claim)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise AccessTokenClaimsError(f"{claim} must be a non-negative integer")
    return value


def _verification_time(now: int | float | None) -> int:
    if now is None:
        return int(time.time())
    if isinstance(now, bool) or not isinstance(now, (int, float)):
        raise AccessTokenClaimsError("verification time must be a numeric timestamp")
    return int(now)


def validate_access_token_claims(
    payload: dict,
    *,
    expected_namespace: str,
    now: int | float | None = None,
) -> dict:
    """Validate the signed claim payload against an explicit clock."""

    verified_at = _verification_time(now)
    if (
        not isinstance(expected_namespace, str)
        or not expected_namespace
        or expected_namespace != expected_namespace.strip()
    ):
        raise AccessTokenClaimsError(
            "expected namespace must be a non-empty canonical string"
        )
    namespace = expected_namespace
    missing = sorted(_REQUIRED_ACCESS_CLAIMS - payload.keys())
    if missing:
        raise AccessTokenClaimsError(
            "access token missing required claims: " + ", ".join(missing)
        )
    unexpected = sorted(payload.keys() - _ALLOWED_ACCESS_CLAIMS)
    if unexpected:
        raise AccessTokenClaimsError(
            "access token contains unsupported claims: " + ", ".join(unexpected)
        )
    if _required_text(payload, "typ") != ACCESS_TOKEN_TYPE:
        raise AccessTokenClaimsError("unexpected token type")
    if _required_text(payload, "iss") != access_token_issuer(namespace):
        raise AccessTokenClaimsError("unexpected token issuer")
    if _required_text(payload, "aud") != access_token_audience(namespace):
        raise AccessTokenClaimsError("unexpected token audience")
    if _nonnegative_integer(payload, "auth_version") != ACCESS_TOKEN_AUTH_VERSION:
        raise AccessTokenClaimsError("unexpected auth protocol version")
    _required_text(payload, "sub")
    _required_text(payload, "jti")
    _required_text(payload, "sid")
    _nonnegative_integer(payload, "auth_epoch")
    if "email" in payload:
        _required_text(payload, "email")

    reason = check_namespace_claim(payload, expected_namespace)
    if reason:
        raise AccessTokenClaimsError(reason)

    issued_at = _numeric_date(payload, "iat")
    expires_at = _numeric_date(payload, "exp")
    if issued_at > verified_at:
        raise AccessTokenClaimsError("token was issued in the future")
    if expires_at <= issued_at:
        raise AccessTokenClaimsError("token expiry must be after issuance")
    if expires_at <= verified_at:
        raise AccessTokenClaimsError("token has expired")

    return payload


def _decode_jwt(token: str, *, now: int | float | None = None) -> dict:
    from jose import jwt, JWTError

    if not _access_jwt_secret or not _expected_namespace:
        raise HTTPException(
            status_code=503,
            detail="Access JWT verification is not configured on Entangled Service",
        )
    try:
        payload = jwt.decode(
            token,
            _access_jwt_secret,
            algorithms=[ACCESS_TOKEN_ALGORITHM],
            audience=access_token_audience(_expected_namespace),
            issuer=access_token_issuer(_expected_namespace),
            options={
                **{f"require_{claim}": True for claim in _REQUIRED_ACCESS_CLAIMS},
                # The pure validator below uses the injected clock.
                "verify_exp": False,
                "verify_iat": False,
                "verify_nbf": False,
                "verify_at_hash": False,
            },
        )
        return validate_access_token_claims(
            payload,
            expected_namespace=_expected_namespace,
            now=now,
        )
    except (JWTError, AccessTokenClaimsError, TypeError, ValueError) as exc:
        logger.debug("[Auth] user access token rejected: %s", exc)
        raise HTTPException(status_code=401, detail="Invalid user access token") from exc


def verify_user_token(authorization: Optional[str] = Header(None)) -> str:
    """FastAPI dependency — extracts user_id from Bearer JWT."""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Bearer token")
    payload = _decode_jwt(authorization[7:])
    return _required_text(payload, "sub")


def verify_service_token(
    x_service_token: Optional[str] = Header(None),
    x_user_id: Optional[str] = Header(None),
) -> str:
    """FastAPI dependency for control-plane routes.

    A user JWT is intentionally not an alternative on this boundary. Service
    callers may still provide ``X-User-ID`` when the operation targets a tenant;
    schema/state-machine control routes normally leave it empty.
    """
    if not _service_token:
        raise HTTPException(status_code=503, detail="ENTANGLED_SERVICE_TOKEN not configured")
    if not x_service_token:
        raise HTTPException(status_code=401, detail="Missing service token")
    if not hmac.compare_digest(x_service_token, _service_token):
        raise HTTPException(status_code=401, detail="Invalid service token")
    return x_user_id or ""


def verify_service_or_user(
    authorization: Optional[str] = Header(None),
    x_service_token: Optional[str] = Header(None),
    x_user_id: Optional[str] = Header(None),
) -> str:
    """Accept either a service token (with X-User-ID) or a user JWT."""
    if x_service_token:
        return verify_service_token(x_service_token, x_user_id)

    if authorization and authorization.startswith("Bearer "):
        return verify_user_token(authorization)

    raise HTTPException(status_code=401, detail="Missing authentication")


def decode_jwt_from_raw(token: str) -> Optional[str]:
    """Decode a raw JWT string and return user_id, or None on failure."""
    try:
        payload = _decode_jwt(token)
        return _required_text(payload, "sub")
    except Exception:
        return None


def decode_principal_from_raw(token: str) -> Optional[SessionPrincipal]:
    """Decode a raw access JWT into a token-free v3 session principal."""

    try:
        return principal_from_claims(_decode_jwt(token))
    except Exception:
        return None
