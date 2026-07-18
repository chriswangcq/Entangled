from __future__ import annotations

import hashlib
import hmac

from fastapi import HTTPException
from jose import jwt
import pytest

from entangled.app import auth


ACCESS_SECRET = "access-jwt-secret-for-tests"
SERVICE_TOKEN = "independent-service-token-for-tests"
NAMESPACE = "prod"
NOW = 1_800_000_000


def _claims(**overrides) -> dict:
    claims = {
        "typ": auth.ACCESS_TOKEN_TYPE,
        "iss": auth.access_token_issuer(NAMESPACE),
        "aud": auth.access_token_audience(NAMESPACE),
        "sub": "user-1",
        "iat": NOW,
        "exp": NOW + 300,
        "ns": NAMESPACE,
        "jti": "access-jti-1",
        "auth_version": 3,
        "sid": "session-1",
        "auth_epoch": 0,
    }
    claims.update(overrides)
    return claims


def _token(*, key: str | bytes = ACCESS_SECRET, **overrides) -> str:
    return jwt.encode(_claims(**overrides), key, algorithm="HS256")


@pytest.fixture(autouse=True)
def _configure_strict_auth() -> None:
    auth.configure_auth(
        access_jwt_secret=ACCESS_SECRET,
        service_token=SERVICE_TOKEN,
        expected_namespace=NAMESPACE,
        strict=True,
    )


def _assert_unauthorized(token: str, *, now: int = NOW) -> None:
    with pytest.raises(HTTPException) as exc:
        auth._decode_jwt(token, now=now)
    assert exc.value.status_code == 401


def test_valid_gateway_access_token_is_accepted() -> None:
    payload = auth._decode_jwt(_token(), now=NOW + 1)
    assert payload["sub"] == "user-1"
    assert payload["ns"] == NAMESPACE


def test_optional_canonical_email_is_accepted() -> None:
    payload = auth._decode_jwt(
        _token(email="person@example.com"),
        now=NOW + 1,
    )
    assert payload["email"] == "person@example.com"


@pytest.mark.parametrize(
    "claim",
    [
        "typ", "iss", "aud", "sub", "exp", "iat", "ns", "jti",
        "auth_version", "sid", "auth_epoch",
    ],
)
def test_every_access_claim_is_required(claim: str) -> None:
    claims = _claims()
    claims.pop(claim)
    _assert_unauthorized(jwt.encode(claims, ACCESS_SECRET, algorithm="HS256"))


@pytest.mark.parametrize(
    ("claim", "value"),
    [
        ("typ", "novaic-shell-capability-v1"),
        ("iss", "novaic-gateway:staging"),
        ("aud", "novaic-api:staging"),
    ],
)
def test_protocol_claims_must_match_gateway_contract(claim: str, value: str) -> None:
    _assert_unauthorized(_token(**{claim: value}))


def test_cross_namespace_token_is_rejected_even_with_same_key() -> None:
    _assert_unauthorized(_token(ns="staging"))


def test_complete_staging_token_cannot_handshake_with_prod_entangled() -> None:
    _assert_unauthorized(
        _token(
            ns="staging",
            iss=auth.access_token_issuer("staging"),
            aud=auth.access_token_audience("staging"),
        )
    )


def test_expired_token_is_rejected_at_exact_expiry() -> None:
    _assert_unauthorized(_token(exp=NOW + 10), now=NOW + 10)


def test_future_issued_at_is_rejected() -> None:
    _assert_unauthorized(_token(iat=NOW + 1, exp=NOW + 10), now=NOW)


@pytest.mark.parametrize("claim", ["iat", "exp"])
@pytest.mark.parametrize("value", [True, 1.5, "1800000000"])
def test_numeric_dates_must_be_integer(claim: str, value: object) -> None:
    _assert_unauthorized(_token(**{claim: value}))


@pytest.mark.parametrize("claim", ["role", "nbf"])
def test_unknown_and_nbf_claims_are_rejected(claim: str) -> None:
    _assert_unauthorized(_token(**{claim: NOW}))


@pytest.mark.parametrize(
    "claim",
    [
        "typ", "iss", "aud", "sub", "jti", "ns", "email", "sid",
    ],
)
def test_text_claims_must_be_canonical(claim: str) -> None:
    value = _claims().get(claim, "person@example.com")
    _assert_unauthorized(_token(**{claim: f" {value}"}))


def test_service_token_cannot_forge_a_user_access_token() -> None:
    _assert_unauthorized(_token(key=SERVICE_TOKEN))


def test_shell_capability_cannot_authenticate_as_a_user() -> None:
    capability_key = hmac.new(
        ACCESS_SECRET.encode(),
        b"novaic-shell-capability-signing-key-v1\0" + NAMESPACE.encode(),
        hashlib.sha256,
    ).digest()
    capability = jwt.encode(
        {
            "typ": "novaic-shell-capability-v1",
            "iss": "novaic-agent-runtime",
            "aud": "novaic-shell-capability",
            "sub": "user-1",
            "agent_id": "agent-1",
            "subagent_id": "subagent-1",
            "scope_id": "scope-1",
            "ns": NAMESPACE,
            "iat": NOW,
            "exp": NOW + 60,
            "jti": "capability-jti-1",
        },
        capability_key,
        algorithm="HS256",
    )
    _assert_unauthorized(capability)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"access_jwt_secret": "", "service_token": SERVICE_TOKEN},
        {"access_jwt_secret": ACCESS_SECRET, "service_token": ""},
    ],
)
def test_strict_auth_rejects_missing_secret(kwargs: dict) -> None:
    with pytest.raises(auth.AuthConfigurationError):
        auth.validate_auth_configuration(
            **kwargs,
            expected_namespace=NAMESPACE,
            strict=True,
        )


def test_strict_auth_rejects_missing_namespace() -> None:
    with pytest.raises(auth.AuthConfigurationError):
        auth.validate_auth_configuration(
            access_jwt_secret=ACCESS_SECRET,
            service_token=SERVICE_TOKEN,
            expected_namespace="",
            strict=True,
        )


def test_access_secret_and_service_token_must_never_be_equal() -> None:
    with pytest.raises(auth.AuthConfigurationError, match="must be different"):
        auth.validate_auth_configuration(
            access_jwt_secret=ACCESS_SECRET,
            service_token=ACCESS_SECRET,
            expected_namespace=NAMESPACE,
            strict=True,
        )


def test_configured_namespace_must_be_canonical() -> None:
    with pytest.raises(auth.AuthConfigurationError, match="canonical"):
        auth.validate_auth_configuration(
            access_jwt_secret=ACCESS_SECRET,
            service_token=SERVICE_TOKEN,
            expected_namespace=" prod",
            strict=True,
        )


@pytest.mark.parametrize("namespace", ["staging", "prod", "production", "dev", "st"])
def test_deployment_namespaces_enable_strict_auth(namespace: str) -> None:
    assert auth.deployment_auth_is_strict(namespace)


def test_empty_namespace_does_not_implicitly_enable_strict_auth() -> None:
    assert not auth.deployment_auth_is_strict("")
