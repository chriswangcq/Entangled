from __future__ import annotations

import pytest

from entangled.app.auth import AuthConfigurationError
from entangled.app.config import ServiceConfig
from entangled.app.factory import create_app
from entangled.app.main import build_parser, config_from_args


def test_cli_loads_access_secret_and_service_token_from_distinct_files(tmp_path) -> None:
    jwt_file = tmp_path / "access-jwt-secret"
    service_file = tmp_path / "service-token"
    authority_file = tmp_path / "authority-service-token"
    jwt_file.write_text("access-secret", encoding="utf-8")
    service_file.write_text("service-secret", encoding="utf-8")
    authority_file.write_text("authority-secret", encoding="utf-8")

    args = build_parser().parse_args(
        [
            "--jwt-secret-file", str(jwt_file),
            "--service-token-file", str(service_file),
            "--revocation-authority-service-token-file", str(authority_file),
            "--namespace", "prod",
        ]
    )
    config = config_from_args(args)

    assert config.access_jwt_secret == "access-secret"
    assert config.service_token == "service-secret"
    assert config.revocation_authority_service_token == "authority-secret"
    assert config.namespace == "prod"


def test_cli_does_not_allow_inline_and_file_for_same_secret(tmp_path) -> None:
    jwt_file = tmp_path / "access-jwt-secret"
    jwt_file.write_text("access-secret", encoding="utf-8")

    with pytest.raises(SystemExit):
        build_parser().parse_args(
            [
                "--jwt-secret", "inline-secret",
                "--jwt-secret-file", str(jwt_file),
            ]
        )


def test_named_deployment_rejects_inline_secrets() -> None:
    args = build_parser().parse_args(
        [
            "--jwt-secret", "access-secret",
            "--service-token", "service-secret",
            "--namespace", "prod",
        ]
    )
    with pytest.raises(ValueError, match="require .*secret-file"):
        config_from_args(args)


@pytest.mark.parametrize(
    "config",
    [
        ServiceConfig(namespace="staging"),
        ServiceConfig(
            namespace="prod",
            access_jwt_secret="same-secret",
            service_token="same-secret",
        ),
        ServiceConfig(
            strict_auth=True,
            access_jwt_secret="access-secret",
            service_token="service-secret",
        ),
    ],
)
def test_app_refuses_unsafe_strict_auth_before_startup(config: ServiceConfig) -> None:
    with pytest.raises(AuthConfigurationError):
        create_app(config)


def test_app_refuses_partial_revocation_configuration() -> None:
    with pytest.raises(AuthConfigurationError, match="configured together"):
        create_app(
            ServiceConfig(
                namespace="staging",
                access_jwt_secret="access-secret",
                service_token="entangled-secret",
                revocation_redis_url="redis://redis",
            )
        )


@pytest.mark.parametrize("authority_token", ["access-secret", "entangled-secret"])
def test_gateway_authority_token_has_independent_trust_domain(
    authority_token: str,
) -> None:
    with pytest.raises(AuthConfigurationError, match="independent"):
        create_app(
            ServiceConfig(
                namespace="staging",
                access_jwt_secret="access-secret",
                service_token="entangled-secret",
                revocation_redis_url="redis://redis",
                revocation_authority_url="http://gateway",
                revocation_authority_service_token=authority_token,
            )
        )
