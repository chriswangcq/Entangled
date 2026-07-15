"""Entangled Service configuration.

All values are set via CLI args in main.py. No environment variable reads.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ServiceConfig:
    host: str = "0.0.0.0"
    port: int = 19900
    postgres_dsn: str = ""
    postgres_dsn_file: str = ""
    access_jwt_secret: str = ""
    service_token: str = ""
    # Explicit strict mode is useful before a namespace is assigned. Any
    # non-empty deployment namespace enables it automatically in app.factory.
    strict_auth: bool = False
    log_level: str = "INFO"
    # Access JWTs always carry this exact namespace. Staging/prod reject a
    # missing namespace and cannot start without one.
    namespace: str = ""
    # WS 建连用户存在性检查(默认关):Entangled 自库的 users 表今天不是权威用户
    # 存储(prod 该表仅 1 条遗留行,真用户在别处)—— 开着会拒掉所有真用户。启用
    # 前提:users 同步进 Entangled 库,或 checker 改查权威源。保留为 opt-in。
    enforce_user_exists: bool = False

    @classmethod
    def from_env(cls) -> ServiceConfig:
        return cls()
