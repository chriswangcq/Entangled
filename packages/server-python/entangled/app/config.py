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
    jwt_secret: str = ""
    service_token: str = ""
    log_level: str = "INFO"
    # 环境绑定(Xiaoniu 跨环境事故,2026-07-07):非空时,携带 ns 声明的用户 JWT
    # 必须与本环境一致才放行;缺 ns 的旧 token 容忍(跨环境已由密钥分叉止血)。
    namespace: str = ""

    @classmethod
    def from_env(cls) -> ServiceConfig:
        return cls()
