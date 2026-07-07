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
    # WS 建连用户存在性检查(默认关):Entangled 自库的 users 表今天不是权威用户
    # 存储(prod 该表仅 1 条遗留行,真用户在别处)—— 开着会拒掉所有真用户。启用
    # 前提:users 同步进 Entangled 库,或 checker 改查权威源。保留为 opt-in。
    enforce_user_exists: bool = False

    @classmethod
    def from_env(cls) -> ServiceConfig:
        return cls()
