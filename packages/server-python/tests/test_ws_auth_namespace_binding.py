"""JWT 环境绑定 + 用户存在性(Xiaoniu 跨环境事故,2026-07-07)。

事故:staging 客户端被(错误下发的 EDGE 地址)指到 prod Entangled;prod 只验
HS256 签名 + sub 非空,既不识别 token 的签发环境、也不查用户是否存在于本环境
→ 为一个 prod 用户表中不存在的 staging 用户建了孤儿 agent。

纵深防御两层(独立于"每环境独立 jwt_secret"的止血):
* ns 环境绑定:token 携带异环境 ns 一律拒;缺 ns 容忍(旧 token 兼容)。
* 用户存在性:WS 建连时本环境 users 表查无此人 → 拒连;表不可用(bootstrap)放行。
"""
from entangled.app.auth import check_namespace_claim
from entangled.app.factory import _make_user_existence_checker


# ── check_namespace_claim(纯核)──────────────────────────────────────────────

def test_ns_mismatch_rejected():
    reason = check_namespace_claim({"sub": "u1", "ns": "staging"}, "prod")
    assert reason is not None and "staging" in reason and "prod" in reason


def test_ns_match_allowed():
    assert check_namespace_claim({"sub": "u1", "ns": "prod"}, "prod") is None


def test_legacy_token_without_ns_allowed():
    # 旧 token 兼容:跨环境已由密钥分叉止血,这里不强制全员重登。
    assert check_namespace_claim({"sub": "u1"}, "prod") is None


def test_binding_disabled_when_expected_empty():
    # 未配置 --namespace(dev/单环境)= 不启用绑定。
    assert check_namespace_claim({"sub": "u1", "ns": "staging"}, "") is None


# ── 用户存在性 checker ───────────────────────────────────────────────────────

class _FakeDb:
    def __init__(self, rows=None, error=None):
        self._rows = rows or {}
        self._error = error
        self.queries = []

    def fetchone(self, sql, params=()):
        self.queries.append((sql, params))
        if self._error is not None:
            raise self._error
        return self._rows.get(params[0])


def test_checker_true_when_user_row_exists():
    db = _FakeDb(rows={"u-known": {"?column?": 1}})
    assert _make_user_existence_checker(db)("u-known") is True


def test_checker_false_when_user_missing():
    # False → ws_sync_handler 拒连 4403(孤儿写入的直接拦截点)。
    db = _FakeDb(rows={})
    assert _make_user_existence_checker(db)("u-ghost") is False


def test_checker_none_when_table_unavailable():
    # bootstrap(users 表未建)fail-open:返回 None,放行 + 只警告一次。
    db = _FakeDb(error=RuntimeError('relation "users" does not exist'))
    checker = _make_user_existence_checker(db)
    assert checker("u1") is None
    assert checker("u2") is None  # 不因首错短路,每次仍探测(表建好即恢复 fail-closed)
