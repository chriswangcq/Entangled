# Entangled

实时实体同步中间件：服务端 Python / 客户端 Rust，增量推送与本地缓存。

详细设计与 API 在**本仓库**历史提交与代码内注释；与 NovAIC 集成的说明见**父仓库** `docs/architecture/entangled-store-and-app-ws.md`。

## 布局

```
packages/server-python/entangled/
├── server/    # 协议层：sync engine, notifier, ws_handler
├── sql/       # SQL 存储层：FieldDef, EntityDef, EntityStore, Database, Locks
└── app/       # 独立服务壳：FastAPI factory, auth, WS, CRUD, schema, health
```

- `entangled.server` — 通用协议引擎，无 I/O 依赖
- `entangled.sql` — SQL 存储层（服务端当前使用 Postgres）
- `entangled.app` — 开箱即用的独立服务（`pip install entangled-server[app]`）

## 快速启动（独立服务）

```bash
# 从源码
cd packages/server-python
pip install -e ".[app]"
python -m entangled.app.main --port 19900 --postgres-dsn-file /opt/novaic/postgres/secrets/novaic_entangled_dsn

# 或直接 CLI
entangled-service --port 19900 --postgres-dsn-file /opt/novaic/postgres/secrets/novaic_entangled_dsn
```

运行配置通过 CLI 参数传入，`packages/server-python/entangled/app/config.py`
不读取任何环境变量。

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--host` | `0.0.0.0` | 绑定地址 |
| `--port` | `19900` | 端口 |
| `--postgres-dsn-file` | _(必填)_ | Postgres DSN 文件 |
| `--service-token` | _(空)_ | 服务间认证 token，同时作为 `jwt_secret` |
| `--log-level` | `INFO` | 日志级别 |

## 作为库使用

```python
from entangled.sql import SqlEntityDef, SqlEntityStore, Database, F

db = Database(Path("data/app.db"))
db.connect()

store = SqlEntityStore(db=db)
store.register(SqlEntityDef(
    name="todos", table="todos",
    fields=[F.text("id", primary=True), F.text("title"), F.bool_("done")],
))
store.ensure_schema(store.get_def("todos"))

# CRUD
todo = store.create("todos", "user1", {"title": "Buy milk", "done": False})
```

## 客户端

```bash
cd packages/client-rust && cargo build
```

## License

MIT
