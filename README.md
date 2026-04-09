# Entangled

实时实体同步中间件：服务端 Python / 客户端 Rust，增量推送与本地缓存。

详细设计与 API 在**本仓库**历史提交与代码内注释；与 NovAIC 集成的说明见**父仓库** `docs/architecture/entangled-store-and-app-ws.md`。

## 布局

- Python 服务端与 Rust 客户端见仓库内 `packages/`、`services/` 等目录（以实际目录为准）。

```bash
cd packages/client-rust && cargo build
```

## License

MIT
