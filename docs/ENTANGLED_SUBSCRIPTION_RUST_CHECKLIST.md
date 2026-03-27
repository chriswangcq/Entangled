# Entangled 订阅下沉 Rust — 实施检查清单

## Phase A — 单一事实来源（Rust ledger + schema）

- [x] `SubscriptionSchemaStore`：解析 Gateway `GET /api/entangled/schema` 的 `subscriptionCascade`（camelCase）
- [x] `SubscriptionLedger`：与 TS 一致的 ref-count / depth / params，按 `CacheKey` 索引
- [x] `EntangledState` 挂载 `subscription_schema` + `subscription_ledger`
- [x] Tauri `entangled_set_subscription_schema`：前端 `loadSubscriptionSchema` 时同步写入 Rust

## Phase B — 级联订阅命令（Tauri + AppBridge）

- [x] `entangled_subscribe_cascade` / `entangled_unsubscribe_cascade`：展开级联并仅在 ref 0→1 / 最后释放时发 WS
- [x] `entangled_resubscribe_all_active`：重连后恢复所有 ref>0 的订阅（合并原 `resubscribeAll` + eager wire-only）

## Phase C — invalidated / 重连闭环

- [x] `app_bridge` 处理 `Sync`：`invalidated` 且 ledger 仍活跃时先发 `subscribe(version: null)` 再 `emit entities_changed`
- [x] `run_connection` 在 `app_bridge_connected` 后 spawn 全量 `resubscribe_all_active`

## Phase D — React 瘦身

- [x] `subscribeWithCascade` / `unsubscribeWithCascade` → invoke Rust cascade 命令
- [x] `syncListener`：`invalidated` 仅 `invalidateQueries`（不再 `resubscribeEntity`）
- [x] `syncListener`：移除 `app_bridge_connected` 上的 `resubscribeAll`（由 Rust 负责）
- [x] `entangledBootstrap`：移除重复的 eager 级联 wire 重订阅

## 验证

- [ ] 本地 `npm run tauri dev`：列表/流订阅、delta mismatch 恢复、断网重连后数据继续推送（`cargo check` / `entangled-client` 单元测试已通过）
