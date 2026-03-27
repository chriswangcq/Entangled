# Entangled Method — 宪法（Constitution）

本文档定义 Entangled 引擎与宿主应用之间的**不可违背契约**。实现细节可以演进，**语义边界**以本文为准。

---

## 第一条：写操作的唯一法 — Entangled Method

**一切对服务端实体状态的变更，必须通过 Entangled Method 发出。**

- **Method** 是宿主意图与 Gateway 实体引擎之间的**唯一写通道**：经 AppBridge（或等价 IPC）送达 Gateway，由 `EntityStore` / `EntityDef` 路由执行，再通过 sync / op-log 回到客户端缓存。
- **TypeScript 权威 API**：`entangledMethod(entity, method, args, params?)`（见 `@entangled/react` / `client.ts`）。标准方法名为 `create` · `update` · `delete` · `upsert`；其余字符串为自定义 Method，对应 `EntityDef.actions`。
- **不允许**在实体数据面上并行存在「另一套写路径」（例如绕过 Method 的直连 HTTP CRUD、随意 `fetch` 改实体表），除非该资源**明确声明**不属于 Entangled 实体域（例如静态资源、一次性 OAuth、非实体 RPC）。

### Method 的两种名字空间（同一宪法，两种注册方式）

| 类别 | 含义 | 服务端注册 |
|------|------|------------|
| **标准 Method** | `create` · `update` · `delete` · `upsert`（以及协议中与之对应的 op） | `EntityDef` 的 `create_fn` / `update_fn` / `delete_fn` 等 |
| **自定义 Method** | 业务动词，如 `refresh` · `clear` · `mark_all_read` | `EntityDef.actions[name]` |

二者**没有高低之分**：标准 Method 只是**命名与契约被协议固定**的 Method；自定义 Method 是**命名由业务定义**的 Method。  
传输层可以仍使用 `op: create | action | …` 等编码，**产品与前端的心智模型必须统一为「一次 Method 调用」**，而不是「有的走 CRUD、有的走 action 两套哲学」。

### 前端表述

- 宿主 UI **不得**自行「管理实体增删改查」的**第二条写管道**；**增删改**一律通过 **`entangledMethod`**（`entityClient` 仅为其薄封装，不得新增旁路）。
- **查**（见第二条）不属于 Method：查是**对本地实体缓存的只读投影**，不是对网关的重复 CRUD。

---

## 第二条：读操作的唯一法 — Rust 实体缓存

**展示用数据一律以 Rust（SQLite）实体缓存为唯一真相来源。**

- 通过 `subscribe` + sync 帧（或流式 `prepend` 等引擎规定的补数手段）填充缓存后，由 `entity_list` / `entity_get`（或等价 API）读取。
- **禁止**为同一实体再维护「以网络 list/get 为主、缓存为辅」的双轨读路径。

---

## 第三条：订阅策略 — 由 Gateway 声明

**谁 lazy、谁 eager、级联订谁，由 Gateway 侧实体定义（及 schema 暴露）声明，不由宿主应用硬编码列表。**

- 客户端只消费 schema（如 `subscriptionMode`、`subscriptionCascade`），执行 subscribe / 级联 subscribe；**不在业务壳层复制一份「要订哪些实体」的魔法常量**（允许临时回退与兼容层，但必须标注为违宪技术债）。

---

## 第四条：与实现的关系

- **宪法约束的是语义与边界**，不是某一版函数名。  
- 当前代码若仍保留 `create` / `update` / `action` 等**分名 API**，应视为 **Method 族的分派表面**；重构方向是**统一暴露单一 Method 入口**，而非增加新的旁路写接口。

---

## 第五条：Stream 同步 — 必有界

**`sync_type == "stream"` 的实体（如消息、日志）在任意同步路径下不得对宿主数据源发起无 `LIMIT` 的全表读取。**

- 首次订阅、op-log 空洞回退等场景一律使用 **`head_n`** 语义：`fetch_data_fn(limit = depth + 1)`，返回窗口内条目并带 `hasMore`。
- **客户端 `depth`** 优先；**未传**时使用 **`EntityDef.sync_limit`**；二者皆缺失时由引擎常量 **`DEFAULT_STREAM_HEAD_DEPTH`**（见 `entangled/server/sync.py`）兜底，**并**受 **`MAX_STREAM_HEAD_DEPTH`** 上界约束。
- **宿主**（Gateway）在调用 `resolve_sync` 时必须传入 **`default_stream_depth=EntityDef.sync_limit`**，且 **`fetch_data_fn`** 支持按 `limit` 查询。

---

## 修订

对本宪法的修改应在版本控制中可审计；破坏性语义变更须同步更新本文与 `@entangled/react` / Gateway 的对外说明。
