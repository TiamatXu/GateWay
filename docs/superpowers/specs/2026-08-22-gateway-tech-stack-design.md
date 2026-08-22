# AI 网关：技术选型与工程基线

- **日期**：2026-08-22
- **状态**：已确认
- **范围**：产品定位、代码基线、语言与框架选型、工程骨架、工程规约
- **不含**：模块内部设计、接口契约、里程碑拆分（见后续 spec）

---

## 1. 背景

### 1.1 要解决的痛点

现有 New API / one-api 系网关存在五个痛点，全部来自两条架构假设——「OpenAI 协议是强制中间表示」和「网关是无状态中转」：

| 痛点 | 根因 |
|---|---|
| 原生协议支持能力差 | 任何请求必须先转成 OpenAI 格式再转出去，厂商特有字段被丢弃 |
| 模型价格配置死板 | 倍率连乘模型无法表达开放计费维度 |
| 表达式控制价格复杂不直观 | 用表达式而非数据表达定价规则 |
| 预付费机制不完善 | 「先查余额、后扣费」在并发、流式、异步下必然透支 |
| 厂商原生请求形态难接 | 非 chat 形态（异步提交、资产管理、文件上传）塞不进 OpenAI 管道 |

典型的难啃案例：火山引擎的资产 API、阿里云百炼的原生异步端点。

**同时必须保留**：跨协议转换能力。

### 1.2 产品定位

**开源自部署为主，并且要能直接满足企业内部平台需求。**

由此推出的硬约束：

- 部署门槛必须低（单二进制 + 一份 `docker-compose.yml`）
- 「余额」需同时表达**真钱**（开源中转站场景）与**配额/成本分摊**（企业内部场景）两种语义
- 需要 SSO（OIDC / LDAP）、按部门/项目的成本归属、审计日志
- 管理后台完整度是硬需求，不可裁剪

### 1.3 代码基线

**全新仓库，不 fork New API。**

由于语言选定为 Rust（见 §3），New API 的 Go 代码无法直接移植。其残留价值仅限于**协议知识来源**：各厂商字段兼容性、SSE 断帧方式、usage 字段位置等。这部分通过阅读其源码获取，不产生代码继承。

另需提供一次性数据导入器，把 New API 的 channel / token / user 数据迁入新 schema。

---

## 2. 继承的架构前提

以下九条在前序设计讨论中已确认，本文档不再论证，仅作为选型依据记录：

1. **双平面路由，透传优先**。路由键为 `(入站协议, 端点/能力, 模型) → (上游渠道, 出站协议)`。入站协议 == 出站协议时原生透传，不解析、不重组 body；协议不同才走转换适配器。转换从「必经之路」降级为「可选插件」。
2. **Provider 以声明式描述文件接入**，不写硬编码分支。每个 endpoint 声明 path 模板 / method / 请求形态（sync·stream·async-submit·async-poll·upload·download）/ 鉴权方式 / 用量提取规则 / 计费维度。复杂逻辑用 hook 兜底。
3. **虚拟资源句柄映射**。所有上游签发的句柄（task_id / file_id / batch_id / asset_id / cached_content_id / response_id）都由网关签发虚拟 ID，记录 `虚拟ID → (渠道, 上游ID, 归属用户, 预扣单号)`。带来渠道亲和与任务生命周期托管两个必需能力。
4. **计费 = 用量向量 + 价格规则表**。用量向量是开放维度的 `map[维度]数量`；价格规则是纯数据（匹配条件 / 阶梯 / 定价方式 / 生效时间），不是表达式。成本价与售价分离，价格版本快照绑定账单，提供 explain 接口。
5. **用量提取与协议转换解耦**。由 Provider 描述文件声明 JSONPath / SSE 聚合规则从原生响应抽 usage，上游不返回时用 tokenizer 兜底。否则透传模式无法计费。
6. **预付费 = Hold / Capture 双分录账本**。准入时按最坏情况原子冻结，完成时按实际捕获，Hold 带 TTL + 后台回收器。所有账本操作带幂等键，账本 append-only，余额是物化视图。
7. **转换必须无损 + 能力协商**。IR 携带原始 body 副本与 extensions 透传映射；目标协议无法表达源协议特性时，按策略选择 strict 报错或 lenient 降级并回写警告 header，绝不静默丢字段。适配器按「规范协议」注册，避免 N×N 爆炸。
8. **数据平面完全无状态**。亲和性是「虚拟ID → 上游渠道」的，不是「客户端 → 网关节点」的，因此不需要 sticky session 或一致性哈希。节点故障不影响已创建的异步任务。
9. **请求日志与账本物理分离**。账本要事务与精确，日志要压缩与聚合，混在同一实例是 New API 已知的规模瓶颈。

---

## 3. 语言选型：Rust

### 3.1 决定

**Rust（2024 edition）+ tokio。**

### 3.2 依据

工作负载特征：单请求延迟预算由上游决定（200ms–120s），网关自身微秒级开销无关紧要。真正的约束是三个——每个并发 SSE 流的内存占用、Provider 适配的开发效率、长连接下的尾延迟稳定性。

Rust 在本项目的三个具体红利：

1. **类型系统直接表达核心领域模型**。用量向量的开放维度、strict/lenient 能力协商、价格规则的多种定价方式，全部是 sum type 场景。`serde` 的 derive + `#[serde(untagged)]` + enum 建模，优于 Go 的 `map[string]interface{}` 体操。协议转换是 Rust 的强项而非短板。
2. **`Hold` 可建模为必须消费的类型**。未 Capture/Void 就被 drop 时可在编译期报错或运行时告警。「忘记释放 Hold 导致用户余额被永久冻结」是本类系统的典型事故，在 Go 中只能靠 code review 拦截。
3. **无 GC**。不需要 `GOMEMLIMIT` 调优，不存在高并发流式下被 OOMKill 的事故类别；每连接内存占用可预测（约 4KB 量级）。

配套优势：`wasmtime` 是 Rust 原生（利好后期 hook 机制）；`tokenizers` 本身是 Rust 库；`loom` 可对账本并发做模型穷举验证，而非靠压测碰运气。

### 3.3 已知代价与缓解

| 代价 | 缓解 |
|---|---|
| 无法移植 New API 的 Go 适配器代码 | 该层本就要重构成「规范协议注册 + 透传优先」模型，可原样移植的比例本来就低；真正的资产是协议知识，读源码即可获取 |
| 大型 workspace 增量编译慢 | 从第一天拆细 crate + `mold` linker + `cargo-nextest` + `sccache` |
| 借用检查器带来的迭代轮次成本 | 接受。属于长跑模式下的可承受成本 |
| 厂商官方 SDK 生态薄 | 不构成问题：透传优先架构下需要的是 HTTP 层原样转发，本就不使用厂商 SDK |

---

## 4. 技术栈

| 位置 | 选型 | 理由 |
|---|---|---|
| 异步运行时 | `tokio`（多线程调度器） | 事实标准 |
| HTTP 服务 | `axum` + `tower` | streaming body 一等公民，中间件生态最完整 |
| 上游客户端（透传） | `hyper` + `hyper-util` legacy client | 必须贴着 hyper，避免高层封装引入 body 缓冲 |
| 上游客户端（转换） | `reqwest` | 转换路径本就要完整读取 body，高层 API 更省事 |
| 反向代理 | **自研**（见 §4.1） | Rust 无 `httputil.ReverseProxy` 等价物 |
| 数据库驱动 | `sqlx`（PostgreSQL） | `query!` 宏在编译期连库校验 SQL 与类型，对账本 SQL 是强正确性保障 |
| 数据库迁移 | `sqlx::migrate!` | 内建，SQL 文件 embed 进二进制，零额外依赖 |
| Redis（可选） | `fred` | 异步、连接池、重连；便于用熔断器包裹以实现降级 |
| 日志分析库 | `clickhouse`（官方 crate） | |
| 序列化 | `serde` + `serde_json`；热点路径可换 `sonic-rs` | 透传路径不解析 JSON，只有转换路径承压 |
| YAML | `serde_norway`（或 `saphyr`） | ⚠️ `serde_yaml` 已停止维护 |
| Schema 校验 | `jsonschema` | Provider 描述文件必须有 schema 校验，否则用户写错 YAML 只能在运行时暴露 |
| 金额表示 | `Money(i64)` newtype，纳单位（1e-9） | 见 §5.2 |
| 配置加载 | `figment` | 多源合并 + serde 集成 |
| 日志 / 追踪 | `tracing` + `tracing-subscriber` + `tracing-opentelemetry` | 结构化日志与分布式追踪同源 |
| 指标 | `metrics` + `metrics-exporter-prometheus` | |
| Tokenizer | `tiktoken-rs` + 离线 BPE 词表 embed | 严禁运行时下载词表 |
| WASM hook（后期） | `wasmtime` | |
| 测试 | `rstest` + `proptest` + `testcontainers` + `loom` | proptest 断言「余额守恒」不变量；loom 验证账本并发 |
| 前端 | React + Vite + Semi Design | 沿用 New API 的前端技术栈；产物用 `rust-embed` 嵌入二进制 |
| 后台作业 | tokio 定时任务 + `pg_advisory_lock` + `FOR UPDATE SKIP LOCKED` | 引入 Asynq 类方案等于强依赖 Redis，与「无 Redis 降级」矛盾 |

### 4.1 需要自研的部分

Rust 生态在以下位置没有可直接使用的组件，均需自行实现。这些是本项目的核心工程量所在：

1. **流式反向代理**。`Body → Stream → hyper client → Stream → Body`，全程零拷贝、零缓冲。在 axum 中实现比 Go 的 `ReverseProxy` 定制更直接，因为不需要绕过框架对 `ResponseWriter` 的包装。
2. **动态 dispatcher**。本网关的路由不是静态路由：需按「协议规范 + 端点形态」匹配，并支持整段通配透传（如 `/v1/*`）。框架 router 只负责最外层挂载。
3. **SSE tee 旁路**。在流经时增量解析 SSE 帧抽取 usage，同时不阻塞、不缓冲下游转发；客户端中途断连时需按已生成部分结算。
4. **Coordinator 的 PG 实现**。原子冻结语句、热点账户分片、配额租约与回收、双模式（严格/租约）切换。
5. **Provider 描述文件的 schema 与解释器**。
6. **用量提取引擎**。JSONPath 取值 + SSE 增量聚合 + tokenizer 兜底的组合。
7. **轻量 job scheduler**。基于 advisory lock 保证单实例执行的巡检任务框架。

### 4.2 生态注意事项

- `serde_yaml` 已停止维护，必须使用 fork 或替代实现。
- `sqlx` 需提交 `.sqlx/` 离线元数据目录，保证 CI 与无数据库环境可编译。
- tokenizer 词表必须 embed 进二进制，不可运行时下载。

---

## 5. 数据存储

### 5.1 决定

| 数据 | 存储 | 说明 |
|---|---|---|
| 账本、余额、Hold、租约 | **PostgreSQL** | 唯一真相源，要事务与精确 |
| 资源句柄映射、任务状态 | PostgreSQL | 低 QPS |
| 配置、价格规则、Provider 描述 | PostgreSQL + 各节点内存快照 | 极低频写，高频读 |
| 请求日志、用量分析 | ClickHouse | 与账本物理分离 |
| 冻结/限流加速、配置 pub/sub | Redis（**可选**） | 纯加速器，故障时降级到 PG 路径而非 fail-closed |

**不支持 SQLite。** 单机模式同样使用 PostgreSQL。

理由：SQLite 分支需要单独实现一套并发语义（进程内锁 vs PG 行锁），对单人长跑是纯负担；目标用户（企业内部平台、认真自部署的开源用户）安装 PG 不构成门槛；「开箱即用」由一份 `docker-compose.yml` 承担。

`Coordinator` 因此有两个生产实现：`pg`（默认，零外部依赖）与 `redis`（内嵌 pg 作为 fallback）。另有 `mem` 实现，仅用于测试。

### 5.2 金额表示

**`Money(i64)` newtype，单位为 1e-9（纳单位）。**

- 范围：±92 亿（以美元计），远超需求
- 精度：可精确表示 1e-9 量级的单次请求费用
- 对外展示与价格配置使用 decimal 字符串，仅在边界转换

采用整数而非 `rust_decimal` 的三个理由：`balance - held >= amt` 在 `BIGINT` 上是整数比较，在 `NUMERIC` 上是变长十进制运算；decimal 在请求热路径上产生大量堆分配；整数无精度歧义，「余额守恒」不变量可被严格断言。

newtype 包装为零成本，同时禁止裸 `i64` 混入金额运算。

---

## 6. 工程骨架

### 6.1 Workspace 布局

```
gateway/
├── Cargo.toml              # [workspace] + [workspace.dependencies] 统一版本
├── crates/
│   ├── core/               # 领域类型：Money / UsageVector / ProtocolKind /
│   │                       #   EndpointShape / HandleId / TenantId …  仅依赖 serde
│   ├── infra/              # figment 配置加载、tracing/otel/prometheus 初始化
│   ├── store/              # sqlx：全部 SQL、迁移、Repository trait 与 PG 实现
│   ├── ledger/             # Coordinator trait + pg/redis/mem 实现
│   │                       #   Hold/Capture/Void、配额租约、限流、advisory lock、幂等
│   ├── pricing/            # 用量向量 → 规则匹配 → 金额；版本快照；explain
│   ├── registry/           # Provider 描述文件 schema、加载校验、渠道注册表、配置热更新
│   ├── meter/              # 用量提取：JSONPath / SSE 聚合 / tokenizer 兜底
│   ├── proto/              # 规范协议类型定义（OpenAI Chat / Responses / Anthropic / Gemini）
│   ├── transform/          # 适配器 + 能力协商 strict/lenient
│   ├── proxy/              # 自研零拷贝流式反向代理 + tee 旁路
│   ├── resource/           # 虚拟句柄映射、渠道亲和、异步任务状态机、孤儿巡检
│   ├── router/             # 路由决策、负载均衡、熔断（纯本地状态）
│   ├── gateway/            # 数据平面 axum app：鉴权、准入、dispatcher、生命周期编排
│   ├── console/            # 控制平面 axum app：管理 API、多租户、SSO
│   └── bin/                # 唯一二进制入口，--role=gateway|console|all
├── web/                    # React + Vite + Semi Design，产物 rust-embed 进 bin
├── migrations/             # sqlx 迁移 SQL
├── providers/              # 内置 Provider 描述文件（YAML）
└── docs/superpowers/specs/ # 设计文档
```

### 6.2 依赖方向

严格单向，禁止环：

```
core     ← 所有 crate（core 自身零业务依赖）
infra    ← store, gateway, console
store    ← ledger, pricing, registry, resource
proto    ← transform, meter
registry ← router, meter, pricing
gateway  ← 顶层编排，依赖除 console 外几乎全部
bin      ← gateway, console, infra
```

### 6.3 划分理由

- **`core` 保持极瘦**。被所有 crate 依赖，任何改动触发全量重编译。只放类型，不放逻辑，不引重依赖。
- **`transform` 单独隔离**。未来改动最频繁（每接一个厂商就动），隔离后其改动不触发 `ledger` / `store` 重编译。
- **`proto` 与 `transform` 分离**。proto 是稳定类型定义，transform 是易变逻辑；合并会让类型改动的编译代价被逻辑改动频率放大。
- **`resource` 暂不拆分**。虚拟句柄映射与异步任务托管共享同一生命周期概念，先合并；待边界在实现中确认清晰后再拆为 `handle` + `task`。

### 6.4 部署形态

同一二进制，通过 `--role` 切换：

| 形态 | 命令 | 依赖 |
|---|---|---|
| 一体化（开源自部署默认） | `--role=all` | PostgreSQL |
| 标准集群 | 多个 `--role=gateway` + 一个 `--role=console` | PostgreSQL + ClickHouse |
| 高负载 | 同上 + `COORDINATOR=redis` | 追加 Redis 作为纯加速器 |

数据平面无状态，可任意横向扩容。

---

## 7. 工程规约

| 项 | 规定 |
|---|---|
| Edition / MSRV | Rust 2024 edition；MSRV 跟随 stable-2 |
| 错误处理 | 库 crate 用 `thiserror` 定义具体错误类型；`bin` 顶层用 `anyhow`。网关错误类型**必须能携带上游原始响应体**——错误同样需要按出站协议格式化后返回客户端 |
| unsafe | 全 workspace `unsafe_code = "forbid"` |
| lint | `clippy::pedantic` 选择性开启；CI 使用 `-D warnings` |
| 依赖版本 | 全部在 `[workspace.dependencies]` 声明，子 crate 仅写 `.workspace = true` |
| 构建加速 | `mold` linker + `cargo-nextest` + `sccache`，从第一天配置 |
| SQL 离线元数据 | 提交 `.sqlx/`，保证无数据库环境可编译 |

---

## 8. 待定事项

以下尚未确定，需在后续 spec 中解决：

1. **里程碑拆分方案**。已提出「纵向骨架优先」与「地基优先」两种切法及 M0–M6 序列，尚未拍板。
2. **首批支持的厂商与端点清单**。将作为 Provider 描述层表达力的验收标准。已点名的必须项：火山引擎资产 API、阿里云百炼原生异步端点。
3. **多租户模型层级**。组织 / 项目 / 用户三层，还是两层。影响成本归属报表与 SSO 映射。
4. **hook 机制的引入时机与形态**。WASM（`wasmtime`）与内置 Rust hook 的取舍，以及是否在早期里程碑就需要。
5. **日志 sink 的抽象边界**。ClickHouse 是必选依赖还是可插拔（另有 PG / 文件 JSONL 实现）。
