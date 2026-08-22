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

**外部参考（不产生代码依赖）**：

- **AISIX**（api7 开源，Apache 2.0，Rust 原生 AI 网关）。其定位为「无论上游是什么协议，面向客户端的 API 始终保持 OpenAI 形状」，恰是本项目要推翻的假设；且开源版仅有速率与 token 限额，预算控制在其 Cloud 商业版。可参考价值在于其**端点形态清单**（chat / embeddings / rerank / images / audio / videos 提交-轮询-获取 / files / batches / fine_tuning）与**五适配器家族划分**（openai / anthropic / bedrock / vertex / azure-openai），可作为 `EndpointShape` 枚举设计的输入。若借鉴代码需处理 license 归属。
- **LiteLLM** 已改为 Rust core + Python SDK 架构，印证本项目的语言选型方向。

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
8. **数据平面对请求-响应型端点完全无状态**。亲和性是「虚拟ID → 上游渠道」的，不是「客户端 → 网关节点」的，因此不需要 sticky session 或一致性哈希。节点故障不影响已创建的异步任务。**唯一例外是 `Duplex`（WebSocket）端点**：连接本身绑定在具体节点上，节点故障会断开会话、需客户端重连。该例外不破坏句柄亲和模型，因为 Duplex 会话不签发可轮询的句柄；详见 §8.7。
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

### 4.1 依赖选型

| 位置 | 选型 | 理由 |
|---|---|---|
| 异步运行时 | `tokio`（多线程调度器） | 事实标准 |
| HTTP 服务 | `axum` + `tower` | streaming body 一等公民，中间件生态最完整 |
| 上游客户端（透传） | `hyper` + `hyper-util` legacy client | 必须贴着 hyper，避免高层封装引入 body 缓冲 |
| 上游客户端（转换） | `reqwest` | 转换路径本就要完整读取 body，高层 API 更省事 |
| 反向代理 | **自研**（见 §4.3） | Rust 无 `httputil.ReverseProxy` 等价物 |
| SSE 解析 | `eventsource-stream` | 只做 `byte stream → Event`，不夹带 HTTP 客户端，正是 tee 旁路需要的形态 |
| WebSocket | `axum::extract::ws`（入站）+ `tokio-tungstenite`（上游） | `Duplex` 端点所需，见 §8.7 |
| JSONPath | `serde_json_path` | RFC 9535 合规，跟随官方 CTS 测试套件；用于描述文件的 usage 提取 |
| 数据库驱动 | `sqlx`（PostgreSQL） | `query!` 宏在编译期连库校验 SQL 与类型，对账本 SQL 是强正确性保障 |
| 数据库迁移 | `sqlx::migrate!` | 内建，SQL 文件 embed 进二进制，零额外依赖 |
| Redis（可选） | `fred` | 异步、连接池、重连；便于用熔断器包裹以实现降级 |
| 日志 sink | `clickhouse`（默认）／`sqlx` MySQL feature（StarRocks·Doris）／`sqlx` PG（兜底） | 抽象为 `LogSink` trait，见 §5.3 |
| 序列化 | `serde` + `serde_json`；热点路径可换 `sonic-rs` | 透传路径不解析 JSON，只有转换路径承压 |
| YAML | **`serde-saphyr`** | 见 §4.5——这是全栈唯一没有老牌稳妥选项的位置 |
| Schema 校验 | `jsonschema` | Provider 描述文件必须有 schema 校验，否则用户写错 YAML 只能在运行时暴露 |
| 限流 | `governor` | GCRA 令牌桶；限流在节点本地执行，见「配额租约」设计 |
| 金额表示 | `Money(i64)` newtype，纳单位（1e-9） | 见 §5.2 |
| 配置加载 | `figment` | 多源合并 + serde 集成 |
| 日志 / 追踪 | `tracing` + `tracing-subscriber` + `tracing-opentelemetry` | 结构化日志与分布式追踪同源 |
| 指标 | `metrics` + `metrics-exporter-prometheus` | |
| Tokenizer | `tiktoken-rs`（OpenAI 系）+ `tokenizers`（HF，开放权重模型），词表离线 embed | 严禁运行时下载词表；覆盖边界见 §4.6 |
| 认证 | `openidconnect`（标准 OIDC）/ `oauth2`（钉钉·飞书·企微·GitHub 等非标）/ `ldap3` / `jsonwebtoken` / `argon2` | 见身份模型 spec §7 |
| 对象存储 | `opendal` | 一份实现覆盖 S3 / OSS / COS / Azure / GCS / 本地文件系统，契合多形态部署 |
| WASM hook（后期） | `wasmtime` | |
| 测试 | `rstest` + `proptest` + `testcontainers` + `loom` | proptest 断言「余额守恒」不变量；loom 验证账本并发 |
| 前端 | React + Vite + Semi Design | 沿用 New API 的前端技术栈；产物用 `rust-embed` 嵌入二进制 |
| 后台作业 | tokio 定时任务 + `pg_advisory_lock` + `FOR UPDATE SKIP LOCKED` | 引入 Asynq 类方案等于强依赖 Redis，与「无 Redis 降级」矛盾 |

### 4.2 生态盘点结论

已对全部所需能力做过生态核查。结论是：**Rust 生态在基础设施层全部有成熟件，缺口集中在业务编排层**——也就是本项目真正的价值所在。不存在因生态缺失而阻塞的能力。

唯一的生态风险点是 YAML 解析库（§4.5），需在 M2 动工前实测确认。

### 4.3 需要自研的部分

以下位置社区无可直接使用的组件，或有但不适用。这些构成本项目的核心工程量：

1. **流式反向代理**。`Body → Stream → hyper client → Stream → Body`，全程零拷贝、零缓冲。
2. **动态 dispatcher**。路由需按「协议规范 + 端点形态」匹配，并支持整段通配透传（如 `/v1/*`）。框架 router 只负责最外层挂载。
3. **SSE tee 旁路**。`eventsource-stream` 提供帧解析，但「边转发边增量抽取 usage、且不阻塞不缓冲下游」的编排需自研；客户端中途断连时需按已生成部分结算。
4. **Coordinator 的 PG 实现**。原子冻结语句、热点账户分片、配额租约与回收、双模式（严格/租约）切换。
5. **Provider 描述文件的 schema 与解释器**。
6. **用量提取引擎**。JSONPath 取值、SSE 增量聚合、tokenizer 兜底三者的编排——零件齐备，编排自研。
7. **价格规则引擎**。规则匹配、阶梯、版本快照、explain。
8. **虚拟句柄映射与异步任务状态机**。
9. **轻量 job scheduler**。基于 advisory lock 保证单实例执行的巡检任务框架。
10. **`proto` 规范协议类型定义**。见 §4.4。
11. **优雅退出与连接排空**。SIGTERM 后停止接受新请求、等待在途流自然结束、归还本节点持有的全部租约。
12. **双向流的双侧计量**。`Duplex` 端点两个方向都需抽取用量（上行音频秒数、下行 token 与音频秒数），且需在会话存续期间滚动结算，而非终态一次性结算。
13. **负载准入探针**。cgroup v2 内存与 CPU 读取（容器内不可用 `sysinfo`，其报告宿主机总量）、并发预算计算、滞回与滑动窗口。
14. **归档管线**。大小分流、本地暂存与补传、断流状态标记、孤儿文件回收。
15. **路由过滤与打分框架**。候选过滤、加权打分、选择器；健康统计与熔断器保持节点本地。
16. **会话级滚动 Hold**。`BillingTiming::Session` 要求 Hold 随会话推进分段追加与捕获，与请求级 Hold 是不同的生命周期模型。

### 4.4 已评估并否决的方案

**Pingora（Cloudflare 开源代理框架）。** 生产验证极充分——每秒 4000 万请求运行数年——并提供连接池、负载均衡、健康检查、TLS 及零停机优雅重载。否决理由有三：

- 它解决的是本项目最不痛的部分。连接池 hyper 自带；负载均衡本就需按渠道健康度自定义，用不上通用实现。核心复杂度在计费、句柄映射、描述文件驱动，这些 Pingora 无法覆盖。
- 它是框架而非库，会绑定整个数据平面的编程模型。控制平面必然使用 axum，两套框架并存使认知成本翻倍。
- 官方定位明确：其适用场景是「CPU 效率成为账单项」的规模。本项目不在该量级。其缓存相关 API 目前仍标注为 experimental 且高度不稳定。

**代价**：放弃零停机优雅重载。对 SSE 流可达 10 分钟的网关这有实际价值，但 K8s 滚动更新可覆盖多数场景；裸机部署的开源用户无法受益。接受此取舍。

**`async-openai` 作为 `proto` crate 的依赖。** 该 crate 支持 `serde(flatten)` 扩展与 `byot`（bring your own types）feature，可容纳未知字段。否决理由：它为「调用 OpenAI」设计，而非「无损转发任意 OpenAI 兼容厂商请求」。本架构要求每个类型携带 `#[serde(flatten)] extra` 与原始 body 副本，这是一等公民要求而非可选扩展；且它仅覆盖 OpenAI，Anthropic / Gemini 的规范协议仍需自写。更关键的是，依赖第三方类型定义等于将协议演进速度交予上游发版节奏。

**结论**：`proto` 自行定义，将 `async-openai` 用作字段清单的参考而非依赖。

### 4.5 生态风险：YAML 解析库

这是全栈唯一没有老牌稳妥选项的位置，需显式管理：

- `serde_yaml` 已停止维护
- 社区曾转向 `serde_yml`，但该项目**因 unsoundness 问题被 archive**，并有对应安全公告
- `serde_norway` 是维护中的 fork，但**依赖 `unsafe-libyaml`**，与本 workspace `unsafe_code = "forbid"` 的规约精神冲突
- **`serde-saphyr`** 为当前最优解：现代解析器、serde 集成、无 Value DOM、纯 Rust 内存安全

**缓解措施**：在 M2（Provider 描述层）动工前，先用真实的 Provider 描述文件对 `serde-saphyr` 做一次实测（含 merge key、嵌套枚举、锚点引用等特性）。若不满足，退回 `yaml-rust2`（低层解析器，自行对接 serde）。此项列为已识别技术风险。

### 4.6 用量计量的生态边界与兜底策略

生态边界是硬事实：`tiktoken-rs` 仅覆盖 OpenAI 系；`tokenizers` 覆盖开放权重模型；**Anthropic 与 Gemini 的 tokenizer 不公开**，官方给出的方案是调用其 `count_tokens` API。

因此用量获取分三档，优先级由高到低：

| 档位 | 条件 | 做法 |
|---|---|---|
| 1 | 上游响应返回 usage | 直接采用。覆盖绝大多数情况 |
| 2 | 上游不返回，但厂商提供 `count_tokens` API | 调用校准，**异步回填，不阻塞请求** |
| 3 | 两者皆无 | 用 tiktoken 对应编码估算 |

**第 3 档必须在账单记录上标记 `estimated = true`，并在 explain 接口中明示该笔为估算值。** 这是产品决策而非纯技术选择：缺少此标记，计费争议将无法追溯举证。

### 4.7 其他实现注意

- `sqlx` 需提交 `.sqlx/` 离线元数据目录，保证 CI 与无数据库环境可编译。
- tokenizer 词表必须 embed 进二进制，不可运行时下载。
- StarRocks 路径下 `sqlx` 的 `query!` 编译期校验不可用（`information_schema` 差异），需退回运行时 `query()` API。

---

## 5. 数据存储

### 5.1 决定

| 数据 | 存储 | 说明 |
|---|---|---|
| 账本、余额、Hold、租约 | **PostgreSQL** | 唯一真相源，要事务与精确 |
| 组织树、账户、API Key、身份绑定 | PostgreSQL（**需 `ltree` 扩展**） | 任意深度部门树；祖先/后代查询走 GiST 索引 |
| 资源句柄映射、任务状态 | PostgreSQL | 低 QPS |
| 配置、价格规则、Provider 描述 | PostgreSQL + 各节点内存快照 | 极低频写，高频读 |
| 请求日志、用量分析 | ClickHouse（默认，**可插拔**） | 与账本物理分离；sink 可替换，见 §5.3 |
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

### 5.3 日志 Sink 可插拔

请求日志与用量分析的写入端抽象为 `LogSink` trait，提供三个实现：

| 实现 | 定位 | 客户端 |
|---|---|---|
| `clickhouse` | **默认**，开源自部署推荐 | `clickhouse` crate |
| `mysql-wire` | 覆盖 StarRocks / Doris / SelectDB，企业已有集群时使用 | `sqlx` MySQL feature |
| `postgres` | 兜底，小规模或不愿新增组件时复用同一个 PG | `sqlx` PostgreSQL |

**默认选 ClickHouse 的理由是部署重量。** 它是单进程、数百 MB 内存即可运行；StarRocks 需要 FE（JVM）与 BE 两类进程，生产高可用需 3 FE + 3 BE。本项目以开源自部署为主要分发形态，`docker-compose` 的组件数量是核心门槛。其次是日志场景的压缩比——model / channel / user_id 等低基数列重复度极高，列存压缩可达 10–20 倍。

**保留 mysql-wire 实现的理由。** 一份实现同时覆盖 StarRocks / Doris / SelectDB 三家，且复用已有的 sqlx 依赖，增量成本低。企业用户通常已有此类集群，可避免为网关单独运维 ClickHouse；BI 工具与既有报表系统亦可直连。

**已评估并否决：将 StarRocks 设为唯一后端。** 其主键模型支持实时 UPSERT，看似契合「异步任务日志需回填终态」的场景。但任务的权威状态本就应存于 PostgreSQL——Hold 的生命周期挂在任务对象上，必须与账本同库同事务——日志侧只需在终态写入一条计费记录，更新需求不成立。StarRocks 的 JOIN 能力优于 ClickHouse 的 Dictionary 维表方案，但不足以抵消部署重量的差距。

**实现注意**见 §4.7。

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
│   ├── identity/           # 组织树、账户链解析、API Key、目录同步（SCIM +
│   │                       #   钉钉/飞书/企微连接器）、第三方登录
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

严格单向，禁止出现环。下表读作「左列的 crate 被右列的 crate 依赖」：

| 被依赖方 | 依赖它的 crate |
|---|---|
| `core` | 全部 crate |
| `infra` | `store`, `gateway`, `console`, `bin` |
| `store` | `ledger`, `pricing`, `registry`, `resource`, `identity` |
| `proto` | `transform`, `meter` |
| `registry` | `router`, `meter`, `pricing` |
| `ledger`, `pricing`, `meter`, `proxy`, `resource`, `router`, `transform`, `identity` | `gateway` |
| `store`, `ledger`, `pricing`, `registry`, `identity` | `console` |
| `gateway`, `console` | `bin` |

`gateway` 是数据平面的顶层编排者，`console` 是控制平面的顶层编排者，两者互不依赖；`bin` 只负责组装它们。

### 6.3 划分理由

- **`core` 保持极瘦**。被所有 crate 依赖，任何改动触发全量重编译。只放类型，不放逻辑，不引重依赖。
- **`transform` 单独隔离**。未来改动最频繁（每接一个厂商就动），隔离后其改动不触发 `ledger` / `store` 重编译。
- **`proto` 与 `transform` 分离**。proto 是稳定类型定义，transform 是易变逻辑；合并会让类型改动的编译代价被逻辑改动频率放大。
- **`identity` 独立于 `console`**。目录同步（SCIM server + 三个国内平台连接器）与第三方登录（两类共十余个平台）的逻辑量大且演进独立；账户链解析还需被 `gateway` 的热路径调用，不能锁在控制平面里。注意 `ledger` **不**依赖 `identity`——它只接受 `&[AccountId]`，账户链由 `gateway` 解析后传入，保持两者解耦。
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
| Edition / MSRV | Rust 2024 edition；MSRV 设为「当前 stable 往前两个版本」，随工具链滚动更新 |
| 错误处理 | 库 crate 用 `thiserror` 定义具体错误类型；`bin` 顶层用 `anyhow`。网关错误类型**必须能携带上游原始响应体**——错误同样需要按出站协议格式化后返回客户端 |
| unsafe | 全 workspace `unsafe_code = "forbid"`。该约束仅作用于本项目自有 crate，不传递至第三方依赖；但在同等条件下优先选择纯 Rust 实现（如 §4.5 的 YAML 库选型） |
| lint | `clippy::pedantic` 选择性开启；CI 使用 `-D warnings` |
| 依赖版本 | 全部在 `[workspace.dependencies]` 声明，子 crate 仅写 `.workspace = true` |
| 构建加速 | `mold` linker + `cargo-nextest` + `sccache`，从第一天配置 |
| SQL 离线元数据 | 提交 `.sqlx/`，保证无数据库环境可编译 |

---

## 8. 端点形态建模（EndpointShape）

### 8.1 问题

产品目标是「所有厂商的所有端点，并在社区网关不支持的端点之上支持更多」。因此端点清单是**开放集合**，任何形式的枚举都会迅速过期；而枚举若定义在 `core` crate 中，每次过期都触发全量重编译。

### 8.2 拆分：端点身份与端点形态

| 概念 | 性质 | 归属 |
|---|---|---|
| **端点身份**（`/v1/chat/completions`、`/api/v3/contents/generations/tasks`） | 开放集合 | 完全由 Provider 描述文件声明，`core` 中不出现 |
| **端点形态** | 有限集合 | `core` 中建模。网关的处理逻辑本就只有数种 |

需要建模的只有后者，且它是若干**正交维度的组合**，不是一维枚举。

### 8.3 五个维度

```rust
pub struct EndpointShape {
    pub request:  RequestForm,    // 请求体如何进入
    pub response: ResponseForm,   // 响应体如何返回
    pub handle:   HandleRole,     // 与虚拟句柄的关系
    pub billing:  BillingTiming,  // 结算时点
    pub retry:    RetryPolicy,    // 故障转移时是否可重试
}

pub enum RequestForm   { None, Json, Multipart, Binary, JsonThenBinary }
pub enum ResponseForm  { Json, Sse, Ndjson, Binary, Duplex }
pub enum HandleRole    { None, Issues(HandleKind), Consumes(HandleKind), Terminates(HandleKind) }
pub enum BillingTiming { InRequest, OnTerminal, Metered, Session, NotBilled }
pub enum RetryPolicy   { Safe, IdempotentWithKey, Unsafe }
```

任何新端点都是这五个维度的一个组合。**新增端点不需要修改 `core`，只需增加一份 YAML 声明。**

### 8.4 模型压测

用六个最刁钻的真实端点验证覆盖度：

| 端点 | request | response | handle | billing | retry |
|---|---|---|---|---|---|
| 阿里云百炼 文生视频提交 | `Json` | `Json` | `Issues(Task)` | `OnTerminal` | `IdempotentWithKey` |
| 阿里云百炼 任务查询 | `None` | `Json` | `Consumes(Task)` | `NotBilled` | `Safe` |
| 火山引擎 资产上传（签名直传） | `JsonThenBinary` | `Json` | `Issues(Asset)` | `Metered` | `Unsafe` |
| Gemini `cachedContents` 创建 | `Json` | `Json` | `Issues(Cache)` | `Metered` | `IdempotentWithKey` |
| Anthropic Batches 结果下载 | `None` | `Ndjson` | `Consumes(Batch)` | `NotBilled` | `Safe` |
| OpenAI Realtime | `None`（WS upgrade） | `Duplex` | `None` | `Session` | `Unsafe` |

六者全部落入模型，无需新增变体。其中三种形态是社区网关普遍不支持的，构成本项目的差异化落点：

- `JsonThenBinary`——签名直传式上传
- `Metered`——按存续时长计费的缓存与资产
- `Session`——会话内滚动计费

### 8.5 衍生推导

两项关键策略可直接从形态推导，无需在描述文件中重复声明，减少两处出错点：

| 推导项 | 依据 | 规则 |
|---|---|---|
| 渠道亲和性 | `HandleRole` | `Consumes` / `Terminates` 必须回到签发该句柄的原渠道；`Issues` / `None` 无亲和要求 |
| 预扣策略 | `BillingTiming` | `InRequest` 按最坏情况估算预扣；`OnTerminal` 按任务上限预扣且 Hold 挂在句柄对象上；`Metered` 按周期滚动；`Session` 随会话分段追加 |

### 8.6 验收标准

产品差异化在于端点覆盖广度，因此验收指标不是「接入了多少厂商」，而是：

> **接入一个全新端点需要编写 0 行 Rust 代码。**

逃生舱为 hook 机制，用于五维模型确实装不下的情形（自定义签名算法、需预先换取 token 的鉴权流程等）。目标比例为 **95% 端点纯 YAML、5% 走 hook**。若实际比例显著低于此，应判定为描述文件表达力设计不足，需回头修正模型——而非逐个添加特例。

### 8.7 `Duplex` 的额外影响

`Duplex`（WebSocket 双向流，如 OpenAI Realtime、豆包实时语音）已确认纳入第一版实现。它对架构有三处超出「多一个枚举变体」的影响：

**一、Duplex 端点仅支持透传，不支持跨协议转换。** 各厂商实时协议的事件模型差异极大（OpenAI Realtime、Gemini Live、豆包实时语音的会话状态机互不兼容），跨协议转换的成本与收益严重不成比例。第一版明确不做；若未来要做，应作为独立的适配器族设计。

**二、会话与节点存在粘性。** 这是 §2 第 8 条「数据平面无状态」的唯一例外。WebSocket 连接绑定在具体节点上，节点故障即断开会话，需客户端重连。该例外不破坏句柄亲和模型——Duplex 会话不签发可轮询的句柄——但要求：负载均衡器必须支持 WebSocket；优雅退出的等待窗口需覆盖实时会话时长。

**三、计量与结算模型不同。** 两个方向都需抽取用量（上行音频秒数、下行 token 与音频秒数），且必须在会话存续期间滚动结算。请求级的「预扣—结算」两段式模型在此不适用，需要 `Session` 专属的分段 Hold 生命周期。此项已列入自研清单第 12、13 条，并将在 M1（账本做实）中一并设计。

---

## 9. 待定事项

以下尚未确定，需在后续 spec 中解决：

1. ~~里程碑拆分方案~~ — **已解决**，见 [里程碑路线图](2026-08-22-milestones-roadmap.md)。结论：纵向骨架优先，M0–M8 共九个里程碑，总估算 32–45 周。
2. **端点覆盖空白区调研**（M2 前置）。厂商与端点清单不需自行枚举——社区网关已有现成清单，本项目按热门度排优先级逐步接入。真正需要产出的是「主流网关普遍不支持的端点」清单，那份空白清单是产品差异化的靶子。不阻塞 `core` 建模，在 M2（Provider 描述层）动工前完成即可。
3. ~~多租户模型层级~~ — **已解决**，见 [身份、组织与计费主体模型](2026-08-22-identity-org-billing-model-design.md)。结论：单棵任意深度组织树（ltree），计费主体 `Account` 与层级解耦、可挂任意节点，请求解析为账户链；嵌套额度第一版完整实现。
4. **hook 机制的具体形态**。引入时机已定为 M2；WASM（`wasmtime`）与内置 Rust hook 的取舍待定——当前倾向内置 Rust hook 优先、`wasmtime` 预留接口。
