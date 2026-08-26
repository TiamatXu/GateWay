# AI 网关：里程碑路线图

- **日期**：2026-08-22
- **状态**：已确认
- **前置**：[技术选型与工程基线](2026-08-22-gateway-tech-stack-design.md)、[身份、组织与计费主体模型](2026-08-22-identity-org-billing-model-design.md)
- **总估算**：36–51 周（约 9–12 个月），单人 + Claude Code

---

## 1. 切法与依据

采用**纵向骨架优先**（walking skeleton），而非自底向上的地基优先。

先建一条极窄但端到端贯通的真实链路，每个环节用最简实现，但**接口按最终形态一次定死**；随后逐环节加深、横向铺开厂商。

**依据**：长跑项目中最昂贵的返工不是「地基不牢」，而是「接口定错」——接口一旦错，所有依赖它的模块一并返工。纵向骨架能在第一个月就用真实流量证伪接口设计。相较之下，地基优先的风险在于账本与计价在没有任何真实用量向量产出之前全是纸上抽象，容易设计出与实际计费维度对不上的模型。

**Rust 强化了这个选择**：M0 的产出不只是文档，而是 `core` crate 的领域类型与各 crate 的 trait 定义——**它本身可编译、可类型检查**。接口设计有误，编译器会在 M1 就报出来，而不是等到 M4 才发现抽象漏了。

**已否决的备选**：地基优先（账本 → 句柄 → 描述层 → 计价 → 转换器）。理由如上。

---

## 2. 跨里程碑的硬规矩

这四条贯穿全程，违反即视为设计缺陷而非进度问题：

1. **接口一次定死。** `core` 的领域类型与各 crate 的 trait 签名在 M0/M1 按**最终形态**确定，不按当期需求裁剪。例如 `EndpointShape` 必须含 `Duplex` 变体（即使 M4 才实现），`Coordinator::hold()` 必须接受账户链（即使 M0 只用链长为 1）。
2. **schema 一次到位。** 数据库结构按最终模型建，不留「后续 migration 改结构」的债。组织树的 ltree 列、同步字段与覆盖层、账本的幂等键与 TTL，M0/M1 一次建好。
3. **每个里程碑结束时系统可运行、可测试。** 不存在「这个里程碑只有代码没有可执行产物」的情况。
4. **最简实现必须显式标记。** 以 `// SIMPLIFIED(Mx):` 注释标注，并在该里程碑的清单中登记。未标记的最简实现会腐化为默认实现，这是纵向骨架切法的主要失败模式。
5. **性能优化由实测触发。** 缓存、分片、租约、批量化一类以性能为唯一目的的机制，不进入里程碑交付清单，除非已有实测数据指认瓶颈。接口与 schema 按最终形态预留，实现推迟——这样推迟的成本恒定，而提前实现的复杂度每天都在付。依据见 [M1 账本范围重定义](2026-08-26-m1-ledger-scope-design.md)。

---

## 3. 里程碑详述

### M0 — 骨架与契约（2–3 周）✅ 已完成（2026-08-23）

**目标**：建立全部接口契约，并用一条真实链路验证其可用。

**交付**

- `core` crate 全部领域类型：`Money(i64)`、`UsageVector`、`ProtocolKind`、`EndpointShape`（五维完整）、`HandleId`、`HandleKind`、`NodeId`、`AccountId`、`TenantRef`
- 各 crate 的 trait 定义：`Coordinator`、`LogSink`、`UsageExtractor`、`PriceEngine`、`ProviderRegistry`、`RouteResolver`、`DirectorySource`
- workspace 骨架、`[workspace.dependencies]`、CI（`cargo-nextest` + clippy + sqlx offline）
- 最小组织树：`Root` + 单个 `Personal` 节点 + 单个 `Account`（schema 按最终形态建，含 ltree 与覆盖层字段）
- 单一 OpenAI 兼容厂商的 chat 端点，含 SSE 流式
- 透传路径打通：鉴权 → 账户链解析 → 路由 → 零拷贝转发 → SSE tee 抽 usage → Hold → Capture → 落账
- `AdmissionGate` 与 `LoadProbe`：并发预算 + cgroup 内存水位 + 滞回；优雅退出与过载共用同一闸门
- 请求头脱敏的硬黑名单
- `Coordinator` 仅实现 `pg`，且仅支持链长为 1
- `PriceEngine` 仅支持「单价 × 用量向量」，但价格规则表结构为最终形态

**验收标准**

1. 真实跑通一次带计费的流式请求，账本余额与用量对得上
2. **客户端中途断连时，按已生成部分正确结算**——这条是核心，它同时验证了 tee、Hold 生命周期与结算路径
3. 内存水位超阈值时拒绝新请求且在途请求不受影响
4. `cargo check` 通过全部 trait 定义，无 `todo!()` 残留于类型签名

**完成情况**（2026-08-23）

四条验收标准全部通过，端到端测试见 `crates/gateway/tests/e2e.rs`。实施中相对
本文档的偏离已就地记入对应 spec：

| 偏离 | 原因 |
|---|---|
| SSE 解析自研，不用 `eventsource-stream` | 推拉模式不匹配，见技术基线 §4.4 |
| `UsageExtractor` 增加 `estimated()` | 断连时末帧未到达，不落 tokenizer 兜底则断连等于免费 |
| `Quote.rule_id` → `rule_ids` | 一次计价按维度各匹配一条规则，单个 id 指不到规则集合 |
| `hold` 增加 `status`，结算后不删行 | 删行会释放 `idempotency_key`，重试将二次扣费 |
| `LogSink::write` 接收批次而非单条 | 与「批量写入任务」的设计一致 |
| `AdmissionGate::check` 不接 `InboundRequest` | 负载闸门与请求内容无关；按 Key 限流在 M1 接入时再加参数 |

**尚未做的 M0 项**：`rate_allow` 与 `try_lock` 两个 `Coordinator` 方法只在文档中
定义，未落代码——它们的使用方（限流、巡检任务）都在 M1，届时随实现一并加入。

---

### M1 — 账本完整（3–4 周）✅ 已完成（2026-08-26）

**目标**：把账本做到生产可信。这是全项目风险最高的模块。

> **范围于 2026-08-26 修订**，见 [M1 账本范围重定义](2026-08-26-m1-ledger-scope-design.md)。
> 原清单中的四项性能优化——配额租约、双模式切换、热点账户分片、Redis Coordinator——
> 经同类项目调研后全部移出：它们解决的都是尚未测量到的问题。M1 由此收窄为
> **账本正确性完整**，性能优化转为由实测触发。三项否决均不改动任何接口契约。

**交付**

- 嵌套额度完整实现：账户链多级冻结、**单事务原子性**、按 `AccountId` 升序排序加锁、跨级回滚、上级额度阻断行为 ✅
- `Coordinator` 一致性测试套件，`pg` 与 `mem` 两实现共用 ✅
- 回收器单实例化：以 `pg_advisory_lock` 保证多节点下不重复扫描
- `Coordinator::try_lock` 与 `rate_allow` 落地（M0 遗留的两个纯文档定义），并补上 `AdmissionGate::check` 的限流参数
- 防死冻结在 M1 可收口的防线全部到位：TTL 强制、回收器、类型层强制、对账与指标。第 3 道（租约归还）随租约否决而不适用；第 5、6 道的使用方分别在 M3、M4
- `Session` 与 `Metered` 两种 `BillingTiming` 的 Hold 生命周期语义（实现，供 M3/M4 使用）
- 指标暴露：`active_holds`、`hold_age_p99`、`expired_holds_reclaimed`

**验收标准**

1. 高并发压力测试：真实 PostgreSQL 上并发执行随机操作序列，结束后三条不变量全部成立
2. `proptest` 在 `mem` 与 `pg` 两实现上断言同一组不变量：任意操作序列后 `held == SUM(活跃 Hold)`、`balance` 恒等于初始值减去全部已 Capture 之和、`held >= 0`
3. 混沌测试：进程在 Hold 与 Capture 之间被强杀，回收器能完整释放，无余额泄漏

**风险**：这是全项目最容易写出隐蔽 bug 的模块。压力测试与 `proptest` 若发现不变量破坏，优先修实现而非放宽断言。

**完成情况**（2026-08-26）

三条验收标准全部通过，286 个测试全绿，clippy `-D warnings -W pedantic` 干净。

| 交付 | 落点 |
|---|---|
| 回收器单实例化 | `crates/bin/src/main.rs` 的 `spawn_reclaimer`，锁名 `ledger.reclaim_expired` |
| `try_lock` | `crates/ledger/src/lock.rs`；`pg` 用 `pg_try_advisory_lock`，`LockGuard` 独占一条摘出的连接 |
| `rate_allow` | `crates/ledger/src/rate.rs` 令牌桶，两实现共用 |
| `Session` / `Metered` 语义 | `crates/ledger/src/rolling.rs` 的 `RollingHold` |
| 高并发压测 | `conformance::concurrent_operations_preserve_invariants`，24 任务 × 16 操作 |
| 不变量 `proptest` 双实现 | `conformance::check_invariants`，`mem` 96 例 / `pg` 8 例 |
| 混沌测试 | `crates/ledger/tests/chaos.rs` + `src/bin/crash_probe.rs` |
| 指标 | `ledger.active_holds`、`ledger.hold_age_p99_seconds`、`ledger.expired_holds_reclaimed` |

二进制冒烟（双节点）已验证互斥与故障转移：节点 A 启动后接管扫描，节点 B 全程
不接管；SIGKILL 节点 A 后，节点 B 在下一个 tick 内接管；SIGTERM 优雅退出。

实施中相对本文档的偏离：

| 偏离 | 原因 |
|---|---|
| `try_lock` 的 `ttl` 参数被 `pg` 实现忽略 | advisory lock 随连接释放，比 TTL 更及时；`mem` 实现按 TTL 生效 |
| `Coordinator` 新增 `hold_stats` | 指标要的活跃数与年龄分位数是库侧聚合，没有它就得让调用方自己写 SQL |
| `RateLimiter` 状态在进程内 | 已标 `SIMPLIFIED(M1)`，跨节点聚合随共享存储一起推迟 |
| `LedgerError` 新增 `TimingNotRolling` | 非滚动 timing 开滚动 Hold 会让 `close` 的语义含糊，在入口挡掉 |

**未做的 M1 项**：`AdmissionGate::check` 的限流参数未接。`rate_allow` 已具备，
但按 Key 限流需要先有 API Key 的限流配置来源——那是 M8 的身份模型，届时一并接入。

---

### M2 — Provider 描述层（3–4 周）✅ 已完成（2026-08-26）

**前置**：~~端点覆盖空白区调研~~ ✅ 已完成（2026-08-26），见
[M2 前置调研：端点覆盖空白区](2026-08-26-m2-endpoint-coverage-research.md)。

**设计**：[M2：Provider 描述层设计](2026-08-26-m2-descriptor-layer-design.md)。

调研结论：`EndpointShape` 五维**无需增加维度**，空白区的难点在描述文件的其余字段——
按端点覆盖 base_url、请求签名类鉴权、一响应多句柄字段、请求体内的句柄解析、
用量提取区分「估算」与「实际」两个时点。以火山引擎资产 API 作为表达力压测对象。

**交付**

- ~~Provider 描述文件的 YAML schema 与 JSON Schema 校验~~ ✅
  schema 由 Rust 类型经 `schemars` 生成，提交入库、CI 校验一致（同 `.sqlx` 约定）
- ~~描述文件解释器~~ ✅ path 模板、method、五维声明、鉴权、用量提取、计费维度
- ~~hook 机制~~ ✅ 定为纯函数字段级算子（无 IO、无状态）；需 IO 与缓存的凭证获取
  拆为 `CredentialDef` 有限枚举，不走 hook。`wasmtime` 不预留抽象——逃生舱的形态
  取决于它要装什么，无用例时设计容易错
- ~~首批厂商全部改为描述文件驱动，主干代码零厂商分支~~ ✅
  `protocol_of()` 的路径前缀判断、`USAGE_SPEC` 全局常量、固定 `BillingTiming`、
  硬取 `model`/`stream`/`max_tokens` 四处已拆除
- ~~YAML 库实测~~ ✅ `serde-saphyr` 1.1 四项全过（锚点、merge key、嵌套枚举、
  带行列的错误信息），不启用 `yaml-rust2` 回退

**验收结果**

| 标准 | 结果 |
|---|---|
| 新增一个端点编写 0 行 Rust | ✅ `providers/*.yaml` 四个 provider、12 个端点（5 个已接客、7 个待 M3），主干无厂商分支 |
| hook 使用比例 ≤ 5% | ✅ 0/35 槽位，由 `Catalog::hook_ratio` 机械计数并在测试中断言 |
| 表达力压测：火山引擎资产 API | ✅ 四个难点（不同 host、AK/SK 签名、跨端点句柄引用、二进制上传）全部落入 schema |

**M2 期间的设计决定**（详见设计文档 §11 已否决方案汇总）

| 决定 | 理由 |
|---|---|
| 描述文件是数据不是程序：只能填值选算子，无表达式语言 | 表达式的内置函数表一样是有限集合，退化为算子表加语法糖；且会把语义错误从加载期推到运行期 |
| 句柄路径用完整 JSONPath，不自造受限方言 | RFC 9535 有现成实现且作者大概率已会；加载期编译化解误用风险 |
| 提交↔轮询显式引用端点 id | 靠 `HandleKind` 隐式匹配时，一个 provider 有两个任务族即静默歧义 |
| 用量三时点三组独立规则 | 三个时点读的是不同的文档，共用一组规则只会更绕 |
| 未实现端点延期而非报错 | 否则 M3 端点的描述文件无法先写出来验证表达力，而那正是验收项 4 |

**顺带的结构调整**

- `Accum` / `Tokenizer` 枚举下沉到 `core`（描述文件要用，而 `registry` 不能反向依赖 `meter`）
- 「JSON 值转账单用量」的截断规则收进 `core::as_billable_i64`——抽取与描述文件求值
  两处都要用，各写一遍必然漂移，而这是计费正确性规则
- `ProtocolKind` 的序列化名显式重命名为 `openai_chat`（snake_case 会拆成 `open_ai_chat`），
  因为这个名字现在是描述文件作者要手写的契约

**未做的 M2 项**：`CredentialDef::Oauth2ClientCredentials` 与 `InjectDef::Sign`
的运行时实现留到 M3——schema 已定义完整，加载期报出所需里程碑。

---

### M3 — 句柄、异步托管与智能路由（5–7 周）

**交付**

- ~~虚拟资源 ID 签发与解析，`虚拟ID → (渠道, 上游ID, 归属, 预扣单号)` 映射~~ ✅
  `gwh_` 前缀 + 定长记号，扫描替换（句柄常嵌在 `asset://<id>` 这类串里）；
  签发按 `(渠道, 类型, 上游ID)` 幂等
- ~~渠道亲和~~ ✅ 改为**由实际解析到的句柄推导**，不再只看 `HandleRole`——
  提交端点的 `shape.handle` 是 `Issues`，却可能引用别的渠道签发的素材。
  `handles.consume` 随之上升为入站契约的一部分（亲和要在选渠道之前算出）
- ~~异步任务状态机、poll 更新、终态结算~~ ✅ 状态解释挂在**提交**端点上
  （轮询端点可能全 provider 共用）；终态结算权靠 `WHERE phase = 0` 条件更新抢占；
  失败任务一律不收费。webhook 更新未做
- ~~孤儿任务巡检（覆盖「用户提交后再不查询」）~~ ✅ 与客户端轮询共用
  `task::advance`；当前只处理任务 ID 在路径参数里的轮询端点
- ~~`OnTerminal` 计费时点~~ ✅ `Hold::detach()` 把预扣托管到句柄上，
  TTL 不取消（防死冻结第 5 道防线）；非 token 维度的预扣上限来自 `usage.estimate`
- 对外同时暴露原生 poll 端点（透传）与统一任务 API
- **智能路由**：过滤（熔断中/不支持该模型/亲和不匹配/已禁用）→ 加权打分（价格、P99 延迟、错误率、在途负载、优先级）→ 选择（最高分 / 加权随机）。健康统计保持节点本地，不跨节点同步。

> ⚠️ **路由策略在实施前需专项讨论**。此处仅定框架形态，具体的打分维度、权重默认值与策略组合方式待单独讨论后确定。
>
> 已知的两个约束：纯价格路由会坑用户（最便宜的渠道通常最易限流、质量最差，且请求前用量未知只能按 `max_tokens` 估），价格应为打分维度之一而非唯一决策；健康统计跨节点同步会导致误判扩散，本地样本量已足够。

**验收标准**：**阿里云百炼原生异步端点 + 火山引擎资产 API 接通**——这是立项时点名的两个痛点，也是 `JsonThenBinary` 与 `Metered` 两种形态的首次实战

**进度**：阿里云百炼原生异步端点已接通（e2e 覆盖提交 → 轮询 → 终态结算、归属隔离、
孤儿巡检）。火山引擎资产 API 仍延期，缺 `RequestForm::JsonThenBinary`、
`auth.inject: sign`（AK/SK 签名）与 `BillingTiming::Metered`。

设计与已否决方案见 `2026-08-26-m3-handle-async-design.md`。

**M3 期间的结构调整**

- 新增 `resource` crate：句柄映射与任务托管共享同一套生命周期概念，按技术基线 §6.3
  先合并不拆
- `TaskPhase` 下沉到 `core`（`registry` 要用它归一化厂商状态串，而 `registry` 不能依赖 `resource`）
- `PriceCtx` 新增 `estimate`：非 token 维度的预扣上限只能来自描述文件的 `usage.estimate`，
  否则 `OnTerminal` 端点的预扣恒为 0
- `SettlementGuard` 的 Hold 变为 `Option`：不计费端点不建 Hold，但仍留日志
- `request_log` 新增 `handle_id`：一次异步任务的提交与终态结算是两行日志，靠它串起来
- `prepare_upstream_headers` 剥离 `content-length`：句柄改写会改变请求体长度

---

### M4 — Duplex 实时（3–4 周）

**交付**

- WebSocket 双向透传（`axum::extract::ws` 入站 + `tokio-tungstenite` 上游）
- 双侧计量：上行音频秒数、下行 token 与音频秒数
- 会话级滚动 Hold（依赖 M1 的 `Session` 语义）
- WS 会话的优雅退出：等待窗口覆盖实时会话时长
- 明确不做跨协议转换（各厂商实时协议事件模型不兼容，见技术基线 §8.7）

**验收标准**：一次完整的 Realtime 会话，双向用量准确，会话异常断开时 Hold 正确释放

---

### M5 — 计价完整（2–3 周）

**交付**

- 价格规则表：匹配条件（模型/渠道/用户组/端点/时间段）、阶梯（按上下文长度、时段、用量）、定价方式（成本价×倍率 / 绝对单价 / 保底封顶）
- 成本价与售价分离，可算每次请求毛利
- 价格版本化与快照绑定（改价不影响历史账单）
- explain 接口：每笔账返回命中规则 ID、各维度数量、单价
- `estimated` 标记的展示（见技术基线 §4.6）

**验收标准**：任意一笔账单可回答「为什么扣这么多」；改价后历史账单金额不变

---

### M6 — 请求归档（2–3 周）

**交付**

- 请求体与响应体归档至对象存储（`opendal`，统一 S3 / OSS / COS / Azure / GCS / 本地文件系统）
- **SSE 默认保留原始帧序列，不做拼接**。拼接需要协议知识，而透传架构的前提是网关可以不认识上游协议；强制拼接会违背「新端点 0 行 Rust」。拼接规则由 Provider 描述文件对已知协议声明，是增强而非必需
- 按大小分流：< 256 KiB 内存缓冲后上传；≥ 256 KiB 边流边写本地临时文件，结束后由后台任务上传并删除。内存占用 O(1)
- 断流与异常：客户端断连标 `truncated` + 字节位置；上游错误标 `failed`；节点崩溃的孤儿临时文件在启动时补传或清理；上传失败进重试队列，超限标 `archive_failed` 并告警
- 三层开关：全局开关 + 采样率（按租户/端点）+ 大小上限
- 本地暂存目录的磁盘配额与 LRU 清理

**验收标准**：归档失败不影响请求本身，也不影响计费——usage 抽取与归档是 tee 的两个独立消费者

**已知取舍**：引入本地磁盘依赖，节点崩溃会丢失未上传的归档。可接受，因为归档不是钱。

---

### M7 — 转换适配器（4–6 周）

**交付**

- `proto` crate：四套规范协议类型（OpenAI Chat、OpenAI Responses、Anthropic Messages、Gemini generateContent），每个类型携带 `#[serde(flatten)] extra` 与原始 body 副本
- 适配器按规范协议注册，避免 N×N 爆炸
- 能力协商：strict 报错 / lenient 降级并回写警告 header，**绝不静默丢字段**

**验收标准**：跨协议转换无损；降级场景有明确 warning，无静默字段丢失

---

### M8 — 组织与身份（5–7 周）

**交付**

- 完整 ltree 组织树：任意深度、节点类型、同步字段与覆盖层
- 目录同步抽象 `DirectorySource`，差分应用逻辑
- SCIM 2.0 server（覆盖 Entra ID / Okta / Google Workspace / OneLogin）
- 钉钉、飞书、企业微信连接器（pull + 事件回调）
- 第三方登录：标准 OIDC 一套代码 + 十余个非标 OAuth2 平台的 profile 映射适配器
- 身份绑定表与首次登录流程
- **热点账户分片**（M1 移出项）：`account_balance` 写入随机落片、读取时 SUM，冻结失败轮询下一分片重试。此处才有真实的企业级根账户可供验证，触发条件见 [M1 账本范围重定义](2026-08-26-m1-ledger-scope-design.md) §3.3

**验收标准**：从钉钉与飞书各完成一次全量同步 + 一次增量事件，本地覆盖层不被覆盖；至少三个平台可完成登录并正确绑定到同步来的成员记录

---

### M9 — 管理面（6–8 周）

**交付**

- React + Vite + Semi Design 管理后台，产物 `rust-embed` 嵌入二进制
- ClickHouse 用量分析与成本归属报表（按 org / project / user / key / model / channel 任意维度聚合）
- 跨部门项目的报表层分摊与内部结算单
- New API 数据导入器（channel / token / user）
- 审计日志

**验收标准**：企业可直接部署使用；从 New API 迁移的数据完整可用

**风险**：前端工作量对单人项目历来是估算黑洞，此处 6–8 周为乐观估计。

---

## 4. 风险登记

| 风险 | 里程碑 | 缓解 |
|---|---|---|
| 账本并发正确性 | M1 | `loom` 模型穷举 + `proptest` 不变量 + 混沌测试，三重验证 |
| ~~`serde-saphyr` 成熟度不足~~ ✅ 已解除 | M2 | 2026-08-26 实测四项全过，采用；回退路径未启用 |
| ~~描述文件表达力不够，hook 比例超标~~ ✅ 已解除 | M2 | 2026-08-26 实测 0/35 槽位用 hook；压测对象火山资产 API 全部落入 schema |
| 前端工作量失控 | M9 | 估算已标注为乐观值；必要时先交付管理 API，UI 分批 |
| 归档存储成本失控 | M6 | 三层开关（全局/采样率/大小上限）；base64 视频类大 body 默认不归档 |
| 路由策略设计未定 | M3 | 实施前需专项讨论，框架先行、策略后定 |
| ltree 扩展不可用 | 全程 | `store` crate 中以 feature 隔离物化路径回退方案 |

---

## 5. 总览

| 里程碑 | 内容 | 估算 | 累计 |
|---|---|---|---|
| M0 | 骨架与契约 | 2–3 周 | 2–3 |
| M1 | 账本完整 | 4–6 周 | 6–9 |
| M2 | Provider 描述层 | 3–4 周 | 9–13 |
| M3 | 句柄、异步托管与智能路由 | 5–7 周 | 14–20 |
| M4 | Duplex 实时 | 3–4 周 | 17–24 |
| M5 | 计价完整 | 2–3 周 | 19–27 |
| M6 | 请求归档 | 2–3 周 | 21–30 |
| M7 | 转换适配器 | 4–6 周 | 25–36 |
| M8 | 组织与身份 | 5–7 周 | 30–43 |
| M9 | 管理面 | 6–8 周 | 36–51 |

每个里程碑各自走一遍 spec → plan → 实现的完整周期。
