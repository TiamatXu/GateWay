# M0：骨架与契约

- **日期**：2026-08-22
- **状态**：待评审
- **估算**：2–3 周
- **前置**：[技术基线](2026-08-22-gateway-tech-stack-design.md)、[身份模型](2026-08-22-identity-org-billing-model-design.md)、[路线图](2026-08-22-milestones-roadmap.md)

---

## 1. 目标

建立全部接口契约，并用一条真实链路验证其可用。

M0 的产出不是「一个能用的网关」，而是**一组经过真实流量验证的接口**。此后所有里程碑都在这组接口上加深实现，不改签名。

---

## 2. `core` 类型

### 2.1 标识

```rust
pub struct NodeId(pub i64);
pub struct AccountId(pub i64);
pub struct ApiKeyId(pub i64);
pub struct ChannelId(pub i64);
pub struct ProviderId(pub SmolStr);

pub struct HoldId(pub Uuid);
pub struct HandleId(pub Uuid);
pub struct RequestId(pub Uuid);
```

组织相关标识用 `i64`，因为 ltree label 不接受连字符（技术基线 §2.3 已记）。对外暴露的虚拟句柄用 UUID。

### 2.2 金额

```rust
pub struct Money(i64);   // 纳单位，1e-9
```

仅提供 checked 运算，禁止裸 `i64` 隐式转入。序列化对外为 decimal 字符串，对内为 `BIGINT`。

### 2.3 用量向量

维度是开放集合，因此以字符串为键：

```rust
pub struct UsageVector(BTreeMap<UsageDim, i64>);
pub struct UsageDim(SmolStr);

pub mod dims {
    pub const INPUT_TOKENS: &str = "input_tokens";
    pub const OUTPUT_TOKENS: &str = "output_tokens";
    pub const CACHED_INPUT: &str = "cached_input";
    pub const CACHE_WRITE: &str = "cache_write";
    pub const REASONING_TOKENS: &str = "reasoning_tokens";
    pub const AUDIO_MILLIS: &str = "audio_millis";
    pub const IMAGES: &str = "images";
    pub const REQUESTS: &str = "requests";
    pub const STORAGE_BYTE_SECONDS: &str = "storage_byte_seconds";
}
```

数量一律用整数。小数单位通过维度自身表达（`audio_millis` 而非 `audio_seconds`），避免浮点进入计费路径。

`BTreeMap` 而非 `HashMap`：账单的 breakdown 需要稳定顺序以便对比与审计。

### 2.4 协议与端点形态

```rust
pub enum ProtocolKind {
    OpenAiChat,
    OpenAiResponses,
    AnthropicMessages,
    GeminiGenerateContent,
    Native(ProviderId),      // 厂商私有协议，仅透传
}

pub struct EndpointShape {
    pub request:  RequestForm,
    pub response: ResponseForm,
    pub handle:   HandleRole,
    pub billing:  BillingTiming,
    pub retry:    RetryPolicy,
}

pub enum RequestForm   { None, Json, Multipart, Binary, JsonThenBinary }
pub enum ResponseForm  { Json, Sse, Ndjson, Binary, Duplex }
pub enum HandleRole    { None, Issues(HandleKind), Consumes(HandleKind), Terminates(HandleKind) }
pub enum BillingTiming { InRequest, OnTerminal, Metered, Session, NotBilled }
pub enum RetryPolicy   { Safe, IdempotentWithKey, Unsafe }

pub enum HandleKind { Task, File, Batch, Asset, Cache, Response, Session }
```

全部变体在 M0 定义完整，即使 M0 只实现 `Json`/`Sse` + `InRequest` + `None` 的组合。

`ProtocolKind::Native` 用于客户端直接使用厂商私有协议的情形——这是透传优先架构的常态，不是例外。

---

## 3. Trait 契约

### 3.1 `Coordinator`

```rust
#[async_trait]
pub trait Coordinator: Send + Sync {
    async fn hold(&self, req: HoldRequest<'_>) -> Result<Hold, LedgerError>;
    async fn capture(&self, hold: Hold, actual: Money) -> Result<(), LedgerError>;
    async fn void(&self, hold: Hold) -> Result<(), LedgerError>;

    async fn extend(&self, hold: &Hold, delta: Money) -> Result<(), LedgerError>;
    async fn capture_partial(&self, hold: &Hold, amount: Money) -> Result<(), LedgerError>;

    async fn rate_allow(&self, key: &RateKey, rate: u32, burst: u32) -> Result<bool, LedgerError>;
    async fn try_lock(&self, key: &str, ttl: Duration) -> Result<Option<LockGuard>, LedgerError>;
}

pub struct HoldRequest<'a> {
    pub chain: &'a [AccountId],      // 由近及远，链首为主计费主体
    pub amount: Money,
    pub ttl: Duration,
    pub idempotency_key: &'a str,
    pub timing: BillingTiming,
}
```

`capture` / `void` 按值取走 `Hold`；`extend` / `capture_partial` 取引用，供 `Session` 与 `Metered` 的滚动场景使用。

M0 只实现 `pg`，且 `chain` 长度恒为 1。

### 3.2 `Hold` 与泄漏防护

```rust
#[must_use = "Hold must be captured or voided"]
pub struct Hold {
    id: HoldId,
    chain: SmallVec<[AccountId; 4]>,
    amount: Money,
    expires_at: DateTime<Utc>,
    consumed: bool,
    reclaimer: mpsc::Sender<HoldId>,
}

impl Drop for Hold {
    fn drop(&mut self) {
        if !self.consumed {
            metrics::counter!("ledger.hold_leaked").increment(1);
            tracing::error!(hold_id = %self.id, "hold dropped without capture/void");
            let _ = self.reclaimer.try_send(self.id);
        }
    }
}
```

**不在 `Drop` 中 panic**：`Drop` 内 panic 在已 panicking 时会 abort，且 async 任务被取消时正走此路径。TTL 回收器是最终兜底。

### 3.3 `UsageExtractor`

```rust
pub trait UsageExtractor: Send {
    fn feed(&mut self, chunk: &[u8]);
    fn snapshot(&self) -> UsageVector;
    fn finish(self: Box<Self>) -> UsageVector;

    /// 用量是否为 tokenizer 估算值
    fn estimated(&self) -> bool { false }
}
```

tee 有两个消费者，职责与可靠性要求不同：

```rust
pub struct Tee {
    usage: Box<dyn UsageExtractor>,          // 必须，关系到账单
    archive: Option<Box<dyn ArchiveSink>>,   // 可选，M6 实现
}
```

**归档失败绝不影响 usage 抽取，也绝不影响请求本身。** 账单必须无条件抽出，归档是可开关的旁路。M0 只实现 usage，但 tee 结构按最终形态建。

`snapshot()` 是断连结算的关键——它返回当前已抽取到的用量，不要求流已结束。

`estimated()` 是 §4.6 三档兜底的必要出口。**客户端中途断连时，携带 usage 的末帧
从未到达**——第 1 档拿不到数据，若不落到第 3 档用 tokenizer 估算，断连就等于免费，
这是可被利用的漏洞。估算结果必须标记并一路传到账单记录，否则计费争议无法举证。

`feed` 必须是纯状态机推进，不做 IO、不做阻塞操作，否则会拖慢转发。

### 3.4 `PriceEngine`

```rust
#[async_trait]
pub trait PriceEngine: Send + Sync {
    async fn estimate_max(&self, ctx: &PriceCtx<'_>) -> Result<Money, PriceError>;
    async fn quote(&self, ctx: &PriceCtx<'_>, usage: &UsageVector) -> Result<Quote, PriceError>;
}

pub struct Quote {
    pub amount: Money,
    pub cost: Money,                              // 成本价，用于毛利计算
    pub rule_ids: SmallVec<[RuleId; 8]>,          // 参与本次计价的全部规则
    pub rule_version: i64,                        // 快照绑定
    pub breakdown: Vec<(UsageDim, i64, Money)>,   // explain 数据
    pub estimated: bool,                          // 用量为估算值时置位
}
```

`rule_ids` 是复数：一次计价按维度各匹配一条规则，单个 `rule_id` 无法指向实际生效的
规则集合。痛点之一是「表达式控制价格复杂不直观」，explain 必须能精确指到每条规则。

```rust
```

```rust
pub struct PriceCtx<'a> {
    pub model: &'a str,
    pub channel: ChannelId,
    pub tier: &'a str,
    pub endpoint: &'a str,
    pub at: DateTime<Utc>,          // 用于时段规则与版本快照
    pub max_output_tokens: Option<u32>,
}
```

`estimate_max` 供准入时的最坏情况预扣使用；`at` 同时决定命中哪个价格版本，改价不影响历史账单。

### 3.5 `AdmissionGate` 与负载保护

准入判断全部收拢到一个闸门——负载过载与优雅退出做的是同一件事：拒绝新请求、不影响在途请求。

```rust
pub trait AdmissionGate: Send + Sync {
    fn check(&self, req: &InboundRequest) -> Admission;
}

pub enum Admission {
    Allow,
    Reject { reason: RejectReason, retry_after: Option<Duration> },
}

pub enum RejectReason {
    Overloaded(Signal),
    Draining,
    RateLimited,
}

pub enum Signal { Concurrency, Memory, Cpu, SchedulerLag }

pub trait LoadProbe: Send + Sync {
    fn sample(&self) -> LoadSample;
}

pub struct LoadSample {
    pub inflight_streams: u32,
    pub memory_ratio: f32,
    pub cpu_ratio: f32,
}
```

**主保护是并发预算，不是被动测量。** 在途 SSE 流是内存占用的主要来源，且可预算：

```
在途流上限 = (容器内存 limit × safety_factor) / per_stream_budget
默认：per_stream_budget = 64 KiB，safety_factor = 0.7
```

Rust 无 GC 不确定性，内存约等于「连接数 × 每连接开销 + 转换路径 body 缓冲」，算得准。RSS 水位与 CPU 水位作为兜底，覆盖非流式的内存占用。

**三条实现约束：**

1. **容器内必须读 cgroup，不可用 `sysinfo` 读系统内存**——后者在容器中报告宿主机总量，会恒定显示空闲。读 cgroup v2 的 `memory.current` / `memory.max`（fallback v1），非 Linux 平台降级为禁用负载保护。
2. **必须有滞回**：进入拒绝态 90%、退出 80%，配滑动窗口平滑瞬时尖峰。单阈值会在临界点反复开关，客户端看到间歇性 503。
3. **`/readyz` 不反映瞬时负载**，只反映结构性状态（启动完成、DB 连通）。过载时摘节点会把流量压向其余节点引发雪崩；若为整体容量不足，摘谁都无用。过载只在请求路径上返回 503。

拒绝响应为 HTTP 503 + `Retry-After`，**错误体按入站协议的错误格式构造**——OpenAI 协议进来就返回 OpenAI 格式的 error 对象，否则客户端 SDK 无法解析。

### 3.6 `RequestRecord` 与请求头脱敏

```rust
pub struct RequestRecord {
    pub request_id: RequestId,
    pub key_id: ApiKeyId,
    pub account_chain: SmallVec<[AccountId; 4]>,
    pub node_path: String,
    pub channel: ChannelId,
    pub model: String,
    pub endpoint: String,
    pub shape: EndpointShape,
    pub usage: UsageVector,
    pub quote: Option<Quote>,
    pub status: RequestStatus,
    pub req_headers: HeaderMap,      // 已脱敏
    pub resp_headers: HeaderMap,     // 已脱敏
    pub archive_ref: Option<ArchiveRef>,   // M6 填充
    pub started_at: DateTime<Utc>,
    pub ended_at: DateTime<Utc>,
}

pub enum RequestStatus { Ok, Truncated, Failed, Rejected }
```

请求头采用黑名单模式记录，**分两层**：

| 层 | 内容 | 可配置 |
|---|---|---|
| 硬黑名单 | `Authorization`、`X-Api-Key`、`Api-Key`、`Cookie`、`Set-Cookie`、`Proxy-Authorization`，及各厂商密钥头（`X-Security-Token`、`X-Amz-Security-Token` 等） | **否** |
| 软黑名单 | `Accept-Encoding`、`Connection` 等噪音头 | 是 |

硬黑名单不可由配置覆盖。理由是安全边界不能依赖管理员配置正确——一次误配即等同密钥泄漏。

### 3.7 其余 trait

```rust
pub trait RouteResolver: Send + Sync {
    fn resolve(&self, req: &InboundRequest) -> Result<Route, RouteError>;
}

pub struct Route {
    pub channel: ChannelId,
    pub outbound: ProtocolKind,
    pub shape: EndpointShape,
    pub upstream: Url,
    pub needs_transform: bool,     // 入站协议 != 出站协议
}

#[async_trait]
pub trait ProviderRegistry: Send + Sync {
    fn lookup(&self, provider: &ProviderId, path: &str) -> Option<EndpointDesc>;
    fn snapshot_version(&self) -> u64;
}

#[async_trait]
pub trait LogSink: Send + Sync {
    async fn write(&self, rec: RequestRecord) -> Result<(), SinkError>;
}

#[async_trait]
pub trait DirectorySource: Send + Sync {
    async fn full_sync(&self) -> Result<DirectorySnapshot, DirectoryError>;
    async fn apply_event(&self, ev: DirectoryEvent) -> Result<(), DirectoryError>;
}
```

`DirectorySource` 在 M0 只定义，不实现——它属于 M7，但签名现在定死。

---

## 4. 数据库 schema

M0 按**最终形态**建表，不留后续改结构的债。表建全，M0 只有部分表有数据。

```sql
CREATE EXTENSION IF NOT EXISTS ltree;

CREATE TABLE org_node (
    id             BIGSERIAL PRIMARY KEY,
    uuid           UUID NOT NULL UNIQUE,
    parent_id      BIGINT REFERENCES org_node(id),
    path           LTREE NOT NULL,
    kind           SMALLINT NOT NULL,
    source         SMALLINT NOT NULL,
    provider       TEXT,
    external_id    TEXT,
    name           TEXT NOT NULL,          -- 同步字段
    name_override  TEXT,                   -- 覆盖层
    note           TEXT,
    cost_center    TEXT,
    deleted_at     TIMESTAMPTZ,
    UNIQUE (provider, external_id)
);
CREATE INDEX org_node_path_gist ON org_node USING GIST (path);

CREATE TABLE account (
    id        BIGSERIAL PRIMARY KEY,
    node_id   BIGINT NOT NULL REFERENCES org_node(id),
    tier      TEXT NOT NULL DEFAULT 'default',
    currency  TEXT NOT NULL DEFAULT 'USD'
);

CREATE TABLE account_balance (
    account_id BIGINT   NOT NULL REFERENCES account(id),
    shard      SMALLINT NOT NULL,
    balance    BIGINT   NOT NULL DEFAULT 0,   -- 纳单位
    held       BIGINT   NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, shard)
);

-- status：0 = 活跃，1 = 已捕获，2 = 已撤销，3 = 已过期回收
CREATE TABLE hold (
    id              UUID PRIMARY KEY,
    amount          BIGINT NOT NULL,
    timing          SMALLINT NOT NULL,
    status          SMALLINT NOT NULL DEFAULT 0,
    idempotency_key TEXT NOT NULL UNIQUE,
    expires_at      TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    settled_at      TIMESTAMPTZ
);
CREATE INDEX hold_expires_idx ON hold (expires_at) WHERE status = 0;

-- 一个 Hold 在账户链上的每一级各占一行。M0 恒为一行，M1 起可多行。
CREATE TABLE hold_leg (
    hold_id    UUID     NOT NULL REFERENCES hold(id) ON DELETE CASCADE,
    account_id BIGINT   NOT NULL,
    shard      SMALLINT NOT NULL,
    amount     BIGINT   NOT NULL,
    depth      SMALLINT NOT NULL,      -- 0 = 链首，主计费主体
    PRIMARY KEY (hold_id, account_id)
);
CREATE INDEX hold_leg_account_idx ON hold_leg (account_id);

CREATE TABLE quota_lease (
    lease_id   UUID PRIMARY KEY,
    account_id BIGINT NOT NULL,
    node_id    TEXT NOT NULL,
    amount     BIGINT NOT NULL,
    consumed   BIGINT NOT NULL DEFAULT 0,
    expires_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX quota_lease_expires_idx ON quota_lease (expires_at);

CREATE TABLE ledger_entry (          -- append-only
    id         BIGSERIAL PRIMARY KEY,
    account_id BIGINT NOT NULL,
    kind       SMALLINT NOT NULL,    -- hold / capture / void / topup / transfer
    amount     BIGINT NOT NULL,
    hold_id    UUID,
    request_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE api_key (
    id            BIGSERIAL PRIMARY KEY,
    node_id       BIGINT NOT NULL REFERENCES org_node(id),
    creator_id    BIGINT,
    hash          BYTEA NOT NULL UNIQUE,
    prefix        TEXT NOT NULL,
    account_chain BIGINT[] NOT NULL,      -- 物化，避免热路径遍历树
    disabled_at   TIMESTAMPTZ
);

CREATE TABLE channel (
    id         BIGSERIAL PRIMARY KEY,
    provider   TEXT NOT NULL,
    base_url   TEXT NOT NULL,
    credential BYTEA NOT NULL,
    enabled    BOOLEAN NOT NULL DEFAULT true
);

CREATE TABLE price_rule (
    id             BIGSERIAL PRIMARY KEY,
    version        BIGINT NOT NULL,
    match_model    TEXT,
    match_channel  BIGINT,
    match_tier     TEXT,
    match_endpoint TEXT,
    dim            TEXT NOT NULL,
    unit_price     BIGINT NOT NULL,   -- 纳单位 per 百万用量单位
    cost_price     BIGINT NOT NULL,   -- 同上
    effective_from TIMESTAMPTZ NOT NULL
);

CREATE TABLE config_version (id INT PRIMARY KEY DEFAULT 1, version BIGINT NOT NULL);
```

`hold` 行结算后不删除，只置 `status`。**删除会释放 `idempotency_key`**——已结算请求的重试会被当作全新请求二次冻结、二次扣费。代价是 `hold` 表持续增长，需要归档策略（M9）。

`request_log` 是 `LogSink` 的 postgres 实现所需，M0 仅此一种落地形态：

```sql
CREATE TABLE request_log (
    request_id    UUID PRIMARY KEY,
    key_id        BIGINT,
    account_chain BIGINT[] NOT NULL,
    node_path     LTREE,
    channel_id    BIGINT,
    model         TEXT NOT NULL,
    endpoint      TEXT NOT NULL,
    shape         JSONB NOT NULL,
    usage         JSONB NOT NULL,
    quote         JSONB,
    status        SMALLINT NOT NULL,
    req_headers   JSONB NOT NULL,      -- 已脱敏
    resp_headers  JSONB NOT NULL,      -- 已脱敏
    archive_ref   TEXT,                -- M6 填充
    started_at    TIMESTAMPTZ NOT NULL,
    ended_at      TIMESTAMPTZ NOT NULL
);
```

`api_key.account_chain` 是物化列，组织树或 Account 绑定变更时批量重算。

`hold` 与 `hold_leg` 分离是嵌套额度的前提：一次冻结在账户链的每一级各占一个 leg，单事务内全部写入，全成或全败。M0 恒为单 leg，但表结构与写入路径按最终形态实现，M1 只需放开链长限制。

`unit_price` 的单位是**纳单位 per 百万用量单位**，而非 per 单位。原因是精度：厂商定价普遍以百万 token 为基准（如 $3/M tokens），若按 per token 存储，低价模型会损失有效数字。按百万为基准时 $3/M 直接存为 `3_000_000_000`，两端都无精度损失。

---

## 5. 请求生命周期

```
0. 准入      AdmissionGate::check  [本地原子读，纳秒级]
1. 入站      axum handler 接收，不读取 body
2. 鉴权      API Key 哈希 → (key_id, node_id, account_chain)   [单次主键查询]
3. 路由      (入站协议, 路径, 模型) → Route
4. 准入      PriceEngine::estimate_max → Coordinator::hold
             创建 SettlementGuard，持有 Hold 与用量累积器
5. 转发      构造上游请求，body 以 stream 转发，不缓冲
6. 响应      上游 body → TeeStream → 客户端
                          ↓
                    UsageExtractor::feed  (同步状态机推进)
7. 终止      正常结束 / 客户端断连 / 上游错误
8. 结算      SettlementGuard 触发 settle：
                 usage = extractor.snapshot()
                 quote = PriceEngine::quote(ctx, usage)
                 Coordinator::capture(hold, quote.amount)
                 → RequestRecord 投递到日志通道（非阻塞）
9. 落账      独立的批量写入任务消费日志通道 → LogSink::write
```

**第 8、9 步必须是两条独立通道。** capture 关系到钱，要求可靠且及时；日志可以批量、可以延迟、极端情况可以丢弃。若共用一个任务，慢 sink（如 ClickHouse 抖动）会拖累 capture。日志投递对结算路径非阻塞——通道满时丢弃并计数告警，绝不反压到结算。

第 4 步的 `SettlementGuard` 是整条链路的核心——它保证第 8 步在任何终止路径下都会发生。

**检查顺序不可调换**：负载闸门在最前，因为过载时应在最便宜的位置拒绝，而非查库之后。整条链路的成本递增——本地原子读 → 缓存/主键查询 → 本地令牌桶 → PG 写。

---

## 6. 断连结算

M0 的核心验收项。

```rust
struct SettlementGuard {
    hold: Option<Hold>,
    extractor: Arc<Mutex<Box<dyn UsageExtractor>>>,
    ctx: SettlementCtx,
    settle_tasks: TaskTracker,      // 进程级，由 app state 持有
}

impl Drop for SettlementGuard {
    fn drop(&mut self) {
        if let Some(hold) = self.hold.take() {
            let (extractor, ctx) = (self.extractor.clone(), self.ctx.clone());
            self.settle_tasks.spawn(async move { settle(hold, extractor, ctx).await });
        }
    }
}
```

**机制**：客户端断连 → axum 丢弃 response body → `TeeStream` 析构 → `SettlementGuard` 析构 → 结算按已抽取用量执行。上游连接同时断开，多数厂商停止生成。

**两处必须处理**：

1. `Drop` 中不能 await，只能 spawn。因此结算任务统一注册到进程级的 `TaskTracker`（由 app state 持有并注入，不用全局 static），**优雅退出时必须等待其排空**，否则关机瞬间断连的请求会漏账。
2. `TeeStream` 的 `feed` 与转发同步执行，不得引入额外 await 点或锁竞争，否则会拖慢转发延迟。

---

## 7. 测试策略

| 层次 | 内容 | 工具 |
|---|---|---|
| 单元 | `Money` 溢出、`UsageVector` 合并、SSE extractor 状态机（含分片边界、跨 chunk 断帧） | `rstest` |
| 集成 | 真实 PG 起容器，跑完整链路 | `testcontainers` |
| 断连 | 客户端在流中途 drop，验证结算金额等于已生成部分 | mock 上游 SSE |
| 账本 | `held == SUM(活跃 Hold)` 不变量 | `proptest` |
| 泄漏 | `Hold` 未消费即 drop，验证指标上报与回收队列投递 | 单元 |
| 通道隔离 | `LogSink` 阻塞时 capture 不受影响 | 集成 |
| 准入 | 内存水位跨越阈值时的滞回行为，不产生抖动 | 单元 |
| 脱敏 | 硬黑名单头在任何配置下都不出现在 `RequestRecord` 中 | 单元 |

SSE extractor 的跨 chunk 断帧测试必须覆盖——真实网络下 SSE 帧被任意切分，这是最容易出 bug 且最难在生产中发现的地方。

---

## 8. 验收标准

1. 真实跑通一次带计费的流式请求，账本余额与用量对得上
2. **客户端中途断连时，按已生成部分正确结算**
3. 优雅退出时结算任务全部排空，无漏账
4. 内存水位超阈值时新请求被拒（503 + `Retry-After`，错误体符合入站协议格式），**在途请求不受影响**
5. `cargo check` 通过全部 trait 定义，类型签名中无 `todo!()` 残留

---

## 9. SIMPLIFIED 登记

以下为 M0 的最简实现，均须标注 `// SIMPLIFIED(M0):`：

| 项 | M0 实现 | 完整实现 |
|---|---|---|
| `Coordinator` | 仅 `pg`，`chain` 长度恒为 1 | M1 |
| 租约与双模式 | 无，逐请求走 PG | M1 |
| `PriceEngine` | 单价 × 用量向量，无阶梯与时段；预扣按固定上限估算 | M5 |
| 组织树 | Root + 单个 Personal 节点 + 单个 Account | M8 |
| `ProviderRegistry` | 硬编码单厂商，不读 YAML | M2 |
| 句柄与异步 | 无 | M3 |
| 转换适配器 | 无，仅透传 | M7 |
| `LogSink` | 仅 `postgres` 实现 | M9 |
| Redis | 无 | M1 |
| `RoutingPolicy` | 单渠道直连，无过滤与打分 | M3 |
| `ArchiveSink` | 仅定义接口，不实现 | M6 |
| `LoadProbe` | 并发预算 + cgroup 内存水位；CPU 与调度延迟留空 | M1 |

---

## 10. 待确认

1. M0 的上游按 OpenAI 兼容协议的官方文档实现，不预先绑定具体厂商。实现完成后向用户索取真实的请求与响应样本用于核对——尤其是 SSE 的帧格式与 usage 字段位置，文档与实际返回常有出入。
2. ~~结算任务的背压策略~~ — **已明确**。M0 不会积压：结算任务的产生速率等于请求完成速率，消费速率受限于 PG 写入，而每个请求在准入时本已做过一次 `hold`（同为 PG 写），两者同数量级；PG 变慢时 `hold` 一并变慢、请求被拒，系统自限。

   **M1 起需要处理**：配额租约使 `hold` 走本地内存不再碰 PG，而 `capture` 仍须写 PG，产生与消费速率就此脱钩。届时的处理是——暴露 `settle_queue_depth` 指标并告警，设上限作为最后保护，触发时阻塞准入而非丢弃（丢弃的是钱）。此项列入 M1 交付。
