# AI 网关：身份、组织与计费主体模型

- **日期**：2026-08-22
- **状态**：已确认
- **范围**：组织树、计费主体与账户链、嵌套额度、冻结机制、目录同步、第三方登录
- **前置**：[技术选型与工程基线](2026-08-22-gateway-tech-stack-design.md)
- **不含**：价格规则引擎、用量提取、协议转换（见后续 spec）

---

## 1. 设计驱动力

三条需求共同决定了本模型的形状：

1. **部门数据从主流 OA 平台同步**——钉钉、飞书、企业微信、Microsoft Entra ID、Okta、Google Workspace。这些平台的部门结构是**任意深度的树**，因此固定层级（组织/项目/用户）的模型不成立。
2. **账户登录支持国内外主流第三方平台**。登录身份必须能与目录同步来的成员记录关联。
3. **一套代码同时服务开源中转站与企业内部平台**两种形态。

---

## 2. 组织树

### 2.1 统一一棵树，节点带类型

不采用「部门树 + 项目层」两套结构——那会导致账户挂载、额度解析、报表聚合三处逻辑各写两遍。改为单棵树，用节点类型区分语义：

```rust
pub struct OrgNode {
    pub id: NodeId,                     // bigint，见 §2.3
    pub parent: Option<NodeId>,
    pub path: LTree,                    // 物化路径，派生列
    pub kind: NodeKind,
    pub source: NodeSource,
    pub external: Option<ExternalRef>,  // (provider, external_id)
}

pub enum NodeKind   { Root, Department, Project, Personal }
pub enum NodeSource { Synced, Local }
```

| 节点类型 | 来源 | 用途 |
|---|---|---|
| `Root` | Local | 每个部署一个根节点 |
| `Department` | Synced 或 Local | 组织架构，通常由 OA 同步 |
| `Project` | Local | 网关内自建，成本归属单元 |
| `Personal` | Local | 个人空间；开源中转站形态下每用户一个 |

### 2.2 同步边界（硬约束）

**目录同步器只写 `source = Synced` 的节点，绝不触碰 `Local` 节点。**

这条边界必须在代码层面强制，而非靠约定。理由：一次全量同步若误删本地自建的 Project 子树，用户的成本归属配置与历史账单关联即全部丢失，且不可逆。同步逻辑的差分计算范围必须显式限定在 `source = Synced` 的集合内。

### 2.3 存储：ltree

采用 PostgreSQL 的 `ltree` 扩展。`parent_id` 为真相源（OA 同步只提供父子关系，写入最简单），`path` 为派生的物化路径列。

```sql
CREATE EXTENSION IF NOT EXISTS ltree;

CREATE TABLE org_node (
    id          BIGSERIAL PRIMARY KEY,
    parent_id   BIGINT REFERENCES org_node(id),
    path        LTREE NOT NULL,
    kind        SMALLINT NOT NULL,
    source      SMALLINT NOT NULL,
    ...
);
CREATE INDEX org_node_path_gist ON org_node USING GIST (path);
```

祖先/后代查询为原生操作：

```sql
-- 某节点及其全部子孙
WHERE path <@ '1.5.23'
-- 某节点的全部祖先
WHERE path @> '1.5.23.47'
```

**实现约束（易踩的坑）**：`ltree` 的 label 仅允许 `[A-Za-z0-9_]`，**不允许连字符**。因此组织节点主键必须使用 `BIGSERIAL` 而非 UUID——UUID 的文本形式含连字符，无法直接作为 ltree label。若需对外暴露不可枚举的标识，另设 UUID 列，路径仅用内部 bigint。

**回退方案**：若目标环境的托管 PostgreSQL 不允许安装 contrib 扩展，回退为 `TEXT` 物化路径（`/1/5/23/`）+ B-tree 索引 + `LIKE '/1/5/%'` 前缀查询。语义等价，仅索引效率与写法差异。此回退需在 `store` crate 中以 feature 隔离。

### 2.4 同步字段与本地覆盖层

同步来的字段与用户的本地修改**分列存储，原始值永久保留**：

| 字段类别 | 写入方 | 示例 |
|---|---|---|
| 同步字段 | 仅同步器 | `name`、`parent_id`、`external_id`、`source` |
| 覆盖层 | 仅管理员 | `name_override`、`note`、`cost_center`、本地标签 |

显示规则：`name_override` 优先，为空则取 `name`。两者始终可分别查询，因此支持「原始值 / 本地值」对比视图，也能在 OA 侧改名后识别出差异。

**全量同步只覆盖同步字段，覆盖层不受影响。** 这是「部分字段可编辑」需求的实现方式——用户从不直接修改同步字段本身。

---

## 3. 计费主体与账户链

### 3.1 Account 是独立实体，可挂在树的任意节点

`Account` 不是层级中的一层，而是可绑定到任意 `OrgNode` 的独立实体。这使同一套代码覆盖两种部署形态：

```
Root
└── 技术中心            [Account A]
    ├── 基础架构部                     → 计费落到 A
    │   └── proj-gateway (Project)     → 计费落到 A
    └── 算法部          [Account B]    → 计费落到 B（更近的祖先）
        └── proj-rag                   → 计费落到 B
```

| 部署形态 | Account 绑定位置 | 效果 |
|---|---|---|
| 开源中转站 | 每个 `Personal` 节点各挂一个 | 等价于「每用户一个钱包」 |
| 企业内部 | 挂在部门节点 | 部门共享预算池 |

### 3.2 账户链解析

一次请求的**账户链** = 从 ApiKey 所属节点向根遍历，途经的全部挂有 Account 的节点，**按由近及远排序**。

链首（最近祖先）为**主计费主体**，其余为上级额度约束点。嵌套额度的实现见 §4。

### 3.3 热路径不遍历树

组织树变更频率为天级，请求频率为毫秒级。因此在 `api_key` 记录上物化 `account_chain`（有序的 `AccountId` 数组），组织树或 Account 绑定关系变更时批量重算受影响子树。

热路径只做一次主键查询，不触碰 `org_node` 表。

### 3.4 跨部门项目：不做实时分摊

跨部门项目**功能上完整支持**——成本可见、可按任意维度归集、可结算——但**不在账本热路径做按比例分摊**。

做法：钱始终从单一 account 出（走账户链），每条账单携带完整归属标签：

```
(org_path, project_id, user_id, key_id, model, channel, tier)
```

需要按比例分摊时在报表层计算，月末生成内部结算单，由一个 account 向另一个 account 转账。这也是企业财务的标准处理方式——实时分摊反而无法与其账期对齐。

**否决实时分摊的理由**：嵌套额度的账户链是确定且有序的（从叶到根唯一路径），可按 `AccountId` 排序加锁避免死锁，回滚路径确定；而分摊的账户集合是任意的、比例可变，一次请求需拆成 N 份分别冻结，任一份失败需全部回滚，且比例调整后历史账单的重算规则不可定义。将其置于热路径会使 `ledger` 成为系统中最脆弱的部分。

---

## 4. 嵌套额度

**第一版即完整实现**，不做降级。

### 4.1 接口

`Coordinator` 接受完整账户链，而非单个账户：

```rust
async fn hold(
    &self,
    chain: &[AccountId],   // 由近及远，链首为主计费主体
    amt: Money,
    ttl: Duration,
    idem: &str,
) -> Result<HoldId>;
```

### 4.2 正确性要求

| 要求 | 做法 |
|---|---|
| 原子性 | 链上全部账户在**同一个 PostgreSQL 事务**内冻结，全成或全败 |
| 死锁避免 | 加锁前将 `chain` 按 `AccountId` **升序排序**，全局统一顺序 |
| 幂等 | `hold` 表 `idempotency_key` 唯一索引，冲突即返回既有 `HoldId` |
| 回滚 | 事务回滚天然覆盖；无需手写补偿逻辑 |

排序加锁与单事务是嵌套额度可行的前提。这也是冻结记录必须落库的直接原因——见 §5.1。

### 4.3 验证策略

此模块的并发正确性不能依赖压测：

- `loom`：对多级冻结的并发交错做模型穷举
- `proptest`：断言任意操作序列后的不变量——`held == SUM(该账户上全部活跃 Hold)`，且 `balance` 恒等于初始值加全部已 Capture 金额之和

---

## 5. 冻结机制

### 5.1 语义：Hold 不改动余额

冻结的唯一目的是**防止用户在低余额时通过高并发请求超支**。

```
balance   -- 实际余额，仅 Capture 时变动
held      -- 冻结总额，Hold 增加、Capture/Void 减少
available = balance - held
```

Hold **不减少 `balance`**，钱在结算前分文未动。

**冻结记录落 PostgreSQL。** 曾评估「仅存 Redis 或进程内存」，否决理由：

- Redis 重启、节点崩溃、网络分区都会导致活跃 Hold 丢失，随后并发请求看到未被冻结的余额，可无限透支——这正是本项目要解决的原始痛点之一
- 与 §4 的嵌套额度直接冲突：多级原子冻结在 PostgreSQL 中是单事务问题，在 Redis 中需 Lua 脚本且无持久化保证，在进程内存中无法跨节点协调。**功能要求越强，越依赖事务**
- 与已确定的架构冲突：PostgreSQL 为默认 Coordinator、无 Redis 亦可集群、Redis 仅为可选加速器

性能顾虑由 §5.3 的配额租约解决，无需牺牲持久性。

### 5.2 PostgreSQL 原子冻结

单条语句完成检查与冻结，绝不 `SELECT` 后再 `UPDATE`：

```sql
UPDATE account_balance
   SET held = held + $2, updated_at = now()
 WHERE account_id = $1
   AND balance - held >= $2
RETURNING balance - held AS available;
```

返回零行即余额不足。此写法依赖 PostgreSQL 行锁在 read-committed 下的重新求值语义，天然并发安全。

热点账户按 `shard` 分片（`PRIMARY KEY (account_id, shard)`），写入随机落片、读取时 SUM，将单账户吞吐从约 500 TPS 提升至约 8000 TPS；冻结失败时轮询下一分片重试。

### 5.3 性能：配额租约 + 双模式切换

节点向 PostgreSQL 一次性申请一笔额度，随后在进程内以原子操作扣减，用完再续。约 1000 次请求塌缩为 1 次数据库写入。

```rust
if available < threshold {          // threshold = 单次最大预扣 × 10
    self.strict_hold(chain, ...)    // 逐请求走 PG，精确
} else {
    self.lease_hold(chain, ...)     // 走本地租约，快
}
```

**余额充足时走租约**，超发风险上限为 `租约额度 × 节点数`，可控且用户不会真正欠款；**余额见底时自动切严格模式**，保证不透支。这正好对应「冻结用于防止低余额时高并发超支」的设计目的——真正需要精确的区间自动获得精确保证。

`quota_lease` 表记录 `lease_id / account_id / node_id / amount / consumed / expires_at`。申请租约 = 一次 §5.2 的原子冻结 + 插入租约行；释放或过期时将 `amount - consumed` 退回 `held`。

### 5.4 Redis 作为加速器

Redis 承担热余额缓存与跨节点 Hold 汇总，降低 PostgreSQL 读压力。**真相源永远是 PostgreSQL。**

Redis 调用全部包裹熔断器，故障时降级到 PostgreSQL 路径，**不 fail-closed 拒绝服务**。此降级行为必须在 `redisCoordinator` 中显式实现并测试，不能依赖超时自然回落。

### 5.5 防止死冻结

冻结泄漏会永久锁死用户余额，是本类系统最典型的事故。采用七道防线：

| # | 防线 | 说明 |
|---|---|---|
| 1 | **TTL 强制** | 每个 Hold 落库时必须带 `expires_at`，无 TTL 的 Hold 不允许创建 |
| 2 | **回收器** | 后台任务扫描 `expires_at < now()` 的 Hold，Void 并释放；由 `pg_advisory_lock` 保证单实例执行 |
| 3 | **租约归还** | 节点优雅退出时主动归还未消费额度；崩溃场景由 TTL 兜底 |
| 4 | **类型层强制** | `Hold` 标记 `#[must_use]`；`Drop` 中若未消费则记录指标、告警，并向回收队列发送 best-effort 撤销。**不在 `Drop` 中 panic**——`Drop` 内 panic 在已 panicking 时会 abort，且 async 任务被取消时正走此路径 |
| 5 | **异步任务** | Hold 挂在任务对象而非请求上；任务终态或超时才释放；孤儿任务巡检覆盖「用户提交后再不查询」的情形 |
| 6 | **Duplex 会话** | 会话级 Hold 随会话滚动，连接断开必须触发释放；节点崩溃由 TTL 兜底 |
| 7 | **对账与可观测** | 定期校验不变量 `held == SUM(活跃 Hold)`，不一致即告警；暴露 `active_holds`、`hold_age_p99`、`expired_holds_reclaimed` 指标 |

第 4 道是 Rust 相较 Go 的实质优势——「忘记释放 Hold」从依赖 code review 变为编译期与运行时可强制。

### 5.6 账本记录规则

- 所有账本操作携带**幂等键**
- 账本 **append-only**，余额为物化视图
- 每笔账单记录命中的价格规则 ID、各维度用量、单价，以支持 explain

---

## 6. 目录同步

### 6.1 抽象

```rust
trait DirectorySource {
    async fn full_sync(&self) -> Result<DirectorySnapshot>;
    async fn apply_event(&self, ev: DirectoryEvent) -> Result<()>;
}
```

两种模式归一到 `DirectorySnapshot` 的差分应用逻辑，同步正确性只需验证一遍。

### 6.2 实现优先级

| 优先级 | 来源 | 模式 | 覆盖范围 |
|---|---|---|---|
| 1 | **SCIM 2.0 server** | push（IdP 主动调用） | Microsoft Entra ID、Okta、Google Workspace、OneLogin |
| 2 | **钉钉连接器** | pull + 事件回调 | 钉钉 |
| 3 | **飞书连接器** | pull + 事件回调 | 飞书 |
| 4 | 企业微信连接器 | pull + 事件回调 | 企业微信 |

SCIM 优先的理由是投入产出比：一份实现覆盖多个国外主流平台，且 push 模式比轮询更实时。钉钉与飞书不支持 SCIM，须各自实现。

### 6.3 差分应用约束

- 作用域严格限定 `source = Synced` 的节点（§2.2）
- 仅覆盖同步字段，覆盖层不受影响（§2.4）
- 节点删除采用软删除，保留历史账单的归属引用完整性

---

## 7. 认证与第三方登录

### 7.1 两类实现

| 类型 | 平台 | crate |
|---|---|---|
| 标准 OIDC | Microsoft Entra ID、Google、Okta、Auth0、Keycloak | `openidconnect` |
| 非标 OAuth2 | 钉钉、飞书、企业微信、微信开放平台、GitHub、GitLab、Gitee、Apple | `oauth2` + 各平台 profile 映射适配器 |

标准 OIDC 共用一套代码，仅配置不同；非标 OAuth2 每家需要一个 profile 映射适配器，将其用户信息端点的响应归一为内部 `ExternalIdentity`。

### 7.2 身份绑定

登录身份必须能关联到目录同步来的成员记录：

```sql
CREATE TABLE identity_binding (
    user_id     BIGINT NOT NULL REFERENCES app_user(id),
    provider    TEXT   NOT NULL,
    external_id TEXT   NOT NULL,
    UNIQUE (provider, external_id)
);
```

一个 User 可绑定多个外部身份——例如同一人的飞书登录与 GitHub 登录指向同一账号。首次通过某平台登录时，若该 `(provider, external_id)` 已由目录同步创建了成员记录，则自动绑定；否则按配置决定是拒绝登录还是创建 `Personal` 节点。

### 7.3 API Key 归属

API Key 必须绑定到一个 `OrgNode`，同时记录创建者 User。成本归属由此永远明确。

---

## 8. 待定事项

1. **首次登录的默认行为**：目录中不存在的外部身份，是拒绝登录、还是自动创建 `Personal` 节点。两种形态（开源中转站需要自助注册、企业内部通常需要白名单）诉求相反，需设计为部署配置项，但默认值待定。
2. **软删除节点的账单归属展示**：部门在 OA 侧被删除后，其历史账单在报表中如何呈现——保留原路径、还是归入「已解散」虚拟节点。
3. **`Metered` 与 `Session` 计费的 Hold 续期粒度**：按固定周期续期还是按用量阈值触发，影响长会话与长期缓存的冻结精度。
