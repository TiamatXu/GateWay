# M1 账本范围重定义：从「性能完整」到「正确性完整」

- **日期**：2026-08-26
- **状态**：已确认
- **前置**：[里程碑路线图](2026-08-22-milestones-roadmap.md) §M1、[身份组织与计费主体模型](2026-08-22-identity-org-billing-model-design.md) §5
- **效果**：本文修订路线图 M1 的交付清单与验收标准，路线图对应小节已同步

---

## 1. 触发

M1 原交付清单里有四项性能优化：配额租约、双模式切换、热点账户分片、Redis Coordinator。开工前对三个同类开源项目做了实现层面的调研，结论是这四项在当前阶段全部属于**为未经测量的问题预付复杂度**，应从 M1 移出。

调研同时确认了另一件事：账本的**正确性**部分（Hold 落库、多级链单事务、TTL 回收、不变量对账）不但不过度，反而是三个项目共同的痛点所在。这部分一条不砍。

---

## 2. 调研：同类项目实际怎么做

读的是代码，不是文档。三个项目分别代表三种计费模型。

### 2.1 new-api — 预付费直扣，无冻结

`model/quota_reserve.go`、`service/funding_source.go`

没有冻结概念。预扣就是从 `users.quota` 里直接扣掉：

```go
DB.Model(&User{}).Where("id = ? AND quota >= ?", id, quota).
   Update("quota", gorm.Expr("quota - ?", quota))
```

结算时按 `delta = 实际 - 预扣` 补扣或退还。Redis 开启时预扣走 Lua 脚本原子 `HINCRBY`，PostgreSQL 异步跟随；`BatchUpdateEnabled` 模式下 delta 先攒在进程内存，定期批量刷库。

代价写在它自己的注释里：

```go
// IncreaseUserQuota 是 quota += N 的非幂等操作，不能重试，否则会多退额度。
```

**进程崩在预扣与结算之间，那笔钱永久损失**——没有 TTL、没有回收器、没有对账。批量模式下崩溃丢失的是已经落到内存队列里的真实账单。

### 2.2 LiteLLM — 后付费累加 + Redis 预留

`litellm/proxy/spend_tracking/budget_reservation.py`（1416 行）

有预留机制，但预留只是 Redis counter 上的一次 `INCRBY`，不落库、不可审计。请求结束时 `reconcile_budget_reservation` 把预留额改写成实际花费。泄漏兜底靠 counter 自身的 TTL。

多级实体链是有的：key → team → team_member → user → end_user → org → tag，逐级独立 counter，**非原子**。链中某级失败后前几级的回退由 `_release_applied_entries_best_effort` 负责——函数名里的 `best_effort` 说明了它的可靠性等级。

该模块内有一条与本项目 M0 验收标准第 2 条完全一致的洞察，来自 `release_budget_reservation_on_cancel` 的注释：

> Reconcile to the request's input-token cost rather than refunding to zero: by the time a request is cancelled in-flight the provider call was already dispatched, so the input tokens were billed even if no chunk reached the client. Refunding to zero would let a caller abort pre-token to dodge that charge.

Redis 不可用时的行为由 `fail_closed_budget_enforcement` 开关决定：要么拒绝请求，要么放行不计费。

### 2.3 TokenHub — 配额约束 + 事后对账

`backend/internal/server/store_call_admission.go`

不做预付费账本。`SELECT FOR UPDATE` 锁 api_key 行，检查分钟/日/月的请求数、token 数、成本上限，通过即放行。代码中的 `lease` 指**并发槽位租约**，与额度无关。真实成本由 `internal/reconciliation/` 与阿里云、new-api、one-api 的外部账单对齐。

### 2.4 对照

| | LiteLLM | new-api | TokenHub | GateWay |
|---|---|---|---|---|
| 计费模型 | 后付费累加 + Redis 预留 | 预付费直扣 | 配额约束 + 事后对账 | 预付费 Hold/Capture |
| 预扣持久化 | 仅 Redis | 扣了就没了 | 不适用 | 落 PostgreSQL |
| 崩溃后果 | TTL 前额度被卡 | **钱永久损失** | 不适用 | TTL 回收 |
| 多级账户链 | 有，逐级非原子 | 无，单级 | 有，逐级计数器 | 有，单事务原子 |
| 内部不变量对账 | 无 | 无 | 只对外部账单 | 有 |
| 额度租约 | 无 | BatchUpdate（牺牲正确性） | 无 | 否决，见 §3 |
| 热点分片 | 无 | 无 | 无 | 否决，见 §3 |
| 并发模型验证 | 无 | 无 | 无 | proptest |

---

## 3. 否决项与理由

判据一条：**先信任 PostgreSQL，不为未经测量的性能问题预付复杂度。**

### 3.1 配额租约 —— 否决，移出 M1

三个项目都没有实现真正的额度租约。new-api 的 `BatchUpdate` 是最接近的等价物，但它是「攒着不写库」，以正确性换性能，与本项目「钱必须落库」的立场正相反。

更要紧的是收益本身被高估了。原设计称租约能把「约 1000 次请求塌缩为 1 次数据库写入」，该数字只统计了 Hold。实际每个请求有两次 PG 写：

- Hold（预扣）—— 租约能省掉
- Capture（结算落账）—— **省不掉**，`ledger_entry` 必须实时 append，§5.1 已明确否决账本只存 Redis 或内存

而 Capture 同样要 `UPDATE account_balance`，抢的是同一行的行锁。所以租约省掉的是 50% 的写次数，**行锁竞争基本没有消除**，单账户 TPS 上限不会因此从约 500 跳到约 8000。

租约还引入一项真实成本：节点被 SIGKILL 或断电时，租约中未消费的部分在 PostgreSQL 里仍处于冻结态，用户可用余额凭空减少「租约余额 × 崩溃节点数」，直到 TTL 到期才恢复。

**重新引入的条件**：实测确认单账户 Hold 路径的行锁竞争是瓶颈，且 Capture 路径已另有优化。

### 3.2 双模式切换 —— 否决，随租约移出

`available < threshold` 走严格模式、否则走租约，是租约的附属机制。租约不做，它不存在。

### 3.3 热点账户分片 —— 否决，推迟至 M8

三个项目都没有分片，且都在真实生产环境运行。原因不难推断：单个用户账户很难打满 500 TPS，热点只可能出现在**企业根账户**上，而多级账户链本身是 M8 的场景。

`account_balance` 的 `PRIMARY KEY (account_id, shard)` 与 `hold_leg.shard` 列已按最终形态建好，`SHARD` 常量当前恒为 0。届时实现只需改动 `reserve` 与 `release_legs` 两个函数的选片逻辑，成本不会因为推迟而增加。

分片的代价是每次读余额都要 `SUM` 全部分片，且冻结失败要轮询下一分片重试——在没有热点账户的阶段，这是纯粹的复杂度。

**重新引入的条件**：出现真实的企业级根账户，且实测其行锁竞争成为瓶颈。

### 3.4 Redis Coordinator —— 否决，移出 M1

设计文档 §5.4 给 Redis 的定位是「热余额缓存与跨节点 Hold 汇总，降低 PostgreSQL 读压力」的**可选加速器**，并明确「真相源永远是 PostgreSQL」。它与前三项属于同一类：解决的是一个尚未测量到的读压力问题。

代价不小：熔断器、显式降级路径、缓存与真相源之间的一致性窗口，以及一整套「降级行为必须被测试覆盖」的要求。

**重新引入的条件**：实测确认余额读取是瓶颈。届时按原设计实现，含熔断包裹与降级到 PG 的显式路径。

`Coordinator` trait 不因此改动——三实现的接口契约在 M0 已定死，Redis 实现是补一个 impl，不是改接口。

### 3.5 `loom` 并发穷举 —— 验收标准改写

原验收标准 1：「`loom` 对多级冻结的并发交错做模型穷举」。

这条标准指向了一个 `loom` 测不到的东西。多级冻结的并发正确性**在 PostgreSQL 的行锁与事务隔离语义里，不在 Rust 的内存模型里**。`loom` 穷举的是原子操作与内存序的交错，它无法模拟 PostgreSQL 事务。

`loom` 真正的用武之地是租约的进程内 `AtomicI64` 扣减——该项已否决，`loom` 随之失去对象。

**改写为**：高并发压力测试，在真实 PostgreSQL 上并发执行随机操作序列，结束后断言 §5 的三条不变量。

---

## 4. M1 修订后的交付清单

保留原清单中的全部正确性项，新增两项调研中暴露的缺口。

### 已完成（M1 前期）

- 嵌套额度多级冻结：单事务原子性、按 `AccountId` 升序加锁、跨级回滚、上级额度阻断
- `Coordinator` 一致性测试套件，`pg` 与 `mem` 两实现共用
- `proptest` 不变量测试（针对 `MemCoordinator`）
- 对账 API `audit()`——第 7 道防线的前半
- 防死冻结第 1 道（TTL 强制）、第 2 道（回收器）、第 4 道（类型层强制）

### 待完成

1. **回收器单实例化**。当前 `spawn_reclaimer` 无互斥，多节点部署会重复扫描同一批过期 Hold。按原设计以 `pg_advisory_lock` 保证单实例执行——这正是 `Coordinator::try_lock` 的首个使用方。
2. **`try_lock` 与 `rate_allow` 落地**。两者在 M0 只有文档定义。`try_lock` 由上一条驱动；`rate_allow` 供按 Key 限流使用，同时补上 `AdmissionGate::check` 的限流参数（M0 偏离清单中登记的遗留项）。
3. **不变量 proptest 覆盖 `PgCoordinator`**。当前只跑 `MemCoordinator`，而真实风险在 SQL 实现里。PG 版用例数调低以控制耗时。
4. **高并发压力测试**（替代 `loom`，见 §3.5）。
5. **混沌测试**：进程在 Hold 与 Capture 之间被强杀，回收器完整释放，无余额泄漏。
6. **`Session` 与 `Metered` 的 Hold 生命周期语义**。底层 `extend` / `capture_partial` 已具备，缺语义层：滚动 Hold 的续期策略、会话断开的释放路径。供 M3 / M4 使用。
7. **指标暴露**（第 7 道防线的后半）：`active_holds`、`hold_age_p99`、`expired_holds_reclaimed`。当前仅有 `ledger.hold_leaked`。

### 防死冻结七道防线的 M1 收口状态

| # | 防线 | M1 结束时 |
|---|---|---|
| 1 | TTL 强制 | 已完成 |
| 2 | 回收器 | 补单实例化后完成 |
| 3 | 租约归还 | **不适用**——租约已否决，无租约可归还 |
| 4 | 类型层强制 | 已完成 |
| 5 | 异步任务 Hold | M3，使用方在那里 |
| 6 | Duplex 会话 Hold | M4，使用方在那里 |
| 7 | 对账与可观测 | 对账已完成，补指标后完成 |

---

## 5. 修订后的验收标准

1. 高并发压力测试：真实 PostgreSQL 上并发执行随机操作序列，结束后 `held == SUM(活跃 Hold)`、`balance` 等于初始值减去全部已 Capture 之和、`held >= 0`
2. `proptest` 在 `mem` 与 `pg` 两实现上断言同一组不变量
3. 混沌测试：进程在 Hold 与 Capture 之间被强杀，回收器完整释放，无余额泄漏
4. ~~Redis 主动断开时服务不中断~~ —— 随 Redis 移出 M1

---

## 6. 对后续里程碑的影响

- **M8**（组织与身份）新增：热点账户分片实现，触发条件见 §3.3
- **性能优化统一后置**：租约、双模式、Redis 均转为「由实测触发」的待办，不绑定具体里程碑。触发条件已分别写明。
- 三项否决均**不改动任何接口契约**，`Coordinator` trait 与数据库 schema 保持 M0 定死的最终形态。重新引入时是补实现，不是改接口——这正是「接口一次定死」这条硬规矩要保护的场景。
