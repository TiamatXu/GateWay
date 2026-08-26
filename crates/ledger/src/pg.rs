use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use gw_core::{AccountId, BillingTiming, HoldId, Money, RateKey};
use smallvec::SmallVec;
use sqlx::{PgPool, Postgres, Transaction};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::lock::{LockGuard, advisory_key};
use crate::rate::RateLimiter;
use crate::{Hold, HoldRequest, LedgerError};

// hold.status
const ACTIVE: i16 = 0;
const CAPTURED: i16 = 1;
const VOIDED: i16 = 2;
const EXPIRED: i16 = 3;

// ledger_entry.kind
const ENTRY_HOLD: i16 = 0;
const ENTRY_CAPTURE: i16 = 1;
const ENTRY_VOID: i16 = 2;
const ENTRY_EXPIRE: i16 = 5;

// SIMPLIFIED(M0): 热点账户分片恒为 0，M1 起按账户热度分片
const SHARD: i16 = 0;

#[async_trait]
pub trait Coordinator: Send + Sync {
    async fn hold(&self, req: HoldRequest<'_>) -> Result<Hold, LedgerError>;
    /// 关闭 Hold：按 `actual` 扣款，释放剩余冻结。
    async fn capture(&self, hold: Hold, actual: Money) -> Result<(), LedgerError>;
    async fn void(&self, hold: Hold) -> Result<(), LedgerError>;
    /// 追加冻结额度，用于 `Metered` / `Session` 的滚动场景。
    async fn extend(&self, hold: &Hold, delta: Money) -> Result<(), LedgerError>;
    /// 分段扣款但不关闭 Hold：扣 `amount`，冻结额同步减少。
    async fn capture_partial(&self, hold: &Hold, amount: Money) -> Result<(), LedgerError>;

    /// 按 id 结算。句柄已丢失时的兜底路径（泄漏回收、崩溃恢复）。
    async fn capture_by_id(&self, id: HoldId, actual: Money) -> Result<(), LedgerError>;
    async fn void_by_id(&self, id: HoldId) -> Result<(), LedgerError>;

    /// TTL 兜底回收：释放已过期仍未结算的 Hold，返回回收数量。
    async fn reclaim_expired(&self, limit: i64) -> Result<u64, LedgerError>;

    /// 账户当前余额与冻结额，用于对账。
    async fn balances(&self, account: gw_core::AccountId) -> Result<(Money, Money), LedgerError>;

    /// 对账：返回 `held` 与活跃 Hold 腿之和不一致的账户。
    /// 这是防死冻结的第七道防线——不一致即告警。
    async fn audit(&self, limit: i64) -> Result<Vec<AuditMismatch>, LedgerError>;

    /// 活跃 Hold 的数量与年龄分布。第七道防线的可观测部分。
    async fn hold_stats(&self) -> Result<HoldStats, LedgerError>;

    /// 取一个令牌。`rate` 为每秒补充数，`burst` 为桶容量，任一为 0 即不限流。
    async fn rate_allow(&self, key: &RateKey, rate: u32, burst: u32)
    -> Result<bool, LedgerError>;

    /// 尝试取得分布式互斥，`None` 表示已被他人持有。释放靠 `LockGuard` 析构。
    ///
    /// `ttl` 是兜底释放时限，防止持有者崩溃后锁永久滞留。`pg` 实现用
    /// `pg_try_advisory_lock`，锁随连接生命周期释放——比 TTL 更及时，
    /// 故忽略该参数。
    async fn try_lock(
        &self,
        key: &str,
        ttl: Duration,
    ) -> Result<Option<LockGuard>, LedgerError>;
}

/// 活跃 Hold 的概览。年龄持续走高意味着有 Hold 迟迟不结算，是死冻结的前兆。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HoldStats {
    pub active: i64,
    /// 最老的 1% 活跃 Hold 已存在多久
    pub age_p99: Duration,
}

/// 一处账实不符。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditMismatch {
    pub account: AccountId,
    /// `account_balance.held` 记录的值
    pub held: Money,
    /// 活跃 Hold 腿的实际之和
    pub active_legs: Money,
}

pub struct PgCoordinator {
    pool: PgPool,
    reclaimer: mpsc::Sender<HoldId>,
    rate: RateLimiter,
}

impl PgCoordinator {
    #[must_use]
    pub fn new(pool: PgPool, reclaimer: mpsc::Sender<HoldId>) -> Self {
        Self {
            pool,
            reclaimer,
            rate: RateLimiter::default(),
        }
    }

    async fn do_reclaim_expired(&self, limit: i64) -> Result<u64, LedgerError> {
        let expired: Vec<Uuid> = sqlx::query_scalar!(
            r#"UPDATE hold SET status = $1, settled_at = now()
                WHERE id IN (
                    SELECT id FROM hold
                     WHERE status = $2 AND expires_at <= now()
                     ORDER BY expires_at
                     LIMIT $3
                     FOR UPDATE SKIP LOCKED)
               RETURNING id"#,
            EXPIRED,
            ACTIVE,
            limit
        )
        .fetch_all(&self.pool)
        .await?;

        let mut n = 0;
        for id in expired {
            let mut tx = self.pool.begin().await?;
            release_legs(&mut tx, id, 0, ENTRY_EXPIRE).await?;
            tx.commit().await?;
            n += 1;
        }
        if n > 0 {
            metrics::counter!("ledger.expired_holds_reclaimed").increment(n);
        }
        Ok(n)
    }

    /// 关闭 Hold 并按 `charge` 扣款。`charge` 为 0 即纯释放。
    async fn settle(
        &self,
        id: HoldId,
        status: i16,
        kind: i16,
        charge: i64,
    ) -> Result<(), LedgerError> {
        let mut tx = self.pool.begin().await?;
        let closed = sqlx::query_scalar!(
            r#"UPDATE hold SET status = $2, settled_at = now()
                WHERE id = $1 AND status = $3
               RETURNING amount"#,
            id.0,
            status,
            ACTIVE
        )
        .fetch_optional(&mut *tx)
        .await?;

        if closed.is_none() {
            tx.rollback().await?;
            return Err(LedgerError::HoldNotActive);
        }

        release_legs(&mut tx, id.0, charge, kind).await?;
        tx.commit().await?;
        Ok(())
    }
}

/// 释放该 Hold 全部腿的冻结额，并按 `charge` 扣减余额。
async fn release_legs(
    tx: &mut Transaction<'_, Postgres>,
    hold_id: Uuid,
    charge: i64,
    kind: i16,
) -> Result<(), LedgerError> {
    // 按 account_id 排序取腿，与 hold 的加锁顺序一致，避免死锁
    let legs = sqlx::query!(
        r#"SELECT account_id, shard, amount FROM hold_leg
            WHERE hold_id = $1 ORDER BY account_id"#,
        hold_id
    )
    .fetch_all(&mut **tx)
    .await?;

    for leg in legs {
        sqlx::query!(
            r#"UPDATE account_balance
                  SET held = held - $3, balance = balance - $4, updated_at = now()
                WHERE account_id = $1 AND shard = $2"#,
            leg.account_id,
            leg.shard,
            leg.amount,
            charge
        )
        .execute(&mut **tx)
        .await?;

        sqlx::query!(
            r#"INSERT INTO ledger_entry (account_id, kind, amount, hold_id)
               VALUES ($1, $2, $3, $4)"#,
            leg.account_id,
            kind,
            charge,
            hold_id
        )
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

/// 冻结指定金额，可用额度不足时返回 `InsufficientFunds`。
async fn reserve(
    tx: &mut Transaction<'_, Postgres>,
    account_id: i64,
    amount: i64,
) -> Result<(), LedgerError> {
    let ok = sqlx::query_scalar!(
        r#"UPDATE account_balance SET held = held + $3, updated_at = now()
            WHERE account_id = $1 AND shard = $2 AND balance - held >= $3
           RETURNING balance - held AS "available!""#,
        account_id,
        SHARD,
        amount
    )
    .fetch_optional(&mut **tx)
    .await?;

    if ok.is_some() {
        return Ok(());
    }

    let available = sqlx::query_scalar!(
        r#"SELECT balance - held AS "available!" FROM account_balance
            WHERE account_id = $1 AND shard = $2"#,
        account_id,
        SHARD
    )
    .fetch_optional(&mut **tx)
    .await?;

    Err(LedgerError::InsufficientFunds {
        account: AccountId(account_id),
        required: Money::from_nanos(amount),
        available: Money::from_nanos(available.unwrap_or(0)),
    })
}

fn timing_code(t: BillingTiming) -> i16 {
    match t {
        BillingTiming::InRequest => 0,
        BillingTiming::OnTerminal => 1,
        BillingTiming::Metered => 2,
        BillingTiming::Session => 3,
        BillingTiming::NotBilled => 4,
    }
}

#[async_trait]
impl Coordinator for PgCoordinator {
    async fn capture_by_id(&self, id: HoldId, actual: Money) -> Result<(), LedgerError> {
        self.settle(id, CAPTURED, ENTRY_CAPTURE, actual.as_nanos())
            .await
    }

    async fn void_by_id(&self, id: HoldId) -> Result<(), LedgerError> {
        self.settle(id, VOIDED, ENTRY_VOID, 0).await
    }

    async fn reclaim_expired(&self, limit: i64) -> Result<u64, LedgerError> {
        self.do_reclaim_expired(limit).await
    }

    async fn audit(&self, limit: i64) -> Result<Vec<AuditMismatch>, LedgerError> {
        let rows = sqlx::query!(
            r#"SELECT b.account_id,
                      b.held,
                      COALESCE(SUM(l.amount) FILTER (WHERE h.status = 0), 0)::BIGINT
                        AS "active_legs!"
                 FROM account_balance b
                 LEFT JOIN hold_leg l ON l.account_id = b.account_id AND l.shard = b.shard
                 LEFT JOIN hold h ON h.id = l.hold_id
                GROUP BY b.account_id, b.held
               HAVING b.held <> COALESCE(SUM(l.amount) FILTER (WHERE h.status = 0), 0)
                LIMIT $1"#,
            limit
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|r| AuditMismatch {
                account: AccountId(r.account_id),
                held: Money::from_nanos(r.held),
                active_legs: Money::from_nanos(r.active_legs),
            })
            .collect())
    }

    async fn hold_stats(&self) -> Result<HoldStats, LedgerError> {
        let row = sqlx::query!(
            r#"SELECT count(*) AS "active!",
                      COALESCE(
                        percentile_disc(0.99) WITHIN GROUP (
                          ORDER BY EXTRACT(EPOCH FROM (now() - created_at))
                        ), 0)::DOUBLE PRECISION AS "age_p99!"
                 FROM hold WHERE status = $1"#,
            ACTIVE
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(HoldStats {
            active: row.active,
            age_p99: Duration::from_secs_f64(row.age_p99.max(0.0)),
        })
    }

    async fn rate_allow(
        &self,
        key: &RateKey,
        rate: u32,
        burst: u32,
    ) -> Result<bool, LedgerError> {
        Ok(self.rate.allow(key, rate, burst))
    }

    async fn try_lock(
        &self,
        key: &str,
        _ttl: Duration,
    ) -> Result<Option<LockGuard>, LedgerError> {
        let mut conn = self.pool.acquire().await?;
        let got = sqlx::query_scalar!(
            r#"SELECT pg_try_advisory_lock($1) AS "got!""#,
            advisory_key(key)
        )
        .fetch_one(&mut *conn)
        .await?;

        if !got {
            // 未持锁，连接照常归还池中
            return Ok(None);
        }
        // advisory lock 绑在会话上，连接一旦回到池里就可能被别的查询复用并
        // 在归还时连带释放。摘出连接由 LockGuard 独占，析构才关闭。
        Ok(Some(LockGuard::pg(key, conn.detach())))
    }

    async fn balances(&self, account: AccountId) -> Result<(Money, Money), LedgerError> {
        let row = sqlx::query!(
            "SELECT balance, held FROM account_balance WHERE account_id = $1 AND shard = $2",
            account.0,
            SHARD
        )
        .fetch_optional(&self.pool)
        .await?;
        Ok(
            row.map_or((Money::from_nanos(0), Money::from_nanos(0)), |r| {
                (Money::from_nanos(r.balance), Money::from_nanos(r.held))
            }),
        )
    }

    async fn hold(&self, req: HoldRequest<'_>) -> Result<Hold, LedgerError> {
        if req.chain.is_empty() {
            return Err(LedgerError::EmptyChain);
        }
        let amount = req.amount.as_nanos();
        let expires_at = Utc::now()
            + chrono::Duration::from_std(req.ttl).unwrap_or_else(|_| chrono::Duration::hours(1));

        let mut tx = self.pool.begin().await?;

        // 幂等：同一 key 只冻结一次。已存在则读回原 Hold。
        let inserted = sqlx::query_scalar!(
            r#"INSERT INTO hold (id, amount, timing, idempotency_key, expires_at)
               VALUES ($1, $2, $3, $4, $5)
               ON CONFLICT (idempotency_key) DO NOTHING
               RETURNING id"#,
            Uuid::new_v4(),
            amount,
            timing_code(req.timing),
            req.idempotency_key,
            expires_at
        )
        .fetch_optional(&mut *tx)
        .await?;

        let Some(hold_id) = inserted else {
            tx.rollback().await?;
            return self.load_existing(req.idempotency_key).await;
        };

        // 按 account_id 排序加锁，避免嵌套链交叉时死锁；depth 保留原始链序
        let mut legs: Vec<(i16, i64)> = req
            .chain
            .iter()
            .enumerate()
            .map(|(depth, a)| (i16::try_from(depth).unwrap_or(i16::MAX), a.0))
            .collect();
        legs.sort_by_key(|(_, account_id)| *account_id);

        for (depth, account_id) in &legs {
            // 任一级不足即整体回滚，不留痕
            if let Err(e) = reserve(&mut tx, *account_id, amount).await {
                tx.rollback().await?;
                return Err(e);
            }
            sqlx::query!(
                r#"INSERT INTO hold_leg (hold_id, account_id, shard, amount, depth)
                   VALUES ($1, $2, $3, $4, $5)"#,
                hold_id,
                account_id,
                SHARD,
                amount,
                depth
            )
            .execute(&mut *tx)
            .await?;

            sqlx::query!(
                r#"INSERT INTO ledger_entry (account_id, kind, amount, hold_id)
                   VALUES ($1, $2, $3, $4)"#,
                account_id,
                ENTRY_HOLD,
                amount,
                hold_id
            )
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;

        Ok(Hold::new(
            HoldId(hold_id),
            req.chain.iter().copied().collect(),
            req.amount,
            expires_at,
            self.reclaimer.clone(),
        ))
    }

    async fn capture(&self, mut hold: Hold, actual: Money) -> Result<(), LedgerError> {
        self.settle(hold.id(), CAPTURED, ENTRY_CAPTURE, actual.as_nanos())
            .await?;
        // 仅在成功后置位：失败时让 Drop 上报，交由回收器兜底
        hold.mark_consumed();
        Ok(())
    }

    async fn void(&self, mut hold: Hold) -> Result<(), LedgerError> {
        self.settle(hold.id(), VOIDED, ENTRY_VOID, 0).await?;
        hold.mark_consumed();
        Ok(())
    }

    async fn extend(&self, hold: &Hold, delta: Money) -> Result<(), LedgerError> {
        let amount = delta.as_nanos();
        let mut tx = self.pool.begin().await?;

        let legs = sqlx::query_scalar!(
            "SELECT account_id FROM hold_leg WHERE hold_id = $1 ORDER BY account_id",
            hold.id().0
        )
        .fetch_all(&mut *tx)
        .await?;

        for account_id in legs {
            if let Err(e) = reserve(&mut tx, account_id, amount).await {
                tx.rollback().await?;
                return Err(e);
            }
            sqlx::query!(
                "UPDATE hold_leg SET amount = amount + $2 WHERE hold_id = $1 AND account_id = $3",
                hold.id().0,
                amount,
                account_id
            )
            .execute(&mut *tx)
            .await?;
        }
        sqlx::query!(
            "UPDATE hold SET amount = amount + $2 WHERE id = $1 AND status = $3",
            hold.id().0,
            amount,
            ACTIVE
        )
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn capture_partial(&self, hold: &Hold, amount: Money) -> Result<(), LedgerError> {
        let charge = amount.as_nanos();
        let mut tx = self.pool.begin().await?;

        let legs = sqlx::query!(
            "SELECT account_id, shard, amount FROM hold_leg
              WHERE hold_id = $1 ORDER BY account_id",
            hold.id().0
        )
        .fetch_all(&mut *tx)
        .await?;

        for leg in legs {
            // 扣款可超出冻结额（用量超估算是常态），但释放量以剩余冻结额为限，
            // 否则 held 会被扣成负数
            let release = leg.amount.min(charge);
            sqlx::query!(
                r#"UPDATE account_balance
                      SET held = held - $3, balance = balance - $4, updated_at = now()
                    WHERE account_id = $1 AND shard = $2"#,
                leg.account_id,
                leg.shard,
                release,
                charge
            )
            .execute(&mut *tx)
            .await?;

            sqlx::query!(
                r#"INSERT INTO ledger_entry (account_id, kind, amount, hold_id)
                   VALUES ($1, $2, $3, $4)"#,
                leg.account_id,
                ENTRY_CAPTURE,
                charge,
                hold.id().0
            )
            .execute(&mut *tx)
            .await?;
        }

        // 冻结额同步减少，维持 held == 活跃腿之和
        sqlx::query!(
            "UPDATE hold_leg SET amount = GREATEST(amount - $2, 0) WHERE hold_id = $1",
            hold.id().0,
            charge
        )
        .execute(&mut *tx)
        .await?;
        sqlx::query!(
            "UPDATE hold SET amount = GREATEST(amount - $2, 0) WHERE id = $1 AND status = $3",
            hold.id().0,
            charge,
            ACTIVE
        )
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }
}

impl PgCoordinator {
    /// 幂等重放：读回同一 key 已创建的 Hold。
    async fn load_existing(&self, idempotency_key: &str) -> Result<Hold, LedgerError> {
        let row = sqlx::query!(
            "SELECT id, amount, status, expires_at FROM hold WHERE idempotency_key = $1",
            idempotency_key
        )
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            return Err(LedgerError::HoldNotActive);
        };
        if row.status != ACTIVE {
            // 已结算的键被重放，与「Hold 非活跃」是两种情况：前者应告知调用方
            // 这是重复请求，后者是内部状态错误
            return Err(LedgerError::DuplicateRequest {
                key: idempotency_key.to_owned(),
            });
        }

        let chain = sqlx::query_scalar!(
            "SELECT account_id FROM hold_leg WHERE hold_id = $1 ORDER BY depth",
            row.id
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(Hold::new(
            HoldId(row.id),
            chain
                .into_iter()
                .map(AccountId)
                .collect::<SmallVec<[AccountId; 4]>>(),
            Money::from_nanos(row.amount),
            row.expires_at,
            self.reclaimer.clone(),
        ))
    }
}
