use std::time::Duration;

use chrono::{DateTime, Utc};
use gw_core::{AccountId, BillingTiming, HoldId, Money};
use smallvec::SmallVec;
use tokio::sync::mpsc;

pub struct HoldRequest<'a> {
    /// 账户链，由近及远，链首为主计费主体。链上每一级都要有额度。
    pub chain: &'a [AccountId],
    pub amount: Money,
    pub ttl: Duration,
    pub idempotency_key: &'a str,
    pub timing: BillingTiming,
}

/// 一次冻结。必须被 capture 或 void，否则析构时上报泄漏并投递到回收队列。
#[must_use = "Hold 必须被 capture 或 void"]
#[derive(Debug)]
pub struct Hold {
    id: HoldId,
    chain: SmallVec<[AccountId; 4]>,
    amount: Money,
    expires_at: DateTime<Utc>,
    consumed: bool,
    reclaimer: mpsc::Sender<HoldId>,
}

impl Hold {
    pub(crate) fn new(
        id: HoldId,
        chain: SmallVec<[AccountId; 4]>,
        amount: Money,
        expires_at: DateTime<Utc>,
        reclaimer: mpsc::Sender<HoldId>,
    ) -> Self {
        Self {
            id,
            chain,
            amount,
            expires_at,
            consumed: false,
            reclaimer,
        }
    }

    /// 构造一个不对应任何数据库行的 Hold，仅供测试替身使用。
    #[cfg(feature = "testkit")]
    pub fn stub(amount: Money, reclaimer: mpsc::Sender<HoldId>) -> Self {
        Self::new(
            HoldId(uuid::Uuid::new_v4()),
            SmallVec::new(),
            amount,
            Utc::now(),
            reclaimer,
        )
    }

    #[must_use]
    pub fn id(&self) -> HoldId {
        self.id
    }

    #[must_use]
    pub fn amount(&self) -> Money {
        self.amount
    }

    #[must_use]
    pub fn chain(&self) -> &[AccountId] {
        &self.chain
    }

    #[must_use]
    pub fn expires_at(&self) -> DateTime<Utc> {
        self.expires_at
    }

    /// 把 Hold 的所有权交给一条持久记录（异步任务的句柄）。返回单号，
    /// 此后析构不再上报泄漏——它不是被泄漏了，是被托管了。
    ///
    /// 托管之后仍受 `expires_at` 约束：任务永远不到终态时由 TTL 回收器撤销，
    /// 这是防死冻结的兜底一道，不能因为「有人管着」就取消。
    #[must_use]
    pub fn detach(mut self) -> HoldId {
        self.consumed = true;
        self.id
    }

    pub(crate) fn mark_consumed(&mut self) {
        self.consumed = true;
    }

    /// 供测试替身在不落库的情况下消费 Hold。
    #[cfg(feature = "testkit")]
    pub fn mark_settled_for_test(&mut self) {
        self.consumed = true;
    }
}

impl Drop for Hold {
    fn drop(&mut self) {
        if self.consumed {
            return;
        }
        // 不在 Drop 中 panic：Drop 内 panic 在已 panicking 时会 abort，
        // 且 async 任务被取消时正走此路径。TTL 回收器是最终兜底。
        metrics::counter!("ledger.hold_leaked").increment(1);
        tracing::error!(hold_id = %self.id, "Hold 未经 capture/void 即析构");
        let _ = self.reclaimer.try_send(self.id);
    }
}
