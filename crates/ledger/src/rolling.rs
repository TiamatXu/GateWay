//! 滚动 Hold：用量边产生边扣，冻结额见底自动续。
//!
//! `Metered`（M3 异步任务）与 `Session`（M4 实时会话）共用这套机制。两者的
//! 差别不在账本，而在**谁负责收尾**：会话断开即结算，异步任务要等终态或
//! 孤儿巡检。因此这里只提供机制，收尾时机由调用方决定。
//!
//! 为什么需要它：`Hold::amount()` 是开户时的冻结额，`capture_partial` 与
//! `extend` 之后就不再是「当前还冻着多少」。滚动场景每一步都要知道这个数，
//! 每次回库查一遍既慢又会在并发下读到过期值，所以在本地跟踪。

use std::sync::Arc;

use gw_core::{BillingTiming, Money};

use crate::{Coordinator, Hold, HoldRequest, LedgerError};

/// 续期策略。
#[derive(Debug, Clone, Copy)]
pub struct RollingPolicy {
    /// 剩余冻结额低于此值即续期。设为 0 表示只在不够扣时才续。
    pub low_watermark: Money,
    /// 每次续期至少追加多少。扣款额超过它时按扣款额续。
    pub top_up: Money,
}

/// 滚动 Hold。析构行为与 `Hold` 一致：未收尾即上报泄漏，由 TTL 兜底。
#[must_use = "RollingHold 必须被 close 或 abandon"]
pub struct RollingHold {
    coord: Arc<dyn Coordinator>,
    hold: Hold,
    reserved: Money,
    policy: RollingPolicy,
}

impl std::fmt::Debug for RollingHold {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // 手写：`coord` 是 trait object，派生不出 Debug
        f.debug_struct("RollingHold")
            .field("hold", &self.hold)
            .field("reserved", &self.reserved)
            .field("policy", &self.policy)
            .finish_non_exhaustive()
    }
}

impl RollingHold {
    /// 开一个滚动 Hold。
    ///
    /// # Errors
    /// `timing` 不是 `Metered` 或 `Session` 时返回 `TimingNotRolling`；
    /// 额度不足时返回 `InsufficientFunds`。
    pub async fn open(
        coord: Arc<dyn Coordinator>,
        req: HoldRequest<'_>,
        policy: RollingPolicy,
    ) -> Result<Self, LedgerError> {
        // 一次性计费用不着滚动，误用会让 close 的语义变得含糊：
        // 究竟是「扣 0」还是「扣全额」？在入口挡掉，不留这个歧义。
        if !matches!(req.timing, BillingTiming::Metered | BillingTiming::Session) {
            return Err(LedgerError::TimingNotRolling { timing: req.timing });
        }
        let reserved = req.amount;
        let hold = coord.hold(req).await?;
        Ok(Self {
            coord,
            hold,
            reserved,
            policy,
        })
    }

    /// 当前仍冻结的额度。
    #[must_use]
    pub fn reserved(&self) -> Money {
        self.reserved
    }

    #[must_use]
    pub fn id(&self) -> gw_core::HoldId {
        self.hold.id()
    }

    /// 扣掉一段已产生的用量，必要时先续冻结额。
    ///
    /// # Errors
    /// 续期时额度不足返回 `InsufficientFunds`——此时**尚未扣款**，
    /// 调用方应终止会话而非继续放量。
    pub async fn charge(&mut self, amount: Money) -> Result<(), LedgerError> {
        if amount <= Money::from_nanos(0) {
            return Ok(());
        }
        self.ensure_reserved(amount).await?;

        self.coord.capture_partial(&self.hold, amount).await?;
        // 扣款可以超出冻结额（用量超估算是常态），但冻结额不会被扣成负数，
        // 本地跟踪必须与 `capture_partial` 的 GREATEST(.., 0) 保持一致
        self.reserved = self.reserved.checked_sub(amount).unwrap_or_default();
        if self.reserved < Money::from_nanos(0) {
            self.reserved = Money::from_nanos(0);
        }
        Ok(())
    }

    /// 正常收尾：关闭 Hold，释放剩余冻结。用量已由 `charge` 逐段扣完，
    /// 因此这里不再扣款。
    ///
    /// # Errors
    /// Hold 已非活跃时返回 `HoldNotActive`。
    pub async fn close(self) -> Result<(), LedgerError> {
        self.coord.capture(self.hold, Money::from_nanos(0)).await
    }

    /// 异常收尾：撤销。已由 `charge` 扣掉的部分不受影响——它们是独立的
    /// 账本条目，撤销只释放剩余冻结。
    ///
    /// # Errors
    /// Hold 已非活跃时返回 `HoldNotActive`。
    pub async fn abandon(self) -> Result<(), LedgerError> {
        self.coord.void(self.hold).await
    }

    /// 保证冻结额足以覆盖本次扣款，且扣完不跌破低水位。
    async fn ensure_reserved(&mut self, amount: Money) -> Result<(), LedgerError> {
        let need = amount
            .checked_add(self.policy.low_watermark)
            .unwrap_or(Money::from_nanos(i64::MAX));
        if self.reserved >= need {
            return Ok(());
        }

        let short = need.checked_sub(self.reserved).unwrap_or_default();
        let delta = short.max(self.policy.top_up);
        self.coord.extend(&self.hold, delta).await?;
        self.reserved = self.reserved.checked_add(delta).unwrap_or(self.reserved);
        Ok(())
    }
}
