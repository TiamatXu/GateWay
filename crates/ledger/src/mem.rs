//! 进程内账本。用于测试与单机无数据库场景。
//!
//! 全部状态在一把锁下，原子性天然成立——它的价值是作为 `pg` 实现的行为基准：
//! 两者必须通过同一套一致性测试。

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, Ordering};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use gw_core::{AccountId, HoldId, Money};
use smallvec::SmallVec;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::pg::{AuditMismatch, Coordinator};
use crate::{Hold, HoldRequest, LedgerError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Status {
    Active,
    Captured,
    Voided,
    Expired,
}

#[derive(Debug)]
struct HoldRow {
    status: Status,
    /// 每级账户当前仍冻结的金额
    legs: Vec<(AccountId, i64)>,
    expires_at: DateTime<Utc>,
}

#[derive(Debug, Default)]
struct Balance {
    balance: i64,
    held: i64,
}

#[derive(Debug, Default)]
struct State {
    accounts: HashMap<AccountId, Balance>,
    holds: HashMap<HoldId, HoldRow>,
    by_key: HashMap<String, HoldId>,
}

pub struct MemCoordinator {
    state: Mutex<State>,
    reclaimer: mpsc::Sender<HoldId>,
    next_account: AtomicI64,
}

impl MemCoordinator {
    #[must_use]
    pub fn new(reclaimer: mpsc::Sender<HoldId>) -> Self {
        Self {
            state: Mutex::new(State::default()),
            reclaimer,
            next_account: AtomicI64::new(1),
        }
    }

    /// 新建账户并充值。
    pub fn create_account(&self, balance: i64) -> AccountId {
        let id = AccountId(self.next_account.fetch_add(1, Ordering::Relaxed));
        self.lock()
            .accounts
            .insert(id, Balance { balance, held: 0 });
        id
    }

    /// 锁中毒不影响账本状态的有效性——余额仍是余额，不能因此拒绝服务。
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// 释放一个 Hold 的全部腿，并按 `charge` 扣减余额。
    fn settle_locked(
        state: &mut State,
        id: HoldId,
        status: Status,
        charge: i64,
    ) -> Result<(), LedgerError> {
        let row = state.holds.get_mut(&id).ok_or(LedgerError::HoldNotActive)?;
        if row.status != Status::Active {
            return Err(LedgerError::HoldNotActive);
        }
        row.status = status;
        let legs = std::mem::take(&mut row.legs);

        for (account, reserved) in legs {
            if let Some(bal) = state.accounts.get_mut(&account) {
                bal.held -= reserved;
                bal.balance -= charge;
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Coordinator for MemCoordinator {
    async fn hold(&self, req: HoldRequest<'_>) -> Result<Hold, LedgerError> {
        if req.chain.is_empty() {
            return Err(LedgerError::EmptyChain);
        }
        let amount = req.amount.as_nanos();
        let expires_at = Utc::now()
            + chrono::Duration::from_std(req.ttl).unwrap_or_else(|_| chrono::Duration::hours(1));
        let mut state = self.lock();

        // 幂等：同一 key 只冻结一次
        if let Some(existing) = state.by_key.get(req.idempotency_key).copied() {
            let row = &state.holds[&existing];
            if row.status != Status::Active {
                return Err(LedgerError::DuplicateRequest {
                    key: req.idempotency_key.to_owned(),
                });
            }
            let chain: SmallVec<[AccountId; 4]> = row.legs.iter().map(|(a, _)| *a).collect();
            let total: i64 = row.legs.first().map_or(0, |(_, amt)| *amt);
            let expires = row.expires_at;
            return Ok(Hold::new(
                existing,
                chain,
                Money::from_nanos(total),
                expires,
                self.reclaimer.clone(),
            ));
        }

        // 按 AccountId 升序处理，与 pg 实现的加锁顺序一致
        let mut chain: Vec<AccountId> = req.chain.to_vec();
        chain.sort_unstable();

        // 先校验全部层级，任一级不足则整体不生效
        for account in &chain {
            let bal = state
                .accounts
                .get(account)
                .ok_or(LedgerError::InsufficientFunds {
                    account: *account,
                    required: req.amount,
                    available: Money::from_nanos(0),
                })?;
            if bal.balance - bal.held < amount {
                return Err(LedgerError::InsufficientFunds {
                    account: *account,
                    required: req.amount,
                    available: Money::from_nanos(bal.balance - bal.held),
                });
            }
        }

        let id = HoldId(Uuid::new_v4());
        let mut legs = Vec::with_capacity(chain.len());
        for account in &chain {
            if let Some(bal) = state.accounts.get_mut(account) {
                bal.held += amount;
            }
            legs.push((*account, amount));
        }
        state.holds.insert(
            id,
            HoldRow {
                status: Status::Active,
                legs,
                expires_at,
            },
        );
        state.by_key.insert(req.idempotency_key.to_owned(), id);

        Ok(Hold::new(
            id,
            req.chain.iter().copied().collect(),
            req.amount,
            expires_at,
            self.reclaimer.clone(),
        ))
    }

    async fn capture(&self, mut hold: Hold, actual: Money) -> Result<(), LedgerError> {
        Self::settle_locked(
            &mut self.lock(),
            hold.id(),
            Status::Captured,
            actual.as_nanos(),
        )?;
        hold.mark_consumed();
        Ok(())
    }

    async fn void(&self, mut hold: Hold) -> Result<(), LedgerError> {
        Self::settle_locked(&mut self.lock(), hold.id(), Status::Voided, 0)?;
        hold.mark_consumed();
        Ok(())
    }

    async fn extend(&self, hold: &Hold, delta: Money) -> Result<(), LedgerError> {
        let amount = delta.as_nanos();
        let mut state = self.lock();
        let row = state
            .holds
            .get(&hold.id())
            .ok_or(LedgerError::HoldNotActive)?;
        if row.status != Status::Active {
            return Err(LedgerError::HoldNotActive);
        }
        let accounts: Vec<AccountId> = row.legs.iter().map(|(a, _)| *a).collect();

        for account in &accounts {
            let bal = state
                .accounts
                .get(account)
                .ok_or(LedgerError::HoldNotActive)?;
            if bal.balance - bal.held < amount {
                return Err(LedgerError::InsufficientFunds {
                    account: *account,
                    required: delta,
                    available: Money::from_nanos(bal.balance - bal.held),
                });
            }
        }
        for account in &accounts {
            if let Some(bal) = state.accounts.get_mut(account) {
                bal.held += amount;
            }
        }
        if let Some(row) = state.holds.get_mut(&hold.id()) {
            for leg in &mut row.legs {
                leg.1 += amount;
            }
        }
        Ok(())
    }

    async fn capture_partial(&self, hold: &Hold, amount: Money) -> Result<(), LedgerError> {
        let charge = amount.as_nanos();
        let mut state = self.lock();
        let row = state
            .holds
            .get_mut(&hold.id())
            .ok_or(LedgerError::HoldNotActive)?;
        if row.status != Status::Active {
            return Err(LedgerError::HoldNotActive);
        }
        // 扣款可超出冻结额（用量超估算是常态），但释放量以剩余冻结额为限，
        // 否则 held 会被扣成负数
        let mut released: Vec<(AccountId, i64)> = Vec::with_capacity(row.legs.len());
        for leg in &mut row.legs {
            let release = leg.1.min(charge);
            leg.1 -= release;
            released.push((leg.0, release));
        }
        for (account, release) in released {
            if let Some(bal) = state.accounts.get_mut(&account) {
                bal.held -= release;
                bal.balance -= charge;
            }
        }
        Ok(())
    }

    async fn capture_by_id(&self, id: HoldId, actual: Money) -> Result<(), LedgerError> {
        Self::settle_locked(&mut self.lock(), id, Status::Captured, actual.as_nanos())
    }

    async fn void_by_id(&self, id: HoldId) -> Result<(), LedgerError> {
        Self::settle_locked(&mut self.lock(), id, Status::Voided, 0)
    }

    async fn reclaim_expired(&self, limit: i64) -> Result<u64, LedgerError> {
        let now = Utc::now();
        let mut state = self.lock();
        let expired: Vec<HoldId> = state
            .holds
            .iter()
            .filter(|(_, r)| r.status == Status::Active && r.expires_at <= now)
            .map(|(id, _)| *id)
            .take(usize::try_from(limit).unwrap_or(usize::MAX))
            .collect();

        let n = expired.len() as u64;
        for id in expired {
            Self::settle_locked(&mut state, id, Status::Expired, 0)?;
        }
        Ok(n)
    }

    async fn audit(&self, limit: i64) -> Result<Vec<AuditMismatch>, LedgerError> {
        let state = self.lock();
        let mut active: HashMap<AccountId, i64> = HashMap::new();
        for row in state.holds.values() {
            if row.status != Status::Active {
                continue;
            }
            for (account, amount) in &row.legs {
                *active.entry(*account).or_default() += amount;
            }
        }
        Ok(state
            .accounts
            .iter()
            .filter_map(|(id, bal)| {
                let legs = active.get(id).copied().unwrap_or(0);
                (bal.held != legs).then_some(AuditMismatch {
                    account: *id,
                    held: Money::from_nanos(bal.held),
                    active_legs: Money::from_nanos(legs),
                })
            })
            .take(usize::try_from(limit).unwrap_or(usize::MAX))
            .collect())
    }

    async fn balances(&self, account: AccountId) -> Result<(Money, Money), LedgerError> {
        let state = self.lock();
        Ok(state
            .accounts
            .get(&account)
            .map_or((Money::from_nanos(0), Money::from_nanos(0)), |b| {
                (Money::from_nanos(b.balance), Money::from_nanos(b.held))
            }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn create_account_allocates_distinct_ids() {
        let (tx, _rx) = mpsc::channel(8);
        let c = MemCoordinator::new(tx);
        assert_ne!(c.create_account(100), c.create_account(100));
    }

    /// 未知账户当作额度不足，而非当作零余额悄悄放行
    #[tokio::test]
    async fn unknown_account_is_insufficient() {
        let (tx, _rx) = mpsc::channel(8);
        let c = MemCoordinator::new(tx);
        let err = c
            .hold(HoldRequest {
                chain: &[AccountId(999)],
                amount: Money::from_nanos(1),
                ttl: std::time::Duration::from_secs(60),
                idempotency_key: "k",
                timing: gw_core::BillingTiming::InRequest,
            })
            .await
            .unwrap_err();
        assert!(matches!(err, LedgerError::InsufficientFunds { .. }));
    }
}
