//! `Coordinator` 一致性测试套件。
//!
//! 三种实现（pg / mem / redis）必须表现一致——账本语义不能因后端而异。
//! 各测试二进制用 `conformance_tests!` 生成同名 `#[tokio::test]` 包装。

#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use gw_core::{AccountId, BillingTiming, Money};
use gw_ledger::{Coordinator, HoldRequest, LedgerError};

/// 后端各自提供账户准备与余额查询。
#[async_trait]
pub trait Backend: Send + Sync {
    /// 新建一个带余额的独立账户。
    async fn account(&self, balance: i64) -> AccountId;
    /// 返回 Arc 而非引用：并发用例需要把它移进 spawn 的任务里
    fn coord(&self) -> Arc<dyn Coordinator>;
}

pub async fn balances(b: &dyn Backend, id: AccountId) -> (i64, i64) {
    let (balance, held) = b.coord().balances(id).await.unwrap();
    (balance.as_nanos(), held.as_nanos())
}

pub fn req<'a>(chain: &'a [AccountId], amount: i64, key: &'a str) -> HoldRequest<'a> {
    HoldRequest {
        chain,
        amount: Money::from_nanos(amount),
        ttl: Duration::from_secs(60),
        idempotency_key: key,
        timing: BillingTiming::InRequest,
    }
}

/// 对账是全库扫描，共用一个开发库时会看到其他测试的账户。
/// 断言只针对本测试的账户。
pub async fn assert_consistent(b: &dyn Backend, account: AccountId) {
    let mine: Vec<_> = b
        .coord()
        .audit(1024)
        .await
        .unwrap()
        .into_iter()
        .filter(|m| m.account == account)
        .collect();
    assert!(mine.is_empty(), "账实不符：{mine:?}");
}

pub fn key() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// 为一组一致性测试生成 `#[tokio::test]` 包装。
#[macro_export]
macro_rules! conformance_tests {
    ($setup:expr; $($name:ident),* $(,)?) => {
        $(
            #[tokio::test]
            async fn $name() {
                let backend = $setup.await;
                $crate::conformance::$name(&backend).await;
            }
        )*
    };
}

// ---------------------------------------------------------------------- hold

pub async fn hold_reserves_without_moving_balance(b: &dyn Backend) {
    let a = b.account(1_000).await;

    let hold = b.coord().hold(req(&[a], 300, &key())).await.unwrap();

    assert_eq!(balances(b, a).await, (1_000, 300));
    b.coord().void(hold).await.unwrap();
}

pub async fn hold_rejects_when_available_is_short(b: &dyn Backend) {
    let a = b.account(100).await;

    let err = b.coord().hold(req(&[a], 300, &key())).await.unwrap_err();

    assert!(
        matches!(err, LedgerError::InsufficientFunds { .. }),
        "{err:?}"
    );
    assert_eq!(balances(b, a).await, (100, 0));
}

/// 已冻结的部分不可再冻结
pub async fn hold_counts_existing_holds_against_available(b: &dyn Backend) {
    let a = b.account(1_000).await;
    let h1 = b.coord().hold(req(&[a], 800, &key())).await.unwrap();

    let err = b.coord().hold(req(&[a], 300, &key())).await.unwrap_err();

    assert!(
        matches!(err, LedgerError::InsufficientFunds { .. }),
        "{err:?}"
    );
    assert_eq!(balances(b, a).await, (1_000, 800));
    b.coord().void(h1).await.unwrap();
}

/// 重试不得二次冻结
pub async fn hold_is_idempotent_by_key(b: &dyn Backend) {
    let a = b.account(1_000).await;
    let k = key();

    let h1 = b.coord().hold(req(&[a], 300, &k)).await.unwrap();
    let h2 = b.coord().hold(req(&[a], 300, &k)).await.unwrap();

    assert_eq!(h1.id(), h2.id());
    assert_eq!(balances(b, a).await, (1_000, 300));
    b.coord().void(h1).await.unwrap();
    std::mem::forget(h2); // 同一个 Hold 的第二个句柄，不参与结算
}

/// 已结算的幂等键被重放：必须能与「Hold 非活跃」区分开
pub async fn replaying_a_settled_key_is_reported_as_duplicate(b: &dyn Backend) {
    let a = b.account(1_000).await;
    let k = key();
    let hold = b.coord().hold(req(&[a], 300, &k)).await.unwrap();
    b.coord()
        .capture(hold, Money::from_nanos(100))
        .await
        .unwrap();

    let err = b.coord().hold(req(&[a], 300, &k)).await.unwrap_err();

    assert!(
        matches!(err, LedgerError::DuplicateRequest { .. }),
        "{err:?}"
    );
    assert_eq!(balances(b, a).await, (900, 0), "重放不得再次冻结");
}

/// 嵌套额度：链上每一级都要有额度
pub async fn nested_chain_reserves_at_every_level(b: &dyn Backend) {
    let child = b.account(1_000).await;
    let parent = b.account(1_000).await;

    let hold = b
        .coord()
        .hold(req(&[child, parent], 300, &key()))
        .await
        .unwrap();

    assert_eq!(balances(b, child).await, (1_000, 300));
    assert_eq!(balances(b, parent).await, (1_000, 300));
    b.coord().void(hold).await.unwrap();
}

/// 任一级不足则整体失败且不留痕
pub async fn nested_chain_rolls_back_when_an_ancestor_is_short(b: &dyn Backend) {
    let child = b.account(1_000).await;
    let parent = b.account(100).await;

    let err = b
        .coord()
        .hold(req(&[child, parent], 300, &key()))
        .await
        .unwrap_err();

    assert!(
        matches!(err, LedgerError::InsufficientFunds { .. }),
        "{err:?}"
    );
    assert_eq!(balances(b, child).await, (1_000, 0), "子账户冻结未回滚");
    assert_eq!(balances(b, parent).await, (100, 0));
}

pub async fn empty_chain_is_rejected(b: &dyn Backend) {
    let err = b.coord().hold(req(&[], 300, &key())).await.unwrap_err();
    assert!(matches!(err, LedgerError::EmptyChain), "{err:?}");
}

// ------------------------------------------------------------- capture/void

pub async fn capture_charges_actual_and_releases_the_rest(b: &dyn Backend) {
    let a = b.account(1_000).await;
    let hold = b.coord().hold(req(&[a], 300, &key())).await.unwrap();

    b.coord()
        .capture(hold, Money::from_nanos(120))
        .await
        .unwrap();

    assert_eq!(balances(b, a).await, (880, 0));
}

/// 实际用量超出估算是常态，按实际扣款，允许透支为负余额
pub async fn capture_above_hold_charges_the_actual_amount(b: &dyn Backend) {
    let a = b.account(400).await;
    let hold = b.coord().hold(req(&[a], 300, &key())).await.unwrap();

    b.coord()
        .capture(hold, Money::from_nanos(500))
        .await
        .unwrap();

    assert_eq!(balances(b, a).await, (-100, 0));
}

/// 嵌套链上每一级都要扣到实际金额
pub async fn capture_charges_every_level_of_the_chain(b: &dyn Backend) {
    let child = b.account(1_000).await;
    let parent = b.account(1_000).await;
    let hold = b
        .coord()
        .hold(req(&[child, parent], 300, &key()))
        .await
        .unwrap();

    b.coord()
        .capture(hold, Money::from_nanos(120))
        .await
        .unwrap();

    assert_eq!(balances(b, child).await, (880, 0));
    assert_eq!(balances(b, parent).await, (880, 0));
}

pub async fn void_releases_without_charging(b: &dyn Backend) {
    let a = b.account(1_000).await;
    let hold = b.coord().hold(req(&[a], 300, &key())).await.unwrap();

    b.coord().void(hold).await.unwrap();

    assert_eq!(balances(b, a).await, (1_000, 0));
}

pub async fn settling_twice_is_rejected(b: &dyn Backend) {
    let a = b.account(1_000).await;
    let h1 = b.coord().hold(req(&[a], 300, &key())).await.unwrap();
    let id = h1.id();
    b.coord().capture(h1, Money::from_nanos(100)).await.unwrap();

    let err = b
        .coord()
        .capture_by_id(id, Money::from_nanos(100))
        .await
        .unwrap_err();

    assert!(matches!(err, LedgerError::HoldNotActive), "{err:?}");
    assert_eq!(balances(b, a).await, (900, 0));
}

// -------------------------------------------------------- extend / 滚动结算

pub async fn extend_increases_the_reservation(b: &dyn Backend) {
    let a = b.account(1_000).await;
    let hold = b.coord().hold(req(&[a], 300, &key())).await.unwrap();

    b.coord()
        .extend(&hold, Money::from_nanos(200))
        .await
        .unwrap();

    assert_eq!(balances(b, a).await, (1_000, 500));
    b.coord().void(hold).await.unwrap();
}

/// 追加额度在链上每一级各加一次，不得按腿数重复累加
pub async fn extend_applies_once_per_account_on_a_chain(b: &dyn Backend) {
    let child = b.account(1_000).await;
    let parent = b.account(1_000).await;
    let hold = b
        .coord()
        .hold(req(&[child, parent], 300, &key()))
        .await
        .unwrap();

    b.coord()
        .extend(&hold, Money::from_nanos(200))
        .await
        .unwrap();

    assert_eq!(balances(b, child).await, (1_000, 500));
    assert_eq!(balances(b, parent).await, (1_000, 500));
    b.coord().void(hold).await.unwrap();
}

pub async fn extend_rejects_when_available_is_short(b: &dyn Backend) {
    let a = b.account(400).await;
    let hold = b.coord().hold(req(&[a], 300, &key())).await.unwrap();

    let err = b
        .coord()
        .extend(&hold, Money::from_nanos(200))
        .await
        .unwrap_err();

    assert!(
        matches!(err, LedgerError::InsufficientFunds { .. }),
        "{err:?}"
    );
    assert_eq!(balances(b, a).await, (400, 300));
    b.coord().void(hold).await.unwrap();
}

/// `Session` / `Metered` 的滚动结算：分段扣款但不关闭 Hold
pub async fn capture_partial_charges_without_closing_the_hold(b: &dyn Backend) {
    let a = b.account(1_000).await;
    let hold = b.coord().hold(req(&[a], 300, &key())).await.unwrap();

    b.coord()
        .capture_partial(&hold, Money::from_nanos(50))
        .await
        .unwrap();

    assert_eq!(balances(b, a).await, (950, 250));
    b.coord().void(hold).await.unwrap();
    assert_eq!(balances(b, a).await, (950, 0));
}

/// 滚动结算可反复进行，直到冻结额耗尽由 extend 续上
pub async fn repeated_partial_captures_accumulate(b: &dyn Backend) {
    let a = b.account(1_000).await;
    let hold = b.coord().hold(req(&[a], 300, &key())).await.unwrap();

    for _ in 0..3 {
        b.coord()
            .capture_partial(&hold, Money::from_nanos(50))
            .await
            .unwrap();
    }

    assert_eq!(balances(b, a).await, (850, 150));
    b.coord()
        .capture(hold, Money::from_nanos(20))
        .await
        .unwrap();
    assert_eq!(balances(b, a).await, (830, 0));
}

// ------------------------------------------------------------------ 兜底回收

pub async fn expired_holds_are_reclaimed(b: &dyn Backend) {
    let a = b.account(1_000).await;
    let hold = b
        .coord()
        .hold(HoldRequest {
            ttl: Duration::ZERO,
            ..req(&[a], 300, &key())
        })
        .await
        .unwrap();
    std::mem::forget(hold); // 模拟进程崩溃，未走 capture/void

    let n = b.coord().reclaim_expired(100).await.unwrap();

    assert!(n >= 1);
    assert_eq!(balances(b, a).await, (1_000, 0));
}

pub async fn reclaim_leaves_active_holds_alone(b: &dyn Backend) {
    let a = b.account(1_000).await;
    let hold = b.coord().hold(req(&[a], 300, &key())).await.unwrap();

    b.coord().reclaim_expired(100).await.unwrap();

    assert_eq!(balances(b, a).await, (1_000, 300));
    b.coord().void(hold).await.unwrap();
}

/// 回收已结算的 Hold 是无害的空操作，不得二次释放
pub async fn reclaiming_a_settled_hold_is_a_no_op(b: &dyn Backend) {
    let a = b.account(1_000).await;
    let hold = b.coord().hold(req(&[a], 300, &key())).await.unwrap();
    let id = hold.id();
    b.coord()
        .capture(hold, Money::from_nanos(100))
        .await
        .unwrap();

    let err = b.coord().void_by_id(id).await.unwrap_err();

    assert!(matches!(err, LedgerError::HoldNotActive), "{err:?}");
    assert_eq!(balances(b, a).await, (900, 0));
}

/// 分段扣款超过剩余冻结额：按实际扣款，但释放量以剩余冻结额为限，
/// 否则 held 会变成负数、账实不符
pub async fn capture_partial_beyond_the_reservation_keeps_held_non_negative(b: &dyn Backend) {
    let a = b.account(1_000).await;
    let hold = b.coord().hold(req(&[a], 100, &key())).await.unwrap();

    b.coord()
        .capture_partial(&hold, Money::from_nanos(300))
        .await
        .unwrap();

    let (remaining, reserved) = balances(b, a).await;
    assert_eq!(remaining, 700, "未按实际金额扣款");
    assert_eq!(reserved, 0, "冻结额被扣成负数");
    assert_consistent(b, a).await;

    b.coord().void(hold).await.unwrap();
    assert_eq!(balances(b, a).await, (700, 0));
}

/// 任何一串操作之后账实都必须相符
pub async fn audit_stays_clean_through_a_lifecycle(b: &dyn Backend) {
    let a = b.account(1_000).await;
    let parent = b.account(1_000).await;

    let h1 = b.coord().hold(req(&[a], 300, &key())).await.unwrap();
    let h2 = b
        .coord()
        .hold(req(&[a, parent], 200, &key()))
        .await
        .unwrap();
    b.coord().extend(&h1, Money::from_nanos(100)).await.unwrap();
    b.coord()
        .capture_partial(&h2, Money::from_nanos(50))
        .await
        .unwrap();
    assert_consistent(b, a).await;
    assert_consistent(b, parent).await;

    b.coord().capture(h1, Money::from_nanos(120)).await.unwrap();
    b.coord().void(h2).await.unwrap();

    assert_consistent(b, a).await;
    assert_consistent(b, parent).await;
    assert_eq!(balances(b, a).await, (830, 0));
}

/// 并发冻结不得超支：可用额度只够 10 笔时，20 个并发请求必须恰好成功 10 笔
pub async fn concurrent_holds_never_overdraw(b: &dyn Backend) {
    let acct = b.account(1_000).await;

    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..20 {
        let coord = b.coord();
        tasks.spawn(async move {
            let idem = key();
            match coord.hold(req(&[acct], 100, &idem)).await {
                Ok(hold) => {
                    std::mem::forget(hold); // 保持冻结，便于核对总量
                    true
                }
                Err(_) => false,
            }
        });
    }
    let granted = tasks.join_all().await.into_iter().filter(|ok| *ok).count();

    assert_eq!(granted, 10, "冻结笔数与可用额度不符");
    assert_eq!(balances(b, acct).await, (1_000, 1_000));
}
