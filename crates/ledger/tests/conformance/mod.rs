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

// ------------------------------------------------------------ 限流与分布式锁

pub async fn rate_allow_spends_the_burst_then_denies(b: &dyn Backend) {
    let k = gw_core::RateKey(uuid::Uuid::new_v4().to_string().into());
    let coord = b.coord();

    // 补充速率 1/秒、桶容量 3：前三次拿满，第四次在补满前必然被拒
    for i in 0..3 {
        assert!(coord.rate_allow(&k, 1, 3).await.unwrap(), "第 {i} 次应放行");
    }
    assert!(!coord.rate_allow(&k, 1, 3).await.unwrap(), "桶空后应拒绝");
}

pub async fn rate_allow_is_unlimited_when_unconfigured(b: &dyn Backend) {
    let k = gw_core::RateKey(uuid::Uuid::new_v4().to_string().into());
    let coord = b.coord();
    for _ in 0..50 {
        assert!(coord.rate_allow(&k, 0, 10).await.unwrap(), "rate=0 不限流");
        assert!(coord.rate_allow(&k, 10, 0).await.unwrap(), "burst=0 不限流");
    }
}

pub async fn rate_allow_keeps_keys_independent(b: &dyn Backend) {
    let a = gw_core::RateKey(uuid::Uuid::new_v4().to_string().into());
    let c = gw_core::RateKey(uuid::Uuid::new_v4().to_string().into());
    let coord = b.coord();

    assert!(coord.rate_allow(&a, 1, 1).await.unwrap());
    assert!(!coord.rate_allow(&a, 1, 1).await.unwrap());
    assert!(
        coord.rate_allow(&c, 1, 1).await.unwrap(),
        "另一个桶不该受影响"
    );
}

pub async fn lock_is_exclusive_while_held(b: &dyn Backend) {
    let name = key();
    let coord = b.coord();

    let guard = coord
        .try_lock(&name, Duration::from_secs(30))
        .await
        .unwrap()
        .expect("首次应取得锁");
    assert_eq!(guard.key(), name);

    assert!(
        coord
            .try_lock(&name, Duration::from_secs(30))
            .await
            .unwrap()
            .is_none(),
        "锁被持有期间不得二次取得"
    );
    drop(guard);
}

pub async fn lock_is_reacquirable_after_release(b: &dyn Backend) {
    let name = key();
    let coord = b.coord();

    let guard = coord
        .try_lock(&name, Duration::from_secs(30))
        .await
        .unwrap()
        .expect("首次应取得锁");
    drop(guard);

    // pg 实现靠关闭连接释放，服务端观察到会话结束有微小延迟
    for _ in 0..50 {
        if let Some(again) = coord
            .try_lock(&name, Duration::from_secs(30))
            .await
            .unwrap()
        {
            drop(again);
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("释放后应能重新取得锁");
}

pub async fn different_lock_keys_do_not_conflict(b: &dyn Backend) {
    let coord = b.coord();
    let one = coord
        .try_lock(&key(), Duration::from_secs(30))
        .await
        .unwrap()
        .expect("第一把锁");
    let two = coord
        .try_lock(&key(), Duration::from_secs(30))
        .await
        .unwrap()
        .expect("不同锁名互不影响");
    drop((one, two));
}

// ---------------------------------------------------------------- 不变量穷举
//
// 任意操作序列之后，三条不变量都必须成立：
//
// 1. `held` 恒等于该账户全部活跃 Hold 腿之和（对账无差异）
// 2. `balance` 恒等于初始值减去全部已扣款之和
// 3. `held` 不为负
//
// 两种实现跑同一组不变量：`mem` 用例多、跑得快，`pg` 用例少但压的是真实 SQL。

use std::collections::HashMap;

use gw_ledger::Hold;
use proptest::prelude::*;

pub const ACCOUNTS: u8 = 3;
pub const INITIAL: i64 = 1_000;

#[derive(Debug, Clone)]
pub enum Op {
    /// 单账户冻结
    Hold {
        account: u8,
        amount: u16,
    },
    /// 两级账户链冻结，覆盖嵌套额度
    HoldChain {
        child: u8,
        parent: u8,
        amount: u16,
    },
    Capture {
        hold: u8,
        amount: u16,
    },
    Void {
        hold: u8,
    },
    Extend {
        hold: u8,
        delta: u16,
    },
    Partial {
        hold: u8,
        amount: u16,
    },
    Reclaim,
}

pub fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        (0u8..ACCOUNTS, 1u16..200).prop_map(|(account, amount)| Op::Hold { account, amount }),
        (0u8..ACCOUNTS, 0u8..ACCOUNTS, 1u16..200).prop_map(|(child, parent, amount)| {
            Op::HoldChain {
                child,
                parent,
                amount,
            }
        }),
        (0u8..16, 0u16..300).prop_map(|(hold, amount)| Op::Capture { hold, amount }),
        (0u8..16).prop_map(|hold| Op::Void { hold }),
        (0u8..16, 1u16..100).prop_map(|(hold, delta)| Op::Extend { hold, delta }),
        (0u8..16, 1u16..50).prop_map(|(hold, amount)| Op::Partial { hold, amount }),
        Just(Op::Reclaim),
    ]
}

/// 已开出的 Hold 及其账户链——扣款时要按链上每一级记账
struct Open {
    hold: Hold,
    chain: Vec<AccountId>,
}

/// 执行一串操作，返回各账户的累计扣款。
#[allow(clippy::too_many_lines)]
async fn run(
    coord: &dyn Coordinator,
    accounts: &[AccountId],
    ops: &[Op],
) -> HashMap<AccountId, i64> {
    let mut charged: HashMap<AccountId, i64> = HashMap::new();
    let mut open: Vec<Open> = Vec::new();
    // 幂等键在 pg 后端跨用例共用一张表，加运行前缀避免不同用例互相命中
    let run_id = uuid::Uuid::new_v4().simple().to_string();
    let mut seq = 0u64;

    let charge = |charged: &mut HashMap<AccountId, i64>, chain: &[AccountId], amount: i64| {
        for a in chain {
            *charged.entry(*a).or_default() += amount;
        }
    };

    for op in ops {
        seq += 1;
        match op {
            Op::Hold { account, amount } => {
                let chain = vec![accounts[usize::from(*account % ACCOUNTS)]];
                if let Ok(hold) = coord
                    .hold(req(&chain, i64::from(*amount), &format!("{run_id}-{seq}")))
                    .await
                {
                    open.push(Open { hold, chain });
                }
            }
            Op::HoldChain {
                child,
                parent,
                amount,
            } => {
                let (c, p) = (
                    accounts[usize::from(*child % ACCOUNTS)],
                    accounts[usize::from(*parent % ACCOUNTS)],
                );
                // 同一账户出现两次不是合法的账户链，跳过
                if c == p {
                    continue;
                }
                let chain = vec![c, p];
                if let Ok(hold) = coord
                    .hold(req(&chain, i64::from(*amount), &format!("{run_id}-{seq}")))
                    .await
                {
                    open.push(Open { hold, chain });
                }
            }
            Op::Capture { hold, amount } => {
                if open.is_empty() {
                    continue;
                }
                let idx = *hold as usize % open.len();
                let entry = open.remove(idx);
                let amount = i64::from(*amount);
                if coord
                    .capture(entry.hold, Money::from_nanos(amount))
                    .await
                    .is_ok()
                {
                    charge(&mut charged, &entry.chain, amount);
                }
            }
            Op::Void { hold } => {
                if open.is_empty() {
                    continue;
                }
                let idx = *hold as usize % open.len();
                let entry = open.remove(idx);
                let _ = coord.void(entry.hold).await;
            }
            Op::Extend { hold, delta } => {
                if open.is_empty() {
                    continue;
                }
                let idx = *hold as usize % open.len();
                let _ = coord
                    .extend(&open[idx].hold, Money::from_nanos(i64::from(*delta)))
                    .await;
            }
            Op::Partial { hold, amount } => {
                if open.is_empty() {
                    continue;
                }
                let idx = *hold as usize % open.len();
                let amount = i64::from(*amount);
                if coord
                    .capture_partial(&open[idx].hold, Money::from_nanos(amount))
                    .await
                    .is_ok()
                {
                    let chain = open[idx].chain.clone();
                    charge(&mut charged, &chain, amount);
                }
            }
            Op::Reclaim => {
                let _ = coord.reclaim_expired(64).await;
            }
        }
    }

    // 剩余 Hold 保持活跃：对账在有活跃 Hold 时同样必须相符
    for entry in open {
        std::mem::forget(entry.hold);
    }
    charged
}

/// 跑完 `ops` 后校验三条不变量。失败返回描述，供 `prop_assert` 使用。
///
/// # Errors
/// 任一不变量被破坏时返回原因。
pub async fn check_invariants(b: &dyn Backend, ops: &[Op]) -> Result<(), String> {
    let coord = b.coord();
    let mut accounts = Vec::with_capacity(ACCOUNTS as usize);
    for _ in 0..ACCOUNTS {
        accounts.push(b.account(INITIAL).await);
    }

    let charged = run(coord.as_ref(), &accounts, ops).await;

    for account in &accounts {
        // 不变量 1：对账无差异。全库扫描会看到别的测试的账户，只看自己的。
        let mine: Vec<_> = coord
            .audit(4096)
            .await
            .map_err(|e| format!("对账查询失败：{e}"))?
            .into_iter()
            .filter(|m| m.account == *account)
            .collect();
        if !mine.is_empty() {
            return Err(format!("账实不符：{mine:?}"));
        }

        let (balance, held) = coord
            .balances(*account)
            .await
            .map_err(|e| format!("余额查询失败：{e}"))?;
        let expected = INITIAL - charged.get(account).copied().unwrap_or(0);

        // 不变量 2：余额等于初始值减去全部已扣款
        if balance.as_nanos() != expected {
            return Err(format!(
                "账户 {account:?} 余额与扣款记录不符：实际 {}，应为 {expected}",
                balance.as_nanos()
            ));
        }
        // 不变量 3：冻结额不为负
        if held.as_nanos() < 0 {
            return Err(format!("冻结额为负：{}", held.as_nanos()));
        }
    }
    Ok(())
}

// -------------------------------------------------------------- 高并发压力

/// 无依赖的确定性伪随机：压测要能复现，又不值得为此引入 rand。
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// 多任务并发跑随机操作序列，结束后三条不变量必须全部成立。
///
/// 这是 M1 验收标准 1。它压的是 `pg` 的行锁与事务隔离——账户链交叉加锁若有
/// 顺序错误，这里会以死锁或超时的形式暴露；扣款与释放若不在同一事务，
/// 对账会立刻发现差额。
pub async fn concurrent_operations_preserve_invariants(b: &dyn Backend) {
    const ACCOUNTS: usize = 4;
    const TASKS: u64 = 24;
    const OPS_PER_TASK: u64 = 16;
    const INITIAL: i64 = 1_000_000;

    let mut accounts = Vec::with_capacity(ACCOUNTS);
    for _ in 0..ACCOUNTS {
        accounts.push(b.account(INITIAL).await);
    }
    let accounts = Arc::new(accounts);
    let charged = Arc::new(std::sync::Mutex::new(std::collections::HashMap::<
        AccountId,
        i64,
    >::new()));

    let mut tasks = tokio::task::JoinSet::new();
    for t in 0..TASKS {
        let coord = b.coord();
        let accounts = Arc::clone(&accounts);
        let charged = Arc::clone(&charged);

        tasks.spawn(async move {
            let mut rng = Rng(t * 2_654_435_761 + 1);
            for op in 0..OPS_PER_TASK {
                let amount = 1 + i64::try_from(rng.below(500)).unwrap_or(1);

                // 一半单账户、一半两级链。链首链尾随机，专门制造交叉加锁
                let first = accounts[usize::try_from(rng.below(ACCOUNTS as u64)).unwrap_or(0)];
                let second = accounts[usize::try_from(rng.below(ACCOUNTS as u64)).unwrap_or(0)];
                let chain: Vec<AccountId> = if rng.below(2) == 0 || first == second {
                    vec![first]
                } else {
                    vec![first, second]
                };

                let idem = format!("stress-{t}-{op}-{}", uuid::Uuid::new_v4().simple());
                let Ok(hold) = coord.hold(req(&chain, amount, &idem)).await else {
                    continue; // 余额不足是合法结果，不是失败
                };

                match rng.below(4) {
                    // 扣一部分后关闭
                    0 => {
                        let part = amount / 2;
                        if coord
                            .capture_partial(&hold, Money::from_nanos(part))
                            .await
                            .is_ok()
                        {
                            record(&charged, &chain, part);
                        }
                        if coord.capture(hold, Money::from_nanos(0)).await.is_ok() {
                            // 关闭时不再扣款
                        }
                    }
                    // 追加冻结后全额扣
                    1 => {
                        let _ = coord.extend(&hold, Money::from_nanos(amount)).await;
                        if coord.capture(hold, Money::from_nanos(amount)).await.is_ok() {
                            record(&charged, &chain, amount);
                        }
                    }
                    // 撤销
                    2 => {
                        let _ = coord.void(hold).await;
                    }
                    // 按实际扣
                    _ => {
                        let actual = amount
                            - i64::try_from(rng.below(u64::try_from(amount).unwrap_or(1)))
                                .unwrap_or(0);
                        if coord.capture(hold, Money::from_nanos(actual)).await.is_ok() {
                            record(&charged, &chain, actual);
                        }
                    }
                }
            }
        });
    }
    tasks.join_all().await;

    let coord = b.coord();
    // 先取出快照：断言里有 await，锁不能跨过去
    let charged = charged.lock().unwrap().clone();
    for account in accounts.iter() {
        assert_consistent(b, *account).await;

        let (balance, held) = coord.balances(*account).await.unwrap();
        let expected = INITIAL - charged.get(account).copied().unwrap_or(0);
        assert_eq!(
            balance.as_nanos(),
            expected,
            "账户 {account:?} 余额与扣款记录不符"
        );
        assert!(held.as_nanos() >= 0, "冻结额为负：{}", held.as_nanos());
        // 全部 Hold 都已结算，冻结额必须归零
        assert_eq!(held.as_nanos(), 0, "账户 {account:?} 仍有残留冻结");
    }
}

fn record(
    charged: &std::sync::Mutex<std::collections::HashMap<AccountId, i64>>,
    chain: &[AccountId],
    amount: i64,
) {
    let mut g = charged.lock().unwrap();
    for a in chain {
        *g.entry(*a).or_default() += amount;
    }
}

// ------------------------------------------------------------------ 滚动 Hold

use gw_ledger::{RollingHold, RollingPolicy};

fn rolling_req<'a>(chain: &'a [AccountId], amount: i64, key: &'a str) -> HoldRequest<'a> {
    HoldRequest {
        chain,
        amount: Money::from_nanos(amount),
        ttl: Duration::from_secs(60),
        idempotency_key: key,
        timing: BillingTiming::Session,
    }
}

pub async fn rolling_rejects_non_rolling_timings(b: &dyn Backend) {
    let a = b.account(1_000).await;
    let chain = [a];
    let policy = RollingPolicy {
        low_watermark: Money::from_nanos(0),
        top_up: Money::from_nanos(100),
    };
    let idem = key();

    let err = RollingHold::open(b.coord(), req(&chain, 100, &idem), policy)
        .await
        .expect_err("InRequest 不该开出滚动 Hold");
    assert!(
        matches!(err, LedgerError::TimingNotRolling { .. }),
        "{err:?}"
    );
}

pub async fn rolling_charges_incrementally_without_closing(b: &dyn Backend) {
    let a = b.account(1_000).await;
    let chain = [a];
    let idem = key();
    let mut roll = RollingHold::open(
        b.coord(),
        rolling_req(&chain, 300, &idem),
        RollingPolicy {
            low_watermark: Money::from_nanos(0),
            top_up: Money::from_nanos(200),
        },
    )
    .await
    .unwrap();

    assert_eq!(balances(b, a).await, (1_000, 300), "开户即冻结，余额不动");

    roll.charge(Money::from_nanos(50)).await.unwrap();
    assert_eq!(balances(b, a).await, (950, 250));
    assert_eq!(roll.reserved().as_nanos(), 250);

    roll.charge(Money::from_nanos(70)).await.unwrap();
    assert_eq!(balances(b, a).await, (880, 180));

    assert_consistent(b, a).await;
    roll.close().await.unwrap();

    // 收尾释放剩余冻结，不再扣款
    assert_eq!(balances(b, a).await, (880, 0));
    assert_consistent(b, a).await;
}

pub async fn rolling_tops_up_when_the_reservation_runs_low(b: &dyn Backend) {
    let a = b.account(10_000).await;
    let chain = [a];
    let idem = key();
    let mut roll = RollingHold::open(
        b.coord(),
        rolling_req(&chain, 100, &idem),
        RollingPolicy {
            low_watermark: Money::from_nanos(50),
            top_up: Money::from_nanos(400),
        },
    )
    .await
    .unwrap();

    // 扣 80：剩余会跌到 20，低于水位 50，故先续 400
    roll.charge(Money::from_nanos(80)).await.unwrap();
    assert_eq!(roll.reserved().as_nanos(), 420, "续期后应为 100 + 400 - 80");
    assert_eq!(balances(b, a).await, (9_920, 420));

    assert_consistent(b, a).await;
    roll.close().await.unwrap();
    assert_eq!(balances(b, a).await, (9_920, 0));
}

pub async fn rolling_stops_before_charging_when_funds_run_out(b: &dyn Backend) {
    let a = b.account(200).await;
    let chain = [a];
    let idem = key();
    let mut roll = RollingHold::open(
        b.coord(),
        rolling_req(&chain, 100, &idem),
        RollingPolicy {
            low_watermark: Money::from_nanos(0),
            top_up: Money::from_nanos(1_000),
        },
    )
    .await
    .unwrap();

    // 续期需要 1000，可用只剩 100——必须在扣款之前失败
    let err = roll
        .charge(Money::from_nanos(150))
        .await
        .expect_err("额度不足时不得扣款");
    assert!(
        matches!(err, LedgerError::InsufficientFunds { .. }),
        "{err:?}"
    );
    assert_eq!(balances(b, a).await, (200, 100), "失败不得留下扣款痕迹");

    roll.abandon().await.unwrap();
    assert_eq!(balances(b, a).await, (200, 0));
    assert_consistent(b, a).await;
}

pub async fn rolling_abandon_keeps_earlier_charges(b: &dyn Backend) {
    let a = b.account(1_000).await;
    let chain = [a];
    let idem = key();
    let mut roll = RollingHold::open(
        b.coord(),
        rolling_req(&chain, 400, &idem),
        RollingPolicy {
            low_watermark: Money::from_nanos(0),
            top_up: Money::from_nanos(100),
        },
    )
    .await
    .unwrap();

    roll.charge(Money::from_nanos(120)).await.unwrap();
    // 会话异常断开：已产生的用量照收，剩余冻结释放
    roll.abandon().await.unwrap();

    assert_eq!(balances(b, a).await, (880, 0));
    assert_consistent(b, a).await;
}

pub async fn rolling_charges_every_level_of_the_chain(b: &dyn Backend) {
    let child = b.account(1_000).await;
    let parent = b.account(1_000).await;
    let chain = [child, parent];
    let idem = key();
    let mut roll = RollingHold::open(
        b.coord(),
        rolling_req(&chain, 300, &idem),
        RollingPolicy {
            low_watermark: Money::from_nanos(0),
            top_up: Money::from_nanos(100),
        },
    )
    .await
    .unwrap();

    roll.charge(Money::from_nanos(90)).await.unwrap();
    roll.close().await.unwrap();

    assert_eq!(balances(b, child).await, (910, 0));
    assert_eq!(balances(b, parent).await, (910, 0), "上级同额记账");
    assert_consistent(b, child).await;
    assert_consistent(b, parent).await;
}
