//! `Coordinator` 的 PG 实现。钱的路径，用真实 PG 跑。

use std::time::Duration;

use gw_core::{AccountId, BillingTiming, Money};
use gw_ledger::{Coordinator, HoldRequest, LedgerError, PgCoordinator};
use sqlx::PgPool;
use tokio::sync::mpsc;

// ------------------------------------------------------------------ 测试脚手架

struct Fixture {
    coord: PgCoordinator,
    pool: PgPool,
    leaked: mpsc::Receiver<gw_core::HoldId>,
}

async fn setup() -> Fixture {
    let pool = gw_store::testkit::pool().await;
    let (tx, leaked) = mpsc::channel(64);
    Fixture {
        coord: PgCoordinator::new(pool.clone(), tx),
        pool,
        leaked,
    }
}

/// 建一个自带余额的独立账户，各测试互不干扰
async fn account(pool: &PgPool, balance: i64) -> AccountId {
    let uuid = uuid::Uuid::new_v4();
    let label = format!("t{}", uuid.simple());
    let node_id: i64 = sqlx::query_scalar(
        "INSERT INTO org_node (uuid, path, kind, source, name)
         VALUES ($1, text2ltree($2), 0, 0, 'test') RETURNING id",
    )
    .bind(uuid)
    .bind(&label)
    .fetch_one(pool)
    .await
    .unwrap();

    let account_id: i64 =
        sqlx::query_scalar("INSERT INTO account (node_id) VALUES ($1) RETURNING id")
            .bind(node_id)
            .fetch_one(pool)
            .await
            .unwrap();

    sqlx::query("INSERT INTO account_balance (account_id, shard, balance) VALUES ($1, 0, $2)")
        .bind(account_id)
        .bind(balance)
        .execute(pool)
        .await
        .unwrap();

    AccountId(account_id)
}

async fn balances(pool: &PgPool, id: AccountId) -> (i64, i64) {
    sqlx::query_as("SELECT balance, held FROM account_balance WHERE account_id = $1 AND shard = 0")
        .bind(id.0)
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn entry_kinds(pool: &PgPool, id: AccountId) -> Vec<i16> {
    sqlx::query_scalar("SELECT kind FROM ledger_entry WHERE account_id = $1 ORDER BY id")
        .bind(id.0)
        .fetch_all(pool)
        .await
        .unwrap()
}

/// 账本不变量：held 必须等于该账户全部活跃 Hold 腿的金额之和
async fn assert_held_matches_active_legs(pool: &PgPool, id: AccountId) {
    let expected: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(l.amount), 0)::BIGINT FROM hold_leg l
         JOIN hold h ON h.id = l.hold_id
         WHERE l.account_id = $1 AND h.status = 0",
    )
    .bind(id.0)
    .fetch_one(pool)
    .await
    .unwrap();
    let (_, held) = balances(pool, id).await;
    assert_eq!(held, expected, "held 与活跃 Hold 腿之和不一致");
}

fn req<'a>(chain: &'a [AccountId], amount: i64, key: &'a str) -> HoldRequest<'a> {
    HoldRequest {
        chain,
        amount: Money::from_nanos(amount),
        ttl: Duration::from_secs(60),
        idempotency_key: key,
        timing: BillingTiming::InRequest,
    }
}

fn key() -> String {
    uuid::Uuid::new_v4().to_string()
}

// ---------------------------------------------------------------------- hold

#[tokio::test]
async fn hold_reserves_without_moving_balance() {
    let f = setup().await;
    let a = account(&f.pool, 1_000).await;

    let hold = f.coord.hold(req(&[a], 300, &key())).await.unwrap();

    assert_eq!(balances(&f.pool, a).await, (1_000, 300));
    assert_eq!(entry_kinds(&f.pool, a).await, vec![0]);
    assert_held_matches_active_legs(&f.pool, a).await;
    f.coord.void(hold).await.unwrap();
}

#[tokio::test]
async fn hold_rejects_when_available_is_short() {
    let f = setup().await;
    let a = account(&f.pool, 100).await;

    let err = f.coord.hold(req(&[a], 300, &key())).await.unwrap_err();

    assert!(
        matches!(err, LedgerError::InsufficientFunds { .. }),
        "{err:?}"
    );
    assert_eq!(balances(&f.pool, a).await, (100, 0));
}

/// 已冻结的部分不可再冻结
#[tokio::test]
async fn hold_counts_existing_holds_against_available() {
    let f = setup().await;
    let a = account(&f.pool, 1_000).await;
    let h1 = f.coord.hold(req(&[a], 800, &key())).await.unwrap();

    let err = f.coord.hold(req(&[a], 300, &key())).await.unwrap_err();

    assert!(
        matches!(err, LedgerError::InsufficientFunds { .. }),
        "{err:?}"
    );
    assert_eq!(balances(&f.pool, a).await, (1_000, 800));
    f.coord.void(h1).await.unwrap();
}

/// 重试不得二次冻结
#[tokio::test]
async fn hold_is_idempotent_by_key() {
    let f = setup().await;
    let a = account(&f.pool, 1_000).await;
    let k = key();

    let h1 = f.coord.hold(req(&[a], 300, &k)).await.unwrap();
    let h2 = f.coord.hold(req(&[a], 300, &k)).await.unwrap();

    assert_eq!(h1.id(), h2.id());
    assert_eq!(balances(&f.pool, a).await, (1_000, 300));
    f.coord.void(h1).await.unwrap();
    std::mem::forget(h2); // 同一个 Hold 的第二个句柄，不参与结算
}

/// 嵌套额度：链上每一级都要有额度，任一级不足则整体失败且不留痕
#[tokio::test]
async fn nested_chain_reserves_at_every_level() {
    let f = setup().await;
    let child = account(&f.pool, 1_000).await;
    let parent = account(&f.pool, 1_000).await;

    let hold = f
        .coord
        .hold(req(&[child, parent], 300, &key()))
        .await
        .unwrap();

    assert_eq!(balances(&f.pool, child).await, (1_000, 300));
    assert_eq!(balances(&f.pool, parent).await, (1_000, 300));
    f.coord.void(hold).await.unwrap();
}

#[tokio::test]
async fn nested_chain_rolls_back_when_an_ancestor_is_short() {
    let f = setup().await;
    let child = account(&f.pool, 1_000).await;
    let parent = account(&f.pool, 100).await;

    let err = f
        .coord
        .hold(req(&[child, parent], 300, &key()))
        .await
        .unwrap_err();

    assert!(
        matches!(err, LedgerError::InsufficientFunds { .. }),
        "{err:?}"
    );
    assert_eq!(
        balances(&f.pool, child).await,
        (1_000, 0),
        "子账户冻结未回滚"
    );
    assert_eq!(balances(&f.pool, parent).await, (100, 0));
}

/// 并发冻结不得超支：可用额度只够 10 笔时，20 个并发请求必须恰好成功 10 笔
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_holds_never_overdraw() {
    let fx = setup().await;
    let acct = account(&fx.pool, 1_000).await;
    let coord = std::sync::Arc::new(fx.coord);

    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..20 {
        let coord = std::sync::Arc::clone(&coord);
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
    assert_eq!(balances(&fx.pool, acct).await, (1_000, 1_000));
    assert_held_matches_active_legs(&fx.pool, acct).await;
}

// ------------------------------------------------------------- capture / void

#[tokio::test]
async fn capture_charges_actual_and_releases_the_rest() {
    let f = setup().await;
    let a = account(&f.pool, 1_000).await;
    let hold = f.coord.hold(req(&[a], 300, &key())).await.unwrap();

    f.coord.capture(hold, Money::from_nanos(120)).await.unwrap();

    assert_eq!(balances(&f.pool, a).await, (880, 0));
    assert_eq!(entry_kinds(&f.pool, a).await, vec![0, 1]);
    assert_held_matches_active_legs(&f.pool, a).await;
}

/// 实际用量超出估算是常态，按实际扣款，允许透支为负余额
#[tokio::test]
async fn capture_above_hold_charges_the_actual_amount() {
    let f = setup().await;
    let a = account(&f.pool, 400).await;
    let hold = f.coord.hold(req(&[a], 300, &key())).await.unwrap();

    f.coord.capture(hold, Money::from_nanos(500)).await.unwrap();

    assert_eq!(balances(&f.pool, a).await, (-100, 0));
}

#[tokio::test]
async fn void_releases_without_charging() {
    let f = setup().await;
    let a = account(&f.pool, 1_000).await;
    let hold = f.coord.hold(req(&[a], 300, &key())).await.unwrap();

    f.coord.void(hold).await.unwrap();

    assert_eq!(balances(&f.pool, a).await, (1_000, 0));
    assert_eq!(entry_kinds(&f.pool, a).await, vec![0, 2]);
}

#[tokio::test]
async fn settling_twice_is_rejected() {
    let f = setup().await;
    let a = account(&f.pool, 1_000).await;
    let h1 = f.coord.hold(req(&[a], 300, &key())).await.unwrap();
    let id = h1.id();
    f.coord.capture(h1, Money::from_nanos(100)).await.unwrap();

    let err = f
        .coord
        .capture_by_id(id, Money::from_nanos(100))
        .await
        .unwrap_err();

    assert!(matches!(err, LedgerError::HoldNotActive), "{err:?}");
    assert_eq!(balances(&f.pool, a).await, (900, 0));
}

// ------------------------------------------------------- extend / 滚动结算

#[tokio::test]
async fn extend_increases_the_reservation() {
    let f = setup().await;
    let a = account(&f.pool, 1_000).await;
    let hold = f.coord.hold(req(&[a], 300, &key())).await.unwrap();

    f.coord.extend(&hold, Money::from_nanos(200)).await.unwrap();

    assert_eq!(balances(&f.pool, a).await, (1_000, 500));
    assert_held_matches_active_legs(&f.pool, a).await;
    f.coord.void(hold).await.unwrap();
}

/// 追加额度在链上每一级各加一次，不得按腿数重复累加
#[tokio::test]
async fn extend_applies_once_per_account_on_a_chain() {
    let f = setup().await;
    let child = account(&f.pool, 1_000).await;
    let parent = account(&f.pool, 1_000).await;
    let hold = f
        .coord
        .hold(req(&[child, parent], 300, &key()))
        .await
        .unwrap();

    f.coord.extend(&hold, Money::from_nanos(200)).await.unwrap();

    assert_eq!(balances(&f.pool, child).await, (1_000, 500));
    assert_eq!(balances(&f.pool, parent).await, (1_000, 500));
    assert_held_matches_active_legs(&f.pool, child).await;
    assert_held_matches_active_legs(&f.pool, parent).await;
    f.coord.void(hold).await.unwrap();
}

#[tokio::test]
async fn extend_rejects_when_available_is_short() {
    let f = setup().await;
    let a = account(&f.pool, 400).await;
    let hold = f.coord.hold(req(&[a], 300, &key())).await.unwrap();

    let err = f
        .coord
        .extend(&hold, Money::from_nanos(200))
        .await
        .unwrap_err();

    assert!(
        matches!(err, LedgerError::InsufficientFunds { .. }),
        "{err:?}"
    );
    assert_eq!(balances(&f.pool, a).await, (400, 300));
    f.coord.void(hold).await.unwrap();
}

/// Session / Metered 的滚动结算：分段扣款但不关闭 Hold
#[tokio::test]
async fn capture_partial_charges_without_closing_the_hold() {
    let f = setup().await;
    let a = account(&f.pool, 1_000).await;
    let hold = f.coord.hold(req(&[a], 300, &key())).await.unwrap();

    f.coord
        .capture_partial(&hold, Money::from_nanos(50))
        .await
        .unwrap();

    assert_eq!(balances(&f.pool, a).await, (950, 250));
    assert_held_matches_active_legs(&f.pool, a).await;
    f.coord.void(hold).await.unwrap();
    assert_eq!(balances(&f.pool, a).await, (950, 0));
}

// ------------------------------------------------------------------ 兜底回收

#[tokio::test]
async fn expired_holds_are_reclaimed() {
    let f = setup().await;
    let a = account(&f.pool, 1_000).await;
    let hold = f
        .coord
        .hold(HoldRequest {
            ttl: Duration::ZERO,
            ..req(&[a], 300, &key())
        })
        .await
        .unwrap();
    std::mem::forget(hold); // 模拟进程崩溃，未走 capture/void

    // 计数是全局的，本测试只断言自己这笔被回收
    let n = f.coord.reclaim_expired(100).await.unwrap();

    assert!(n >= 1);
    assert_eq!(balances(&f.pool, a).await, (1_000, 0));
    assert_eq!(entry_kinds(&f.pool, a).await, vec![0, 5]);
}

#[tokio::test]
async fn reclaim_leaves_active_holds_alone() {
    let f = setup().await;
    let a = account(&f.pool, 1_000).await;
    let hold = f.coord.hold(req(&[a], 300, &key())).await.unwrap();

    f.coord.reclaim_expired(100).await.unwrap();

    assert_eq!(balances(&f.pool, a).await, (1_000, 300));
    f.coord.void(hold).await.unwrap();
}

/// Hold 未结算即析构：不 panic（析构可能发生在任务取消或已 panicking 期间），
/// 只上报并投递到回收队列
#[tokio::test]
async fn dropping_an_unsettled_hold_reports_it_for_reclaim() {
    let mut f = setup().await;
    let a = account(&f.pool, 1_000).await;

    let id = {
        let hold = f.coord.hold(req(&[a], 300, &key())).await.unwrap();
        let id = hold.id();
        drop(hold);
        id
    };

    assert_eq!(f.leaked.recv().await, Some(id));
    f.coord.void_by_id(id).await.unwrap();
}

#[tokio::test]
async fn settled_hold_does_not_report_a_leak() {
    let mut f = setup().await;
    let a = account(&f.pool, 1_000).await;
    let hold = f.coord.hold(req(&[a], 300, &key())).await.unwrap();

    f.coord.capture(hold, Money::from_nanos(10)).await.unwrap();

    assert!(f.leaked.try_recv().is_err(), "已结算的 Hold 不应上报泄漏");
}
