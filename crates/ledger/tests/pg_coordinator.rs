//! `Coordinator` 的 PG 实现。钱的路径，用真实 PG 跑。

#[macro_use]
mod conformance;

use std::sync::Arc;

use async_trait::async_trait;
use conformance::{Backend, balances, key, req};
use gw_core::{AccountId, HoldId, Money};
use gw_ledger::{Coordinator, PgCoordinator};
use sqlx::PgPool;
use tokio::sync::mpsc;

struct PgBackend {
    coord: Arc<PgCoordinator>,
    pool: PgPool,
    leaked: tokio::sync::Mutex<mpsc::Receiver<HoldId>>,
    reclaim_tx: mpsc::Sender<HoldId>,
}

async fn backend() -> PgBackend {
    let pool = gw_store::testkit::pool().await;
    let (tx, rx) = mpsc::channel(64);
    PgBackend {
        coord: Arc::new(PgCoordinator::new(pool.clone(), tx.clone())),
        pool,
        leaked: tokio::sync::Mutex::new(rx),
        reclaim_tx: tx,
    }
}

#[async_trait]
impl Backend for PgBackend {
    /// 每个测试用独立账户，共用一个库也互不干扰
    async fn account(&self, balance: i64) -> AccountId {
        let uuid = uuid::Uuid::new_v4();
        let node_id: i64 = sqlx::query_scalar(
            "INSERT INTO org_node (uuid, path, kind, source, name)
             VALUES ($1, text2ltree($2), 0, 0, 'test') RETURNING id",
        )
        .bind(uuid)
        .bind(format!("t{}", uuid.simple()))
        .fetch_one(&self.pool)
        .await
        .unwrap();

        let account_id: i64 =
            sqlx::query_scalar("INSERT INTO account (node_id) VALUES ($1) RETURNING id")
                .bind(node_id)
                .fetch_one(&self.pool)
                .await
                .unwrap();

        sqlx::query("INSERT INTO account_balance (account_id, shard, balance) VALUES ($1, 0, $2)")
            .bind(account_id)
            .bind(balance)
            .execute(&self.pool)
            .await
            .unwrap();

        AccountId(account_id)
    }

    fn coord(&self) -> Arc<dyn Coordinator> {
        self.coord.clone()
    }
}

conformance_tests!(
    backend();
    hold_reserves_without_moving_balance,
    hold_rejects_when_available_is_short,
    hold_counts_existing_holds_against_available,
    hold_is_idempotent_by_key,
    replaying_a_settled_key_is_reported_as_duplicate,
    nested_chain_reserves_at_every_level,
    nested_chain_rolls_back_when_an_ancestor_is_short,
    empty_chain_is_rejected,
    capture_charges_actual_and_releases_the_rest,
    capture_above_hold_charges_the_actual_amount,
    capture_charges_every_level_of_the_chain,
    void_releases_without_charging,
    settling_twice_is_rejected,
    extend_increases_the_reservation,
    extend_applies_once_per_account_on_a_chain,
    extend_rejects_when_available_is_short,
    capture_partial_charges_without_closing_the_hold,
    repeated_partial_captures_accumulate,
    expired_holds_are_reclaimed,
    reclaim_leaves_active_holds_alone,
    reclaiming_a_settled_hold_is_a_no_op,
    capture_partial_beyond_the_reservation_keeps_held_non_negative,
    audit_stays_clean_through_a_lifecycle,
);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_holds_never_overdraw() {
    conformance::concurrent_holds_never_overdraw(&backend().await).await;
}

// -------------------------------------------------------------- PG 特有断言

async fn entry_kinds(pool: &PgPool, id: AccountId) -> Vec<i16> {
    sqlx::query_scalar("SELECT kind FROM ledger_entry WHERE account_id = $1 ORDER BY id")
        .bind(id.0)
        .fetch_all(pool)
        .await
        .unwrap()
}

/// 账本是 append-only：每步操作都留下分录
#[tokio::test]
async fn every_operation_appends_a_ledger_entry() {
    let b = backend().await;
    let a = b.account(1_000).await;

    let hold = b.coord().hold(req(&[a], 300, &key())).await.unwrap();
    assert_eq!(entry_kinds(&b.pool, a).await, vec![0]);

    b.coord()
        .capture(hold, Money::from_nanos(120))
        .await
        .unwrap();
    assert_eq!(entry_kinds(&b.pool, a).await, vec![0, 1]);
}

#[tokio::test]
async fn void_appends_a_void_entry() {
    let b = backend().await;
    let a = b.account(1_000).await;
    let hold = b.coord().hold(req(&[a], 300, &key())).await.unwrap();

    b.coord().void(hold).await.unwrap();

    assert_eq!(entry_kinds(&b.pool, a).await, vec![0, 2]);
}

/// 不变量：held 恒等于该账户全部活跃 Hold 腿的金额之和
#[tokio::test]
async fn held_always_matches_active_legs() {
    let b = backend().await;
    let a = b.account(1_000).await;
    let h1 = b.coord().hold(req(&[a], 300, &key())).await.unwrap();
    let h2 = b.coord().hold(req(&[a], 200, &key())).await.unwrap();
    b.coord()
        .capture_partial(&h1, Money::from_nanos(50))
        .await
        .unwrap();

    let expected: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(l.amount), 0)::BIGINT FROM hold_leg l
         JOIN hold h ON h.id = l.hold_id
         WHERE l.account_id = $1 AND h.status = 0",
    )
    .bind(a.0)
    .fetch_one(&b.pool)
    .await
    .unwrap();

    assert_eq!(balances(&b, a).await.1, expected);
    b.coord().void(h1).await.unwrap();
    b.coord().void(h2).await.unwrap();
    assert_eq!(balances(&b, a).await.1, 0);
}

/// Hold 未结算即析构：不 panic，只上报并投递到回收队列
#[tokio::test]
async fn dropping_an_unsettled_hold_reports_it_for_reclaim() {
    let b = backend().await;
    let a = b.account(1_000).await;

    let id = {
        let hold = b.coord().hold(req(&[a], 300, &key())).await.unwrap();
        let id = hold.id();
        drop(hold);
        id
    };

    assert_eq!(b.leaked.lock().await.recv().await, Some(id));
    b.coord().void_by_id(id).await.unwrap();
}

#[tokio::test]
async fn settled_hold_does_not_report_a_leak() {
    let b = backend().await;
    let a = b.account(1_000).await;
    let hold = b.coord().hold(req(&[a], 300, &key())).await.unwrap();
    let _ = &b.reclaim_tx;

    b.coord()
        .capture(hold, Money::from_nanos(10))
        .await
        .unwrap();

    assert!(
        b.leaked.lock().await.try_recv().is_err(),
        "已结算的 Hold 不应上报泄漏"
    );
}
