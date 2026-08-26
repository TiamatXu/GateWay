//! 混沌测试：进程在 Hold 与 Capture 之间被强杀。
//!
//! M1 验收标准 3。这条比单元测试值钱的地方在于它验证的是**真实的进程死亡**：
//! Hold 已提交进库，持有它的进程连同内存里的句柄一并消失，没有任何析构跑过。
//! 此时余额必须仍被冻结（钱没被吞，也没被凭空放掉），且 TTL 到期后回收器
//! 必须完整释放，不留残额。

use std::process::Command;
use std::time::Duration;

use gw_core::AccountId;
use gw_ledger::{Coordinator, PgCoordinator};
use sqlx::PgPool;
use tokio::sync::mpsc;

const BALANCE: i64 = 10_000;
const AMOUNT: i64 = 2_500;
const TTL_MS: u64 = 800;

async fn account(pool: &PgPool) -> AccountId {
    let uuid = uuid::Uuid::new_v4();
    let node_id: i64 = sqlx::query_scalar(
        "INSERT INTO org_node (uuid, path, kind, source, name)
         VALUES ($1, text2ltree($2), 0, 0, 'chaos') RETURNING id",
    )
    .bind(uuid)
    .bind(format!("c{}", uuid.simple()))
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
        .bind(BALANCE)
        .execute(pool)
        .await
        .unwrap();

    AccountId(account_id)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hold_survives_a_killed_process_and_is_reclaimed_on_expiry() {
    let pool = gw_store::testkit::pool().await;
    let acct = account(&pool).await;

    let (tx, _rx) = mpsc::channel(16);
    let coord = PgCoordinator::new(pool.clone(), tx);

    let idem = uuid::Uuid::new_v4().to_string();
    let probe = Command::new(env!("CARGO_BIN_EXE_crash_probe"))
        .args([
            acct.0.to_string(),
            AMOUNT.to_string(),
            TTL_MS.to_string(),
            idem,
        ])
        .output()
        .expect("启动探针失败");

    assert!(
        !probe.status.success(),
        "探针应异常终止，实际退出状态：{:?}",
        probe.status
    );
    let hold_id = String::from_utf8(probe.stdout).unwrap().trim().to_owned();
    assert!(!hold_id.is_empty(), "探针未打印 hold id，可能死在冻结之前");

    // 进程没了，句柄没了，但钱必须还冻着——既没被扣，也没被凭空释放
    let (balance, held) = coord.balances(acct).await.unwrap();
    assert_eq!(balance.as_nanos(), BALANCE, "崩溃不应扣款");
    assert_eq!(held.as_nanos(), AMOUNT, "崩溃后冻结额应原样留存");

    // 此刻账实是相符的：冻结额确有一条活跃 Hold 腿对应
    let mismatches: Vec<_> = coord
        .audit(4096)
        .await
        .unwrap()
        .into_iter()
        .filter(|m| m.account == acct)
        .collect();
    assert!(mismatches.is_empty(), "崩溃后账实不符：{mismatches:?}");

    // TTL 到期前不得回收，否则在途请求会被误伤
    coord.reclaim_expired(500).await.unwrap();
    let (_, still_held) = coord.balances(acct).await.unwrap();
    assert_eq!(still_held.as_nanos(), AMOUNT, "未到期的 Hold 不应被回收");

    tokio::time::sleep(Duration::from_millis(TTL_MS + 300)).await;
    coord.reclaim_expired(500).await.unwrap();

    let (balance, held) = coord.balances(acct).await.unwrap();
    assert_eq!(held.as_nanos(), 0, "过期 Hold 未被完整释放，余额泄漏");
    assert_eq!(balance.as_nanos(), BALANCE, "回收不是扣款，余额不应变化");

    let mismatches: Vec<_> = coord
        .audit(4096)
        .await
        .unwrap()
        .into_iter()
        .filter(|m| m.account == acct)
        .collect();
    assert!(mismatches.is_empty(), "回收后账实不符：{mismatches:?}");
}
