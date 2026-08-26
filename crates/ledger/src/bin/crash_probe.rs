//! 混沌测试探针。建一个 Hold，打印其 id，然后在结算之前强杀自己。
//!
//! 用 `std::process::abort()` 而非正常退出：它不运行任何析构，
//! `Hold` 的 `Drop`（泄漏上报 + 投递回收队列）因此不会执行——这正是
//! 节点被 SIGKILL 或断电时的真实情形，也是 TTL 回收器要兜住的场景。
//!
//! 由 `tests/chaos.rs` 以子进程方式调用，不参与线上二进制。

use std::io::Write;
use std::time::Duration;

use gw_core::{AccountId, BillingTiming, Money};
use gw_ledger::{Coordinator, HoldRequest, PgCoordinator};
use tokio::sync::mpsc;

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let account: i64 = args.next().expect("账户 id").parse().expect("账户 id 非法");
    let amount: i64 = args.next().expect("冻结额").parse().expect("冻结额非法");
    let ttl_ms: u64 = args.next().expect("TTL 毫秒").parse().expect("TTL 非法");
    let idempotency_key = args.next().expect("幂等键");

    let url = std::env::var("TEST_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .expect("需要 TEST_DATABASE_URL 或 DATABASE_URL");
    let pool = gw_store::connect(&url, 2).await.expect("连接失败");

    let (tx, _rx) = mpsc::channel(1);
    let coord = PgCoordinator::new(pool, tx);
    let chain = [AccountId(account)];

    let hold = coord
        .hold(HoldRequest {
            chain: &chain,
            amount: Money::from_nanos(amount),
            ttl: Duration::from_millis(ttl_ms),
            idempotency_key: &idempotency_key,
            timing: BillingTiming::InRequest,
        })
        .await
        .expect("冻结失败");

    println!("{}", hold.id());
    std::io::stdout().flush().expect("刷新 stdout 失败");

    // 此处即「Hold 与 Capture 之间」。不给任何收尾机会。
    std::process::abort();
}
