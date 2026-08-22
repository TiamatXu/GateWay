//! `Coordinator` 的进程内实现。与 pg 跑同一套一致性测试——账本语义不能因后端而异。

#[macro_use]
mod conformance;

use std::sync::Arc;

use async_trait::async_trait;
use conformance::Backend;
use gw_core::AccountId;
use gw_ledger::{Coordinator, MemCoordinator};
use tokio::sync::mpsc;

struct MemBackend {
    coord: Arc<MemCoordinator>,
    _reclaim_rx: mpsc::Receiver<gw_core::HoldId>,
}

#[allow(clippy::unused_async)] // 与 pg 后端保持同一签名，供 conformance_tests! 复用
async fn backend() -> MemBackend {
    let (tx, rx) = mpsc::channel(256);
    MemBackend {
        coord: Arc::new(MemCoordinator::new(tx)),
        _reclaim_rx: rx,
    }
}

#[async_trait]
impl Backend for MemBackend {
    async fn account(&self, balance: i64) -> AccountId {
        self.coord.create_account(balance)
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
