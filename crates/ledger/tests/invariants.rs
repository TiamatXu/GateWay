//! 账本不变量。任意操作序列之后都必须成立：
//!
//! 1. `held` 恒等于该账户全部活跃 Hold 腿之和（对账无差异）
//! 2. `balance` 恒等于初始值减去全部已扣款之和
//! 3. `held` 不为负

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use gw_core::{AccountId, BillingTiming, Money};
use gw_ledger::{Coordinator, Hold, HoldRequest, MemCoordinator};
use proptest::prelude::*;
use tokio::sync::mpsc;

const ACCOUNTS: u8 = 3;
const INITIAL: i64 = 1_000;

#[derive(Debug, Clone)]
enum Op {
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

fn op_strategy() -> impl Strategy<Value = Op> {
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

/// 执行一串操作，返回各账户的累计扣款
#[allow(clippy::too_many_lines)]
async fn run(
    coord: &MemCoordinator,
    accounts: &[AccountId],
    ops: &[Op],
) -> HashMap<AccountId, i64> {
    let mut charged: HashMap<AccountId, i64> = HashMap::new();
    let mut open: Vec<Open> = Vec::new();
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
                    .hold(HoldRequest {
                        chain: &chain,
                        amount: Money::from_nanos(i64::from(*amount)),
                        ttl: Duration::from_secs(60),
                        idempotency_key: &format!("k{seq}"),
                        timing: BillingTiming::InRequest,
                    })
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
                    .hold(HoldRequest {
                        chain: &chain,
                        amount: Money::from_nanos(i64::from(*amount)),
                        ttl: Duration::from_secs(60),
                        idempotency_key: &format!("k{seq}"),
                        timing: BillingTiming::InRequest,
                    })
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

proptest! {
    #![proptest_config(ProptestConfig::with_cases(96))]

    #[test]
    fn ledger_invariants_hold_after_any_operation_sequence(
        ops in proptest::collection::vec(op_strategy(), 0..40)
    ) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();

        rt.block_on(async {
            // 容量取大：泄漏上报是 best-effort，通道满不影响账本，但会掩盖问题
            let (tx, _rx) = mpsc::channel(4096);
            let coord = Arc::new(MemCoordinator::new(tx));
            let accounts: Vec<AccountId> =
                (0..ACCOUNTS).map(|_| coord.create_account(INITIAL)).collect();

            let charged = run(&coord, &accounts, &ops).await;

            // 不变量 1：对账无差异
            let mismatches = coord.audit(64).await.unwrap();
            prop_assert!(mismatches.is_empty(), "账实不符：{mismatches:?}");

            for account in &accounts {
                let (balance, held) = coord.balances(*account).await.unwrap();
                let expected = INITIAL - charged.get(account).copied().unwrap_or(0);

                // 不变量 2：余额等于初始值减去全部已扣款
                prop_assert_eq!(
                    balance.as_nanos(), expected,
                    "账户 {:?} 余额与扣款记录不符", account
                );
                // 不变量 3：冻结额不为负
                prop_assert!(held.as_nanos() >= 0, "冻结额为负：{}", held.as_nanos());
            }
            Ok(())
        }).unwrap();
    }
}
