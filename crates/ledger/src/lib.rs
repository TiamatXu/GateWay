//! Hold / Capture 双分录账本。
//!
//! 账本是 append-only 的：`hold` 行不删除、`ledger_entry` 只追加，余额是物化结果。
//! 不变量：`account_balance.held` 恒等于该账户全部活跃 Hold 腿的金额之和。

pub mod error;
pub mod hold;
pub mod lock;
pub mod mem;
pub mod pg;
mod rate;
pub mod rolling;

pub use error::LedgerError;
pub use hold::{Hold, HoldRequest};
pub use lock::LockGuard;
pub use mem::MemCoordinator;
pub use pg::{AuditMismatch, Coordinator, HoldStats, PgCoordinator};
pub use rolling::{RollingHold, RollingPolicy};
