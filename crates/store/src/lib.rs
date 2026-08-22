//! 全部 SQL、迁移与 Repository 实现。

pub mod pool;

#[cfg(feature = "testkit")]
pub mod testkit;

pub use pool::{StoreError, connect};

/// 迁移 SQL 以 embed 方式进二进制，无需外部文件。
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../migrations");
