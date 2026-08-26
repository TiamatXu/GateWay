//! 分布式互斥。用于把巡检、回收一类后台任务收敛到单实例执行。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};

/// 持有凭证。析构即释放，因此必须被绑定到变量而非立即丢弃。
#[must_use = "LockGuard 一经析构，锁即释放"]
#[derive(Debug)]
pub struct LockGuard {
    key: String,
    inner: Release,
}

#[derive(Debug)]
enum Release {
    /// `pg`：独占一条已从池中摘出的连接。析构关闭连接，PostgreSQL 随会话结束
    /// 释放 advisory lock——这比 TTL 更及时，且持有者崩溃时不会留下滞留锁。
    #[expect(dead_code, reason = "只为 RAII 持有：连接一析构，advisory lock 即释放")]
    Pg(Box<sqlx::PgConnection>),
    /// `mem`：析构时从持有表移除。
    Mem(Arc<Mutex<HashMap<String, DateTime<Utc>>>>),
}

impl LockGuard {
    pub(crate) fn pg(key: &str, conn: sqlx::PgConnection) -> Self {
        Self {
            key: key.to_owned(),
            inner: Release::Pg(Box::new(conn)),
        }
    }

    pub(crate) fn mem(key: &str, locks: Arc<Mutex<HashMap<String, DateTime<Utc>>>>) -> Self {
        Self {
            key: key.to_owned(),
            inner: Release::Mem(locks),
        }
    }

    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        match &self.inner {
            // 连接随 Box 析构关闭，PostgreSQL 检测到会话结束即释放，无需显式解锁
            Release::Pg(_) => {}
            Release::Mem(locks) => {
                if let Ok(mut held) = locks.lock() {
                    held.remove(&self.key);
                }
            }
        }
    }
}

/// 把任意锁名映射为 advisory lock 的 `bigint` 键。
///
/// 用 SHA-256 而非 `DefaultHasher`：后者的输出不保证跨 Rust 版本稳定，而这个
/// 键要在不同版本的节点之间对齐，一旦不一致互斥就失效了。
pub(crate) fn advisory_key(key: &str) -> i64 {
    let digest = Sha256::digest(key.as_bytes());
    i64::from_be_bytes(digest[..8].try_into().expect("SHA-256 输出至少 8 字节"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn advisory_key_is_stable_and_distinct() {
        assert_eq!(advisory_key("ledger.reclaimer"), advisory_key("ledger.reclaimer"));
        assert_ne!(advisory_key("ledger.reclaimer"), advisory_key("ledger.audit"));
    }
}
