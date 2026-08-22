//! 集成测试脚手架。
//!
//! 连接由 `TEST_DATABASE_URL`（回退 `DATABASE_URL`）指定的 PostgreSQL，每次调用
//! 新建连接池。**不共用连接池**：每个 `#[tokio::test]` 各有独立运行时，跨运行时
//! 复用 sqlx 连接池会因后台任务失去归属而死锁。
//!
//! 各测试用独立的账户行隔离，无需独立数据库。

use sqlx::PgPool;

/// 返回已跑完迁移的连接池。迁移是幂等的，重复调用无副作用。
///
/// # Panics
/// 未配置数据库地址，或连接、迁移失败——测试环境不具备时应当立刻失败。
pub async fn pool() -> PgPool {
    let url = std::env::var("TEST_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .expect("集成测试需要 TEST_DATABASE_URL 或 DATABASE_URL，见 scripts/dev-db.sh");
    crate::connect(&url, 4).await.expect("连接或迁移失败")
}
