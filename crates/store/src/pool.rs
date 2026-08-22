use std::time::Duration;

use sqlx::postgres::{PgPool, PgPoolOptions};

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("数据库连接失败: {0}")]
    Connect(#[from] sqlx::Error),
    #[error("迁移失败: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),
}

/// 建立连接池并跑到最新迁移。
///
/// # Errors
/// 连接失败或迁移失败。
pub async fn connect(url: &str, max_connections: u32) -> Result<PgPool, StoreError> {
    let pool = PgPoolOptions::new()
        .max_connections(max_connections)
        .acquire_timeout(Duration::from_secs(10))
        .connect(url)
        .await?;
    crate::MIGRATOR.run(&pool).await?;
    Ok(pool)
}
