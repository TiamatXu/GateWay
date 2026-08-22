//! 组织树、账户链解析、API Key、目录同步与第三方登录。
//!
//! M0 只定义 `DirectorySource` 契约，实现属于 M7。签名现在定死。
//!
//! 注意 `ledger` **不**依赖本 crate——它只接受 `&[AccountId]`，账户链由
//! `gateway` 解析后传入，两者保持解耦。

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use gw_core::NodeId;

#[derive(Debug, thiserror::Error)]
pub enum DirectoryError {
    #[error("上游目录服务错误: {0}")]
    Upstream(String),
    #[error("同步边界冲突: {0}")]
    Boundary(String),
}

/// 一次全量同步的结果。
#[derive(Debug, Clone)]
pub struct DirectorySnapshot {
    pub provider: String,
    pub nodes: Vec<DirectoryNode>,
    pub taken_at: DateTime<Utc>,
}

/// 同步字段与覆盖层分列：同步只写 `name`，用户的修改落在覆盖层，
/// 原始值始终保留。
#[derive(Debug, Clone)]
pub struct DirectoryNode {
    pub external_id: String,
    pub parent_external_id: Option<String>,
    pub name: String,
    pub deleted: bool,
}

#[derive(Debug, Clone)]
pub enum DirectoryEvent {
    Upserted(DirectoryNode),
    Deleted {
        external_id: String,
    },
    Moved {
        external_id: String,
        new_parent: Option<String>,
    },
    Mapped {
        external_id: String,
        node_id: NodeId,
    },
}

#[async_trait]
pub trait DirectorySource: Send + Sync {
    /// # Errors
    /// 上游目录服务不可达或返回错误。
    async fn full_sync(&self) -> Result<DirectorySnapshot, DirectoryError>;

    /// # Errors
    /// 事件违反同步边界约束，或写入失败。
    async fn apply_event(&self, ev: DirectoryEvent) -> Result<(), DirectoryError>;
}
