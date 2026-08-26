//! 句柄与异步任务的存储契约。
//!
//! 两者共享同一套生命周期概念（签发 → 归属 → 终态 → 结算），先合并为一个
//! 契约；边界在实现中确认清晰后再拆。

use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use gw_core::{
    AccountId, ApiKeyId, ChannelId, HandleId, HandleKind, HoldId, ProviderId, RequestId, TaskPhase,
};
use smallvec::SmallVec;

#[derive(Debug, thiserror::Error)]
pub enum ResourceError {
    #[error("句柄存储读写失败: {0}")]
    Db(#[from] sqlx::Error),
    /// 同一渠道上同一个上游 ID 已属于另一个账户。理论上只有上游把同一个 ID
    /// 发给两个人才会出现，真发生了就是上游或渠道配置有问题，宁可失败。
    #[error("上游 ID {upstream_id} 在渠道 {channel} 上已归属其他账户")]
    OwnerConflict { channel: i64, upstream_id: String },
    /// 描述文件改过、句柄却还在表里。宁可报错也不能当作「查不到」——
    /// 那会把一个真实存在的资源说成不存在。
    #[error("句柄 {id} 的类型 {kind} 已不在描述文件中")]
    UnknownKind { id: HandleId, kind: String },
}

/// 一次签发。
pub struct NewHandle<'a> {
    pub kind: HandleKind,
    pub channel: ChannelId,
    pub provider: &'a ProviderId,
    pub upstream_id: &'a str,
    /// 归属，链首为主计费主体
    pub account_chain: &'a [AccountId],
    pub key_id: Option<ApiKeyId>,
    /// 签发它的端点 id
    pub endpoint: &'a str,
    /// `OnTerminal` 的预扣单号，终态时按它结算
    pub hold_id: Option<HoldId>,
}

#[derive(Debug, Clone)]
pub struct Handle {
    pub id: HandleId,
    pub kind: HandleKind,
    pub channel: ChannelId,
    pub provider: ProviderId,
    pub upstream_id: String,
    pub account_chain: SmallVec<[AccountId; 4]>,
    pub key_id: Option<ApiKeyId>,
    pub endpoint: String,
    pub hold_id: Option<HoldId>,
}

impl Handle {
    /// 调用方的账户链里必须含有该句柄的主计费主体。
    ///
    /// 父账户的链不含子账户，因此父账户的 Key 查不到子账户的任务——
    /// 越权查看是管理面的功能，不该由数据平面顺手放开。
    #[must_use]
    pub fn owned_by(&self, caller_chain: &[AccountId]) -> bool {
        self.account_chain
            .first()
            .is_some_and(|owner| caller_chain.contains(owner))
    }
}

/// 一次提交。任务记录挂在句柄上，1:1。
pub struct NewTask<'a> {
    pub handle: HandleId,
    /// 提交端点 id：`async` 声明与 `usage.actual` 规则都在它身上
    pub submit_endpoint: &'a str,
    /// 提交端点的入站路径，计价与日志用
    pub inbound: &'a str,
    pub model: &'a str,
    pub request_id: RequestId,
}

#[derive(Debug, Clone)]
pub struct Task {
    pub handle: HandleId,
    pub submit_endpoint: String,
    pub inbound: String,
    pub model: String,
    pub request_id: RequestId,
    pub phase: TaskPhase,
    /// 上游的原始状态串，不做归一化——排查时要看到厂商到底说了什么
    pub state: Option<String>,
    /// 提交时刻。计价按它取价格版本，改价不影响已提交的任务。
    pub created_at: DateTime<Utc>,
}

#[async_trait]
pub trait ResourceStore: Send + Sync {
    /// 签发虚拟句柄。同一渠道上同一个上游 ID 重复签发时返回既有句柄——
    /// 提交重试拿到相同上游 ID 时，客户端必须看到同一个虚拟 ID。
    ///
    /// # Errors
    /// 存储不可用，或该上游 ID 已归属其他账户。
    async fn issue(&self, h: NewHandle<'_>) -> Result<HandleId, ResourceError>;

    /// # Errors
    /// 存储不可用。
    async fn resolve(&self, id: HandleId) -> Result<Option<Handle>, ResourceError>;

    /// # Errors
    /// 存储不可用。
    async fn open_task(&self, t: NewTask<'_>) -> Result<(), ResourceError>;

    /// # Errors
    /// 存储不可用。
    async fn task(&self, handle: HandleId) -> Result<Option<Task>, ResourceError>;

    /// 记下一次轮询看到的状态。返回 `Some` 表示**本次调用**把任务推入了终态，
    /// 结算权归调用者；并发轮询里只有一个调用能拿到它。
    ///
    /// # Errors
    /// 存储不可用。
    async fn observe(
        &self,
        handle: HandleId,
        state: &str,
        phase: TaskPhase,
    ) -> Result<Option<Task>, ResourceError>;

    /// 孤儿巡检：提交后久无人问津、仍在运行中的任务。
    ///
    /// # Errors
    /// 存储不可用。
    async fn stale_tasks(
        &self,
        idle_for: Duration,
        limit: i64,
    ) -> Result<Vec<(Handle, Task)>, ResourceError>;
}

pub(crate) fn phase_code(p: TaskPhase) -> i16 {
    match p {
        TaskPhase::Running => 0,
        TaskPhase::Succeeded => 1,
        TaskPhase::Failed => 2,
    }
}

pub(crate) fn phase_of(code: i16) -> TaskPhase {
    match code {
        1 => TaskPhase::Succeeded,
        2 => TaskPhase::Failed,
        _ => TaskPhase::Running,
    }
}
