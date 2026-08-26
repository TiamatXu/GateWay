//! `PostgreSQL` 实现。句柄与任务是低 QPS 的状态数据，与账本同库同事务边界。

use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use gw_core::{
    AccountId, ApiKeyId, ChannelId, HandleId, HandleKind, HoldId, ProviderId, RequestId, TaskPhase,
};
use smallvec::SmallVec;
use sqlx::PgPool;

use crate::store::{
    Handle, NewHandle, NewTask, ResourceError, ResourceStore, Task, phase_code, phase_of,
};

pub struct PgResourceStore {
    pool: PgPool,
}

impl PgResourceStore {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

fn chain(v: Vec<i64>) -> SmallVec<[AccountId; 4]> {
    v.into_iter().map(AccountId).collect()
}

fn kind_of(id: HandleId, raw: &str) -> Result<HandleKind, ResourceError> {
    HandleKind::parse(raw).ok_or_else(|| ResourceError::UnknownKind {
        id,
        kind: raw.to_owned(),
    })
}

#[async_trait]
impl ResourceStore for PgResourceStore {
    async fn issue(&self, h: NewHandle<'_>) -> Result<HandleId, ResourceError> {
        let fresh = HandleId(uuid::Uuid::new_v4());
        let chain_in: Vec<i64> = h.account_chain.iter().map(|a| a.0).collect();

        // ON CONFLICT DO UPDATE（而非 DO NOTHING）才能在冲突时也 RETURNING 到既有行。
        // 把 id 赋成自身是这条语义唯一的写法。
        let row = sqlx::query!(
            "INSERT INTO resource_handle
                 (id, kind, channel_id, provider, upstream_id, account_chain, key_id, endpoint, hold_id)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
             ON CONFLICT (channel_id, kind, upstream_id)
             DO UPDATE SET id = resource_handle.id
             RETURNING id, account_chain",
            fresh.0,
            h.kind.as_str(),
            h.channel.0,
            h.provider.as_str(),
            h.upstream_id,
            &chain_in,
            h.key_id.map(|k| k.0),
            h.endpoint,
            h.hold_id.map(|i| i.0),
        )
        .fetch_one(&self.pool)
        .await?;

        if row.account_chain.first() != chain_in.first() {
            return Err(ResourceError::OwnerConflict {
                channel: h.channel.0,
                upstream_id: h.upstream_id.to_owned(),
            });
        }
        Ok(HandleId(row.id))
    }

    async fn resolve(&self, id: HandleId) -> Result<Option<Handle>, ResourceError> {
        let Some(r) = sqlx::query!(
            "SELECT id, kind, channel_id, provider, upstream_id, account_chain,
                    key_id, endpoint, hold_id
               FROM resource_handle WHERE id = $1",
            id.0
        )
        .fetch_optional(&self.pool)
        .await?
        else {
            return Ok(None);
        };

        Ok(Some(Handle {
            id: HandleId(r.id),
            kind: kind_of(HandleId(r.id), &r.kind)?,
            channel: ChannelId(r.channel_id),
            provider: ProviderId(r.provider.into()),
            upstream_id: r.upstream_id,
            account_chain: chain(r.account_chain),
            key_id: r.key_id.map(ApiKeyId),
            endpoint: r.endpoint,
            hold_id: r.hold_id.map(HoldId),
        }))
    }

    async fn open_task(&self, t: NewTask<'_>) -> Result<(), ResourceError> {
        // 句柄签发是幂等的，任务开立也必须是：提交重试不该开出第二条任务
        sqlx::query!(
            "INSERT INTO async_task (handle_id, submit_endpoint, inbound, model, request_id)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (handle_id) DO NOTHING",
            t.handle.0,
            t.submit_endpoint,
            t.inbound,
            t.model,
            t.request_id.0,
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn task(&self, handle: HandleId) -> Result<Option<Task>, ResourceError> {
        let row = sqlx::query!(
            "SELECT handle_id, submit_endpoint, inbound, model, request_id, phase, state, created_at
               FROM async_task WHERE handle_id = $1",
            handle.0
        )
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| Task {
            handle: HandleId(r.handle_id),
            submit_endpoint: r.submit_endpoint,
            inbound: r.inbound,
            model: r.model,
            request_id: RequestId(r.request_id),
            phase: phase_of(r.phase),
            state: r.state,
            created_at: r.created_at,
        }))
    }

    async fn observe(
        &self,
        handle: HandleId,
        state: &str,
        phase: TaskPhase,
    ) -> Result<Option<Task>, ResourceError> {
        // `WHERE phase = 0` 就是结算权的抢占：并发轮询里只有一个 UPDATE 能命中。
        let row = sqlx::query!(
            "UPDATE async_task
                SET state = $2,
                    phase = $3,
                    polled_at = now(),
                    settled_at = CASE WHEN $3::SMALLINT <> 0 THEN now() ELSE settled_at END
              WHERE handle_id = $1 AND phase = 0
              RETURNING handle_id, submit_endpoint, inbound, model, request_id, phase, state, created_at",
            handle.0,
            state,
            phase_code(phase),
        )
        .fetch_optional(&self.pool)
        .await?;

        Ok(row
            .map(|r| Task {
                handle: HandleId(r.handle_id),
                submit_endpoint: r.submit_endpoint,
                inbound: r.inbound,
                model: r.model,
                request_id: RequestId(r.request_id),
                phase: phase_of(r.phase),
                state: r.state,
                created_at: r.created_at,
            })
            .filter(|t| t.phase.is_terminal()))
    }

    async fn stale_tasks(
        &self,
        idle_for: Duration,
        limit: i64,
    ) -> Result<Vec<(Handle, Task)>, ResourceError> {
        let cutoff = Utc::now()
            - chrono::Duration::from_std(idle_for).unwrap_or_else(|_| chrono::Duration::zero());
        let rows = sqlx::query!(
            "SELECT h.id, h.kind, h.channel_id, h.provider, h.upstream_id, h.account_chain,
                    h.key_id, h.endpoint, h.hold_id,
                    t.submit_endpoint, t.inbound, t.model, t.request_id, t.phase, t.state, t.created_at
               FROM async_task t JOIN resource_handle h ON h.id = t.handle_id
              WHERE t.phase = 0 AND COALESCE(t.polled_at, t.created_at) < $1
              ORDER BY t.created_at
              LIMIT $2",
            cutoff,
            limit,
        )
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|r| {
                let id = HandleId(r.id);
                Ok((
                    Handle {
                        id,
                        kind: kind_of(id, &r.kind)?,
                        channel: ChannelId(r.channel_id),
                        provider: ProviderId(r.provider.into()),
                        upstream_id: r.upstream_id,
                        account_chain: chain(r.account_chain),
                        key_id: r.key_id.map(ApiKeyId),
                        endpoint: r.endpoint,
                        hold_id: r.hold_id.map(HoldId),
                    },
                    Task {
                        handle: id,
                        submit_endpoint: r.submit_endpoint,
                        inbound: r.inbound,
                        model: r.model,
                        request_id: RequestId(r.request_id),
                        phase: phase_of(r.phase),
                        state: r.state,
                        created_at: r.created_at,
                    },
                ))
            })
            .collect()
    }
}
