//! 异步任务的推进与孤儿巡检。
//!
//! 客户端轮询是任务状态的主要来源，但不能是唯一来源：用户提交完就再不查询是
//! 常态，那笔预扣会一直挂着直到 TTL 到期被撤销——钱不会丢，但额度被白占了
//! 整个 TTL。巡检把这些任务捞出来主动查一次。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use chrono::Utc;
use gw_core::RequestId;
use gw_proxy::{Upstream, prepare_upstream_headers};
use gw_registry::locator::Locator;
use gw_registry::schema::AsyncDef;
use gw_registry::{EndpointDesc, Injected};
use gw_resource::store::{Handle, ResourceStore, Task};
use http::{HeaderMap, HeaderName, HeaderValue};
use http_body_util::{BodyExt, Full, Limited};
use smol_str::SmolStr;
use sqlx::PgPool;
use uuid::Uuid;

use crate::endpoint::{Endpoints, SpecTable};
use crate::settlement::{SettlementCtx, Settler, TaskOutcome};

/// 推进一次任务状态所需的一切。
pub struct Advance<'a> {
    pub resources: &'a dyn ResourceStore,
    pub settler: &'a Settler,
    pub specs: &'a SpecTable,
    /// 提交端点：状态怎么读、哪些算终态、终态用量怎么求，都写在它身上
    pub submit: &'a EndpointDesc,
    pub async_def: &'a AsyncDef,
    pub handle: &'a Handle,
    pub task: &'a Task,
    /// 轮询响应的原始字节，用量抽取按它求值
    pub body: &'a Bytes,
    pub doc: &'a serde_json::Value,
    /// 日志上下文骨架，由调用方按自己的来源填好
    pub ctx: &'a SettlementCtx,
}

/// 用一份轮询响应推进任务状态。抢到终态转换的那次调用负责结算。
///
/// 状态怎么读、哪些串算终态，全部写在**提交**端点上——轮询端点可能是全 provider
/// 共用的一个（阿里云百炼即如此），它自己说不出这个任务算不算完成。
pub async fn advance(a: Advance<'_>) {
    let Advance {
        resources,
        settler,
        specs,
        submit,
        async_def,
        handle,
        task,
        body,
        doc,
        ctx,
    } = a;

    let Some(state) =
        crate::handles::read_locator(&async_def.state, doc, &HashMap::new(), &HeaderMap::new())
    else {
        return;
    };

    let phase = async_def.phase_of(&state);
    let won = match resources.observe(handle.id, &state, phase).await {
        Ok(t) => t,
        Err(e) => {
            tracing::error!(handle = %handle.id, error = %e, "记录任务状态失败");
            return;
        }
    };
    // observe 返回 Some 才是本次调用把任务推入了终态，结算权唯一
    if won.is_none() {
        return;
    }
    let Some(hold_id) = handle.hold_id else {
        tracing::warn!(handle = %handle.id, "任务到达终态但没有托管的预扣");
        return;
    };

    // 终态用量按提交端点的 actual 规则、在轮询响应上求值
    let spec = specs.get(submit).map(|s| Arc::clone(&s.response));
    let (usage, estimated) = spec.map_or_else(
        || (gw_core::UsageVector::new(), false),
        |spec| {
            use gw_core::UsageExtractor;
            let mut ex = gw_meter::JsonUsageExtractor::new(spec);
            ex.feed(body);
            let estimated = ex.estimated();
            (Box::new(ex).finish(), estimated)
        },
    );

    settler
        .settle_task(TaskOutcome {
            hold: hold_id,
            phase,
            usage,
            estimated,
            ctx: SettlementCtx {
                // 终态结算独立成一行日志，与触发它的那次轮询分开
                request_id: RequestId(Uuid::new_v4()),
                model: task.model.clone(),
                endpoint: task.inbound.clone(),
                channel: handle.channel,
                // 价格版本按提交时刻取，改价不影响已提交的任务
                started_at: task.created_at,
                handle_id: Some(handle.id),
                ..ctx.clone()
            },
        })
        .await;
}

#[derive(Debug, Clone, Copy)]
pub struct SweepConfig {
    /// 多久没人查询就算孤儿
    pub idle_for: Duration,
    /// 一轮最多处理多少个
    pub batch: i64,
    pub max_response: usize,
}

impl Default for SweepConfig {
    fn default() -> Self {
        Self {
            idle_for: Duration::from_secs(300),
            batch: 64,
            max_response: 1024 * 1024,
        }
    }
}

/// 孤儿任务巡检器。
pub struct TaskSweeper {
    pool: PgPool,
    resources: Arc<dyn ResourceStore>,
    endpoints: Arc<Endpoints>,
    upstream: Upstream,
    settler: Arc<Settler>,
    config: SweepConfig,
}

impl TaskSweeper {
    #[must_use]
    pub fn new(
        pool: PgPool,
        resources: Arc<dyn ResourceStore>,
        endpoints: Arc<Endpoints>,
        upstream: Upstream,
        settler: Arc<Settler>,
        config: SweepConfig,
    ) -> Self {
        Self {
            pool,
            resources,
            endpoints,
            upstream,
            settler,
            config,
        }
    }

    /// 跑一轮。返回推进过状态的任务数。
    ///
    /// 单轮失败不重试：下一轮会再捞到同一批任务，重试逻辑交给周期本身。
    pub async fn sweep_once(&self) -> usize {
        let stale = match self
            .resources
            .stale_tasks(self.config.idle_for, self.config.batch)
            .await
        {
            Ok(v) => v,
            Err(e) => {
                tracing::error!(error = %e, "巡检取任务失败");
                return 0;
            }
        };

        let (catalog, specs) = self.endpoints.snapshot();
        let mut advanced = 0;
        for (handle, task) in stale {
            let Some(submit) = catalog.endpoint(&handle.provider, &task.submit_endpoint) else {
                continue;
            };
            let Some(async_def) = submit.async_task.as_ref() else {
                continue;
            };
            let Some(poll) = catalog.endpoint(&handle.provider, &async_def.poll) else {
                continue;
            };
            let Some((body, doc)) = self.poll_upstream(&handle, poll).await else {
                continue;
            };
            advance(Advance {
                resources: self.resources.as_ref(),
                settler: &self.settler,
                specs: &specs,
                submit,
                async_def,
                handle: &handle,
                task: &task,
                body: &body,
                doc: &doc,
                ctx: &sweep_ctx(&handle, &task),
            })
            .await;
            advanced += 1;
        }
        advanced
    }

    /// 按轮询端点的声明向上游发一次查询。没有客户端请求，路径参数只能来自句柄本身。
    async fn poll_upstream(
        &self,
        handle: &Handle,
        poll: &EndpointDesc,
    ) -> Option<(Bytes, serde_json::Value)> {
        // 轮询端点把任务 ID 放在路径参数里才能由巡检构造；放在请求体里的
        // 需要一份请求模板，那是描述文件目前装不下的东西，跳过等客户端来查。
        let param = poll.handles.consume.iter().find_map(|f| match &f.at {
            Locator::PathParam(n) if f.kind == gw_core::HandleKind::Task => Some(n.clone()),
            _ => None,
        })?;

        let channel = sqlx::query!(
            "SELECT base_url, credential FROM channel WHERE id = $1 AND enabled",
            handle.channel.0
        )
        .fetch_optional(&self.pool)
        .await
        .ok()
        .flatten()?;

        let params: HashMap<SmolStr, String> = HashMap::from([(param, handle.upstream_id.clone())]);
        let path = poll.upstream.render(&params).ok()?;
        let credential = String::from_utf8(channel.credential).unwrap_or_default();
        let uri = format!("{}{}", channel.base_url.trim_end_matches('/'), path);

        let mut inject = HeaderMap::new();
        for (name, value) in &poll.headers {
            if let (Ok(n), Ok(v)) = (
                HeaderName::try_from(name.as_str()),
                HeaderValue::from_str(value),
            ) {
                inject.insert(n, v);
            }
        }
        if let Ok(Injected::Header { name, value }) = gw_registry::inject(&poll.auth, &credential)
            && let (Ok(n), Ok(v)) = (
                HeaderName::try_from(name.as_str()),
                HeaderValue::from_str(&value),
            )
        {
            inject.insert(n, v);
        }

        let req = http::Request::builder()
            .method(poll.method.as_str())
            .uri(&uri)
            .body(Full::new(Bytes::new()))
            .ok()?;
        let mut req = req;
        *req.headers_mut() = prepare_upstream_headers(&HeaderMap::new(), &inject);

        let resp = match self.upstream.send(req).await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(handle = %handle.id, error = %e, "巡检轮询上游失败");
                return None;
            }
        };
        if !resp.status().is_success() {
            tracing::warn!(handle = %handle.id, status = %resp.status(), "巡检轮询被上游拒绝");
            return None;
        }
        let body = Limited::new(resp.into_body(), self.config.max_response)
            .collect()
            .await
            .ok()?
            .to_bytes();
        let doc = serde_json::from_slice(&body).ok()?;
        Some((body, doc))
    }
}

/// 巡检没有客户端请求，日志上下文只能从句柄与任务还原。
fn sweep_ctx(handle: &Handle, task: &Task) -> SettlementCtx {
    SettlementCtx {
        request_id: RequestId(Uuid::new_v4()),
        key_id: handle.key_id,
        account_chain: handle.account_chain.clone(),
        model: task.model.clone(),
        channel: handle.channel,
        tier: "default".into(),
        endpoint: task.inbound.clone(),
        started_at: task.created_at,
        handle_id: Some(handle.id),
        req_headers: HeaderMap::new(),
        resp_headers: HeaderMap::new(),
    }
}

/// 周期性巡检。`shutdown` 触发后退出。
#[must_use]
pub fn spawn_sweeper(
    sweeper: TaskSweeper,
    interval: Duration,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let n = sweeper.sweep_once().await;
                    if n > 0 {
                        let started = Utc::now();
                        tracing::info!(advanced = n, at = %started, "孤儿任务巡检推进了任务状态");
                    }
                }
                _ = shutdown.changed() => return,
            }
        }
    })
}
