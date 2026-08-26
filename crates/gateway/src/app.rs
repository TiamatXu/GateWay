//! 数据平面的请求生命周期编排。
//!
//! 检查顺序即成本递增顺序，不可调换：
//! 本地原子读（准入）→ 主键查询（鉴权）→ 句柄解析 → 路由 → PG 写（冻结）→ 转发。
//! 过载时应在最便宜的位置拒绝，而非查库之后。
//!
//! 句柄解析排在选渠道**之前**：消费句柄的请求必须回到签发它的那个渠道，
//! 亲和是路由的输入而不是结果。

use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Body;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get};
use axum::{Json, Router};
use bytes::Bytes;
use chrono::Utc;
use futures::TryStreamExt;
use gw_core::{
    BillingTiming, ChannelId, HandleId, HandleKind, HandleRole, ProtocolKind, ProviderId,
    RequestId, UsageVector, dims,
};
use gw_ledger::{Coordinator, Hold, HoldRequest, LedgerError};
use gw_meter::{JsonUsageExtractor, SseUsageExtractor, UsageSpec};
use gw_pricing::{PriceCtx, PriceEngine};
use gw_proxy::{Tee, TeeStream, Upstream, prepare_upstream_headers};
use gw_registry::{Catalog, EndpointDesc, InboundMatch, Method};
use gw_resource::ResourceStore;
use gw_resource::store::NewTask;
use http_body_util::{BodyExt, BodyStream, Full, Limited};
use sqlx::PgPool;
use uuid::Uuid;

use crate::admission::{Admission, InflightToken, RejectReason};
use crate::endpoint::{EndpointSpecs, Endpoints, SpecTable};
use crate::handles::{Consumed, HandleReject, IssueCtx, read_locator};
use crate::settlement::{RequestRecord, RequestStatus, SettlementCtx, Settler};
use crate::{LoadGuard, authenticate, error_body, extract_bearer};

#[derive(Debug, Clone, Copy)]
pub struct GatewayConfig {
    /// JSON 端点需读取 body 才能取到 model，故设上限。
    /// 流式与二进制端点不走此路径（M0 未实现）。
    pub max_request_body: usize,
    /// 需要改写的响应（签发句柄、读任务状态）要读全，故设上限。
    pub max_buffered_response: usize,
    pub hold_ttl: Duration,
    /// `OnTerminal` 的预扣托管在任务上，TTL 必须覆盖任务时长。
    /// 它同时是防死冻结的兜底：任务永不终态时由 TTL 回收器撤销。
    pub task_hold_ttl: Duration,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            max_request_body: 4 * 1024 * 1024,
            max_buffered_response: 4 * 1024 * 1024,
            hold_ttl: Duration::from_secs(600),
            task_hold_ttl: Duration::from_hours(24),
        }
    }
}

pub struct AppState {
    pub pool: PgPool,
    pub redactor: gw_core::HeaderRedactor,
    pub load: Arc<LoadGuard>,
    pub coord: Arc<dyn Coordinator>,
    pub pricing: Arc<dyn PriceEngine>,
    pub settler: Arc<Settler>,
    pub upstream: Upstream,
    pub endpoints: Arc<Endpoints>,
    pub resources: Arc<dyn ResourceStore>,
    pub config: GatewayConfig,
}

pub fn router(state: Arc<AppState>) -> Router {
    let limit = state.config.max_request_body;
    Router::new()
        .route("/healthz", get(|| async { StatusCode::OK }))
        // readyz 只反映结构性状态，不反映瞬时负载：过载时摘节点会把流量压向
        // 其余节点引发雪崩。过载只在请求路径上返回 503。
        .route("/readyz", get(readyz))
        // 入站路径由描述文件声明，这里只兜住全部路径交给目录去匹配
        .route("/{*path}", any(proxy_request))
        .layer(DefaultBodyLimit::max(limit))
        .with_state(state)
}

async fn readyz(State(st): State<Arc<AppState>>) -> Response {
    match sqlx::query_scalar!("SELECT 1").fetch_one(&st.pool).await {
        Ok(_) => StatusCode::OK.into_response(),
        Err(e) => {
            tracing::warn!(error = %e, "readyz: 数据库不可达");
            (StatusCode::SERVICE_UNAVAILABLE, "database unreachable").into_response()
        }
    }
}

struct Reject {
    status: StatusCode,
    kind: &'static str,
    message: String,
    retry_after: Option<Duration>,
}

fn reject(status: StatusCode, kind: &'static str, message: impl Into<String>) -> Reject {
    Reject {
        status,
        kind,
        message: message.into(),
        retry_after: None,
    }
}

fn internal() -> Reject {
    reject(
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal_error",
        "内部错误",
    )
}

impl Reject {
    fn into_response(self, protocol: &ProtocolKind) -> Response {
        let body = error_body(protocol, self.status.as_u16(), self.kind, &self.message);
        let mut resp = (self.status, Json(body)).into_response();
        if let Some(after) = self.retry_after
            && let Ok(v) = axum::http::HeaderValue::from_str(&after.as_secs().to_string())
        {
            resp.headers_mut().insert("retry-after", v);
        }
        resp
    }
}

/// 句柄环节的拒绝理由到 HTTP 的映射。资源不存在与不属于你都答 404——
/// 用 403 区分开等于告诉对方「这个 ID 是存在的」。
fn handle_reject(e: HandleReject) -> Reject {
    match e {
        HandleReject::NotFound { .. } | HandleReject::Forbidden { .. } => {
            reject(StatusCode::NOT_FOUND, "not_found", "资源不存在")
        }
        HandleReject::Store(err) => {
            tracing::error!(error = %err, "句柄存储不可用");
            internal()
        }
        other => reject(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            other.to_string(),
        ),
    }
}

async fn proxy_request(
    State(st): State<Arc<AppState>>,
    method: axum::http::Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let path = uri.path().to_owned();
    let (catalog, specs) = st.endpoints.snapshot();
    let Some(method) = to_catalog_method(&method) else {
        return reject(
            StatusCode::METHOD_NOT_ALLOWED,
            "unsupported_method",
            "该方法未被任何端点声明",
        )
        .into_response(&ProtocolKind::OpenAiChat);
    };
    // 错误体的协议形态也来自声明：匹配不到就退回 OpenAI 兼容格式
    let protocol = catalog
        .resolve(method, &path)
        .map_or(ProtocolKind::OpenAiChat, |m| m.route.protocol.clone());

    let snap = Snapshot {
        catalog: &catalog,
        specs: &specs,
    };
    match handle(&st, snap, method, &path, &headers, body).await {
        Ok(resp) => resp,
        Err(r) => r.into_response(&protocol),
    }
}

fn to_catalog_method(m: &axum::http::Method) -> Option<Method> {
    Some(match *m {
        axum::http::Method::GET => Method::Get,
        axum::http::Method::POST => Method::Post,
        axum::http::Method::PUT => Method::Put,
        axum::http::Method::PATCH => Method::Patch,
        axum::http::Method::DELETE => Method::Delete,
        _ => return None,
    })
}

/// 目录与规则表的一份快照。两者同代换新，永远成对传递。
#[derive(Clone, Copy)]
struct Snapshot<'a> {
    catalog: &'a Catalog,
    specs: &'a SpecTable,
}

/// 一次请求走到转发之前已经确定下来的一切。
struct Bound<'a> {
    desc: &'a EndpointDesc,
    specs: Arc<EndpointSpecs>,
    channel_id: ChannelId,
    credential: String,
    consumed: Consumed,
    model: String,
    streaming: bool,
}

#[allow(clippy::too_many_lines)]
async fn handle(
    st: &Arc<AppState>,
    snap: Snapshot<'_>,
    method: Method,
    path: &str,
    headers: &HeaderMap,
    body: Bytes,
) -> Result<Response, Reject> {
    // 1. 准入。最便宜的检查放最前。
    let inflight = match st.load.check() {
        Admission::Allow(token) => token,
        Admission::Reject {
            reason,
            retry_after,
        } => {
            let (status, kind, msg) = match reason {
                RejectReason::Draining => (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "server_draining",
                    "节点正在退出，请重试",
                ),
                RejectReason::RateLimited => (
                    StatusCode::TOO_MANY_REQUESTS,
                    "rate_limit_exceeded",
                    "请求过于频繁",
                ),
                RejectReason::Overloaded(_) => (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "server_overloaded",
                    "服务过载，请稍后重试",
                ),
            };
            return Err(Reject {
                status,
                kind,
                message: msg.into(),
                retry_after,
            });
        }
    };

    // 2. 鉴权。单次主键查询。
    let raw_key = extract_bearer(headers).ok_or_else(|| {
        reject(
            StatusCode::UNAUTHORIZED,
            "invalid_api_key",
            "缺少或格式错误的 Authorization 头",
        )
    })?;
    let principal = authenticate(&st.pool, raw_key).await.map_err(|e| {
        tracing::debug!(error = %e, "鉴权失败");
        reject(StatusCode::UNAUTHORIZED, "invalid_api_key", "API Key 无效")
    })?;

    // 3. 入站解析。端点身份、协议、model 位置、计费时点全部来自描述文件。
    let mut matched = snap.catalog.resolve(method, path).ok_or_else(|| {
        reject(
            StatusCode::NOT_FOUND,
            "unknown_endpoint",
            "没有描述文件声明这个端点",
        )
    })?;

    // 请求体可能为空（RequestForm::None 的端点），空体按 null 处理而非报错
    let mut doc: serde_json::Value = if body.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&body).map_err(|e| {
            reject(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                format!("请求体不是合法 JSON: {e}"),
            )
        })?
    };

    // 4. 句柄解析。要在选渠道之前——亲和是路由的输入。
    let consumed = crate::handles::resolve_consumed(
        st.resources.as_ref(),
        &matched.route.consume,
        &doc,
        &matched.params,
        headers,
        &principal.account_chain,
    )
    .await
    .map_err(handle_reject)?;

    let model = match &matched.route.model {
        Some(loc) => Some(
            read_locator(loc, &doc, &matched.params, headers).ok_or_else(|| {
                reject(
                    StatusCode::BAD_REQUEST,
                    "invalid_request_error",
                    format!("请求里取不到 model（声明位置 {loc}）"),
                )
            })?,
        ),
        None => None,
    };
    let streaming = matched
        .route
        .stream_flag
        .as_ref()
        .and_then(|loc| read_locator(loc, &doc, &matched.params, headers))
        .is_some_and(|v| v == "true");

    // 5. 选渠道。只在承接该入站路径的 provider 里选；引用了句柄时必须回原渠道。
    // SIMPLIFIED(M0): 无过滤与打分，M3 智能路由待专项讨论后接入。
    let providers: Vec<String> = matched
        .route
        .providers()
        .map(|p| p.as_str().to_owned())
        .collect();
    let channel = sqlx::query!(
        "SELECT id, provider, base_url, credential FROM channel \
         WHERE enabled AND provider = ANY($1) AND ($2::BIGINT IS NULL OR id = $2) \
         ORDER BY id LIMIT 1",
        &providers,
        consumed.channel.map(|c| c.0),
    )
    .fetch_optional(&st.pool)
    .await
    .map_err(|e| {
        tracing::error!(error = %e, "查询渠道失败");
        internal()
    })?
    .ok_or_else(|| {
        if consumed.channel.is_some() {
            reject(
                StatusCode::SERVICE_UNAVAILABLE,
                "channel_unavailable",
                "签发该资源的渠道当前不可用，此请求只能走该渠道",
            )
        } else {
            reject(
                StatusCode::SERVICE_UNAVAILABLE,
                "no_channel_available",
                "没有可用渠道",
            )
        }
    })?;

    let provider = ProviderId(channel.provider.as_str().into());
    let desc = matched.binding(&provider).ok_or_else(|| {
        tracing::error!(provider = %channel.provider, path, "渠道的 provider 未绑定该端点");
        internal()
    })?;
    let endpoint_specs = Arc::clone(snap.specs.get(desc).ok_or_else(|| {
        tracing::error!(endpoint = %desc.id, "规则表缺少该端点");
        internal()
    })?);

    // 6. 请求改写：虚拟句柄换回上游 ID。
    let extra_headers = if consumed.is_empty() {
        Vec::new()
    } else {
        crate::handles::rewrite_request(
            &desc.handles.consume,
            &consumed,
            &mut doc,
            &mut matched.params,
            headers,
        )
    };
    let outbound_body = if consumed.is_empty() || body.is_empty() {
        body.clone()
    } else {
        Bytes::from(serde_json::to_vec(&doc).map_err(|e| {
            tracing::error!(error = %e, "改写后的请求体无法序列化");
            internal()
        })?)
    };

    let bound = Bound {
        desc,
        specs: endpoint_specs,
        channel_id: ChannelId(channel.id),
        credential: String::from_utf8(channel.credential).unwrap_or_default(),
        consumed,
        model: model.unwrap_or_default(),
        streaming,
    };

    // 7. 预扣。估算规则读请求体，计费时点来自端点形态。
    let request_id = RequestId(Uuid::new_v4());
    let started_at = Utc::now();

    // 客户端提供 Idempotency-Key 时按其去重；否则每请求独立。
    // 必须按 key_id 隔离，否则一个租户能靠猜键阻塞另一个租户。
    let idempotency_key = headers
        .get("idempotency-key")
        .and_then(|v| v.to_str().ok())
        .filter(|v| !v.is_empty())
        .map_or_else(
            || format!("req:{request_id}"),
            |v| format!("idem:{}:{v}", principal.key_id.0),
        );

    let mut hold = if matches!(bound.desc.shape.billing, BillingTiming::NotBilled) {
        // 不计费端点没有 model、也不该要求配价格：轮询与取消属于此类
        None
    } else {
        let estimate = bound.specs.estimate.evaluate(&doc);
        let price_ctx = PriceCtx {
            model: &bound.model,
            channel: bound.channel_id,
            tier: "default",
            endpoint: path,
            at: started_at,
            max_output_tokens: u32::try_from(estimate.get(dims::MAX_OUTPUT_TOKENS))
                .ok()
                .filter(|v| *v > 0),
            // 请求体已在内存中，估算规则直接数出真实输入量
            input_tokens: Some(estimate.get(dims::INPUT_TOKENS)).filter(|v| *v > 0),
            estimate: Some(&estimate),
        };
        let max_cost = st.pricing.estimate_max(&price_ctx).await.map_err(|e| {
            tracing::warn!(model = %bound.model, error = %e, "预扣估算失败");
            reject(
                StatusCode::BAD_REQUEST,
                "model_not_found",
                format!("模型 {} 无可用价格", bound.model),
            )
        })?;

        // OnTerminal 的预扣要活到任务终态，TTL 与同步请求不是一个量级
        let ttl = if matches!(bound.desc.shape.billing, BillingTiming::OnTerminal) {
            st.config.task_hold_ttl
        } else {
            st.config.hold_ttl
        };

        Some(
            st.coord
                .hold(HoldRequest {
                    chain: &principal.account_chain,
                    amount: max_cost,
                    ttl,
                    idempotency_key: &idempotency_key,
                    timing: bound.desc.shape.billing,
                })
                .await
                .map_err(|e| match e {
                    LedgerError::InsufficientFunds { .. } => reject(
                        StatusCode::PAYMENT_REQUIRED,
                        "insufficient_quota",
                        "额度不足",
                    ),
                    LedgerError::DuplicateRequest { .. } => reject(
                        StatusCode::CONFLICT,
                        "duplicate_request",
                        "该 Idempotency-Key 对应的请求已完成",
                    ),
                    other => {
                        tracing::error!(error = %other, "冻结失败");
                        internal()
                    }
                })?,
        )
    };

    // 8. 转发。上游地址、方法、注入的头全部来自描述文件。
    // 渠道的 base_url 覆盖描述文件的默认值——描述文件给的是厂商官方地址，
    // 渠道可能指向自建代理或另一个地域。
    let uri = build_upstream_uri(&matched, bound.desc, &channel.base_url, &bound.credential)?;
    let mut req = axum::http::Request::builder()
        .method(bound.desc.method.as_str())
        .uri(&uri)
        .body(Full::new(outbound_body))
        .map_err(|e| {
            tracing::error!(error = %e, uri = %uri, "构造上游请求失败");
            internal()
        })?;
    *req.headers_mut() = prepare_upstream_headers(
        headers,
        &upstream_headers(bound.desc, &bound.credential, &extra_headers)?,
    );

    let upstream_resp = match st.upstream.send(req).await {
        Ok(r) => r,
        Err(e) => {
            // 未产生任何用量，直接撤销冻结
            tracing::warn!(error = %e, uri = %uri, "上游请求失败");
            void(st, hold.take()).await;
            return Err(reject(
                StatusCode::BAD_GATEWAY,
                "upstream_error",
                "上游不可达",
            ));
        }
    };

    // 9. 结算上下文。响应头此时才可知。
    let ctx = SettlementCtx {
        request_id,
        key_id: Some(principal.key_id),
        account_chain: principal.account_chain,
        model: bound.model.clone(),
        channel: bound.channel_id,
        tier: "default".into(),
        endpoint: path.to_owned(),
        started_at,
        handle_id: None,
        req_headers: st.redactor.redact(headers),
        resp_headers: st.redactor.redact(upstream_resp.headers()),
    };

    // 10. 响应。要改写句柄或要读任务状态时必须读全，其余一律流式透传。
    let issues = !bound.desc.handles.issue.is_empty();
    let polls = matches!(
        bound.desc.shape.handle,
        HandleRole::Consumes(HandleKind::Task)
    ) && bound.consumed.of_kind(HandleKind::Task).is_some();

    if issues || polls {
        finish_buffered(st, snap, &bound, ctx, hold, upstream_resp, inflight).await
    } else {
        Ok(finish_streaming(
            st,
            &bound,
            ctx,
            hold,
            upstream_resp,
            inflight,
        ))
    }
}

/// 流式透传：响应体一个字节都不落地，结算随流析构。
fn finish_streaming(
    st: &Arc<AppState>,
    bound: &Bound<'_>,
    ctx: SettlementCtx,
    hold: Option<Hold>,
    upstream_resp: axum::http::Response<hyper::body::Incoming>,
    inflight: InflightToken,
) -> Response {
    let is_sse = bound.streaming
        || upstream_resp
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.starts_with("text/event-stream"));

    let spec = Arc::clone(&bound.specs.response);
    let extractor: Box<dyn gw_core::UsageExtractor> = if is_sse {
        Box::new(SseUsageExtractor::new(spec))
    } else {
        Box::new(JsonUsageExtractor::new(spec))
    };
    let tee = Arc::new(Mutex::new(Tee::new(extractor)));
    let guard = crate::SettlementGuard::new(hold, Arc::clone(&tee), ctx, Arc::clone(&st.settler));

    let status = upstream_resp.status();
    let resp_headers = upstream_resp.headers().clone();
    let stream = BodyStream::new(upstream_resp.into_body())
        .try_filter_map(|frame| async move { Ok(frame.into_data().ok()) });

    let out = copy_response_headers(status, &resp_headers);
    // guard 与并发名额随流一同析构：断连、报错、正常结束都会走到
    let body = Body::from_stream(TeeStream::new(stream, tee, Settled(guard, inflight)));
    out.body(body)
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

/// 读全响应：签发句柄要改写响应体，判定任务终态要读状态字段。
///
/// 只有声明了这两件事之一的端点走这条路径——透传优先，缓冲是例外。
#[allow(clippy::too_many_lines)]
async fn finish_buffered(
    st: &Arc<AppState>,
    snap: Snapshot<'_>,
    bound: &Bound<'_>,
    mut ctx: SettlementCtx,
    mut hold: Option<Hold>,
    upstream_resp: axum::http::Response<hyper::body::Incoming>,
    _inflight: InflightToken,
) -> Result<Response, Reject> {
    let status = upstream_resp.status();
    let resp_headers = upstream_resp.headers().clone();

    let bytes = match Limited::new(upstream_resp.into_body(), st.config.max_buffered_response)
        .collect()
        .await
    {
        Ok(c) => c.to_bytes(),
        Err(e) => {
            tracing::warn!(endpoint = %bound.desc.id, error = %e, "读取上游响应失败");
            void(st, hold.take()).await;
            return Err(reject(
                StatusCode::BAD_GATEWAY,
                "upstream_error",
                "上游响应无法读取",
            ));
        }
    };

    let mut doc: Option<serde_json::Value> = serde_json::from_slice(&bytes).ok();
    let usable = status.is_success() && doc.is_some();
    let mut out_bytes = bytes.clone();
    let mut issued: Vec<(HandleKind, HandleId)> = Vec::new();

    // 10a. 签发。上游 ID 一律换成虚拟 ID——客户端从头到尾见不到上游 ID。
    if usable && !bound.desc.handles.issue.is_empty() {
        let d = doc.as_mut().expect("usable 已断言为 Some");
        let issue_ctx = IssueCtx {
            channel: bound.channel_id,
            provider: &bound.desc.provider,
            account_chain: &ctx.account_chain,
            key_id: ctx.key_id,
            endpoint: &bound.desc.id,
            // 只有 OnTerminal 把预扣挂到句柄上：其余时点的 Hold 随请求结束就结算了
            hold_id: matches!(bound.desc.shape.billing, BillingTiming::OnTerminal)
                .then(|| hold.as_ref().map(Hold::id))
                .flatten(),
        };
        match crate::handles::issue_handles(
            st.resources.as_ref(),
            &bound.desc.handles.issue,
            d,
            &issue_ctx,
        )
        .await
        {
            Ok(v) => {
                issued = v;
                out_bytes = Bytes::from(serde_json::to_vec(d).map_err(|e| {
                    tracing::error!(error = %e, "改写后的响应体无法序列化");
                    internal()
                })?);
            }
            Err(e) => {
                // 签发不落库就等于把上游 ID 直接给了客户端，宁可整个请求失败
                tracing::error!(endpoint = %bound.desc.id, error = %e, "签发虚拟句柄失败");
                void(st, hold.take()).await;
                return Err(internal());
            }
        }
    }

    let task_handle = issued
        .iter()
        .find(|(k, _)| *k == HandleKind::Task)
        .map(|(_, id)| *id);
    ctx.handle_id = task_handle.or_else(|| issued.first().map(|(_, id)| *id));

    // 10b. 提交：把预扣托管到任务上，不在此刻结算。
    if matches!(bound.desc.shape.billing, BillingTiming::OnTerminal) {
        let submitted = handle_submission(st, bound, &ctx, &mut hold, &bytes, task_handle).await;
        if submitted {
            return Ok(buffered_response(status, &resp_headers, out_bytes));
        }
    }

    // 10c. 轮询：读状态、记状态，抢到终态的那次调用负责结算。
    if let Some(h) = bound.consumed.of_kind(HandleKind::Task) {
        ctx.handle_id = Some(h.id);
        if let Some(d) = doc.as_ref() {
            observe_task(st, snap, bound, &ctx, h, d, &bytes).await;
        }
    }

    // 10d. 常规结算。响应已读全，没有可等的，构造即析构。
    let mut tee = Tee::new(Box::new(JsonUsageExtractor::new(Arc::clone(
        &bound.specs.response,
    ))));
    tee.feed(&bytes);
    drop(crate::SettlementGuard::new(
        hold,
        Arc::new(Mutex::new(tee)),
        ctx,
        Arc::clone(&st.settler),
    ));

    Ok(buffered_response(status, &resp_headers, out_bytes))
}

/// 提交端点的收尾。返回 `true` 表示预扣已托管给任务，调用方不得再结算。
async fn handle_submission(
    st: &Arc<AppState>,
    bound: &Bound<'_>,
    ctx: &SettlementCtx,
    hold: &mut Option<Hold>,
    body: &Bytes,
    task_handle: Option<HandleId>,
) -> bool {
    let (Some(handle), Some(_async_def)) = (task_handle, bound.desc.async_task.as_ref()) else {
        // 上游没给出任务 ID（提交被拒，或响应与声明不符）：没有可托管的对象
        void(st, hold.take()).await;
        return false;
    };

    if let Err(e) = st
        .resources
        .open_task(NewTask {
            handle,
            submit_endpoint: &bound.desc.id,
            inbound: &ctx.endpoint,
            model: &ctx.model,
            request_id: ctx.request_id,
        })
        .await
    {
        tracing::error!(error = %e, "开立异步任务失败");
        void(st, hold.take()).await;
        return false;
    }

    // on_submit：提交响应可能带回比请求体更准的用量，据此把预扣抬到够用
    let usage = evaluate(&bound.specs.on_submit, body);
    if let Some(h) = hold.as_ref() {
        extend_if_short(st, bound, ctx, h, &usage).await;
    }

    let Some(h) = hold.take() else {
        return false;
    };
    let hold_id = h.detach();
    tracing::info!(handle = %handle, hold = %hold_id, "预扣已托管到异步任务");

    st.settler.log(RequestRecord {
        request_id: ctx.request_id,
        key_id: ctx.key_id,
        account_chain: ctx.account_chain.clone(),
        model: ctx.model.clone(),
        channel: ctx.channel,
        endpoint: ctx.endpoint.clone(),
        usage,
        // 提交本身不产生金额，账在终态那一行
        amount: None,
        status: RequestStatus::Ok,
        handle_id: Some(handle),
        estimated: false,
        req_headers: ctx.req_headers.clone(),
        resp_headers: ctx.resp_headers.clone(),
        started_at: ctx.started_at,
        ended_at: Utc::now(),
    });
    true
}

/// 提交响应给出的用量若已超过预扣额，把预扣抬上去。
///
/// 只抬不降：降低预扣不会少收钱（终态按实际用量捕获），却会让后续可用额度
/// 看起来变多，反而放松了防超支。
async fn extend_if_short(
    st: &Arc<AppState>,
    bound: &Bound<'_>,
    ctx: &SettlementCtx,
    hold: &Hold,
    usage: &UsageVector,
) {
    if usage.is_empty() {
        return;
    }
    let price_ctx = PriceCtx {
        model: &ctx.model,
        channel: bound.channel_id,
        tier: &ctx.tier,
        endpoint: &ctx.endpoint,
        at: ctx.started_at,
        max_output_tokens: None,
        input_tokens: None,
        estimate: None,
    };
    let Ok(quote) = st.pricing.quote(&price_ctx, usage).await else {
        return;
    };
    let Some(delta) = quote.amount.checked_sub(hold.amount()) else {
        return;
    };
    if delta.as_nanos() <= 0 {
        return;
    }
    if let Err(e) = st.coord.extend(hold, delta).await {
        tracing::warn!(error = %e, "按提交响应追加预扣失败，终态仍按实际用量结算");
    }
}

/// 轮询响应的状态判定。真正的推进与结算在 `task::advance` 里，
/// 与孤儿巡检共用一套——两条路径若各写一遍，终态语义必然漂移。
async fn observe_task(
    st: &Arc<AppState>,
    snap: Snapshot<'_>,
    bound: &Bound<'_>,
    ctx: &SettlementCtx,
    handle: &gw_resource::Handle,
    doc: &serde_json::Value,
    body: &Bytes,
) {
    let Ok(Some(task)) = st.resources.task(handle.id).await else {
        return;
    };
    let Some(submit) = snap
        .catalog
        .endpoint(&handle.provider, &task.submit_endpoint)
    else {
        tracing::warn!(endpoint = %task.submit_endpoint, "任务的提交端点已不在目录中");
        return;
    };
    let Some(async_def) = submit.async_task.as_ref() else {
        return;
    };
    // 声明的轮询端点不是这个：不按它的状态表解读，免得误判终态
    if async_def.poll != bound.desc.id {
        return;
    }

    crate::task::advance(crate::task::Advance {
        resources: st.resources.as_ref(),
        settler: &st.settler,
        specs: snap.specs,
        submit,
        async_def,
        handle,
        task: &task,
        body,
        doc,
        ctx,
    })
    .await;
}

fn evaluate(spec: &UsageSpec, body: &Bytes) -> UsageVector {
    if spec.is_empty() {
        return UsageVector::new();
    }
    serde_json::from_slice(body)
        .map(|d: serde_json::Value| spec.evaluate(&d))
        .unwrap_or_default()
}

async fn void(st: &Arc<AppState>, hold: Option<Hold>) {
    let Some(h) = hold else { return };
    if let Err(e) = st.coord.void(h).await {
        tracing::error!(error = %e, "撤销冻结失败");
    }
}

fn copy_response_headers(
    status: StatusCode,
    resp_headers: &HeaderMap,
) -> axum::http::response::Builder {
    let mut out = Response::builder().status(status);
    if let Some(h) = out.headers_mut() {
        for (name, value) in resp_headers {
            // 逐跳头与长度信息由本段连接重新决定
            if matches!(
                name.as_str(),
                "connection" | "transfer-encoding" | "content-length" | "keep-alive"
            ) {
                continue;
            }
            h.append(name.clone(), value.clone());
        }
    }
    out
}

fn buffered_response(status: StatusCode, resp_headers: &HeaderMap, body: Bytes) -> Response {
    copy_response_headers(status, resp_headers)
        .body(Body::from(body))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

/// 拼上游 URL：渠道地址 + 渲染后的上游路径 + 声明的查询参数。
fn build_upstream_uri(
    matched: &InboundMatch<'_>,
    desc: &EndpointDesc,
    channel_base_url: &str,
    credential: &str,
) -> Result<String, Reject> {
    let path = matched.upstream_path(desc).map_err(|e| {
        tracing::error!(endpoint = %desc.id, error = %e, "渲染上游路径失败");
        internal()
    })?;
    let mut uri = format!("{}{}", channel_base_url.trim_end_matches('/'), path);

    let mut query: Vec<(String, String)> = desc.query.clone();
    if let Ok(gw_registry::Injected::Query { name, value }) =
        gw_registry::inject(&desc.auth, credential)
    {
        query.push((name, value));
    }
    if !query.is_empty() {
        let joined = query
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&");
        uri.push(if uri.contains('?') { '&' } else { '?' });
        uri.push_str(&joined);
    }
    Ok(uri)
}

/// 注入上游的头：描述文件声明的固定头、句柄改写后的头，加上凭证注入。
fn upstream_headers(
    desc: &EndpointDesc,
    credential: &str,
    rewritten: &[(String, String)],
) -> Result<HeaderMap, Reject> {
    let mut out = HeaderMap::new();
    let mut put = |name: &str, value: &str| {
        if let (Ok(n), Ok(v)) = (HeaderName::try_from(name), HeaderValue::from_str(value)) {
            out.insert(n, v);
        } else {
            tracing::warn!(endpoint = %desc.id, name, "声明的头名或值不合法，已跳过");
        }
    };
    for (name, value) in &desc.headers {
        put(name, value);
    }
    for (name, value) in rewritten {
        put(name, value);
    }
    if !credential.is_empty()
        && let gw_registry::Injected::Header { name, value } =
            gw_registry::inject(&desc.auth, credential).map_err(|e| {
                tracing::error!(endpoint = %desc.id, error = %e, "凭证注入失败");
                internal()
            })?
    {
        put(&name, &value);
    }
    Ok(out)
}

/// 随响应流一同析构的两样东西：结算哨兵与并发名额。
/// 字段不被读取，存在的意义就是析构。
#[allow(dead_code)]
struct Settled(crate::SettlementGuard, InflightToken);
