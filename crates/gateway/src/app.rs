//! 数据平面的请求生命周期编排。
//!
//! 检查顺序即成本递增顺序，不可调换：
//! 本地原子读（准入）→ 主键查询（鉴权）→ 路由 → PG 写（冻结）→ 转发。
//! 过载时应在最便宜的位置拒绝，而非查库之后。

use std::collections::HashMap;
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
use gw_core::{ChannelId, ProtocolKind, ProviderId, RequestId, dims};
use gw_ledger::{Coordinator, HoldRequest, LedgerError};
use gw_meter::{JsonUsageExtractor, SseUsageExtractor};
use gw_pricing::{PriceCtx, PriceEngine};
use gw_proxy::{Tee, TeeStream, Upstream, prepare_upstream_headers};
use gw_registry::{EndpointDesc, InboundMatch, Locator, Method};
use http_body_util::{BodyStream, Full};
use sqlx::PgPool;
use uuid::Uuid;

use crate::admission::{Admission, InflightToken, RejectReason};
use crate::endpoint::Endpoints;
use crate::settlement::{SettlementCtx, Settler};
use crate::{LoadGuard, authenticate, error_body, extract_bearer};

#[derive(Debug, Clone, Copy)]
pub struct GatewayConfig {
    /// JSON 端点需读取 body 才能取到 model，故设上限。
    /// 流式与二进制端点不走此路径（M0 未实现）。
    pub max_request_body: usize,
    pub hold_ttl: Duration,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            max_request_body: 4 * 1024 * 1024,
            hold_ttl: Duration::from_secs(600),
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

    match handle(&st, &catalog, &specs, method, &path, &headers, body).await {
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

/// 按声明的位置取一个字符串值。三种前缀是封闭集合，无需分支到厂商。
fn read_locator(
    loc: &Locator,
    doc: &serde_json::Value,
    params: &HashMap<smol_str::SmolStr, String>,
    headers: &HeaderMap,
) -> Option<String> {
    match loc {
        Locator::Body(p) => p.one(doc).map(|v| match v {
            serde_json::Value::String(s) => s.clone(),
            other => other.to_string(),
        }),
        Locator::PathParam(n) => params.get(n).cloned(),
        Locator::Header(n) => headers
            .get(n.as_str())
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned),
    }
}

#[allow(clippy::too_many_lines)]
async fn handle(
    st: &Arc<AppState>,
    catalog: &gw_registry::Catalog,
    specs: &crate::endpoint::SpecTable,
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
    let matched = catalog.resolve(method, path).ok_or_else(|| {
        reject(
            StatusCode::NOT_FOUND,
            "unknown_endpoint",
            "没有描述文件声明这个端点",
        )
    })?;

    // 请求体可能为空（RequestForm::None 的端点），空体按 null 处理而非报错
    let doc: serde_json::Value = if body.is_empty() {
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

    // 4. 选渠道。只在承接该入站路径的 provider 里选。
    // SIMPLIFIED(M0): 无过滤与打分，M3 接入路由策略。
    let providers: Vec<String> = matched
        .route
        .providers()
        .map(|p| p.as_str().to_owned())
        .collect();
    let channel = sqlx::query!(
        "SELECT id, provider, base_url, credential FROM channel \
         WHERE enabled AND provider = ANY($1) ORDER BY id LIMIT 1",
        &providers
    )
    .fetch_optional(&st.pool)
    .await
    .map_err(|e| {
        tracing::error!(error = %e, "查询渠道失败");
        reject(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "内部错误",
        )
    })?
    .ok_or_else(|| {
        reject(
            StatusCode::SERVICE_UNAVAILABLE,
            "no_channel_available",
            "没有可用渠道",
        )
    })?;

    let provider = ProviderId(channel.provider.as_str().into());
    let desc = matched.binding(&provider).ok_or_else(|| {
        tracing::error!(provider = %channel.provider, path, "渠道的 provider 未绑定该端点");
        reject(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "内部错误",
        )
    })?;
    let endpoint_specs = specs.get(desc).ok_or_else(|| {
        tracing::error!(endpoint = %desc.id, "规则表缺少该端点");
        reject(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "内部错误",
        )
    })?;

    // 5. 预扣。估算规则读请求体，计费时点来自端点形态。
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
    let estimate = endpoint_specs.estimate.evaluate(&doc);
    let model = model.unwrap_or_default();
    let price_ctx = PriceCtx {
        model: &model,
        channel: ChannelId(channel.id),
        tier: "default",
        endpoint: path,
        at: started_at,
        max_output_tokens: u32::try_from(estimate.get(dims::MAX_OUTPUT_TOKENS))
            .ok()
            .filter(|v| *v > 0),
        // 请求体已在内存中，估算规则直接数出真实输入量
        input_tokens: Some(estimate.get(dims::INPUT_TOKENS)).filter(|v| *v > 0),
    };
    let max_cost = st.pricing.estimate_max(&price_ctx).await.map_err(|e| {
        tracing::warn!(model = %model, error = %e, "预扣估算失败");
        reject(
            StatusCode::BAD_REQUEST,
            "model_not_found",
            format!("模型 {model} 无可用价格"),
        )
    })?;

    let hold = st
        .coord
        .hold(HoldRequest {
            chain: &principal.account_chain,
            amount: max_cost,
            ttl: st.config.hold_ttl,
            idempotency_key: &idempotency_key,
            timing: desc.shape.billing,
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
                reject(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    "内部错误",
                )
            }
        })?;

    // 6. 转发。上游地址、方法、注入的头全部来自描述文件。
    // 渠道的 base_url 覆盖描述文件的默认值——描述文件给的是厂商官方地址，
    // 渠道可能指向自建代理或另一个地域。
    let credential = String::from_utf8(channel.credential).unwrap_or_default();
    let uri = build_upstream_uri(&matched, desc, &channel.base_url, &credential)?;
    let mut req = axum::http::Request::builder()
        .method(desc.method.as_str())
        .uri(&uri)
        .body(Full::new(body))
        .map_err(|e| {
            tracing::error!(error = %e, uri = %uri, "构造上游请求失败");
            reject(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                "内部错误",
            )
        })?;
    *req.headers_mut() = prepare_upstream_headers(headers, &upstream_headers(desc, &credential)?);

    let upstream_resp = match st.upstream.send(req).await {
        Ok(r) => r,
        Err(e) => {
            // 未产生任何用量，直接撤销冻结
            tracing::warn!(error = %e, uri = %uri, "上游请求失败");
            if let Err(e) = st.coord.void(hold).await {
                tracing::error!(error = %e, "撤销冻结失败");
            }
            return Err(reject(
                StatusCode::BAD_GATEWAY,
                "upstream_error",
                "上游不可达",
            ));
        }
    };

    // 7. tee 旁路 + 结算哨兵。响应头此时才可知，故在此构造结算上下文。
    let ctx = SettlementCtx {
        request_id,
        key_id: Some(principal.key_id),
        account_chain: principal.account_chain,
        model,
        channel: ChannelId(channel.id),
        tier: "default".into(),
        endpoint: path.to_owned(),
        started_at,
        req_headers: st.redactor.redact(headers),
        resp_headers: st.redactor.redact(upstream_resp.headers()),
    };

    let is_sse = streaming
        || upstream_resp
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.starts_with("text/event-stream"));

    let spec = Arc::clone(&endpoint_specs.response);
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

    let mut out = Response::builder().status(status);
    if let Some(h) = out.headers_mut() {
        for (name, value) in &resp_headers {
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

    // guard 与并发名额随流一同析构：断连、报错、正常结束都会走到
    let body = Body::from_stream(TeeStream::new(stream, tee, Settled(guard, inflight)));
    Ok(out
        .body(body)
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response()))
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
        reject(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "内部错误",
        )
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

/// 注入上游的头：描述文件声明的固定头，加上凭证注入。
fn upstream_headers(desc: &EndpointDesc, credential: &str) -> Result<HeaderMap, Reject> {
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
    if !credential.is_empty()
        && let gw_registry::Injected::Header { name, value } =
            gw_registry::inject(&desc.auth, credential).map_err(|e| {
                tracing::error!(endpoint = %desc.id, error = %e, "凭证注入失败");
                reject(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    "内部错误",
                )
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
