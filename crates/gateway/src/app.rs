//! 数据平面的请求生命周期编排。
//!
//! 检查顺序即成本递增顺序，不可调换：
//! 本地原子读（准入）→ 主键查询（鉴权）→ 路由 → PG 写（冻结）→ 转发。
//! 过载时应在最便宜的位置拒绝，而非查库之后。

use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Body;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use bytes::Bytes;
use chrono::Utc;
use futures::TryStreamExt;
use gw_core::{BillingTiming, ChannelId, ProtocolKind, RequestId};
use gw_ledger::{Coordinator, HoldRequest, LedgerError};
use gw_meter::{Accum, JsonUsageExtractor, SseUsageExtractor, Tokenizer, UsageSpec};
use gw_pricing::{PriceCtx, PriceEngine};
use gw_proxy::{Tee, TeeStream, Upstream, prepare_upstream_headers};
use http_body_util::{BodyStream, Full};
use sqlx::PgPool;
use uuid::Uuid;

use crate::admission::{Admission, InflightToken, RejectReason};
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
    pub config: GatewayConfig,
}

pub fn router(state: Arc<AppState>) -> Router {
    let limit = state.config.max_request_body;
    Router::new()
        .route("/healthz", get(|| async { StatusCode::OK }))
        // readyz 只反映结构性状态，不反映瞬时负载：过载时摘节点会把流量压向
        // 其余节点引发雪崩。过载只在请求路径上返回 503。
        .route("/readyz", get(readyz))
        .route("/v1/{*path}", post(proxy_request))
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

/// M0 只识别 `OpenAI` 兼容协议；`Native` 由 M2 的描述文件决定。
fn protocol_of(path: &str) -> ProtocolKind {
    if path.starts_with("/v1/responses") {
        ProtocolKind::OpenAiResponses
    } else {
        ProtocolKind::OpenAiChat
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

/// M0 的用量抽取规则：OpenAI 兼容协议，硬编码。M2 起由描述文件提供。
fn openai_usage_spec() -> UsageSpec {
    UsageSpec::new()
        .rule(
            gw_core::dims::INPUT_TOKENS,
            "$.usage.prompt_tokens",
            Accum::Last,
        )
        .and_then(|s| {
            s.rule(
                gw_core::dims::OUTPUT_TOKENS,
                "$.usage.completion_tokens",
                Accum::Last,
            )
        })
        .and_then(|s| {
            s.text_fallback(
                gw_core::dims::OUTPUT_TOKENS,
                "$.choices[*].delta.content",
                Tokenizer::O200kBase,
            )
        })
        .expect("内置 JSONPath 必然合法")
}

async fn proxy_request(
    State(st): State<Arc<AppState>>,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let path = uri.path().to_owned();
    let protocol = protocol_of(&path);
    match handle(&st, &path, &headers, body).await {
        Ok(resp) => resp,
        Err(r) => r.into_response(&protocol),
    }
}

#[allow(clippy::too_many_lines)]
async fn handle(
    st: &Arc<AppState>,
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

    // 3. 路由。M0 读取 body 取 model——OpenAI 兼容协议的 model 只在 body 里。
    let doc: serde_json::Value = serde_json::from_slice(&body).map_err(|e| {
        reject(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            format!("请求体不是合法 JSON: {e}"),
        )
    })?;
    let model = doc
        .get("model")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            reject(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                "请求体缺少 model 字段",
            )
        })?
        .to_owned();
    let streaming = doc
        .get("stream")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let max_output_tokens = doc
        .get("max_tokens")
        .or_else(|| doc.get("max_completion_tokens"))
        .and_then(serde_json::Value::as_u64)
        .and_then(|v| u32::try_from(v).ok());

    // SIMPLIFIED(M0): 单渠道直连，无过滤与打分。M3 接入路由策略。
    let channel = sqlx::query!(
        "SELECT id, base_url, credential FROM channel WHERE enabled ORDER BY id LIMIT 1"
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

    // 4. 预扣。
    let request_id = RequestId(Uuid::new_v4());
    let started_at = Utc::now();
    let price_ctx = PriceCtx {
        model: &model,
        channel: ChannelId(channel.id),
        tier: "default",
        endpoint: path,
        at: started_at,
        max_output_tokens,
    };
    let estimate = st.pricing.estimate_max(&price_ctx).await.map_err(|e| {
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
            amount: estimate,
            ttl: st.config.hold_ttl,
            idempotency_key: &request_id.to_string(),
            timing: BillingTiming::InRequest,
        })
        .await
        .map_err(|e| match e {
            LedgerError::InsufficientFunds { .. } => reject(
                StatusCode::PAYMENT_REQUIRED,
                "insufficient_quota",
                "额度不足",
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

    // 5. 转发。body 已在内存中（JSON 端点），响应流不缓冲。
    let credential = String::from_utf8(channel.credential).ok();
    let uri = format!("{}{}", channel.base_url.trim_end_matches('/'), path);
    let mut req = axum::http::Request::builder()
        .method("POST")
        .uri(&uri)
        .body(Full::new(body))
        .map_err(|e| {
            tracing::error!(error = %e, "构造上游请求失败");
            reject(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                "内部错误",
            )
        })?;
    *req.headers_mut() = prepare_upstream_headers(headers, credential.as_deref());

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

    // 6. tee 旁路 + 结算哨兵。响应头此时才可知，故在此构造结算上下文。
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

    let spec = openai_usage_spec();
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

/// 随响应流一同析构的两样东西：结算哨兵与并发名额。
/// 字段不被读取，存在的意义就是析构。
#[allow(dead_code)]
struct Settled(crate::SettlementGuard, InflightToken);
