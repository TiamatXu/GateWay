//! 虚拟句柄的解析与改写。
//!
//! 请求侧：把客户端给的虚拟 ID 换回上游 ID，并由此定出必须回哪个渠道。
//! 响应侧：把上游签发的 ID 换成虚拟 ID，客户端从头到尾只见得到虚拟 ID。
//!
//! 「哪些字段是句柄」全部来自描述文件（§4.3），此处没有任何厂商分支。

use std::collections::HashMap;

use axum::http::HeaderMap;
use gw_core::{AccountId, ChannelId, HandleId, HandleKind, HoldId, ProviderId};
use gw_registry::locator::{Locator, get_mut};
use gw_registry::schema::HandleFieldDef;
use gw_resource::store::{Handle, NewHandle, ResourceError, ResourceStore};
use smol_str::SmolStr;

/// 按声明的位置取一个字符串值。三种前缀是封闭集合，无需分支到厂商。
#[must_use]
pub(crate) fn read_locator(
    loc: &Locator,
    doc: &serde_json::Value,
    params: &HashMap<SmolStr, String>,
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

/// 句柄环节能给出的全部拒绝理由。数据平面据此选状态码。
#[derive(Debug, thiserror::Error)]
pub enum HandleReject {
    #[error("{at} 处不是一个网关签发的资源 ID")]
    NotAHandle { at: String },
    #[error("资源 {id} 不存在")]
    NotFound { id: HandleId },
    #[error("资源 {id} 不属于当前账户")]
    Forbidden { id: HandleId },
    #[error("资源 {id} 是 {actual:?}，此处需要 {expected:?}")]
    KindMismatch {
        id: HandleId,
        expected: HandleKind,
        actual: HandleKind,
    },
    #[error("一个请求里引用了分属不同渠道的资源，无法同时满足亲和")]
    ChannelConflict,
    #[error(transparent)]
    Store(#[from] ResourceError),
}

/// 请求里消费到的句柄。
#[derive(Debug, Default)]
pub struct Consumed {
    /// 虚拟 ID → 上游句柄
    pub by_id: HashMap<HandleId, Handle>,
    /// 亲和渠道。有任何消费即被钉死，与 `shape.handle` 无关——
    /// 提交端点的 `shape.handle` 是 `Issues`，但它引用的素材同样只在原渠道上存在。
    pub channel: Option<ChannelId>,
}

impl Consumed {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }

    /// 消费到的唯一一个指定类型的句柄。
    #[must_use]
    pub fn of_kind(&self, kind: HandleKind) -> Option<&Handle> {
        let mut it = self.by_id.values().filter(|h| h.kind == kind);
        let first = it.next()?;
        it.next().is_none().then_some(first)
    }
}

/// 解析入站契约声明的全部消费位置。
///
/// # Errors
/// 值不是虚拟句柄、句柄不存在、不属于调用方、类型不符，或多个句柄分属不同渠道。
pub(crate) async fn resolve_consumed(
    store: &dyn ResourceStore,
    fields: &[HandleFieldDef],
    doc: &serde_json::Value,
    params: &HashMap<SmolStr, String>,
    headers: &HeaderMap,
    caller_chain: &[AccountId],
) -> Result<Consumed, HandleReject> {
    let mut out = Consumed::default();
    for f in fields {
        // 位置上没有值，说明这个可选字段本次没带。带了但不是句柄才是错。
        let Some(raw) = read_locator(&f.at, doc, params, headers) else {
            continue;
        };
        let ids = gw_resource::wire::scan(&raw);
        if ids.is_empty() {
            return Err(HandleReject::NotAHandle {
                at: f.at.to_string(),
            });
        }
        for id in ids {
            if out.by_id.contains_key(&id) {
                continue;
            }
            let h = store
                .resolve(id)
                .await?
                .ok_or(HandleReject::NotFound { id })?;
            if !h.owned_by(caller_chain) {
                return Err(HandleReject::Forbidden { id });
            }
            if h.kind != f.kind {
                return Err(HandleReject::KindMismatch {
                    id,
                    expected: f.kind,
                    actual: h.kind,
                });
            }
            match out.channel {
                Some(c) if c != h.channel => return Err(HandleReject::ChannelConflict),
                _ => out.channel = Some(h.channel),
            }
            out.by_id.insert(id, h);
        }
    }
    Ok(out)
}

/// 把请求里的虚拟句柄换回上游 ID。就地改写请求体与路径参数，
/// 返回需要覆盖到上游请求上的头。
pub(crate) fn rewrite_request(
    fields: &[HandleFieldDef],
    consumed: &Consumed,
    doc: &mut serde_json::Value,
    params: &mut HashMap<SmolStr, String>,
    headers: &HeaderMap,
) -> Vec<(String, String)> {
    let upstream = |id: HandleId| consumed.by_id.get(&id).map(|h| h.upstream_id.clone());
    let mut rewritten_headers = Vec::new();

    for f in fields {
        match &f.at {
            Locator::Body(p) => {
                let Some(steps) = p.one_location(doc) else {
                    continue;
                };
                let Some(node) = get_mut(doc, &steps) else {
                    continue;
                };
                if let Some(s) = node.as_str() {
                    *node = serde_json::Value::String(gw_resource::wire::replace(s, upstream));
                }
            }
            Locator::PathParam(n) => {
                if let Some(v) = params.get_mut(n) {
                    *v = gw_resource::wire::replace(v, upstream);
                }
            }
            Locator::Header(n) => {
                if let Some(v) = headers.get(n.as_str()).and_then(|v| v.to_str().ok()) {
                    rewritten_headers
                        .push((n.to_string(), gw_resource::wire::replace(v, upstream)));
                }
            }
        }
    }
    rewritten_headers
}

/// 一次签发所需的、与端点无关的上下文。
pub struct IssueCtx<'a> {
    pub channel: ChannelId,
    pub provider: &'a ProviderId,
    pub account_chain: &'a [AccountId],
    pub key_id: Option<gw_core::ApiKeyId>,
    pub endpoint: &'a str,
    /// `OnTerminal` 的预扣单号，挂在签发出的句柄上
    pub hold_id: Option<HoldId>,
}

/// 把响应体里上游签发的 ID 换成虚拟句柄，返回签发出的 `(类型, 虚拟ID)`。
///
/// 声明了却没出现在响应里的字段直接跳过——一个响应签发几个句柄由上游决定
/// （batch 失败时没有 `error_file_id`），描述文件声明的是上界。
///
/// # Errors
/// 句柄落库失败。签发不落库就等于把上游 ID 直接给了客户端，宁可整个请求失败。
pub(crate) async fn issue_handles(
    store: &dyn ResourceStore,
    fields: &[HandleFieldDef],
    doc: &mut serde_json::Value,
    ctx: &IssueCtx<'_>,
) -> Result<Vec<(HandleKind, HandleId)>, ResourceError> {
    let mut issued = Vec::new();
    for f in fields {
        let Some(p) = f.at.body_path() else { continue };
        let Some(steps) = p.one_location(doc) else {
            continue;
        };
        let Some(upstream_id) = get_mut(doc, &steps)
            .and_then(|n| n.as_str())
            .map(str::to_owned)
            .filter(|s| !s.is_empty())
        else {
            continue;
        };

        let id = store
            .issue(NewHandle {
                kind: f.kind,
                channel: ctx.channel,
                provider: ctx.provider,
                upstream_id: &upstream_id,
                account_chain: ctx.account_chain,
                key_id: ctx.key_id,
                endpoint: ctx.endpoint,
                hold_id: ctx.hold_id,
            })
            .await?;

        if let Some(node) = get_mut(doc, &steps) {
            *node = serde_json::Value::String(gw_resource::wire::to_wire(id));
        }
        issued.push((f.kind, id));
    }
    Ok(issued)
}
