//! 上游请求的地址与头。
//!
//! 地址与头必须一起算：签名要把方法、路径、查询、体一并摁进 HMAC，
//! 拆成两步就得把请求上下文传两遍，两遍之间只要有一处不一致签名就作废。
//!
//! 客户端轮询与孤儿巡检走同一份，两条路径不能各拼各的。

use bytes::Bytes;
use chrono::DateTime;
use chrono::Utc;
use gw_registry::{AuthError, EndpointDesc, Injected, SignCtx};
use http::{HeaderMap, HeaderName, HeaderValue};

/// 发往上游的地址与头。
pub(crate) struct Outbound {
    pub uri: String,
    pub headers: HeaderMap,
}

/// 拼上游请求：渠道地址 + 已渲染的上游路径 + 声明的查询参数 + 凭证附加。
///
/// # Errors
/// 凭证附加失败（签名凭证格式不对、模板不合法）。
pub(crate) fn prepare(
    desc: &EndpointDesc,
    path: &str,
    channel_base_url: &str,
    credential: &str,
    rewritten: &[(String, String)],
    body: &Bytes,
    at: DateTime<Utc>,
) -> Result<Outbound, AuthError> {
    let base = channel_base_url.trim_end_matches('/');
    let mut query: Vec<(String, String)> = desc.query.clone();

    let mut headers = HeaderMap::new();
    let put = |map: &mut HeaderMap, name: &str, value: &str| {
        if let (Ok(n), Ok(v)) = (HeaderName::try_from(name), HeaderValue::from_str(value)) {
            map.insert(n, v);
        } else {
            tracing::warn!(endpoint = %desc.id, name, "声明的头名或值不合法，已跳过");
        }
    };
    for (name, value) in &desc.headers {
        put(&mut headers, name, value);
    }
    for (name, value) in rewritten {
        put(&mut headers, name, value);
    }

    if !credential.is_empty() {
        let ctx = SignCtx {
            method: desc.method.as_str(),
            host: host_of(base),
            path,
            query: &query,
            body,
            // 签名要覆盖 content-type，而签的那个值随后会原样写回头上，
            // 因此这里必须给出一个确定的值，不能留给客户端的头去决定
            content_type: Some(
                headers
                    .get("content-type")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("application/json"),
            ),
            at,
        };
        match gw_registry::inject(&desc.auth, credential, &ctx)? {
            Injected::Header { name, value } => put(&mut headers, &name, &value),
            Injected::Query { name, value } => query.push((name, value)),
            // 签名产出的每个头都要原样发出去，包括被签进去的 content-type：
            // 签什么就得发什么，差一个字符上游算出的规范请求就不同
            Injected::Signed(signed) => {
                for (name, value) in &signed {
                    put(&mut headers, name, value);
                }
            }
        }
    }

    let mut uri = format!("{base}{path}");
    if !query.is_empty() {
        let joined = query
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&");
        uri.push(if uri.contains('?') { '&' } else { '?' });
        uri.push_str(&joined);
    }
    Ok(Outbound { uri, headers })
}

/// 从渠道地址里取 host（含端口）。签名要把它算进规范请求。
fn host_of(base_url: &str) -> &str {
    let rest = base_url
        .split_once("://")
        .map_or(base_url, |(_, rest)| rest);
    let rest = rest.split(['/', '?', '#']).next().unwrap_or(rest);
    // userinfo 极罕见，但照抄进签名会与上游算的 host 不同
    rest.rsplit_once('@').map_or(rest, |(_, host)| host)
}
