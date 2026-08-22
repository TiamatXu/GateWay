//! 上游转发。贴着 hyper 走，避免高层封装引入 body 缓冲。

use bytes::Bytes;
use http::{HeaderMap, HeaderName, HeaderValue, Request, Response};
use http_body_util::Full;
use hyper::body::Incoming;
use hyper_util::client::legacy::{Client, connect::HttpConnector};
use hyper_util::rt::TokioExecutor;

/// 逐跳头属于本段连接，不得转发。
const HOP_BY_HOP: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

/// 客户端凭据头。转发前一律剥离——那是我们的 key，不是上游的。
const CLIENT_CREDENTIALS: &[&str] = &[
    "authorization",
    "x-api-key",
    "api-key",
    "x-goog-api-key",
    "cookie",
];

#[derive(Debug, thiserror::Error)]
pub enum ProxyError {
    #[error("上游请求失败: {0}")]
    Upstream(#[from] hyper_util::client::legacy::Error),
    #[error("请求构造失败: {0}")]
    Build(#[from] http::Error),
}

/// 构造发往上游的请求头。
///
/// `channel_credential` 为 `None` 时不带任何凭据——绝不回退到客户端凭据。
#[must_use]
pub fn prepare_upstream_headers(
    incoming: &HeaderMap,
    channel_credential: Option<&str>,
) -> HeaderMap {
    let mut out = HeaderMap::with_capacity(incoming.len() + 1);
    for (name, value) in incoming {
        let n = name.as_str();
        // host 由上游地址决定，照抄会打到错误的虚拟主机
        if n == "host" || HOP_BY_HOP.contains(&n) || CLIENT_CREDENTIALS.contains(&n) {
            continue;
        }
        out.append(name.clone(), value.clone());
    }
    if let Some(cred) = channel_credential
        && let Ok(v) = HeaderValue::from_str(cred)
    {
        out.insert(HeaderName::from_static("authorization"), v);
    }
    out
}

/// 上游 HTTP 客户端。连接池由 hyper-util 管理。
#[derive(Clone)]
pub struct Upstream {
    client: Client<hyper_rustls::HttpsConnector<HttpConnector>, Full<Bytes>>,
}

impl Upstream {
    /// # Panics
    /// 系统根证书不可用时 panic——这是启动期配置问题，应当立刻暴露。
    #[must_use]
    pub fn new() -> Self {
        let https = hyper_rustls::HttpsConnectorBuilder::new()
            .with_webpki_roots()
            .https_or_http()
            .enable_all_versions()
            .build();
        Self {
            client: Client::builder(TokioExecutor::new()).build(https),
        }
    }

    /// # Errors
    /// 连接失败、上游拒绝或超时。
    pub async fn send(&self, req: Request<Full<Bytes>>) -> Result<Response<Incoming>, ProxyError> {
        Ok(self.client.request(req).await?)
    }
}

impl Default for Upstream {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use http::{HeaderMap, HeaderName, HeaderValue};

    use super::*;

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.append(
                HeaderName::from_bytes(k.as_bytes()).unwrap(),
                HeaderValue::from_str(v).unwrap(),
            );
        }
        h
    }

    /// 逐跳头属于本段连接，转发过去会破坏上游连接管理
    #[test]
    fn strips_hop_by_hop_headers() {
        let out = prepare_upstream_headers(
            &headers(&[
                ("connection", "keep-alive"),
                ("keep-alive", "timeout=5"),
                ("transfer-encoding", "chunked"),
                ("upgrade", "h2c"),
                ("te", "trailers"),
                ("trailer", "x"),
                ("proxy-authenticate", "basic"),
                ("proxy-authorization", "basic"),
                ("content-type", "application/json"),
            ]),
            None,
        );

        assert_eq!(out.len(), 1);
        assert_eq!(out.get("content-type").unwrap(), "application/json");
    }

    /// 客户端的凭据绝不能透传给上游——那是我们的 key，不是他们的
    #[test]
    fn replaces_client_credentials_with_the_channel_credential() {
        let out = prepare_upstream_headers(
            &headers(&[
                ("authorization", "Bearer sk-user-key"),
                ("x-api-key", "user-key"),
            ]),
            Some("Bearer sk-channel-key"),
        );

        assert_eq!(out.get("authorization").unwrap(), "Bearer sk-channel-key");
        assert!(out.get("x-api-key").is_none());
    }

    /// 渠道未配置凭据时不得把客户端凭据漏出去
    #[test]
    fn drops_client_credentials_when_channel_has_none() {
        let out = prepare_upstream_headers(&headers(&[("authorization", "Bearer sk-user")]), None);
        assert!(out.get("authorization").is_none());
    }

    /// host 由上游地址决定，照抄客户端的会打到错误的虚拟主机
    #[test]
    fn strips_host_header() {
        let out = prepare_upstream_headers(&headers(&[("host", "gateway.local")]), None);
        assert!(out.get("host").is_none());
    }

    #[test]
    fn preserves_unrelated_headers() {
        let out = prepare_upstream_headers(
            &headers(&[("accept", "text/event-stream"), ("user-agent", "curl/8")]),
            None,
        );
        assert_eq!(out.get("accept").unwrap(), "text/event-stream");
        assert_eq!(out.get("user-agent").unwrap(), "curl/8");
    }
}
