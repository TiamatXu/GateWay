use std::collections::HashSet;

use http::{HeaderMap, HeaderName, HeaderValue};

/// 密钥类请求头。**不可由配置覆盖**——安全边界不能依赖管理员配置正确，
/// 一次误配即等同密钥泄漏。
const HARD_BLACKLIST: &[&str] = &[
    "authorization",
    "proxy-authorization",
    "cookie",
    "set-cookie",
    "x-api-key",
    "api-key",
    "x-goog-api-key",
    "x-security-token",
    "x-amz-security-token",
    "x-acs-security-token",
    "x-tc-token",
    "x-auth-token",
    "openai-organization",
];

/// 黑名单模式的请求头脱敏器。硬黑名单恒定生效，软黑名单来自配置。
#[derive(Debug, Clone, Default)]
pub struct HeaderRedactor {
    soft: HashSet<HeaderName>,
}

impl HeaderRedactor {
    #[must_use]
    pub fn new(soft_blacklist: impl IntoIterator<Item = HeaderName>) -> Self {
        Self {
            soft: soft_blacklist.into_iter().collect(),
        }
    }

    /// 硬黑名单内容，仅供检视，不可修改。
    pub fn hard_blacklist() -> impl Iterator<Item = &'static str> {
        HARD_BLACKLIST.iter().copied()
    }

    fn is_blocked(&self, name: &HeaderName) -> bool {
        // HeaderName 恒为小写，与硬黑名单直接比对即可
        HARD_BLACKLIST.contains(&name.as_str()) || self.soft.contains(name)
    }

    #[must_use]
    pub fn redact(&self, headers: &HeaderMap) -> HeaderMap<HeaderValue> {
        let mut out = HeaderMap::with_capacity(headers.len());
        for (name, value) in headers {
            if !self.is_blocked(name) {
                out.append(name.clone(), value.clone());
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderMap, HeaderName, HeaderValue};
    use rstest::rstest;

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

    #[rstest]
    #[case("authorization")]
    #[case("Authorization")]
    #[case("AUTHORIZATION")]
    #[case("x-api-key")]
    #[case("api-key")]
    #[case("cookie")]
    #[case("set-cookie")]
    #[case("proxy-authorization")]
    #[case("x-security-token")]
    #[case("x-amz-security-token")]
    #[case("x-goog-api-key")]
    fn removes_hard_blacklisted_header(#[case] name: &str) {
        let r = HeaderRedactor::new([]);
        let out = r.redact(&headers(&[(name, "sk-secret")]));
        assert!(out.is_empty(), "{name} 未被脱敏");
    }

    /// 安全边界不能依赖管理员配置正确：软黑名单配置无法把硬黑名单头放出来
    #[test]
    fn soft_config_cannot_exempt_hard_blacklist() {
        let r = HeaderRedactor::new(["authorization".parse().unwrap()]);
        let out = r.redact(&headers(&[
            ("authorization", "sk-secret"),
            ("accept", "*/*"),
        ]));
        assert_eq!(out.len(), 1);
        assert_eq!(out.get("accept").unwrap(), "*/*");
    }

    #[test]
    fn removes_soft_blacklisted_header() {
        let r = HeaderRedactor::new(["accept-encoding".parse().unwrap()]);
        let out = r.redact(&headers(&[("accept-encoding", "gzip"), ("accept", "*/*")]));
        assert_eq!(out.len(), 1);
        assert!(out.get("accept-encoding").is_none());
    }

    #[test]
    fn keeps_unlisted_headers() {
        let r = HeaderRedactor::new([]);
        let out = r.redact(&headers(&[("content-type", "application/json")]));
        assert_eq!(out.get("content-type").unwrap(), "application/json");
    }

    /// 同名多值头必须整体移除，不能只删第一个
    #[test]
    fn removes_all_values_of_a_repeated_header() {
        let r = HeaderRedactor::new([]);
        let out = r.redact(&headers(&[("set-cookie", "a=1"), ("set-cookie", "b=2")]));
        assert!(out.is_empty());
    }

    #[test]
    fn hard_blacklist_is_not_configurable_away() {
        assert!(HeaderRedactor::hard_blacklist().any(|h| h == "authorization"));
    }
}
