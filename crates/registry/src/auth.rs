//! 凭证的附加。
//!
//! 获取（`CredentialDef`，可能做 IO、有缓存、渠道级）与附加（`InjectDef`，
//! 纯函数、请求级）正交。这里只做附加——纯函数，不碰网络。

use std::collections::HashMap;

use smol_str::SmolStr;

use crate::schema::{AuthDef, InjectDef};
use crate::template::PathTemplate;

/// 凭证附加到请求的哪个位置。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Injected {
    Header { name: String, value: String },
    Query { name: String, value: String },
}

#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("凭证模板无效: {0}")]
    Template(String),
    #[error("签名鉴权需要 M3")]
    SignNotImplemented,
}

/// 按声明把凭证附加到请求上。
///
/// # Errors
/// 模板不合法，或声明的是当前未实现的签名鉴权时返回错误。
pub fn inject(auth: &AuthDef, credential: &str) -> Result<Injected, AuthError> {
    let args = HashMap::from([(SmolStr::new("credential"), credential.to_owned())]);
    let render = |t: &str| {
        PathTemplate::parse(t)
            .and_then(|tpl| tpl.render(&args))
            .map_err(AuthError::Template)
    };
    match &auth.inject {
        InjectDef::Header(h) => Ok(Injected::Header {
            name: h.name.clone(),
            value: render(&h.template)?,
        }),
        InjectDef::Query(q) => Ok(Injected::Query {
            name: q.name.clone(),
            value: render(&q.template)?,
        }),
        // 门禁已在加载期拦下，这里是纵深防御
        InjectDef::Sign(_) => Err(AuthError::SignNotImplemented),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse_str;

    fn auth_of(yaml: &str) -> AuthDef {
        let src = format!(
            "schema_version: 1
provider: probe
defaults:
  base_url: https://example.test
  auth:
{yaml}
endpoints: []
"
        );
        parse_str(&src).unwrap().defaults.auth.unwrap()
    }

    #[test]
    fn renders_a_bearer_header() {
        let a = auth_of("    credential: static\n    inject: { header: { name: authorization } }");
        assert_eq!(
            inject(&a, "sk-abc").unwrap(),
            Injected::Header {
                name: "authorization".into(),
                value: "Bearer sk-abc".into()
            }
        );
    }

    /// Anthropic 用裸值的 x-api-key，不是 Bearer——模板可覆盖
    #[test]
    fn renders_a_bare_valued_header() {
        let a = auth_of(
            "    credential: static\n    inject: { header: { name: x-api-key, template: '{credential}' } }",
        );
        assert_eq!(
            inject(&a, "sk-ant").unwrap(),
            Injected::Header {
                name: "x-api-key".into(),
                value: "sk-ant".into()
            }
        );
    }

    #[test]
    fn renders_a_query_parameter() {
        let a = auth_of("    credential: static\n    inject: { query: { name: key } }");
        assert_eq!(
            inject(&a, "k1").unwrap(),
            Injected::Query {
                name: "key".into(),
                value: "k1".into()
            }
        );
    }

    #[test]
    fn signature_auth_is_refused_until_m3() {
        let a = auth_of(
            "    credential: static\n    inject: { sign: { alg: volc_v4, service: cv, region: cn-beijing } }",
        );
        assert!(matches!(
            inject(&a, "x"),
            Err(AuthError::SignNotImplemented)
        ));
    }
}
