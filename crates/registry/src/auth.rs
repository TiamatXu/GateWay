//! 凭证的附加。
//!
//! 获取（`CredentialDef`，可能做 IO、有缓存、渠道级）与附加（`InjectDef`，
//! 纯函数、请求级）正交。这里只做附加——纯函数，不碰网络。

use std::collections::HashMap;

use smol_str::SmolStr;

use crate::schema::{AuthDef, InjectDef};
use crate::sign::{SignCtx, SignError};
use crate::template::PathTemplate;

/// 凭证附加到请求的哪个位置。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Injected {
    Header {
        name: String,
        value: String,
    },
    Query {
        name: String,
        value: String,
    },
    /// 签名产出一组头，缺一不可：少了 `x-date` 或体摘要，
    /// 上游算出来的规范请求就与我方不同。
    Signed(Vec<(String, String)>),
}

#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("凭证模板无效: {0}")]
    Template(String),
    #[error(transparent)]
    Sign(#[from] SignError),
}

/// 按声明把凭证附加到请求上。
///
/// `ctx` 只有签名会用到。静态注入也要求传它，是为了让数据平面一次调用打完，
/// 不必按鉴权方式分支——分支就是厂商知识渗回主干的开始。
///
/// # Errors
/// 模板不合法，或签名凭证格式不对、声明缺字段。
pub fn inject(auth: &AuthDef, credential: &str, ctx: &SignCtx<'_>) -> Result<Injected, AuthError> {
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
        InjectDef::Sign(d) => Ok(Injected::Signed(crate::sign::sign(d, credential, ctx)?)),
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;
    use crate::parse_str;

    fn ctx() -> SignCtx<'static> {
        SignCtx {
            method: "POST",
            host: "example.test",
            path: "/",
            query: &[],
            body: b"",
            content_type: None,
            at: Utc::now(),
        }
    }

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
            inject(&a, "sk-abc", &ctx()).unwrap(),
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
            inject(&a, "sk-ant", &ctx()).unwrap(),
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
            inject(&a, "k1", &ctx()).unwrap(),
            Injected::Query {
                name: "key".into(),
                value: "k1".into()
            }
        );
    }

    /// 签名走同一个入口，只是产出一组头而非一个
    #[test]
    fn signature_auth_yields_a_set_of_headers() {
        let a = auth_of(
            "    credential: static\n    inject: { sign: { alg: volc_v4, service: cv, region: cn-beijing } }",
        );
        let Injected::Signed(headers) = inject(&a, "ak:sk", &ctx()).unwrap() else {
            panic!("应当产出签名头");
        };
        assert!(headers.iter().any(|(k, _)| k == "authorization"));
        assert!(headers.iter().any(|(k, _)| k == "x-date"));
        assert!(headers.iter().any(|(k, _)| k == "x-content-sha256"));
    }

    /// 渠道凭证没按 `<AK>:<SK>` 配时，必须在附加环节就失败——
    /// 带着半截凭证发出去只会拿到上游的鉴权错误，排查要绕一圈
    #[test]
    fn signature_auth_rejects_a_malformed_credential() {
        let a = auth_of(
            "    credential: static\n    inject: { sign: { alg: volc_v4, service: cv, region: cn-beijing } }",
        );
        assert!(matches!(
            inject(&a, "no-colon", &ctx()),
            Err(AuthError::Sign(_))
        ));
    }
}
