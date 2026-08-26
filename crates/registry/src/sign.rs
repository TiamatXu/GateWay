//! 请求签名。
//!
//! 签名是 `inject` 的一个变体，不是独立概念——它的本质仍是「把凭证附加到请求上」，
//! 只是附加方式复杂：要把方法、路径、查询、体一起摁进一个 HMAC 链。
//!
//! 火山引擎 V4 与 AWS `SigV4` 是同一套骨架，差别只有四处（算法名、派生密钥的起点、
//! 作用域结尾、头名），因此参数化成一张表而不是两份实现。签名算法是**有限枚举**，
//! 不走 hook：hook 是纯函数逃生舱，而签名需要整个请求的上下文，装不进字段级算子。

use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

use crate::schema::{SignAlg, SignDef};

type HmacSha256 = Hmac<Sha256>;

/// 签名要看的请求上下文。静态注入用不上，但接口统一，数据平面不必分支。
pub struct SignCtx<'a> {
    pub method: &'a str,
    pub host: &'a str,
    /// 已渲染的上游路径
    pub path: &'a str,
    /// 查询参数，未排序、未编码
    pub query: &'a [(String, String)],
    pub body: &'a [u8],
    pub content_type: Option<&'a str>,
    pub at: DateTime<Utc>,
}

#[derive(Debug, thiserror::Error)]
pub enum SignError {
    /// 签名要 AK 与 SK 两段，渠道凭证按 `<AK>:<SK>` 存。
    #[error("签名凭证格式应为 <AK>:<SK>")]
    MalformedCredential,
    #[error("签名声明缺少 {0}")]
    MissingField(&'static str),
}

/// 两种算法的差异全在这张表里。
struct Params {
    /// `StringToSign` 首行与 Authorization 头的前缀
    algorithm: &'static str,
    /// 作用域结尾
    terminator: &'static str,
    /// 派生密钥的起点前缀
    key_prefix: &'static str,
    date_header: &'static str,
    /// 体摘要头。AWS `SigV4` 的通用测试向量不带它，火山引擎必须带
    payload_header: Option<&'static str>,
}

const fn params(alg: SignAlg) -> Params {
    match alg {
        SignAlg::VolcV4 => Params {
            algorithm: "HMAC-SHA256",
            terminator: "request",
            key_prefix: "",
            date_header: "x-date",
            payload_header: Some("x-content-sha256"),
        },
        SignAlg::AwsSigV4 => Params {
            algorithm: "AWS4-HMAC-SHA256",
            terminator: "aws4_request",
            key_prefix: "AWS4",
            date_header: "x-amz-date",
            payload_header: None,
        },
    }
}

/// 按声明给请求签名，返回必须原样附加的头。
///
/// 返回的是一组头而不是单个：签名值本身没有意义，缺了 `x-date` 或体摘要，
/// 上游算出来的规范请求就与我方不同。
///
/// # Errors
/// 凭证不是 `<AK>:<SK>` 形式，或声明缺少 `service` / `region`。
pub fn sign(
    def: &SignDef,
    credential: &str,
    ctx: &SignCtx<'_>,
) -> Result<Vec<(String, String)>, SignError> {
    let (access_key, secret_key) = credential
        .split_once(':')
        .filter(|(a, s)| !a.is_empty() && !s.is_empty())
        .ok_or(SignError::MalformedCredential)?;
    let service = def
        .service
        .as_deref()
        .ok_or(SignError::MissingField("service"))?;
    let region = def
        .region
        .as_deref()
        .ok_or(SignError::MissingField("region"))?;

    let p = params(def.alg);
    let stamp = ctx.at.format("%Y%m%dT%H%M%SZ").to_string();
    let date = ctx.at.format("%Y%m%d").to_string();
    let payload = hex(&Sha256::digest(ctx.body));

    // 签的头就是我们要附加的头：不签客户端带来的头，避免上游改写中间头导致签名失效
    let mut signed: Vec<(String, String)> = Vec::with_capacity(4);
    if let Some(ct) = ctx.content_type {
        signed.push(("content-type".to_owned(), ct.to_owned()));
    }
    signed.push(("host".to_owned(), ctx.host.to_owned()));
    if let Some(name) = p.payload_header {
        signed.push((name.to_owned(), payload.clone()));
    }
    signed.push((p.date_header.to_owned(), stamp.clone()));
    signed.sort_by(|a, b| a.0.cmp(&b.0));

    let canonical_headers = signed.iter().fold(String::new(), |mut acc, (k, v)| {
        use std::fmt::Write as _;
        let _ = writeln!(acc, "{k}:{}", v.trim());
        acc
    });
    let signed_names = signed
        .iter()
        .map(|(k, _)| k.as_str())
        .collect::<Vec<_>>()
        .join(";");

    let canonical_request = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        ctx.method,
        canonical_path(ctx.path),
        canonical_query(ctx.query),
        canonical_headers,
        signed_names,
        payload
    );

    let scope = format!("{date}/{region}/{service}/{}", p.terminator);
    let string_to_sign = format!(
        "{}\n{stamp}\n{scope}\n{}",
        p.algorithm,
        hex(&Sha256::digest(canonical_request.as_bytes()))
    );

    let mut key = format!("{}{secret_key}", p.key_prefix).into_bytes();
    for part in [date.as_str(), region, service, p.terminator] {
        key = hmac(&key, part.as_bytes());
    }
    let signature = hex(&hmac(&key, string_to_sign.as_bytes()));

    let mut out = signed;
    out.push((
        "authorization".to_owned(),
        format!(
            "{} Credential={access_key}/{scope}, SignedHeaders={signed_names}, Signature={signature}",
            p.algorithm
        ),
    ));
    // host 由上游地址决定，交给 HTTP 客户端自己填，附加过去反而可能重复
    out.retain(|(k, _)| k != "host");
    Ok(out)
}

fn hmac(key: &[u8], msg: &[u8]) -> Vec<u8> {
    let mut m = <HmacSha256 as Mac>::new_from_slice(key).expect("HMAC 接受任意长度密钥");
    m.update(msg);
    m.finalize().into_bytes().to_vec()
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(String::new(), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}

/// 逐段编码，`/` 保留。路径参数可能带上游 ID 里的任意字符。
fn canonical_path(path: &str) -> String {
    if path.is_empty() {
        return "/".to_owned();
    }
    path.split('/').map(encode).collect::<Vec<_>>().join("/")
}

/// 按 (名, 值) 排序后编码。上游按同样规则重算，顺序必须确定。
fn canonical_query(query: &[(String, String)]) -> String {
    let mut pairs: Vec<(String, String)> =
        query.iter().map(|(k, v)| (encode(k), encode(v))).collect();
    pairs.sort();
    pairs
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

/// RFC 3986 unreserved 之外一律百分号编码。
fn encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            use std::fmt::Write as _;
            let _ = write!(out, "%{b:02X}");
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    fn at(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    fn find<'a>(headers: &'a [(String, String)], name: &str) -> &'a str {
        headers
            .iter()
            .find(|(k, _)| k == name)
            .map_or_else(|| panic!("缺少头 {name}"), |(_, v)| v.as_str())
    }

    /// AWS 官方测试套件 get-vanilla 的已知答案。
    /// 抄错一个换行、少 trim 一个头都过不了这条。
    #[test]
    fn matches_the_aws_sigv4_get_vanilla_vector() {
        let def = SignDef {
            alg: SignAlg::AwsSigV4,
            service: Some("service".into()),
            region: Some("us-east-1".into()),
        };
        let headers = sign(
            &def,
            "AKIDEXAMPLE:wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            &SignCtx {
                method: "GET",
                host: "example.amazonaws.com",
                path: "/",
                query: &[],
                body: b"",
                content_type: None,
                at: at("2015-08-30T12:36:00Z"),
            },
        )
        .unwrap();

        assert_eq!(
            find(&headers, "authorization"),
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, \
             SignedHeaders=host;x-amz-date, \
             Signature=5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31"
        );
        assert_eq!(find(&headers, "x-amz-date"), "20150830T123600Z");
    }

    /// 火山引擎 V4：独立实现算出的同一向量。
    /// 与 AWS 的差别全在四处——算法名、派生起点、作用域结尾、头名。
    #[test]
    fn matches_the_volc_v4_vector() {
        let def = SignDef {
            alg: SignAlg::VolcV4,
            service: Some("cv".into()),
            region: Some("cn-beijing".into()),
        };
        let headers = sign(
            &def,
            "AKLTexample:c2VjcmV0LWtleS1leGFtcGxl",
            &SignCtx {
                method: "POST",
                host: "open.volcengineapi.com",
                path: "/",
                query: &[
                    ("Action".into(), "UploadAsset".into()),
                    ("Version".into(), "2024-06-06".into()),
                ],
                body: br#"{"name":"a"}"#,
                content_type: Some("application/json"),
                at: at("2026-08-26T10:15:00Z"),
            },
        )
        .unwrap();

        assert_eq!(
            find(&headers, "x-content-sha256"),
            "d9d719b27480b55cd4918020e7473e716ed3569c8adafe926cf9b10b4f8ef064"
        );
        assert!(
            find(&headers, "authorization").ends_with(
                "Signature=5157c672dd9521911d4c4b066fcfda8f3a6f4faa458dd6c82ebf514e300444db"
            ),
            "实际: {}",
            find(&headers, "authorization")
        );
        assert_eq!(find(&headers, "x-date"), "20260826T101500Z");
    }

    /// host 交给 HTTP 客户端填：附加过去会与连接自己写的那份重复
    #[test]
    fn host_is_signed_but_not_emitted() {
        let def = SignDef {
            alg: SignAlg::VolcV4,
            service: Some("cv".into()),
            region: Some("cn-beijing".into()),
        };
        let headers = sign(&def, "a:b", &ctx()).unwrap();
        assert!(!headers.iter().any(|(k, _)| k == "host"));
        assert!(find(&headers, "authorization").contains("SignedHeaders=host;"));
    }

    fn ctx() -> SignCtx<'static> {
        SignCtx {
            method: "POST",
            host: "open.volcengineapi.com",
            path: "/x",
            query: &[],
            body: b"{}",
            content_type: None,
            at: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
        }
    }

    /// 查询参数的书写顺序不能影响签名——上游按排序后的规范串重算
    #[test]
    fn query_order_does_not_change_the_signature() {
        let def = SignDef {
            alg: SignAlg::VolcV4,
            service: Some("cv".into()),
            region: Some("cn-beijing".into()),
        };
        let one = [
            ("b".to_owned(), "2".to_owned()),
            ("a".to_owned(), "1".to_owned()),
        ];
        let two = [
            ("a".to_owned(), "1".to_owned()),
            ("b".to_owned(), "2".to_owned()),
        ];
        let sig = |q: &[(String, String)]| {
            sign(&def, "a:b", &SignCtx { query: q, ..ctx() })
                .unwrap()
                .into_iter()
                .find(|(k, _)| k == "authorization")
                .unwrap()
                .1
        };
        assert_eq!(sig(&one), sig(&two));
    }

    /// 体变了签名必须变，否则重放能改内容
    #[test]
    fn the_body_is_covered_by_the_signature() {
        let def = SignDef {
            alg: SignAlg::VolcV4,
            service: Some("cv".into()),
            region: Some("cn-beijing".into()),
        };
        let sig = |body: &'static [u8]| {
            sign(&def, "a:b", &SignCtx { body, ..ctx() })
                .unwrap()
                .into_iter()
                .find(|(k, _)| k == "authorization")
                .unwrap()
                .1
        };
        assert_ne!(sig(b"{}"), sig(br#"{"x":1}"#));
    }

    #[test]
    fn rejects_a_credential_that_is_not_ak_colon_sk() {
        let def = SignDef {
            alg: SignAlg::VolcV4,
            service: Some("cv".into()),
            region: Some("cn-beijing".into()),
        };
        for bad in ["nocolon", ":sk", "ak:"] {
            assert!(matches!(
                sign(&def, bad, &ctx()),
                Err(SignError::MalformedCredential)
            ));
        }
    }
}
