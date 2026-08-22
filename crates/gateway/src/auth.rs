//! API Key 鉴权。
//!
//! 热路径只做一次主键查询：`account_chain` 在 `api_key` 上物化，无需遍历组织树。

use gw_core::{AccountId, ApiKeyId, NodeId};
use http::HeaderMap;
use sha2::{Digest, Sha256};
use smallvec::SmallVec;
use sqlx::PgPool;

/// 前缀仅用于在控制台辨识 key，不参与鉴权。
const PREFIX_LEN: usize = 12;

#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("缺少或格式错误的 Authorization 头")]
    MissingCredential,
    #[error("API Key 无效或已停用")]
    InvalidKey,
    #[error("数据库错误: {0}")]
    Db(#[from] sqlx::Error),
}

#[derive(Debug, Clone)]
pub struct Principal {
    pub key_id: ApiKeyId,
    pub node_id: NodeId,
    /// 由近及远的账户链，链首为主计费主体
    pub account_chain: SmallVec<[AccountId; 4]>,
}

#[must_use]
pub fn key_hash(raw: &str) -> [u8; 32] {
    Sha256::digest(raw.as_bytes()).into()
}

#[must_use]
pub fn key_prefix(raw: &str) -> &str {
    let end = raw
        .char_indices()
        .nth(PREFIX_LEN)
        .map_or(raw.len(), |(i, _)| i);
    &raw[..end]
}

/// 取出 `Authorization: Bearer <token>` 中的 token。
#[must_use]
pub fn extract_bearer(headers: &HeaderMap) -> Option<&str> {
    let raw = headers.get("authorization")?.to_str().ok()?;
    let (scheme, token) = raw.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let token = token.trim_start();
    (!token.is_empty()).then_some(token)
}

/// # Errors
/// 凭据缺失、无效、已停用，或数据库错误。
pub async fn authenticate(pool: &PgPool, raw_key: &str) -> Result<Principal, AuthError> {
    let hash = key_hash(raw_key);
    let row = sqlx::query!(
        "SELECT id, node_id, account_chain FROM api_key
          WHERE hash = $1 AND disabled_at IS NULL",
        &hash[..]
    )
    .fetch_optional(pool)
    .await?;

    let row = row.ok_or(AuthError::InvalidKey)?;
    Ok(Principal {
        key_id: ApiKeyId(row.id),
        node_id: NodeId(row.node_id),
        account_chain: row.account_chain.into_iter().map(AccountId).collect(),
    })
}

#[cfg(test)]
mod tests {
    use http::{HeaderMap, HeaderValue};
    use rstest::rstest;

    use super::*;

    fn auth_header(v: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert("authorization", HeaderValue::from_str(v).unwrap());
        h
    }

    #[test]
    fn hash_is_sha256_of_the_raw_key() {
        // 已知向量：sha256("abc")
        let expected =
            hex_literal("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
        assert_eq!(key_hash("abc").to_vec(), expected);
    }

    fn hex_literal(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    /// 前缀用于在控制台辨识 key，不参与鉴权
    #[test]
    fn prefix_is_the_leading_segment() {
        assert_eq!(key_prefix("sk-gw-abcdefghijklmn"), "sk-gw-abcdef");
        assert_eq!(key_prefix("short"), "short");
    }

    #[rstest]
    #[case("Bearer sk-abc", Some("sk-abc"))]
    #[case("bearer sk-abc", Some("sk-abc"))]
    #[case("BEARER sk-abc", Some("sk-abc"))]
    #[case("Bearer   sk-abc", Some("sk-abc"))]
    #[case("sk-abc", None)]
    #[case("Basic sk-abc", None)]
    #[case("Bearer", None)]
    #[case("Bearer ", None)]
    fn extracts_bearer_token(#[case] header: &str, #[case] expected: Option<&str>) {
        assert_eq!(extract_bearer(&auth_header(header)), expected);
    }

    #[test]
    fn missing_authorization_header_yields_nothing() {
        assert_eq!(extract_bearer(&HeaderMap::new()), None);
    }
}
