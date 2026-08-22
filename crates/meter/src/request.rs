//! 请求体的输入 token 计数，用于预扣估算。
//!
//! 按 `JSONPath` 取文本再用 tokenizer 计数——与用量抽取同一套机制，
//! M2 起路径由 Provider 描述文件提供，主干不需要厂商分支。

use serde_json_path::{JsonPath, ParseError};

use crate::Tokenizer;

/// 从请求体中取文本的路径集合。
#[derive(Debug)]
pub struct TextSpec {
    paths: Vec<JsonPath>,
    tokenizer: Tokenizer,
}

impl TextSpec {
    #[must_use]
    pub fn new(tokenizer: Tokenizer) -> Self {
        Self {
            paths: Vec::new(),
            tokenizer,
        }
    }

    /// # Errors
    /// `path` 不是合法的 RFC 9535 `JSONPath` 时返回错误。
    pub fn path(mut self, path: &str) -> Result<Self, ParseError> {
        self.paths.push(JsonPath::parse(path)?);
        Ok(self)
    }

    /// 返回 `None` 表示取不到文本——调用方应退回配置上限，
    /// 宁可高估也不能不预扣。
    #[must_use]
    pub fn count(&self, body: &[u8]) -> Option<i64> {
        let doc: serde_json::Value = serde_json::from_slice(body).ok()?;
        let mut found = false;
        let mut tokens = 0i64;

        for path in &self.paths {
            for node in path.query(&doc).all() {
                if let Some(text) = node.as_str() {
                    found = true;
                    tokens = tokens.saturating_add(
                        i64::try_from(self.tokenizer.count(text)).unwrap_or(i64::MAX),
                    );
                }
            }
        }
        found.then_some(tokens)
    }
}

/// 按协议预置的计数器。M1 硬编码 `OpenAI` 兼容协议，M2 起改由描述文件驱动。
pub struct RequestTokenCounter;

impl RequestTokenCounter {
    /// # Panics
    /// 内置路径必然合法。
    #[must_use]
    pub fn openai_chat() -> TextSpec {
        let build = || -> Result<TextSpec, ParseError> {
            TextSpec::new(Tokenizer::O200kBase)
                .path("$.messages[*].content")?
                .path("$.messages[*].content[*].text")?
                .path("$.input")?
                .path("$.prompt")
        };
        build().expect("内置 JSONPath 必然合法")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Tokenizer;

    fn openai() -> TextSpec {
        TextSpec::new(Tokenizer::O200kBase)
            .path("$.messages[*].content")
            .unwrap()
            .path("$.messages[*].content[*].text")
            .unwrap()
    }

    #[test]
    fn counts_tokens_across_messages() {
        let body = br#"{"model":"x","messages":[
            {"role":"user","content":" hello world"},
            {"role":"assistant","content":" hi"}
        ]}"#;
        // " hello"+" world"+" hi" = 3 token
        assert_eq!(openai().count(body), Some(3));
    }

    /// 多模态请求的 content 是数组，文本在 text 字段里
    #[test]
    fn counts_tokens_in_array_content() {
        let body = br#"{"messages":[{"role":"user","content":[
            {"type":"text","text":" hello world"},
            {"type":"image_url","image_url":{"url":"data:image/png;base64,AAAA"}}
        ]}]}"#;
        assert_eq!(openai().count(body), Some(2));
    }

    /// 取不到文本时返回 None，由调用方退回配置上限——宁可高估也不能不预扣
    #[test]
    fn returns_none_when_no_text_is_found() {
        assert_eq!(openai().count(br#"{"model":"x"}"#), None);
    }

    #[test]
    fn returns_none_for_non_json() {
        assert_eq!(openai().count(b"not json"), None);
    }

    /// 空字符串不算 token，但也不该让整体退化为 None
    #[test]
    fn empty_content_counts_as_zero() {
        let body = br#"{"messages":[{"role":"user","content":""}]}"#;
        assert_eq!(openai().count(body), Some(0));
    }

    #[test]
    fn rejects_invalid_jsonpath() {
        assert!(TextSpec::new(Tokenizer::O200kBase).path("$.[").is_err());
    }
}
