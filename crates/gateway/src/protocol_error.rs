//! 拒绝与错误响应的 body 构造。
//!
//! **按入站协议的错误格式构造**——`OpenAI` 协议进来就返回同样格式的 error 对象，
//! 否则客户端 SDK 无法解析，用户看到的是一堆解析异常而非「服务过载」。

use gw_core::ProtocolKind;
use serde_json::{Value, json};

/// `kind` 是协议内的错误类型标识，`message` 是给人看的说明。
#[must_use]
pub fn error_body(protocol: &ProtocolKind, status: u16, kind: &str, message: &str) -> Value {
    match protocol {
        ProtocolKind::OpenAiChat | ProtocolKind::OpenAiResponses => json!({
            "error": {
                "message": message,
                "type": kind,
                "code": kind,
                "param": Value::Null,
            }
        }),
        ProtocolKind::AnthropicMessages => json!({
            "type": "error",
            "error": { "type": kind, "message": message }
        }),
        ProtocolKind::GeminiGenerateContent => json!({
            "error": { "code": status, "message": message, "status": kind }
        }),
        // 厂商私有协议无从得知其错误格式，退回中性结构
        ProtocolKind::Native(_) => json!({
            "error": { "code": kind, "message": message }
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gw_core::{ProtocolKind, ProviderId};
    use serde_json::json;

    #[test]
    fn openai_shape() {
        let body = error_body(
            &ProtocolKind::OpenAiChat,
            503,
            "server_overloaded",
            "服务过载，请稍后重试",
        );
        assert_eq!(
            body,
            json!({"error": {
                "message": "服务过载，请稍后重试",
                "type": "server_overloaded",
                "code": "server_overloaded",
                "param": null,
            }})
        );
    }

    #[test]
    fn responses_protocol_uses_the_openai_shape() {
        let a = error_body(&ProtocolKind::OpenAiChat, 503, "t", "m");
        let b = error_body(&ProtocolKind::OpenAiResponses, 503, "t", "m");
        assert_eq!(a, b);
    }

    #[test]
    fn anthropic_shape() {
        let body = error_body(
            &ProtocolKind::AnthropicMessages,
            529,
            "overloaded_error",
            "服务过载",
        );
        assert_eq!(
            body,
            json!({
                "type": "error",
                "error": {"type": "overloaded_error", "message": "服务过载"}
            })
        );
    }

    /// Gemini 的错误体把 HTTP 状态码复制进 body
    #[test]
    fn gemini_shape_carries_the_status_code() {
        let body = error_body(
            &ProtocolKind::GeminiGenerateContent,
            503,
            "UNAVAILABLE",
            "服务过载",
        );
        assert_eq!(
            body,
            json!({"error": {"code": 503, "message": "服务过载", "status": "UNAVAILABLE"}})
        );
    }

    /// 厂商私有协议无从得知其错误格式，退回一个中性结构
    #[test]
    fn native_protocol_falls_back_to_a_neutral_shape() {
        let body = error_body(
            &ProtocolKind::Native(ProviderId("volc".into())),
            503,
            "overloaded",
            "服务过载",
        );
        assert_eq!(
            body,
            json!({"error": {"code": "overloaded", "message": "服务过载"}})
        );
    }
}
