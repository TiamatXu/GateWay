use serde::{Deserialize, Serialize};

/// 端点形态。五个维度正交，是**有限集合**，由 `core` 建模；
/// 端点身份是开放集合，由描述文件表达。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EndpointShape {
    pub request: RequestForm,
    pub response: ResponseForm,
    pub handle: HandleRole,
    pub billing: BillingTiming,
    pub retry: RetryPolicy,
}

impl EndpointShape {
    /// 渠道亲和性由 `HandleRole` 推导，无需在描述文件中重复声明。
    #[must_use]
    pub const fn requires_channel_affinity(&self) -> bool {
        self.handle.requires_channel_affinity()
    }

    /// `Duplex` 仅支持透传：各厂商实时协议的会话状态机互不兼容。
    #[must_use]
    pub const fn supports_transform(&self) -> bool {
        !matches!(self.response, ResponseForm::Duplex)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestForm {
    None,
    Json,
    Multipart,
    Binary,
    JsonThenBinary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResponseForm {
    Json,
    Sse,
    Ndjson,
    Binary,
    Duplex,
}

/// 端点与虚拟句柄的关系。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HandleRole {
    None,
    Issues(HandleKind),
    Consumes(HandleKind),
    Terminates(HandleKind),
}

impl HandleRole {
    #[must_use]
    pub const fn requires_channel_affinity(&self) -> bool {
        matches!(self, Self::Consumes(_) | Self::Terminates(_))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HandleKind {
    Task,
    File,
    Batch,
    Asset,
    Cache,
    Response,
    Session,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BillingTiming {
    InRequest,
    OnTerminal,
    Metered,
    Session,
    NotBilled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetryPolicy {
    Safe,
    IdempotentWithKey,
    Unsafe,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    fn shape(handle: HandleRole, response: ResponseForm) -> EndpointShape {
        EndpointShape {
            request: RequestForm::Json,
            response,
            handle,
            billing: BillingTiming::InRequest,
            retry: RetryPolicy::Safe,
        }
    }

    /// 消费或终结句柄的端点必须回到签发该句柄的原渠道
    #[rstest]
    #[case(HandleRole::Consumes(HandleKind::Task), true)]
    #[case(HandleRole::Terminates(HandleKind::Batch), true)]
    #[case(HandleRole::Issues(HandleKind::File), false)]
    #[case(HandleRole::None, false)]
    fn derives_channel_affinity_from_handle_role(#[case] role: HandleRole, #[case] expected: bool) {
        assert_eq!(
            shape(role, ResponseForm::Json).requires_channel_affinity(),
            expected
        );
    }

    /// Duplex 各厂商会话状态机互不兼容，第一版仅透传
    #[rstest]
    #[case(ResponseForm::Duplex, false)]
    #[case(ResponseForm::Json, true)]
    #[case(ResponseForm::Sse, true)]
    #[case(ResponseForm::Ndjson, true)]
    #[case(ResponseForm::Binary, true)]
    fn duplex_alone_forbids_cross_protocol_transform(
        #[case] response: ResponseForm,
        #[case] expected: bool,
    ) {
        assert_eq!(
            shape(HandleRole::None, response).supports_transform(),
            expected
        );
    }

    /// 描述文件是 YAML/JSON，形态字段以 `snake_case` 字符串表达
    #[test]
    fn deserializes_from_descriptor_representation() {
        let json = r#"{
            "request": "json",
            "response": "sse",
            "handle": "none",
            "billing": "in_request",
            "retry": "safe"
        }"#;
        let s: EndpointShape = serde_json::from_str(json).unwrap();
        assert_eq!(s.response, ResponseForm::Sse);
        assert_eq!(s.billing, BillingTiming::InRequest);
    }

    #[test]
    fn deserializes_handle_role_with_kind() {
        let json = r#"{
            "request": "json",
            "response": "json",
            "handle": { "issues": "task" },
            "billing": "on_terminal",
            "retry": "idempotent_with_key"
        }"#;
        let s: EndpointShape = serde_json::from_str(json).unwrap();
        assert_eq!(s.handle, HandleRole::Issues(HandleKind::Task));
        assert!(!s.requires_channel_affinity());
    }
}
