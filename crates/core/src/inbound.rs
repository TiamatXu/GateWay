use crate::{ApiKeyId, ProtocolKind};

/// 入站请求的路由与准入判据。不含 body——透传优先，body 只在需要时读取。
#[derive(Debug, Clone)]
pub struct InboundRequest<'a> {
    pub protocol: ProtocolKind,
    pub path: &'a str,
    /// 部分端点的模型在 body 中，解析后回填
    pub model: Option<&'a str>,
    pub key_id: Option<ApiKeyId>,
}
