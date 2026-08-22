//! 路由决策、负载均衡、熔断。健康统计与熔断器保持节点本地。
//!
//! M0 只定义契约：数据平面走单渠道直连（SIMPLIFIED(M0)）。
//! 打分与过滤在 M3 实现，路由策略在实施前需专项讨论。

use gw_core::{ChannelId, EndpointShape, InboundRequest, ProtocolKind};
use url::Url;

#[derive(Debug, thiserror::Error)]
pub enum RouteError {
    #[error("没有可用渠道: {0}")]
    NoChannel(String),
    #[error("端点未知: {0}")]
    UnknownEndpoint(String),
}

#[derive(Debug, Clone)]
pub struct Route {
    pub channel: ChannelId,
    pub outbound: ProtocolKind,
    pub shape: EndpointShape,
    pub upstream: Url,
    /// 入站协议 != 出站协议。为 false 时走原生透传，不解析 body。
    pub needs_transform: bool,
}

pub trait RouteResolver: Send + Sync {
    /// # Errors
    /// 无可用渠道或端点未知。
    fn resolve(&self, req: &InboundRequest<'_>) -> Result<Route, RouteError>;
}
