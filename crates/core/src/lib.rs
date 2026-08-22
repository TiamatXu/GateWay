//! 领域类型。仅类型与纯逻辑，不做 IO。

pub mod extract;
pub mod ids;
pub mod inbound;
pub mod money;
pub mod redact;
pub mod shape;
pub mod usage;

pub use extract::UsageExtractor;
pub use ids::{AccountId, ApiKeyId, ChannelId, HandleId, HoldId, NodeId, ProviderId, RequestId};
pub use inbound::InboundRequest;
pub use money::Money;
pub use redact::HeaderRedactor;
pub use shape::{
    BillingTiming, EndpointShape, HandleKind, HandleRole, ProtocolKind, RequestForm, ResponseForm,
    RetryPolicy,
};
pub use usage::{UsageDim, UsageVector, dims};
