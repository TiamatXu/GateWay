//! 标识类型。
//!
//! 组织相关标识用 `i64`：ltree 的 label 不接受连字符，UUID 无法直接作路径分量。
//! 对外暴露的虚拟句柄用 UUID，避免泄漏内部序号。

use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use uuid::Uuid;

macro_rules! int_id {
    ($($(#[$m:meta])* $name:ident),* $(,)?) => {$(
        $(#[$m])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub i64);
    )*};
}

macro_rules! uuid_id {
    ($($(#[$m:meta])* $name:ident),* $(,)?) => {$(
        $(#[$m])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub Uuid);

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fmt(f)
            }
        }
    )*};
}

int_id!(NodeId, AccountId, ApiKeyId, ChannelId);
uuid_id!(HoldId, HandleId, RequestId);

/// Provider 是开放集合，以名称标识。
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
#[cfg_attr(
    feature = "schema",
    derive(schemars::JsonSchema),
    schemars(transparent)
)]
pub struct ProviderId(#[cfg_attr(feature = "schema", schemars(with = "String"))] pub SmolStr);

impl ProviderId {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// 限流桶标识。落在同一个桶里的请求共享令牌。
///
/// 用字符串而非枚举：限流维度是开放集合（Key、账户、模型、渠道，M3 起还有
/// 路由组），枚举每加一个维度就要改 `core`，而 `core` 要保持极瘦。
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RateKey(pub SmolStr);

impl RateKey {
    #[must_use]
    pub fn api_key(id: ApiKeyId) -> Self {
        Self(SmolStr::new(format!("k:{}", id.0)))
    }

    #[must_use]
    pub fn account(id: AccountId) -> Self {
        Self(SmolStr::new(format!("a:{}", id.0)))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for RateKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
