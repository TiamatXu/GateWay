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
pub struct ProviderId(pub SmolStr);

impl ProviderId {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
