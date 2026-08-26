//! Provider 描述文件的 schema、加载校验与端点目录。
//!
//! 描述文件是**数据**，不是程序：只能填值和选算子，算子是有限具名集合。
//! 验收指标是「接入一个全新端点需要编写 0 行 Rust 代码」。
//!
//! 设计见 `docs/superpowers/specs/2026-08-26-m2-descriptor-layer-design.md`。

pub mod auth;
pub mod catalog;
pub mod compile;
pub mod error;
pub mod hook;
pub mod load;
pub mod locator;
pub mod milestone;
pub mod rule;
pub mod schema;
pub mod sign;
pub mod template;

pub use auth::{AuthError, Injected, inject};
pub use catalog::{Catalog, EndpointDesc, InboundMatch, InboundRoute, Usage};
pub use compile::{Deferred, Loaded, compile};
pub use error::RegistryError;
pub use hook::{HookError, HookRegistry, OperatorHook};
pub use load::{Registry, load_dir, parse_file, parse_str};
pub use locator::{Locator, PathExpr};
pub use milestone::{IMPLEMENTED, Milestone, Unsupported};
pub use rule::{Source, UsageRule};
pub use schema::{Descriptor, Method, SCHEMA_VERSION};
pub use sign::{SignCtx, SignError};
pub use template::PathTemplate;

/// 端点目录的只读视图。数据平面按此取绑定，不关心目录怎么来的。
pub trait ProviderCatalog: Send + Sync {
    /// 按入站方法与路径解析端点。
    fn resolve(&self, method: Method, path: &str) -> Option<InboundMatch<'_>>;
    /// 配置热更新的版本号，用于快照绑定。
    fn snapshot_version(&self) -> u64;
}

impl ProviderCatalog for Catalog {
    fn resolve(&self, method: Method, path: &str) -> Option<InboundMatch<'_>> {
        Self::resolve(self, method, path)
    }

    fn snapshot_version(&self) -> u64 {
        self.version()
    }
}
