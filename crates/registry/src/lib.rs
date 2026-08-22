//! Provider 描述文件的 schema、加载校验与渠道注册表。
//!
//! M0 只定义契约：数据平面硬编码单厂商（SIMPLIFIED(M0)）。M2 起读 YAML。
//! 验收指标是「接入一个全新端点需要编写 0 行 Rust 代码」。

use gw_core::{EndpointShape, ProviderId};
use smol_str::SmolStr;

#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    #[error("描述文件不合 schema: {0}")]
    Schema(String),
    #[error("描述文件解析失败: {0}")]
    Parse(String),
}

/// 一个端点的描述。端点身份是开放集合，由描述文件表达；
/// 端点形态是有限集合，由 `core` 建模。
#[derive(Debug, Clone)]
pub struct EndpointDesc {
    pub id: SmolStr,
    pub provider: ProviderId,
    pub shape: EndpointShape,
    /// 上游路径模板，可含占位符
    pub upstream_path: String,
    /// 用量抽取的 `JSONPath` 规则，维度名 → 路径
    pub usage_paths: Vec<(SmolStr, SmolStr)>,
}

pub trait ProviderRegistry: Send + Sync {
    fn lookup(&self, provider: &ProviderId, path: &str) -> Option<EndpointDesc>;
    /// 配置热更新的版本号，用于快照绑定。
    fn snapshot_version(&self) -> u64;
}
