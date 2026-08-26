//! 描述文件驱动的端点解析。
//!
//! M0/M1 时数据平面靠路径前缀猜协议、靠全局常量取用量规则、按固定时点计费。
//! 这些厂商分支在此处收口：请求怎么处理，全部来自 Provider 描述文件。

use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use gw_core::ProviderId;
use gw_meter::UsageSpec;
use gw_registry::{Catalog, EndpointDesc, HookRegistry, Registry, RegistryError};
use smol_str::SmolStr;

/// 一个端点的两份用量抽取规则。它们读的是不同的文档，不能共用。
pub struct EndpointSpecs {
    /// 读请求体，用于预扣估算
    pub estimate: Arc<UsageSpec>,
    /// 读响应体（含流式聚合与文本兜底），用于结算
    pub response: Arc<UsageSpec>,
}

/// 与某个目录快照对应的规则表。`JSONPath` 编译代价不小，
/// 按快照构建一次，请求路径上只做查表。
pub struct SpecTable {
    version: u64,
    by_endpoint: HashMap<(ProviderId, SmolStr), Arc<EndpointSpecs>>,
}

impl SpecTable {
    fn build(catalog: &Catalog) -> Self {
        let by_endpoint = catalog
            .endpoints()
            .iter()
            .map(|e| {
                let mut response = e.usage.actual.clone();
                response.extend(e.usage.fallback.iter().cloned());
                let specs = EndpointSpecs {
                    estimate: Arc::new(UsageSpec::from_rules(&e.usage.estimate)),
                    response: Arc::new(UsageSpec::from_rules(&response)),
                };
                ((e.provider.clone(), e.id.clone()), Arc::new(specs))
            })
            .collect();
        Self {
            version: catalog.version(),
            by_endpoint,
        }
    }

    #[must_use]
    pub fn get(&self, desc: &EndpointDesc) -> Option<&Arc<EndpointSpecs>> {
        self.by_endpoint
            .get(&(desc.provider.clone(), desc.id.clone()))
    }
}

/// 端点目录与其规则表。热更新时两者一同换代。
pub struct Endpoints {
    registry: Registry,
    specs: ArcSwap<SpecTable>,
}

impl Endpoints {
    /// 从描述文件目录打开。
    ///
    /// # Errors
    /// 目录不可读或任一描述文件校验不通过时返回错误——启动期没有旧快照可退回。
    pub fn open(
        dir: impl Into<std::path::PathBuf>,
        hooks: HookRegistry,
    ) -> Result<Self, RegistryError> {
        let registry = Registry::open(dir, hooks)?;
        let specs = SpecTable::build(&registry.snapshot());
        Ok(Self {
            registry,
            specs: ArcSwap::from_pointee(specs),
        })
    }

    /// 取当前快照。规则表落后于目录时就地重建——重载不在请求路径上，
    /// 由第一个撞上新版本的请求付这次代价。
    #[must_use]
    pub fn snapshot(&self) -> (Arc<Catalog>, Arc<SpecTable>) {
        let catalog = self.registry.snapshot();
        let specs = self.specs.load_full();
        if specs.version == catalog.version() {
            return (catalog, specs);
        }
        let rebuilt = Arc::new(SpecTable::build(&catalog));
        self.specs.store(Arc::clone(&rebuilt));
        (catalog, rebuilt)
    }

    /// 重新加载描述文件。失败时保留旧快照。
    ///
    /// # Errors
    /// 新目录校验不通过时返回错误，此时活动快照不变。
    pub fn reload(&self) -> Result<u64, RegistryError> {
        self.registry.reload()
    }
}
