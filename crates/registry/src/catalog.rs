//! 编译后的端点目录：入站匹配 + provider 绑定。
//!
//! 入站与绑定分两步查，是因为同一条入站路径可以由多个 provider 承接
//! （`/v1/chat/completions` 上 openai 与 azure 并存）。入站面的属性
//! （形态、协议、model 位置）属于**客户端契约**，各 provider 必须一致；
//! `base_url`、上游路径、鉴权、用量规则属于 provider 各自的绑定。

use std::collections::HashMap;

use gw_core::{EndpointShape, ProtocolKind, ProviderId};
use smol_str::SmolStr;

use crate::locator::Locator;
use crate::rule::UsageRule;
use crate::schema::{AsyncDef, AuthDef, HandleFieldDef, Method};
use crate::template::PathTemplate;

/// 一个 provider 对某端点的具体绑定。
#[derive(Debug, Clone)]
pub struct EndpointDesc {
    pub id: SmolStr,
    pub provider: ProviderId,
    pub shape: EndpointShape,
    pub protocol: ProtocolKind,
    pub method: Method,
    pub inbound: String,
    pub upstream: PathTemplate,
    pub base_url: String,
    /// 路由用的 model 从哪取。属入站契约，同路径上各 provider 须一致。
    pub model: Option<Locator>,
    /// 客户端要求流式的标志位在哪。同上。
    pub stream_flag: Option<Locator>,
    pub headers: Vec<(String, String)>,
    pub query: Vec<(String, String)>,
    pub auth: AuthDef,
    pub usage: Usage,
    pub handles: Handles,
    pub async_task: Option<AsyncDef>,
}

/// 三个时点各一组规则——它们读的是不同的文档。
#[derive(Debug, Clone, Default)]
pub struct Usage {
    /// 读请求体，用于预扣
    pub estimate: Vec<UsageRule>,
    /// 读提交响应体，用于调整
    pub on_submit: Vec<UsageRule>,
    /// 读终态响应体，用于结算
    pub actual: Vec<UsageRule>,
    /// 上游无权威 usage 时的第 3 档估算
    pub fallback: Vec<UsageRule>,
}

impl Usage {
    /// 可求值槽位数与其中走 hook 的个数。验收标准「hook 比例 ≤ 5%」按此机械计数。
    #[must_use]
    pub fn slot_counts(&self) -> (usize, usize) {
        let all = [
            &self.estimate,
            &self.on_submit,
            &self.actual,
            &self.fallback,
        ];
        let total: usize = all.iter().map(|v| v.len()).sum();
        let hooked = all
            .iter()
            .flat_map(|v| v.iter())
            .filter(|r| matches!(r.source, crate::rule::Source::Hook(_)))
            .count();
        (total, hooked)
    }
}

#[derive(Debug, Clone, Default)]
pub struct Handles {
    pub issue: Vec<HandleFieldDef>,
    pub consume: Vec<HandleFieldDef>,
}

/// 入站契约。同一 `(method, path)` 上所有 provider 共享。
#[derive(Debug, Clone)]
pub struct InboundRoute {
    pub method: Method,
    pub path: String,
    pub shape: EndpointShape,
    pub protocol: ProtocolKind,
    /// 路由用的 model 从哪取
    pub model: Option<Locator>,
    /// 客户端要求流式的标志位在哪
    pub stream_flag: Option<Locator>,
    /// provider → 该 provider 在此路由上的端点下标
    bindings: HashMap<ProviderId, usize>,
}

impl InboundRoute {
    pub fn providers(&self) -> impl Iterator<Item = &ProviderId> {
        self.bindings.keys()
    }

    pub(crate) fn bind(&mut self, provider: ProviderId, idx: usize) {
        self.bindings.insert(provider, idx);
    }

    pub(crate) fn new(
        method: Method,
        path: String,
        shape: EndpointShape,
        protocol: ProtocolKind,
        model: Option<Locator>,
        stream_flag: Option<Locator>,
    ) -> Self {
        Self {
            method,
            path,
            shape,
            protocol,
            model,
            stream_flag,
            bindings: HashMap::new(),
        }
    }

    /// 入站面属性与另一条声明的第一处差异。`None` 表示契约一致。
    ///
    /// 返回字段名而非布尔：描述文件作者需要知道差在哪，否则只能逐字段对眼。
    pub(crate) fn contract_diff(&self, other: &Self) -> Option<&'static str> {
        if self.shape != other.shape {
            return Some("shape");
        }
        if self.protocol != other.protocol {
            return Some("protocol");
        }
        if self.model != other.model {
            return Some("model");
        }
        if self.stream_flag != other.stream_flag {
            return Some("stream_flag");
        }
        None
    }
}

/// 一次入站匹配的结果。
pub struct InboundMatch<'a> {
    catalog: &'a Catalog,
    pub route: &'a InboundRoute,
    /// 路径模板参数，按名传递到上游路径
    pub params: HashMap<SmolStr, String>,
}

impl<'a> InboundMatch<'a> {
    /// 取指定 provider 在该路由上的绑定。
    #[must_use]
    pub fn binding(&self, provider: &ProviderId) -> Option<&'a EndpointDesc> {
        self.route
            .bindings
            .get(provider)
            .map(|&i| &self.catalog.endpoints[i])
    }

    /// 渲染上游路径。
    ///
    /// # Errors
    /// 路径参数缺失时返回错误——加载期已校验两侧参数名一致，运行期出现说明匹配器与模板不一致。
    pub fn upstream_path(&self, desc: &EndpointDesc) -> Result<String, String> {
        desc.upstream.render(&self.params)
    }
}

/// 一次加载产生的不可变快照。热更新是整体原子替换，在途请求绑定发起时的快照。
#[derive(Debug, Default)]
pub struct Catalog {
    pub(crate) endpoints: Vec<EndpointDesc>,
    pub(crate) routes: Vec<InboundRoute>,
    /// 按方法分表：matchit 的路由树不区分方法
    pub(crate) routers: HashMap<Method, matchit::Router<usize>>,
    pub(crate) by_id: HashMap<(ProviderId, SmolStr), usize>,
    pub(crate) version: u64,
}

impl Catalog {
    #[must_use]
    pub fn version(&self) -> u64 {
        self.version
    }

    #[must_use]
    pub fn endpoint(&self, provider: &ProviderId, id: &str) -> Option<&EndpointDesc> {
        self.by_id
            .get(&(provider.clone(), SmolStr::new(id)))
            .map(|&i| &self.endpoints[i])
    }

    #[must_use]
    pub fn endpoints(&self) -> &[EndpointDesc] {
        &self.endpoints
    }

    /// 按入站方法与路径匹配端点。
    #[must_use]
    pub fn resolve(&self, method: Method, path: &str) -> Option<InboundMatch<'_>> {
        let m = self.routers.get(&method)?.at(path).ok()?;
        let params = m
            .params
            .iter()
            .map(|(k, v)| (SmolStr::new(k), v.to_owned()))
            .collect();
        Some(InboundMatch {
            catalog: self,
            route: &self.routes[*m.value],
            params,
        })
    }

    /// 全目录的 hook 比例。M2 验收标准要求 ≤ 5%。
    #[must_use]
    pub fn hook_ratio(&self) -> (usize, usize) {
        self.endpoints
            .iter()
            .map(|e| e.usage.slot_counts())
            .fold((0, 0), |(t, h), (dt, dh)| (t + dt, h + dh))
    }
}
