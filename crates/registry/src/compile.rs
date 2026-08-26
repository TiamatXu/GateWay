//! 描述文件 → 目录：语义校验、里程碑门禁、编译。
//!
//! 三层校验的第三层在这里。JSON Schema 查不出「引用了不存在的端点」，
//! 这一层才是描述层的价值所在。
//!
//! 未实现的端点是**延期**而非报错：它们通过全部校验、进入目录记录，
//! 但不挂上入站路由。这样一份 M3 端点的描述文件可以先写出来验证
//! schema 装得下，而不必假装它跑得起来。

use std::collections::{HashMap, HashSet};

use gw_core::{HandleRole, ProtocolKind, ProviderId, ResponseForm};
use smol_str::SmolStr;

use crate::catalog::{Catalog, EndpointDesc, Handles, InboundRoute, Usage};
use crate::error::RegistryError;
use crate::hook::HookRegistry;
use crate::locator::Locator;
use crate::milestone::{self, Unsupported};
use crate::rule::{Source, UsageRule};
use crate::schema::{
    AuthDef, Descriptor, EndpointDef, Method, SCHEMA_VERSION, UsageDef, UsageRuleDef,
};
use crate::template::PathTemplate;

/// 一次加载的产物。
#[derive(Debug)]
pub struct Loaded {
    pub catalog: Catalog,
    /// 校验通过但当前解释器跑不了的端点
    pub deferred: Vec<Deferred>,
}

#[derive(Debug, Clone)]
pub struct Deferred {
    pub provider: ProviderId,
    pub endpoint: SmolStr,
    pub gaps: Vec<Unsupported>,
}

/// 把若干份描述文件编译成一份快照。
///
/// # Errors
/// 任一描述文件存在语义错误时整体失败——热更新是原子替换，
/// 半份目录比旧目录更危险。
pub fn compile(
    descriptors: Vec<Descriptor>,
    hooks: &HookRegistry,
    version: u64,
) -> Result<Loaded, RegistryError> {
    let mut cx = Cx {
        hooks,
        catalog: Catalog {
            version,
            ..Catalog::default()
        },
        deferred: Vec::new(),
        route_index: HashMap::new(),
    };
    for d in descriptors {
        cx.add_provider(d)?;
    }
    cx.build_routers()?;
    Ok(Loaded {
        catalog: cx.catalog,
        deferred: cx.deferred,
    })
}

struct Cx<'a> {
    hooks: &'a HookRegistry,
    catalog: Catalog,
    deferred: Vec<Deferred>,
    /// `(method, inbound path)` → routes 下标
    route_index: HashMap<(Method, String), usize>,
}

impl Cx<'_> {
    fn add_provider(&mut self, d: Descriptor) -> Result<(), RegistryError> {
        let provider = d.provider.clone();
        let err = |ep: &str, msg: String| RegistryError::Semantic {
            provider: provider.as_str().to_owned(),
            endpoint: ep.to_owned(),
            message: msg,
        };

        if d.schema_version != SCHEMA_VERSION {
            return Err(err(
                "",
                format!(
                    "schema_version {} 与当前支持的 {SCHEMA_VERSION} 不符",
                    d.schema_version
                ),
            ));
        }

        let mut seen = HashSet::new();
        for ep in &d.endpoints {
            if !seen.insert(ep.id.clone()) {
                return Err(err(&ep.id, "端点 id 在 provider 内重复".to_owned()));
            }
        }

        // 交叉引用要在全部端点已知之后才能查
        validate_async_refs(&d, &provider)?;

        for ep in d.endpoints {
            let auth = ep
                .auth
                .clone()
                .or_else(|| d.defaults.auth.clone())
                .ok_or_else(|| err(&ep.id, "既无端点级 auth 也无 provider 默认 auth".into()))?;

            let gaps = milestone::gaps(&ep, Some(&auth));
            let desc = self.compile_endpoint(&d.defaults, &provider, ep, auth)?;
            let idx = self.catalog.endpoints.len();
            self.catalog
                .by_id
                .insert((provider.clone(), desc.id.clone()), idx);

            if gaps.is_empty() {
                self.attach_route(&desc, idx)?;
            } else {
                tracing::info!(
                    provider = provider.as_str(),
                    endpoint = %desc.id,
                    gaps = gaps.len(),
                    "端点声明合法但当前解释器未实现，已延期"
                );
                self.deferred.push(Deferred {
                    provider: provider.clone(),
                    endpoint: desc.id.clone(),
                    gaps,
                });
            }
            self.catalog.endpoints.push(desc);
        }
        Ok(())
    }

    fn compile_endpoint(
        &self,
        defaults: &crate::schema::Defaults,
        provider: &ProviderId,
        ep: EndpointDef,
        auth: AuthDef,
    ) -> Result<EndpointDesc, RegistryError> {
        let err = |msg: String| RegistryError::Semantic {
            provider: provider.as_str().to_owned(),
            endpoint: ep.id.to_string(),
            message: msg,
        };

        let base_url = ep
            .route
            .base_url
            .clone()
            .or_else(|| defaults.base_url.clone())
            .ok_or_else(|| err("既无端点级 base_url 也无 provider 默认 base_url".into()))?;

        let inbound = PathTemplate::parse(&ep.route.inbound).map_err(&err)?;
        let upstream = PathTemplate::parse(&ep.route.upstream).map_err(&err)?;

        // 上游模板只能用入站匹配得到的参数，否则运行期必然渲染失败
        let inbound_params: HashSet<_> = inbound.params().iter().collect();
        for p in upstream.params() {
            if !inbound_params.contains(p) {
                return Err(err(format!(
                    "上游路径引用了参数 {p}，但入站路径 {} 没有声明它",
                    ep.route.inbound
                )));
            }
        }

        validate_handles(&ep, &inbound_params, &err)?;
        validate_auth(&auth, &err)?;

        let protocol = ep
            .protocol
            .clone()
            .or_else(|| defaults.protocol.clone())
            .unwrap_or_else(|| ProtocolKind::Native(provider.clone()));

        Ok(EndpointDesc {
            id: ep.id.clone(),
            provider: provider.clone(),
            shape: ep.shape,
            protocol,
            method: ep.route.method,
            inbound: ep.route.inbound.clone(),
            upstream,
            base_url,
            model: ep.model.clone(),
            stream_flag: ep.stream_flag.clone(),
            headers: ep.route.headers.into_iter().collect(),
            query: ep.route.query.into_iter().collect(),
            auth,
            usage: self.compile_usage(&ep.usage, &err)?,
            handles: Handles {
                issue: ep.handles.issue,
                consume: ep.handles.consume,
            },
            async_task: ep.async_task,
        })
    }

    fn compile_usage(
        &self,
        u: &UsageDef,
        err: &impl Fn(String) -> RegistryError,
    ) -> Result<Usage, RegistryError> {
        Ok(Usage {
            estimate: self.compile_rules(&u.estimate, "estimate", err)?,
            on_submit: self.compile_rules(&u.on_submit, "on_submit", err)?,
            actual: self.compile_rules(&u.actual, "actual", err)?,
            fallback: self.compile_rules(&u.fallback, "fallback", err)?,
        })
    }

    fn compile_rules(
        &self,
        rules: &[UsageRuleDef],
        slot: &str,
        err: &impl Fn(String) -> RegistryError,
    ) -> Result<Vec<UsageRule>, RegistryError> {
        rules
            .iter()
            .map(|r| {
                let dim = r.dim.as_str();
                let source = match (&r.read, &r.hook) {
                    (Some(p), None) => Source::Read(p.clone()),
                    (None, Some(name)) => Source::Hook(self.hooks.get(name).ok_or_else(|| {
                        err(format!("usage.{slot}[{dim}] 引用了未注册的 hook {name}"))
                    })?),
                    (Some(_), Some(_)) => {
                        return Err(err(format!("usage.{slot}[{dim}] 同时写了 read 与 hook")));
                    }
                    (None, None) => {
                        return Err(err(format!("usage.{slot}[{dim}] 既无 read 也无 hook")));
                    }
                };
                // tokenize 要一条指向文本的路径，hook 返回值不是文本来源
                if r.tokenize.is_some() && !matches!(source, Source::Read(_)) {
                    return Err(err(format!("usage.{slot}[{dim}] 的 tokenize 需要配 read")));
                }
                Ok(UsageRule {
                    dim: r.dim.clone(),
                    source,
                    map: r.map.clone(),
                    scale: r.scale,
                    default: r.default.clone(),
                    accum: r.accum,
                    tokenize: r.tokenize,
                })
            })
            .collect()
    }

    /// 把端点挂到入站路由上。同一入站路径可由多个 provider 承接，
    /// 但它们的入站面契约必须一致。
    fn attach_route(&mut self, desc: &EndpointDesc, idx: usize) -> Result<(), RegistryError> {
        let key = (desc.method, desc.inbound.clone());
        let candidate = InboundRoute::new(
            desc.method,
            desc.inbound.clone(),
            desc.shape,
            desc.protocol.clone(),
            desc.model.clone(),
            desc.stream_flag.clone(),
            desc.handles.consume.clone(),
        );
        if let Some(&ri) = self.route_index.get(&key) {
            if let Some(field) = self.catalog.routes[ri].contract_diff(&candidate) {
                return Err(RegistryError::RouteConflict {
                    method: desc.method.as_str(),
                    path: desc.inbound.clone(),
                    provider: desc.provider.as_str().to_owned(),
                    field,
                });
            }
            self.catalog.routes[ri].bind(desc.provider.clone(), idx);
        } else {
            let ri = self.catalog.routes.len();
            let mut route = candidate;
            route.bind(desc.provider.clone(), idx);
            self.catalog.routes.push(route);
            self.route_index.insert(key, ri);
        }
        Ok(())
    }

    fn build_routers(&mut self) -> Result<(), RegistryError> {
        for (i, route) in self.catalog.routes.iter().enumerate() {
            self.catalog
                .routers
                .entry(route.method)
                .or_default()
                .insert(route.path.clone(), i)
                .map_err(|e| RegistryError::Semantic {
                    provider: String::new(),
                    endpoint: String::new(),
                    message: format!("入站路径 {} 无法注册: {e}", route.path),
                })?;
        }
        Ok(())
    }
}

/// `handles.issue` 至少要有一项与 `shape.handle` 的 `Issues(k)` 同类型，
/// 否则粗粒度维度与细粒度字段映射互相矛盾。同时校验取值位置在运行期够得着：
/// 签发只能改写 JSON 响应体，消费的路径参数必须真的存在于入站路径。
fn validate_handles(
    ep: &EndpointDef,
    inbound_params: &HashSet<&SmolStr>,
    err: &impl Fn(String) -> RegistryError,
) -> Result<(), RegistryError> {
    if let HandleRole::Issues(kind) = ep.shape.handle
        && !ep.handles.issue.is_empty()
        && !ep.handles.issue.iter().any(|h| h.kind == kind)
    {
        return Err(err(format!(
            "shape.handle 声明签发 {kind:?}，但 handles.issue 里没有该类型的字段"
        )));
    }

    for h in &ep.handles.issue {
        if !h.at.is_body() {
            return Err(err(format!(
                "handles.issue 的位置 {} 不在响应体内——签发只能改写 JSON 响应体",
                h.at
            )));
        }
        if ep.shape.response != ResponseForm::Json {
            return Err(err(format!(
                "handles.issue 需要 response: json，当前是 {:?}——流式与二进制响应无法原地改写",
                ep.shape.response
            )));
        }
    }

    for h in &ep.handles.consume {
        if let Locator::PathParam(name) = &h.at
            && !inbound_params.contains(name)
        {
            return Err(err(format!(
                "handles.consume 引用了路径参数 {name}，但入站路径 {} 没有声明它",
                ep.route.inbound
            )));
        }
    }
    Ok(())
}

/// 签名要按 `<service, region>` 派生密钥，缺一算出来的签名必然被上游拒。
/// 加载期报比运行期拿一个上游鉴权错误好排查得多。
fn validate_auth(
    auth: &AuthDef,
    err: &impl Fn(String) -> RegistryError,
) -> Result<(), RegistryError> {
    if let crate::schema::InjectDef::Sign(d) = &auth.inject {
        if d.service.is_none() {
            return Err(err("auth.inject.sign 缺少 service".to_owned()));
        }
        if d.region.is_none() {
            return Err(err("auth.inject.sign 缺少 region".to_owned()));
        }
    }
    Ok(())
}

/// 提交端点显式引用轮询/取消端点，此处校验被引用者存在且形态匹配。
fn validate_async_refs(d: &Descriptor, provider: &ProviderId) -> Result<(), RegistryError> {
    let by_id: HashMap<&SmolStr, &EndpointDef> = d.endpoints.iter().map(|e| (&e.id, e)).collect();
    for ep in &d.endpoints {
        let Some(a) = &ep.async_task else { continue };
        let err = |msg: String| RegistryError::Semantic {
            provider: provider.as_str().to_owned(),
            endpoint: ep.id.to_string(),
            message: msg,
        };
        let poll = by_id
            .get(&a.poll)
            .ok_or_else(|| err(format!("async.poll 引用的端点 {} 不存在", a.poll)))?;
        if !matches!(poll.shape.handle, HandleRole::Consumes(_)) {
            return Err(err(format!(
                "async.poll 指向的 {} 形态是 {:?}，应为 Consumes(Task)",
                a.poll, poll.shape.handle
            )));
        }
        if let Some(cancel) = &a.cancel {
            let c = by_id
                .get(cancel)
                .ok_or_else(|| err(format!("async.cancel 引用的端点 {cancel} 不存在")))?;
            if !matches!(c.shape.handle, HandleRole::Terminates(_)) {
                return Err(err(format!(
                    "async.cancel 指向的 {cancel} 形态是 {:?}，应为 Terminates(Task)",
                    c.shape.handle
                )));
            }
        }
    }
    Ok(())
}
