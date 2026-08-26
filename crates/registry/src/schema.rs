//! Provider 描述文件的结构定义。
//!
//! 描述文件是**数据**，不是程序：只能填值和选算子，算子是有限具名集合。
//! schema 一次定义完整（覆盖 M2–M4 已知需求），解释器分阶段实现——
//! 未实现的字段在加载期由 `validate` 报出所需里程碑，而非运行期才发现。

use std::collections::BTreeMap;

use gw_core::{Accum, EndpointShape, HandleKind, ProtocolKind, ProviderId, Tokenizer, UsageDim};
use serde::Deserialize;
use smol_str::SmolStr;

use crate::locator::{Locator, PathExpr};

/// 派生 `JsonSchema` 仅在 `schema` feature 下发生：默认构建不引 `schemars`。
macro_rules! desc {
    ($($item:item)*) => {$(
        #[derive(Debug, Clone, Deserialize)]
        #[serde(deny_unknown_fields)]
        #[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
        $item
    )*};
}

/// 当前描述文件 schema 版本。不兼容变更时递增。
pub const SCHEMA_VERSION: u32 = 1;

desc! {
    /// 一个 provider 一个文件。
    pub struct Descriptor {
        pub schema_version: u32,
        pub provider: ProviderId,
        /// provider 级默认值，任意端点可覆盖
        #[serde(default)]
        pub defaults: Defaults,
        /// 锚点存放处。内容不参与解析，只为让端点之间共享 YAML 锚点
        /// （`deny_unknown_fields` 下需要一个合法的落脚点）。
        #[serde(default)]
        pub anchors: Option<serde_json::Value>,
        pub endpoints: Vec<EndpointDef>,
    }

    #[derive(Default)]
    pub struct Defaults {
        #[serde(default)]
        pub base_url: Option<String>,
        #[serde(default)]
        pub protocol: Option<ProtocolKind>,
        #[serde(default)]
        pub auth: Option<AuthDef>,
    }

    pub struct EndpointDef {
        /// 端点标识，provider 内唯一。`async.poll` 等交叉引用用它。
        #[cfg_attr(feature = "schema", schemars(with = "String"))]
        pub id: SmolStr,
        pub shape: EndpointShape,
        pub route: RouteDef,
        #[serde(default)]
        pub protocol: Option<ProtocolKind>,
        #[serde(default)]
        pub auth: Option<AuthDef>,
        /// 路由用的 model 从哪取。缺省表示该端点不按 model 路由。
        #[serde(default)]
        pub model: Option<Locator>,
        /// 客户端要求流式的标志位在哪。缺省表示该端点无流式开关。
        #[serde(default)]
        pub stream_flag: Option<Locator>,
        #[serde(default)]
        pub usage: UsageDef,
        #[serde(default)]
        pub handles: HandlesDef,
        #[serde(default, rename = "async")]
        pub async_task: Option<AsyncDef>,
    }

    /// 入站与上游分离：透传优先下两者多数相同，但资产库类端点必须能分离
    /// （不同 host、不同路径）。路径模板参数用 `{name}`，两侧同名参数自动传递。
    pub struct RouteDef {
        pub method: Method,
        pub inbound: String,
        pub upstream: String,
        /// 覆盖 provider 默认 `base_url`
        #[serde(default)]
        pub base_url: Option<String>,
        /// 固定注入的上游头，值支持 `{参数名}` 模板
        #[serde(default)]
        pub headers: BTreeMap<String, String>,
        /// 固定注入的上游查询参数
        #[serde(default)]
        pub query: BTreeMap<String, String>,
    }

    /// 凭证的获取与附加正交：`credential` 可能做 IO、有缓存、渠道级；
    /// `inject` 是纯函数、请求级。签名是 `inject` 的一个变体——
    /// 签名的本质就是把凭证附加到请求上，只是附加方式复杂。
    pub struct AuthDef {
        #[serde(default)]
        pub credential: CredentialDef,
        pub inject: InjectDef,
    }

    pub struct SignDef {
        pub alg: SignAlg,
        #[serde(default)]
        pub service: Option<String>,
        #[serde(default)]
        pub region: Option<String>,
    }

    pub struct Oauth2Def {
        pub token_url: String,
        #[serde(default)]
        pub scope: Vec<String>,
        /// 换取到的 token 的缓存时长，秒
        pub cache_ttl_secs: u64,
    }

    /// 三个时点读的是不同的文档，因此是三组独立规则，而非一组规则加时点标记。
    #[derive(Default)]
    pub struct UsageDef {
        /// 读请求体，用于 `hold` 的预扣金额
        #[serde(default)]
        pub estimate: Vec<UsageRuleDef>,
        /// 读提交响应体，用于 `extend` / `capture_partial` 调整
        #[serde(default)]
        pub on_submit: Vec<UsageRuleDef>,
        /// 读终态响应体（或流式聚合结果），用于 capture
        #[serde(default)]
        pub actual: Vec<UsageRuleDef>,
        /// 上游未返回权威 usage 时的第 3 档估算，结果标记为 estimated
        #[serde(default)]
        pub fallback: Vec<UsageRuleDef>,
    }

    /// 一次取值。算子按固定顺序求值：read/hook → map → scale，全程取不到值时落到 default。
    /// 固定顺序而非可编排管道——可编排就是表达式语言的另一种写法。
    pub struct UsageRuleDef {
        pub dim: UsageDim,
        #[serde(default)]
        pub read: Option<PathExpr>,
        /// 逃生舱：算子表里没有的那个算子。纯函数、无 IO、无状态。
        #[serde(default)]
        #[cfg_attr(feature = "schema", schemars(with = "Option<String>"))]
        pub hook: Option<SmolStr>,
        #[serde(default)]
        pub map: Option<BTreeMap<String, serde_json::Value>>,
        #[serde(default)]
        pub scale: Option<f64>,
        #[serde(default)]
        pub default: Option<serde_json::Value>,
        #[serde(default)]
        pub accum: Accum,
        /// 文本字段按 tokenizer 计数。路径可命中多个节点，逐个计数求和。
        #[serde(default)]
        pub tokenize: Option<Tokenizer>,
    }

    /// `shape.handle` 是粗粒度维度（推导渠道亲和与 Hold 挂载）；
    /// 这里是细粒度字段映射（响应里哪几个串要重写）。两者不重复。
    #[derive(Default)]
    pub struct HandlesDef {
        /// 本端点签发的句柄。一个响应可签发多个、各自不同类型。
        #[serde(default)]
        pub issue: Vec<HandleFieldDef>,
        /// 请求里引用的句柄，需换回上游 ID。位置可在路径参数或请求体任意处。
        #[serde(default)]
        pub consume: Vec<HandleFieldDef>,
    }

    pub struct HandleFieldDef {
        pub at: Locator,
        pub kind: HandleKind,
    }

    /// 提交与轮询是各自独立声明的两个端点，在提交端点上显式引用。
    /// 不靠 `HandleKind` 隐式匹配——一个 provider 有两个任务族时会静默歧义。
    pub struct AsyncDef {
        /// 轮询端点 id，加载期校验存在且形态为 `Consumes(Task)`
        #[cfg_attr(feature = "schema", schemars(with = "String"))]
        pub poll: SmolStr,
        /// 取消端点 id，形态须为 `Terminates(Task)`
        #[serde(default)]
        #[cfg_attr(feature = "schema", schemars(with = "Option<String>"))]
        pub cancel: Option<SmolStr>,
        /// 任务状态字段的位置
        pub state: Locator,
        pub terminal: TerminalStates,
    }

    pub struct TerminalStates {
        pub succeeded: Vec<String>,
        pub failed: Vec<String>,
    }
}

/// 句柄字段是入站契约的一部分：同一入站路径上各 provider 必须声明一致的
/// 消费位置，否则客户端在不同渠道上要写不同的请求。
impl PartialEq for HandleFieldDef {
    fn eq(&self, other: &Self) -> bool {
        self.kind == other.kind && self.at == other.at
    }
}

impl Eq for HandleFieldDef {}

impl AsyncDef {
    /// 把上游的状态串归一化成三值。不在两张终态表里的一律算运行中——
    /// 厂商随时可能加中间状态，把未知状态当失败会误结算。
    #[must_use]
    pub fn phase_of(&self, state: &str) -> gw_core::TaskPhase {
        use gw_core::TaskPhase;
        if self.terminal.succeeded.iter().any(|s| s == state) {
            return TaskPhase::Succeeded;
        }
        if self.terminal.failed.iter().any(|s| s == state) {
            return TaskPhase::Failed;
        }
        TaskPhase::Running
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub enum Method {
    Get,
    Post,
    Put,
    Patch,
    Delete,
}

impl Method {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Post => "POST",
            Self::Put => "PUT",
            Self::Patch => "PATCH",
            Self::Delete => "DELETE",
        }
    }
}

/// 凭证怎么来。有限枚举加参数，不是 hook——
/// token 换取需要渠道级缓存与刷新，无状态 hook 装不下。
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub enum CredentialDef {
    /// 渠道凭证原样使用
    #[default]
    Static,
    Oauth2ClientCredentials(Oauth2Def),
}

/// 凭证怎么用。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub enum InjectDef {
    Header(HeaderInject),
    Query(QueryInject),
    Sign(SignDef),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct HeaderInject {
    pub name: String,
    /// 值模板，`{credential}` 为占位符
    #[serde(default = "bearer_template")]
    pub template: String,
}

fn bearer_template() -> String {
    "Bearer {credential}".to_owned()
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct QueryInject {
    pub name: String,
    #[serde(default = "credential_template")]
    pub template: String,
}

fn credential_template() -> String {
    "{credential}".to_owned()
}

/// 签名算法是有限枚举，与 AWS service model 的 `signatureVersion` 同构。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub enum SignAlg {
    VolcV4,
    AwsSigV4,
}
