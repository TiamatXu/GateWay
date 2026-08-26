//! 里程碑门禁。
//!
//! schema 一次定义完整，解释器分阶段实现。未实现的字段在**加载期**报出
//! 所需里程碑，而不是运行期才发现——描述文件作者应当立刻知道自己写的
//! 端点跑不跑得起来。

use gw_core::{BillingTiming, EndpointShape, RequestForm, ResponseForm};

use crate::schema::{CredentialDef, EndpointDef};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Milestone {
    M2,
    M3,
    M4,
}

impl std::fmt::Display for Milestone {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::M2 => "M2",
            Self::M3 => "M3",
            Self::M4 => "M4",
        })
    }
}

/// 解释器**完整**实现到哪个里程碑。里程碑分片实现期间，某个功能到底能不能跑
/// 以 `gaps` 为准——它按字段回答，这个常量只回答整体进度。
pub const IMPLEMENTED: Milestone = Milestone::M2;

/// 一处尚未实现的声明。
#[derive(Debug, Clone)]
pub struct Unsupported {
    pub field: &'static str,
    pub value: String,
    pub needs: Milestone,
}

fn shape_gaps(shape: EndpointShape, out: &mut Vec<Unsupported>) {
    let mut push = |field, value: String, needs| {
        out.push(Unsupported {
            field,
            value,
            needs,
        });
    };

    match shape.request {
        RequestForm::None | RequestForm::Json => {}
        f @ (RequestForm::Multipart | RequestForm::Binary | RequestForm::JsonThenBinary) => {
            push("shape.request", format!("{f:?}"), Milestone::M3);
        }
    }
    match shape.response {
        ResponseForm::Json | ResponseForm::Sse => {}
        f @ (ResponseForm::Ndjson | ResponseForm::Binary) => {
            push("shape.response", format!("{f:?}"), Milestone::M3);
        }
        ResponseForm::Duplex => push("shape.response", "Duplex".to_owned(), Milestone::M4),
    }
    // shape.handle 三种角色都已实现（M3 §4.3），无需门禁
    match shape.billing {
        BillingTiming::InRequest | BillingTiming::NotBilled | BillingTiming::OnTerminal => {}
        BillingTiming::Metered => push("shape.billing", "Metered".to_owned(), Milestone::M3),
        BillingTiming::Session => push("shape.billing", "Session".to_owned(), Milestone::M4),
    }
}

/// 盘点一个端点声明里尚未实现的部分。返回空表示当前解释器能跑。
#[must_use]
pub fn gaps(ep: &EndpointDef, auth: Option<&crate::schema::AuthDef>) -> Vec<Unsupported> {
    let mut out = Vec::new();
    shape_gaps(ep.shape, &mut out);

    // auth.inject: sign 已实现（M3 §2），只有 token 换取还需要 IO 与缓存
    if let Some(auth) = auth
        && matches!(auth.credential, CredentialDef::Oauth2ClientCredentials(_))
    {
        out.push(Unsupported {
            field: "auth.credential",
            value: "oauth2_client_credentials".to_owned(),
            needs: Milestone::M3,
        });
    }

    out
}
