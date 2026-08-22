use async_trait::async_trait;
use chrono::{DateTime, Utc};
use gw_core::{ChannelId, Money, UsageDim, UsageVector, dims};
use smallvec::SmallVec;
use sqlx::PgPool;

/// 单价的分母：`unit_price` 的单位是纳单位 per 百万用量单位。
/// 厂商定价普遍以百万 token 为基准，按 per 单位存储会让低价模型损失有效数字。
const PRICE_DENOMINATOR: i64 = 1_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct RuleId(pub i64);

#[derive(Debug, thiserror::Error)]
pub enum PriceError {
    #[error("模型 {model} 在 {endpoint} 上没有任何可用价格规则")]
    NoRule { model: String, endpoint: String },
    #[error("金额溢出")]
    Overflow,
    #[error("数据库错误: {0}")]
    Db(#[from] sqlx::Error),
}

pub struct PriceCtx<'a> {
    pub model: &'a str,
    pub channel: ChannelId,
    pub tier: &'a str,
    pub endpoint: &'a str,
    /// 同时决定命中哪个价格版本
    pub at: DateTime<Utc>,
    pub max_output_tokens: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct Quote {
    pub amount: Money,
    /// 成本价，用于毛利计算
    pub cost: Money,
    /// 参与本次计价的全部规则，供 explain 精确指向
    pub rule_ids: SmallVec<[RuleId; 8]>,
    /// 快照绑定
    pub rule_version: i64,
    pub breakdown: Vec<(UsageDim, i64, Money)>,
    /// 用量为估算值时置位
    pub estimated: bool,
}

/// 预扣估算的最坏情况上限。
#[derive(Debug, Clone, Copy)]
pub struct EstimateCeilings {
    pub input_tokens: i64,
    pub output_tokens: i64,
}

impl Default for EstimateCeilings {
    fn default() -> Self {
        Self {
            input_tokens: 128_000,
            output_tokens: 16_384,
        }
    }
}

#[async_trait]
pub trait PriceEngine: Send + Sync {
    /// 准入时的最坏情况预扣。
    async fn estimate_max(&self, ctx: &PriceCtx<'_>) -> Result<Money, PriceError>;
    async fn quote(&self, ctx: &PriceCtx<'_>, usage: &UsageVector) -> Result<Quote, PriceError>;
}

struct MatchedRule {
    id: i64,
    dim: String,
    unit_price: i64,
    cost_price: i64,
    version: i64,
}

pub struct PgPriceEngine {
    pool: PgPool,
    ceilings: EstimateCeilings,
}

impl PgPriceEngine {
    #[must_use]
    pub fn new(pool: PgPool, ceilings: EstimateCeilings) -> Self {
        Self { pool, ceilings }
    }

    /// 每个维度取一条规则：匹配维度最多者优先，同specificity 取生效最晚者。
    async fn match_rules(&self, ctx: &PriceCtx<'_>) -> Result<Vec<MatchedRule>, PriceError> {
        let rows = sqlx::query!(
            r#"SELECT DISTINCT ON (dim)
                      id, dim, unit_price, cost_price, version
                 FROM price_rule
                WHERE (match_model    IS NULL OR match_model    = $1)
                  AND (match_channel  IS NULL OR match_channel  = $2)
                  AND (match_tier     IS NULL OR match_tier     = $3)
                  AND (match_endpoint IS NULL OR match_endpoint = $4)
                  AND effective_from <= $5
                ORDER BY dim,
                         (match_model    IS NOT NULL)::int
                       + (match_channel  IS NOT NULL)::int
                       + (match_tier     IS NOT NULL)::int
                       + (match_endpoint IS NOT NULL)::int DESC,
                         effective_from DESC,
                         id DESC"#,
            ctx.model,
            ctx.channel.0,
            ctx.tier,
            ctx.endpoint,
            ctx.at
        )
        .fetch_all(&self.pool)
        .await?;

        if rows.is_empty() {
            return Err(PriceError::NoRule {
                model: ctx.model.to_owned(),
                endpoint: ctx.endpoint.to_owned(),
            });
        }
        Ok(rows
            .into_iter()
            .map(|r| MatchedRule {
                id: r.id,
                dim: r.dim,
                unit_price: r.unit_price,
                cost_price: r.cost_price,
                version: r.version,
            })
            .collect())
    }
}

/// 单价 × 用量 ÷ 百万
fn line_amount(unit_price: i64, quantity: i64) -> Result<Money, PriceError> {
    Money::from_nanos(unit_price)
        .checked_mul_div(quantity, PRICE_DENOMINATOR)
        .ok_or(PriceError::Overflow)
}

#[async_trait]
impl PriceEngine for PgPriceEngine {
    async fn estimate_max(&self, ctx: &PriceCtx<'_>) -> Result<Money, PriceError> {
        let rules = self.match_rules(ctx).await?;
        let output_ceiling = ctx
            .max_output_tokens
            .map_or(self.ceilings.output_tokens, i64::from);

        let mut total = Money::from_nanos(0);
        for rule in &rules {
            // 只有可预估上限的维度参与预扣，其余维度在结算时才知道
            let quantity = match rule.dim.as_str() {
                dims::INPUT_TOKENS => self.ceilings.input_tokens,
                dims::OUTPUT_TOKENS | dims::REASONING_TOKENS => output_ceiling,
                _ => continue,
            };
            total = total
                .checked_add(line_amount(rule.unit_price, quantity)?)
                .ok_or(PriceError::Overflow)?;
        }
        Ok(total)
    }

    async fn quote(&self, ctx: &PriceCtx<'_>, usage: &UsageVector) -> Result<Quote, PriceError> {
        let rules = self.match_rules(ctx).await?;

        let mut amount = Money::from_nanos(0);
        let mut cost = Money::from_nanos(0);
        let mut rule_ids = SmallVec::new();
        let mut breakdown = Vec::new();
        let mut rule_version = 0;

        // 按用量向量迭代，维持 breakdown 的稳定顺序
        for (dim, quantity) in usage.iter() {
            // 厂商随时新增维度，无规则的维度跳过而非报错
            let Some(rule) = rules.iter().find(|r| r.dim == dim.as_str()) else {
                tracing::debug!(dim = dim.as_str(), "无匹配价格规则，跳过该维度");
                continue;
            };
            let line = line_amount(rule.unit_price, quantity)?;
            amount = amount.checked_add(line).ok_or(PriceError::Overflow)?;
            cost = cost
                .checked_add(line_amount(rule.cost_price, quantity)?)
                .ok_or(PriceError::Overflow)?;
            rule_ids.push(RuleId(rule.id));
            rule_version = rule_version.max(rule.version);
            breakdown.push((dim.clone(), quantity, line));
        }

        Ok(Quote {
            amount,
            cost,
            rule_ids,
            rule_version,
            breakdown,
            estimated: false,
        })
    }
}
