//! 价格引擎：用量向量 → 规则匹配 → 金额。
//!
//! 价格是**纯数据**，不是表达式。规则按匹配维度数量决定优先级，按 `effective_from`
//! 决定版本——改价不影响历史账单。

pub mod engine;

pub use engine::{
    EstimateCeilings, PgPriceEngine, PriceCtx, PriceEngine, PriceError, Quote, RuleId,
};
