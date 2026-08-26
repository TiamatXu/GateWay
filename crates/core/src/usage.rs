use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

/// JSON 值转账单用量。数量一律为整数，浮点数向零截断，非数值与超出 `i64`
/// 范围的值一律返回 `None`——宁可漏记一个维度，也不能把垃圾数字带进账单。
///
/// 放在 `core`：抽取（`meter`）与描述文件求值（`registry`）都要用，
/// 而这是一条计费正确性规则，两处各写一遍必然漂移。
#[must_use]
pub fn as_billable_i64(v: &serde_json::Value) -> Option<i64> {
    if let Some(n) = v.as_i64() {
        return Some(n);
    }
    let f = v.as_f64()?.trunc();
    if f.is_finite() && f >= -(2f64.powi(63)) && f < 2f64.powi(63) {
        // 上一行已确保落在 i64 范围内
        #[allow(clippy::cast_possible_truncation)]
        Some(f as i64)
    } else {
        None
    }
}

/// 内置用量维度。维度是开放集合，这里只是常用名的常量，不构成穷举。
pub mod dims {
    pub const INPUT_TOKENS: &str = "input_tokens";
    pub const OUTPUT_TOKENS: &str = "output_tokens";
    pub const CACHED_INPUT: &str = "cached_input";
    pub const CACHE_WRITE: &str = "cache_write";
    pub const REASONING_TOKENS: &str = "reasoning_tokens";
    pub const AUDIO_MILLIS: &str = "audio_millis";
    pub const IMAGES: &str = "images";
    pub const REQUESTS: &str = "requests";
    /// 客户端声明的输出上限。不是账单维度，是预扣估算的输入——
    /// 放在这里是为了让它也由描述文件的 `usage.estimate` 规则提供，
    /// 而不是在数据平面硬取 `max_tokens` 字段。
    pub const MAX_OUTPUT_TOKENS: &str = "max_output_tokens";
    pub const STORAGE_BYTE_SECONDS: &str = "storage_byte_seconds";
}

/// 同一维度多次命中时的累积方式。描述文件的 `accum` 算子。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub enum Accum {
    /// 上游上报累计值，后值覆盖前值
    #[default]
    Last,
    /// 上游上报增量，逐帧累加
    Sum,
}

/// tokenizer 编码。词表 embed 进二进制，不做运行时下载。
///
/// 枚举在此，实现在 `meter`——`core` 不引 `tiktoken-rs`。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub enum Tokenizer {
    O200kBase,
    Cl100kBase,
}

/// 用量维度名。
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
#[cfg_attr(
    feature = "schema",
    derive(schemars::JsonSchema),
    schemars(transparent)
)]
pub struct UsageDim(#[cfg_attr(feature = "schema", schemars(with = "String"))] SmolStr);

impl UsageDim {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<T: Into<SmolStr>> From<T> for UsageDim {
    fn from(v: T) -> Self {
        Self(v.into())
    }
}

/// 用量向量。数量一律为整数，小数单位由维度自身表达（如 `audio_millis`）。
///
/// 用 `BTreeMap` 而非 `HashMap`：账单 breakdown 需要稳定顺序以便对比与审计。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct UsageVector(BTreeMap<UsageDim, i64>);

impl UsageVector {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn get(&self, dim: &str) -> i64 {
        self.0.get(&UsageDim::from(dim)).copied().unwrap_or(0)
    }

    pub fn set(&mut self, dim: impl Into<UsageDim>, value: i64) {
        self.0.insert(dim.into(), value);
    }

    /// 按维度累加。用于按增量上报用量的协议。
    pub fn add(&mut self, other: &Self) {
        for (dim, value) in &other.0 {
            self.0
                .entry(dim.clone())
                .and_modify(|v| *v = v.saturating_add(*value))
                .or_insert(*value);
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&UsageDim, i64)> {
        self.0.iter().map(|(d, v)| (d, *v))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_dimension_reads_as_zero() {
        let v = UsageVector::new();
        assert_eq!(v.get(dims::INPUT_TOKENS), 0);
    }

    #[test]
    fn set_then_get_returns_value() {
        let mut v = UsageVector::new();
        v.set(dims::INPUT_TOKENS, 120);
        assert_eq!(v.get(dims::INPUT_TOKENS), 120);
    }

    #[test]
    fn set_overwrites_previous_value() {
        let mut v = UsageVector::new();
        v.set(dims::OUTPUT_TOKENS, 10);
        v.set(dims::OUTPUT_TOKENS, 40);
        assert_eq!(v.get(dims::OUTPUT_TOKENS), 40);
    }

    #[test]
    fn add_sums_overlapping_and_keeps_disjoint_dimensions() {
        let mut a = UsageVector::new();
        a.set(dims::INPUT_TOKENS, 100);
        a.set(dims::OUTPUT_TOKENS, 5);

        let mut b = UsageVector::new();
        b.set(dims::OUTPUT_TOKENS, 7);
        b.set(dims::REASONING_TOKENS, 3);

        a.add(&b);

        assert_eq!(a.get(dims::INPUT_TOKENS), 100);
        assert_eq!(a.get(dims::OUTPUT_TOKENS), 12);
        assert_eq!(a.get(dims::REASONING_TOKENS), 3);
    }

    /// 用量维度是开放集合，厂商私有维度必须能直接落进来
    #[test]
    fn accepts_unknown_dimension() {
        let mut v = UsageVector::new();
        v.set("volc_asset_bytes", 4096);
        assert_eq!(v.get("volc_asset_bytes"), 4096);
    }

    /// 账单 breakdown 需要稳定顺序以便对比与审计
    #[test]
    fn serializes_in_stable_key_order() {
        let mut v = UsageVector::new();
        v.set(dims::OUTPUT_TOKENS, 5);
        v.set(dims::INPUT_TOKENS, 100);
        v.set(dims::CACHED_INPUT, 20);

        assert_eq!(
            serde_json::to_string(&v).unwrap(),
            r#"{"cached_input":20,"input_tokens":100,"output_tokens":5}"#
        );
    }

    #[test]
    fn roundtrips_through_json() {
        let mut v = UsageVector::new();
        v.set(dims::INPUT_TOKENS, 100);
        v.set("audio_millis", 2500);

        let json = serde_json::to_string(&v).unwrap();
        assert_eq!(serde_json::from_str::<UsageVector>(&json).unwrap(), v);
    }
}
