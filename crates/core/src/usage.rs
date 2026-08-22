use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

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
    pub const STORAGE_BYTE_SECONDS: &str = "storage_byte_seconds";
}

/// 用量维度名。
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct UsageDim(SmolStr);

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
