//! 算子求值。
//!
//! 算子是有限具名集合，按**固定顺序**求值：`read`/`hook` → `map` → `scale`，
//! 全程取不到值时落到 `default`。固定顺序而非可编排管道——可编排就是
//! 表达式语言的另一种写法，那条路已在设计中否决。

use std::collections::BTreeMap;

use gw_core::{Accum, Tokenizer, UsageDim, as_billable_i64};
use serde_json::Value;

use crate::hook::OperatorHook;
use crate::locator::PathExpr;

/// 值从哪来。`read` 与 `hook` 互斥。
#[derive(Debug, Clone)]
pub enum Source {
    Read(PathExpr),
    /// 逃生舱，加载期已解析为函数指针
    Hook(OperatorHook),
}

/// 编译后的用量规则。`PathExpr` 已含编译好的 `JSONPath`，运行期不再解析。
#[derive(Debug, Clone)]
pub struct UsageRule {
    pub dim: UsageDim,
    pub source: Source,
    pub map: Option<BTreeMap<String, Value>>,
    pub scale: Option<f64>,
    pub default: Option<Value>,
    pub accum: Accum,
    /// 有值时该规则按文本计 token，由 `meter` 消费（要遍历多个节点），不走 `eval`
    pub tokenize: Option<Tokenizer>,
}

impl UsageRule {
    /// 该规则是否为文本 token 兜底。
    #[must_use]
    pub const fn is_tokenized(&self) -> bool {
        self.tokenize.is_some()
    }

    /// 取值路径。`hook` 来源没有路径。
    #[must_use]
    pub const fn source_path(&self) -> Option<&PathExpr> {
        match &self.source {
            Source::Read(p) => Some(p),
            Source::Hook(_) => None,
        }
    }

    /// 按算子顺序求值。返回 `None` 表示这条规则在本文档上没有结果，
    /// 调用方应当跳过该维度而非记 0——漏记与记零在账单上含义不同。
    #[must_use]
    pub fn eval(&self, doc: &Value) -> Option<i64> {
        debug_assert!(!self.is_tokenized(), "tokenize 规则由 meter 消费");
        self.raw(doc)
            .and_then(|v| self.apply_map(v))
            .and_then(|v| self.apply_scale(&v))
            .or_else(|| self.default.as_ref().and_then(as_billable_i64))
    }

    fn raw(&self, doc: &Value) -> Option<Value> {
        match &self.source {
            Source::Read(p) => p.one(doc).cloned(),
            Source::Hook(h) => match h(doc) {
                Ok(v) => Some(v),
                Err(e) => {
                    tracing::warn!(dim = self.dim.as_str(), error = %e, "hook 求值失败");
                    None
                }
            },
        }
    }

    /// 查表。键按字符串比较：字符串值用其本身，其余用 JSON 表示形式。
    fn apply_map(&self, v: Value) -> Option<Value> {
        let Some(table) = &self.map else {
            return Some(v);
        };
        let key = match &v {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        table.get(&key).cloned()
    }

    fn apply_scale(&self, v: &Value) -> Option<i64> {
        let Some(k) = self.scale else {
            return as_billable_i64(v);
        };
        let f = v.as_f64()? * k;
        as_billable_i64(&serde_json::json!(f))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hook::HookError;
    use rstest::rstest;

    fn rule(source: Source) -> UsageRule {
        UsageRule {
            dim: UsageDim::from("d"),
            source,
            map: None,
            scale: None,
            default: None,
            accum: Accum::Last,
            tokenize: None,
        }
    }

    fn read(path: &str) -> UsageRule {
        rule(Source::Read(PathExpr::parse(path).unwrap()))
    }

    #[test]
    fn reads_a_plain_field() {
        let doc = serde_json::json!({"usage": {"prompt_tokens": 42}});
        assert_eq!(read("$.usage.prompt_tokens").eval(&doc), Some(42));
    }

    #[test]
    fn missing_field_without_default_yields_nothing() {
        // 漏记与记零在账单上含义不同，取不到就是取不到
        assert_eq!(
            read("$.usage.prompt_tokens").eval(&serde_json::json!({})),
            None
        );
    }

    #[test]
    fn default_covers_a_missing_field() {
        let mut r = read("$.duration");
        r.default = Some(serde_json::json!(5));
        assert_eq!(r.eval(&serde_json::json!({})), Some(5));
    }

    #[test]
    fn map_translates_an_enum_valued_field() {
        let mut r = read("$.size");
        r.map = Some(BTreeMap::from([
            ("1080p".to_owned(), serde_json::json!(2_073_600)),
            ("720p".to_owned(), serde_json::json!(921_600)),
        ]));
        assert_eq!(
            r.eval(&serde_json::json!({"size": "1080p"})),
            Some(2_073_600)
        );
    }

    #[test]
    fn map_miss_falls_through_to_default() {
        let mut r = read("$.size");
        r.map = Some(BTreeMap::from([(
            "720p".to_owned(),
            serde_json::json!(921_600),
        )]));
        r.default = Some(serde_json::json!(0));
        assert_eq!(r.eval(&serde_json::json!({"size": "4k"})), Some(0));
    }

    #[test]
    fn scale_converts_units() {
        let mut r = read("$.seconds");
        r.scale = Some(1000.0);
        assert_eq!(r.eval(&serde_json::json!({"seconds": 2.5})), Some(2500));
    }

    /// 算子顺序固定：read → map → scale
    #[test]
    fn operators_apply_in_declared_order() {
        let mut r = read("$.size");
        r.map = Some(BTreeMap::from([("hd".to_owned(), serde_json::json!(2))]));
        r.scale = Some(1.5);
        assert_eq!(r.eval(&serde_json::json!({"size": "hd"})), Some(3));
    }

    #[rstest]
    #[case(serde_json::json!({"n": "abc"}))]
    #[case(serde_json::json!({"n": {"a": 1}}))]
    #[case(serde_json::json!({"n": 1e300}))]
    fn non_numeric_or_out_of_range_is_dropped(#[case] doc: serde_json::Value) {
        let mut r = read("$.n");
        r.scale = Some(1e300);
        assert_eq!(r.eval(&doc), None);
    }

    #[test]
    fn hook_supplies_a_value_the_operator_table_cannot() {
        fn double(v: &Value) -> Result<Value, HookError> {
            let n = v
                .get("n")
                .and_then(Value::as_i64)
                .ok_or(HookError("no n".into()))?;
            Ok(serde_json::json!(n * 2))
        }
        let r = rule(Source::Hook(double));
        assert_eq!(r.eval(&serde_json::json!({"n": 21})), Some(42));
    }

    #[test]
    fn failing_hook_falls_back_rather_than_aborting() {
        fn boom(_: &Value) -> Result<Value, HookError> {
            Err(HookError("boom".into()))
        }
        let mut r = rule(Source::Hook(boom));
        r.default = Some(serde_json::json!(7));
        assert_eq!(r.eval(&serde_json::json!({})), Some(7));
    }
}
