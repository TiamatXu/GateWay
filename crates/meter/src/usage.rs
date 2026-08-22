use gw_core::{UsageDim, UsageExtractor, UsageVector};
use serde_json_path::{JsonPath, ParseError};

use crate::SseParser;

/// 同一维度多次命中时的累积方式。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Accum {
    /// 上游上报累计值，后值覆盖前值
    Last,
    /// 上游上报增量，逐帧累加
    Sum,
}

#[derive(Debug)]
struct UsageRule {
    dim: UsageDim,
    path: JsonPath,
    accum: Accum,
}

/// 用量抽取规则集。最终形态由 Provider 描述文件提供，M0 在代码中构造。
#[derive(Debug, Default)]
pub struct UsageSpec {
    rules: Vec<UsageRule>,
}

impl UsageSpec {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// # Errors
    /// `path` 不是合法的 RFC 9535 `JSONPath` 时返回错误。
    pub fn rule(
        mut self,
        dim: impl Into<UsageDim>,
        path: &str,
        accum: Accum,
    ) -> Result<Self, ParseError> {
        self.rules.push(UsageRule {
            dim: dim.into(),
            path: JsonPath::parse(path)?,
            accum,
        });
        Ok(self)
    }

    fn apply(&self, doc: &serde_json::Value, out: &mut UsageVector) {
        for rule in &self.rules {
            let Some(v) = rule.path.query(doc).exactly_one().ok() else {
                continue;
            };
            let Some(n) = as_i64(v) else { continue };
            match rule.accum {
                Accum::Last => out.set(rule.dim.clone(), n),
                Accum::Sum => out.set(rule.dim.clone(), out.get(rule.dim.as_str()) + n),
            }
        }
    }
}

/// 数量一律为整数。浮点数向零截断，非数值与超出 i64 范围的值一律忽略——
/// 宁可漏记一个维度，也不能把垃圾数字带进账单。
fn as_i64(v: &serde_json::Value) -> Option<i64> {
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

/// 从 SSE 响应流中抽取用量。
#[derive(Debug)]
pub struct SseUsageExtractor {
    parser: SseParser,
    spec: UsageSpec,
    usage: UsageVector,
}

impl SseUsageExtractor {
    #[must_use]
    pub fn new(spec: UsageSpec) -> Self {
        Self {
            parser: SseParser::new(),
            spec,
            usage: UsageVector::new(),
        }
    }
}

impl UsageExtractor for SseUsageExtractor {
    fn feed(&mut self, chunk: &[u8]) {
        let (spec, usage) = (&self.spec, &mut self.usage);
        self.parser.feed(chunk, |ev| {
            // 上游可能发回 [DONE] 哨兵或错误文本，解析失败即跳过
            if let Ok(doc) = serde_json::from_str::<serde_json::Value>(ev.data) {
                spec.apply(&doc, usage);
            }
        });
    }

    fn snapshot(&self) -> UsageVector {
        self.usage.clone()
    }

    fn finish(self: Box<Self>) -> UsageVector {
        self.usage
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gw_core::{UsageExtractor, dims};

    fn openai_spec() -> UsageSpec {
        UsageSpec::new()
            .rule(dims::INPUT_TOKENS, "$.usage.prompt_tokens", Accum::Last)
            .unwrap()
            .rule(
                dims::OUTPUT_TOKENS,
                "$.usage.completion_tokens",
                Accum::Last,
            )
            .unwrap()
    }

    #[test]
    fn extracts_usage_from_final_chunk() {
        let mut e = SseUsageExtractor::new(openai_spec());
        e.feed(b"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n");
        e.feed(b"data: {\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5}}\n\n");
        e.feed(b"data: [DONE]\n\n");

        let u = Box::new(e).finish();
        assert_eq!(u.get(dims::INPUT_TOKENS), 10);
        assert_eq!(u.get(dims::OUTPUT_TOKENS), 5);
    }

    /// 断连结算的关键：流未结束时也能读到已抽取的用量
    #[test]
    fn snapshot_reflects_partial_stream() {
        let mut e = SseUsageExtractor::new(openai_spec());
        assert!(e.snapshot().is_empty());

        e.feed(b"data: {\"usage\":{\"prompt_tokens\":10}}\n\n");
        assert_eq!(e.snapshot().get(dims::INPUT_TOKENS), 10);
        assert_eq!(e.snapshot().get(dims::OUTPUT_TOKENS), 0);
    }

    /// 上游可能发回非 JSON 的哨兵或错误文本，不得 panic
    #[test]
    fn ignores_non_json_payloads() {
        let mut e = SseUsageExtractor::new(openai_spec());
        e.feed(b"data: [DONE]\n\ndata: <html>502</html>\n\n");
        assert!(e.snapshot().is_empty());
    }

    #[test]
    fn ignores_missing_fields() {
        let mut e = SseUsageExtractor::new(openai_spec());
        e.feed(b"data: {\"usage\":{\"completion_tokens\":5}}\n\n");
        let u = e.snapshot();
        assert_eq!(u.get(dims::OUTPUT_TOKENS), 5);
        assert_eq!(u.iter().count(), 1);
    }

    /// Last：后到的累计值覆盖先前值
    #[test]
    fn last_mode_overwrites() {
        let mut e = SseUsageExtractor::new(openai_spec());
        e.feed(b"data: {\"usage\":{\"completion_tokens\":3}}\n\n");
        e.feed(b"data: {\"usage\":{\"completion_tokens\":9}}\n\n");
        assert_eq!(e.snapshot().get(dims::OUTPUT_TOKENS), 9);
    }

    /// Sum：按增量上报的协议逐帧累加
    #[test]
    fn sum_mode_accumulates() {
        let spec = UsageSpec::new()
            .rule(dims::OUTPUT_TOKENS, "$.delta.tokens", Accum::Sum)
            .unwrap();
        let mut e = SseUsageExtractor::new(spec);
        e.feed(b"data: {\"delta\":{\"tokens\":2}}\n\n");
        e.feed(b"data: {\"delta\":{\"tokens\":3}}\n\n");
        assert_eq!(e.snapshot().get(dims::OUTPUT_TOKENS), 5);
    }

    /// 帧被任意切分时抽取结果不变
    #[test]
    fn works_across_arbitrary_chunk_boundaries() {
        let payload = b"data: {\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5}}\n\n";
        let mut e = SseUsageExtractor::new(openai_spec());
        for c in payload.chunks(1) {
            e.feed(c);
        }
        assert_eq!(e.snapshot().get(dims::INPUT_TOKENS), 10);
        assert_eq!(e.snapshot().get(dims::OUTPUT_TOKENS), 5);
    }

    /// 上游可能把整数写成 JSON 浮点，需接受但不得引入浮点误差
    #[test]
    fn accepts_integral_float() {
        let mut e = SseUsageExtractor::new(openai_spec());
        e.feed(b"data: {\"usage\":{\"prompt_tokens\":10.0}}\n\n");
        assert_eq!(e.snapshot().get(dims::INPUT_TOKENS), 10);
    }

    /// 超出 i64 范围或非数值的字段必须忽略，不得产生垃圾金额
    #[rstest::rstest]
    #[case("1e30")]
    #[case("-1e30")]
    #[case("\"abc\"")]
    #[case("null")]
    #[case("{}")]
    fn ignores_out_of_range_or_non_numeric(#[case] raw: &str) {
        let mut e = SseUsageExtractor::new(openai_spec());
        e.feed(format!("data: {{\"usage\":{{\"prompt_tokens\":{raw}}}}}\n\n").as_bytes());
        assert!(e.snapshot().is_empty(), "{raw} 未被忽略");
    }

    #[test]
    fn rejects_invalid_jsonpath() {
        assert!(
            UsageSpec::new()
                .rule(dims::INPUT_TOKENS, "$.[", Accum::Last)
                .is_err()
        );
    }
}
