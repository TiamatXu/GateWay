use std::collections::BTreeMap;
use std::sync::Arc;

use gw_core::{Accum, Tokenizer, UsageDim, UsageExtractor, UsageVector};
use gw_registry::{PathExpr, Source, UsageRule};

use crate::SseParser;

/// `Tokenizer` 枚举定义在 `core`（描述文件要用），实现留在这里——`core` 不引 `tiktoken-rs`。
pub(crate) fn count_tokens(tokenizer: Tokenizer, text: &str) -> usize {
    let bpe = match tokenizer {
        Tokenizer::O200kBase => tiktoken_rs::o200k_base_singleton(),
        Tokenizer::Cl100kBase => tiktoken_rs::cl100k_base_singleton(),
    };
    bpe.encode_ordinary(text).len()
}

/// 用量抽取规则集。规则来自 Provider 描述文件（`registry` 编译产物），
/// 这里只负责把它们作用到响应文档上——`registry` 出数据，`meter` 出行为。
#[derive(Debug, Default)]
pub struct UsageSpec {
    /// 直接求值的规则
    rules: Vec<UsageRule>,
    /// 按文本计 token 的兜底规则，仅在对应维度没有权威值时生效
    fallbacks: Vec<UsageRule>,
}

impl UsageSpec {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// 从描述文件编译出的规则构造。`estimate` / `actual` 各自成一份 spec——
    /// 它们读的是不同的文档。
    #[must_use]
    pub fn from_rules(rules: &[UsageRule]) -> Self {
        let (fallbacks, rules) = rules
            .iter()
            .cloned()
            .partition::<Vec<_>, _>(UsageRule::is_tokenized);
        Self { rules, fallbacks }
    }

    /// 描述文件之外手工构造一条读取规则。测试与内置兜底用。
    ///
    /// # Errors
    /// `path` 不是合法的 RFC 9535 `JSONPath` 时返回错误。
    pub fn rule(
        mut self,
        dim: impl Into<UsageDim>,
        path: &str,
        accum: Accum,
    ) -> Result<Self, String> {
        self.rules.push(UsageRule {
            dim: dim.into(),
            source: Source::Read(PathExpr::parse(path)?),
            map: None,
            scale: None,
            default: None,
            accum,
            tokenize: None,
        });
        Ok(self)
    }

    /// 第 3 档兜底：上游未返回权威 usage 时，按生成文本估算该维度。
    ///
    /// # Errors
    /// `path` 不是合法的 RFC 9535 `JSONPath` 时返回错误。
    pub fn text_fallback(
        mut self,
        dim: impl Into<UsageDim>,
        path: &str,
        tokenizer: Tokenizer,
    ) -> Result<Self, String> {
        self.fallbacks.push(UsageRule {
            dim: dim.into(),
            source: Source::Read(PathExpr::parse(path)?),
            map: None,
            scale: None,
            default: None,
            accum: Accum::Sum,
            tokenize: Some(tokenizer),
        });
        Ok(self)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rules.is_empty() && self.fallbacks.is_empty()
    }

    /// 一次性文档（请求体、非流式响应体）的求值。
    #[must_use]
    pub fn evaluate(&self, doc: &serde_json::Value) -> UsageVector {
        let mut out = UsageVector::new();
        self.apply(doc, &mut out);
        let mut tokens = BTreeMap::new();
        self.accumulate_fallback(doc, &mut tokens);
        for (dim, n) in tokens {
            if out.get(dim.as_str()) == 0 {
                out.set(dim, n);
            }
        }
        out
    }

    /// 逐帧累加：全量缓存生成文本会突破每流内存预算。分帧编码在边界处
    /// 略有偏差，但结果本就标记为估算值。
    fn accumulate_fallback(&self, doc: &serde_json::Value, tokens: &mut BTreeMap<UsageDim, i64>) {
        for fb in &self.fallbacks {
            let (Some(path), Some(tk)) = (fb.source_path(), fb.tokenize) else {
                continue;
            };
            let mut n = 0i64;
            for node in path.compiled().query(doc).all() {
                if let Some(text) = node.as_str()
                    && !text.is_empty()
                {
                    n += i64::try_from(count_tokens(tk, text)).unwrap_or(i64::MAX);
                }
            }
            if n != 0 {
                *tokens.entry(fb.dim.clone()).or_insert(0) += n;
            }
        }
    }

    fn apply(&self, doc: &serde_json::Value, out: &mut UsageVector) {
        for rule in &self.rules {
            let Some(n) = rule.eval(doc) else { continue };
            match rule.accum {
                Accum::Last => out.set(rule.dim.clone(), n),
                Accum::Sum => out.set(rule.dim.clone(), out.get(rule.dim.as_str()) + n),
            }
        }
    }

    /// 权威 usage 是否已到齐：每个配了兜底的维度都拿到了非零权威值。
    fn has_authoritative(&self, usage: &UsageVector) -> bool {
        self.fallbacks
            .iter()
            .all(|fb| usage.get(fb.dim.as_str()) != 0)
    }
}

/// 从 SSE 响应流中抽取用量。
#[derive(Debug)]
pub struct SseUsageExtractor {
    parser: SseParser,
    spec: Arc<UsageSpec>,
    usage: UsageVector,
    /// 兜底估算出的 token 数，仅在对应维度的权威 usage 缺席时生效
    fallback_tokens: BTreeMap<UsageDim, i64>,
}

impl SseUsageExtractor {
    #[must_use]
    pub fn new(spec: Arc<UsageSpec>) -> Self {
        Self {
            parser: SseParser::new(),
            spec,
            usage: UsageVector::new(),
            fallback_tokens: BTreeMap::new(),
        }
    }

    /// 权威 usage 是否已到齐。
    fn has_authoritative(&self) -> bool {
        self.spec.has_authoritative(&self.usage)
    }

    fn resolved(&self) -> UsageVector {
        let mut u = self.usage.clone();
        for (dim, n) in &self.fallback_tokens {
            if u.get(dim.as_str()) == 0 {
                u.set(dim.clone(), *n);
            }
        }
        u
    }
}

impl UsageExtractor for SseUsageExtractor {
    fn feed(&mut self, chunk: &[u8]) {
        let (spec, usage) = (&self.spec, &mut self.usage);
        let fallback = &mut self.fallback_tokens;
        self.parser.feed(chunk, |ev| {
            // 上游可能发回 [DONE] 哨兵或错误文本，解析失败即跳过
            if let Ok(doc) = serde_json::from_str::<serde_json::Value>(ev.data) {
                spec.apply(&doc, usage);
                spec.accumulate_fallback(&doc, fallback);
            }
        });
    }

    fn snapshot(&self) -> UsageVector {
        self.resolved()
    }

    fn finish(self: Box<Self>) -> UsageVector {
        self.resolved()
    }

    fn estimated(&self) -> bool {
        !self.has_authoritative() && !self.fallback_tokens.is_empty()
    }
}

/// 从非流式 JSON 响应中抽取用量。整段缓冲后解析——单次响应体量有限。
#[derive(Debug)]
pub struct JsonUsageExtractor {
    spec: Arc<UsageSpec>,
    buf: Vec<u8>,
}

impl JsonUsageExtractor {
    #[must_use]
    pub fn new(spec: Arc<UsageSpec>) -> Self {
        Self {
            spec,
            buf: Vec::new(),
        }
    }

    fn parse(&self) -> UsageVector {
        serde_json::from_slice::<serde_json::Value>(&self.buf)
            .map(|doc| self.spec.evaluate(&doc))
            .unwrap_or_default()
    }
}

impl UsageExtractor for JsonUsageExtractor {
    fn feed(&mut self, chunk: &[u8]) {
        self.buf.extend_from_slice(chunk);
    }

    fn snapshot(&self) -> UsageVector {
        self.parse()
    }

    fn finish(self: Box<Self>) -> UsageVector {
        self.parse()
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
        let mut e = SseUsageExtractor::new(Arc::new(openai_spec()));
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
        let mut e = SseUsageExtractor::new(Arc::new(openai_spec()));
        assert!(e.snapshot().is_empty());

        e.feed(b"data: {\"usage\":{\"prompt_tokens\":10}}\n\n");
        assert_eq!(e.snapshot().get(dims::INPUT_TOKENS), 10);
        assert_eq!(e.snapshot().get(dims::OUTPUT_TOKENS), 0);
    }

    /// 上游可能发回非 JSON 的哨兵或错误文本，不得 panic
    #[test]
    fn ignores_non_json_payloads() {
        let mut e = SseUsageExtractor::new(Arc::new(openai_spec()));
        e.feed(b"data: [DONE]\n\ndata: <html>502</html>\n\n");
        assert!(e.snapshot().is_empty());
    }

    #[test]
    fn ignores_missing_fields() {
        let mut e = SseUsageExtractor::new(Arc::new(openai_spec()));
        e.feed(b"data: {\"usage\":{\"completion_tokens\":5}}\n\n");
        let u = e.snapshot();
        assert_eq!(u.get(dims::OUTPUT_TOKENS), 5);
        assert_eq!(u.iter().count(), 1);
    }

    /// Last：后到的累计值覆盖先前值
    #[test]
    fn last_mode_overwrites() {
        let mut e = SseUsageExtractor::new(Arc::new(openai_spec()));
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
        let mut e = SseUsageExtractor::new(Arc::new(spec));
        e.feed(b"data: {\"delta\":{\"tokens\":2}}\n\n");
        e.feed(b"data: {\"delta\":{\"tokens\":3}}\n\n");
        assert_eq!(e.snapshot().get(dims::OUTPUT_TOKENS), 5);
    }

    /// 帧被任意切分时抽取结果不变
    #[test]
    fn works_across_arbitrary_chunk_boundaries() {
        let payload = b"data: {\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5}}\n\n";
        let mut e = SseUsageExtractor::new(Arc::new(openai_spec()));
        for c in payload.chunks(1) {
            e.feed(c);
        }
        assert_eq!(e.snapshot().get(dims::INPUT_TOKENS), 10);
        assert_eq!(e.snapshot().get(dims::OUTPUT_TOKENS), 5);
    }

    /// 上游可能把整数写成 JSON 浮点，需接受但不得引入浮点误差
    #[test]
    fn accepts_integral_float() {
        let mut e = SseUsageExtractor::new(Arc::new(openai_spec()));
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
        let mut e = SseUsageExtractor::new(Arc::new(openai_spec()));
        e.feed(format!("data: {{\"usage\":{{\"prompt_tokens\":{raw}}}}}\n\n").as_bytes());
        assert!(e.snapshot().is_empty(), "{raw} 未被忽略");
    }

    // ------------------------------------------------- 第 3 档：tokenizer 兜底

    fn spec_with_fallback() -> UsageSpec {
        openai_spec()
            .text_fallback(
                dims::OUTPUT_TOKENS,
                "$.choices[*].delta.content",
                Tokenizer::O200kBase,
            )
            .unwrap()
    }

    /// 客户端中途断连，携带 usage 的末帧从未到达：按已生成文本估算
    #[test]
    fn estimates_output_tokens_when_usage_never_arrives() {
        let mut e = SseUsageExtractor::new(Arc::new(spec_with_fallback()));
        e.feed(b"data: {\"choices\":[{\"delta\":{\"content\":\" hello\"}}]}\n\n");
        e.feed(b"data: {\"choices\":[{\"delta\":{\"content\":\" world\"}}]}\n\n");

        assert_eq!(e.snapshot().get(dims::OUTPUT_TOKENS), 2);
        assert!(e.estimated(), "估算值未被标记");
    }

    /// 权威 usage 一旦到达即覆盖估算值，且不再标记为估算
    #[test]
    fn authoritative_usage_overrides_the_estimate() {
        let mut e = SseUsageExtractor::new(Arc::new(spec_with_fallback()));
        e.feed(b"data: {\"choices\":[{\"delta\":{\"content\":\" hello\"}}]}\n\n");
        e.feed(b"data: {\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":42}}\n\n");

        assert_eq!(e.snapshot().get(dims::OUTPUT_TOKENS), 42);
        assert!(!e.estimated());
    }

    /// 未配置兜底时保持原样：不估算、不标记
    #[test]
    fn without_fallback_a_truncated_stream_yields_nothing() {
        let mut e = SseUsageExtractor::new(Arc::new(openai_spec()));
        e.feed(b"data: {\"choices\":[{\"delta\":{\"content\":\" hello\"}}]}\n\n");

        assert!(e.snapshot().is_empty());
        assert!(!e.estimated());
    }

    /// 空流不产生估算
    #[test]
    fn empty_stream_estimates_nothing() {
        let e = SseUsageExtractor::new(Arc::new(spec_with_fallback()));
        assert!(e.snapshot().is_empty());
    }

    // ------------------------------------------------------- 非流式 JSON

    #[test]
    fn extracts_usage_from_a_json_response() {
        let mut e = JsonUsageExtractor::new(Arc::new(openai_spec()));
        e.feed(b"{\"usage\":{\"prompt_tokens\":10,");
        e.feed(b"\"completion_tokens\":5}}");

        let u = Box::new(e).finish();
        assert_eq!(u.get(dims::INPUT_TOKENS), 10);
        assert_eq!(u.get(dims::OUTPUT_TOKENS), 5);
    }

    /// 响应未收完即断连，JSON 不完整：抽不出用量但不得 panic
    #[test]
    fn truncated_json_yields_nothing() {
        let mut e = JsonUsageExtractor::new(Arc::new(openai_spec()));
        e.feed(b"{\"usage\":{\"prompt_tok");
        assert!(e.snapshot().is_empty());
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
