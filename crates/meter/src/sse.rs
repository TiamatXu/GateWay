//! 推模式 SSE 解析器。
//!
//! 不用 `eventsource-stream`：它只提供拉模式的 `Stream` 适配器，而 tee 旁路要求
//! `feed(&[u8])` 与转发同步执行，套 `Stream` 需引入通道与任务，会在转发路径上
//! 增加 await 点。

/// 一个已派发的 SSE 事件，借用解析器内部缓冲，不额外分配。
#[derive(Debug)]
pub struct SseEvent<'a> {
    pub name: Option<&'a str>,
    pub data: &'a str,
}

/// 增量 SSE 解析器。字节可在任意位置切分。
#[derive(Debug, Default)]
pub struct SseParser {
    /// 尚未构成完整行的字节
    buf: Vec<u8>,
    /// 当前事件已累积的 data
    data: String,
    /// 当前事件的 event 字段
    name: Option<String>,
    has_data: bool,
    /// 上一行以 \r 结束，若下一字节是 \n 则属同一行尾
    pending_lf: bool,
}

impl SseParser {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// 推入一段字节，对本次推入后完成的每个事件调用一次 `on_event`。
    ///
    /// 纯状态机推进，不做 IO、不阻塞。
    pub fn feed(&mut self, chunk: &[u8], mut on_event: impl FnMut(SseEvent<'_>)) {
        // 取走缓冲以获得局部所有权，避免与 self 其余字段的借用冲突
        let mut buf = core::mem::take(&mut self.buf);
        buf.extend_from_slice(chunk);

        let mut pos = 0;
        while pos < buf.len() {
            let b = buf[pos];
            if self.pending_lf {
                self.pending_lf = false;
                if b == b'\n' {
                    // \r\n 的后半，该行已在 \r 处结束
                    pos += 1;
                    continue;
                }
            }
            match memchr2(b'\n', b'\r', &buf[pos..]) {
                Some(rel) => {
                    let end = pos + rel;
                    let line = &buf[pos..end];
                    if buf[end] == b'\r' {
                        self.pending_lf = true;
                    }
                    process_line(
                        line,
                        &mut self.data,
                        &mut self.name,
                        &mut self.has_data,
                        &mut on_event,
                    );
                    pos = end + 1;
                }
                None => break,
            }
        }

        buf.drain(..pos);
        self.buf = buf;
    }
}

fn memchr2(a: u8, b: u8, haystack: &[u8]) -> Option<usize> {
    haystack.iter().position(|&c| c == a || c == b)
}

fn process_line(
    line: &[u8],
    data: &mut String,
    name: &mut Option<String>,
    has_data: &mut bool,
    on_event: &mut impl FnMut(SseEvent<'_>),
) {
    if line.is_empty() {
        // 空行：派发事件
        if *has_data {
            on_event(SseEvent {
                name: name.as_deref(),
                data,
            });
        }
        data.clear();
        *name = None;
        *has_data = false;
        return;
    }
    if line[0] == b':' {
        return; // 注释
    }

    let (field, mut value) = match line.iter().position(|&c| c == b':') {
        Some(i) => (&line[..i], &line[i + 1..]),
        None => (line, &line[line.len()..]),
    };
    // 冒号后至多去掉一个前导空格
    if value.first() == Some(&b' ') {
        value = &value[1..];
    }

    match field {
        b"data" => {
            if *has_data {
                data.push('\n');
            }
            data.push_str(&String::from_utf8_lossy(value));
            *has_data = true;
        }
        b"event" => *name = Some(String::from_utf8_lossy(value).into_owned()),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    /// 按给定切分方式喂入，收集解析出的事件
    fn parse(chunks: &[&[u8]]) -> Vec<(Option<String>, String)> {
        let mut p = SseParser::new();
        let mut out = Vec::new();
        for c in chunks {
            p.feed(c, |ev| {
                out.push((ev.name.map(str::to_owned), ev.data.to_owned()));
            });
        }
        out
    }

    proptest::proptest! {
        /// 任意切分方式下，解析结果必须与整体喂入一致
        #[test]
        fn any_chunk_split_yields_same_events(cuts in proptest::collection::vec(0usize..80, 0..12)) {
            let payload: &[u8] =
                b": c\nevent: delta\ndata: {\"a\":1}\ndata: x\n\nid: 7\r\ndata: [DONE]\r\n\r\n";
            let mut bounds: Vec<usize> = cuts.into_iter().map(|c| c.min(payload.len())).collect();
            bounds.sort_unstable();

            let mut chunks: Vec<&[u8]> = Vec::new();
            let mut prev = 0;
            for b in bounds {
                chunks.push(&payload[prev..b]);
                prev = b;
            }
            chunks.push(&payload[prev..]);

            proptest::prop_assert_eq!(parse(&chunks), parse(&[payload]));
        }
    }

    #[test]
    fn parses_a_single_event() {
        assert_eq!(
            parse(&[b"data: hello\n\n"]),
            vec![(None, "hello".to_string())]
        );
    }

    #[test]
    fn strips_exactly_one_leading_space_after_colon() {
        assert_eq!(parse(&[b"data:hello\n\n"]), vec![(None, "hello".into())]);
        assert_eq!(parse(&[b"data:  hello\n\n"]), vec![(None, " hello".into())]);
    }

    #[test]
    fn joins_multiple_data_lines_with_newline() {
        assert_eq!(
            parse(&[b"data: a\ndata: b\n\n"]),
            vec![(None, "a\nb".to_string())]
        );
    }

    #[test]
    fn captures_event_name() {
        assert_eq!(
            parse(&[b"event: ping\ndata: {}\n\n"]),
            vec![(Some("ping".into()), "{}".into())]
        );
    }

    #[test]
    fn ignores_comment_lines() {
        assert_eq!(
            parse(&[b": keep-alive\ndata: x\n\n"]),
            vec![(None, "x".to_string())]
        );
    }

    #[test]
    fn ignores_unknown_fields() {
        assert_eq!(
            parse(&[b"id: 1\nretry: 500\nfoo: bar\ndata: x\n\n"]),
            vec![(None, "x".to_string())]
        );
    }

    /// 空事件（无 data 字段）不派发
    #[test]
    fn does_not_dispatch_event_without_data() {
        assert!(parse(&[b"event: ping\n\n"]).is_empty());
    }

    /// 未以空行结束的事件按 SSE 规范丢弃
    #[test]
    fn discards_trailing_incomplete_event() {
        assert!(parse(&[b"data: partial\n"]).is_empty());
    }

    #[rstest]
    #[case::lf(b"data: a\n\ndata: b\n\n")]
    #[case::crlf(b"data: a\r\n\r\ndata: b\r\n\r\n")]
    #[case::cr(b"data: a\r\rdata: b\r\r")]
    fn accepts_all_three_line_terminators(#[case] input: &[u8]) {
        assert_eq!(
            parse(&[input]),
            vec![(None, "a".to_string()), (None, "b".to_string())]
        );
    }

    /// 真实网络下 SSE 帧被任意切分：逐字节喂入必须与整体喂入结果一致
    #[test]
    fn byte_by_byte_matches_whole_chunk() {
        let payload: &[u8] =
            b": ping\nevent: delta\ndata: {\"a\":1}\ndata: {\"b\":2}\n\nid: 7\ndata: [DONE]\n\n";
        let whole = parse(&[payload]);
        let split: Vec<&[u8]> = payload.chunks(1).collect();
        assert_eq!(parse(&split), whole);
        assert_eq!(whole.len(), 2);
    }

    /// \r\n 恰好被切在中间时不得当作两个行尾
    #[test]
    fn crlf_split_across_chunks_is_one_terminator() {
        assert_eq!(
            parse(&[b"data: a\r", b"\n\r", b"\n"]),
            vec![(None, "a".to_string())]
        );
    }

    /// 多字节字符被切开时不得产生替换字符
    #[test]
    fn multibyte_char_split_across_chunks_is_preserved() {
        let payload = "data: 你好\n\n".as_bytes();
        let (a, b) = payload.split_at(8); // 切在「你」的 UTF-8 序列中间
        assert_eq!(parse(&[a, b]), vec![(None, "你好".to_string())]);
    }
}
