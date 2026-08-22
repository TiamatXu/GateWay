//! tee 旁路：边转发边抽取用量。
//!
//! 两个消费者的可靠性要求不同——**归档失败绝不影响 usage 抽取，也绝不影响请求
//! 本身**。账单必须无条件抽出，归档是可开关的旁路。

use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use bytes::Bytes;
use futures_core::Stream;
use gw_core::{UsageExtractor, UsageVector};
use pin_project_lite::pin_project;

#[derive(Debug, thiserror::Error)]
pub enum ArchiveError {
    #[error("归档后端错误: {0}")]
    Backend(String),
}

/// 请求体 / 响应体归档。M0 只定义接口，M6 实现。
pub trait ArchiveSink: Send {
    /// # Errors
    /// 后端不可用或写入失败。失败后本次请求不再投喂。
    fn write(&mut self, chunk: &[u8]) -> Result<(), ArchiveError>;
}

/// 一条响应流的两个消费者。
pub struct Tee {
    usage: Box<dyn UsageExtractor>,
    archive: Option<Box<dyn ArchiveSink>>,
    archive_failed: bool,
}

impl Tee {
    #[must_use]
    pub fn new(usage: Box<dyn UsageExtractor>) -> Self {
        Self {
            usage,
            archive: None,
            archive_failed: false,
        }
    }

    #[must_use]
    pub fn with_archive(mut self, sink: Box<dyn ArchiveSink>) -> Self {
        self.archive = Some(sink);
        self
    }

    /// 推进两个消费者。usage 先行，且不受归档结果影响。
    pub fn feed(&mut self, chunk: &[u8]) {
        self.usage.feed(chunk);

        if self.archive_failed {
            return;
        }
        if let Some(sink) = self.archive.as_mut()
            && let Err(e) = sink.write(chunk)
        {
            // 一次失败即停止投喂：逐分片重试会拖慢转发，而归档本就是旁路
            self.archive_failed = true;
            metrics::counter!("proxy.archive_failed").increment(1);
            tracing::warn!(error = %e, "归档写入失败，本次请求停止归档");
        }
    }

    #[must_use]
    pub fn snapshot(&self) -> UsageVector {
        self.usage.snapshot()
    }

    #[must_use]
    pub fn archive_failed(&self) -> bool {
        self.archive_failed
    }
}

/// tee 由转发路径与结算方共享。结算方只在流被丢弃后取一次快照，
/// 与转发路径不存在锁竞争。
pub type SharedTee = Arc<Mutex<Tee>>;

pin_project! {
    /// 转发上游字节流，同步推进 tee。
    ///
    /// `guard` 的析构即结算触发点：无论正常结束、客户端断连还是上游报错，
    /// 流被丢弃时都会走到。
    pub struct TeeStream<S, G> {
        #[pin]
        inner: S,
        tee: SharedTee,
        _guard: G,
    }
}

impl<S, G> TeeStream<S, G> {
    pub fn new(inner: S, tee: SharedTee, guard: G) -> Self {
        Self {
            inner,
            tee,
            _guard: guard,
        }
    }
}

impl<S, G, E> Stream for TeeStream<S, G>
where
    S: Stream<Item = Result<Bytes, E>>,
{
    type Item = Result<Bytes, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let next = this.inner.poll_next(cx);
        if let Poll::Ready(Some(Ok(chunk))) = &next {
            // 同步推进，不引入额外 await 点。锁在稳态下无竞争。
            if let Ok(mut tee) = this.tee.lock() {
                tee.feed(chunk);
            }
        }
        next
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use bytes::Bytes;
    use futures::StreamExt;
    use gw_core::{UsageExtractor, UsageVector};

    use super::*;

    // -------------------------------------------------------------- 测试替身

    #[derive(Default)]
    struct CountingExtractor {
        chunks: Arc<AtomicUsize>,
        bytes: usize,
    }

    impl UsageExtractor for CountingExtractor {
        fn feed(&mut self, chunk: &[u8]) {
            self.chunks.fetch_add(1, Ordering::Relaxed);
            self.bytes += chunk.len();
        }
        fn snapshot(&self) -> UsageVector {
            let mut u = UsageVector::new();
            u.set("bytes", i64::try_from(self.bytes).unwrap());
            u
        }
        fn finish(self: Box<Self>) -> UsageVector {
            self.snapshot()
        }
    }

    struct FailingSink {
        calls: Arc<AtomicUsize>,
    }

    impl ArchiveSink for FailingSink {
        fn write(&mut self, _chunk: &[u8]) -> Result<(), ArchiveError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Err(ArchiveError::Backend("对象存储不可用".into()))
        }
    }

    #[derive(Default)]
    struct RecordingSink {
        bytes: Arc<AtomicUsize>,
    }

    impl ArchiveSink for RecordingSink {
        fn write(&mut self, chunk: &[u8]) -> Result<(), ArchiveError> {
            self.bytes.fetch_add(chunk.len(), Ordering::Relaxed);
            Ok(())
        }
    }

    /// 析构即触发的结算哨兵
    struct Sentinel(Arc<AtomicUsize>);

    impl Drop for Sentinel {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn tee(extractor: CountingExtractor) -> Tee {
        Tee::new(Box::new(extractor))
    }

    // ------------------------------------------------------------------ Tee

    #[test]
    fn feeds_usage_extractor() {
        let chunks = Arc::new(AtomicUsize::new(0));
        let mut t = tee(CountingExtractor {
            chunks: Arc::clone(&chunks),
            bytes: 0,
        });

        t.feed(b"abc");
        t.feed(b"de");

        assert_eq!(chunks.load(Ordering::Relaxed), 2);
        assert_eq!(t.snapshot().get("bytes"), 5);
    }

    #[test]
    fn feeds_archive_sink() {
        let seen = Arc::new(AtomicUsize::new(0));
        let mut t = tee(CountingExtractor::default()).with_archive(Box::new(RecordingSink {
            bytes: Arc::clone(&seen),
        }));

        t.feed(b"abcde");

        assert_eq!(seen.load(Ordering::Relaxed), 5);
    }

    /// 账单必须无条件抽出，归档只是可开关的旁路
    #[test]
    fn archive_failure_does_not_affect_usage() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut t = tee(CountingExtractor::default()).with_archive(Box::new(FailingSink {
            calls: Arc::clone(&calls),
        }));

        t.feed(b"abc");
        t.feed(b"de");

        assert_eq!(t.snapshot().get("bytes"), 5, "归档失败影响了用量抽取");
        assert!(t.archive_failed());
    }

    /// 归档一旦失败即停止投喂，避免每个分片都重试拖慢转发
    #[test]
    fn archive_stops_after_first_failure() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut t = tee(CountingExtractor::default()).with_archive(Box::new(FailingSink {
            calls: Arc::clone(&calls),
        }));

        for _ in 0..5 {
            t.feed(b"x");
        }

        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    // ----------------------------------------------------------- TeeStream

    fn ok_stream(parts: &[&'static str]) -> impl futures::Stream<Item = Result<Bytes, String>> {
        futures::stream::iter(
            parts
                .iter()
                .map(|s| Ok(Bytes::from_static(s.as_bytes())))
                .collect::<Vec<_>>(),
        )
    }

    #[tokio::test]
    async fn forwards_chunks_unchanged() {
        let settled = Arc::new(AtomicUsize::new(0));
        let s = TeeStream::new(
            ok_stream(&["ab", "cd"]),
            Arc::new(std::sync::Mutex::new(tee(CountingExtractor::default()))),
            Sentinel(Arc::clone(&settled)),
        );

        let out: Vec<Bytes> = s.map(Result::unwrap).collect().await;

        assert_eq!(
            out,
            vec![Bytes::from_static(b"ab"), Bytes::from_static(b"cd")]
        );
    }

    #[tokio::test]
    async fn feeds_every_chunk_to_the_extractor() {
        let chunks = Arc::new(AtomicUsize::new(0));
        let s = TeeStream::new(
            ok_stream(&["ab", "cd", "ef"]),
            Arc::new(std::sync::Mutex::new(tee(CountingExtractor {
                chunks: Arc::clone(&chunks),
                bytes: 0,
            }))),
            Sentinel(Arc::new(AtomicUsize::new(0))),
        );

        let _: Vec<_> = s.collect().await;

        assert_eq!(chunks.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn settles_after_normal_completion() {
        let settled = Arc::new(AtomicUsize::new(0));
        let s = TeeStream::new(
            ok_stream(&["ab"]),
            Arc::new(std::sync::Mutex::new(tee(CountingExtractor::default()))),
            Sentinel(Arc::clone(&settled)),
        );

        let _: Vec<_> = s.collect().await;

        assert_eq!(settled.load(Ordering::Relaxed), 1);
    }

    /// 客户端中途断连：axum 丢弃 response body，结算按已抽取部分执行
    #[tokio::test]
    async fn settles_when_dropped_midway() {
        let settled = Arc::new(AtomicUsize::new(0));
        let mut s = Box::pin(TeeStream::new(
            ok_stream(&["ab", "cd", "ef"]),
            Arc::new(std::sync::Mutex::new(tee(CountingExtractor::default()))),
            Sentinel(Arc::clone(&settled)),
        ));

        let first = s.next().await.unwrap().unwrap();
        assert_eq!(first, Bytes::from_static(b"ab"));
        assert_eq!(settled.load(Ordering::Relaxed), 0, "流未结束却已结算");

        drop(s);

        assert_eq!(settled.load(Ordering::Relaxed), 1);
    }

    /// 上游报错同样要结算已生成的部分
    #[tokio::test]
    async fn settles_after_upstream_error() {
        let settled = Arc::new(AtomicUsize::new(0));
        let inner = futures::stream::iter(vec![
            Ok(Bytes::from_static(b"ab")),
            Err("上游断开".to_string()),
        ]);
        let s = TeeStream::new(
            inner,
            Arc::new(std::sync::Mutex::new(tee(CountingExtractor::default()))),
            Sentinel(Arc::clone(&settled)),
        );

        let out: Vec<Result<Bytes, String>> = s.collect().await;

        assert!(out[1].is_err());
        assert_eq!(settled.load(Ordering::Relaxed), 1);
    }

    /// 结算方必须能在流被丢弃后读到已抽取的用量
    #[tokio::test]
    async fn settlement_reads_usage_after_stream_is_dropped() {
        struct Settler {
            tee: SharedTee,
            seen: Arc<AtomicUsize>,
        }
        impl Drop for Settler {
            fn drop(&mut self) {
                let usage = self.tee.lock().unwrap().snapshot();
                self.seen.store(
                    usize::try_from(usage.get("bytes")).unwrap(),
                    Ordering::Relaxed,
                );
            }
        }

        let shared: SharedTee = Arc::new(std::sync::Mutex::new(tee(CountingExtractor::default())));
        let seen = Arc::new(AtomicUsize::new(0));
        let mut s = Box::pin(TeeStream::new(
            ok_stream(&["ab", "cd", "ef"]),
            Arc::clone(&shared),
            Settler {
                tee: Arc::clone(&shared),
                seen: Arc::clone(&seen),
            },
        ));

        s.next().await;
        s.next().await;
        drop(s);

        assert_eq!(seen.load(Ordering::Relaxed), 4, "结算未按已生成部分计量");
    }

    /// 用真实的 SSE 抽取器跑一遍：断连时账单按已生成部分算
    #[tokio::test]
    async fn partial_sse_stream_yields_partial_usage() {
        use gw_meter::{Accum, SseUsageExtractor, UsageSpec};

        let spec = UsageSpec::new()
            .rule("output_tokens", "$.usage.completion_tokens", Accum::Last)
            .unwrap();
        let mut t = Tee::new(Box::new(SseUsageExtractor::new(spec)));

        t.feed(b"data: {\"usage\":{\"completion_tokens\":7}}\n\n");
        t.feed(b"data: {\"usage\":{\"completion_toke");

        assert_eq!(t.snapshot().get("output_tokens"), 7);
    }
}
