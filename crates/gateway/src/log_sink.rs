//! 请求日志落库。
//!
//! 与结算是**两条独立通道**：capture 关系到钱，要求可靠且及时；日志可以批量、
//! 可以延迟、极端情况可以丢。共用一个任务时，慢 sink 会拖累 capture。

use std::collections::BTreeMap;
use std::time::Duration;

use async_trait::async_trait;
use http::HeaderMap;
use serde_json::{Value, json};
use sqlx::PgPool;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::settlement::{RequestRecord, RequestStatus};

#[derive(Debug, thiserror::Error)]
pub enum SinkError {
    #[error("日志写入失败: {0}")]
    Db(#[from] sqlx::Error),
}

/// M0 只有 postgres 实现；M9 起可切 `ClickHouse` / `StarRocks`。
#[async_trait]
pub trait LogSink: Send + Sync {
    /// 批量写入。空批次是合法输入。
    ///
    /// # Errors
    /// 后端不可用或写入失败。
    async fn write(&self, batch: &[RequestRecord]) -> Result<(), SinkError>;
}

fn status_code(s: RequestStatus) -> i16 {
    match s {
        RequestStatus::Ok => 0,
        RequestStatus::Truncated => 1,
        RequestStatus::Failed => 2,
        RequestStatus::Rejected => 3,
    }
}

/// 头以 `BTreeMap` 落库，顺序稳定便于比对。同名多值合并为逗号分隔。
fn headers_json(h: &HeaderMap) -> Value {
    let mut map: BTreeMap<&str, String> = BTreeMap::new();
    for (name, value) in h {
        let text = value.to_str().unwrap_or("<binary>");
        map.entry(name.as_str())
            .and_modify(|v| {
                v.push_str(", ");
                v.push_str(text);
            })
            .or_insert_with(|| text.to_owned());
    }
    json!(map)
}

pub struct PgLogSink {
    pool: PgPool,
}

impl PgLogSink {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl LogSink for PgLogSink {
    async fn write(&self, batch: &[RequestRecord]) -> Result<(), SinkError> {
        if batch.is_empty() {
            return Ok(());
        }

        let rows: Vec<Value> = batch
            .iter()
            .map(|r| {
                json!({
                    "request_id": r.request_id.0,
                    "handle_id": r.handle_id.map(|h| h.0),
                    "key_id": r.key_id.map(|k| k.0),
                    "account_chain": r.account_chain.iter().map(|a| a.0).collect::<Vec<_>>(),
                    "channel_id": r.channel.0,
                    "model": r.model,
                    "endpoint": r.endpoint,
                    // SIMPLIFIED(M0): 形态固定为 Json/Sse + InRequest，不逐条落库
                    "shape": {},
                    "usage": r.usage,
                    "quote": {
                        "amount": r.amount.map(gw_core::Money::as_nanos),
                        "estimated": r.estimated,
                    },
                    "status": status_code(r.status),
                    "req_headers": headers_json(&r.req_headers),
                    "resp_headers": headers_json(&r.resp_headers),
                    "started_at": r.started_at,
                    "ended_at": r.ended_at,
                })
            })
            .collect();

        // 整批一条语句。account_chain 是每行不等长的数组，UNNEST 无法承载，
        // 故用 jsonb 展开。重复 request_id 直接忽略，写入重试不得整批失败。
        sqlx::query(
            r"INSERT INTO request_log
                 (request_id, key_id, account_chain, channel_id, model, endpoint,
                  shape, usage, quote, status, req_headers, resp_headers,
                  started_at, ended_at, handle_id)
               SELECT (r->>'request_id')::UUID,
                      (r->>'key_id')::BIGINT,
                      ARRAY(SELECT jsonb_array_elements_text(r->'account_chain')::BIGINT),
                      (r->>'channel_id')::BIGINT,
                      r->>'model',
                      r->>'endpoint',
                      r->'shape',
                      r->'usage',
                      r->'quote',
                      (r->>'status')::SMALLINT,
                      r->'req_headers',
                      r->'resp_headers',
                      (r->>'started_at')::TIMESTAMPTZ,
                      (r->>'ended_at')::TIMESTAMPTZ,
                      (r->>'handle_id')::UUID
                 FROM jsonb_array_elements($1::JSONB) AS r
               ON CONFLICT (request_id) DO NOTHING",
        )
        .bind(Value::Array(rows))
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}

/// 消费日志通道，按批量或超时触发写入。
///
/// 通道关闭后排空剩余记录再退出——优雅退出时不能把已结算的请求记录丢掉。
#[must_use]
pub fn spawn_log_writer(
    sink: Box<dyn LogSink>,
    mut rx: mpsc::Receiver<RequestRecord>,
    batch_size: usize,
    flush_interval: Duration,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut batch: Vec<RequestRecord> = Vec::with_capacity(batch_size);
        let mut ticker = tokio::time::interval(flush_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                received = rx.recv() => {
                    let Some(rec) = received else {
                        // 通道关闭：排空后退出
                        while let Ok(rec) = rx.try_recv() {
                            batch.push(rec);
                        }
                        flush(sink.as_ref(), &mut batch).await;
                        return;
                    };
                    batch.push(rec);
                    if batch.len() >= batch_size {
                        flush(sink.as_ref(), &mut batch).await;
                    }
                }
                _ = ticker.tick() => flush(sink.as_ref(), &mut batch).await,
            }
        }
    })
}

/// 写入失败只记录不重试：日志是旁路，重试会把内存越堆越大。
async fn flush(sink: &dyn LogSink, batch: &mut Vec<RequestRecord>) {
    if batch.is_empty() {
        return;
    }
    if let Err(e) = sink.write(batch).await {
        metrics::counter!("gateway.log_write_failed").increment(batch.len() as u64);
        tracing::error!(error = %e, count = batch.len(), "请求日志写入失败");
    }
    batch.clear();
}
