//! 日志落库。与结算是两条独立通道：capture 关系到钱，日志可以延迟、可以丢。

use std::time::Duration;

use chrono::Utc;
use gw_core::{AccountId, ApiKeyId, ChannelId, Money, RequestId, UsageVector, dims};
use gw_gateway::log_sink::{LogSink, PgLogSink, spawn_log_writer};
use gw_gateway::settlement::{RequestRecord, RequestStatus};
use http::{HeaderMap, HeaderValue};
use sqlx::PgPool;
use tokio::sync::mpsc;

fn record(id: RequestId) -> RequestRecord {
    let mut usage = UsageVector::new();
    usage.set(dims::INPUT_TOKENS, 10);
    usage.set(dims::OUTPUT_TOKENS, 5);

    let mut req_headers = HeaderMap::new();
    req_headers.insert("content-type", HeaderValue::from_static("application/json"));

    RequestRecord {
        handle_id: None,
        request_id: id,
        key_id: Some(ApiKeyId(1)),
        account_chain: smallvec::smallvec![AccountId(7)],
        model: "gpt-4o".into(),
        channel: ChannelId(1),
        endpoint: "/v1/chat/completions".into(),
        usage,
        amount: Some(Money::from_nanos(15)),
        status: RequestStatus::Ok,
        estimated: false,
        req_headers,
        resp_headers: HeaderMap::new(),
        started_at: Utc::now(),
        ended_at: Utc::now(),
    }
}

async fn count(pool: &PgPool, id: RequestId) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM request_log WHERE request_id = $1")
        .bind(id.0)
        .fetch_one(pool)
        .await
        .unwrap()
}

#[tokio::test]
async fn writes_a_record() {
    let pool = gw_store::testkit::pool().await;
    let sink = PgLogSink::new(pool.clone());
    let id = RequestId(uuid::Uuid::new_v4());

    sink.write(&[record(id)]).await.unwrap();

    let row: (String, i16, i64, serde_json::Value) = sqlx::query_as(
        "SELECT model, status, (quote->>'amount')::BIGINT, usage
           FROM request_log WHERE request_id = $1",
    )
    .bind(id.0)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.0, "gpt-4o");
    assert_eq!(row.1, 0);
    assert_eq!(row.2, 15);
    assert_eq!(row.3["input_tokens"], 10);
}

#[tokio::test]
async fn writes_a_batch_in_one_call() {
    let pool = gw_store::testkit::pool().await;
    let sink = PgLogSink::new(pool.clone());
    let ids: Vec<RequestId> = (0..5).map(|_| RequestId(uuid::Uuid::new_v4())).collect();

    let batch: Vec<RequestRecord> = ids.iter().map(|i| record(*i)).collect();
    sink.write(&batch).await.unwrap();

    for id in ids {
        assert_eq!(count(&pool, id).await, 1);
    }
}

/// 写入重试不得因主键冲突整批失败
#[tokio::test]
async fn duplicate_request_id_is_ignored() {
    let pool = gw_store::testkit::pool().await;
    let sink = PgLogSink::new(pool.clone());
    let id = RequestId(uuid::Uuid::new_v4());

    sink.write(&[record(id)]).await.unwrap();
    sink.write(&[record(id)]).await.unwrap();

    assert_eq!(count(&pool, id).await, 1);
}

#[tokio::test]
async fn empty_batch_is_a_no_op() {
    let pool = gw_store::testkit::pool().await;
    PgLogSink::new(pool).write(&[]).await.unwrap();
}

/// 写入任务在通道关闭后排空剩余记录再退出
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn writer_drains_the_channel_before_exiting() {
    let pool = gw_store::testkit::pool().await;
    let (tx, rx) = mpsc::channel(64);
    let ids: Vec<RequestId> = (0..10).map(|_| RequestId(uuid::Uuid::new_v4())).collect();

    let handle = spawn_log_writer(
        Box::new(PgLogSink::new(pool.clone())),
        rx,
        8,
        Duration::from_millis(20),
    );
    for id in &ids {
        tx.send(record(*id)).await.unwrap();
    }
    drop(tx);
    handle.await.unwrap();

    for id in ids {
        assert_eq!(count(&pool, id).await, 1, "关闭时丢了记录");
    }
}
