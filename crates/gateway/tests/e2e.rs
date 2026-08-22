//! M0 验收：一条真实链路，从准入到落账。

use std::net::SocketAddr;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use gw_gateway::LoadGuard;
use gw_gateway::admission::{LoadGuardConfig, LoadSample};
use gw_gateway::app::{AppState, GatewayConfig};
use gw_gateway::settlement::Settler;
use gw_ledger::PgCoordinator;
use gw_pricing::{EstimateCeilings, PgPriceEngine};
use gw_proxy::Upstream;
use sqlx::PgPool;
use tokio::sync::mpsc;

// ------------------------------------------------------------------ 模拟上游

const SSE_FRAMES: &[&str] = &[
    "data: {\"choices\":[{\"delta\":{\"content\":\"he\"}}]}\n\n",
    "data: {\"choices\":[{\"delta\":{\"content\":\"llo\"}}]}\n\n",
    "data: {\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5}}\n\n",
    "data: [DONE]\n\n",
];

async fn sse_handler() -> Response {
    let stream = futures::stream::unfold(0usize, |i| async move {
        if i >= SSE_FRAMES.len() {
            return None;
        }
        // 留出窗口，便于测试在流中途断连
        tokio::time::sleep(Duration::from_millis(60)).await;
        Some((
            Ok::<_, std::io::Error>(bytes::Bytes::from_static(SSE_FRAMES[i].as_bytes())),
            i + 1,
        ))
    });
    Response::builder()
        .header("content-type", "text/event-stream")
        .body(Body::from_stream(stream))
        .unwrap()
}

async fn json_handler() -> Response {
    (
        [("content-type", "application/json")],
        r#"{"usage":{"prompt_tokens":10,"completion_tokens":5}}"#,
    )
        .into_response()
}

async fn spawn_upstream() -> SocketAddr {
    let app = Router::new()
        .route("/v1/chat/completions", post(sse_handler))
        .route("/v1/nostream", post(json_handler));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    addr
}

// -------------------------------------------------------------------- 脚手架

/// M0 路由是「取第一个 enabled 渠道」（SIMPLIFIED(M0)），各测试的渠道会互相覆盖。
/// 这是共用一个库带来的脚手架问题，不是产品逻辑问题，串行执行即可。
static SERIAL: LazyLock<tokio::sync::Mutex<()>> = LazyLock::new(|| tokio::sync::Mutex::new(()));

const START_BALANCE: i64 = 1_000_000;
/// 单价：纳单位 per 百万用量单位。`1_000_000` 即 1 纳单位 / token
const UNIT_PRICE: i64 = 1_000_000;

struct Fixture {
    _serial: tokio::sync::MutexGuard<'static, ()>,
    base: String,
    pool: PgPool,
    load: Arc<LoadGuard>,
    settler: Arc<Settler>,
    account: i64,
    api_key: String,
    model: String,
    _keepalive: (
        mpsc::Receiver<gw_core::HoldId>,
        mpsc::Receiver<gw_gateway::settlement::RequestRecord>,
    ),
}

#[allow(clippy::too_many_lines)]
async fn setup(balance: i64, max_inflight: u32) -> Fixture {
    let serial = SERIAL.lock().await;
    let pool = gw_store::testkit::pool().await;
    let upstream_addr = spawn_upstream().await;
    let suffix = uuid::Uuid::new_v4().simple().to_string();

    // SIMPLIFIED(M0): Root + 单个节点 + 单个 Account
    let node_id: i64 = sqlx::query_scalar(
        "INSERT INTO org_node (uuid, path, kind, source, name)
         VALUES ($1, text2ltree($2), 0, 0, 'e2e') RETURNING id",
    )
    .bind(uuid::Uuid::new_v4())
    .bind(format!("n{suffix}"))
    .fetch_one(&pool)
    .await
    .unwrap();

    let account: i64 = sqlx::query_scalar("INSERT INTO account (node_id) VALUES ($1) RETURNING id")
        .bind(node_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO account_balance (account_id, shard, balance) VALUES ($1, 0, $2)")
        .bind(account)
        .bind(balance)
        .execute(&pool)
        .await
        .unwrap();

    let api_key = format!("sk-gw-{suffix}");
    sqlx::query(
        "INSERT INTO api_key (node_id, hash, prefix, account_chain) VALUES ($1, $2, $3, $4)",
    )
    .bind(node_id)
    .bind(&gw_gateway::key_hash(&api_key)[..])
    .bind(gw_gateway::key_prefix(&api_key))
    .bind(vec![account])
    .execute(&pool)
    .await
    .unwrap();

    // SIMPLIFIED(M0): 单渠道直连，无过滤与打分
    sqlx::query("UPDATE channel SET enabled = false")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO channel (provider, base_url, credential, enabled)
         VALUES ('mock', $1, $2, true)",
    )
    .bind(format!("http://{upstream_addr}"))
    .bind(b"Bearer upstream-secret".to_vec())
    .execute(&pool)
    .await
    .unwrap();

    let model = format!("m-{suffix}");
    for dim in ["input_tokens", "output_tokens"] {
        sqlx::query(
            "INSERT INTO price_rule
               (version, match_model, dim, unit_price, cost_price, effective_from)
             VALUES (1, $1, $2, $3, 0, now() - interval '1 day')",
        )
        .bind(&model)
        .bind(dim)
        .bind(UNIT_PRICE)
        .execute(&pool)
        .await
        .unwrap();
    }

    let (reclaim_tx, reclaim_rx) = mpsc::channel(64);
    let (log_tx, log_rx) = mpsc::channel(1024);

    let load = Arc::new(LoadGuard::new(LoadGuardConfig {
        max_inflight,
        enter_ratio: 0.90,
        exit_ratio: 0.80,
        window: 2,
        retry_after: Duration::from_secs(3),
    }));
    let coord = Arc::new(PgCoordinator::new(pool.clone(), reclaim_tx));
    let pricing = Arc::new(PgPriceEngine::new(
        pool.clone(),
        EstimateCeilings {
            input_tokens: 1_000,
            output_tokens: 1_000,
        },
    ));
    let settler = Arc::new(Settler::new(coord.clone(), pricing.clone(), log_tx));

    let state = Arc::new(AppState {
        pool: pool.clone(),
        load: Arc::clone(&load),
        coord,
        pricing,
        settler: Arc::clone(&settler),
        upstream: Upstream::new(),
        config: GatewayConfig::default(),
    });

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, gw_gateway::router(state))
            .await
            .unwrap();
    });

    Fixture {
        _serial: serial,
        base: format!("http://{addr}"),
        pool,
        load,
        settler,
        account,
        api_key,
        model,
        _keepalive: (reclaim_rx, log_rx),
    }
}

impl Fixture {
    fn request(&self, path: &str, stream: bool) -> reqwest::RequestBuilder {
        reqwest::Client::new()
            .post(format!("{}{path}", self.base))
            .bearer_auth(&self.api_key)
            .json(&serde_json::json!({
                "model": self.model,
                "stream": stream,
                "max_tokens": 100,
                "messages": [{"role": "user", "content": "hi"}],
            }))
    }

    async fn balances(&self) -> (i64, i64) {
        sqlx::query_as(
            "SELECT balance, held FROM account_balance WHERE account_id = $1 AND shard = 0",
        )
        .bind(self.account)
        .fetch_one(&self.pool)
        .await
        .unwrap()
    }

    /// 结算在独立任务中完成，轮询等待其落库
    async fn settled(&self) -> (i64, i64) {
        for _ in 0..100 {
            let b = self.balances().await;
            if b.1 == 0 && b.0 != START_BALANCE {
                return b;
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
        panic!("结算未在预期时间内完成：{:?}", self.balances().await)
    }
}

// ------------------------------------------------------------------ 验收项 1

/// 真实跑通一次带计费的流式请求，账本余额与用量对得上
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bills_a_streaming_request_end_to_end() {
    let fx = setup(START_BALANCE, 64).await;

    let resp = fx
        .request("/v1/chat/completions", true)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(body.contains("[DONE]"), "响应未透传完整");

    // prompt_tokens 10 + completion_tokens 5，各 1 纳单位
    let (balance, held) = fx.settled().await;
    assert_eq!(held, 0, "冻结未释放");
    assert_eq!(balance, START_BALANCE - 15);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bills_a_non_streaming_request() {
    let fx = setup(START_BALANCE, 64).await;

    let resp = fx.request("/v1/nostream", false).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    resp.text().await.unwrap();

    let (balance, held) = fx.settled().await;
    assert_eq!(held, 0);
    assert_eq!(balance, START_BALANCE - 15);
}

// ------------------------------------------------------------------ 验收项 2

/// 客户端中途断连时，按已生成部分正确结算
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn settles_by_generated_portion_when_client_disconnects() {
    use futures::StreamExt;

    let fx = setup(START_BALANCE, 64).await;

    let resp = fx
        .request("/v1/chat/completions", true)
        .send()
        .await
        .unwrap();
    let mut stream = resp.bytes_stream();
    let first = stream.next().await.unwrap().unwrap();
    assert!(String::from_utf8_lossy(&first).contains("he"));
    drop(stream); // 断连：携带 usage 的末帧从未到达

    let (balance, held) = fx.settled().await;
    let charged = START_BALANCE - balance;
    assert_eq!(held, 0, "断连后冻结未释放");
    assert!(charged > 0, "断连等于免费，用量兜底未生效");
    assert!(charged < 15, "断连却按完整用量收费：{charged}");
}

// ------------------------------------------------------------------ 验收项 4

/// 内存水位超阈值时新请求被拒，在途请求不受影响
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn overload_rejects_new_requests_without_affecting_inflight() {
    use futures::StreamExt;

    let fx = setup(START_BALANCE, 64).await;

    // 先发起一个在途流
    let resp = fx
        .request("/v1/chat/completions", true)
        .send()
        .await
        .unwrap();
    let mut inflight = resp.bytes_stream();
    inflight.next().await.unwrap().unwrap();

    // 水位越过进入阈值
    for _ in 0..2 {
        fx.load.observe(LoadSample {
            memory_ratio: 0.97,
            cpu_ratio: 0.0,
        });
    }

    let rejected = fx
        .request("/v1/chat/completions", true)
        .send()
        .await
        .unwrap();
    assert_eq!(rejected.status(), 503);
    assert_eq!(rejected.headers().get("retry-after").unwrap(), "3");
    let body: serde_json::Value = rejected.json().await.unwrap();
    assert_eq!(body["error"]["type"], "server_overloaded");
    assert!(
        body["error"]["message"].is_string(),
        "错误体不符合入站协议格式"
    );

    // 在途请求继续读完，不受影响
    let mut tail = Vec::new();
    while let Some(chunk) = inflight.next().await {
        tail.extend_from_slice(&chunk.unwrap());
    }
    assert!(
        String::from_utf8_lossy(&tail).contains("[DONE]"),
        "在途流被打断"
    );
}

/// 并发预算耗尽同样返回 503
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrency_budget_rejects_beyond_capacity() {
    use futures::StreamExt;

    let fx = setup(START_BALANCE, 1).await;

    let resp = fx
        .request("/v1/chat/completions", true)
        .send()
        .await
        .unwrap();
    let mut inflight = resp.bytes_stream();
    inflight.next().await.unwrap().unwrap();

    let rejected = fx
        .request("/v1/chat/completions", true)
        .send()
        .await
        .unwrap();
    assert_eq!(rejected.status(), 503);
}

// ------------------------------------------------------------------ 验收项 3

/// 优雅退出时结算任务全部排空，无漏账
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graceful_shutdown_drains_settlements() {
    let fx = setup(START_BALANCE, 64).await;

    let resp = fx
        .request("/v1/chat/completions", true)
        .send()
        .await
        .unwrap();
    resp.text().await.unwrap();

    fx.load.start_draining();
    fx.settler.tasks().close();
    fx.settler.tasks().wait().await;

    let (balance, held) = fx.balances().await;
    assert_eq!(held, 0);
    assert_eq!(balance, START_BALANCE - 15);

    // 退出后不再接受新请求
    let rejected = fx
        .request("/v1/chat/completions", true)
        .send()
        .await
        .unwrap();
    assert_eq!(rejected.status(), 503);
    let body: serde_json::Value = rejected.json().await.unwrap();
    assert_eq!(body["error"]["type"], "server_draining");
}

// ------------------------------------------------------------------ 拒绝路径

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insufficient_balance_is_rejected_before_forwarding() {
    let fx = setup(10, 64).await;

    let resp = fx
        .request("/v1/chat/completions", true)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 402);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["type"], "insufficient_quota");
    assert_eq!(fx.balances().await, (10, 0), "被拒请求不应留下冻结");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_api_key_is_rejected() {
    let fx = setup(START_BALANCE, 64).await;

    let resp = reqwest::Client::new()
        .post(format!("{}/v1/chat/completions", fx.base))
        .bearer_auth("sk-nonexistent")
        .json(&serde_json::json!({"model": fx.model, "stream": true}))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 401);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["type"], "invalid_api_key");
}

/// 无价格规则的模型必须在转发前拒绝，否则等于免费送
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn model_without_price_rule_is_rejected_before_forwarding() {
    let fx = setup(START_BALANCE, 64).await;

    let resp = reqwest::Client::new()
        .post(format!("{}/v1/chat/completions", fx.base))
        .bearer_auth(&fx.api_key)
        .json(&serde_json::json!({"model": "no-such-model", "stream": true}))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 400);
    assert_eq!(fx.balances().await, (START_BALANCE, 0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn healthz_is_always_ok() {
    let fx = setup(START_BALANCE, 64).await;
    let resp = reqwest::get(format!("{}/healthz", fx.base)).await.unwrap();
    assert_eq!(resp.status(), 200);
}

/// readyz 只反映结构性状态，不反映瞬时负载
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn readyz_ignores_transient_overload() {
    let fx = setup(START_BALANCE, 64).await;
    for _ in 0..2 {
        fx.load.observe(LoadSample {
            memory_ratio: 0.99,
            cpu_ratio: 0.99,
        });
    }
    let resp = reqwest::get(format!("{}/readyz", fx.base)).await.unwrap();
    assert_eq!(resp.status(), 200);
}
