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

/// 回显收到的路径与头，用于验证转发行为完全由描述文件决定
async fn echo_handler(uri: axum::http::Uri, headers: axum::http::HeaderMap) -> Response {
    let h: serde_json::Map<String, serde_json::Value> = headers
        .iter()
        .map(|(k, v)| {
            (
                k.as_str().to_owned(),
                serde_json::Value::String(v.to_str().unwrap_or_default().to_owned()),
            )
        })
        .collect();
    axum::Json(serde_json::json!({ "path": uri.path(), "headers": h })).into_response()
}

async fn spawn_upstream() -> SocketAddr {
    let app = Router::new()
        .route("/v1/chat/completions", post(sse_handler))
        .route("/v1/embeddings", post(json_handler))
        // Anthropic 原生端点：入站是 /anthropic/v1/messages，上游是 /v1/messages
        .route("/v1/messages", post(echo_handler));
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
    initial_balance: i64,
    base: String,
    pool: PgPool,
    load: Arc<LoadGuard>,
    settler: Arc<Settler>,
    account: i64,
    api_key: String,
    model: String,
    upstream_base: String,
    _keepalive: mpsc::Receiver<gw_core::HoldId>,
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
    // provider 用 openai：端点行为全部来自 providers/openai.yaml，
    // 渠道的 base_url 把上游指到本地 mock。凭据是裸值，
    // "Bearer " 前缀由描述文件的注入模板加。
    sqlx::query(
        "INSERT INTO channel (provider, base_url, credential, enabled)
         VALUES ('openai', $1, $2, true)",
    )
    .bind(format!("http://{upstream_addr}"))
    .bind(b"upstream-secret".to_vec())
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
    drop(gw_gateway::spawn_log_writer(
        Box::new(gw_gateway::PgLogSink::new(pool.clone())),
        log_rx,
        16,
        Duration::from_millis(20),
    ));

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
        redactor: gw_core::HeaderRedactor::default(),
        load: Arc::clone(&load),
        coord,
        pricing,
        settler: Arc::clone(&settler),
        upstream: Upstream::new(),
        endpoints: Arc::new(
            gw_gateway::Endpoints::open(
                concat!(env!("CARGO_MANIFEST_DIR"), "/../../providers"),
                gw_registry::HookRegistry::new(),
            )
            .expect("内置描述文件应当可加载"),
        ),
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
        initial_balance: balance,
        base: format!("http://{addr}"),
        pool,
        load,
        settler,
        account,
        api_key,
        model,
        upstream_base: format!("http://{upstream_addr}"),
        _keepalive: reclaim_rx,
    }
}

impl Fixture {
    /// 追加一个渠道。入站路径由描述文件决定走哪个 provider。
    async fn add_channel(&self, provider: &str, credential: &[u8]) {
        sqlx::query(
            "INSERT INTO channel (provider, base_url, credential, enabled)
             VALUES ($1, $2, $3, true)",
        )
        .bind(provider)
        .bind(&self.upstream_base)
        .bind(credential.to_vec())
        .execute(&self.pool)
        .await
        .unwrap();
    }

    /// 在同一 fixture 内再建一把 API Key（各自独立账户）。
    /// 不能再调 setup()——它持有串行锁，重复申请会自锁。
    async fn add_api_key(&self, balance: i64) -> String {
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let node_id: i64 = sqlx::query_scalar(
            "INSERT INTO org_node (uuid, path, kind, source, name)
             VALUES ($1, text2ltree($2), 0, 0, 'e2e') RETURNING id",
        )
        .bind(uuid::Uuid::new_v4())
        .bind(format!("n{suffix}"))
        .fetch_one(&self.pool)
        .await
        .unwrap();
        let account: i64 =
            sqlx::query_scalar("INSERT INTO account (node_id) VALUES ($1) RETURNING id")
                .bind(node_id)
                .fetch_one(&self.pool)
                .await
                .unwrap();
        sqlx::query("INSERT INTO account_balance (account_id, shard, balance) VALUES ($1, 0, $2)")
            .bind(account)
            .bind(balance)
            .execute(&self.pool)
            .await
            .unwrap();
        let key = format!("sk-gw-{suffix}");
        sqlx::query(
            "INSERT INTO api_key (node_id, hash, prefix, account_chain) VALUES ($1, $2, $3, $4)",
        )
        .bind(node_id)
        .bind(&gw_gateway::key_hash(&key)[..])
        .bind(gw_gateway::key_prefix(&key))
        .bind(vec![account])
        .execute(&self.pool)
        .await
        .unwrap();
        key
    }

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
        let initial = self.initial_balance;
        for _ in 0..100 {
            let b = self.balances().await;
            if b.1 == 0 && b.0 != initial {
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

    // embeddings 是描述文件里声明的非流式端点（response: json）
    let resp = fx.request("/v1/embeddings", false).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    resp.text().await.unwrap();

    // 上游同时返回了 prompt_tokens 10 与 completion_tokens 5，但 embeddings 端点
    // 的描述文件只声明了 input_tokens 一个维度——计什么由声明决定，不由响应决定
    let (balance, held) = fx.settled().await;
    assert_eq!(held, 0);
    assert_eq!(balance, START_BALANCE - 10);
}

/// 账单落库，且密钥类头绝不出现在日志中
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writes_a_billing_record_with_redacted_headers() {
    let fx = setup(START_BALANCE, 64).await;

    let resp = fx
        .request("/v1/chat/completions", true)
        .send()
        .await
        .unwrap();
    resp.text().await.unwrap();
    fx.settled().await;

    let row = loop {
        let row: Option<(String, serde_json::Value, serde_json::Value, i16)> = sqlx::query_as(
            "SELECT model, usage, req_headers, status FROM request_log
              WHERE model = $1 ORDER BY started_at DESC LIMIT 1",
        )
        .bind(&fx.model)
        .fetch_optional(&fx.pool)
        .await
        .unwrap();
        if let Some(r) = row {
            break r;
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    };

    assert_eq!(row.1["input_tokens"], 10);
    assert_eq!(row.1["output_tokens"], 5);
    assert_eq!(row.3, 0);

    let headers = row.2.as_object().unwrap();
    assert!(
        !headers.contains_key("authorization"),
        "密钥类头出现在了请求日志中：{headers:?}"
    );
    assert!(headers.contains_key("content-type"), "普通头被误删");
    assert!(
        !row.2.to_string().contains(&fx.api_key),
        "API Key 明文出现在了请求日志中"
    );
}

/// 预扣按真实输入量估算：小额余额也要能发出小请求
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn holds_only_what_the_request_actually_needs() {
    use futures::StreamExt;

    // 余额远小于「输入按 1000 token 上限」所需的预扣
    let fx = setup(300, 64).await;

    let resp = fx
        .request("/v1/chat/completions", true)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "预扣过粗，小请求被误拒");

    // 在途时读一下冻结额：输入是极短的 "hi"，冻结应远小于上限
    let mut stream = resp.bytes_stream();
    stream.next().await.unwrap().unwrap();
    let (_, held) = fx.balances().await;
    assert!(held > 0, "未冻结");
    assert!(held < 200, "冻结额仍按上限估算：{held}");

    drop(stream);
    fx.settled().await;
}

/// 同一 Idempotency-Key 的重放不得二次扣费
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replaying_an_idempotency_key_does_not_charge_twice() {
    let fx = setup(START_BALANCE, 64).await;

    let first = fx
        .request("/v1/chat/completions", true)
        .header("idempotency-key", "abc-123")
        .send()
        .await
        .unwrap();
    assert_eq!(first.status(), 200);
    first.text().await.unwrap();
    let after_first = fx.settled().await;

    let replay = fx
        .request("/v1/chat/completions", true)
        .header("idempotency-key", "abc-123")
        .send()
        .await
        .unwrap();

    assert_eq!(replay.status(), 409);
    let body: serde_json::Value = replay.json().await.unwrap();
    assert_eq!(body["error"]["type"], "duplicate_request");
    assert_eq!(fx.balances().await, after_first, "重放二次扣费了");
}

/// 不同 Key 用同一个 Idempotency-Key 互不影响
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idempotency_keys_are_scoped_per_api_key() {
    let fx = setup(START_BALANCE, 64).await;

    let first = fx
        .request("/v1/chat/completions", true)
        .header("idempotency-key", "shared-key")
        .send()
        .await
        .unwrap();
    first.text().await.unwrap();
    fx.settled().await;

    // 另一把 Key 用同样的 Idempotency-Key
    let other_key = fx.add_api_key(START_BALANCE).await;
    let resp = reqwest::Client::new()
        .post(format!("{}/v1/chat/completions", fx.base))
        .bearer_auth(&other_key)
        .header("idempotency-key", "shared-key")
        .json(&serde_json::json!({
            "model": fx.model, "stream": true, "max_tokens": 100,
            "messages": [{"role": "user", "content": "hi"}],
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200, "幂等键跨租户串了");
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

/// 主干零厂商分支：同一份代码，两个 provider 的鉴权方式、上游路径与协议
/// 全部只由各自的描述文件决定。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn descriptors_alone_decide_auth_and_path_rewriting() {
    let fx = setup(START_BALANCE, 64).await;
    fx.add_channel("anthropic", b"sk-ant-secret").await;

    let resp = reqwest::Client::new()
        .post(format!("{}/anthropic/v1/messages", fx.base))
        .bearer_auth(&fx.api_key)
        .json(&serde_json::json!({
            "model": fx.model,
            "messages": [{"role": "user", "content": "hi"}],
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let echoed: serde_json::Value = resp.json().await.unwrap();

    // 上游路径由 route.upstream 重写，与入站路径不同
    assert_eq!(echoed["path"], "/v1/messages");

    // 凭据注入到 x-api-key 且是裸值——描述文件里写的模板是 "{credential}"，
    // 不是 openai 那套 "Bearer {credential}"
    assert_eq!(echoed["headers"]["x-api-key"], "sk-ant-secret");
    assert!(
        echoed["headers"].get("authorization").is_none(),
        "客户端凭据不得透传给上游"
    );

    // route.headers 里声明的固定头也注入了
    assert_eq!(echoed["headers"]["anthropic-version"], "2023-06-01");

    // 回显响应里没有 usage，本次不产生费用，但冻结必须释放
    for _ in 0..100 {
        if fx.balances().await.1 == 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    panic!("冻结未释放：{:?}", fx.balances().await);
}
