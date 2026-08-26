//! 句柄与任务存储的行为约定。

use std::time::Duration;

use gw_core::{
    AccountId, ChannelId, HandleId, HandleKind, HoldId, ProviderId, RequestId, TaskPhase,
};
use gw_resource::store::{NewHandle, NewTask, ResourceError, ResourceStore};
use gw_resource::{Handle, PgResourceStore};
use sqlx::PgPool;

struct Fixture {
    store: PgResourceStore,
    channel: ChannelId,
    account: AccountId,
    other: AccountId,
    suffix: String,
}

async fn fixture() -> Fixture {
    let pool: PgPool = gw_store::testkit::pool().await;
    let suffix = uuid::Uuid::new_v4().simple().to_string();

    let channel: i64 = sqlx::query_scalar(
        "INSERT INTO channel (provider, base_url, credential, enabled)
         VALUES ('aliyun', 'http://localhost', $1, true) RETURNING id",
    )
    .bind(b"secret".to_vec())
    .fetch_one(&pool)
    .await
    .unwrap();

    let mut accounts = Vec::new();
    for i in 0..2 {
        let node: i64 = sqlx::query_scalar(
            "INSERT INTO org_node (uuid, path, kind, source, name)
             VALUES ($1, text2ltree($2), 0, 0, 'res') RETURNING id",
        )
        .bind(uuid::Uuid::new_v4())
        .bind(format!("n{suffix}{i}"))
        .fetch_one(&pool)
        .await
        .unwrap();
        accounts.push(
            sqlx::query_scalar::<_, i64>("INSERT INTO account (node_id) VALUES ($1) RETURNING id")
                .bind(node)
                .fetch_one(&pool)
                .await
                .unwrap(),
        );
    }

    Fixture {
        store: PgResourceStore::new(pool),
        channel: ChannelId(channel),
        account: AccountId(accounts[0]),
        other: AccountId(accounts[1]),
        suffix,
    }
}

fn provider() -> ProviderId {
    ProviderId("aliyun".into())
}

impl Fixture {
    async fn issue(&self, upstream: &str, owner: AccountId, hold: Option<HoldId>) -> HandleId {
        self.store
            .issue(NewHandle {
                kind: HandleKind::Task,
                channel: self.channel,
                provider: &provider(),
                upstream_id: upstream,
                account_chain: &[owner],
                key_id: None,
                endpoint: "text2video_submit",
                hold_id: hold,
            })
            .await
            .unwrap()
    }

    async fn resolve(&self, id: HandleId) -> Handle {
        self.store.resolve(id).await.unwrap().unwrap()
    }

    async fn open(&self, handle: HandleId) {
        self.store
            .open_task(NewTask {
                handle,
                submit_endpoint: "text2video_submit",
                inbound: "/aliyun/x",
                model: "wanx",
                request_id: RequestId(uuid::Uuid::new_v4()),
            })
            .await
            .unwrap();
    }
}

/// 提交重试拿到相同的上游 ID 时，客户端必须看到同一个虚拟 ID——
/// 否则同一个资源会有两个句柄、两笔预扣
#[tokio::test]
async fn reissuing_the_same_upstream_id_returns_the_same_handle() {
    let f = fixture().await;
    let up = format!("task-{}", f.suffix);

    let first = f.issue(&up, f.account, None).await;
    let again = f.issue(&up, f.account, None).await;

    assert_eq!(first, again);
}

/// 上游把同一个 ID 发给了两个账户：宁可失败也不能把别人的资源交出去
#[tokio::test]
async fn a_foreign_owner_on_the_same_upstream_id_is_refused() {
    let f = fixture().await;
    let up = format!("task-{}", f.suffix);
    f.issue(&up, f.account, None).await;

    let err = f
        .store
        .issue(NewHandle {
            kind: HandleKind::Task,
            channel: f.channel,
            provider: &provider(),
            upstream_id: &up,
            account_chain: &[f.other],
            key_id: None,
            endpoint: "text2video_submit",
            hold_id: None,
        })
        .await
        .expect_err("跨账户复用同一上游 ID 应当失败");
    assert!(matches!(err, ResourceError::OwnerConflict { .. }));
}

/// 归属看的是链首。父账户的链里没有子账户，因此查不到子账户的任务
#[tokio::test]
async fn ownership_follows_the_head_of_the_account_chain() {
    let f = fixture().await;
    let h = f
        .resolve(
            f.issue(&format!("task-{}", f.suffix), f.account, None)
                .await,
        )
        .await;

    assert!(h.owned_by(&[f.account, f.other]));
    assert!(!h.owned_by(&[f.other]));
    assert!(!h.owned_by(&[]));
}

/// 并发轮询里只有一次调用能把任务推入终态——结算权必须唯一，否则重复扣费
#[tokio::test]
async fn only_one_observer_wins_the_terminal_transition() {
    let f = fixture().await;
    let h = f
        .issue(&format!("task-{}", f.suffix), f.account, None)
        .await;
    f.open(h).await;

    // 未终态：记下状态，但不交出结算权
    assert!(
        f.store
            .observe(h, "RUNNING", TaskPhase::Running)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        f.store.task(h).await.unwrap().unwrap().state.as_deref(),
        Some("RUNNING")
    );

    let first = f
        .store
        .observe(h, "SUCCEEDED", TaskPhase::Succeeded)
        .await
        .unwrap();
    let second = f
        .store
        .observe(h, "SUCCEEDED", TaskPhase::Succeeded)
        .await
        .unwrap();

    assert!(first.is_some(), "第一次到达终态应当拿到结算权");
    assert!(second.is_none(), "终态不可重复结算");
    assert_eq!(
        f.store.task(h).await.unwrap().unwrap().phase,
        TaskPhase::Succeeded
    );
}

/// 任务开立与句柄签发一样必须幂等：提交重试不得开出第二条任务
#[tokio::test]
async fn opening_the_same_task_twice_is_idempotent() {
    let f = fixture().await;
    let h = f
        .issue(&format!("task-{}", f.suffix), f.account, None)
        .await;
    f.open(h).await;
    let first = f.store.task(h).await.unwrap().unwrap();
    f.open(h).await;

    assert_eq!(
        f.store.task(h).await.unwrap().unwrap().request_id,
        first.request_id
    );
}

/// 孤儿巡检只捞运行中且久无人问津的任务
#[tokio::test]
async fn stale_scan_skips_fresh_and_settled_tasks() {
    let f = fixture().await;
    let fresh = f
        .issue(&format!("fresh-{}", f.suffix), f.account, None)
        .await;
    let done = f
        .issue(&format!("done-{}", f.suffix), f.account, None)
        .await;
    f.open(fresh).await;
    f.open(done).await;
    f.store
        .observe(done, "SUCCEEDED", TaskPhase::Succeeded)
        .await
        .unwrap();

    // 刚建的任务不算孤儿
    let stale = f
        .store
        .stale_tasks(Duration::from_secs(3600), 100)
        .await
        .unwrap();
    assert!(!stale.iter().any(|(h, _)| h.id == fresh || h.id == done));

    // 放宽到「一秒没人问就算」，仍不该捞到已终态的
    let stale = f.store.stale_tasks(Duration::ZERO, 100).await.unwrap();
    assert!(stale.iter().any(|(h, _)| h.id == fresh));
    assert!(!stale.iter().any(|(h, _)| h.id == done));
}

/// 预扣单号跟着句柄走：终态结算时只能靠它找回那笔冻结
#[tokio::test]
async fn the_hold_id_survives_reissue() {
    let f = fixture().await;
    let up = format!("task-{}", f.suffix);
    let hold = HoldId(uuid::Uuid::new_v4());

    let h = f.issue(&up, f.account, Some(hold)).await;
    // 轮询响应回显上游 ID 时会再签发一次，且不带 hold——不能把原来的抹掉
    let again = f.issue(&up, f.account, None).await;

    assert_eq!(h, again);
    assert_eq!(f.resolve(h).await.hold_id, Some(hold));
}
