//! 结算编排。
//!
//! `SettlementGuard` 是整条链路的核心——它保证结算在**任何**终止路径下都会发生：
//! 正常结束、客户端断连、上游报错、任务被取消。

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use chrono::{DateTime, Utc};
use gw_core::{
    AccountId, ApiKeyId, ChannelId, HandleId, HoldId, Money, RequestId, TaskPhase, UsageVector,
};
use gw_ledger::{Coordinator, Hold};
use gw_pricing::{PriceCtx, PriceEngine};
use gw_proxy::SharedTee;
use http::HeaderMap;
use smallvec::SmallVec;
use tokio::sync::mpsc;
use tokio_util::task::TaskTracker;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestStatus {
    Ok,
    Truncated,
    Failed,
    Rejected,
}

/// 落到日志通道的一条请求记录。M0 只带结算必需的字段。
#[derive(Debug, Clone)]
pub struct RequestRecord {
    pub request_id: RequestId,
    pub key_id: Option<ApiKeyId>,
    pub account_chain: SmallVec<[AccountId; 4]>,
    pub model: String,
    pub channel: ChannelId,
    pub endpoint: String,
    pub usage: UsageVector,
    pub amount: Option<Money>,
    pub status: RequestStatus,
    /// 关联的虚拟句柄。一次异步任务的提交与终态结算是两行日志，靠它串起来。
    pub handle_id: Option<HandleId>,
    /// 用量为 tokenizer 估算值。explain 接口须明示。
    pub estimated: bool,
    /// 已脱敏
    pub req_headers: HeaderMap,
    /// 已脱敏
    pub resp_headers: HeaderMap,
    pub started_at: DateTime<Utc>,
    pub ended_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct SettlementCtx {
    pub request_id: RequestId,
    pub key_id: Option<ApiKeyId>,
    pub account_chain: SmallVec<[AccountId; 4]>,
    pub model: String,
    pub channel: ChannelId,
    pub tier: String,
    pub endpoint: String,
    pub started_at: DateTime<Utc>,
    pub handle_id: Option<HandleId>,
    /// 已脱敏
    pub req_headers: HeaderMap,
    /// 已脱敏
    pub resp_headers: HeaderMap,
}

/// 一次异步任务的终态结算。与同步结算共用计价与日志，区别只在
/// Hold 已经脱离请求生命周期，只能按单号操作。
pub struct TaskOutcome {
    pub hold: HoldId,
    pub phase: TaskPhase,
    pub usage: UsageVector,
    pub estimated: bool,
    pub ctx: SettlementCtx,
}

/// 结算执行者。进程级单例，由 app state 持有。
pub struct Settler {
    coord: Arc<dyn Coordinator>,
    pricing: Arc<dyn PriceEngine>,
    logs: mpsc::Sender<RequestRecord>,
    dropped_logs: AtomicU64,
    tasks: TaskTracker,
}

impl Settler {
    #[must_use]
    pub fn new(
        coord: Arc<dyn Coordinator>,
        pricing: Arc<dyn PriceEngine>,
        logs: mpsc::Sender<RequestRecord>,
    ) -> Self {
        Self {
            coord,
            pricing,
            logs,
            dropped_logs: AtomicU64::new(0),
            tasks: TaskTracker::new(),
        }
    }

    /// 优雅退出时必须 `close()` 后 `wait()`，否则关机瞬间断连的请求会漏账。
    #[must_use]
    pub fn tasks(&self) -> &TaskTracker {
        &self.tasks
    }

    #[must_use]
    pub fn dropped_logs(&self) -> u64 {
        self.dropped_logs.load(Ordering::Relaxed)
    }

    /// 异步任务到达终态：按终态响应体的用量结算那笔早已托管的预扣。
    ///
    /// 失败的任务一律撤销预扣：厂商对失败的生成任务普遍不收费，而我方也拿不到
    /// 权威用量——没有依据的扣费比漏收更糟。真需要对失败收费时，
    /// 应当由描述文件在失败终态上声明用量规则，届时再放开。
    pub async fn settle_task(&self, o: TaskOutcome) {
        let TaskOutcome {
            hold,
            phase,
            usage,
            estimated,
            ctx,
        } = o;

        let (status, amount) = if matches!(phase, TaskPhase::Succeeded) {
            let price_ctx = PriceCtx {
                model: &ctx.model,
                channel: ctx.channel,
                tier: &ctx.tier,
                endpoint: &ctx.endpoint,
                // 按提交时刻取价格版本：改价不影响已提交的任务
                at: ctx.started_at,
                max_output_tokens: None,
                input_tokens: None,
                estimate: None,
            };
            match self.pricing.quote(&price_ctx, &usage).await {
                Ok(q) => match self.coord.capture_by_id(hold, q.amount).await {
                    Ok(()) => (RequestStatus::Ok, Some(q.amount)),
                    Err(e) => {
                        metrics::counter!("gateway.capture_failed").increment(1);
                        tracing::error!(hold_id = %hold, error = %e, "任务终态捕获失败");
                        (RequestStatus::Failed, None)
                    }
                },
                Err(e) => {
                    metrics::counter!("gateway.settle_price_failed").increment(1);
                    tracing::error!(hold_id = %hold, error = %e, "任务终态计价失败，撤销冻结待补价");
                    self.void_id(hold).await;
                    (RequestStatus::Failed, None)
                }
            }
        } else {
            self.void_id(hold).await;
            (RequestStatus::Failed, None)
        };

        self.deliver_log(RequestRecord {
            request_id: ctx.request_id,
            key_id: ctx.key_id,
            account_chain: ctx.account_chain,
            model: ctx.model,
            channel: ctx.channel,
            endpoint: ctx.endpoint,
            usage,
            amount,
            status,
            handle_id: ctx.handle_id,
            estimated,
            req_headers: ctx.req_headers,
            resp_headers: ctx.resp_headers,
            started_at: ctx.started_at,
            ended_at: Utc::now(),
        });
    }

    async fn void_id(&self, hold: HoldId) {
        if let Err(e) = self.coord.void_by_id(hold).await {
            tracing::error!(hold_id = %hold, error = %e, "撤销冻结失败");
        }
    }

    async fn settle(&self, hold: Option<Hold>, tee: SharedTee, ctx: SettlementCtx) {
        // 锁中毒说明有线程 panic 过，但已抽取的用量仍然有效，不能因此漏账
        let (usage, estimated) = match tee.lock() {
            Ok(t) => (t.snapshot(), t.estimated()),
            Err(e) => {
                let t = e.into_inner();
                (t.snapshot(), t.estimated())
            }
        };

        // 不计费端点没有 Hold，只留日志：轮询、取消这类端点必须有账可查，
        // 但没有任何金额动作。
        let Some(hold) = hold else {
            self.deliver_log(RequestRecord {
                request_id: ctx.request_id,
                key_id: ctx.key_id,
                account_chain: ctx.account_chain,
                model: ctx.model,
                channel: ctx.channel,
                endpoint: ctx.endpoint,
                usage,
                amount: None,
                status: RequestStatus::Ok,
                handle_id: ctx.handle_id,
                estimated,
                req_headers: ctx.req_headers,
                resp_headers: ctx.resp_headers,
                started_at: ctx.started_at,
                ended_at: Utc::now(),
            });
            return;
        };

        let price_ctx = PriceCtx {
            model: &ctx.model,
            channel: ctx.channel,
            tier: &ctx.tier,
            endpoint: &ctx.endpoint,
            at: ctx.started_at,
            // 结算用的是实际用量，估算参数无关
            max_output_tokens: None,
            input_tokens: None,
            estimate: None,
        };

        let (status, amount) = match self.pricing.quote(&price_ctx, &usage).await {
            Ok(quote) => {
                let amount = quote.amount;
                if let Err(e) = self.coord.capture(hold, amount).await {
                    metrics::counter!("gateway.capture_failed").increment(1);
                    tracing::error!(request_id = %ctx.request_id, error = %e, "捕获失败");
                    (RequestStatus::Failed, None)
                } else {
                    (RequestStatus::Ok, Some(amount))
                }
            }
            Err(e) => {
                // 计价失败是我方故障。向用户按预扣上限超收，比吃下这笔成本更糟；
                // 用量已记录在案，可事后补价。
                metrics::counter!("gateway.settle_price_failed").increment(1);
                tracing::error!(request_id = %ctx.request_id, error = %e, "计价失败，撤销冻结待补价");
                if let Err(e) = self.coord.void(hold).await {
                    tracing::error!(request_id = %ctx.request_id, error = %e, "撤销失败");
                }
                (RequestStatus::Failed, None)
            }
        };

        self.deliver_log(RequestRecord {
            request_id: ctx.request_id,
            key_id: ctx.key_id,
            account_chain: ctx.account_chain,
            model: ctx.model,
            channel: ctx.channel,
            endpoint: ctx.endpoint,
            usage,
            amount,
            status,
            handle_id: ctx.handle_id,
            estimated,
            req_headers: ctx.req_headers,
            resp_headers: ctx.resp_headers,
            started_at: ctx.started_at,
            ended_at: Utc::now(),
        });
    }

    /// 直接投递一条记录。异步任务的提交那一行没有金额动作，走不到结算，
    /// 但必须有账可查。
    pub fn log(&self, rec: RequestRecord) {
        self.deliver_log(rec);
    }

    /// 非阻塞投递：通道满时丢弃并计数，绝不反压到结算路径。
    /// capture 关系到钱，日志可以延迟、极端情况可以丢。
    fn deliver_log(&self, rec: RequestRecord) {
        if self.logs.try_send(rec).is_err() {
            let n = self.dropped_logs.fetch_add(1, Ordering::Relaxed) + 1;
            metrics::counter!("gateway.log_dropped").increment(1);
            tracing::warn!(dropped = n, "日志通道已满，丢弃请求记录");
        }
    }
}

/// 持有 Hold 直到响应流终止。析构即触发结算。
pub struct SettlementGuard {
    /// `None` 表示不计费端点：仍要留日志，只是没有金额动作。
    hold: Option<Hold>,
    /// 结算是否已触发。Drop 可能被走到多次的路径调用，只认第一次。
    fired: bool,
    tee: SharedTee,
    ctx: SettlementCtx,
    settler: Arc<Settler>,
}

impl SettlementGuard {
    #[must_use]
    pub fn new(
        hold: Option<Hold>,
        tee: SharedTee,
        ctx: SettlementCtx,
        settler: Arc<Settler>,
    ) -> Self {
        Self {
            hold,
            fired: false,
            tee,
            ctx,
            settler,
        }
    }
}

impl Drop for SettlementGuard {
    fn drop(&mut self) {
        if self.fired {
            return;
        }
        self.fired = true;
        let hold = self.hold.take();

        // Drop 中不能 await，只能 spawn。无运行时时让 Hold 自身的 Drop 上报泄漏，
        // 交由 TTL 回收器兜底。
        if tokio::runtime::Handle::try_current().is_err() {
            tracing::error!(request_id = %self.ctx.request_id, "无运行时，结算交由 TTL 回收兜底");
            return;
        }

        let settler = Arc::clone(&self.settler);
        let tee = Arc::clone(&self.tee);
        let ctx = self.ctx.clone();
        self.settler
            .tasks
            .spawn(async move { settler.settle(hold, tee, ctx).await });
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    use async_trait::async_trait;
    use chrono::Utc;
    use gw_core::{ChannelId, Money, RequestId, UsageExtractor, UsageVector};
    use gw_ledger::{Coordinator, Hold, HoldRequest, LedgerError};
    use gw_pricing::{PriceCtx, PriceEngine, PriceError, Quote};
    use gw_proxy::Tee;
    use tokio::sync::mpsc;

    use super::*;

    // -------------------------------------------------------------- 测试替身

    #[derive(Default)]
    struct FakeCoordinator {
        captured: Mutex<Vec<Money>>,
        voided: AtomicUsize,
    }

    #[async_trait]
    impl Coordinator for FakeCoordinator {
        async fn hold(&self, _req: HoldRequest<'_>) -> Result<Hold, LedgerError> {
            unimplemented!("测试不经由此路径创建 Hold")
        }
        async fn capture(&self, mut hold: Hold, actual: Money) -> Result<(), LedgerError> {
            self.captured.lock().unwrap().push(actual);
            hold.mark_settled_for_test();
            Ok(())
        }
        async fn void(&self, mut hold: Hold) -> Result<(), LedgerError> {
            self.voided.fetch_add(1, Ordering::Relaxed);
            hold.mark_settled_for_test();
            Ok(())
        }
        async fn extend(&self, _hold: &Hold, _delta: Money) -> Result<(), LedgerError> {
            Ok(())
        }
        async fn capture_partial(&self, _hold: &Hold, _amt: Money) -> Result<(), LedgerError> {
            Ok(())
        }
        async fn capture_by_id(&self, _id: gw_core::HoldId, a: Money) -> Result<(), LedgerError> {
            self.captured.lock().unwrap().push(a);
            Ok(())
        }
        async fn void_by_id(&self, _id: gw_core::HoldId) -> Result<(), LedgerError> {
            self.voided.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        async fn reclaim_expired(&self, _limit: i64) -> Result<u64, LedgerError> {
            Ok(0)
        }
        async fn balances(&self, _a: gw_core::AccountId) -> Result<(Money, Money), LedgerError> {
            Ok((Money::from_nanos(0), Money::from_nanos(0)))
        }
        async fn audit(&self, _limit: i64) -> Result<Vec<gw_ledger::AuditMismatch>, LedgerError> {
            Ok(vec![])
        }
        async fn hold_stats(&self) -> Result<gw_ledger::HoldStats, LedgerError> {
            Ok(gw_ledger::HoldStats {
                active: 0,
                age_p99: std::time::Duration::ZERO,
            })
        }
        async fn rate_allow(
            &self,
            _key: &gw_core::RateKey,
            _rate: u32,
            _burst: u32,
        ) -> Result<bool, LedgerError> {
            Ok(true)
        }
        async fn try_lock(
            &self,
            _key: &str,
            _ttl: std::time::Duration,
        ) -> Result<Option<gw_ledger::LockGuard>, LedgerError> {
            unimplemented!("结算路径不取分布式锁")
        }
    }

    /// 按 `output_tokens` × 2 纳单位计价
    struct FakePricing {
        fail: bool,
    }

    #[async_trait]
    impl PriceEngine for FakePricing {
        async fn estimate_max(&self, _ctx: &PriceCtx<'_>) -> Result<Money, PriceError> {
            Ok(Money::from_nanos(1_000))
        }
        async fn quote(
            &self,
            ctx: &PriceCtx<'_>,
            usage: &UsageVector,
        ) -> Result<Quote, PriceError> {
            if self.fail {
                return Err(PriceError::NoRule {
                    model: ctx.model.to_owned(),
                    endpoint: ctx.endpoint.to_owned(),
                });
            }
            Ok(Quote {
                amount: Money::from_nanos(usage.get("output_tokens") * 2),
                cost: Money::from_nanos(0),
                rule_ids: smallvec::SmallVec::new(),
                rule_version: 1,
                breakdown: Vec::new(),
                estimated: false,
            })
        }
    }

    struct FixedUsage {
        tokens: i64,
        estimated: bool,
    }

    impl UsageExtractor for FixedUsage {
        fn feed(&mut self, _chunk: &[u8]) {}
        fn snapshot(&self) -> UsageVector {
            let mut u = UsageVector::new();
            u.set("output_tokens", self.tokens);
            u
        }
        fn finish(self: Box<Self>) -> UsageVector {
            self.snapshot()
        }
        fn estimated(&self) -> bool {
            self.estimated
        }
    }

    // ------------------------------------------------------------------ 装配

    struct Harness {
        settler: Arc<Settler>,
        coord: Arc<FakeCoordinator>,
        /// 持住接收端，否则发送端会因通道关闭而报错
        _reclaim_rx: mpsc::Receiver<gw_core::HoldId>,
        reclaim_tx: mpsc::Sender<gw_core::HoldId>,
        logs: mpsc::Receiver<RequestRecord>,
    }

    fn harness(price_fails: bool, log_capacity: usize) -> Harness {
        let coord = Arc::new(FakeCoordinator::default());
        let (reclaim_tx, reclaim_rx) = mpsc::channel(16);
        let (log_tx, logs) = mpsc::channel(log_capacity);
        Harness {
            settler: Arc::new(Settler::new(
                coord.clone(),
                Arc::new(FakePricing { fail: price_fails }),
                log_tx,
            )),
            coord,
            _reclaim_rx: reclaim_rx,
            reclaim_tx,
            logs,
        }
    }

    fn ctx() -> SettlementCtx {
        SettlementCtx {
            request_id: RequestId(uuid::Uuid::new_v4()),
            key_id: None,
            account_chain: smallvec::SmallVec::new(),
            req_headers: http::HeaderMap::new(),
            resp_headers: http::HeaderMap::new(),
            model: "gpt-4o".into(),
            channel: ChannelId(1),
            tier: "default".into(),
            endpoint: "/v1/chat/completions".into(),
            started_at: Utc::now(),
            handle_id: None,
        }
    }

    fn guard(h: &Harness, tokens: i64) -> SettlementGuard {
        guard_with(h, tokens, false)
    }

    fn guard_with(h: &Harness, tokens: i64, estimated: bool) -> SettlementGuard {
        let tee = Arc::new(Mutex::new(Tee::new(Box::new(FixedUsage {
            tokens,
            estimated,
        }))));
        SettlementGuard::new(
            Some(Hold::stub(Money::from_nanos(1_000), h.reclaim_tx.clone())),
            tee,
            ctx(),
            Arc::clone(&h.settler),
        )
    }

    // ------------------------------------------------------------------ 测试

    /// 结算按析构那一刻已抽取到的用量执行
    #[tokio::test]
    async fn settles_with_the_usage_captured_so_far() {
        let h = harness(false, 16);

        drop(guard(&h, 30));
        h.settler.tasks().close();
        h.settler.tasks().wait().await;

        assert_eq!(
            *h.coord.captured.lock().unwrap(),
            vec![Money::from_nanos(60)]
        );
    }

    /// 流未结束时不得结算
    #[tokio::test]
    async fn does_not_settle_while_the_guard_is_alive() {
        let h = harness(false, 16);
        let g = guard(&h, 30);

        tokio::task::yield_now().await;
        assert!(h.coord.captured.lock().unwrap().is_empty());

        drop(g);
        h.settler.tasks().close();
        h.settler.tasks().wait().await;
        assert_eq!(h.coord.captured.lock().unwrap().len(), 1);
    }

    /// 优雅退出必须等结算任务排空，否则关机瞬间断连的请求会漏账
    #[tokio::test]
    async fn shutdown_drains_pending_settlements() {
        let h = harness(false, 16);
        for _ in 0..20 {
            drop(guard(&h, 5));
        }

        h.settler.tasks().close();
        h.settler.tasks().wait().await;

        assert_eq!(h.coord.captured.lock().unwrap().len(), 20);
    }

    /// 计价失败是我方故障，不能向用户超收：撤销冻结并留记录待补价
    #[tokio::test]
    async fn pricing_failure_voids_instead_of_overcharging() {
        let mut h = harness(true, 16);

        drop(guard(&h, 30));
        h.settler.tasks().close();
        h.settler.tasks().wait().await;

        assert!(h.coord.captured.lock().unwrap().is_empty());
        assert_eq!(h.coord.voided.load(Ordering::Relaxed), 1);

        let rec = h.logs.try_recv().expect("应留下待补价的记录");
        assert_eq!(rec.status, RequestStatus::Failed);
        assert_eq!(rec.usage.get("output_tokens"), 30);
    }

    #[tokio::test]
    async fn successful_settlement_records_the_quote() {
        let mut h = harness(false, 16);

        drop(guard(&h, 30));
        h.settler.tasks().close();
        h.settler.tasks().wait().await;

        let rec = h.logs.try_recv().unwrap();
        assert_eq!(rec.status, RequestStatus::Ok);
        assert_eq!(rec.amount, Some(Money::from_nanos(60)));
    }

    /// 估算值必须一路传到账单记录
    #[tokio::test]
    async fn records_that_usage_was_estimated() {
        let mut h = harness(false, 16);

        drop(guard_with(&h, 30, true));
        h.settler.tasks().close();
        h.settler.tasks().wait().await;

        let rec = h.logs.try_recv().unwrap();
        assert!(rec.estimated);
        assert_eq!(rec.amount, Some(Money::from_nanos(60)));
    }

    fn task_outcome(phase: gw_core::TaskPhase, tokens: i64) -> TaskOutcome {
        let mut usage = UsageVector::new();
        usage.set("output_tokens", tokens);
        TaskOutcome {
            hold: gw_core::HoldId(uuid::Uuid::new_v4()),
            phase,
            usage,
            estimated: false,
            ctx: SettlementCtx {
                handle_id: Some(gw_core::HandleId(uuid::Uuid::new_v4())),
                ..ctx()
            },
        }
    }

    /// 任务成功：按终态用量捕获那笔早已托管的预扣
    #[tokio::test]
    async fn terminal_success_captures_the_detached_hold() {
        let mut h = harness(false, 16);

        h.settler
            .settle_task(task_outcome(gw_core::TaskPhase::Succeeded, 30))
            .await;

        assert_eq!(
            *h.coord.captured.lock().unwrap(),
            vec![Money::from_nanos(60)]
        );
        let rec = h.logs.try_recv().unwrap();
        assert_eq!(rec.status, RequestStatus::Ok);
        assert!(rec.handle_id.is_some(), "结算记录要能追回是哪个任务");
    }

    /// 任务失败一律不收费：没有权威用量时扣费没有依据
    #[tokio::test]
    async fn terminal_failure_voids_without_charging() {
        let mut h = harness(false, 16);

        h.settler
            .settle_task(task_outcome(gw_core::TaskPhase::Failed, 30))
            .await;

        assert!(h.coord.captured.lock().unwrap().is_empty());
        assert_eq!(h.coord.voided.load(Ordering::Relaxed), 1);
        assert_eq!(h.logs.try_recv().unwrap().status, RequestStatus::Failed);
    }

    /// 不计费端点没有 Hold，但必须留下日志
    #[tokio::test]
    async fn unbilled_endpoint_logs_without_touching_money() {
        let mut h = harness(false, 16);

        let tee = Arc::new(Mutex::new(Tee::new(Box::new(FixedUsage {
            tokens: 0,
            estimated: false,
        }))));
        drop(SettlementGuard::new(
            None,
            tee,
            ctx(),
            Arc::clone(&h.settler),
        ));
        h.settler.tasks().close();
        h.settler.tasks().wait().await;

        assert!(h.coord.captured.lock().unwrap().is_empty());
        assert_eq!(h.coord.voided.load(Ordering::Relaxed), 0);
        let rec = h.logs.try_recv().expect("不计费也要有账可查");
        assert_eq!(rec.amount, None);
    }

    /// 日志投递对结算路径非阻塞：通道满时丢弃并计数，绝不反压到结算
    #[tokio::test]
    async fn full_log_channel_never_blocks_settlement() {
        let h = harness(false, 1);

        for _ in 0..10 {
            drop(guard(&h, 5));
        }
        h.settler.tasks().close();
        h.settler.tasks().wait().await;

        assert_eq!(
            h.coord.captured.lock().unwrap().len(),
            10,
            "日志反压到了结算"
        );
        assert_eq!(h.settler.dropped_logs(), 9);
    }
}
