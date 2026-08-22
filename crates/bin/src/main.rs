//! 网关入口。
//!
//! 优雅退出的顺序不可调换：
//! 1. 停止接受新请求（准入闸门置为 draining）
//! 2. 等在途流自然结束（axum graceful shutdown）
//! 3. 排空结算任务——关机瞬间断连的请求会在此落账
//! 4. 关闭日志通道并等写入任务收尾

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use gw_gateway::LoadProbe;
use gw_gateway::admission::LoadGuardConfig;
use gw_gateway::app::{AppState, GatewayConfig};
use gw_gateway::settlement::Settler;
use gw_gateway::{CgroupProbe, LoadGuard, PgLogSink, spawn_log_writer};
use gw_infra::Config;
use gw_ledger::{Coordinator, PgCoordinator};
use gw_pricing::{EstimateCeilings, PgPriceEngine};
use gw_proxy::Upstream;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::load("gateway.toml").context("加载配置失败")?;
    gw_infra::init_tracing(false);

    let pool = gw_store::connect(&config.database_url, config.db_max_connections)
        .await
        .context("连接数据库失败")?;

    let (reclaim_tx, reclaim_rx) = mpsc::channel(1024);
    let (log_tx, log_rx) = mpsc::channel(config.log.channel_capacity);

    let coord = Arc::new(PgCoordinator::new(pool.clone(), reclaim_tx));
    let pricing = Arc::new(PgPriceEngine::new(
        pool.clone(),
        EstimateCeilings::default(),
    ));
    let settler = Arc::new(Settler::new(coord.clone(), pricing.clone(), log_tx.clone()));

    let probe = CgroupProbe::new();
    let load = Arc::new(LoadGuard::new(load_config(&config, &probe)));

    let log_writer = spawn_log_writer(
        Box::new(PgLogSink::new(pool.clone())),
        log_rx,
        config.log.batch_size,
        Duration::from_millis(config.log.flush_interval_ms),
    );
    let sampler = spawn_load_sampler(
        Arc::clone(&load),
        probe,
        Duration::from_millis(config.load.sample_interval_ms),
    );
    let reclaimer = spawn_reclaimer(coord.clone(), reclaim_rx);

    let state = Arc::new(AppState {
        pool,
        redactor: gw_core::HeaderRedactor::default(),
        load: Arc::clone(&load),
        coord,
        pricing,
        settler: Arc::clone(&settler),
        upstream: Upstream::new(),
        config: GatewayConfig::default(),
    });

    let listener = tokio::net::TcpListener::bind(config.listen)
        .await
        .with_context(|| format!("监听 {} 失败", config.listen))?;
    tracing::info!(addr = %config.listen, "网关已启动");

    let draining = Arc::clone(&load);
    axum::serve(listener, gw_gateway::router(state))
        .with_graceful_shutdown(async move {
            shutdown_signal().await;
            // 先停止接受新请求，在途请求不受影响
            draining.start_draining();
            tracing::info!("收到退出信号，停止接受新请求");
        })
        .await
        .context("服务异常退出")?;

    // 在途流已结束，此时才排空结算——否则关机瞬间断连的请求会漏账
    settler.tasks().close();
    settler.tasks().wait().await;
    tracing::info!("结算任务已排空");

    // Settler 内部持有一份日志通道发送端，必须先放掉，写入任务才能收到关闭信号
    drop(settler);
    drop(log_tx);
    // 加超时兜底：若有 Arc 未释放导致通道不关，宁可丢日志也不能卡住退出
    if tokio::time::timeout(Duration::from_secs(10), log_writer)
        .await
        .is_err()
    {
        tracing::warn!("日志写入任务未在 10 秒内收尾，强制退出");
    }
    sampler.abort();
    reclaimer.abort();
    tracing::info!("已退出");
    Ok(())
}

/// 主保护是并发预算：在途流是内存占用的主要来源，且可预算。
/// 未显式配置时按容器内存 limit 推导。
fn load_config(config: &Config, probe: &CgroupProbe) -> LoadGuardConfig {
    let c = &config.load;
    let mut cfg = if c.max_inflight > 0 {
        LoadGuardConfig {
            max_inflight: c.max_inflight,
            ..LoadGuardConfig::from_memory_limit(0)
        }
    } else {
        // 读不到 cgroup 时退回一个保守值，而非放开无限并发
        let derived = LoadGuardConfig::from_memory_limit(2 * 1024 * 1024 * 1024);
        tracing::info!(
            max_inflight = derived.max_inflight,
            memory_ratio = probe.sample().memory_ratio,
            "按内存预算推导并发上限"
        );
        derived
    };
    cfg.enter_ratio = c.enter_ratio;
    cfg.exit_ratio = c.exit_ratio;
    cfg.window = c.window;
    cfg.retry_after = Duration::from_secs(c.retry_after_secs);
    cfg
}

fn spawn_load_sampler(
    load: Arc<LoadGuard>,
    probe: CgroupProbe,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            load.observe(probe.sample());
        }
    })
}

/// 兜底回收：既消费泄漏上报，也定期扫描过期 Hold。
fn spawn_reclaimer(
    coord: Arc<dyn Coordinator>,
    mut leaked: mpsc::Receiver<gw_core::HoldId>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(30));
        loop {
            tokio::select! {
                Some(id) = leaked.recv() => {
                    if let Err(e) = coord.void_by_id(id).await {
                        tracing::warn!(hold_id = %id, error = %e, "回收泄漏 Hold 失败");
                    }
                }
                _ = ticker.tick() => {
                    match coord.reclaim_expired(500).await {
                        Ok(n) if n > 0 => tracing::info!(count = n, "回收过期 Hold"),
                        Ok(_) => {}
                        Err(e) => tracing::warn!(error = %e, "扫描过期 Hold 失败"),
                    }
                }
            }
        }
    })
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        if let Ok(mut sig) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            sig.recv().await;
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
}
