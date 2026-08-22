//! 负载准入。过载与优雅退出做的是同一件事：拒绝新请求、不影响在途请求。

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU8, AtomicU32, Ordering},
};
use std::time::Duration;

/// 每个在途流的内存预算
const PER_STREAM_BUDGET: u64 = 64 * 1024;
/// 内存 limit 的安全系数
const SAFETY_FACTOR: f64 = 0.7;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Signal {
    Concurrency,
    Memory,
    Cpu,
    SchedulerLag,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectReason {
    Overloaded(Signal),
    Draining,
    RateLimited,
}

#[derive(Debug)]
pub enum Admission {
    /// 放行。令牌在途期间占用一个并发名额，析构即归还。
    Allow(InflightToken),
    Reject {
        reason: RejectReason,
        retry_after: Option<Duration>,
    },
}

#[derive(Debug, Clone, Copy, Default)]
pub struct LoadSample {
    pub memory_ratio: f32,
    pub cpu_ratio: f32,
}

#[derive(Debug, Clone, Copy)]
pub struct LoadGuardConfig {
    pub max_inflight: u32,
    /// 进入拒绝态的水位
    pub enter_ratio: f32,
    /// 退出拒绝态的水位，必须低于 `enter_ratio`
    pub exit_ratio: f32,
    /// 滑动窗口长度，用于平滑瞬时尖峰
    pub window: usize,
    pub retry_after: Duration,
}

impl LoadGuardConfig {
    /// 主保护是并发预算而非被动测量：在途流是内存占用的主要来源，且可预算。
    #[must_use]
    pub fn from_memory_limit(bytes: u64) -> Self {
        #[allow(
            clippy::cast_precision_loss,
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss
        )]
        let budget = ((bytes as f64 * SAFETY_FACTOR) / PER_STREAM_BUDGET as f64) as u64;
        Self {
            max_inflight: u32::try_from(budget.max(1)).unwrap_or(u32::MAX),
            enter_ratio: 0.90,
            exit_ratio: 0.80,
            window: 8,
            retry_after: Duration::from_secs(1),
        }
    }
}

/// 滞回状态：0 = 正常，1 = 内存过载，2 = CPU 过载
const STATE_OK: u8 = 0;
const STATE_MEM: u8 = 1;
const STATE_CPU: u8 = 2;

#[derive(Debug)]
struct Shared {
    cfg: LoadGuardConfig,
    inflight: AtomicU32,
    state: AtomicU8,
    draining: AtomicBool,
}

/// 准入闸门。`check` 只做本地原子读，纳秒级；水位由后台采样任务经 `observe` 推进。
#[derive(Debug)]
pub struct LoadGuard {
    shared: Arc<Shared>,
    window: Mutex<Window>,
}

impl LoadGuard {
    #[must_use]
    pub fn new(cfg: LoadGuardConfig) -> Self {
        Self {
            shared: Arc::new(Shared {
                cfg,
                inflight: AtomicU32::new(0),
                state: AtomicU8::new(STATE_OK),
                draining: AtomicBool::new(false),
            }),
            window: Mutex::new(Window::new(cfg.window)),
        }
    }

    /// 检查顺序即成本递增顺序：退出态 → 并发预算 → 水位。
    pub fn check(&self) -> Admission {
        let cfg = &self.shared.cfg;
        if self.shared.draining.load(Ordering::Relaxed) {
            return reject(RejectReason::Draining, None);
        }
        match self.shared.state.load(Ordering::Relaxed) {
            STATE_MEM => {
                return reject(
                    RejectReason::Overloaded(Signal::Memory),
                    Some(cfg.retry_after),
                );
            }
            STATE_CPU => {
                return reject(RejectReason::Overloaded(Signal::Cpu), Some(cfg.retry_after));
            }
            _ => {}
        }
        match InflightToken::acquire(&self.shared) {
            Some(token) => Admission::Allow(token),
            None => reject(
                RejectReason::Overloaded(Signal::Concurrency),
                Some(cfg.retry_after),
            ),
        }
    }

    /// 由后台采样任务调用，推进滑动窗口与滞回状态。
    pub fn observe(&self, sample: LoadSample) {
        let cfg = &self.shared.cfg;
        let (mem, cpu) = {
            let mut w = self
                .window
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            w.push(sample);
            w.average()
        };

        let was = self.shared.state.load(Ordering::Relaxed);
        // 已在拒绝态时用 exit_ratio 判定，避免临界点反复开关
        let threshold = |signal: u8| {
            if was == signal {
                cfg.exit_ratio
            } else {
                cfg.enter_ratio
            }
        };
        // 内存优先：它是流式负载的主要成因
        let next = if mem >= threshold(STATE_MEM) {
            STATE_MEM
        } else if cpu >= threshold(STATE_CPU) {
            STATE_CPU
        } else {
            STATE_OK
        };
        self.shared.state.store(next, Ordering::Relaxed);
    }

    /// 优雅退出：停止接受新请求，在途请求不受影响。
    pub fn start_draining(&self) {
        self.shared.draining.store(true, Ordering::Relaxed);
    }

    #[must_use]
    pub fn inflight(&self) -> u32 {
        self.shared.inflight.load(Ordering::Relaxed)
    }
}

fn reject(reason: RejectReason, retry_after: Option<Duration>) -> Admission {
    Admission::Reject {
        reason,
        retry_after,
    }
}

/// 占用一个并发名额，析构即归还。
#[derive(Debug)]
pub struct InflightToken {
    shared: Arc<Shared>,
}

impl InflightToken {
    fn acquire(shared: &Arc<Shared>) -> Option<Self> {
        let max = shared.cfg.max_inflight;
        shared
            .inflight
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < max).then_some(n + 1)
            })
            .ok()
            .map(|_| Self {
                shared: Arc::clone(shared),
            })
    }
}

impl Drop for InflightToken {
    fn drop(&mut self) {
        self.shared.inflight.fetch_sub(1, Ordering::AcqRel);
    }
}

/// 定长滑动窗口，用于平滑瞬时尖峰。
#[derive(Debug)]
struct Window {
    samples: Vec<LoadSample>,
    next: usize,
    len: usize,
}

impl Window {
    fn new(cap: usize) -> Self {
        Self {
            samples: vec![LoadSample::default(); cap.max(1)],
            next: 0,
            len: 0,
        }
    }

    fn push(&mut self, s: LoadSample) {
        self.samples[self.next] = s;
        self.next = (self.next + 1) % self.samples.len();
        self.len = (self.len + 1).min(self.samples.len());
    }

    /// 窗口未填满时按已有采样数取均值
    fn average(&self) -> (f32, f32) {
        if self.len == 0 {
            return (0.0, 0.0);
        }
        #[allow(clippy::cast_precision_loss)]
        let n = self.len as f32;
        let (mut mem, mut cpu) = (0.0, 0.0);
        for s in self.samples.iter().take(self.len) {
            mem += s.memory_ratio;
            cpu += s.cpu_ratio;
        }
        (mem / n, cpu / n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    fn cfg() -> LoadGuardConfig {
        LoadGuardConfig {
            max_inflight: 2,
            enter_ratio: 0.90,
            exit_ratio: 0.80,
            window: 4,
            retry_after: Duration::from_secs(1),
        }
    }

    fn allow(a: Admission) -> InflightToken {
        match a {
            Admission::Allow(t) => t,
            Admission::Reject { reason, .. } => panic!("预期放行，实际拒绝：{reason:?}"),
        }
    }

    fn rejected_signal(a: &Admission) -> Option<Signal> {
        match a {
            Admission::Reject {
                reason: RejectReason::Overloaded(s),
                ..
            } => Some(*s),
            _ => None,
        }
    }

    /// 内存 limit × 0.7 / 64KiB
    #[test]
    fn derives_inflight_budget_from_memory_limit() {
        let c = LoadGuardConfig::from_memory_limit(1024 * 1024 * 1024);
        assert_eq!(c.max_inflight, 11468);
    }

    #[test]
    fn allows_when_idle() {
        let g = LoadGuard::new(cfg());
        assert!(matches!(g.check(), Admission::Allow(_)));
    }

    #[test]
    fn rejects_when_inflight_budget_exhausted() {
        let g = LoadGuard::new(cfg());
        let _a = allow(g.check());
        let _b = allow(g.check());
        assert_eq!(rejected_signal(&g.check()), Some(Signal::Concurrency));
    }

    /// 在途请求结束后名额必须归还
    #[test]
    fn releases_budget_when_token_dropped() {
        let g = LoadGuard::new(cfg());
        let a = allow(g.check());
        let _b = allow(g.check());
        drop(a);
        assert!(matches!(g.check(), Admission::Allow(_)));
    }

    #[rstest]
    #[case::memory(true, Signal::Memory)]
    #[case::cpu(false, Signal::Cpu)]
    fn hysteresis_enters_at_90_and_exits_at_80(#[case] is_mem: bool, #[case] signal: Signal) {
        let g = LoadGuard::new(cfg());
        let sample = |r: f32| {
            if is_mem {
                LoadSample {
                    memory_ratio: r,
                    cpu_ratio: 0.0,
                }
            } else {
                LoadSample {
                    memory_ratio: 0.0,
                    cpu_ratio: r,
                }
            }
        };

        // 持续高水位填满窗口后进入拒绝态
        for _ in 0..4 {
            g.observe(sample(0.95));
        }
        assert_eq!(rejected_signal(&g.check()), Some(signal));

        // 落到 enter 与 exit 之间仍保持拒绝，避免临界点反复开关
        for _ in 0..4 {
            g.observe(sample(0.85));
        }
        assert_eq!(rejected_signal(&g.check()), Some(signal));

        // 低于 exit 才恢复
        for _ in 0..4 {
            g.observe(sample(0.70));
        }
        assert!(matches!(g.check(), Admission::Allow(_)));
    }

    /// 单次尖峰被滑动窗口平滑，不得触发拒绝
    #[test]
    fn single_spike_does_not_trip_the_gate() {
        let g = LoadGuard::new(cfg());
        for _ in 0..4 {
            g.observe(LoadSample {
                memory_ratio: 0.10,
                cpu_ratio: 0.0,
            });
        }
        g.observe(LoadSample {
            memory_ratio: 1.0,
            cpu_ratio: 0.0,
        });
        assert!(matches!(g.check(), Admission::Allow(_)));
    }

    #[test]
    fn draining_rejects_regardless_of_load() {
        let g = LoadGuard::new(cfg());
        g.start_draining();
        assert!(matches!(
            g.check(),
            Admission::Reject {
                reason: RejectReason::Draining,
                ..
            }
        ));
    }

    /// 退出与过载都只拒绝新请求，不影响在途请求
    #[test]
    fn draining_does_not_invalidate_existing_tokens() {
        let g = LoadGuard::new(cfg());
        let token = allow(g.check());
        g.start_draining();
        assert_eq!(g.inflight(), 1);
        drop(token);
        assert_eq!(g.inflight(), 0);
    }

    #[test]
    fn reject_carries_retry_after() {
        let g = LoadGuard::new(cfg());
        let _a = allow(g.check());
        let _b = allow(g.check());
        match g.check() {
            Admission::Reject { retry_after, .. } => {
                assert_eq!(retry_after, Some(Duration::from_secs(1)));
            }
            Admission::Allow(_) => panic!("预期拒绝"),
        }
    }

    /// 内存与 CPU 同时超标时，先报内存——它是流式负载的主要成因
    #[test]
    fn reports_memory_first_when_both_exceed() {
        let g = LoadGuard::new(cfg());
        for _ in 0..4 {
            g.observe(LoadSample {
                memory_ratio: 0.95,
                cpu_ratio: 0.99,
            });
        }
        assert_eq!(rejected_signal(&g.check()), Some(Signal::Memory));
    }
}
