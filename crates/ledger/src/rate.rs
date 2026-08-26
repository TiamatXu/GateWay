//! 令牌桶限流。
//!
//! SIMPLIFIED(M1): 桶状态在进程内，多节点部署时实际速率是 `rate × 节点数`。
//! 跨节点聚合需要共享存储，已随 Redis Coordinator 一并移出 M1（见
//! `docs/superpowers/specs/2026-08-26-m1-ledger-scope-design.md` §3.4）。
//! 两种实现共用本模块，因此换成共享存储时只有这一处要改。

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;

use gw_core::RateKey;

/// 超过这个桶数就顺手清掉已回满的空闲桶。回满意味着该 key 近期无流量，
/// 丢掉它不改变限流结果——下次请求会以满桶重建。
const PRUNE_THRESHOLD: usize = 4096;

#[derive(Debug)]
struct Bucket {
    tokens: f64,
    last: Instant,
}

#[derive(Debug, Default)]
pub(crate) struct RateLimiter {
    buckets: Mutex<HashMap<RateKey, Bucket>>,
}

impl RateLimiter {
    /// 取一个令牌。`rate` 为每秒补充数，`burst` 为桶容量。
    ///
    /// `rate` 或 `burst` 为 0 视为不限流——限流未配置时的自然表达。
    pub(crate) fn allow(&self, key: &RateKey, rate: u32, burst: u32) -> bool {
        if rate == 0 || burst == 0 {
            return true;
        }
        let burst = f64::from(burst);
        let rate = f64::from(rate);
        let now = Instant::now();

        let Ok(mut buckets) = self.buckets.lock() else {
            // 锁中毒说明某次持锁时 panic 了。限流是尽力而为的保护，
            // 不该因为它的内部故障把请求全部拒掉。
            return true;
        };

        if buckets.len() >= PRUNE_THRESHOLD {
            buckets.retain(|_, b| {
                let refilled = b.tokens + now.duration_since(b.last).as_secs_f64() * rate;
                refilled < burst
            });
        }

        let bucket = buckets.entry(key.clone()).or_insert(Bucket {
            tokens: burst,
            last: now,
        });
        let elapsed = now.duration_since(bucket.last).as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * rate).min(burst);
        bucket.last = now;

        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> RateKey {
        RateKey::api_key(gw_core::ApiKeyId(1))
    }

    #[test]
    fn burst_is_spent_then_denied() {
        let rl = RateLimiter::default();
        for _ in 0..5 {
            assert!(rl.allow(&key(), 1, 5));
        }
        assert!(!rl.allow(&key(), 1, 5));
    }

    #[test]
    fn zero_rate_or_burst_means_unlimited() {
        let rl = RateLimiter::default();
        for _ in 0..100 {
            assert!(rl.allow(&key(), 0, 10));
            assert!(rl.allow(&key(), 10, 0));
        }
    }

    #[test]
    fn tokens_refill_over_time() {
        let rl = RateLimiter::default();
        assert!(rl.allow(&key(), 1_000_000, 1));
        // 桶已空；补充速率高到微秒级也能回满一个令牌
        std::thread::sleep(std::time::Duration::from_millis(5));
        assert!(rl.allow(&key(), 1_000_000, 1));
    }

    #[test]
    fn separate_keys_have_separate_buckets() {
        let rl = RateLimiter::default();
        let a = RateKey::api_key(gw_core::ApiKeyId(1));
        let b = RateKey::api_key(gw_core::ApiKeyId(2));
        assert!(rl.allow(&a, 1, 1));
        assert!(!rl.allow(&a, 1, 1));
        assert!(rl.allow(&b, 1, 1), "另一个 key 不应受影响");
    }
}
