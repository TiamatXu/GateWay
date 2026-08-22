//! 负载探针。
//!
//! 容器内必须读 cgroup，不可用 `sysinfo` 读系统内存——后者在容器中报告宿主机总量，
//! 会恒定显示空闲。非 Linux 平台无 cgroup，负载保护降级为禁用。

use std::path::{Path, PathBuf};

use crate::LoadSample;

/// cgroup v1 用接近 `i64::MAX` 的哨兵值表示「无限制」
const V1_UNLIMITED: u64 = 9_223_372_036_854_771_712;

pub trait LoadProbe: Send + Sync {
    fn sample(&self) -> LoadSample;
}

/// 从 cgroup 文件读取内存水位。
#[derive(Debug, Clone)]
pub struct CgroupProbe {
    root: PathBuf,
}

impl CgroupProbe {
    /// 默认挂载点。
    #[must_use]
    pub fn new() -> Self {
        Self::at("/sys/fs/cgroup")
    }

    #[must_use]
    pub fn at(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
        }
    }

    fn read(&self, name: &str) -> Option<String> {
        std::fs::read_to_string(self.root.join(name)).ok()
    }
}

impl Default for CgroupProbe {
    fn default() -> Self {
        Self::new()
    }
}

impl LoadProbe for CgroupProbe {
    fn sample(&self) -> LoadSample {
        // v2 优先，回退 v1；两者都读不到时视为无限制，返回 0
        let ratio = self
            .read("memory.current")
            .zip(self.read("memory.max"))
            .or_else(|| {
                self.read("memory.usage_in_bytes")
                    .zip(self.read("memory.limit_in_bytes"))
            })
            .and_then(|(cur, max)| memory_ratio(&cur, &max))
            .unwrap_or(0.0);

        LoadSample {
            memory_ratio: ratio,
            // SIMPLIFIED(M0): CPU 与调度延迟留空，M1 补齐
            cpu_ratio: 0.0,
        }
    }
}

/// 无上限、无法解析、上限为 0 时返回 `None`——宁可不保护，也不能报出假水位。
fn memory_ratio(current: &str, max: &str) -> Option<f32> {
    let max = max.trim();
    if max == "max" {
        return None;
    }
    let limit: u64 = max.parse().ok()?;
    if limit == 0 || limit >= V1_UNLIMITED {
        return None;
    }
    let used: u64 = current.trim().parse().ok()?;
    // 比值恒在 0..=1 附近，f64→f32 的精度损失不影响水位判定
    #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
    Some((used as f64 / limit as f64) as f32)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    /// cgroup v2：memory.max 为 "max" 表示无限制
    #[rstest]
    #[case("536870912", "1073741824", Some(0.5))]
    #[case("0", "1073741824", Some(0.0))]
    #[case("1073741824", "1073741824", Some(1.0))]
    #[case("536870912", "max", None)]
    #[case("536870912", "0", None)]
    fn parses_cgroup_v2_memory_ratio(
        #[case] current: &str,
        #[case] max: &str,
        #[case] expected: Option<f32>,
    ) {
        assert_eq!(memory_ratio(current, max), expected);
    }

    /// cgroup v1 用一个接近 `i64::MAX` 的哨兵值表示无限制，不能当成真实上限
    #[test]
    fn treats_v1_unlimited_sentinel_as_no_limit() {
        assert_eq!(memory_ratio("536870912", "9223372036854771712"), None);
    }

    #[rstest]
    #[case("", "1073741824")]
    #[case("abc", "1073741824")]
    #[case("536870912", "abc")]
    fn rejects_unparseable_values(#[case] current: &str, #[case] max: &str) {
        assert_eq!(memory_ratio(current, max), None);
    }

    /// 文件带换行，读进来必须先 trim
    #[test]
    fn tolerates_trailing_newline() {
        assert_eq!(memory_ratio("536870912\n", "1073741824\n"), Some(0.5));
    }

    /// 非 Linux 平台无 cgroup，负载保护降级为禁用
    #[test]
    fn returns_no_sample_when_cgroup_absent() {
        let p = CgroupProbe::at("/nonexistent/cgroup/path");
        assert!(p.sample().memory_ratio.abs() < f32::EPSILON);
    }
}
