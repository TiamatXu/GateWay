use std::net::SocketAddr;

use figment::Figment;
use figment::providers::{Env, Format, Serialized, Toml};
use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    // figment::Error 体积大（200+ 字节），装箱避免撑大每个调用点的 Result
    #[error("配置加载失败: {0}")]
    Figment(#[from] Box<figment::Error>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub listen: SocketAddr,
    /// 无合理默认值：缺失必须报错，不能静默连到某处
    pub database_url: String,
    pub db_max_connections: u32,
    #[serde(default)]
    pub load: LoadConfig,
    #[serde(default)]
    pub log: LogConfig,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct LoadConfig {
    /// 每个在途流的内存预算
    pub per_stream_budget_bytes: u64,
    /// 内存 limit 的安全系数
    pub safety_factor: f64,
    /// 显式并发上限。为 0 时按内存预算推导。
    pub max_inflight: u32,
    pub enter_ratio: f32,
    pub exit_ratio: f32,
    pub window: usize,
    pub retry_after_secs: u64,
    pub sample_interval_ms: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct LogConfig {
    pub channel_capacity: usize,
    pub batch_size: usize,
    pub flush_interval_ms: u64,
}

impl Default for LoadConfig {
    fn default() -> Self {
        Self {
            per_stream_budget_bytes: 64 * 1024,
            safety_factor: 0.7,
            max_inflight: 0,
            enter_ratio: 0.90,
            exit_ratio: 0.80,
            window: 8,
            retry_after_secs: 3,
            sample_interval_ms: 500,
        }
    }
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            channel_capacity: 4096,
            batch_size: 256,
            flush_interval_ms: 500,
        }
    }
}

/// 仅用于提供默认值，`database_url` 不在其中。
#[derive(Serialize)]
struct Defaults {
    listen: SocketAddr,
    db_max_connections: u32,
    load: LoadConfig,
    log: LogConfig,
}

impl Config {
    /// 加载顺序：内置默认 → 配置文件 → 环境变量（`GW_` 前缀，嵌套用 `__` 分隔）。
    ///
    /// 配置文件缺失不是错误——容器部署常只给环境变量。
    ///
    /// # Errors
    /// 必填项缺失或类型不匹配。
    pub fn load(path: &str) -> Result<Self, ConfigError> {
        let defaults = Defaults {
            listen: "0.0.0.0:3000"
                .parse()
                .unwrap_or(SocketAddr::from(([0, 0, 0, 0], 3000))),
            db_max_connections: 32,
            load: LoadConfig::default(),
            log: LogConfig::default(),
        };
        Ok(Figment::from(Serialized::defaults(defaults))
            .merge(Toml::file(path))
            .merge(Env::prefixed("GW_").split("__"))
            .extract()
            .map_err(Box::new)?)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::result_large_err)]

    use figment::Jail;

    use super::*;

    #[test]
    fn loads_from_a_toml_file() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "gateway.toml",
                r#"
                listen = "0.0.0.0:9000"
                database_url = "postgres://localhost/gw"
                "#,
            )?;
            let c = Config::load("gateway.toml").unwrap();
            assert_eq!(c.listen, "0.0.0.0:9000".parse().unwrap());
            assert_eq!(c.database_url, "postgres://localhost/gw");
            Ok(())
        });
    }

    /// 环境变量覆盖文件：容器部署常只给环境变量
    #[test]
    fn environment_overrides_the_file() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "gateway.toml",
                r#"database_url = "postgres://from-file/gw""#,
            )?;
            jail.set_env("GW_DATABASE_URL", "postgres://from-env/gw");
            let c = Config::load("gateway.toml").unwrap();
            assert_eq!(c.database_url, "postgres://from-env/gw");
            Ok(())
        });
    }

    /// 配置文件缺失不是错误：全用环境变量即可跑起来
    #[test]
    fn missing_file_is_not_an_error_when_env_is_complete() {
        Jail::expect_with(|jail| {
            jail.set_env("GW_DATABASE_URL", "postgres://localhost/gw");
            let c = Config::load("no-such-file.toml").unwrap();
            assert_eq!(c.database_url, "postgres://localhost/gw");
            Ok(())
        });
    }

    /// 数据库地址没有合理默认值，缺失必须报错而非静默连到某处
    #[test]
    fn missing_database_url_is_an_error() {
        Jail::expect_with(|_| {
            assert!(Config::load("no-such-file.toml").is_err());
            Ok(())
        });
    }

    #[test]
    fn defaults_cover_everything_but_the_database() {
        Jail::expect_with(|jail| {
            jail.set_env("GW_DATABASE_URL", "postgres://localhost/gw");
            let c = Config::load("no-such-file.toml").unwrap();
            assert_eq!(c.listen.port(), 3000);
            assert_eq!(c.db_max_connections, 32);
            assert_eq!(c.load.per_stream_budget_bytes, 65_536);
            assert_eq!(c.log.batch_size, 256);
            Ok(())
        });
    }

    /// 嵌套段落也能被环境变量覆盖
    #[test]
    fn nested_sections_are_overridable() {
        Jail::expect_with(|jail| {
            jail.set_env("GW_DATABASE_URL", "postgres://localhost/gw");
            jail.set_env("GW_LOAD__ENTER_RATIO", "0.75");
            let c = Config::load("no-such-file.toml").unwrap();
            assert!((c.load.enter_ratio - 0.75).abs() < f32::EPSILON);
            Ok(())
        });
    }
}
