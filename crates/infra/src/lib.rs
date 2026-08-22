//! 配置加载与可观测性初始化。

pub mod config;
pub mod observability;

pub use config::{Config, ConfigError, LoadConfig, LogConfig};
pub use observability::init_tracing;
