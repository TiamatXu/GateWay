//! 数据平面：鉴权、准入、dispatcher、生命周期编排。

pub mod admission;
pub mod app;
pub mod auth;
pub mod endpoint;
pub(crate) mod handles;
pub mod log_sink;
pub mod probe;
pub mod protocol_error;
pub mod settlement;
pub mod task;
pub(crate) mod upstream;

pub use admission::{Admission, LoadGuard, LoadGuardConfig, LoadSample, RejectReason, Signal};
pub use app::{AppState, GatewayConfig, router};
pub use auth::{AuthError, Principal, authenticate, extract_bearer, key_hash, key_prefix};
pub use endpoint::{EndpointSpecs, Endpoints, SpecTable};
pub use log_sink::{LogSink, PgLogSink, SinkError, spawn_log_writer};
pub use probe::{CgroupProbe, LoadProbe};
pub use protocol_error::error_body;
pub use settlement::{SettlementCtx, SettlementGuard, Settler, TaskOutcome};
pub use task::{SweepConfig, TaskSweeper, spawn_sweeper};
