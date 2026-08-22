//! 数据平面：鉴权、准入、dispatcher、生命周期编排。

pub mod admission;
pub mod probe;

pub use admission::{Admission, LoadGuard, LoadGuardConfig, LoadSample, RejectReason, Signal};
pub use probe::{CgroupProbe, LoadProbe};
