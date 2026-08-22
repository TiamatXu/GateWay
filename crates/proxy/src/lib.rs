//! 流式反向代理与 tee 旁路。

pub mod tee;

pub use tee::{ArchiveError, ArchiveSink, SharedTee, Tee, TeeStream};
