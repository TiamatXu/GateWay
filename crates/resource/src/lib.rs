//! 虚拟句柄映射、渠道亲和与异步任务托管。
//!
//! 上游签发的每个句柄（task / file / batch / asset / …）都由网关换成自己的虚拟 ID
//! 对外暴露，记录 `虚拟ID → (渠道, 上游ID, 归属, 预扣单号)`。由此得到两件必需能力：
//!
//! - **渠道亲和**：消费句柄的请求必须回到签发它的那个渠道；
//! - **任务生命周期托管**：`OnTerminal` 的预扣挂在句柄上，终态才结算。
//!
//! 设计见 `docs/superpowers/specs/2026-08-26-m2-descriptor-layer-design.md` §4.3/§4.4。

pub mod pg;
pub mod store;
pub mod wire;

pub use pg::PgResourceStore;
pub use store::{Handle, NewHandle, NewTask, ResourceError, ResourceStore, Task};
