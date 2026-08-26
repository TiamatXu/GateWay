//! 逃生舱。
//!
//! hook 是「算子表里没有的那个算子」：纯函数、无 IO、无状态、无上下文，
//! 只能出现在槽位级。刻意做窄——能做 IO 就能阻塞转发路径，能见全上下文就会
//! 成为绕过表达力不足的默认选项，从而掩盖模型缺陷。
//!
//! 需要 IO 与缓存的凭证获取不走这里，走 `CredentialDef` 的有限枚举。

use std::collections::HashMap;

use smol_str::SmolStr;

#[derive(Debug, thiserror::Error)]
#[error("hook 求值失败: {0}")]
pub struct HookError(pub String);

/// 函数指针而非闭包：hook 无状态，可捕获状态的类型会诱使人把缓存塞进来。
pub type OperatorHook = fn(&serde_json::Value) -> Result<serde_json::Value, HookError>;

/// 内置 hook 的注册表。`wasmtime` 载体待有真实需求时再抽 trait——
/// 逃生舱的形态取决于它要装什么，无用例时设计容易错。
#[derive(Debug, Default, Clone)]
pub struct HookRegistry {
    hooks: HashMap<SmolStr, OperatorHook>,
}

impl HookRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with(mut self, name: impl Into<SmolStr>, hook: OperatorHook) -> Self {
        self.hooks.insert(name.into(), hook);
        self
    }

    #[must_use]
    pub fn get(&self, name: &str) -> Option<OperatorHook> {
        self.hooks.get(name).copied()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.hooks.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.hooks.is_empty()
    }
}
