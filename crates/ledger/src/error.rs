use gw_core::{AccountId, Money};

#[derive(Debug, thiserror::Error)]
pub enum LedgerError {
    #[error("账户 {account:?} 可用额度不足：需要 {required}，可用 {available}")]
    InsufficientFunds {
        account: AccountId,
        required: Money,
        available: Money,
    },
    #[error("Hold 不处于活跃状态，无法结算")]
    HoldNotActive,
    #[error("账户链为空")]
    EmptyChain,
    #[error("数据库错误: {0}")]
    Db(#[from] sqlx::Error),
}
