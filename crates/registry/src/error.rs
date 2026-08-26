//! 加载与校验的错误。三层校验各有其错误形态。

use crate::milestone::Milestone;

#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    #[error("读取 {path} 失败: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },

    /// 语法层与结构层：`serde-saphyr` 的报错已带行列与出错字段，原样透出
    #[error("{path} 解析失败:\n{message}")]
    Parse { path: String, message: String },

    /// 语义层：JSON Schema 查不出的那些
    #[error("{provider}/{endpoint}: {message}")]
    Semantic {
        provider: String,
        endpoint: String,
        message: String,
    },

    /// 入站面属性属于客户端契约，同一路径上各 provider 必须一致。
    /// 未显式声明 `protocol` 的 provider 会退化为 `Native(自己)`，因而天然冲突——
    /// 要共享一条入站路径，双方都得声明同一个规范协议。
    #[error(
        "入站路径 {method} {path} 已被其他 provider 以不同的入站契约声明：{field} 不一致（冲突方: {provider}）"
    )]
    RouteConflict {
        method: &'static str,
        path: String,
        provider: String,
        field: &'static str,
    },

    #[error(
        "{provider}/{endpoint} 的 {field} = {value} 需要 {needs}，当前解释器实现到 {implemented}"
    )]
    NotYetImplemented {
        provider: String,
        endpoint: String,
        field: &'static str,
        value: String,
        needs: Milestone,
        implemented: Milestone,
    },
}
