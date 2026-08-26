//! JSON Schema 由 Rust 类型生成并提交入库，CI 校验其与类型定义一致——
//! 与 `.sqlx/` 离线元数据同一套约定。手写 JSON Schema 与类型必然漂移。
//!
//! 重新生成：`UPDATE_SCHEMA=1 cargo test -p gw-registry --features schema`

#![cfg(feature = "schema")]

use std::path::PathBuf;

fn schema_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("providers/provider.schema.json")
}

fn generate() -> String {
    let schema = schemars::schema_for!(gw_registry::Descriptor);
    let mut s = serde_json::to_string_pretty(&schema).expect("schema 应当可序列化");
    s.push('\n');
    s
}

#[test]
fn committed_schema_matches_the_types() {
    let generated = generate();
    let path = schema_path();
    if std::env::var_os("UPDATE_SCHEMA").is_some() {
        std::fs::write(&path, &generated).expect("写入 schema 失败");
        return;
    }
    let committed = std::fs::read_to_string(&path).unwrap_or_default();
    assert_eq!(
        committed, generated,
        "providers/provider.schema.json 与类型定义不一致，\
         用 UPDATE_SCHEMA=1 cargo test -p gw-registry --features schema 重新生成并提交"
    );
}
