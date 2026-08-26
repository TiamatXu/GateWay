//! 路线图 M2 要求的 YAML 库实测：`serde-saphyr` 是否满足描述文件的四项需求。
//! 不满足则回退 `yaml-rust2`。

use gw_core::{EndpointShape, HandleKind, HandleRole};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Doc {
    endpoints: Vec<Ep>,
}

#[derive(Debug, Deserialize)]
struct Ep {
    id: String,
    #[serde(default)]
    base_url: Option<String>,
    shape: EndpointShape,
}

/// 1+2：锚点引用与 merge key——端点继承 provider 默认值、共享规则块
#[test]
fn anchors_and_merge_keys() {
    let src = r"
defaults: &defaults
  base_url: https://api.openai.com
  shape: &json_shape
    request: json
    response: json
    handle: none
    billing: in_request
    retry: safe

endpoints:
  - <<: *defaults
    id: chat
  - <<: *defaults
    id: embeddings
    shape: *json_shape
";
    let doc: Doc = serde_saphyr::from_str(src).expect("锚点与 merge key 应当支持");
    assert_eq!(doc.endpoints.len(), 2);
    assert_eq!(doc.endpoints[0].id, "chat");
    assert_eq!(
        doc.endpoints[0].base_url.as_deref(),
        Some("https://api.openai.com")
    );
    assert_eq!(doc.endpoints[1].id, "embeddings");
}

/// 3：嵌套枚举——`handle: { issues: task }` 的 externally-tagged 形式
#[test]
fn nested_externally_tagged_enum() {
    let src = r"
endpoints:
  - id: video_submit
    shape:
      request: json
      response: json
      handle:
        issues: task
      billing: on_terminal
      retry: idempotent_with_key
";
    let doc: Doc = serde_saphyr::from_str(src).expect("嵌套枚举应当支持");
    assert_eq!(
        doc.endpoints[0].shape.handle,
        HandleRole::Issues(HandleKind::Task)
    );
}

/// 4：错误信息质量——能否指到出错的行列与字段
#[test]
fn error_message_points_at_the_offending_field() {
    let src = r"
endpoints:
  - id: chat
    shape:
      request: json
      response: json
      handle: none
      billing: not_a_real_timing
      retry: safe
";
    let err = serde_saphyr::from_str::<Doc>(src).expect_err("非法枚举值应当报错");
    let msg = format!("{err}");
    eprintln!("--- serde-saphyr 错误信息 ---\n{msg}\n---");
    assert!(
        msg.contains("not_a_real_timing") || msg.contains('7'),
        "错误信息既没提到出错的值也没提到行号: {msg}"
    );
}
