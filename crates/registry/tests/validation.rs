//! 语义层校验：JSON Schema 查不出的那些错。这一层是描述层的价值所在。

use gw_registry::{HookRegistry, RegistryError, compile, parse_str};

const HEAD: &str = r"
schema_version: 1
provider: probe
defaults:
  base_url: https://example.test
  auth:
    credential: static
    inject: { header: { name: authorization } }
endpoints:
";

fn check(endpoints: &str) -> Result<gw_registry::Loaded, RegistryError> {
    let d = parse_str(&format!("{HEAD}{endpoints}")).expect("结构层应当通过，这里测语义层");
    compile(vec![d], &HookRegistry::new(), 1)
}

fn message(endpoints: &str) -> String {
    check(endpoints)
        .expect_err("应当被语义校验拦下")
        .to_string()
}

#[test]
fn rejects_async_poll_pointing_at_a_missing_endpoint() {
    let msg = message(
        r"
  - id: submit
    shape: { request: json, response: json, handle: { issues: task }, billing: on_terminal, retry: safe }
    route: { method: POST, inbound: /probe/submit, upstream: /submit }
    handles: { issue: [{ at: '$.id', kind: task }] }
    async:
      poll: does_not_exist
      state: '$.status'
      terminal: { succeeded: [ok], failed: [bad] }
",
    );
    assert!(msg.contains("does_not_exist"), "{msg}");
}

#[test]
fn rejects_async_poll_pointing_at_a_wrong_shaped_endpoint() {
    let msg = message(
        r"
  - id: submit
    shape: { request: json, response: json, handle: { issues: task }, billing: on_terminal, retry: safe }
    route: { method: POST, inbound: /probe/submit, upstream: /submit }
    handles: { issue: [{ at: '$.id', kind: task }] }
    async:
      poll: not_a_poller
      state: '$.status'
      terminal: { succeeded: [ok], failed: [bad] }
  - id: not_a_poller
    shape: { request: none, response: json, handle: none, billing: not_billed, retry: safe }
    route: { method: GET, inbound: /probe/thing, upstream: /thing }
",
    );
    assert!(msg.contains("Consumes(Task)"), "{msg}");
}

/// 粗粒度维度与细粒度字段映射不能互相矛盾
#[test]
fn rejects_handle_role_contradicting_the_field_map() {
    let msg = message(
        r"
  - id: submit
    shape: { request: json, response: json, handle: { issues: task }, billing: on_terminal, retry: safe }
    route: { method: POST, inbound: /probe/submit, upstream: /submit }
    handles: { issue: [{ at: '$.id', kind: file }] }
",
    );
    assert!(msg.contains("Task"), "{msg}");
}

/// 上游模板只能用入站匹配得到的参数，否则运行期必然渲染失败
#[test]
fn rejects_upstream_param_absent_from_inbound_path() {
    let msg = message(
        r"
  - id: thing
    shape: { request: none, response: json, handle: none, billing: not_billed, retry: safe }
    route: { method: GET, inbound: /probe/things, upstream: '/v1/things/{thing_id}' }
",
    );
    assert!(msg.contains("thing_id"), "{msg}");
}

#[test]
fn rejects_unregistered_hook() {
    let msg = message(
        r"
  - id: thing
    shape: { request: json, response: json, handle: none, billing: in_request, retry: safe }
    route: { method: POST, inbound: /probe/things, upstream: /v1/things }
    usage:
      actual:
        - { dim: images, hook: no_such_hook }
",
    );
    assert!(msg.contains("no_such_hook"), "{msg}");
}

#[test]
fn rejects_rule_with_both_read_and_hook() {
    let msg = message(
        r"
  - id: thing
    shape: { request: json, response: json, handle: none, billing: in_request, retry: safe }
    route: { method: POST, inbound: /probe/things, upstream: /v1/things }
    usage:
      actual:
        - { dim: images, read: '$.n', hook: whatever }
",
    );
    assert!(msg.contains("同时写了"), "{msg}");
}

#[test]
fn rejects_rule_with_neither_read_nor_hook() {
    let msg = message(
        r"
  - id: thing
    shape: { request: json, response: json, handle: none, billing: in_request, retry: safe }
    route: { method: POST, inbound: /probe/things, upstream: /v1/things }
    usage:
      actual:
        - { dim: images, default: 1 }
",
    );
    assert!(msg.contains("既无 read 也无 hook"), "{msg}");
}

#[test]
fn rejects_duplicate_endpoint_ids() {
    let msg = message(
        r"
  - id: thing
    shape: { request: json, response: json, handle: none, billing: in_request, retry: safe }
    route: { method: POST, inbound: /probe/a, upstream: /a }
  - id: thing
    shape: { request: json, response: json, handle: none, billing: in_request, retry: safe }
    route: { method: POST, inbound: /probe/b, upstream: /b }
",
    );
    assert!(msg.contains("重复"), "{msg}");
}

#[test]
fn rejects_endpoint_without_any_base_url() {
    let d = parse_str(
        r"
schema_version: 1
provider: probe
defaults:
  auth:
    credential: static
    inject: { header: { name: authorization } }
endpoints:
  - id: thing
    shape: { request: json, response: json, handle: none, billing: in_request, retry: safe }
    route: { method: POST, inbound: /probe/a, upstream: /a }
",
    )
    .unwrap();
    let msg = compile(vec![d], &HookRegistry::new(), 1)
        .expect_err("没有 base_url 应当被拦下")
        .to_string();
    assert!(msg.contains("base_url"), "{msg}");
}

/// 同一入站路径可由多个 provider 承接，但入站面契约必须一致
#[test]
fn rejects_conflicting_inbound_contracts_across_providers() {
    let a = parse_str(&format!(
        "{HEAD}{}",
        r"
  - id: chat
    shape: { request: json, response: sse, handle: none, billing: in_request, retry: safe }
    route: { method: POST, inbound: /v1/chat/completions, upstream: /v1/chat/completions }
    model: '$.model'
"
    ))
    .unwrap();
    let b = parse_str(
        r"
schema_version: 1
provider: other
defaults:
  base_url: https://other.test
  auth:
    credential: static
    inject: { header: { name: authorization } }
endpoints:
  - id: chat
    shape: { request: json, response: json, handle: none, billing: in_request, retry: safe }
    route: { method: POST, inbound: /v1/chat/completions, upstream: /chat }
    model: '$.prompt_model'
",
    )
    .unwrap();
    let msg = compile(vec![a, b], &HookRegistry::new(), 1)
        .expect_err("入站契约冲突应当被拦下")
        .to_string();
    assert!(msg.contains("/v1/chat/completions"), "{msg}");
}

/// 入站契约一致时，多个 provider 可以共享同一条路径
#[test]
fn accepts_several_providers_on_one_inbound_path() {
    // 双方都声明同一个规范协议——未声明时会各自退化为 Native(自己)，那是另一回事
    let shared = r"
  - id: chat
    protocol: openai_chat
    shape: { request: json, response: sse, handle: none, billing: in_request, retry: safe }
    route: { method: POST, inbound: /v1/chat/completions, upstream: /v1/chat/completions }
    model: '$.model'
";
    let a = parse_str(&format!("{HEAD}{shared}")).unwrap();
    let b = parse_str(&format!(
        "{}{shared}",
        HEAD.replace("provider: probe", "provider: other")
    ))
    .unwrap();
    let loaded = compile(vec![a, b], &HookRegistry::new(), 1).expect("契约一致应当接受");
    let m = loaded
        .catalog
        .resolve(gw_registry::Method::Post, "/v1/chat/completions")
        .unwrap();
    assert_eq!(m.route.providers().count(), 2);
}

/// tokenize 需要一条指向文本的路径，hook 的返回值不是文本来源
#[test]
fn rejects_tokenize_without_read() {
    let msg = message(
        r"
  - id: thing
    shape: { request: json, response: json, handle: none, billing: in_request, retry: safe }
    route: { method: POST, inbound: /probe/things, upstream: /v1/things }
    usage:
      fallback:
        - { dim: output_tokens, hook: h, tokenize: o200k_base }
",
    );
    assert!(msg.contains("tokenize") || msg.contains("hook"), "{msg}");
}

/// 结构层：JSONPath 语法错误带行列位置，因为解析发生在 Deserialize 里
#[test]
fn malformed_jsonpath_is_reported_with_position() {
    let err = parse_str(&format!(
        "{HEAD}{}",
        r"
  - id: thing
    shape: { request: json, response: json, handle: none, billing: in_request, retry: safe }
    route: { method: POST, inbound: /probe/things, upstream: /v1/things }
    usage:
      actual:
        - { dim: images, read: '$.[' }
"
    ))
    .expect_err("非法 JSONPath 应当在结构层就被拦下");
    assert!(err.contains("JSONPath"), "{err}");
    assert!(err.contains("line"), "错误信息应当带行号: {err}");
}
