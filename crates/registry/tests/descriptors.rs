//! 对内置描述文件跑三层校验，并检查 M2 的验收标准。

use std::path::PathBuf;

use gw_core::{BillingTiming, HandleKind, HandleRole, ProtocolKind, ProviderId, RequestForm};
use gw_registry::{HookRegistry, Method, load_dir};

fn providers_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("providers")
}

fn load() -> gw_registry::Loaded {
    load_dir(&providers_dir(), &HookRegistry::new(), 1).expect("内置描述文件应当通过全部三层校验")
}

/// 首批 provider 全部由描述文件驱动，主干代码零厂商分支
#[test]
fn bundled_descriptors_compile() {
    let loaded = load();
    let providers: std::collections::BTreeSet<_> = loaded
        .catalog
        .endpoints()
        .iter()
        .map(|e| e.provider.as_str().to_owned())
        .collect();
    assert!(providers.contains("openai"));
    assert!(providers.contains("anthropic"));
    assert!(providers.contains("volcengine"));
    assert!(providers.contains("aliyun"));
}

/// 入站路径解析出端点，协议来自声明而非路径前缀猜测
#[test]
fn resolves_inbound_paths_to_endpoints() {
    let loaded = load();
    let m = loaded
        .catalog
        .resolve(Method::Post, "/v1/chat/completions")
        .expect("OpenAI 兼容入口应当可解析");
    assert_eq!(m.route.protocol, ProtocolKind::OpenAiChat);
    assert_eq!(m.route.model.as_ref().unwrap().to_string(), "$.model");

    let openai = ProviderId("openai".into());
    let desc = m.binding(&openai).expect("openai 应当绑定在该路由上");
    assert_eq!(desc.id, "chat_completions");
    assert_eq!(m.upstream_path(desc).unwrap(), "/v1/chat/completions");

    // 原生协议端点：协议由声明给出，不靠路径前缀判断
    let native = loaded
        .catalog
        .resolve(Method::Post, "/anthropic/v1/messages")
        .unwrap();
    assert_eq!(native.route.protocol, ProtocolKind::AnthropicMessages);
}

/// 路径模板参数按名从入站传到上游。入站与上游路径可以不同。
#[test]
fn path_params_flow_from_inbound_to_upstream() {
    let src = r"
schema_version: 1
provider: probe
defaults:
  base_url: https://example.test
  auth:
    credential: static
    inject: { header: { name: authorization } }
endpoints:
  - id: file_content
    shape: { request: none, response: json, handle: none, billing: not_billed, retry: safe }
    route:
      method: GET
      inbound:  /probe/files/{file_id}/content
      upstream: /v1/files/{file_id}/raw
";
    let d = gw_registry::parse_str(src).unwrap();
    let loaded = gw_registry::compile(vec![d], &HookRegistry::new(), 1).unwrap();
    let m = loaded
        .catalog
        .resolve(Method::Get, "/probe/files/file-abc/content")
        .expect("带参数的入站路径应当可解析");
    let desc = m.binding(&ProviderId("probe".into())).unwrap();
    assert_eq!(m.upstream_path(desc).unwrap(), "/v1/files/file-abc/raw");
}

/// 调研 §3.5：轮询端点与提交端点各自独立声明，通过显式引用关联
#[test]
fn poll_endpoint_is_declared_independently_of_submit() {
    let loaded = load();
    let aliyun = ProviderId("aliyun".into());
    let submit = loaded
        .catalog
        .endpoint(&aliyun, "text2video_submit")
        .unwrap();
    let poll = loaded.catalog.endpoint(&aliyun, "task_query").unwrap();

    // 轮询路径不是提交路径加后缀——两者没有构造关系
    assert!(!poll.inbound.starts_with(&submit.inbound));
    let a = submit.async_task.as_ref().unwrap();
    assert_eq!(a.poll, "task_query");
    assert!(matches!(
        poll.shape.handle,
        HandleRole::Consumes(HandleKind::Task)
    ));
}

/// 调研 §3.4 的四个难点：不同 host、签名鉴权、跨端点句柄引用、二进制上传
#[test]
fn volcengine_asset_api_fits_the_schema() {
    let loaded = load();
    let volc = ProviderId("volcengine".into());
    let chat = loaded.catalog.endpoint(&volc, "chat_completions").unwrap();
    let upload = loaded.catalog.endpoint(&volc, "asset_upload").unwrap();

    // 难点 1：与同厂商 chat 端点不同 host
    assert_ne!(chat.base_url, upload.base_url);
    assert_eq!(upload.base_url, "https://open.volcengineapi.com");

    // 难点 2：AK/SK 请求签名，而非静态 Bearer
    assert!(matches!(
        upload.auth.inject,
        gw_registry::schema::InjectDef::Sign(_)
    ));

    // 难点 4：二进制上传
    assert_eq!(upload.shape.request, RequestForm::JsonThenBinary);
    assert_eq!(upload.shape.billing, BillingTiming::Metered);

    // 难点 3：签发的句柄在另一个端点的请求体里被解析
    assert_eq!(upload.handles.issue[0].kind, HandleKind::Asset);
    let submit = loaded
        .catalog
        .endpoint(&volc, "video_generation_submit")
        .unwrap();
    let asset_ref = submit
        .handles
        .consume
        .iter()
        .find(|h| h.kind == HandleKind::Asset)
        .expect("视频生成请求体里应当引用 asset 句柄");
    assert!(asset_ref.at.is_body(), "asset 引用在请求体里，不是路径参数");
}

/// 调研 §3.3：一个响应签发多个句柄字段、各自不同类型
#[test]
fn one_response_can_issue_several_handle_fields() {
    let src = r"
schema_version: 1
provider: probe
defaults:
  base_url: https://example.test
  auth:
    credential: static
    inject: { header: { name: authorization } }
endpoints:
  - id: batch_create
    shape:
      request: json
      response: json
      handle: { issues: batch }
      billing: on_terminal
      retry: idempotent_with_key
    route: { method: POST, inbound: /v1/batches, upstream: /v1/batches }
    handles:
      issue:
        - { at: '$.id',              kind: batch }
        - { at: '$.input_file_id',   kind: file }
        - { at: '$.output_file_id',  kind: file }
        - { at: '$.error_file_id',   kind: file }
";
    let d = gw_registry::parse_str(src).expect("多字段句柄声明应当合法");
    let loaded = gw_registry::compile(vec![d], &HookRegistry::new(), 1).unwrap();
    let ep = loaded
        .catalog
        .endpoint(&ProviderId("probe".into()), "batch_create")
        .unwrap();
    assert_eq!(ep.handles.issue.len(), 4);
    assert_eq!(
        ep.handles
            .issue
            .iter()
            .filter(|h| h.kind == HandleKind::File)
            .count(),
        3
    );
}

/// M2 验收标准：hook 使用比例不超过 5%
#[test]
fn hook_ratio_stays_within_budget() {
    let loaded = load();
    let (total, hooked) = loaded.catalog.hook_ratio();
    assert!(total > 0, "描述文件里应当有可求值槽位");
    assert!(
        hooked * 20 <= total,
        "hook 比例 {hooked}/{total} 超过 5%，按风险登记这是模型设计缺陷，应回头修正模型而非加特例"
    );
}

/// 未实现的端点是延期而非报错：通过校验、进目录，但不挂入站路由
#[test]
fn unimplemented_endpoints_are_deferred_not_routed() {
    let loaded = load();
    let deferred: Vec<_> = loaded
        .deferred
        .iter()
        .map(|d| format!("{}/{}", d.provider.as_str(), d.endpoint))
        .collect();
    // 二进制上传与 AK/SK 签名仍未实现
    assert!(deferred.contains(&"volcengine/asset_upload".to_owned()));

    // 已声明、可查，但入站解析不到
    assert!(
        loaded
            .catalog
            .endpoint(&ProviderId("volcengine".into()), "asset_upload")
            .is_some()
    );
    assert!(
        loaded
            .catalog
            .resolve(Method::Post, "/volcengine/assets/upload")
            .is_none(),
        "尚未实现的端点不应当接客"
    );
}

/// M3 §4.3/§4.4：句柄映射与异步托管落地后，阿里云百炼的提交与轮询端点开始接客
#[test]
fn async_task_endpoints_are_live() {
    let loaded = load();
    let deferred: Vec<_> = loaded
        .deferred
        .iter()
        .map(|d| format!("{}/{}", d.provider.as_str(), d.endpoint))
        .collect();
    assert!(!deferred.contains(&"aliyun/task_query".to_owned()));
    assert!(!deferred.contains(&"aliyun/text2video_submit".to_owned()));

    assert!(
        loaded
            .catalog
            .resolve(
                Method::Post,
                "/aliyun/api/v1/services/aigc/text2video/video-synthesis"
            )
            .is_some()
    );
    let poll = loaded
        .catalog
        .resolve(Method::Get, "/aliyun/api/v1/tasks/abc")
        .expect("轮询端点应当接客");
    assert_eq!(poll.params.get("task_id").map(String::as_str), Some("abc"));
    // 消费位置属于入站契约：不解析出它就无从做渠道亲和
    assert_eq!(poll.route.consume.len(), 1);
}
