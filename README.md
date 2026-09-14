# GateWay

AI 网关。**透传优先**、**Provider 描述文件驱动**、**Hold/Capture 预付费账本**。Rust + PostgreSQL。

[![CI](https://github.com/TiamatXu/GateWay/actions/workflows/ci.yml/badge.svg)](https://github.com/TiamatXu/GateWay/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

> ⚠️ **开发中，尚不可用于生产。** 当前进度 M3（句柄与异步托管），身份组织（M8）与管理面（M9）尚未开始。
> 见下方[项目状态](#项目状态)。

---

## 要解决什么

现有 New API / one-api 系网关的五个痛点，根因都是两条架构假设——「OpenAI 协议是强制中间表示」和「网关是无状态中转」：

| 痛点 | 根因 |
|---|---|
| 原生协议支持能力差 | 请求必须先转成 OpenAI 格式再转出去，厂商特有字段被丢弃 |
| 模型价格配置死板 | 倍率连乘模型无法表达开放计费维度 |
| 表达式控制价格复杂不直观 | 用表达式而非数据表达定价规则 |
| 预付费机制不完善 | 「先查余额、后扣费」在并发、流式、异步下必然透支 |
| 厂商原生请求形态难接 | 非 chat 形态（异步提交、资产管理、文件上传）塞不进 OpenAI 管道 |

典型难啃案例：火山引擎的资产 API、阿里云百炼的原生异步端点。

## 怎么解决

**双平面路由，透传优先。** 入站协议 == 出站协议时原生透传，不解析、不重组 body。协议转换从「必经之路」降级为可选插件，厂商特有字段不再丢失。

**Provider 以声明式描述文件接入。** 端点接入写 `providers/*.yaml`，不写 Rust。描述文件只能填值和选算子，算子是有限具名集合——装不下时先考虑加算子，hook 是最后手段（hook 比例 > 5% 即判定为模型缺陷）。

**计价 = 用量向量 + 价格规则表。** 用量向量是开放维度的 `map[维度]数量`；价格规则是纯数据（匹配条件 / 阶梯 / 定价方式 / 生效时间），不是表达式。成本价与售价分离，价格版本快照绑定账单。

**预付费 = Hold / Capture 双分录账本。** 准入时按最坏情况原子冻结，完成时按实际捕获；Hold 带 TTL + 后台回收器，所有账本操作带幂等键，账本 append-only，余额是物化视图。客户端中途断连时按已生成部分正确结算。

**虚拟资源句柄映射。** 上游签发的所有句柄（task_id / file_id / batch_id / asset_id）都由网关重新签发虚拟 ID，记录 `虚拟ID → (渠道, 上游ID, 归属, 预扣单号)`，由此得到渠道亲和与异步任务生命周期托管。

## 接一个端点长什么样

不写 Rust，只描述形态——这是 `providers/openai.yaml` 的片段：

```yaml
endpoints:
  - id: chat_completions
    shape: *json_sse   # 五维：请求 json / 响应 sse / 无句柄 / 请求内计费 / 可安全重试
    route:
      method: POST
      inbound:  /v1/chat/completions
      upstream: /v1/chat/completions
    model: "$.model"
    stream_flag: "$.stream"
    usage: *chat_usage            # 见下
```

用量提取同样是数据。`estimate` 用于准入时预扣，`actual` 读上游权威 usage，`fallback` 只在权威 usage 缺席（客户端中途断连，末帧从未到达）时兜底：

```yaml
chat_usage: &chat_usage
  estimate:
    - { dim: input_tokens, read: "$.messages[*].content", tokenize: o200k_base }
    - { dim: max_output_tokens, read: "$.max_tokens" }
  actual:
    - { dim: input_tokens,  read: "$.usage.prompt_tokens",     accum: last }
    - { dim: output_tokens, read: "$.usage.completion_tokens", accum: last }
    - { dim: cached_input,  read: "$.usage.prompt_tokens_details.cached_tokens", accum: last }
  fallback:
    - { dim: output_tokens, read: "$.choices[*].delta.content", tokenize: o200k_base }
```

已接入：`openai` / `anthropic` / `aliyun` / `volcengine`。描述文件的 JSON Schema 由 Rust 类型生成并提交，二者不一致 CI 即失败。

## 快速开始

需要 Rust stable（edition 2024，MSRV 1.96）与 Docker。

```bash
./scripts/dev-db.sh                 # 起开发库（PostgreSQL 17，端口 5433）
export DATABASE_URL=postgres://postgres:gwdev@localhost:5433/gateway
cargo test --workspace              # 集成测试需要 DATABASE_URL
```

运行网关：

```bash
cp gateway.toml.example gateway.toml
cargo run -p gw-bin
```

所有配置项都可用环境变量覆盖：`GW_` 前缀，嵌套段落用 `__` 分隔（如 `GW_DATABASE_URL`、`GW_LOAD__ENTER_RATIO`）。

## 项目结构

```
crates/
  core       领域类型。仅类型与纯逻辑，不做 IO
  store      全部 SQL、迁移与 Repository 实现
  ledger     Hold / Capture 双分录账本
  pricing    价格引擎：用量向量 → 规则匹配 → 金额
  meter      用量提取：SSE 帧解析、JSONPath 抽取、tokenizer 兜底
  registry   Provider 描述文件的 schema、加载校验与端点目录
  router     路由决策、负载均衡、熔断。健康统计与熔断器保持节点本地
  resource   虚拟句柄映射、渠道亲和与异步任务托管
  proxy      流式反向代理与 tee 旁路
  gateway    数据平面：鉴权、准入、dispatcher、生命周期编排
  identity   组织树、账户链解析、API Key、目录同步与第三方登录
  infra      配置加载与可观测性初始化
  bin        二进制入口（binary 名 gateway）
providers/   Provider 描述文件（接端点改这里）
migrations/  数据库迁移
docs/        设计文档
```

全 workspace `unsafe_code = "forbid"`。

## 项目状态

| 里程碑 | 内容 | 状态 |
|---|---|---|
| M0 | 骨架与契约 | ✅ 2026-08-23 |
| M1 | 账本完整 | ✅ 2026-08-26 |
| M2 | Provider 描述层 | ✅ 2026-08-26 |
| M3 | 句柄、异步托管与智能路由 | 🚧 进行中 |
| M4 | Duplex 实时（WebSocket） | — |
| M5 | 计价完整 | — |
| M6 | 请求归档 | — |
| M7 | 转换适配器 | — |
| M8 | 组织与身份 | — |
| M9 | 管理面 | — |

M3 已完成虚拟句柄签发解析、渠道亲和、异步任务状态机与终态结算、孤儿任务巡检、AK/SK 请求签名（火山引擎 V4 / AWS SigV4）。剩余：火山引擎 `asset_upload` 所需的 `JsonThenBinary` 请求形态与 `Metered` 计费时点、智能路由。

## 设计文档

动手改之前先读对应章节。文档同时记录了**被否决的方案及其理由**（如为何不用 Pingora、为何不用 StarRocks 作唯一后端、为何冻结记录必须落库）。

| 文档 | 内容 |
|---|---|
| [技术选型与工程基线](docs/superpowers/specs/2026-08-22-gateway-tech-stack-design.md) | 痛点与定位、九条架构前提、技术选型、crate 骨架、EndpointShape 五维建模 |
| [身份、组织与计费主体模型](docs/superpowers/specs/2026-08-22-identity-org-billing-model-design.md) | 组织树、账户链、嵌套额度、冻结机制七道防线、目录同步、第三方登录 |
| [里程碑路线图](docs/superpowers/specs/2026-08-22-milestones-roadmap.md) | M0–M9 里程碑、验收标准、风险登记 |
| [M0 骨架与契约](docs/superpowers/specs/2026-08-22-m0-skeleton-contracts-design.md) | 领域类型、trait 签名、透传链路 |
| [M1 账本范围重定义](docs/superpowers/specs/2026-08-26-m1-ledger-scope-design.md) | 账本正确性边界、四项性能优化的否决理由 |
| [M2 描述层设计](docs/superpowers/specs/2026-08-26-m2-descriptor-layer-design.md) | 描述文件结构、算子表、hook 边界、三层校验 |
| [M3 句柄与异步](docs/superpowers/specs/2026-08-26-m3-handle-async-design.md) | 虚拟句柄形态与归属、渠道亲和、异步任务状态机、终态结算 |

## 开发

```bash
cargo clippy --workspace --all-targets -- -D warnings -W clippy::pedantic
cargo sqlx prepare --workspace -- --all-targets              # 改过 SQL 后重新生成
UPDATE_SCHEMA=1 cargo test -p gw-registry --features schema  # 改过描述文件类型后重新生成
```

改动 SQL 或迁移后必须重新生成 `.sqlx/` 并提交，否则 CI 编译不过。

## License

[MIT](LICENSE)
