# M2：Provider 描述层设计

- **日期**：2026-08-26
- **状态**：设计定稿
- **前置**：[M2 前置调研：端点覆盖空白区](2026-08-26-m2-endpoint-coverage-research.md)
- **上游约束**：[技术基线 §8 端点形态建模](2026-08-22-gateway-tech-stack-design.md)、[里程碑路线图 M2](2026-08-22-milestones-roadmap.md)

---

## 1. 目标与非目标

**目标**：让接入一个新端点只需写一份 YAML 声明，0 行 Rust。

调研给出的量化基线是：这份 YAML 要替代同步 chat 厂商的 567–1259 行 Go、异步任务厂商的
174–651 行 Go（中位 ~430）。

**非目标**：

- 不增加 `EndpointShape` 维度。调研已确认五维够用，空白区的难点在描述文件的其余字段。
- 不做跨协议转换的声明式配置。`transform` crate 是另一条线，描述文件只声明端点说哪种协议。
- 不做渠道（channel）配置。渠道是运行期数据、在库里；描述文件是**厂商能力的静态描述**，随二进制发布。
  两者的关系是「渠道引用一个 provider」。

**范围与解释器进度的分离**：schema 一次设计完整，覆盖 M2–M4 全部已知需求（句柄字段映射、
签名鉴权、Session 计费维度）。M2 的解释器只实现同步端点用得到的部分。

未实现的端点是**延期**而非报错：它通过全部三层校验、进入目录可被查询，但**不挂入站路由**，
加载时以 WARN 报出所需里程碑。这样一份 M3 端点的描述文件可以先写出来验证 schema 装得下，
而不必假装它跑得起来——`providers/volcengine.yaml` 与 `providers/aliyun.yaml` 正是这样存在的。

理由：schema 是描述文件作者面对的契约，一旦有真实描述文件写出来，改动代价远高于多写几个字段定义。
而现在调研结论在手、压测对象（火山引擎资产 API）已选定，正是一次设计完整的时机。

---

## 2. 核心决策：描述文件是数据，不是程序

描述文件只能**填值**和**选算子**，不能写表达式。算子是有限具名集合，由 Rust 实现。

```yaml
usage:
  estimate:
    - dim: video_seconds
      read: "$.duration"          # 算子 read
      default: 5                  # 算子 default
    - dim: video_pixels
      read: "$.size"
      map: { "1080p": 2073600, "720p": 921600 }   # 算子 map
```

**否决的方案：受限表达式语言**（`"req.duration ?? 5"`、`"pixels(req.size)"`）。

理由三条：

1. 表达式里的内置函数表一样是有限集合，于是它退化成「算子表 + 一层语法糖」，表达力没有实质增益。
2. 描述文件从「数据」变成「程序」后，JSON Schema 只能校验它是个字符串，语义错误从加载期推到运行期。
   本项目的整个设计取向是把错误往前推（形态可推导、亲和性可推导、schema 可校验），表达式与之相悖。
3. 求值器本身要测试、要防递归、要防超时。这是一个需要长期维护的子系统，而收益只是语法更短。

**代价**：遇到算子表装不下的形式，要么加算子（改 Rust，但一次性，不是每厂商一次），要么走 hook。
这个代价是可接受的——算子的增长曲线应当迅速收敛，若不收敛，说明算子的抽象层次选错了，
这本身就是需要发现的信号。

---

## 3. hook：纯函数字段级算子

```rust
/// hook 是「算子表里没有的那个算子」。无 IO、无状态、无上下文。
pub type OperatorHook = fn(&serde_json::Value) -> Result<serde_json::Value, HookError>;
```

hook 只能出现在**槽位级**，不能出现在端点级：

```yaml
usage:
  estimate:
    - dim: video_pixels
      read: "$.size"
      hook: volc_size_to_pixels     # 单个槽位降级，不拖累整个端点
```

**否决的方案：阶段 hook**（挂在「构造上游请求前 / 改写响应后」，可见完整上下文、可做 IO）。

理由：能做 IO 就能阻塞转发路径，需要超时与并发保护；更重要的是它太好用，会成为绕过算子表
表达力不足的默认选项，从而掩盖模型缺陷——而路线图风险登记明确写着「hook 比例 > 5% 即判定为
模型设计缺陷，回头修正而非加特例」。逃生舱越窄，这个信号越灵敏。

**验收标准的可量化形式**：hook 比例 = 描述文件中 `hook:` 槽位数 / 总可求值槽位数 ≤ 5%。
这个分母是机械可数的，不依赖判断。

### 3.1 需要 IO 的鉴权不是 hook

技术基线 §8.6 举的逃生舱例子里有「需预先换取 token 的鉴权流程」。这一类**不走 hook**，
而是 `CredentialProvider` 的一个有限枚举变体。

理由：OAuth client_credentials、STS 临时凭证的本质是**渠道级、带缓存与刷新的凭证获取**，
生命周期跨请求。把它塞进无状态 hook 里，缓存无处安放。

行业实践一致：OpenAPI 3.1 的 `securitySchemes` 是 `apiKey | http | mutualTLS | oauth2 |
openIdConnect` 有限枚举加参数；AWS service model 用 `"signatureVersion": "v4"` 声明签名算法；
Envoy 把 SigV4 签名与 OAuth2 做成各自专用的 filter 而非通用扩展点。没有人把这类东西做成通用插件。

---

## 4. 描述文件结构

一个 provider 一个文件，`providers/<provider>.yaml`。

```yaml
schema_version: 1
provider: openai

defaults:                          # provider 级默认值，任意端点可覆盖
  base_url: https://api.openai.com
  protocol: openai_chat
  auth:
    credential: static             # 凭证怎么来
    inject:                        # 凭证怎么用
      header:
        name: authorization
        template: "Bearer {credential}"

# 锚点存放处。内容不参与解析——`deny_unknown_fields` 下锚点需要一个合法落脚点，
# 而未知键一律报错这条不能为了锚点放弃：拼错字段名必须当场失败。
anchors:
  json_sse: &json_sse
    request: json
    response: sse
    handle: none
    billing: in_request
    retry: safe

endpoints:
  - id: chat_completions
    shape:
      request: json
      response: sse
      handle: none
      billing: in_request
      retry: safe
    route:
      method: POST
      inbound:  /v1/chat/completions      # 客户端看到的路径
      upstream: /v1/chat/completions      # 厂商的路径
    model: "$.model"                      # 路由用的 model 在哪（取值位置，见 §4.3）
    stream_flag: "$.stream"               # 客户端要求流式的标志位在哪
    usage:
      actual:
        - { dim: input_tokens,  read: "$.usage.prompt_tokens",     accum: last }
        - { dim: output_tokens, read: "$.usage.completion_tokens", accum: last }
      fallback:                           # 上游未返回权威 usage 时的第 3 档兜底
        - { dim: output_tokens, read: "$.choices[*].delta.content", tokenize: o200k_base }
```

### 4.1 `route`：入站与上游分离

调研 §3.4 要求 base_url 能按端点覆盖（火山引擎资产 API 与 chat 端点不同 host）。因此
`base_url` 既可在 `defaults` 声明，也可在 `route` 覆盖：

```yaml
    route:
      method: POST
      base_url: https://open.volcengineapi.com    # 覆盖 provider 默认
      inbound:  /volc/assets/upload
      upstream: /
```

`inbound` 与 `upstream` 分开写，而不是「上游路径 = 入站路径」。透传优先架构下两者绝大多数
时候相同，但资产 API、阿里云百炼这类端点必须能分离。路径模板参数用 `{name}` 声明，
两侧同名参数自动传递：`inbound: /v1/files/{file_id}` → `upstream: /v1/files/{file_id}`。

### 4.2 `auth`：获取与附加两段

```yaml
auth:
  credential: static                        # 渠道凭证原样使用
  credential:                               # 或：先换 token，渠道级缓存
    oauth2_client_credentials:
      token_url: https://.../oauth/token
      scope: [model.inference]
      cache_ttl_secs: 3300
  inject:
    header: { name: authorization, template: "Bearer {credential}" }
  inject:
    query:  { name: key }
  inject:
    sign:   { alg: volc_v4, service: cv, region: cn-beijing }   # 签名算法有限枚举
```

`credential`（可能做 IO、有缓存、渠道级）与 `inject`（纯函数、请求级）正交。
签名算法是 `inject` 的一个变体而非独立概念——签名的本质就是「把凭证附加到请求上」，
只是附加方式复杂。

M2 实现 `static` + `header`/`query`。`oauth2_client_credentials` 与 `sign` 在 schema 里
定义完整，解释器加载时报 `需要 M3`。

### 4.3 `handles`：一响应多字段，各自类型

调研 §3.3 指出 LiteLLM 的硬编码表要解决的是「一个响应里多个字段是句柄、各是不同类型」
（batch 一次返回 4 个）。

```yaml
    handles:
      issue:                                    # 本端点签发的句柄
        - { at: "$.id",              kind: batch }
        - { at: "$.input_file_id",   kind: file }
        - { at: "$.output_file_id",  kind: file }
        - { at: "$.error_file_id",   kind: file }
      consume:                                  # 请求里引用的句柄，需换回上游 ID
        - { at: "path.batch_id",     kind: batch }
        - { at: "$.asset_id",        kind: asset }   # 请求体内任意位置
```

**与 `shape.handle` 的关系**（这一点容易误解，明确记下）：

`EndpointShape.handle` 是 `HandleRole::Issues(HandleKind)`，单个。它是**粗粒度维度**，
只用于推导渠道亲和性与 Hold 的挂载对象，声明的是「本端点与句柄体系的主关系」。
`handles.issue/consume` 是**细粒度字段映射**，声明「具体哪些字段、各是什么类型」。

两者不冲突，也不重复：前者回答「这个端点要不要回原渠道」，后者回答「响应里哪几个串要重写」。
调研说的「五维不需要增加维度」在此成立——多字段是描述文件的事，不是形态的事。

加载期校验：`handles.issue` 中至少一项的 `kind` 必须与 `shape.handle` 的 `Issues(k)` 一致，
否则是声明矛盾。

`at` 的语法：`$....` 是响应/请求体的 JSONPath；`path.<name>` 是路径模板参数；
`header.<name>` 是头字段。三种前缀构成一个封闭集合。

### 4.4 `async`：提交与轮询显式关联

调研 §3.5 明确：不能假设「poll 路径 = submit 路径 + /{id}」。阿里云百炼的轮询端点是全局
统一的 `GET /api/v1/tasks/{task_id}`，与提交路径没有构造关系。

因此提交与轮询是**各自独立声明的两个端点**，在提交端点上显式引用：

```yaml
  - id: video_generation_submit
    shape: { request: json, response: json, handle: { issues: task },
             billing: on_terminal, retry: idempotent_with_key }
    async:
      poll:   video_task_query        # 端点 id，加载期校验存在且形态匹配
      cancel: video_task_cancel
      state:  { read: "$.output.task_status" }
      terminal:
        succeeded: [SUCCEEDED]
        failed:    [FAILED, CANCELED, UNKNOWN]
```

**否决的方案：靠 `HandleKind` 隐式匹配**（找同 provider 下 `Consumes(Task)` 的端点）。
一个 provider 有两个互不相干的任务族时立刻歧义，且歧义是静默的。Smithy 的 `@waitable`
同样显式引用 operation 名。

加载期校验：`poll` 指向的端点必须存在、必须是 `Consumes(Task)`；`cancel` 必须是
`Terminates(Task)`。

### 4.5 `usage`：三个时点，三组规则

调研 §3.2 记录 new-api 的三段式计费与本项目 `Metered` 的 Hold 生命周期同构。落到描述文件：

| 键 | 读哪个文档 | 对应账本动作 |
|---|---|---|
| `estimate` | 请求体 | `hold` 的预扣金额 |
| `on_submit` | 提交响应体 | `extend` / `capture_partial` 调整 |
| `actual` | 终态响应体（或流式聚合结果） | `capture` |
| `fallback` | 响应体文本字段 | 上游无权威 usage 时的第 3 档估算，标记 `estimated` |

`estimate` 除了预扣用的用量维度，还负责提供 `max_output_tokens`——客户端声明的输出上限。
它不是账单维度，但是预扣估算的输入。放进 `estimate` 规则而非数据平面硬取 `max_tokens` 字段，
是因为它的字段名各协议不同（OpenAI 有 `max_tokens` 与 `max_completion_tokens` 两个别名，
Responses 用 `max_output_tokens`）。同维度写两条规则、后命中者生效，即可覆盖别名，
无需在主干代码里写 `or_else` 分支。

**否决的方案：一组规则加时点标记**。三个时点读的是**不同的文档**（请求体 / 提交响应 /
终态响应），字段路径完全不同，共用一组规则只会让每条规则都得写清楚自己读哪个文档——
那就是三组规则，只是写法更绕。

### 4.6 算子表

有限具名集合。每个槽位是「一次取值」，可组合的算子按固定顺序求值：

| 算子 | 作用 | 求值顺序 |
|---|---|---|
| `read` | 按 JSONPath 从来源文档取值 | 1 |
| `hook` | 交给具名纯函数（逃生舱） | 1（与 `read` 互斥） |
| `map` | 查表替换 | 2 |
| `scale` | 乘常数系数（单位换算，如秒 → 毫秒） | 3 |
| `default` | 前序步骤取不到值时的兜底 | 4 |
| `accum` | 同一维度多次命中的累积方式：`last`/`sum` | 流式聚合时 |
| `tokenize` | 文本字段按 tokenizer 计数，路径可命中多个节点、逐个求和 | 替代 1–4 |

`tokenize` 在任一时点都可用，不限于 `fallback`：`estimate` 里读请求体文本估算输入 token，
用的就是它。唯一约束是必须配 `read`——它需要一条指向文本的路径，hook 的返回值不是文本来源。

固定顺序而非可编排管道：可编排就是表达式语言的另一种写法，回到 §2 已否决的路径。
实测中若发现顺序不够用，加算子或调整顺序，不引入编排。

---

## 5. JSONPath：用完整的，不自造方言

`serde_json_path`（RFC 9535）已是 workspace 依赖，`meter` 已在用。描述文件沿用它。

**否决的方案：受限路径语法**（只支持 `a.b`、`items[*]`，即 LiteLLM 手写 JSONPath 的形式）。

理由：受限语法的表达力上限无法预知，而 JSONPath 是 RFC、有现成实现、描述文件作者大概率已经会。
Kubernetes 的 `additionalPrinterColumns.jsonPath`、AWS CLI 的 `--query`（JMESPath）都是这个取向。
「给作者太多绳子」的担忧由**加载期编译**化解：所有 JSONPath 在加载时解析并编译，语法错误、
以及在需要单值的位置写了多值查询，都在加载期报错，不会到运行期才炸。

---

## 6. 加载、校验与热更新

### 6.1 三层校验

| 层 | 时机 | 查什么 |
|---|---|---|
| 语法 | YAML 解析 | 文件是否合法 YAML |
| 结构 | serde 反序列化 + JSON Schema | 字段是否合 schema、枚举值是否合法 |
| 语义 | 加载期 | `async.poll` 引用存在且形态匹配、`handles` 与 `shape.handle` 不矛盾、上游模板参数是入站路径的子集、hook 名已注册、入站契约无冲突 |

语义校验是这一层的价值所在——JSON Schema 查不出「引用了不存在的端点」。

JSONPath 与取值位置的解析放在 `Deserialize` 里而非事后校验，因此语法错误落在结构层，
能拿到 YAML 的行列位置。

**入站契约冲突**：同一 `(method, inbound)` 可由多个 provider 承接（`/v1/chat/completions`
上 openai 与 azure 并存），但入站面属性——形态、协议、`model` 与 `stream_flag` 的位置——
属于客户端契约，必须一致，否则加载期报错并指出差在哪个字段。注意未显式声明 `protocol`
的 provider 会退化为 `Native(自己)`，因而天然冲突：要共享一条入站路径，双方都得声明同一个规范协议。

### 6.2 JSON Schema 由 Rust 类型生成

用 `schemars` 从 serde 类型生成 `providers/provider.schema.json` 并提交入库，
CI 校验其与类型定义一致（与 `.sqlx/` 离线元数据同一套约定）。

收益：描述文件作者在编辑器里就有补全与校验，不必先跑网关才知道字段写错。
手写 JSON Schema 会与 Rust 类型漂移，这是必然发生的事。

### 6.3 热更新

描述文件随二进制发布，但支持不重启重载（`SIGHUP` 或文件监听）。重载是**原子替换**：
新快照全部校验通过才切换，任一文件出错则保留旧快照并告警。`snapshot_version` 单调递增，
在途请求绑定发起时的快照，不受中途重载影响。

---

## 7. crate 边界

依赖方向已在技术基线 §6.2 定死：`registry` 被 `router`、`meter`、`pricing` 依赖，
因此 `registry` **不能**依赖 `meter`。

这决定了职责切分：

- `registry`：描述文件的**声明**——解析、校验、持有编译后的规则数据（含已编译的 `JsonPath`）
- `meter`：把 `registry` 给的用量规则**变成抽取器**（`UsageSpec` / `UsageExtractor`）
- `gateway`：按 `EndpointShape` 编排流水线，从 `registry` 取绑定

`registry` 只输出数据与已编译的查询对象，不输出行为。算子的求值实现放在 `registry`
（纯函数、无 IO、不依赖上层），hook 注册表同理。

---

## 8. 数据平面的改造点

M2 要拆掉 `crates/gateway/src/app.rs` 里的三处硬编码：

| 现状 | 改为 |
|---|---|
| `protocol_of()` 按路径前缀判断协议 | 描述文件的 `route.inbound` 匹配得到端点，协议来自端点声明 |
| `USAGE_SPEC` / `INPUT_SPEC` 全局常量 | 端点绑定里的 `usage` 规则编译而来 |
| 固定 `BillingTiming::InRequest` | 端点的 `shape.billing` |
| 从 body 里硬取 `model` / `stream` / `max_tokens` | 端点的 `model.read` 与协议声明 |

改造后主干代码无厂商分支。这是 M2「首批厂商全部改为描述文件驱动」的具体含义。

---

## 9. YAML 库实测

路线图要求用真实描述文件验证 `serde-saphyr`（merge key、嵌套枚举、锚点引用），
不满足则回退 `yaml-rust2`。

实测项固定为四条，用 M2 的真实描述文件跑（`crates/registry/tests/yaml_probe.rs`）：

| 实测项 | 结果 |
|---|---|
| 锚点与引用（`&x` / `*x`）——多端点共享 usage 规则块 | ✅ |
| merge key（`<<:`）——端点继承 provider 默认值 | ✅ |
| 嵌套枚举——`handle: { issues: task }` 这种 externally-tagged 形式 | ✅ |
| 错误信息质量——能否指到出错的行列与字段 | ✅ rustc 风格，带列指示 |

第 4 条是硬要求：描述文件是给人写的，报错说不清位置等于没有校验。实测输出：

```
error: line 8 column 16: unknown variant `not_a_real_timing`,
       expected one of in_request, on_terminal, metered, session, not_billed
8 |       billing: not_a_real_timing
  |                ^
```

**结论：采用 `serde-saphyr` 1.1，不启用 `yaml-rust2` 回退路径。**

---

## 10. 验收标准

1. 新增一个端点需要编写 0 行 Rust
2. hook 槽位数 / 总可求值槽位数 ≤ 5%
3. 首批厂商全部由描述文件驱动，`gateway` 主干无厂商分支
4. **表达力压测**：火山引擎资产 API 的描述文件能写出来并通过全部三层校验
   （运行时属 M3，M2 只验证 schema 装得下）

第 4 条是调研给出的靶子——三家同类网关全部不支持，且同时踩中不同 host、签名鉴权、
跨端点句柄引用、二进制上传四个难点。schema 若装不下它，说明表达力设计不足。

---

## 11. 已否决方案汇总

| 方案 | 否决理由 |
|---|---|
| 描述文件内写受限表达式 | 退化为算子表加语法糖；错误从加载期推到运行期；求值器需长期维护 |
| 阶段 hook（可见全上下文、可做 IO） | 会成为绕过表达力不足的默认选项，掩盖模型缺陷；IO 阻塞转发路径 |
| 把 OAuth/STS 换取 token 做成 hook | 需要渠道级缓存与刷新，无状态 hook 装不下；行业均作专用机制 |
| 受限路径语法替代 JSONPath | 表达力上限不可预知；JSONPath 是 RFC 且作者已会；加载期编译已化解误用风险 |
| 靠 `HandleKind` 隐式关联提交与轮询端点 | 一个 provider 有两个任务族时静默歧义 |
| 用量规则一组加时点标记 | 三个时点读不同文档，共用一组规则只会更绕 |
| 未实现端点在加载期硬报错 | 会让 M3 端点的描述文件无法先写出来验证表达力，而那正是 M2 的验收项 4 |
| 描述文件顶层允许任意 `x-` 扩展键 | 为了放锚点而放弃 `deny_unknown_fields`，代价是拼错字段名静默通过；改为单个 `anchors` 键 |
| 手写 JSON Schema | 与 Rust 类型必然漂移 |
| 可编排算子管道 | 编排即表达式语言的另一种写法 |
