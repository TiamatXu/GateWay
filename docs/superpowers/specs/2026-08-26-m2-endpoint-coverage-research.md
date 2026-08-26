# M2 前置调研：端点覆盖空白区

- **日期**：2026-08-26
- **状态**：已完成
- **用途**：作为 Provider 描述层表达力设计的输入（路线图 M2 的前置项）
- **方法**：读三个同类网关的实际代码，而非文档；盘点它们**做不到**或**只能靠写代码做到**的端点

---

## 1. 结论先行

三家网关（LiteLLM、new-api、TokenHub）的端点覆盖有一条共同的分界线：

**同步 chat 类端点建模完善；一旦离开这条线，全部退化为「通配透传」或「每厂商手写适配器」。**

而通配透传的代价是丢掉计费、句柄映射与预扣——恰好是本项目的三个核心能力。这印证了 M2 的定位：描述层要解决的不是「支持更多厂商」，而是**让非 chat 端点也能进入计费与句柄体系，且不写代码**。

---

## 2. 覆盖对比

| 端点族 | LiteLLM | new-api | TokenHub | 接入方式 |
|---|---|---|---|---|
| chat / completions | ✅ | ✅ | ✅ | 每厂商适配器 |
| embeddings / rerank | ✅ | ✅ | ✅ | 每厂商适配器 |
| images 生成/编辑 | ✅ | ✅ | ✅ | 每厂商适配器 |
| audio 转写/合成 | ✅ | ✅ | ❌ | 每厂商适配器 |
| files / batches | ✅ | ✅ | ❌ | 每厂商适配器 |
| fine-tuning | ✅ | ✅ | ❌ | 每厂商适配器 |
| responses（含 cancel） | ✅ | ✅ | ✅ | 每厂商适配器 |
| realtime（WebSocket） | ✅ | ✅ | ❌ | 每厂商适配器 |
| 异步任务（视频等） | 部分 | ✅ 11 家 | 仅图像 | **每厂商 Go 适配器** |
| vector stores / containers / OCR | ✅ | ❌ | ❌ | 仅 OpenAI |
| 厂商私有端点 | **通配透传** | 部分 | ❌ | 放弃建模 |
| 资产库（asset://） | ❌ | ❌ | ❌ | **无人支持** |

---

## 3. 空白区盘点

### 3.1 通配透传：放弃建模的自认

LiteLLM 维护一张 `mapped_pass_through_routes`（`litellm/proxy/_types.py`）：

```python
mapped_pass_through_routes = [
    "/bedrock", "/vertex-ai", "/vertex_ai", "/cohere", "/gemini",
    "/anthropic", "/azure", "/azure_ai", "/openai", "/assemblyai",
    "/vllm", "/mistral", "/milvus", "/watsonx", ...
]
```

凡是命中这些前缀的路径，一律 `{endpoint:path}` 原样转发。它们自己的架构文档写得很清楚，透传时只做两件必要的事——构造 URL、替换鉴权头——**其余一概不碰**。

代价是三条能力同时失效：

1. **计费退化为事后尽力而为**。用量靠 `jsonpath_extractor.py` 从响应里抽，抽不到就没账。该模块是手写的简易 JSONPath，只支持 `a.b`、`items[*].text` 两种形式。
2. **无预扣**。透传路径上没有任何准入时的额度冻结，超支只能事后发现。
3. **句柄不受管**。透传响应里的上游 ID 原样返回给客户端（除非命中下面 3.3 的硬编码表）。

**对 M2 的含义**：通配透传是描述层的失败模式。描述文件必须能表达这些端点，否则我们只是换个语言重复同样的妥协。

### 3.2 异步任务：必然写代码，且不便宜

new-api 是三家里异步任务做得最全的，抽象是一个 **Go interface**（`relay/channel/adapter.go`）：

```go
type TaskAdaptor interface {
    ValidateRequestAndSetAction(...)
    EstimateBilling(...) map[string]float64        // 预扣估算
    AdjustBillingOnSubmit(...) map[string]float64  // 提交后按上游实参调整
    AdjustBillingOnComplete(...) int               // 终态结算
    BuildRequestURL(...) / BuildRequestHeader(...) / BuildRequestBody(...)
    DoRequest(...) / DoResponse(...) / ParseTaskResult(...)
    ...
}
```

每个厂商实现一遍。实测代码量：

| 厂商 | 行数 | 厂商 | 行数 |
|---|---|---|---|
| ali | 651 | jimeng | 481 |
| gemini | 606 | doubao | 428 |
| hailuo | 525 | vertex | 417 |
| kling | 418 | sora | 339 |
| vidu | 301 | suno | 174 |

**中位数约 430 行 Go / 厂商**，共 11 家。作为对照，同步 chat 适配器 567–1259 行（ali 1035、volcengine 1259、gemini 1191、claude 567）。

**对 M2 的含义**：这是「新增一个端点 0 行 Rust」这条验收标准的价值基线。若描述文件替代不了这 430 行，M2 就没有达到目的。

值得借鉴的一点：new-api 的三段式计费（估算 → 提交后调整 → 终态结算）与本项目 `Metered` 的 `Hold → extend/capture_partial → capture` 同构，且它的 ratios 是 `map[string]float64`（如 `{"seconds": 5, "size": 1.666}`），与 `UsageVector` 的开放维度一致。这条设计路径已被独立验证过一次。

### 3.3 句柄映射：硬编码表，覆盖面极窄

LiteLLM 的 `managed_id_rewriter.py`（1261 行）负责把上游 ID 换成自己签发的托管 ID。映射表 `BUILTIN_OUTPUT_ID_FIELD_MAP` 是写死的 Python 字面量：

```python
("openai", "POST", "/v1/files"):   [("id", "file-")],
("openai", "POST", "/v1/batches"): [("id", "batch_"), ("input_file_id", "file-"),
                                    ("output_file_id", "file-"), ("error_file_id", "file-")],
("openai", "POST", "/v1/responses"): [("id", "resp_")],
("azure", ...): 同上
```

**只有 openai 与 azure 两个厂商，只有 file / batch / response 三种资源。** 没有 task、asset、cache。加一个厂商或一种资源都要改 Python。

它做对的地方值得记下：入站方向对托管 ID 做三重校验——跨路由检查（ID 里编码的 provider 必须与当前路由一致）、存在性检查（伪造 ID 一律 404，且**原始串绝不转发上游**）、归属检查（403）。这三条本项目 M3 应当照做。

**对 M2 的含义**：`HandleRole::Issues/Consumes/Terminates` + `HandleKind` 七种，正是要把这张硬编码表变成描述文件里的声明。表达力需求是明确的：**要能声明「响应体的哪些字段是句柄，各是什么类型」**——注意是复数字段（batch 一次返回 4 个），且分布在 body 的不同路径上。

### 3.4 资产库：三家全部不支持

火山引擎 Seedance 2.0 的私域素材库（虚拟人像 / 真人人脸）要求先把素材入库为「可信资产」，拿到 `asset://` ID，再在视频生成请求里引用。素材库管理（建组 / 上传 / 查询 / 删除）**走原生接口，通过 AK/SK 签名直连 `open.volcengineapi.com`**。

这个端点族同时踩中四个难点：

1. **与同厂商 chat 端点不同 host**（`ark.cn-beijing.volces.com` vs `open.volcengineapi.com`）
2. **不同鉴权方式**（AK/SK 请求签名，而非静态 Bearer）
3. **签发的句柄要在另一个端点的请求体里被解析**（`asset://` 出现在视频生成的 body 中）
4. **上传是二进制**

这正是路线图把「火山引擎资产 API」列为 M3 验收标准的原因。三家都不支持，是真空白区。

### 3.5 轮询端点不与提交端点共享路径族

阿里云百炼的异步任务：提交是 `POST /api/v1/services/{domain}/{task}/{model}` 并带 `X-DashScope-Async: enable` 头，轮询却是**全局统一的** `GET /api/v1/tasks/{task_id}`——所有异步任务共用一个查询端点，与提交路径没有任何构造关系。

**对 M2 的含义**：描述文件不能假设「poll 路径 = submit 路径 + /{id}」这种就近推导。提交端点与轮询端点必须是**各自独立声明的两个端点**，通过 `HandleKind::Task` 关联，而非通过路径模板关联。

---

## 4. 对 `EndpointShape` 现有定义的检验

拿空白区逐条比对当前 `core` 里的五维定义（`crates/core/src/shape.rs`）：

| 空白区 | 现有维度是否够用 |
|---|---|
| 异步提交 → 轮询 | ✅ `HandleRole::Issues(Task)` / `Consumes(Task)` |
| 视频/资产下载 | ✅ `ResponseForm::Binary` |
| 资产上传 | ✅ `RequestForm::Multipart` / `Binary` / `JsonThenBinary` |
| 实时会话 | ✅ `ResponseForm::Duplex` |
| 按量长任务计费 | ✅ `BillingTiming::Metered`，M1 已有 `RollingHold` |
| 轮询端点独立声明 | ✅ 五维不涉及路径，路径本就在描述文件里 |

**五维不需要增加维度。** 空白区的难点不在「形态」，而在描述文件的**其余字段**，这些是 M2 设计要重点解决的：

1. **base_url 必须可按端点覆盖**，不能只在 provider 级声明一次（§3.4 的不同 host）
2. **鉴权方式必须支持请求签名**，不止静态头注入（§3.4 的 AK/SK）
3. **句柄声明要支持一个响应里的多个字段、各自不同类型**（§3.3 的 batch 四字段）
4. **句柄要能在请求体的任意位置被解析回上游 ID**，不止路径参数（§3.4 的 `asset://` 出现在 body 里）
5. **用量提取要支持多维度、且区分「提交时估算」与「终态实际」两个时点**（§3.2 的三段式计费）

---

## 5. M2 验收标准的量化基线

路线图原文：「新增一个端点需要编写 **0 行 Rust**；hook 使用比例不超过 5%」。

据本次调研，这条标准要替代的是：

- 同步 chat 厂商：567–1259 行 Go
- 异步任务厂商：174–651 行 Go（中位 ~430）

建议在 M2 结束时用**火山引擎资产 API**（§3.4）作为表达力的压力测试对象——它是三家全部不支持、且同时踩中四个难点的端点族。若描述文件能表达它，M2 的表达力设计就是够的；若必须靠 hook，正好检验「hook 比例不超过 5%」这条约束是否现实。

---

## 6. 待 M2 设计阶段决策的问题

1. 鉴权方式做成有限枚举（bearer / header / query / aksk-sign / oauth）还是可扩展？签名算法各厂商不同，是否必须落到 hook？
2. 句柄字段声明用 JSONPath 还是更受限的路径语法？`serde_json_path` 已是 workspace 依赖，表达力远超 LiteLLM 的手写实现，但完整 JSONPath 会不会给描述文件作者太多绳子？
3. 提交端点与轮询端点的关联方式：显式引用端点名，还是靠 `HandleKind` 隐式匹配？
4. 用量提取的「估算」与「实际」两个时点，在描述文件里是两组规则还是一组规则加时点标记？

---

## 参考来源

- LiteLLM 源码：`litellm/proxy/_types.py`、`litellm/proxy/pass_through_endpoints/`（architecture.md、jsonpath_extractor.py、managed_id_rewriter.py）
- new-api 源码：`relay/channel/adapter.go`、`relay/channel/task/*`、`router/relay-router.go`
- TokenHub 源码：`backend/internal/server/*`、`backend/internal/billing/`
- [如何查询和取消异步任务？— 阿里云百炼](https://help.aliyun.com/zh/model-studio/manage-asynchronous-tasks)
- [异步调用 API 参考 — 阿里云百炼](https://help.aliyun.com/zh/model-studio/asynchronous-call-api-reference)
- [创建视频生成任务 — 火山方舟](https://docs.volcengine.com/docs/82379/1520757)
- [查询视频生成任务 — 火山方舟](https://www.volcengine.com/docs/82379/1521309)
- [Seedance 2.0 私域素材库](https://docs.apiyi.com/api-capabilities/seedance2/asset-library)
