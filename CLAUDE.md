# GateWay

AI 网关。透传优先、Provider 描述文件驱动、Hold/Capture 预付费账本。Rust + PostgreSQL。

## 动手前先读设计文档

`docs/superpowers/specs/`：

| 文档 | 内容 |
|---|---|
| `2026-08-22-gateway-tech-stack-design.md` | 痛点与定位、九条架构前提、技术选型、crate 骨架、EndpointShape 五维建模 |
| `2026-08-22-identity-org-billing-model-design.md` | 组织树、账户链、嵌套额度、冻结机制七道防线、目录同步、第三方登录 |
| `2026-08-22-milestones-roadmap.md` | M0–M8 里程碑、验收标准、风险登记 |

文档同时记录了**被否决方案及其理由**（如为何不用 Pingora、为何不用 StarRocks 作唯一后端、为何冻结记录必须落库）。改动任何已定决策前，先读对应章节。

## 工程规约

- 全 workspace `unsafe_code = "forbid"`
- 依赖版本统一在 `[workspace.dependencies]` 声明，子 crate 只写 `.workspace = true`
- crate 依赖方向严格单向，见技术基线 §6.2；`core` 保持极瘦
- 最简实现必须标注 `// SIMPLIFIED(Mx):` 并登记在该里程碑清单中
- 提交 `.sqlx/` 离线元数据

## 第三方库外迁规则

**默认在 workspace 内建 crate，不新建独立仓库。**

同时满足以下三条才外迁：

1. 接口连续三个月无破坏性变更
2. 不依赖本项目任何业务类型
3. 有真实外部需求（他人询问，或自己在其他项目需要）

理由：模块化收益在 workspace 内已经拿到；独立仓库额外带来的只有"可被外部依赖"，代价是跨仓库改动无法原子提交、双份版本与 CI 维护，单人项目里这个成本每天都在付。

当前候选（暂不外迁）：SSE tee、Hold/Capture 账本、SCIM 2.0 server、sqlx 的 ltree 类型支持。
