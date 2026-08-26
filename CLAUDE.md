# GateWay

AI 网关。透传优先、Provider 描述文件驱动、Hold/Capture 预付费账本。Rust + PostgreSQL。

## 动手前先读设计文档

`docs/superpowers/specs/`：

| 文档 | 内容 |
|---|---|
| `2026-08-22-gateway-tech-stack-design.md` | 痛点与定位、九条架构前提、技术选型、crate 骨架、EndpointShape 五维建模 |
| `2026-08-22-identity-org-billing-model-design.md` | 组织树、账户链、嵌套额度、冻结机制七道防线、目录同步、第三方登录 |
| `2026-08-22-milestones-roadmap.md` | M0–M8 里程碑、验收标准、风险登记 |
| `2026-08-26-m2-descriptor-layer-design.md` | 描述文件结构、算子表、hook 边界、三层校验 |
| `2026-08-26-m3-handle-async-design.md` | 虚拟句柄形态与归属、渠道亲和、异步任务状态机、终态结算 |

文档同时记录了**被否决方案及其理由**（如为何不用 Pingora、为何不用 StarRocks 作唯一后端、为何冻结记录必须落库）。改动任何已定决策前，先读对应章节。

## 开发环境

```bash
./scripts/dev-db.sh                 # 起开发库（Docker，端口 5433）
export DATABASE_URL=postgres://postgres:gwdev@localhost:5433/gateway
cargo test --workspace              # 集成测试需要 DATABASE_URL
cargo clippy --workspace --all-targets -- -D warnings -W clippy::pedantic
cargo sqlx prepare --workspace -- --all-targets   # 改过 SQL 后重新生成 .sqlx
UPDATE_SCHEMA=1 cargo test -p gw-registry --features schema   # 改过描述文件类型后重新生成 JSON Schema
```

改动 SQL 或迁移后必须重新生成 `.sqlx/` 并提交，否则 CI 编译不过。

## 工程规约

- 全 workspace `unsafe_code = "forbid"`
- 依赖版本统一在 `[workspace.dependencies]` 声明，子 crate 只写 `.workspace = true`
- crate 依赖方向严格单向，见技术基线 §6.2；`core` 保持极瘦
- 最简实现必须标注 `// SIMPLIFIED(Mx):` 并登记在该里程碑清单中
- 提交 `.sqlx/` 离线元数据与 `providers/provider.schema.json`，二者都由 CI 校验一致
- 端点接入写 `providers/*.yaml`，不写 Rust。描述文件只能填值和选算子，
  算子是有限具名集合；装不下时先考虑加算子，hook 是最后手段（比例 > 5% 即判定为模型缺陷）
- 直接在 `master` 上开发提交，不开特性分支（单人无远程，分支只带来合并开销）

## 第三方库外迁规则

**默认在 workspace 内建 crate，不新建独立仓库。**

同时满足以下三条才外迁：

1. 接口连续三个月无破坏性变更
2. 不依赖本项目任何业务类型
3. 有真实外部需求（他人询问，或自己在其他项目需要）

理由：模块化收益在 workspace 内已经拿到；独立仓库额外带来的只有"可被外部依赖"，代价是跨仓库改动无法原子提交、双份版本与 CI 维护，单人项目里这个成本每天都在付。

当前候选（暂不外迁）：SSE tee、Hold/Capture 账本、SCIM 2.0 server、sqlx 的 ltree 类型支持。
