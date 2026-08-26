-- M3 §4.3/§4.4：虚拟句柄映射与异步任务托管。
-- 低 QPS，落 PostgreSQL；kind/state 用 TEXT 而非 SMALLINT——
-- 它们的取值来自描述文件，运维直接读表时不该再查一份编码表。

-- 上游签发的每个句柄都换成网关自己的虚拟 ID 对外暴露。
-- 记录归属，是为了防止 A 用户猜到 B 用户的上游 ID 后经由同一渠道凭证去查。
CREATE TABLE resource_handle (
    id            UUID PRIMARY KEY,
    kind          TEXT   NOT NULL,          -- HandleKind 的 snake_case 名
    channel_id    BIGINT NOT NULL REFERENCES channel(id),
    provider      TEXT   NOT NULL,
    upstream_id   TEXT   NOT NULL,
    account_chain BIGINT[] NOT NULL,        -- 归属，链首为主计费主体
    key_id        BIGINT,
    endpoint      TEXT   NOT NULL,          -- 签发它的端点 id
    hold_id       UUID,                     -- OnTerminal 的预扣单号，终态时按它结算
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- 同一渠道上同一个上游 ID 只签发一次：提交重试拿到相同上游 ID 时，
    -- 客户端必须看到同一个虚拟 ID，否则同一资源会有两个句柄、两笔预扣。
    UNIQUE (channel_id, kind, upstream_id)
);
CREATE INDEX resource_handle_chain_idx ON resource_handle USING GIN (account_chain);

-- phase：0 = 运行中，1 = 成功，2 = 失败。
-- 终态结算靠 `WHERE phase = 0` 的条件更新抢占，避免并发轮询重复 capture。
CREATE TABLE async_task (
    handle_id       UUID PRIMARY KEY REFERENCES resource_handle(id) ON DELETE CASCADE,
    submit_endpoint TEXT NOT NULL,          -- 提交端点 id：async 声明与 usage.actual 规则都在它身上
    inbound         TEXT NOT NULL,          -- 提交端点的入站路径，计价与日志用
    model           TEXT NOT NULL,
    request_id      UUID NOT NULL,          -- 提交请求的 request_id
    phase           SMALLINT NOT NULL DEFAULT 0,
    state           TEXT,                   -- 上游原始状态串，不做归一化
    polled_at       TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    settled_at      TIMESTAMPTZ
);
-- 孤儿巡检按此扫描：提交后再没人查询的任务
CREATE INDEX async_task_open_idx ON async_task (created_at) WHERE phase = 0;

-- 一次异步任务产生两行日志（提交与终态结算），靠句柄串起来
ALTER TABLE request_log ADD COLUMN handle_id UUID;
CREATE INDEX request_log_handle_idx ON request_log (handle_id) WHERE handle_id IS NOT NULL;
