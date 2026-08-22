-- M0 按最终形态建表，不留后续改结构的债。表建全，M0 只有部分表有数据。

CREATE EXTENSION IF NOT EXISTS ltree;

-- ---------------------------------------------------------------- 组织与身份

CREATE TABLE org_node (
    id             BIGSERIAL PRIMARY KEY,
    uuid           UUID NOT NULL UNIQUE,
    parent_id      BIGINT REFERENCES org_node(id),
    path           LTREE NOT NULL,
    kind           SMALLINT NOT NULL,
    source         SMALLINT NOT NULL,
    provider       TEXT,
    external_id    TEXT,
    name           TEXT NOT NULL,          -- 同步字段，目录同步时覆盖
    name_override  TEXT,                   -- 覆盖层，同步不动
    note           TEXT,
    cost_center    TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at     TIMESTAMPTZ,
    UNIQUE (provider, external_id)
);
CREATE INDEX org_node_path_gist ON org_node USING GIST (path);

CREATE TABLE account (
    id        BIGSERIAL PRIMARY KEY,
    node_id   BIGINT NOT NULL REFERENCES org_node(id),
    tier      TEXT NOT NULL DEFAULT 'default',
    currency  TEXT NOT NULL DEFAULT 'USD'
);
CREATE INDEX account_node_idx ON account (node_id);

CREATE TABLE api_key (
    id            BIGSERIAL PRIMARY KEY,
    node_id       BIGINT NOT NULL REFERENCES org_node(id),
    creator_id    BIGINT,
    hash          BYTEA NOT NULL UNIQUE,
    prefix        TEXT NOT NULL,
    account_chain BIGINT[] NOT NULL,      -- 物化，避免热路径遍历树
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    disabled_at   TIMESTAMPTZ
);

-- ---------------------------------------------------------------------- 账本

CREATE TABLE account_balance (
    account_id BIGINT   NOT NULL REFERENCES account(id),
    shard      SMALLINT NOT NULL,
    balance    BIGINT   NOT NULL DEFAULT 0,   -- 纳单位
    held       BIGINT   NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, shard)
);

-- status：0 = 活跃，1 = 已捕获，2 = 已撤销，3 = 已过期回收。
-- 行不删除：删除会释放 idempotency_key，已结算请求的重试将被当作新请求二次扣费。
CREATE TABLE hold (
    id              UUID PRIMARY KEY,
    amount          BIGINT NOT NULL,
    timing          SMALLINT NOT NULL,
    status          SMALLINT NOT NULL DEFAULT 0,
    idempotency_key TEXT NOT NULL UNIQUE,
    expires_at      TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    settled_at      TIMESTAMPTZ
);
CREATE INDEX hold_expires_idx ON hold (expires_at) WHERE status = 0;

-- 一个 Hold 在账户链上的每一级各占一行。M0 恒为一行，M1 起可多行。
CREATE TABLE hold_leg (
    hold_id    UUID     NOT NULL REFERENCES hold(id) ON DELETE CASCADE,
    account_id BIGINT   NOT NULL REFERENCES account(id),
    shard      SMALLINT NOT NULL,
    amount     BIGINT   NOT NULL,
    depth      SMALLINT NOT NULL,      -- 0 = 链首，主计费主体
    PRIMARY KEY (hold_id, account_id)
);
CREATE INDEX hold_leg_account_idx ON hold_leg (account_id);

CREATE TABLE quota_lease (
    lease_id   UUID PRIMARY KEY,
    account_id BIGINT NOT NULL REFERENCES account(id),
    node_id    TEXT NOT NULL,          -- 持有该租约的网关节点
    amount     BIGINT NOT NULL,
    consumed   BIGINT NOT NULL DEFAULT 0,
    expires_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX quota_lease_expires_idx ON quota_lease (expires_at);

-- append-only。kind：0 = hold，1 = capture，2 = void，3 = topup，4 = transfer，5 = expire
CREATE TABLE ledger_entry (
    id         BIGSERIAL PRIMARY KEY,
    account_id BIGINT NOT NULL REFERENCES account(id),
    kind       SMALLINT NOT NULL,
    amount     BIGINT NOT NULL,
    hold_id    UUID,
    request_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ledger_entry_account_idx ON ledger_entry (account_id, id);
CREATE INDEX ledger_entry_hold_idx ON ledger_entry (hold_id);

-- ------------------------------------------------------------ 渠道与价格

CREATE TABLE channel (
    id         BIGSERIAL PRIMARY KEY,
    provider   TEXT NOT NULL,
    base_url   TEXT NOT NULL,
    credential BYTEA NOT NULL,
    enabled    BOOLEAN NOT NULL DEFAULT true
);

CREATE TABLE price_rule (
    id             BIGSERIAL PRIMARY KEY,
    version        BIGINT NOT NULL,
    match_model    TEXT,
    match_channel  BIGINT,
    match_tier     TEXT,
    match_endpoint TEXT,
    dim            TEXT NOT NULL,
    unit_price     BIGINT NOT NULL,   -- 纳单位 per 百万用量单位
    cost_price     BIGINT NOT NULL,   -- 同上
    effective_from TIMESTAMPTZ NOT NULL
);
CREATE INDEX price_rule_lookup_idx ON price_rule (dim, effective_from DESC);

CREATE TABLE config_version (
    id      INT PRIMARY KEY DEFAULT 1,
    version BIGINT NOT NULL
);

-- ---------------------------------------------------------------- 请求日志

-- LogSink 的 postgres 实现。M0 仅此一种，M9 起可切 ClickHouse / StarRocks。
CREATE TABLE request_log (
    request_id    UUID PRIMARY KEY,
    key_id        BIGINT,
    account_chain BIGINT[] NOT NULL,
    node_path     LTREE,
    channel_id    BIGINT,
    model         TEXT NOT NULL,
    endpoint      TEXT NOT NULL,
    shape         JSONB NOT NULL,
    usage         JSONB NOT NULL,
    quote         JSONB,
    status        SMALLINT NOT NULL,   -- 0 = ok，1 = truncated，2 = failed，3 = rejected
    req_headers   JSONB NOT NULL,      -- 已脱敏
    resp_headers  JSONB NOT NULL,      -- 已脱敏
    archive_ref   TEXT,                -- M6 填充
    started_at    TIMESTAMPTZ NOT NULL,
    ended_at      TIMESTAMPTZ NOT NULL
);
CREATE INDEX request_log_started_idx ON request_log (started_at DESC);
