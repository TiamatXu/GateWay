#!/usr/bin/env bash
# 起一个本地开发/测试用 PostgreSQL。sqlx 的 query! 宏需要编译期连库校验，
# 集成测试也连这一个实例。
set -euo pipefail

NAME=gw-dev-pg
PORT=${PORT:-5433}

if [ "$(docker inspect -f '{{.State.Running}}' "$NAME" 2>/dev/null)" = "true" ]; then
  echo "$NAME 已在运行"
else
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  docker run -d --name "$NAME" \
    -e POSTGRES_PASSWORD=gwdev -e POSTGRES_DB=gateway \
    -p "$PORT:5432" postgres:17-alpine >/dev/null
  until docker exec "$NAME" pg_isready -U postgres -q 2>/dev/null; do sleep 1; done
  echo "$NAME 已启动"
fi

echo "DATABASE_URL=postgres://postgres:gwdev@localhost:$PORT/gateway"
