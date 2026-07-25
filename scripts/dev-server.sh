#!/bin/sh

set -eu

port="${PORT:?locald must provide PORT}"
data_dir="${AGENT_LAB_DATA_DIR:-.agent-lab/dev-runs}"
public_origin="${AGENT_LAB_PUBLIC_ORIGIN:-http://127.0.0.1:$port}"
if [ -n "${AGENT_LAB_HARNESS_CONFIG:-}" ]; then
  harness_config="$AGENT_LAB_HARNESS_CONFIG"
elif [ -f .agent-lab/harnesses.toml ]; then
  harness_config=.agent-lab/harnesses.toml
else
  harness_config=apps/web/tests/fixtures/harnesses.toml
fi

pnpm web:build
cargo build \
  -p agent-lab-nushell-mcp \
  -p agent-lab-driver-protocol \
  -p agent-lab-web \
  --bins

exec target/debug/agent-lab-web \
  --port "$port" \
  --public-origin "$public_origin" \
  --data "$data_dir" \
  --harness-config "$harness_config"
