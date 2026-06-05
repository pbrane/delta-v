#!/usr/bin/env bash
set -euo pipefail

# 1. Safety check — verify the proto file is actually mounted.
if [[ ! -f /delta-v/proto/deltav-flows.proto ]]; then
    echo "ERROR: deltav-flows.proto not found at /delta-v/proto/deltav-flows.proto" >&2
    echo "       Check that docker compose is being run from opennms-container/delta-v/" >&2
    echo "       (the volume mount '../../core/flow-enricher/src/main/proto' resolves" >&2
    echo "        relative to the compose file's directory)." >&2
    exit 1
fi

echo "[init-runner] found deltav-flows.proto, proceeding with DDL bootstrap"

# 2. Client invocation helper. Uses sed for TTL placeholder substitution
#    because gettext-base (envsubst) is not in the clickhouse-server image.
run_sql() {
    local file="$1"
    echo "[init-runner] applying $file"
    sed -e "s/\${DELTAV_CLICKHOUSE_FLOWS_RAW_TTL_DAYS}/${DELTAV_CLICKHOUSE_FLOWS_RAW_TTL_DAYS}/g" \
        -e "s/\${DELTAV_CLICKHOUSE_FLOWS_AGG_TTL_DAYS}/${DELTAV_CLICKHOUSE_FLOWS_AGG_TTL_DAYS}/g" \
        < "$file" | clickhouse-client \
        --host "${CLICKHOUSE_HOST}" \
        --user "${CLICKHOUSE_USER}" \
        --password "${CLICKHOUSE_PASSWORD}" \
        --multiquery
}

# 3. Apply default DDL in order, then user overrides.
for f in /delta-v/init/*.sql; do
    [[ -e "$f" ]] || continue
    run_sql "$f"
done

for f in /delta-v/user-init/*.sql; do
    [[ -e "$f" ]] || continue
    run_sql "$f"
done

echo "[init-runner] DDL bootstrap complete"
