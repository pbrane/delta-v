#!/usr/bin/env bash
#
# minion-kafka-api-rollback.sh — atomic transport toggle for the v1.2.0-rc2
# Minion ↔ minion-gateway ↔ Kafka path.
#
# v1.2.0-rc2 split the Minion's outbound channels across five emergency-
# rollback flags (Heartbeat + RPC bundled under MINION_TRANSPORT, Twin
# under OPENNMS_MINION_TRANSPORT_TWIN, three sinks under
# MINION_SINK_<TYPE>_TRANSPORT). There's no single master flag, so a real
# rollback requires setting all five together. This script does that
# atomically + recreates the Minion container so the new env vars take
# effect.
#
# Usage:
#   minion-kafka-api-rollback.sh status     Show current per-channel transport
#   minion-kafka-api-rollback.sh kafka      Roll all channels back to Kafka
#   minion-kafka-api-rollback.sh grpc       Restore the rc2 default (all gRPC)
#
# Notes:
#   - Per the Phase 0 audit in PR3, MINION_TRANSPORT=kafka requires the
#     HeartbeatKafkaFallbackConfiguration's @ConditionalOnExpression to
#     find a Kafka MessageDispatcherFactory bean — its gating expression
#     requires BOTH MINION_TRANSPORT=kafka AND
#     MINION_SINK_SYSLOG_TRANSPORT=kafka. The `kafka` mode below sets
#     both, so Heartbeat resolves cleanly. Hand-mixing flag values (e.g.
#     MINION_TRANSPORT=kafka while sinks stay on grpc) leaves Heartbeat
#     without a dispatcher and the Minion fails to start.
#   - This script is intended for the dev-env stack at
#     opennms-container/delta-v. The labbox Minion uses its own env vars
#     (passed via `docker run -e ...`) and is out of scope here.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_DIR="${REPO_ROOT}/opennms-container/delta-v"
ENV_FILE="${COMPOSE_DIR}/.env"

FLAGS=(
    MINION_TRANSPORT
    OPENNMS_MINION_TRANSPORT_TWIN
    MINION_SINK_SYSLOG_TRANSPORT
    MINION_SINK_TRAP_TRANSPORT
    MINION_SINK_TELEMETRY_TRANSPORT
)

usage() {
    sed -n '2,/^$/{ s/^# //; s/^#//; p; }' "$0"
}

require_env_file() {
    if [ ! -f "${ENV_FILE}" ]; then
        echo "ERROR: ${ENV_FILE} not found." >&2
        echo "       Copy from .env.example first:" >&2
        echo "       cp ${COMPOSE_DIR}/.env.example ${ENV_FILE}" >&2
        exit 2
    fi
}

show_status() {
    require_env_file
    echo "Current transport flags in ${ENV_FILE}:"
    for flag in "${FLAGS[@]}"; do
        # `|| true` prevents `set -e` from killing the loop when a flag
        # isn't yet present in .env (treated as unset).
        line=$(grep "^${flag}=" "${ENV_FILE}" 2>/dev/null | head -1 || true)
        value="${line#*=}"
        if [ -z "${line}" ]; then
            printf "  %-40s = %s\n" "${flag}" "(unset → defaults to grpc via compose \${VAR:-grpc})"
        else
            printf "  %-40s = %s\n" "${flag}" "${value}"
        fi
    done
}

set_all_to() {
    require_env_file
    local target="$1"
    local backup="${ENV_FILE}.bak.$(date +%Y%m%d-%H%M%S)"
    cp "${ENV_FILE}" "${backup}"
    echo "Backed up current .env to ${backup}"

    for flag in "${FLAGS[@]}"; do
        if grep -q "^${flag}=" "${ENV_FILE}"; then
            # In-place edit; `.tmp` suffix needed for portable BSD/GNU sed compat.
            sed -i.tmp "s|^${flag}=.*|${flag}=${target}|" "${ENV_FILE}"
            rm -f "${ENV_FILE}.tmp"
        else
            printf '%s=%s\n' "${flag}" "${target}" >> "${ENV_FILE}"
        fi
    done

    echo ""
    echo "Set all 5 flags to '${target}'."
    show_status

    echo ""
    echo "Recreating minion container so the new env vars take effect..."
    (cd "${COMPOSE_DIR}" && docker compose up -d --force-recreate minion)

    echo ""
    echo "Verify with: docker compose -f ${COMPOSE_DIR}/docker-compose.yml logs minion --tail 50"
    echo "Wait ~10s for Spring Boot startup before checking 'stream opened' lines."
}

case "${1:-status}" in
    status)    show_status ;;
    kafka)     set_all_to kafka ;;
    grpc)      set_all_to grpc ;;
    -h|--help) usage ;;
    *)         usage; exit 2 ;;
esac
