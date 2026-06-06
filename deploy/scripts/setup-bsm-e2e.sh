#!/usr/bin/env bash
#
# setup-bsm-e2e.sh — BSM End-to-End test for Delta-V
#
# Creates a BSM hierarchy that monitors Delta-V container health:
#   BSM: Delta-V
#     ├── BSM: Delta-V-Infra (postgresql, kafka, minion)
#     └── BSM: Delta-V Passive Monitoring (trapd, syslogd, eventtranslator)
#
# Prerequisites:
#   - Delta-V deployed with full profile: make up PROFILE=full
#   - curl and jq installed on the host
#
# Usage:
#   ./scripts/setup-bsm-e2e.sh              Setup BSM hierarchy
#   ./scripts/setup-bsm-e2e.sh --test       Setup + run failure/recovery test
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

# ── Configuration ──────────────────────────────────────────────
BSMD_URL="http://localhost:8180"
BSM_API="${BSMD_URL}/api/v3/business-services"
SVC_API="${BSMD_URL}/api/v3/monitored-services"
TIMEOUT=120  # seconds to wait for conditions
POLL_INTERVAL=5

# ── Helpers ────────────────────────────────────────────────────
log() { echo "[$(date '+%H:%M:%S')] $*"; }
die() { log "FATAL: $*"; exit 1; }

wait_for_url() {
    local url="$1" label="$2" elapsed=0
    log "Waiting for ${label}..."
    while ! curl -sf "$url" > /dev/null 2>&1; do
        sleep "$POLL_INTERVAL"
        elapsed=$((elapsed + POLL_INTERVAL))
        if [ "$elapsed" -ge "$TIMEOUT" ]; then
            die "${label} not ready after ${TIMEOUT}s"
        fi
    done
    log "${label} is ready"
}

# ── Step 1: Wait for services ─────────────────────────────────
log "=== BSM E2E Setup ==="
wait_for_url "${BSMD_URL}/actuator/health" "BSMd"

# ── Step 2: Wait for nodes to be provisioned ──────────────────
log "Waiting for nodes to be provisioned..."
EXPECTED_SERVICES=6
elapsed=0
while true; do
    SVC_COUNT=$(curl -sf "${SVC_API}" 2>/dev/null | jq 'length' 2>/dev/null || echo 0)
    if [ "$SVC_COUNT" -ge "$EXPECTED_SERVICES" ]; then
        log "All nodes provisioned (${SVC_COUNT} monitored services found)"
        break
    fi
    sleep "$POLL_INTERVAL"
    elapsed=$((elapsed + POLL_INTERVAL))
    if [ "$elapsed" -ge "$TIMEOUT" ]; then
        die "Only ${SVC_COUNT} services found after ${TIMEOUT}s (expected ${EXPECTED_SERVICES}+)"
    fi
    log "Waiting for provisioning... (${SVC_COUNT} services so far)"
done

# ── Step 3: Look up ipService IDs ─────────────────────────────
log "Looking up monitored service IDs..."
SERVICES=$(curl -sf "${SVC_API}")

get_svc_id() {
    local node_label="$1" svc_name="$2"
    echo "$SERVICES" | jq -r ".[] | select(.nodeLabel==\"${node_label}\" and .serviceName==\"${svc_name}\") | .id"
}

PG_SVC_ID=$(get_svc_id "postgres" "PostgreSQL")
KAFKA_SVC_ID=$(get_svc_id "kafka" "Kafka")
MINION_SVC_ID=$(get_svc_id "minion" "Minion-Health")
TRAPD_SVC_ID=$(get_svc_id "trapd" "Deltav-Health")
SYSLOGD_SVC_ID=$(get_svc_id "syslogd" "Deltav-Health")
ET_SVC_ID=$(get_svc_id "eventtranslator" "Deltav-Health")

for var in PG_SVC_ID KAFKA_SVC_ID MINION_SVC_ID TRAPD_SVC_ID SYSLOGD_SVC_ID ET_SVC_ID; do
    [ -n "${!var}" ] || die "Could not find service ID for ${var}"
    log "  ${var}=${!var}"
done

# ── Step 4: Create BSM hierarchy ──────────────────────────────
log "Creating BSM hierarchy..."

create_bs() {
    local json="$1"
    local result
    result=$(curl -sf -X POST "${BSM_API}" \
        -H "Content-Type: application/json" \
        -d "$json")
    echo "$result" | jq -r '.id'
}

# Create leaf BSMs first
INFRA_ID=$(create_bs "$(cat <<EOF
{
  "name": "Delta-V-Infra",
  "reduceFunction": { "type": "highestSeverity" },
  "edges": [
    { "type": "ipService", "ipServiceId": ${PG_SVC_ID}, "mapFunction": { "type": "identity" }, "weight": 1, "friendlyName": "PostgreSQL" },
    { "type": "ipService", "ipServiceId": ${KAFKA_SVC_ID}, "mapFunction": { "type": "identity" }, "weight": 1, "friendlyName": "Kafka" },
    { "type": "ipService", "ipServiceId": ${MINION_SVC_ID}, "mapFunction": { "type": "identity" }, "weight": 1, "friendlyName": "Minion" }
  ]
}
EOF
)")
log "  Created 'Delta-V-Infra' (id=${INFRA_ID})"

PASSIVE_ID=$(create_bs "$(cat <<EOF
{
  "name": "Delta-V Passive Monitoring",
  "reduceFunction": { "type": "highestSeverity" },
  "edges": [
    { "type": "ipService", "ipServiceId": ${TRAPD_SVC_ID}, "mapFunction": { "type": "identity" }, "weight": 1, "friendlyName": "Trapd" },
    { "type": "ipService", "ipServiceId": ${SYSLOGD_SVC_ID}, "mapFunction": { "type": "identity" }, "weight": 1, "friendlyName": "Syslogd" },
    { "type": "ipService", "ipServiceId": ${ET_SVC_ID}, "mapFunction": { "type": "identity" }, "weight": 1, "friendlyName": "EventTranslator" }
  ]
}
EOF
)")
log "  Created 'Delta-V Passive Monitoring' (id=${PASSIVE_ID})"

# Create parent BSM
ROOT_ID=$(create_bs "$(cat <<EOF
{
  "name": "Delta-V",
  "reduceFunction": { "type": "highestSeverity" },
  "edges": [
    { "type": "child", "childId": ${INFRA_ID}, "mapFunction": { "type": "identity" }, "weight": 1 },
    { "type": "child", "childId": ${PASSIVE_ID}, "mapFunction": { "type": "identity" }, "weight": 1 }
  ]
}
EOF
)")
log "  Created 'Delta-V' (id=${ROOT_ID})"

log "=== BSM hierarchy created successfully ==="
log ""
log "  Delta-V (id=${ROOT_ID})"
log "  ├── Delta-V-Infra (id=${INFRA_ID})"
log "  └── Delta-V Passive Monitoring (id=${PASSIVE_ID})"
log ""
log "View status: curl -s ${BSM_API}/${ROOT_ID}/status | jq"

# ── Step 5: Optional failure/recovery test ─────────────────────
if [ "${1:-}" = "--test" ]; then
    log ""
    log "=== Running failure/recovery test ==="

    # Wait for initial status to settle
    log "Waiting for BSM status to settle..."
    elapsed=0
    while true; do
        STATUS=$(curl -sf "${BSM_API}/${ROOT_ID}/status" | jq -r '.operationalStatus')
        if [ "$STATUS" != "indeterminate" ]; then
            log "BSM status: ${STATUS}"
            break
        fi
        sleep "$POLL_INTERVAL"
        elapsed=$((elapsed + POLL_INTERVAL))
        if [ "$elapsed" -ge "$TIMEOUT" ]; then
            log "WARNING: BSM status still indeterminate after ${TIMEOUT}s — proceeding anyway"
            break
        fi
    done

    # Simulate failure: stop trapd
    log "Stopping trapd container..."
    docker compose stop trapd

    # Wait for alarm propagation
    log "Waiting for BSM to detect failure..."
    elapsed=0
    while true; do
        STATUS=$(curl -sf "${BSM_API}/${ROOT_ID}/status" | jq -r '.operationalStatus')
        if [ "$STATUS" != "normal" ] && [ "$STATUS" != "indeterminate" ]; then
            ROOT_CAUSE=$(curl -sf "${BSM_API}/${ROOT_ID}/status" | jq -r '.rootCause[]' 2>/dev/null || echo "unknown")
            log "PASS: BSM detected failure — status=${STATUS}, rootCause=${ROOT_CAUSE}"
            break
        fi
        sleep "$POLL_INTERVAL"
        elapsed=$((elapsed + POLL_INTERVAL))
        if [ "$elapsed" -ge "$TIMEOUT" ]; then
            die "BSM did not detect trapd failure after ${TIMEOUT}s (status=${STATUS})"
        fi
    done

    # Recover: start trapd
    log "Starting trapd container..."
    docker compose start trapd

    # Wait for recovery
    log "Waiting for BSM to recover..."
    elapsed=0
    while true; do
        STATUS=$(curl -sf "${BSM_API}/${ROOT_ID}/status" | jq -r '.operationalStatus')
        if [ "$STATUS" = "normal" ]; then
            log "PASS: BSM recovered — status=normal"
            break
        fi
        sleep "$POLL_INTERVAL"
        elapsed=$((elapsed + POLL_INTERVAL))
        if [ "$elapsed" -ge "$TIMEOUT" ]; then
            die "BSM did not recover after ${TIMEOUT}s (status=${STATUS})"
        fi
    done

    log ""
    log "=== BSM E2E test PASSED ==="
fi
