#!/usr/bin/env bash
#
# test-grpc-twin-e2e.sh -- v1.2.0-rc2-pr2 gRPC Twin channel smoke harness
#
# Decision 10 of the rc2 decisions doc set the release gate:
#   gRPC Twin subscribe-to-first-snapshot p99 <= Heartbeat baseline + 100 ms
#   gRPC Twin propagation lag p99 <= Heartbeat baseline + 50 ms
# With Heartbeat-proxy baseline = 2 ms: subscribe gate <= 102 ms,
# propagation gate <= 52 ms. As with PR1, strict latency enforcement
# is deferred to OTel tracing post-PR; this harness verifies channel
# liveness only.
#
# Usage:
#   ./test-grpc-twin-e2e.sh
#
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
source "${SCRIPT_DIR}/test-lib.sh"

log() { echo "==> $*"; }
ok()  { echo "  [PASS] $*"; }
err() { echo "ERROR: $*" >&2; exit 2; }

log "v1.2.0-rc2-pr2 gRPC Twin channel smoke harness"

if ! docker compose ps --status running --format '{{.Name}}' 2>/dev/null | grep -qw "minion-gateway"; then
    err "minion-gateway is not running. Deploy with: ./deploy.sh up full"
fi
ok "minion-gateway is running"

DEADLINE=$(( $(date +%s) + 60 ))
STREAM_LOG=""
while (( $(date +%s) < DEADLINE )); do
    STREAM_LOG=$(docker logs delta-v-minion-gateway 2>&1 \
        | grep -E "Twin stream opened for minion=.* location=" | head -3)
    if [ -n "$STREAM_LOG" ]; then break; fi
    sleep 3
done
if [ -z "$STREAM_LOG" ]; then
    err "minion-gateway never logged 'Twin stream opened' for any location"
fi
ok "gRPC Twin channel live; observed stream(s):"
echo "$STREAM_LOG" | sed 's/^/      /'

# Verify state cache population: the gateway should log a SUBSCRIBE for at
# least passive-status (assumes deploy.sh up full has provisioned a Minion
# with PassiveStatusTwinSubscriber active).
SUBS_LOG=$(docker logs delta-v-minion-gateway 2>&1 \
    | grep -E "Twin SUBSCRIBE minion=.* key=" | head -5)
if [ -z "$SUBS_LOG" ]; then
    log "WARN: no Twin SUBSCRIBE logged yet — Minion subscriptions may not have flowed"
else
    ok "Twin SUBSCRIBEs observed:"
    echo "$SUBS_LOG" | sed 's/^/      /'
fi

log "===================================================================="
log "SMOKE PASS: gRPC Twin channel is live."
log "Strict latency gate (subscribe p99 <= 102ms, propagation p99 <= 52ms)"
log "is calibrated empirically by Task 15's full E2E suite run + Decision"
log "10-v widening if needed. OTel tracing contributes the strict gate"
log "post-PR2."
log "===================================================================="
exit 0
