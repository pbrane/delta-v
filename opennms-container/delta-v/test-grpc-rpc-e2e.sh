#!/usr/bin/env bash
#
# test-grpc-rpc-e2e.sh -- v1.2.0-rc2-pr1 gRPC RPC channel smoke + bench harness
#
# Decision 10 of the rc2 decisions doc set the release gate for PR1 as:
#   gRPC RPC p99 <= Kafka RPC p99 + 20 ms
# The Heartbeat-as-proxy baseline (Task 1 / pre-work findings) is 2 ms,
# so the gate is <= 22 ms.
#
# The plan's original Echo-RPC measurement mechanism (driving via Karaf
# `opennms:health-check` shell) does not apply: delta-v's Minion is a
# Spring Boot daemon, not Karaf-based (feedback_karaf_is_dead). A real
# strict gate measurement requires:
#   1. A daemon issuing Echo RPCs at sustained rate (Pollerd + provisioned
#      synthetic node OR a custom Java probe using horizon's
#      RpcClientFactory.getClient(echoModule).execute(...)).
#   2. Capturing end-to-end caller-side timing on the daemon side, OR
#      gateway-side per-call duration histograms emitted via Micrometer.
#
# Phase 1 did NOT add explicit gateway-side RPC duration metrics (deferred
# per the rc2 decisions doc — OTel tracing is the planned vehicle for
# distributed RPC timing, contributed AFTER PR1 ships). So this script
# does what it can with the present surface:
#
#   1. PRE-FLIGHT: verify minion-gateway is up and an RPC stream is open
#      from at least one Minion (the load-bearing PR1 wiring check).
#   2. OBSERVE: snapshot gateway-side actuator/prometheus metrics for any
#      RPC-related counters (currently only `executor_*` and JVM ones —
#      we report deltas as a smoke signal of dispatcher activity if PR1
#      adds counters in a follow-up).
#   3. REPORT: document the gate target and note that strict latency
#      enforcement is deferred to Task 14's full E2E run + Decision 10-v
#      widening if the empirical p99 lands above 22 ms with documented
#      justification.
#
# Usage:
#   ./test-grpc-rpc-e2e.sh                     Run the smoke check
#   ./test-grpc-rpc-e2e.sh --observe-secs N    Observe metrics for N seconds (default 60)
#
# Exit codes:
#   0 = channel live, smoke pass
#   1 = channel not live OR gateway unreachable
#   2 = prerequisite failure
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
source "${SCRIPT_DIR}/test-lib.sh"

# -- Parse flags ------------------------------------------------------------
OBSERVE_SECS=60
for ((i=1; i<=$#; i++)); do
    case "${!i}" in
        --observe-secs)
            ((i++))
            OBSERVE_SECS="${!i}"
            ;;
        --help|-h)
            sed -n '2,30p' "$0"
            exit 0
            ;;
    esac
done

# -- Read gate from pre-work findings (Heartbeat-proxy baseline) -----------
FINDINGS="${SCRIPT_DIR}/../../docs/plans/2026-04-26-v1.2.0-rc2-pr1-pre-work-findings.md"
if [ ! -f "$FINDINGS" ]; then
    err "Pre-work findings not found at $FINDINGS"
fi
BASELINE_MS=2   # Heartbeat-proxy baseline per Task 1 / pre-work findings
WIDENING_MS=20  # Decision 10 standard widening for RPC
GATE_MS=$((BASELINE_MS + WIDENING_MS))

log "v1.2.0-rc2-pr1 gRPC RPC channel smoke + bench harness"
log "  Heartbeat-proxy baseline:  ${BASELINE_MS} ms (Decision 10 / Task 1 pivot)"
log "  Gate widening:             ${WIDENING_MS} ms"
log "  Release gate (Task 14):    gRPC RPC p99 <= ${GATE_MS} ms"

# -- Pre-flight: gateway up + RPC stream open -------------------------------
log ""
log "Pre-flight: minion-gateway running + RPC stream open..."

if ! docker compose ps --status running --format '{{.Name}}' 2>/dev/null | grep -qw "minion-gateway"; then
    err "minion-gateway is not running. Deploy with: ./deploy.sh up full"
fi
ok "minion-gateway is running"

# Wait up to 60s for at least one RPC stream to open (matches Tasks 11/12 pattern)
DEADLINE=$(( $(date +%s) + 60 ))
STREAM_LOG=""
while (( $(date +%s) < DEADLINE )); do
    if STREAM_LOG=$(docker logs delta-v-minion-gateway 2>&1 | grep -E "RPC stream opened for minion=.* location=" | head -5); then
        if [ -n "$STREAM_LOG" ]; then
            break
        fi
    fi
    sleep 3
done
if [ -z "$STREAM_LOG" ]; then
    err "minion-gateway never logged 'RPC stream opened' for any location; gRPC RPC channel not live"
fi
ok "gRPC RPC channel live; observed stream(s):"
echo "$STREAM_LOG" | sed 's/^/      /'

# -- Snapshot gateway prometheus metrics, baseline -------------------------
log ""
log "Snapshotting gateway prometheus metrics (T=0)..."
T0=$(docker exec delta-v-minion-gateway sh -c \
    'wget -qO- http://localhost:8080/actuator/prometheus 2>/dev/null' \
    | grep -E "^[a-zA-Z][a-zA-Z0-9_]+ [0-9.E+-]+$" \
    | sort)
T0_LINES=$(echo "$T0" | wc -l | tr -d ' ')
log "  Captured ${T0_LINES} metric points at T=0"

# -- Observe for the configured window -------------------------------------
log ""
log "Observing for ${OBSERVE_SECS}s while ambient RPC traffic flows..."
log "  (drive a test like ./test-perspective-e2e.sh in another terminal for"
log "   sustained traffic, OR rely on incidental polling activity)"
sleep "$OBSERVE_SECS"

# -- Snapshot again, compute deltas ----------------------------------------
log ""
log "Snapshotting gateway prometheus metrics (T=${OBSERVE_SECS}s)..."
T1=$(docker exec delta-v-minion-gateway sh -c \
    'wget -qO- http://localhost:8080/actuator/prometheus 2>/dev/null' \
    | grep -E "^[a-zA-Z][a-zA-Z0-9_]+ [0-9.E+-]+$" \
    | sort)
T1_LINES=$(echo "$T1" | wc -l | tr -d ' ')
log "  Captured ${T1_LINES} metric points at T=${OBSERVE_SECS}s"

# Compute changed counters (simplistic delta — assumes one value per metric name)
log ""
log "Counter deltas over the ${OBSERVE_SECS}s window:"
DELTA=$(diff <(echo "$T0") <(echo "$T1") | grep -E "^[<>]" | awk '
    /^</ { name=$2; before[name]=$3 }
    /^>/ { name=$2; after[name]=$3 }
    END {
        for (n in after) {
            if (n in before) {
                d = after[n] - before[n]
                if (d > 0) printf "  %-60s +%g\n", n, d
            } else {
                printf "  %-60s NEW=%s\n", n, after[n]
            }
        }
    }' | sort -k2 -n -r | head -30)
if [ -n "$DELTA" ]; then
    echo "$DELTA"
else
    log "  (no counter changes — system is idle; drive load via test-perspective-e2e.sh or similar)"
fi

# -- Result ----------------------------------------------------------------
log ""
log "===================================================================="
log "SMOKE PASS: gRPC RPC channel is live."
log ""
log "Strict latency gate (gRPC p99 <= ${GATE_MS} ms) is calibrated"
log "empirically by Task 14's full E2E suite run, NOT by this script."
log "Per Decision 10 sub-decision 10-v of the rc2 decisions doc, if the"
log "Task 14 measurement lands above ${GATE_MS} ms but the overhead is"
log "purely necessary translator+dispatcher work, the gate may be widened"
log "with documented justification."
log "===================================================================="

exit 0
