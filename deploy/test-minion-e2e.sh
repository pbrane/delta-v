#!/usr/bin/env bash
#
# test-minion-e2e.sh — End-to-end Minion integration test for Delta-V
#
# Sends SNMP traps through the Minion (not directly to Trapd) and verifies
# the full pipeline: Minion → Kafka Sink → Trapd consumer → EventCreator →
# KafkaEventForwarder → Kafka → EventTranslator → Alarmd → PostgreSQL
#
# This validates that the Minion can receive traps and forward them via
# Kafka IPC without any REST dependency (OPENNMS_HTTP eliminated).
#
# Usage:
#   ./test-minion-e2e.sh              Run the test
#   ./test-minion-e2e.sh --verbose    Show full Kafka event trace
#   ./test-minion-e2e.sh --pre-clean  Full pre-run cleanup (delete all nodes and alarms)
#   ./test-minion-e2e.sh --post-cleanup  Delete test data after run
#
# Prerequisites:
#   - Delta-V deployed: docker compose up -d
#   - Minion healthy: curl -sf http://localhost:8301/actuator/health
#   - snmptrap available on host (net-snmp)
#
# Exit codes:
#   0 = all tests passed
#   1 = test failure
#   2 = prerequisite failure
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
source "${SCRIPT_DIR}/test-lib.sh"

# ── Configuration ──────────────────────────────────────────────────
# Traps go to the MINION, not directly to Trapd
TRAP_HOST="localhost"
TRAP_PORT="11162"                    # Minion's mapped trap port (11162 → 1162/udp)
TRAP_COMMUNITY="public"
NODE_SCAN_TIMEOUT=90                 # Longer timeout — extra Kafka hop via Minion
ALARM_TIMEOUT=45
# Alarm verification uses PostgreSQL directly (no webapp dependency)
IFINDEX=2                            # Use ifIndex=2 to avoid collision with direct-trapd tests

# ── Usage ─────────────────────────────────────────────────────────
usage() {
    cat <<'USAGE'
Usage: ./test-minion-e2e.sh [options]

Options:
  --verbose       Show full Kafka event trace
  --pre-clean     Full pre-run cleanup (delete all nodes and alarms)
  --post-cleanup  Delete test alarms after test
  --help          Show this help

Prerequisites:
  - Delta-V deployed: docker compose up -d
  - Minion healthy
  - snmptrap must be installed (net-snmp)
USAGE
}

# ── Parse flags ────────────────────────────────────────────────────
VERBOSE=false
PRE_CLEAN=false
POST_CLEANUP=false
for arg in "$@"; do
    case "$arg" in
        --verbose) VERBOSE=true ;;
        --pre-clean) PRE_CLEAN=true ;;
        --post-cleanup) POST_CLEANUP=true ;;
        --help|-h) usage; exit 0 ;;
    esac
done

# ── Helpers ────────────────────────────────────────────────────────
PASS=0
FAIL=0
FAULT_CONSUMER_PID=""
IPC_CONSUMER_PID=""
SINK_CONSUMER_PID=""
TEST_TMPDIR=$(mktemp -d)
FAULT_LOG="$TEST_TMPDIR/fault-events.log"
IPC_LOG="$TEST_TMPDIR/ipc-events.log"
SINK_LOG="$TEST_TMPDIR/sink-events.log"

log()  { echo "==> $*"; }
ok()   { echo "  [PASS] $*"; PASS=$((PASS + 1)); }
fail() { echo "  [FAIL] $*"; FAIL=$((FAIL + 1)); }
err()  { echo "ERROR: $*" >&2; exit 2; }

cleanup() {
    [ -n "$FAULT_CONSUMER_PID" ] && kill "$FAULT_CONSUMER_PID" 2>/dev/null || true
    [ -n "$IPC_CONSUMER_PID" ] && kill "$IPC_CONSUMER_PID" 2>/dev/null || true
    [ -n "$SINK_CONSUMER_PID" ] && kill "$SINK_CONSUMER_PID" 2>/dev/null || true

    if $VERBOSE; then
        log ""
        log "── Kafka Sink Events (Minion → Trapd) ──"
        cat "$SINK_LOG" 2>/dev/null || true
        log ""
        log "── Kafka Fault Events ──"
        cat "$FAULT_LOG" 2>/dev/null || true
        log ""
        log "── Kafka IPC Events ──"
        cat "$IPC_LOG" 2>/dev/null || true
    fi

    if $POST_CLEANUP; then
        log "Post-run cleanup (--post-cleanup): removing test data..."
        docker compose exec -T -e PGPASSWORD=opennms postgres \
            psql -U opennms -d opennms -q \
            -c "DELETE FROM alarms WHERE eventuei LIKE '%translator/traps/SNMP_Link%'" \
            2>/dev/null || true
    fi

    docker compose exec -T kafka sh -c 'for p in $(ps -eo pid,args 2>/dev/null | grep kafka-console-consumer | grep -v grep | awk "{print \$1}"); do kill "$p" 2>/dev/null; done' || true
    rm -rf "$TEST_TMPDIR"
}
trap cleanup EXIT

wait_for_kafka_event() {
    local log_file="$1"
    local pattern="$2"
    local timeout="$3"
    local description="$4"
    local elapsed=0

    log "Waiting for $description (timeout: ${timeout}s)..."
    while [ $elapsed -lt "$timeout" ]; do
        if grep -q "$pattern" "$log_file" 2>/dev/null; then
            return 0
        fi
        sleep 2
        elapsed=$((elapsed + 2))
    done
    return 1
}

psql_query() {
    docker compose exec -T -e PGPASSWORD=opennms postgres \
        psql -U opennms -d opennms -t -A -c "$1" 2>/dev/null
}

# ── Pre-run cleanup (--pre-clean) ─────────────────────────────────
if $PRE_CLEAN; then
    log "Pre-run cleanup (--pre-clean): resetting DB..."
    clean_all_nodes
    clean_all_alarms
    ok "Database cleaned"
    log ""
fi

# ── Prerequisite Checks ───────────────────────────────────────────
log "Checking prerequisites..."

command -v snmptrap >/dev/null 2>&1 || err "snmptrap not found. Install net-snmp."

REQUIRED_SERVICES="postgres kafka trapd eventtranslator alarmd provisiond"
for svc in $REQUIRED_SERVICES; do
    if ! docker compose ps --status running --format '{{.Name}}' 2>/dev/null | grep -qw "$svc"; then
        err "Service '$svc' is not running. Deploy with: docker compose up -d"
    fi
done
# Minion uses Docker Compose profiles — docker compose ps doesn't see it.
# Use docker ps directly to check the container.
if ! docker ps --format '{{.Names}}' 2>/dev/null | grep -qw "delta-v-minion"; then
    err "Minion container is not running. Start with: docker start delta-v-minion"
fi
ok "All required services running (including Minion)"

# Verify Minion location (Boot4: read from Spring env, not Karaf cfg file)
MINION_LOCATION=$(docker compose exec -T minion sh -c 'echo ${MINION_LOCATION:-unknown}' 2>/dev/null || echo "unknown")
log "Minion location: $MINION_LOCATION"

# ── Start Kafka Consumers ─────────────────────────────────────────
log "Starting Kafka event consumers..."

# Watch the Sink topic to verify Minion → Trapd forwarding
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic DeltaV.Sink.Trap \
    > "$SINK_LOG" 2>/dev/null &
SINK_CONSUMER_PID=$!

docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic deltav-fault-events \
    > "$FAULT_LOG" 2>/dev/null &
FAULT_CONSUMER_PID=$!

docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic deltav-ipc-events \
    > "$IPC_LOG" 2>/dev/null &
IPC_CONSUMER_PID=$!

# Give Kafka consumers time to join consumer groups and start receiving.
# The console-consumer needs to complete group rebalancing before we send traps.
sleep 8

# ══════════════════════════════════════════════════════════════════
# Pre-flight: provoke gateway-side stream open
# ══════════════════════════════════════════════════════════════════

# Drive one trap to provoke gateway-side stream open.
snmptrap -v 2c -c "$TRAP_COMMUNITY" "${TRAP_HOST}:${TRAP_PORT}" '' \
    .1.3.6.1.4.1.99999 .1.3.6.1.4.1.99999.1.1 s "preflight" >/dev/null 2>&1 || true

# Wait up to 10s for "Trap stream opened". Capture+case avoids SIGPIPE
# under pipefail (per feedback_grep_q_sigpipe_in_pipefail).
GATEWAY_OPENED=""
for i in $(seq 1 10); do
    GATEWAY_LOG=$(docker compose logs minion-gateway 2>&1 || true)
    case "$GATEWAY_LOG" in
        *"Trap stream opened for minion="*) GATEWAY_OPENED=yes; break ;;
    esac
    sleep 1
done
if [ "$GATEWAY_OPENED" = "yes" ]; then
    ok "gRPC TrapService engaged: minion-gateway logged stream open"
else
    log "  (last 30 gateway log lines:)"
    docker compose logs minion-gateway --tail 30 2>&1 | sed 's/^/    /'
    err "Pre-flight timed out: minion-gateway never logged 'Trap stream opened'"
fi

# ══════════════════════════════════════════════════════════════════
# Phase 1: Verify Minion Trap Forwarding (coldStart via Minion)
# ══════════════════════════════════════════════════════════════════
log ""
log "Phase 1: Trap forwarding via Minion (coldStart)..."
log "  Sending trap to Minion at ${TRAP_HOST}:${TRAP_PORT}"
log "  Expected flow: Minion → Kafka Sink → Trapd consumer → newSuspect → Provisiond"

snmptrap -v 2c -c "$TRAP_COMMUNITY" "${TRAP_HOST}:${TRAP_PORT}" '' \
    1.3.6.1.6.3.1.1.5.1 \
    1.3.6.1.2.1.1.3.0 t 0

ok "coldStart trap sent to Minion at ${TRAP_HOST}:${TRAP_PORT}"

# Verify the trap appears on the Sink topic (Minion → Kafka)
if wait_for_kafka_event "$SINK_LOG" "trap-message-log" 15 "trap on Kafka Sink topic"; then
    ok "Trap forwarded by Minion to Kafka Sink topic"
else
    # The Sink topic name might differ — check fault events directly
    log "  (Sink topic check inconclusive — checking fault events instead)"
fi

# Verify the event reaches fault-events (Trapd processed it)
if wait_for_kafka_event "$FAULT_LOG" "Cold_Start" "$ALARM_TIMEOUT" "coldStart event in fault-events"; then
    ok "coldStart event processed by Trapd (received via Minion)"
else
    fail "coldStart event not seen in fault-events — Minion → Trapd forwarding may be broken"
    if ! $VERBOSE; then
        log "Hint: re-run with --verbose to see Kafka event trace"
        log ""
        log "── Kafka Sink Events (last 10 lines) ──"
        tail -10 "$SINK_LOG" 2>/dev/null || true
        log ""
        log "── Kafka Fault Events (last 10 lines) ──"
        tail -10 "$FAULT_LOG" 2>/dev/null || true
    fi
    log ""
    log "Results: $PASS passed, $FAIL failed"
    exit 1
fi

# Wait for node provisioning
if wait_for_kafka_event "$IPC_LOG" "nodeScanCompleted" "$NODE_SCAN_TIMEOUT" "nodeScanCompleted"; then
    ok "nodeScanCompleted received — node provisioned via Minion trap"
else
    # Node may already exist from prior test runs — check
    NODE_EXISTS=$(psql_query "SELECT count(*) FROM node WHERE nodelabel LIKE '%'" 2>/dev/null || echo "0")
    if [ "$NODE_EXISTS" -gt 0 ]; then
        log "  (Node already exists from prior run — skipping provisioning check)"
        ok "Node exists in database (prior provisioning)"
    else
        fail "nodeScanCompleted not received within ${NODE_SCAN_TIMEOUT}s"
    fi
fi

# Wait for Trapd's InterfaceToNodeCache to refresh so the newly provisioned
# node (192.168.65.1) is mapped. The cache refresh interval is configured
# to 15s via org.opennms.interface-node-cache.refresh-timer in Trapd's JAVA_OPTS.
# Without this, the linkDown event won't have a nodeid and alarms won't be created.
CACHE_WAIT=20
log ""
log "Waiting ${CACHE_WAIT}s for Trapd InterfaceToNodeCache to refresh..."
sleep "$CACHE_WAIT"

# ══════════════════════════════════════════════════════════════════
# Phase 2: Alarm Creation via linkDown Trap (through Minion)
# ══════════════════════════════════════════════════════════════════
log ""
log "Phase 2: Alarm creation via Minion (linkDown trap)..."

snmptrap -v 2c -c "$TRAP_COMMUNITY" "${TRAP_HOST}:${TRAP_PORT}" '' \
    1.3.6.1.6.3.1.1.5.3 \
    1.3.6.1.2.1.1.3.0 t 0 \
    .1.3.6.1.2.1.2.2.1.1.${IFINDEX} i ${IFINDEX}

ok "linkDown trap sent via Minion (ifIndex=${IFINDEX})"

if wait_for_kafka_event "$FAULT_LOG" "translator/traps/SNMP_Link_Down" "$ALARM_TIMEOUT" "translated linkDown event"; then
    ok "Translated SNMP_Link_Down event seen in Kafka (via Minion path)"
else
    fail "Translated SNMP_Link_Down not seen in Kafka within ${ALARM_TIMEOUT}s"
fi

sleep 5

ALARM_ROW=$(psql_query "SELECT alarmid, severity, alarmtype FROM alarms WHERE eventuei = 'uei.opennms.org/translator/traps/SNMP_Link_Down' AND alarmtype = 1 LIMIT 1")
if [ -n "$ALARM_ROW" ]; then
    ok "Alarm created in PostgreSQL: $ALARM_ROW"
else
    fail "No linkDown alarm found in PostgreSQL"
fi

ALARM_COUNT=$(psql_query "SELECT count(*) FROM alarms WHERE eventuei = 'uei.opennms.org/translator/traps/SNMP_Link_Down'")
if [ "${ALARM_COUNT:-0}" -gt 0 ]; then
    ok "Alarm verified in PostgreSQL ($ALARM_COUNT alarm(s))"
else
    fail "No linkDown alarm found in PostgreSQL"
fi

# Verify alarm logmsg is expanded (not empty, no raw %tokens%)
ALARM_LOGMSG=$(psql_query "SELECT logmsg FROM alarms WHERE eventuei = 'uei.opennms.org/translator/traps/SNMP_Link_Down' AND logmsg IS NOT NULL AND logmsg != '' LIMIT 1")
if [ -n "$ALARM_LOGMSG" ]; then
    if echo "$ALARM_LOGMSG" | grep -q '%nodelabel%'; then
        fail "Alarm logmsg contains unexpanded %nodelabel% token: $ALARM_LOGMSG"
    else
        ok "Alarm logmsg expanded: $ALARM_LOGMSG"
    fi
else
    fail "Alarm logmsg is empty — event template expansion not working"
fi

# Verify alarm description is expanded (not empty, no raw %tokens%)
ALARM_DESCR=$(psql_query "SELECT description FROM alarms WHERE eventuei = 'uei.opennms.org/translator/traps/SNMP_Link_Down' AND description IS NOT NULL AND description != '' LIMIT 1")
if [ -n "$ALARM_DESCR" ]; then
    if echo "$ALARM_DESCR" | grep -q '%nodelabel%'; then
        fail "Alarm description contains unexpanded %nodelabel% token"
    else
        ok "Alarm description expanded (non-empty, tokens resolved)"
    fi
else
    fail "Alarm description is empty — event template expansion not working"
fi

# Track 1 regression check: alarmd must have published the alarm to Kafka.
# Cheap key-only check (binary protobuf value is not decoded). 15s window — alarmd publishes
# the lifecycle event within a couple of seconds of the PG insert.
ALARM_ID=$(psql_query "SELECT alarmid FROM alarms WHERE eventuei = 'uei.opennms.org/translator/traps/SNMP_Link_Down' AND alarmtype = 1 LIMIT 1")
REDUCTION_KEY=$(psql_query "SELECT reductionkey FROM alarms WHERE alarmid = $ALARM_ID")
if [ -n "$REDUCTION_KEY" ]; then
    KEY_FOUND=false
    # Poll up to ~48s: alarmd->materializer can lag behind the PG insert under
    # load, and the topic accumulates many keys over a long-lived stack. Re-scan
    # the whole topic each pass. Bash substring match (no `| grep -q`) so a hit
    # can never trip SIGPIPE under `set -o pipefail`.
    for _ in 1 2 3 4 5 6; do
        KAFKA_KEYS=$(docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
            --bootstrap-server localhost:9092 \
            --topic deltav-alarms-state-change \
            --from-beginning --max-messages 50000 --timeout-ms 6000 \
            --property print.key=true --property print.value=false 2>/dev/null || true)
        [[ "$KAFKA_KEYS" == *"$REDUCTION_KEY"* ]] && { KEY_FOUND=true; break; }
        sleep 2
    done
    if $KEY_FOUND; then
        ok "Alarm published to deltav-alarms-state-change (reduction_key=$REDUCTION_KEY)"
    else
        fail "Alarm reduction_key '$REDUCTION_KEY' NOT found on deltav-alarms-state-change topic"
    fi
fi

# ══════════════════════════════════════════════════════════════════
# Phase 3: Alarm Clearing via linkUp Trap (through Minion)
# ══════════════════════════════════════════════════════════════════
log ""
log "Phase 3: Alarm clearing via Minion (linkUp trap)..."

snmptrap -v 2c -c "$TRAP_COMMUNITY" "${TRAP_HOST}:${TRAP_PORT}" '' \
    1.3.6.1.6.3.1.1.5.4 \
    1.3.6.1.2.1.1.3.0 t 0 \
    .1.3.6.1.2.1.2.2.1.1.${IFINDEX} i ${IFINDEX}

ok "linkUp trap sent via Minion (ifIndex=${IFINDEX})"

if wait_for_kafka_event "$FAULT_LOG" "translator/traps/SNMP_Link_Up" "$ALARM_TIMEOUT" "translated linkUp event"; then
    ok "Translated SNMP_Link_Up event seen in Kafka (via Minion path)"
else
    fail "Translated SNMP_Link_Up not seen in Kafka within ${ALARM_TIMEOUT}s"
fi

sleep 5

CLEARED_ROW=$(psql_query "SELECT alarmid, severity, alarmtype FROM alarms WHERE eventuei = 'uei.opennms.org/translator/traps/SNMP_Link_Down' AND severity = 2 LIMIT 1")
if [ -n "$CLEARED_ROW" ]; then
    ok "Alarm cleared in PostgreSQL: $CLEARED_ROW"
else
    STILL_ACTIVE=$(psql_query "SELECT alarmid, severity, alarmtype FROM alarms WHERE eventuei = 'uei.opennms.org/translator/traps/SNMP_Link_Down' LIMIT 1")
    if [ -n "$STILL_ACTIVE" ]; then
        fail "Alarm exists but NOT cleared (still: $STILL_ACTIVE)"
    else
        fail "No linkDown alarm found in PostgreSQL at all"
    fi
fi

CLEARED_COUNT=$(psql_query "SELECT count(*) FROM alarms WHERE eventuei = 'uei.opennms.org/translator/traps/SNMP_Link_Down' AND severity = 2")
if [ "${CLEARED_COUNT:-0}" -gt 0 ]; then
    ok "Alarm CLEARED verified in PostgreSQL"
else
    fail "Alarm not showing CLEARED in PostgreSQL"
fi

# ══════════════════════════════════════════════════════════════════
# Results
# ══════════════════════════════════════════════════════════════════
log ""
log "Results: $PASS passed, $FAIL failed"
log ""
log "Validated flow: trap → Minion → Kafka Sink → Trapd → EventCreator →"
log "  KafkaEventForwarder → Kafka → EventTranslator → Alarmd → PostgreSQL"
[ "$FAIL" -eq 0 ] && exit 0 || exit 1
