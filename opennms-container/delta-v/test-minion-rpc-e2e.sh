#!/usr/bin/env bash
#
# test-minion-rpc-e2e.sh — End-to-end Minion RPC canary for Delta-V
#
# Provisions a canary node at location="Default" targeting the snmp-agent
# mock container. Verifies that SnmpDetector + IcmpDetector executed via
# Minion RPC (evidence: services in ifservices table) and that Pollerd
# is actively polling those services via Minion RPC (evidence: poll
# activity in Pollerd logs).
#
# This test is the CANARY for Phase 3 horizon-extraction. Any PR that
# regresses Minion RPC detection or monitoring will fail this test.
#
# Usage:
#   ./test-minion-rpc-e2e.sh              Run the test
#   ./test-minion-rpc-e2e.sh --verbose    Show diagnostic queries on failure
#   ./test-minion-rpc-e2e.sh --pre-clean  Full pre-run cleanup (DB + restart daemons)
#   ./test-minion-rpc-e2e.sh --post-cleanup  Delete canary node and alarms after run
#
# Prerequisites:
#   - Delta-V deployed with lite or full profile: ./deploy.sh up lite
#   - snmp-agent, minion, provisiond, pollerd, postgres, kafka running
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
FOREIGN_SOURCE="rpc-canary"
EXPECTED_NODES=1
PROVISION_TIMEOUT=120        # 2 min — wait for node provisioning via Minion
DETECT_TIMEOUT=180           # 3 min — wait for detectors to complete via RPC
POLL_TIMEOUT=180             # 3 min — wait for Pollerd to start polling via RPC
POLL_INTERVAL=10             # seconds between DB checks

# ── Parse flags ────────────────────────────────────────────────────
VERBOSE=false
PRE_CLEAN=false
POST_CLEANUP=false
for arg in "$@"; do
    case "$arg" in
        --verbose) VERBOSE=true ;;
        --pre-clean) PRE_CLEAN=true ;;
        --post-cleanup) POST_CLEANUP=true ;;
        --help|-h)
            sed -n '2,/^$/{ s/^# //; s/^#//; p }' "$0"
            exit 0
            ;;
    esac
done
# ── Helpers ────────────────────────────────────────────────────────
PASS=0
FAIL=0

log()  { echo "==> $*"; }
ok()   { echo "  [PASS] $*"; PASS=$((PASS + 1)); }
fail() { echo "  [FAIL] $*"; FAIL=$((FAIL + 1)); }
err()  { echo "ERROR: $*" >&2; exit 2; }

cleanup() {
    # Kill background Kafka consumers
    docker compose exec -T kafka sh -c 'for p in $(ps -eo pid,args 2>/dev/null | grep kafka-console-consumer | grep -v grep | awk "{print \$1}"); do kill "$p" 2>/dev/null; done' || true
    rm -rf "${TEST_TMPDIR:-}"
    # Safety: ensure Minion is unpaused if script exits mid-Phase-4
    docker unpause delta-v-minion 2>/dev/null || true
    if $POST_CLEANUP; then
        log "Post-run cleanup (--post-cleanup): removing canary node..."
        psql_query "DELETE FROM outages WHERE nodeid IN (SELECT nodeid FROM node WHERE foreignsource = '${FOREIGN_SOURCE}')" || true
        psql_query "DELETE FROM ifservices WHERE ipinterfaceid IN (SELECT id FROM ipinterface WHERE nodeid IN (SELECT nodeid FROM node WHERE foreignsource = '${FOREIGN_SOURCE}'))" || true
        psql_query "UPDATE ipinterface SET snmpinterfaceid = NULL WHERE nodeid IN (SELECT nodeid FROM node WHERE foreignsource = '${FOREIGN_SOURCE}')" || true
        psql_query "DELETE FROM snmpinterface WHERE nodeid IN (SELECT nodeid FROM node WHERE foreignsource = '${FOREIGN_SOURCE}')" || true
        psql_query "DELETE FROM ipinterface WHERE nodeid IN (SELECT nodeid FROM node WHERE foreignsource = '${FOREIGN_SOURCE}')" || true
        psql_query "DELETE FROM node WHERE foreignsource = '${FOREIGN_SOURCE}'" || true
        log "  Canary node deleted"
    fi
}
trap cleanup EXIT

psql_query() {
    docker compose exec -T -e PGPASSWORD=opennms postgres \
        psql -U opennms -d opennms -t -A -c "$1" 2>/dev/null
}

wait_for_db() {
    local query="$1"
    local timeout="$2"
    local description="$3"
    local poll_interval="${4:-$POLL_INTERVAL}"
    local elapsed=0

    log "Waiting for $description (timeout: ${timeout}s)..."
    while [ $elapsed -lt "$timeout" ]; do
        local result
        result=$(psql_query "$query" 2>/dev/null || echo "")
        if [ -n "$result" ] && [ "$result" != "0" ]; then
            return 0
        fi
        sleep "$poll_interval"
        elapsed=$((elapsed + poll_interval))
        if [ $((elapsed % 60)) -eq 0 ]; then
            log "  ... ${elapsed}s elapsed"
        fi
    done
    return 1
}

wait_for_health() {
    local container="$1"
    local health_url="$2"
    local timeout="${3:-120}"
    local elapsed=0

    log "Waiting for $container health check (timeout: ${timeout}s)..."
    while [ $elapsed -lt "$timeout" ]; do
        if docker compose exec -T "$container" curl -sf "$health_url" >/dev/null 2>&1; then
            log "  ... $container is healthy"
            return 0
        fi
        sleep 5
        elapsed=$((elapsed + 5))
    done
    return 1
}

# ── Kafka consumer for fault-events (needed for Phase 4 negative assertions) ──
TEST_TMPDIR=$(mktemp -d)
FAULT_LOG="$TEST_TMPDIR/fault-events.log"

docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic opennms-fault-events \
    > "$FAULT_LOG" 2>/dev/null &
FAULT_CONSUMER_PID=$!
sleep 3

# Resolves the snmp-agent container's IP on the Docker internal network.
# Uses docker inspect from the host — more reliable than exec'ing into Minion
# (Alpine-based Minion container lacks ping, nslookup, and may lack getent).
# The IP returned is the container's IP on the shared Docker network, which
# is exactly what Minion will use to reach snmp-agent.
resolve_snmp_agent_ip() {
    docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{"\n"}}{{end}}' delta-v-snmp-agent 2>/dev/null | grep -v '^$' | head -1
}

show_diagnostics() {
    if ! $VERBOSE; then
        log "Hint: re-run with --verbose for diagnostic output"
        return
    fi
    log ""
    log "── Diagnostic Queries ──"
    log ""
    log "Canary node:"
    psql_query "SELECT nodeid, nodelabel, foreignid, location, lastcapsdpoll FROM node WHERE foreignsource = '${FOREIGN_SOURCE}'" || true
    log ""
    log "IP Interfaces:"
    psql_query "SELECT n.nodelabel, ip.ipaddr, ip.issnmpprimary FROM ipinterface ip JOIN node n ON ip.nodeid = n.nodeid WHERE n.foreignsource = '${FOREIGN_SOURCE}'" || true
    log ""
    log "Services detected:"
    psql_query "SELECT n.nodelabel, svc.servicename, s.status FROM ifservices s JOIN service svc ON s.serviceid = svc.serviceid JOIN ipinterface ip ON s.ipinterfaceid = ip.id JOIN node n ON ip.nodeid = n.nodeid WHERE n.foreignsource = '${FOREIGN_SOURCE}' ORDER BY svc.servicename" || true
    log ""
    log "Recent Pollerd log lines mentioning canary node:"
    docker compose logs --tail=100 pollerd 2>/dev/null | grep -i "snmp-agent-canary" || log "  (no matches)"
}
# ── Prerequisite Checks ───────────────────────────────────────────
log "Checking prerequisites..."

REQUIRED_SERVICES="postgres kafka provisiond pollerd minion snmp-agent"
for svc in $REQUIRED_SERVICES; do
    if ! docker compose ps --status running --format '{{.Name}}' 2>/dev/null | grep -qw "$svc"; then
        err "Service '$svc' is not running. Deploy with: ./deploy.sh up lite"
    fi
done
ok "Required services running (postgres, kafka, provisiond, pollerd, minion, snmp-agent)"

# Resolve snmp-agent IP via Minion's DNS view (this is the IP Minion will use)
SNMP_AGENT_IP=$(resolve_snmp_agent_ip)
if [ -z "${SNMP_AGENT_IP:-}" ]; then
    err "Could not resolve snmp-agent IP from Minion's perspective. Is Docker DNS working?"
fi
ok "Resolved snmp-agent IP (from Minion's perspective): ${SNMP_AGENT_IP}"

# Quick sanity check — Minion can reach the SNMP port
if ! docker compose exec -T minion sh -c "nc -uzvw 2 snmp-agent 161" >/dev/null 2>&1; then
    log "  [WARN] Minion could not reach snmp-agent:161/udp (may still work; netcat may be unavailable)"
else
    ok "Minion can reach snmp-agent:161/udp"
fi

# ══════════════════════════════════════════════════════════════════
# Pre-run cleanup (--pre-clean): remove any prior canary data
# ══════════════════════════════════════════════════════════════════
if $PRE_CLEAN; then
    log ""
    log "Pre-run cleanup (--pre-clean): removing existing canary data..."

    psql_query "DELETE FROM outages WHERE nodeid IN (SELECT nodeid FROM node WHERE foreignsource = '${FOREIGN_SOURCE}')" || true
    psql_query "DELETE FROM ifservices WHERE ipinterfaceid IN (SELECT id FROM ipinterface WHERE nodeid IN (SELECT nodeid FROM node WHERE foreignsource = '${FOREIGN_SOURCE}'))" || true
    psql_query "UPDATE ipinterface SET snmpinterfaceid = NULL WHERE nodeid IN (SELECT nodeid FROM node WHERE foreignsource = '${FOREIGN_SOURCE}')" || true
    psql_query "DELETE FROM snmpinterface WHERE nodeid IN (SELECT nodeid FROM node WHERE foreignsource = '${FOREIGN_SOURCE}')" || true
    psql_query "DELETE FROM ipinterface WHERE nodeid IN (SELECT nodeid FROM node WHERE foreignsource = '${FOREIGN_SOURCE}')" || true
    psql_query "DELETE FROM events WHERE nodeid IN (SELECT nodeid FROM node WHERE foreignsource = '${FOREIGN_SOURCE}')" || true
    psql_query "DELETE FROM node WHERE foreignsource = '${FOREIGN_SOURCE}'" || true
    ok "Prior canary data cleaned"
fi
# ══════════════════════════════════════════════════════════════════
# Phase 1: Provision the canary node via Minion
# ══════════════════════════════════════════════════════════════════
log ""
log "Phase 1: Provisioning canary node targeting snmp-agent (${SNMP_AGENT_IP}) at location=Default..."

# Generate the requisition inline with the resolved IP.
#
# Write directly into the running provisiond container's named-volume mount at
# /opt/deltav/etc/imports/. The host bind-mount path provisiond-overlay/etc/imports/
# is NOT mounted into provisiond (per feedback_named_volume_autopopulate +
# project_provisiond_seed_split_followup): provisiond reads only from the
# provisiond_imports named volume, which the init sidecar seeds from
# provisiond-overlay/etc/imports-seed/ at first boot. Writing to the host bind-mount
# path silently does nothing — tests previously "passed" only because the baked
# seed file's hardcoded IP (172.18.0.2) happens to match dev-env Docker network
# defaults. Writing into the container makes the dynamic IP injection actually take
# effect, so the test fails fast on networks where the snmp-agent isn't at .2.
CANARY_REQ_IN_CONTAINER="/opt/deltav/etc/imports/${FOREIGN_SOURCE}.xml"
docker exec -i delta-v-provisiond tee "${CANARY_REQ_IN_CONTAINER}" > /dev/null <<REQEOF
<model-import xmlns="http://xmlns.opennms.org/xsd/config/model-import"
              date-stamp="$(date -u +%Y-%m-%dT%H:%M:%S.000Z)"
              foreign-source="${FOREIGN_SOURCE}">
   <node location="Default" foreign-id="snmp-agent-canary" node-label="snmp-agent-canary">
      <interface ip-addr="${SNMP_AGENT_IP}" status="1" snmp-primary="P">
         <monitored-service service-name="ICMP"/>
         <monitored-service service-name="SNMP"/>
      </interface>
   </node>
</model-import>
REQEOF
ok "Requisition written into container at ${CANARY_REQ_IN_CONTAINER}"

# Update provisiond-configuration.xml to auto-import this foreign source.
# If an existing config has other requisition-defs (e.g., delta-v, mhuot-labs),
# we need to append ours without destroying theirs.
PROV_CONFIG="provisiond-overlay/etc/provisiond-configuration.xml"
mkdir -p provisiond-overlay/etc
PROVISIOND_NEEDS_RESTART=false
if [ ! -f "$PROV_CONFIG" ]; then
    log "  Writing new provisiond-configuration.xml with ${FOREIGN_SOURCE} import"
    cat > "$PROV_CONFIG" <<PROVEOF
<?xml version="1.0" encoding="UTF-8"?>
<provisiond-configuration xmlns="http://xmlns.opennms.org/xsd/config/provisiond-configuration"
  foreign-source-dir="/opt/deltav/etc/foreign-sources"
  requistion-dir="/opt/deltav/etc/imports"
  importThreads="4" scanThreads="4" rescanThreads="4" writeThreads="4" >
  <requisition-def import-name="${FOREIGN_SOURCE}"
                   import-url-resource="file:///opt/deltav/etc/imports/${FOREIGN_SOURCE}.xml">
    <cron-schedule>0/30 * * * * ?</cron-schedule>
  </requisition-def>
</provisiond-configuration>
PROVEOF
    PROVISIOND_NEEDS_RESTART=true
elif ! grep -q "import-name=\"${FOREIGN_SOURCE}\"" "$PROV_CONFIG"; then
    log "  Adding ${FOREIGN_SOURCE} requisition-def to existing provisiond-configuration.xml"
    # Insert the rpc-canary requisition-def before the closing </provisiond-configuration>
    sed -i.bak "s|</provisiond-configuration>|  <requisition-def import-name=\"${FOREIGN_SOURCE}\" import-url-resource=\"file:///opt/deltav/etc/imports/${FOREIGN_SOURCE}.xml\">\n    <cron-schedule>0/30 * * * * ?</cron-schedule>\n  </requisition-def>\n</provisiond-configuration>|" "$PROV_CONFIG"
    rm -f "${PROV_CONFIG}.bak"
    PROVISIOND_NEEDS_RESTART=true
fi

if $PROVISIOND_NEEDS_RESTART; then
    log "  Restarting Provisiond to pick up requisition-def..."
    docker compose restart provisiond
    if wait_for_health "provisiond" "http://localhost:8080/actuator/health" 120; then
        ok "Provisiond restarted and healthy"
    else
        fail "Provisiond not healthy after restart"
        exit 1
    fi
else
    ok "Provisiond configuration already includes ${FOREIGN_SOURCE}"
fi

# Wait for node to appear in DB
if wait_for_db \
    "SELECT count(*) FROM node WHERE foreignsource = '${FOREIGN_SOURCE}'" \
    "$PROVISION_TIMEOUT" "canary node in database" 10; then
    ok "Canary node provisioned"
else
    fail "Canary node not provisioned within ${PROVISION_TIMEOUT}s"
    show_diagnostics
    log ""
    log "Results: $PASS passed, $FAIL failed"
    exit 1
fi

# ══════════════════════════════════════════════════════════════════
# Phase 2: Detector Assertion — ICMP and SNMP both detected via Minion RPC
# ══════════════════════════════════════════════════════════════════
log ""
log "Phase 2: Verifying detectors executed on Minion via RPC..."

# Assertion: ICMP service detected
ICMP_QUERY="SELECT count(*) FROM ifservices s JOIN service svc ON s.serviceid = svc.serviceid JOIN ipinterface ip ON s.ipinterfaceid = ip.id JOIN node n ON ip.nodeid = n.nodeid WHERE n.foreignsource = '${FOREIGN_SOURCE}' AND svc.servicename = 'ICMP'"
if wait_for_db "$ICMP_QUERY" "$DETECT_TIMEOUT" "ICMP service detected (proves IcmpDetector ran via Minion RPC)" 10; then
    ok "ICMP service detected via Minion RPC"
else
    fail "ICMP service NOT detected within ${DETECT_TIMEOUT}s — IcmpDetector RPC path broken"
    show_diagnostics
    log ""
    log "Results: $PASS passed, $FAIL failed"
    exit 1
fi

# Assertion: SNMP service detected
SNMP_QUERY="SELECT count(*) FROM ifservices s JOIN service svc ON s.serviceid = svc.serviceid JOIN ipinterface ip ON s.ipinterfaceid = ip.id JOIN node n ON ip.nodeid = n.nodeid WHERE n.foreignsource = '${FOREIGN_SOURCE}' AND svc.servicename = 'SNMP'"
if wait_for_db "$SNMP_QUERY" "$DETECT_TIMEOUT" "SNMP service detected (proves SnmpDetector ran via Minion RPC)" 10; then
    ok "SNMP service detected via Minion RPC"
else
    fail "SNMP service NOT detected within ${DETECT_TIMEOUT}s — SnmpDetector RPC path broken OR snmp-agent community string mismatch"
    show_diagnostics
    log ""
    log "Hint: mock-snmp-agent default community is 'public'. If SnmpConfig overrides this,"
    log "      check that the community string in the SNMP config matches."
    log ""
    log "Results: $PASS passed, $FAIL failed"
    exit 1
fi

# Confirm the node's lastcapsdpoll timestamp is recent (detection actually completed)
LASTPOLL_EPOCH=$(psql_query "SELECT EXTRACT(EPOCH FROM lastcapsdpoll) FROM node WHERE foreignsource = '${FOREIGN_SOURCE}'" | head -1 | cut -d. -f1)
NOW_EPOCH=$(date +%s)
if [ -n "${LASTPOLL_EPOCH:-}" ] && [ "${LASTPOLL_EPOCH:-0}" -gt 0 ]; then
    AGE=$((NOW_EPOCH - LASTPOLL_EPOCH))
    if [ "$AGE" -lt 600 ]; then
        ok "Node lastcapsdpoll is recent (${AGE}s ago) — full detection scan completed"
    else
        log "  [WARN] Node lastcapsdpoll is stale (${AGE}s ago)"
    fi
else
    log "  [WARN] Node lastcapsdpoll is NULL — detection may not have completed"
fi
# ══════════════════════════════════════════════════════════════════
# Phase 3: Monitor Assertion — Pollerd is polling via Minion RPC
# ══════════════════════════════════════════════════════════════════
log ""
log "Phase 3: Verifying Pollerd is polling canary services via Minion RPC..."

# Wait for Pollerd to schedule the newly-detected services and make first poll attempts.
# Pollerd reacts to nodeGainedService events; first poll typically happens within 30-60s.
log "Waiting up to ${POLL_TIMEOUT}s for Pollerd to poll canary services..."

POLL_EVIDENCE_FOUND=false
ELAPSED=0
while [ $ELAPSED -lt "$POLL_TIMEOUT" ]; do
    # Look for Pollerd log entries mentioning the canary node or its IP.
    # Delta-V Pollerd's log format includes service + IP on poll dispatches.
    if docker compose logs --since="5m" pollerd 2>/dev/null | grep -qE "(snmp-agent-canary|${SNMP_AGENT_IP//./\\.})" ; then
        POLL_EVIDENCE_FOUND=true
        break
    fi
    sleep "$POLL_INTERVAL"
    ELAPSED=$((ELAPSED + POLL_INTERVAL))
    if [ $((ELAPSED % 60)) -eq 0 ]; then
        log "  ... ${ELAPSED}s elapsed"
    fi
done

if $POLL_EVIDENCE_FOUND; then
    ok "Pollerd poll activity observed for canary node (proves polling dispatched to Minion RPC)"
else
    fail "No Pollerd poll activity observed for canary node within ${POLL_TIMEOUT}s — Monitor RPC path may be broken"
    show_diagnostics
    log ""
    log "Results: $PASS passed, $FAIL failed"
    exit 1
fi

# Verify polls actually COMPLETED — not just dispatched. A poll that times out
# on Minion (e.g., a monitor that can't reach the target, or a result that fails
# to round-trip back) would still show dispatch in logs but never update
# lastgood/lastfail timestamps.
LASTPOLL_QUERY="SELECT count(*) FROM ifservices s JOIN ipinterface ip ON s.ipinterfaceid = ip.id JOIN node n ON ip.nodeid = n.nodeid WHERE n.foreignsource = '${FOREIGN_SOURCE}' AND (s.lastgood IS NOT NULL OR s.lastfail IS NOT NULL)"
if wait_for_db "$LASTPOLL_QUERY" 120 "poll results recorded (lastgood/lastfail timestamp)" 10; then
    ok "Poll results recorded — polls completed successfully via Minion RPC (not just dispatched)"
else
    fail "No poll results recorded within 120s — polls may be dispatched but timing out on Minion"
    show_diagnostics
    log ""
    log "Hint: check that the requisition IP (${SNMP_AGENT_IP}) is actually reachable from the Minion container."
    log "      The PSM page-sequence serialization bug (#125) is fixed; see project_psm_bug_false_positive memo."
    log "      Also check the pollerd outage table — feedback indicates intermittent row-write latency."
    log ""
    log "Results: $PASS passed, $FAIL failed"
    exit 1
fi

# No service should be in a stuck-open outage state immediately after detection.
# If the monitor IS running but cannot reach the service, an open outage would appear.
# If the monitor is running AND the service is reachable, the service is up and no open outage exists.
STUCK_OUTAGE_QUERY="SELECT count(*) FROM outages o JOIN ifservices s ON o.ifserviceid = s.id JOIN ipinterface ip ON s.ipinterfaceid = ip.id JOIN node n ON ip.nodeid = n.nodeid WHERE n.foreignsource = '${FOREIGN_SOURCE}' AND o.ifregainedservice IS NULL"
STUCK_OUTAGES=$(psql_query "$STUCK_OUTAGE_QUERY" || echo "0")
if [ "${STUCK_OUTAGES:-0}" -eq 0 ]; then
    ok "No stuck-open outages for canary services (services are reachable via Minion RPC)"
else
    fail "${STUCK_OUTAGES} stuck-open outage(s) on canary services — Monitor reports service unreachable via Minion RPC"
    show_diagnostics
    log ""
    log "Hint: If outages are for SNMP, verify mock-snmp-agent responds on UDP/161"
    log "      If outages are for ICMP, verify Minion has NET_RAW capability (docker-compose.yml minion.cap_add)"
    log ""
    log "Results: $PASS passed, $FAIL failed"
    exit 1
fi
# ══════════════════════════════════════════════════════════════════
# Phase 4: RPC Timeout Resilience
# ══════════════════════════════════════════════════════════════════
# Pause the Minion container to simulate a complete RPC black-hole.
# All RPC requests (polls, detections, collections) will timeout.
# Policy: RPC timeout = infrastructure problem, NOT service problem.
# No outages, no alarms, no fault events should be created.
#
# Timing: Default Kafka RPC TTL = 20s. With retry=2, worst case per
# poll = 3 × 20s = 60s. Pause window = 120s covers this + margin.
log ""
log "Phase 4: RPC timeout resilience (docker pause)..."

# 4a: Snapshot current state
OUTAGE_BASELINE=$(psql_query "SELECT count(*) FROM outages o
    JOIN ifservices s ON o.ifserviceid = s.id
    JOIN ipinterface ip ON s.ipinterfaceid = ip.id
    JOIN node n ON ip.nodeid = n.nodeid
    WHERE n.foreignsource = '${FOREIGN_SOURCE}'" || echo "0")
ALARM_BASELINE=$(psql_query "SELECT count(*) FROM alarms a
    JOIN node n ON a.nodeid = n.nodeid
    WHERE n.foreignsource = '${FOREIGN_SOURCE}'
      AND a.alarmtype = 1" || echo "0")
KAFKA_BASELINE=$(wc -l < "$FAULT_LOG" 2>/dev/null || echo "0")

# 4b: Pause Minion — all RPC requests will timeout
docker pause delta-v-minion
ok "Minion paused (RPC black-hole active)"

# 4c: Wait 120s for poll cycles to timeout
log "Waiting 120s for RPC timeouts to occur..."
sleep 120

# 4d: Unpause Minion
docker unpause delta-v-minion
ok "Minion unpaused"

# 4e: Negative Kafka assertion — no nodeLostService or dataCollectionFailed
# events should have been published during the pause window.
# IMPORTANT: grep returns exit code 1 when no match (which is SUCCESS here).
# Use if-! guard to prevent set -e from killing the script.
KAFKA_EVENTS_DURING_PAUSE=$(tail -n +"$((KAFKA_BASELINE + 1))" "$FAULT_LOG" 2>/dev/null || true)
if ! echo "$KAFKA_EVENTS_DURING_PAUSE" | grep -q "nodeLostService\|dataCollectionFailed" 2>/dev/null; then
    ok "No nodeLostService or dataCollectionFailed events on Kafka during pause"
else
    fail "False fault events detected on Kafka during Minion pause — RPC timeout created events"
    if $VERBOSE; then
        log "  Events during pause window:"
        echo "$KAFKA_EVENTS_DURING_PAUSE" | grep "nodeLostService\|dataCollectionFailed" || true
    fi
fi

# 4f: Assert no new outages
OUTAGE_AFTER=$(psql_query "SELECT count(*) FROM outages o
    JOIN ifservices s ON o.ifserviceid = s.id
    JOIN ipinterface ip ON s.ipinterfaceid = ip.id
    JOIN node n ON ip.nodeid = n.nodeid
    WHERE n.foreignsource = '${FOREIGN_SOURCE}'" || echo "0")
if [ "${OUTAGE_AFTER:-0}" -eq "${OUTAGE_BASELINE:-0}" ]; then
    ok "No new outages created during Minion pause (${OUTAGE_AFTER} total, unchanged)"
else
    fail "New outages created during pause: before=${OUTAGE_BASELINE}, after=${OUTAGE_AFTER}"
fi

# 4g: Assert no new problem alarms
ALARM_AFTER=$(psql_query "SELECT count(*) FROM alarms a
    JOIN node n ON a.nodeid = n.nodeid
    WHERE n.foreignsource = '${FOREIGN_SOURCE}'
      AND a.alarmtype = 1" || echo "0")
if [ "${ALARM_AFTER:-0}" -eq "${ALARM_BASELINE:-0}" ]; then
    ok "No new problem alarms created during Minion pause (${ALARM_AFTER} total, unchanged)"
else
    fail "New problem alarms during pause: before=${ALARM_BASELINE}, after=${ALARM_AFTER}"
    if $VERBOSE; then
        psql_query "SELECT a.alarmid, a.eventuei, a.severity, a.firsteventtime FROM alarms a JOIN node n ON a.nodeid = n.nodeid WHERE n.foreignsource = '${FOREIGN_SOURCE}' AND a.alarmtype = 1" || true
    fi
fi

# 4h: Wait for Minion health recovery
if wait_for_health minion "http://localhost:8080/actuator/health" 120; then
    ok "Minion health recovered after unpause"
else
    fail "Minion health check did not recover within 120s"
fi

# 4i: Verify polling resumes — lastgood should advance beyond pause start
RESUME_QUERY="SELECT count(*) FROM ifservices s
    JOIN ipinterface ip ON s.ipinterfaceid = ip.id
    JOIN node n ON ip.nodeid = n.nodeid
    WHERE n.foreignsource = '${FOREIGN_SOURCE}'
      AND s.lastgood > NOW() - INTERVAL '3 minutes'"
if wait_for_db "$RESUME_QUERY" 180 "polling resume (lastgood advancing)" 15; then
    ok "Polling resumed after Minion recovery (lastgood advancing)"
else
    fail "Polling did not resume within 180s after Minion unpause"
    show_diagnostics
fi

# ══════════════════════════════════════════════════════════════════
# Summary
# ══════════════════════════════════════════════════════════════════
if $VERBOSE; then
    show_diagnostics
fi

log ""
log "══════════════════════════════════════════════════════════════"
log "Results: $PASS passed, $FAIL failed"
log ""
log "Validated:"
log "  Phase 1: Canary node provisioned at location=Default"
log "  Phase 2: ICMP + SNMP detectors executed via Minion RPC"
log "  Phase 3: Pollerd polls dispatched, completed, and services reachable via Minion RPC"
log "  Phase 4: RPC timeout resilience — Minion paused 120s, zero false outages/alarms/events"
log "══════════════════════════════════════════════════════════════"

[ $FAIL -eq 0 ] || exit 1
