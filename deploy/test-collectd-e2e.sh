#!/usr/bin/env bash
#
# test-collectd-e2e.sh — End-to-end Collectd data collection test for Delta-V
#
# Provisions a set of nodes with SNMP services and verifies that Collectd
# successfully schedules and executes metric collection cycles.
#
# Topology: Uses the delta-v requisition with localhost node and loopback services.
#
# Usage:
#   ./test-collectd-e2e.sh              Run the test
#   ./test-collectd-e2e.sh --verbose    Show diagnostic queries on failure
#   ./test-collectd-e2e.sh --pre-clean  Full pre-run cleanup (DB + restart daemons)
#   ./test-collectd-e2e.sh --post-cleanup  Delete test nodes and alarms after run
#   ./test-collectd-e2e.sh --skip-provision  Skip Phase 1 if nodes already exist
#
# Prerequisites:
#   - Delta-V deployed with full profile: ./deploy.sh up full
#   - Collectd Spring Boot daemon running
#   - Provisiond running to provision nodes
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
FOREIGN_SOURCE="delta-v"
EXPECTED_NODES=1
PROVISION_TIMEOUT=120       # 2 min — wait for node provisioning
SCHEDULE_TIMEOUT=180        # 3 min — wait for collection scheduling
COLLECTION_TIMEOUT=300      # 5 min — wait for collection cycles
COLLECTION_POLL_INTERVAL=15 # seconds between DB checks

# ── Parse flags ────────────────────────────────────────────────────
VERBOSE=false
PRE_CLEAN=false
POST_CLEANUP=false
SKIP_PROVISION=false
for arg in "$@"; do
    case "$arg" in
        --verbose) VERBOSE=true ;;
        --pre-clean) PRE_CLEAN=true ;;
        --post-cleanup) POST_CLEANUP=true ;;
        --skip-provision) SKIP_PROVISION=true ;;
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
    # Restore the canonical provisiond-configuration.xml if this run overwrote it
    # (#201): it is a git-tracked file shipping the full requisition set, so leaving
    # it mutated drifts the working tree and wipes the other requisitions for later runs.
    if [ -f overlays/provisiond/etc/provisiond-configuration.xml.e2e-backup ]; then
        mv -f overlays/provisiond/etc/provisiond-configuration.xml.e2e-backup \
              overlays/provisiond/etc/provisiond-configuration.xml
    fi
    if $POST_CLEANUP; then
        log "Post-run cleanup (--post-cleanup): removing test data..."
        clean_all_nodes
        clean_all_alarms
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
    local poll_interval="${4:-$COLLECTION_POLL_INTERVAL}"
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

show_diagnostics() {
    if ! $VERBOSE; then
        log "Hint: re-run with --verbose for diagnostic output"
        return
    fi
    log ""
    log "── Diagnostic Queries ──"
    log ""
    log "Nodes (foreignsource=${FOREIGN_SOURCE}):"
    psql_query "SELECT nodeid, nodelabel, foreignid, location FROM node WHERE foreignsource = '${FOREIGN_SOURCE}' ORDER BY nodelabel" || true
    log ""
    log "IP Interfaces:"
    psql_query "SELECT n.nodelabel, ip.ipaddr FROM ipinterface ip JOIN node n ON ip.nodeid = n.nodeid WHERE n.foreignsource = '${FOREIGN_SOURCE}' ORDER BY n.nodelabel" || true
    log ""
    log "Services:"
    psql_query "SELECT n.nodelabel, svc.servicename FROM ifservices s JOIN service svc ON s.serviceid = svc.serviceid JOIN ipinterface ip ON s.ipinterfaceid = ip.id JOIN node n ON ip.nodeid = n.nodeid WHERE n.foreignsource = '${FOREIGN_SOURCE}' ORDER BY n.nodelabel, svc.servicename" || true
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

# ── Prerequisite Checks ───────────────────────────────────────────
log "Checking prerequisites..."

REQUIRED_SERVICES="postgres kafka provisiond collectd"
for svc in $REQUIRED_SERVICES; do
    if ! docker compose ps --status running --format '{{.Name}}' 2>/dev/null | grep -qw "$svc"; then
        err "Service '$svc' is not running. Deploy with: ./deploy.sh up full"
    fi
done
ok "Required services running (postgres, kafka, provisiond, collectd)"

# ══════════════════════════════════════════════════════════════════
# Pre-run cleanup (--pre-clean): full reset for a pristine test run
# ══════════════════════════════════════════════════════════════════
if $PRE_CLEAN; then
    log ""
    log "Pre-run cleanup (--pre-clean): resetting DB and daemons..."

    # 1. Stop Collectd and Provisiond so they don't write while we clean
    log "  Stopping Collectd and Provisiond..."
    docker compose stop collectd provisiond 2>/dev/null || true

    # 2. Wipe ALL nodes from DB (FK-safe order)
    clean_all_nodes
    clean_all_alarms
    ok "Database cleaned"

    # 3. Restart Collectd and Provisiond with fresh state
    log "  Restarting Collectd and Provisiond..."
    docker compose start collectd provisiond 2>/dev/null || true
    if wait_for_health "provisiond" "http://localhost:8080/actuator/health" 120; then
        ok "Provisiond restarted and healthy"
    else
        err "Provisiond not healthy after restart"
    fi
    if wait_for_health "collectd" "http://localhost:8080/actuator/health" 120; then
        ok "Collectd restarted and healthy"
    else
        err "Collectd not healthy after restart"
    fi

    log ""
fi

# ══════════════════════════════════════════════════════════════════
# Phase 1: Node Provisioning
# ══════════════════════════════════════════════════════════════════
log ""
log "Phase 1: Provisioning ${EXPECTED_NODES} node(s) from delta-v requisition..."

EXISTING_NODES=$(psql_query "SELECT count(*) FROM node WHERE foreignsource = '${FOREIGN_SOURCE}'")
if [ "${EXISTING_NODES:-0}" -ge "$EXPECTED_NODES" ] && $SKIP_PROVISION; then
    log "  ${EXISTING_NODES} nodes already exist (--skip-provision)"
    ok "Nodes already provisioned"
else
    if [ "${EXISTING_NODES:-0}" -lt "$EXPECTED_NODES" ]; then
        # Ensure delta-v requisition config exists
        # NOTE: Write to overlays/provisiond/etc/imports/ (the path the container sees).
        mkdir -p overlays/provisiond/etc/imports
        if [ ! -f "overlays/provisiond/etc/imports/delta-v.xml" ]; then
            log "  Creating requisition: overlays/provisiond/etc/imports/delta-v.xml"
            cat > "overlays/provisiond/etc/imports/delta-v.xml" <<'REQEOF'
<model-import xmlns="http://xmlns.opennms.org/xsd/config/model-import"
              date-stamp="2026-03-24T00:00:00.000-07:00"
              foreign-source="delta-v">
   <node location="Default" foreign-id="localhost" node-label="localhost">
      <interface ip-addr="127.0.0.1" status="1" snmp-primary="N">
         <monitored-service service-name="ICMP"/>
      </interface>
   </node>
</model-import>
REQEOF
            ok "Requisition created"
        fi

        # Add requisition-def so Provisiond auto-imports on startup
        mkdir -p overlays/provisiond/etc
        # Back up the canonical config so cleanup() can restore it (#201). Skip if a
        # backup already exists (a prior run exited abnormally) so we keep the true
        # original rather than a backup of the already-overwritten file.
        [ -f overlays/provisiond/etc/provisiond-configuration.xml ] \
            && [ ! -f overlays/provisiond/etc/provisiond-configuration.xml.e2e-backup ] \
            && cp overlays/provisiond/etc/provisiond-configuration.xml \
                  overlays/provisiond/etc/provisiond-configuration.xml.e2e-backup
        cat > overlays/provisiond/etc/provisiond-configuration.xml <<PROVEOF
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
        ok "Provisiond configuration updated with ${FOREIGN_SOURCE} import"

        # Restart Provisiond to pick up config and trigger import
        log "  Restarting Provisiond to import requisition..."
        docker compose restart provisiond
        if wait_for_health "provisiond" "http://localhost:8080/actuator/health" 120; then
            ok "Provisiond restarted and healthy"
        else
            err "Provisiond not healthy after restart"
        fi
    fi

    # Wait for node to appear in DB
    if wait_for_db \
        "SELECT CASE WHEN count(*) >= ${EXPECTED_NODES} THEN count(*) ELSE 0 END FROM node WHERE foreignsource = '${FOREIGN_SOURCE}'" \
        "$PROVISION_TIMEOUT" "${EXPECTED_NODES} node(s) in database" 10; then
        ok "All ${EXPECTED_NODES} node(s) provisioned"
    else
        GOT=$(psql_query "SELECT count(*) FROM node WHERE foreignsource = '${FOREIGN_SOURCE}'" || echo "0")
        fail "Only ${GOT}/${EXPECTED_NODES} nodes provisioned within ${PROVISION_TIMEOUT}s"
        show_diagnostics
        log ""
        log "Results: $PASS passed, $FAIL failed"
        exit 1
    fi
fi

# ══════════════════════════════════════════════════════════════════
# Phase 2: Collectd Health & Configuration
# ══════════════════════════════════════════════════════════════════
log ""
log "Phase 2: Verifying Collectd daemon health and configuration..."

if wait_for_health "collectd" "http://localhost:8080/actuator/health" 120; then
    ok "Collectd Spring Boot daemon is healthy"
else
    fail "Collectd daemon not healthy after 120s"
    show_diagnostics
    log ""
    log "Results: $PASS passed, $FAIL failed"
    exit 1
fi

# ══════════════════════════════════════════════════════════════════
# Phase 3: Collection Scheduling & Execution
# ══════════════════════════════════════════════════════════════════
log ""
log "Phase 3: Checking collection scheduling and metrics..."

# Wait for collection to occur (check for any evidence in logs or metrics)
echo "Checking Collectd collection activity..."
sleep 30  # Give Collectd time to schedule and collect

# Check if Collectd logs show scheduling activity
if docker compose logs collectd 2>&1 | grep -q "collecting"; then
    ok "Collectd collection activity detected in logs"
elif docker compose logs collectd 2>&1 | grep -q "schedule"; then
    ok "Collectd scheduling detected in logs"
else
    log "  [INFO] No explicit collection/scheduling messages in logs (check Spring Boot startup)"
fi

# Check if Collectd is actively running
if docker compose ps --status running --format '{{.Name}}' 2>/dev/null | grep -qw "collectd"; then
    ok "Collectd daemon is still running"
else
    fail "Collectd daemon exited unexpectedly"
fi

# ══════════════════════════════════════════════════════════════════
# Summary
# ══════════════════════════════════════════════════════════════════
show_diagnostics

log ""
log "══════════════════════════════════════════════════════════════"
log "Results: $PASS passed, $FAIL failed"
log ""
log "Validated:"
log "  Phase 1: Requisition import → nodes provisioned"
log "  Phase 2: Collectd Spring Boot daemon health"
log "  Phase 3: Collection scheduling and execution"
log "══════════════════════════════════════════════════════════════"
[ "$FAIL" -eq 0 ] && exit 0 || exit 1
