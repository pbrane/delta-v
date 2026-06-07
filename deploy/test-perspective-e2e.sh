#!/usr/bin/env bash
#
# test-perspective-e2e.sh -- PerspectivePollerd E2E test
#
# Provisions a "google.com" node at link-local 169.254.1.1 with a
# "Google-Search" service using PageSequenceMonitor. The PSM config
# uses ${nodelabel} as the hostname, so each Minion resolves "google.com"
# via DNS and polls https://google.com/ from its own vantage point.
#
# Creates an Application ("Google-Search-App") that maps the service to
# two perspective locations (Default + mhuot-labs), then verifies that
# PerspectivePollerd executes polls from both Minions without creating
# perspective outages. Phases 4/5 simulate a real service failure by
# DNS-blocking google.com on the Default Minion, verifying outage creation
# and recovery while confirming RPC timeouts do NOT create false outages.
#
# Usage:
#   ./test-perspective-e2e.sh              Run the test
#   ./test-perspective-e2e.sh --verbose    Show diagnostic queries on failure
#   ./test-perspective-e2e.sh --pre-clean  Delete prior test data before run
#   ./test-perspective-e2e.sh --post-cleanup  Delete test data after run
#
# Prerequisites:
#   - Delta-V deployed with full profile: ./deploy.sh up full
#   - perspectivepollerd, provisiond, pollerd, minion, postgres, kafka running
#   - Labbox Minion (mhuot-labs location) connected via SSH tunnel
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

# -- Configuration ----------------------------------------------------------
FOREIGN_SOURCE="perspective-test"
NODE_LABEL="google.com"
NODE_IP="169.254.1.1"
SERVICE_NAME="Google-Search"
APP_NAME="Google-Search-App"
LOCATION_A="Default"
LOCATION_B="mhuot-labs"

PROVISION_TIMEOUT=120
PERSPECTIVE_TIMEOUT=180
POLL_INTERVAL=10

# -- Parse flags ------------------------------------------------------------
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

# -- Helpers ----------------------------------------------------------------
PASS=0
FAIL=0

log()  { echo "==> $*"; }
ok()   { echo "  [PASS] $*"; PASS=$((PASS + 1)); }
fail() { echo "  [FAIL] $*"; FAIL=$((FAIL + 1)); }
err()  { echo "ERROR: $*" >&2; exit 2; }

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

show_diagnostics() {
    if ! $VERBOSE; then
        log "Hint: re-run with --verbose for diagnostic output"
        return
    fi
    log ""
    log "-- Diagnostic: node --"
    psql_query "SELECT nodeid, nodelabel, foreignsource, foreignid, location FROM node WHERE foreignsource = '${FOREIGN_SOURCE}'" || true
    log ""
    log "-- Diagnostic: ifservices --"
    psql_query "SELECT s.id, n.nodelabel, ip.ipaddr, st.servicename
                FROM ifservices s
                JOIN ipinterface ip ON s.ipinterfaceid = ip.id
                JOIN node n ON ip.nodeid = n.nodeid
                JOIN service st ON s.serviceid = st.serviceid
                WHERE n.foreignsource = '${FOREIGN_SOURCE}'" || true
    log ""
    log "-- Diagnostic: application --"
    psql_query "SELECT a.id, a.name FROM applications a WHERE a.name = '${APP_NAME}'" || true
    log ""
    log "-- Diagnostic: application_service_map --"
    psql_query "SELECT asm.appid, asm.ifserviceid
                FROM application_service_map asm
                JOIN applications a ON asm.appid = a.id
                WHERE a.name = '${APP_NAME}'" || true
    log ""
    log "-- Diagnostic: application_perspective_location_map --"
    psql_query "SELECT aplm.appid, aplm.monitoringlocationid
                FROM application_perspective_location_map aplm
                JOIN applications a ON aplm.appid = a.id
                WHERE a.name = '${APP_NAME}'" || true
    log ""
    log "-- Diagnostic: perspective outages --"
    psql_query "SELECT o.outageid, o.ifserviceid, o.perspective, o.iflostservice, o.ifregainedservice
                FROM outages o
                WHERE o.perspective IS NOT NULL
                  AND o.ifserviceid IN (SELECT s.id FROM ifservices s JOIN ipinterface ip ON s.ipinterfaceid = ip.id JOIN node n ON ip.nodeid = n.nodeid WHERE n.foreignsource = '${FOREIGN_SOURCE}')
                ORDER BY o.outageid DESC LIMIT 20" || true
    log ""
    log "-- Diagnostic: perspectivepollerd recent logs --"
    docker logs delta-v-perspectivepollerd 2>&1 | tail -30 || true
}

PROVISIOND_CONFIG="${SCRIPT_DIR}/overlays/provisiond/etc/provisiond-configuration.xml"
PROVISIOND_CONFIG_BACKUP="$(mktemp -t perspective-provisiond-config.XXXXXX.xml)"

cleanup() {
    docker compose exec -T kafka sh -c 'for p in $(ps -eo pid,args 2>/dev/null | grep kafka-console-consumer | grep -v grep | awk "{print \$1}"); do kill "$p" 2>/dev/null; done' || true
    rm -rf "$TEST_TMPDIR"
    # Always undo /etc/hosts override on Default Minion (safety net for early exit)
    docker exec -u root delta-v-minion sh -c \
        'grep -v "192.0.2.1" /etc/hosts > /tmp/h && cat /tmp/h > /etc/hosts && rm /tmp/h' 2>/dev/null || true
    # Restore provisiond-configuration.xml from the committed state (captured
    # before the mhuot-labs inject) so the working tree stays clean for other
    # E2E tests that depend on the canonical committed config.
    if [ -f "${PROVISIOND_CONFIG_BACKUP}" ]; then
        cp "${PROVISIOND_CONFIG_BACKUP}" "${PROVISIOND_CONFIG}"
        rm -f "${PROVISIOND_CONFIG_BACKUP}"
    fi
    if $POST_CLEANUP; then
        log "Post-run cleanup..."
        psql_query "DELETE FROM application_perspective_location_map WHERE appid IN (SELECT id FROM applications WHERE name = '${APP_NAME}')" || true
        psql_query "DELETE FROM application_service_map WHERE appid IN (SELECT id FROM applications WHERE name = '${APP_NAME}')" || true
        psql_query "DELETE FROM applications WHERE name = '${APP_NAME}'" || true
        psql_query "DELETE FROM outages WHERE ifserviceid IN (SELECT s.id FROM ifservices s JOIN ipinterface ip ON s.ipinterfaceid = ip.id JOIN node n ON ip.nodeid = n.nodeid WHERE n.foreignsource = '${FOREIGN_SOURCE}')" || true
        psql_query "DELETE FROM ifservices WHERE ipinterfaceid IN (SELECT id FROM ipinterface WHERE nodeid IN (SELECT nodeid FROM node WHERE foreignsource = '${FOREIGN_SOURCE}'))" || true
        psql_query "UPDATE ipinterface SET snmpinterfaceid = NULL WHERE nodeid IN (SELECT nodeid FROM node WHERE foreignsource = '${FOREIGN_SOURCE}')" || true
        psql_query "DELETE FROM snmpinterface WHERE nodeid IN (SELECT nodeid FROM node WHERE foreignsource = '${FOREIGN_SOURCE}')" || true
        psql_query "DELETE FROM ipinterface WHERE nodeid IN (SELECT nodeid FROM node WHERE foreignsource = '${FOREIGN_SOURCE}')" || true
        psql_query "DELETE FROM events WHERE nodeid IN (SELECT nodeid FROM node WHERE foreignsource = '${FOREIGN_SOURCE}')" || true
        psql_query "DELETE FROM node WHERE foreignsource = '${FOREIGN_SOURCE}'" || true
        log "  Test data cleaned"
    fi
}
trap cleanup EXIT

TEST_TMPDIR=$(mktemp -d)
FAULT_LOG="$TEST_TMPDIR/fault-events.log"

docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic deltav-fault-events \
    > "$FAULT_LOG" 2>/dev/null &
FAULT_CONSUMER_PID=$!
sleep 3

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

# ===========================================================================
# Prerequisites
# ===========================================================================
log "Checking prerequisites..."

RUNNING=$(docker compose ps --status running --format '{{.Name}}')
for svc in postgres kafka provisiond perspectivepollerd minion minion-gateway; do
    if ! echo "$RUNNING" | grep -q "$svc"; then
        err "Service '$svc' is not running. Deploy with: ./deploy.sh up full"
    fi
done
ok "Required services running (postgres, kafka, provisiond, perspectivepollerd, minion, minion-gateway)"

# v1.2.0-rc2 PR1: RPC channel migrated to gRPC bidi via minion-gateway.
# Default for opennms.minion.transport.rpc is "grpc" (matchIfMissing=true);
# rollback path opennms.minion.transport.rpc=kafka is opt-in only.
# Verify the gateway has accepted at least one RPC stream from the Default
# Minion before we drive perspective polling — without this, RPC traffic
# silently fails over to no path at all (no Kafka path, no gRPC path) since
# the migration is hard-cut at the channel level.
log "Checking gRPC RPC transport (rc2 PR1)..."
# Note: capture-then-grep avoids SIGPIPE under set -o pipefail. `grep -q`
# exits early on first match → upstream `docker logs` SIGPIPEs → pipefail
# treats whole pipeline as failure even though the match was found
# (per feedback_grep_q_sigpipe_in_pipefail).
RPC_STREAM_DEADLINE=$(( $(date +%s) + 30 ))
while (( $(date +%s) < RPC_STREAM_DEADLINE )); do
    GATEWAY_LOG=$(docker logs delta-v-minion-gateway 2>&1 || true)
    case "$GATEWAY_LOG" in
        *"RPC stream opened for minion="*"location=${LOCATION_A}"*) break ;;
    esac
    sleep 3
done
GATEWAY_LOG=$(docker logs delta-v-minion-gateway 2>&1 || true)
case "$GATEWAY_LOG" in
    *"RPC stream opened for minion="*"location=${LOCATION_A}"*) ;;
    *) err "minion-gateway never logged 'RPC stream opened' for location=${LOCATION_A}; gRPC RPC channel not live" ;;
esac
ok "gRPC RPC stream live for location=${LOCATION_A} (rc2 PR1)"
# The mhuot-labs perspective stream is only present when labbox SSH tunnel
# is up and the labbox Minion has connected. Logged-not-required: if it's
# missing, perspective polls from that side will not reach a Minion, which
# the existing Phase 3/4/5 assertions already cover.
case "$GATEWAY_LOG" in
    *"RPC stream opened for minion="*"location=${LOCATION_B}"*)
        ok "gRPC RPC stream live for location=${LOCATION_B} (rc2 PR1, labbox)" ;;
    *)
        log "  Note: no RPC stream from location=${LOCATION_B} yet — labbox tunnel may not be up" ;;
esac

# The committed provisiond-configuration.xml has the mhuot-labs requisition-def
# commented out (lab devices at 172.20.20.x need VPN). This test is the labbox-
# dependent path, so we inject the requisition-def as active, then restore the
# committed state on EXIT. Provisiond's import of mhuot-labs.xml (which has
# nodes with location="mhuot-labs") is what populates monitoringlocations with
# the mhuot-labs row — without this step, the LOC_COUNT check below would fail
# because only Default (+ nl6-lab) get registered.
cp "${PROVISIOND_CONFIG}" "${PROVISIOND_CONFIG_BACKUP}"
python3 - "${PROVISIOND_CONFIG}" <<'PYEOF'
import re, sys
path = sys.argv[1]
with open(path) as f: content = f.read()
active_pattern = re.compile(
    r'<requisition-def\s+import-name="mhuot-labs"[\s\S]+?</requisition-def>')
stripped = re.sub(r'<!--[\s\S]*?-->', '', content)
if active_pattern.search(stripped):
    sys.exit(0)
snippet = (
    '  <requisition-def import-name="mhuot-labs"\n'
    '                   import-url-resource="file:///opt/deltav/etc/imports/mhuot-labs.xml">\n'
    '    <cron-schedule>0/30 * * * * ?</cron-schedule>\n'
    '  </requisition-def>\n'
)
marker = '</provisiond-configuration>'
with open(path, 'w') as f: f.write(content.replace(marker, snippet + marker, 1))
PYEOF
docker restart delta-v-provisiond >/dev/null 2>&1
log "  Waiting up to 60s for provisiond to re-import mhuot-labs and register monitoringlocation..."
deadline=$(( $(date +%s) + 60 ))
while (( $(date +%s) < deadline )); do
    if [ "$(psql_query "SELECT count(*) FROM monitoringlocations WHERE id='${LOCATION_B}'" || echo 0)" = "1" ]; then
        break
    fi
    sleep 3
done

# Verify both monitoring locations exist
LOC_COUNT=$(psql_query "SELECT count(*) FROM monitoringlocations WHERE id IN ('${LOCATION_A}', '${LOCATION_B}')")
if [ "${LOC_COUNT:-0}" -ne 2 ]; then
    err "Need both locations (${LOCATION_A}, ${LOCATION_B}). Found: ${LOC_COUNT}"
fi
ok "Both monitoring locations exist (${LOCATION_A}, ${LOCATION_B})"

# ===========================================================================
# Pre-clean (optional)
# ===========================================================================
if $PRE_CLEAN; then
    log "Pre-run cleanup (--pre-clean)..."
    psql_query "DELETE FROM application_perspective_location_map WHERE appid IN (SELECT id FROM applications WHERE name = '${APP_NAME}')" || true
    psql_query "DELETE FROM application_service_map WHERE appid IN (SELECT id FROM applications WHERE name = '${APP_NAME}')" || true
    psql_query "DELETE FROM applications WHERE name = '${APP_NAME}'" || true
    psql_query "DELETE FROM outages WHERE ifserviceid IN (SELECT s.id FROM ifservices s JOIN ipinterface ip ON s.ipinterfaceid = ip.id JOIN node n ON ip.nodeid = n.nodeid WHERE n.foreignsource = '${FOREIGN_SOURCE}')" || true
    psql_query "DELETE FROM ifservices WHERE ipinterfaceid IN (SELECT id FROM ipinterface WHERE nodeid IN (SELECT nodeid FROM node WHERE foreignsource = '${FOREIGN_SOURCE}'))" || true
    psql_query "UPDATE ipinterface SET snmpinterfaceid = NULL WHERE nodeid IN (SELECT nodeid FROM node WHERE foreignsource = '${FOREIGN_SOURCE}')" || true
    psql_query "DELETE FROM snmpinterface WHERE nodeid IN (SELECT nodeid FROM node WHERE foreignsource = '${FOREIGN_SOURCE}')" || true
    psql_query "DELETE FROM ipinterface WHERE nodeid IN (SELECT nodeid FROM node WHERE foreignsource = '${FOREIGN_SOURCE}')" || true
    psql_query "DELETE FROM events WHERE nodeid IN (SELECT nodeid FROM node WHERE foreignsource = '${FOREIGN_SOURCE}')" || true
    psql_query "DELETE FROM node WHERE foreignsource = '${FOREIGN_SOURCE}'" || true
    # Restart Provisiond to trigger immediate re-import (otherwise we wait for cron)
    docker restart delta-v-provisiond >/dev/null 2>&1
    sleep 15
    ok "Prior test data cleaned and Provisiond restarted"
fi

# ===========================================================================
# Phase 1: Provision "google.com" node with Google-Search service
# ===========================================================================
log ""
log "Phase 1: Provisioning ${NODE_LABEL} node..."

REQUISITION_FILE="${SCRIPT_DIR}/overlays/provisiond/etc/imports/${FOREIGN_SOURCE}.xml"
FOREIGN_SOURCE_FILE="${SCRIPT_DIR}/overlays/provisiond/etc/foreign-sources/${FOREIGN_SOURCE}.xml"
TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%S.000Z")

# Foreign source with no detectors — node uses only explicitly provisioned services
mkdir -p "$(dirname "$FOREIGN_SOURCE_FILE")"
cat > "$FOREIGN_SOURCE_FILE" <<EOF
<foreign-source xmlns="http://xmlns.opennms.org/xsd/config/foreign-source"
                name="${FOREIGN_SOURCE}" date-stamp="${TIMESTAMP}">
  <scan-interval>1d</scan-interval>
  <detectors/>
  <policies/>
</foreign-source>
EOF

cat > "$REQUISITION_FILE" <<EOF
<model-import xmlns="http://xmlns.opennms.org/xsd/config/model-import"
              date-stamp="${TIMESTAMP}"
              foreign-source="${FOREIGN_SOURCE}">
   <node location="${LOCATION_A}" foreign-id="google-search" node-label="${NODE_LABEL}">
      <interface ip-addr="${NODE_IP}" status="1" snmp-primary="N">
         <monitored-service service-name="${SERVICE_NAME}"/>
      </interface>
   </node>
</model-import>
EOF
ok "Requisition and foreign source written (no detectors)"

# Ensure provisiond-configuration.xml has an *active* (uncommented)
# requisition-def for ${FOREIGN_SOURCE}. The committed config already
# includes perspective-test so the inject is a no-op in the happy path;
# the Python check skips comment blocks so a commented-out entry
# wouldn't fool us into leaving it inactive. PROVISIOND_CONFIG_BACKUP
# was already captured before the mhuot-labs inject, so cleanup()
# restores both injects in one shot.
python3 - "${PROVISIOND_CONFIG}" "${FOREIGN_SOURCE}" <<'PYEOF'
import re, sys
path, fs = sys.argv[1], sys.argv[2]
with open(path) as f: content = f.read()
active = re.compile(
    rf'<requisition-def\s+import-name="{re.escape(fs)}"[\s\S]+?</requisition-def>')
stripped = re.sub(r'<!--[\s\S]*?-->', '', content)
if active.search(stripped):
    sys.exit(0)
snippet = (
    f'  <requisition-def import-name="{fs}" import-url-resource="file:///opt/deltav/etc/imports/{fs}.xml">\n'
    f'    <cron-schedule>0 0/1 * * * ? *</cron-schedule>\n'
    f'  </requisition-def>\n'
)
marker = '</provisiond-configuration>'
with open(path, 'w') as f: f.write(content.replace(marker, snippet + marker, 1))
PYEOF
# Restart provisiond to pick up the config change. `docker compose ps
# --status running` shows the container even in its "restarting" state,
# which fooled the previous check into passing prematurely; instead we
# poll the /actuator/health endpoint until it returns 200 (or give up
# at 60s). The first restart for mhuot-labs inject already waited for
# the monitoringlocations row, so provisiond is known-healthy before we
# enter this block.
docker restart delta-v-provisiond >/dev/null 2>&1 || true
deadline=$(( $(date +%s) + 60 ))
prov_healthy=false
while (( $(date +%s) < deadline )); do
    if docker compose exec -T provisiond curl -sf http://localhost:8080/actuator/health >/dev/null 2>&1; then
        prov_healthy=true
        break
    fi
    sleep 3
done
if $prov_healthy; then
    ok "Provisiond has active ${FOREIGN_SOURCE} requisition-def and restarted healthy"
else
    fail "Provisiond failed to become healthy within 60s after restart"
    show_diagnostics
fi

# Wait for node to be provisioned
if wait_for_db \
    "SELECT count(*) FROM node WHERE foreignsource = '${FOREIGN_SOURCE}' AND nodelabel = '${NODE_LABEL}'" \
    "$PROVISION_TIMEOUT" \
    "node provisioning"; then
    ok "Node '${NODE_LABEL}' provisioned"
else
    fail "Node '${NODE_LABEL}' not provisioned within ${PROVISION_TIMEOUT}s"
    show_diagnostics
fi

# Verify the Google-Search service exists in ifservices
SVC_COUNT=$(psql_query "SELECT count(*)
    FROM ifservices s
    JOIN ipinterface ip ON s.ipinterfaceid = ip.id
    JOIN node n ON ip.nodeid = n.nodeid
    JOIN service st ON s.serviceid = st.serviceid
    WHERE n.foreignsource = '${FOREIGN_SOURCE}'
      AND st.servicename = '${SERVICE_NAME}'")
if [ "${SVC_COUNT:-0}" -ge 1 ]; then
    ok "${SERVICE_NAME} service exists on node"
else
    fail "${SERVICE_NAME} service not found in ifservices"
    show_diagnostics
fi

# ===========================================================================
# Phase 2: Create Application with perspective locations
# ===========================================================================
log ""
log "Phase 2: Creating Application with perspective locations..."

# Get the monitored service ID
IFSERVICE_ID=$(psql_query "SELECT s.id
    FROM ifservices s
    JOIN ipinterface ip ON s.ipinterfaceid = ip.id
    JOIN node n ON ip.nodeid = n.nodeid
    JOIN service st ON s.serviceid = st.serviceid
    WHERE n.foreignsource = '${FOREIGN_SOURCE}'
      AND st.servicename = '${SERVICE_NAME}'
    LIMIT 1")

if [ -z "$IFSERVICE_ID" ]; then
    fail "Could not find ifservice ID for ${SERVICE_NAME}"
    show_diagnostics
    log ""
    log "Results: $PASS passed, $FAIL failed"
    exit 1
fi

# Create application (idempotent)
psql_query "INSERT INTO applications (id, name) VALUES (nextval('opennmsnxtid'), '${APP_NAME}') ON CONFLICT (name) DO NOTHING" || true
APP_ID=$(psql_query "SELECT id FROM applications WHERE name = '${APP_NAME}'")
if [ -n "$APP_ID" ]; then
    ok "Application '${APP_NAME}' created (id=${APP_ID})"
else
    fail "Failed to create application"
    show_diagnostics
fi

# Link service to application
psql_query "INSERT INTO application_service_map (appid, ifserviceid) VALUES (${APP_ID}, ${IFSERVICE_ID}) ON CONFLICT DO NOTHING" || true
LINKED=$(psql_query "SELECT count(*) FROM application_service_map WHERE appid = ${APP_ID} AND ifserviceid = ${IFSERVICE_ID}")
if [ "${LINKED:-0}" -ge 1 ]; then
    ok "Service linked to application"
else
    fail "Failed to link service to application"
fi

# Add perspective locations
for LOC in "$LOCATION_A" "$LOCATION_B"; do
    psql_query "INSERT INTO application_perspective_location_map (appid, monitoringlocationid) VALUES (${APP_ID}, '${LOC}') ON CONFLICT DO NOTHING" || true
done
PLOC_COUNT=$(psql_query "SELECT count(*) FROM application_perspective_location_map WHERE appid = ${APP_ID}")
if [ "${PLOC_COUNT:-0}" -eq 2 ]; then
    ok "Both perspective locations mapped (${LOCATION_A}, ${LOCATION_B})"
else
    fail "Expected 2 perspective locations, found ${PLOC_COUNT}"
fi

# Restart PerspectivePollerd so its PerspectiveServiceTracker discovers the
# new application on startup. The tracker only re-queries the DB when it
# receives events (APPLICATION_CREATED, etc.) via Kafka — our raw SQL insert
# doesn't generate those events.
log "Restarting PerspectivePollerd to pick up application..."
docker restart delta-v-perspectivepollerd >/dev/null 2>&1
# Wait long enough for Kafka consumer to rejoin and RPC connections to stabilize.
# Too short → first polls timeout → phantom startup outages.
sleep 45
if docker compose ps --status running --format '{{.Name}}' | grep -q perspectivepollerd; then
    ok "PerspectivePollerd restarted"
else
    fail "PerspectivePollerd failed to restart"
fi

# ===========================================================================
# Phase 3: Wait for PerspectivePollerd to poll from both locations
# ===========================================================================
log ""
log "Phase 3: Verifying perspective polling..."

# Wait for perspective polls to complete from both locations.
# PerspectivePollerd sends polls via Kafka RPC to each Minion.
# On success: no perspective outages are created.
# On failure: perspectiveNodeLostService events + open outages.
# We wait for poll activity in the logs (DEBUG enabled) then verify no open outages.
log "Waiting for perspective polls to execute (timeout: ${PERSPECTIVE_TIMEOUT}s)..."
POLL_ELAPSED=0
POLLS_DETECTED=false

while [ $POLL_ELAPSED -lt "$PERSPECTIVE_TIMEOUT" ]; do
    # Check for poll scheduling evidence in logs
    POLL_COUNT=$(docker logs delta-v-perspectivepollerd 2>&1 | grep -c "onServicePerspectiveAdded\|Scheduling\|${SERVICE_NAME}" 2>/dev/null || echo "0")
    POLL_COUNT=$(echo "$POLL_COUNT" | tr -d '[:space:]')

    if [ "${POLL_COUNT:-0}" -gt 0 ]; then
        POLLS_DETECTED=true
        # Give time for polls to complete (RPC round-trip through Minion)
        sleep 30
        break
    fi

    sleep "$POLL_INTERVAL"
    POLL_ELAPSED=$((POLL_ELAPSED + POLL_INTERVAL))
    if [ $((POLL_ELAPSED % 30)) -eq 0 ]; then
        log "  ... ${POLL_ELAPSED}s elapsed"
    fi
done

if $POLLS_DETECTED; then
    ok "PerspectivePollerd scheduled perspective polls"
else
    # Even without log evidence, check the DB for outage state
    log "  No log evidence found — checking DB directly"
fi

# Verify no open perspective outages (steady-state healthy)
OPEN_OUTAGES=$(psql_query "SELECT count(*) FROM outages o
    WHERE o.perspective IS NOT NULL
      AND o.ifregainedservice IS NULL
      AND o.ifserviceid IN (
        SELECT s.id FROM ifservices s
        JOIN ipinterface ip ON s.ipinterfaceid = ip.id
        JOIN node n ON ip.nodeid = n.nodeid
        WHERE n.foreignsource = '${FOREIGN_SOURCE}'
      )")
if [ "${OPEN_OUTAGES:-0}" -eq 0 ]; then
    ok "No open perspective outages (healthy baseline)"
else
    fail "Expected 0 open perspective outages, found ${OPEN_OUTAGES}"
    show_diagnostics
fi

# Verify polls actually completed on Minion (not just dispatched).
LASTGOOD_QUERY="SELECT count(*) FROM ifservices s
    JOIN ipinterface ip ON s.ipinterfaceid = ip.id
    JOIN node n ON ip.nodeid = n.nodeid
    WHERE n.foreignsource = '${FOREIGN_SOURCE}'
      AND s.lastgood IS NOT NULL"
if wait_for_db "$LASTGOOD_QUERY" 120 "poll completion (lastgood timestamp)"; then
    ok "Perspective polls completed on Minion (lastgood recorded)"
else
    fail "No lastgood timestamps recorded — polls may be dispatched but timing out on Minion"
    show_diagnostics
fi

# ===========================================================================
# Phase 4: Simulate service failure from Default Minion
# ===========================================================================
# Block google.com on the Default Minion by redirecting DNS to a TEST-NET
# address (RFC 5737). PSM will resolve google.com → 192.0.2.1 → connection
# fails → Unavailable. This is a real service failure (monitor executed,
# target unreachable), not an infrastructure failure (RPC timeout).
#
# The mhuot-labs Minion (if connected) would still reach google.com, proving
# perspective isolation. If mhuot-labs RPC times out, the onTimedOut() fix
# ensures no false outage is created for that location.
log ""
log "Phase 4: Simulating service failure from Default Minion..."

# Record outage count before the block
OUTAGE_COUNT_BEFORE=$(psql_query "SELECT count(*) FROM outages o
    WHERE o.perspective = '${LOCATION_A}'
      AND o.ifserviceid IN (
        SELECT s.id FROM ifservices s
        JOIN ipinterface ip ON s.ipinterfaceid = ip.id
        JOIN node n ON ip.nodeid = n.nodeid
        WHERE n.foreignsource = '${FOREIGN_SOURCE}'
      )")
OUTAGE_COUNT_BEFORE=${OUTAGE_COUNT_BEFORE:-0}

# Block google.com on Default Minion (requires root — container runs as uid 10001)
docker exec -u root delta-v-minion sh -c 'echo "192.0.2.1 google.com www.google.com" >> /etc/hosts'
ok "Blocked google.com on Default Minion (→ 192.0.2.1)"

# Wait for perspective outage from Default location.
# Poll interval=30s, timeout=10s, retry=1 → worst case ~80s to detect failure.
OUTAGE_TIMEOUT=120
if wait_for_db \
    "SELECT count(*) FROM outages o
     WHERE o.perspective = '${LOCATION_A}'
       AND o.ifregainedservice IS NULL
       AND o.ifserviceid IN (
         SELECT s.id FROM ifservices s
         JOIN ipinterface ip ON s.ipinterfaceid = ip.id
         JOIN node n ON ip.nodeid = n.nodeid
         WHERE n.foreignsource = '${FOREIGN_SOURCE}'
       )" \
    "$OUTAGE_TIMEOUT" \
    "perspective outage from ${LOCATION_A}"; then
    ok "Perspective outage created for ${LOCATION_A} (service failure detected)"
else
    fail "No perspective outage from ${LOCATION_A} within ${OUTAGE_TIMEOUT}s"
    show_diagnostics
fi

# Verify corresponding perspective alarm was created.
PERSPECTIVE_ALARM_QUERY="SELECT count(*) FROM alarms a
    JOIN node n ON a.nodeid = n.nodeid
    WHERE n.foreignsource = '${FOREIGN_SOURCE}'
      AND a.eventuei = 'uei.opennms.org/perspective/nodes/nodeLostService'
      AND a.alarmtype = 1"
if wait_for_db "$PERSPECTIVE_ALARM_QUERY" 30 "perspective alarm creation"; then
    ok "Perspective alarm created (nodeLostService from ${LOCATION_A})"
else
    fail "No perspective alarm found for ${LOCATION_A}"
    show_diagnostics
fi

# Track 1 regression check: alarmd must have published the alarm to Kafka.
# Cheap key-only check (binary protobuf value is not decoded). 15s window — alarmd publishes
# the lifecycle event within a couple of seconds of the PG insert.
REDUCTION_KEY=$(psql_query "SELECT a.reductionkey FROM alarms a
    JOIN node n ON a.nodeid = n.nodeid
    WHERE n.foreignsource = '${FOREIGN_SOURCE}'
      AND a.eventuei = 'uei.opennms.org/perspective/nodes/nodeLostService'
      AND a.alarmtype = 1
    LIMIT 1")
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

# Verify mhuot-labs did NOT get a false outage from RPC timeout
MHUOT_OPEN=$(psql_query "SELECT count(*) FROM outages o
    WHERE o.perspective = '${LOCATION_B}'
      AND o.ifregainedservice IS NULL
      AND o.ifserviceid IN (
        SELECT s.id FROM ifservices s
        JOIN ipinterface ip ON s.ipinterfaceid = ip.id
        JOIN node n ON ip.nodeid = n.nodeid
        WHERE n.foreignsource = '${FOREIGN_SOURCE}'
      )")
if [ "${MHUOT_OPEN:-0}" -eq 0 ]; then
    ok "No false outage for ${LOCATION_B} (RPC timeout handled correctly)"
else
    fail "${LOCATION_B} has ${MHUOT_OPEN} open outage(s) — onTimedOut() may be broken"
fi

# ===========================================================================
# Phase 5: Restore service — outage should clear
# ===========================================================================
log ""
log "Phase 5: Restoring google.com on Default Minion..."

docker exec -u root delta-v-minion sh -c \
    'grep -v "192.0.2.1" /etc/hosts > /tmp/h && cat /tmp/h > /etc/hosts && rm /tmp/h'
ok "Unblocked google.com on Default Minion"

# Wait for all open perspective outages to clear.
# The wait_for_db helper returns success when the result is non-zero, so we
# invert: query returns 1 when NO open outages remain (count == 0).
RECOVERY_TIMEOUT=180
if wait_for_db \
    "SELECT CASE WHEN count(*) = 0 THEN 1 ELSE 0 END
     FROM outages o
     WHERE o.perspective IS NOT NULL
       AND o.ifregainedservice IS NULL
       AND o.ifserviceid IN (
         SELECT s.id FROM ifservices s
         JOIN ipinterface ip ON s.ipinterfaceid = ip.id
         JOIN node n ON ip.nodeid = n.nodeid
         WHERE n.foreignsource = '${FOREIGN_SOURCE}'
       )" \
    "$RECOVERY_TIMEOUT" \
    "perspective outage recovery (zero open outages)"; then
    ok "Perspective outage cleared for ${LOCATION_A} (service recovered)"
    ok "All perspective outages resolved — steady state restored"
else
    fail "Perspective outage from ${LOCATION_A} did not clear within ${RECOVERY_TIMEOUT}s"
    show_diagnostics
fi

# Verify the resolution event reached Kafka (authoritative — no events table).
if wait_for_kafka_event "$FAULT_LOG" "nodeRegainedService" 30 "perspective nodeRegainedService on Kafka"; then
    ok "Perspective nodeRegainedService event confirmed on Kafka"
else
    fail "nodeRegainedService event not seen on Kafka fault-events topic"
fi

# Verify alarm lifecycle: cleared by Alarmd, then deleted by Drools.
# Drools delete can happen very quickly, so accept either state.
ALARM_CLEARED_QUERY="SELECT count(*) FROM alarms a
    JOIN node n ON a.nodeid = n.nodeid
    WHERE n.foreignsource = '${FOREIGN_SOURCE}'
      AND a.eventuei = 'uei.opennms.org/perspective/nodes/nodeLostService'
      AND a.severity = 2"
ALARM_GONE_QUERY="SELECT CASE WHEN count(*) = 0 THEN 1 ELSE 0 END FROM alarms a
    JOIN node n ON a.nodeid = n.nodeid
    WHERE n.foreignsource = '${FOREIGN_SOURCE}'
      AND a.eventuei = 'uei.opennms.org/perspective/nodes/nodeLostService'
      AND a.alarmtype = 1"
sleep 5
CLEARED=$(psql_query "$ALARM_CLEARED_QUERY" || echo "0")
GONE=$(psql_query "$ALARM_GONE_QUERY" || echo "0")
if [ "${CLEARED:-0}" -gt 0 ]; then
    ok "Perspective alarm CLEARED (Drools delete pending)"
elif [ "${GONE:-0}" -gt 0 ]; then
    ok "Perspective alarm deleted by Drools (fast clear+delete cycle)"
else
    fail "Perspective alarm neither cleared nor deleted — Alarmd/Drools issue"
    show_diagnostics
fi

# ===========================================================================
# Summary
# ===========================================================================
if [ $FAIL -gt 0 ]; then
    show_diagnostics
fi

log ""
log "Results: $PASS passed, $FAIL failed"
log ""
log "Validated:"
log "  Phase 1: Requisition + foreign source → google.com node provisioned"
log "  Phase 2: Application created with Default + mhuot-labs perspectives"
log "  Phase 3: Perspective polls completed (lastgood), no open outages"
log "  Phase 4: DNS block → outage + alarm created for Default, no false outage for mhuot-labs"
log "  Phase 5: DNS unblock → outage cleared, alarm cleared/deleted, nodeRegainedService on Kafka"
[ $FAIL -eq 0 ] || exit 1
