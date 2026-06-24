#!/usr/bin/env bash
#
# test-pollerd-restart-outage-e2e.sh — Restart-during-outage E2E for Delta-V Pollerd
#
# Closes Open Verification Item #4: no existing suite covers
#   outage open  ->  pollerd restart WHILE the outage is open  ->  resolution after recovery.
#
# This is the *active poll* init-path reconciliation (not the syslog/passive path): it proves
# that when Pollerd restarts with a pre-existing open outage in the DB, its init path adopts
# that outage and closes the SAME row on recovery — no orphaned open outage, no duplicate.
#
#   Phase 0: start a throwaway TCP listener on the compose network (the poll target).
#   Phase 1: provision a node at the target IP with the active "Minion-Health" service
#            (TcpMonitor, port 8181, 30s interval); Pollerd polls it via Minion RPC.
#   Phase 2: stop the listener  -> poll fails (connection refused) -> outage opens
#            (outages row, ifregainedservice IS NULL). Capture its outageid.
#   Phase 3: `docker compose restart pollerd` WHILE the outage is open.
#   Phase 4: restart the listener -> service recovers -> the SAME outageid gets
#            ifregainedservice set, and no second open outage exists.
#
# NFR1: poll execution stays Minion-only (RPC). NFR2: a real connection-refused Down is a
# genuine outage — distinct from an RPC timeout, which must never synthesize one.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
source "${SCRIPT_DIR}/test-lib.sh"

# ── Configuration ──────────────────────────────────────────────────
TARGET_NAME="deltav-restart-outage-target"
TARGET_IMAGE="alpine/socat:latest"
TARGET_PORT=8181                 # Minion-Health is a TcpMonitor service bound to 8181 in the catalog
SERVICE_NAME="Minion-Health"     # active TcpMonitor service; no hostname/banner params, 30s interval
NODE_LABEL="restart-outage-target"
FOREIGN_SOURCE="restart-outage-test"
PROVISION_TIMEOUT=120
OUTAGE_TIMEOUT=150               # ~4-5 poll cycles (30s) plus scheduling slack
RESOLVE_TIMEOUT=150

# ── Parse flags ────────────────────────────────────────────────────
VERBOSE=false
PRE_CLEAN=false
POST_CLEANUP=false
for arg in "$@"; do
    case "$arg" in
        --verbose) VERBOSE=true ;;
        --pre-clean) PRE_CLEAN=true ;;
        --post-cleanup) POST_CLEANUP=true ;;
        --help|-h) echo "Usage: $0 [--verbose] [--pre-clean] [--post-cleanup]"; exit 0 ;;
    esac
done

# ── Helpers ────────────────────────────────────────────────────────
PASS=0
FAIL=0

log()  { echo "==> $*"; }
ok()   { echo "  [PASS] $*"; PASS=$((PASS + 1)); }
fail() { echo "  [FAIL] $*"; FAIL=$((FAIL + 1)); }
err()  { echo "ERROR: $*" >&2; exit 2; }

psql_query() {
    docker compose exec -T -e PGPASSWORD=deltav postgres \
        psql -U deltav -d deltav -t -A -c "$1" 2>/dev/null
}

wait_for_db() {
    local query="$1" timeout="$2" description="$3" elapsed=0 result
    log "Waiting for $description (timeout: ${timeout}s)..."
    while [ "$elapsed" -lt "$timeout" ]; do
        result=$(psql_query "$query" 2>/dev/null || echo "")
        if [ -n "$result" ] && [ "$result" != "0" ]; then
            return 0
        fi
        sleep 3
        elapsed=$((elapsed + 3))
    done
    return 1
}

wait_for_healthy() {
    local container="$1" elapsed=0 health
    while [ "$elapsed" -lt 90 ]; do
        health=$(docker inspect --format='{{.State.Health.Status}}' "$container" 2>/dev/null || echo "unknown")
        if [ "$health" = "healthy" ]; then return 0; fi
        sleep 5
        elapsed=$((elapsed + 5))
    done
    return 1
}

# Outages for OUR provisioned service only (scoped by node label + service name).
outage_query() {
    local extra="$1"
    echo "SELECT count(*) FROM outages o
            JOIN ifservices s ON o.ifserviceid = s.id
            JOIN service svc ON s.serviceid = svc.serviceid
            JOIN ipinterface ip ON s.ipinterfaceid = ip.id
            JOIN node n ON ip.nodeid = n.nodeid
           WHERE n.nodelabel = '${NODE_LABEL}' AND svc.servicename = '${SERVICE_NAME}' ${extra}"
}

target_listener_up()   { docker exec -d "$TARGET_NAME" socat "TCP-LISTEN:${TARGET_PORT},reuseaddr,fork" OPEN:/dev/null; }
target_listener_down() { docker exec "$TARGET_NAME" pkill -x socat 2>/dev/null || true; }

cleanup() {
    docker rm -f "$TARGET_NAME" >/dev/null 2>&1 || true
    # Remove the in-container provisioning artifacts so the scheduler stops importing
    # the throwaway requisition and re-runs start clean. (Best-effort; container may
    # already be gone.)
    docker exec "$PROV" sh -c "rm -f /opt/deltav/etc/imports/${FOREIGN_SOURCE}.xml \
        /opt/deltav/etc/foreign-sources/${FOREIGN_SOURCE}.xml" 2>/dev/null || true
    docker exec "$PROV" sh -c "grep -q 'import-name=\"${FOREIGN_SOURCE}\"' /opt/deltav/etc/provisiond-configuration.xml 2>/dev/null" \
        && { TMP_C="$(mktemp)"; docker cp "${PROV}:/opt/deltav/etc/provisiond-configuration.xml" "$TMP_C" 2>/dev/null \
             && python3 -c "import re,sys; p=sys.argv[1]; fs=sys.argv[2]; s=open(p).read(); \
import_re=re.compile(r'\s*<requisition-def import-name=\"'+re.escape(fs)+r'\".*?</requisition-def>\n', re.S); \
open(p,'w').write(import_re.sub('', s))" "$TMP_C" "$FOREIGN_SOURCE" \
             && docker cp "$TMP_C" "${PROV}:/opt/deltav/etc/provisiond-configuration.xml" 2>/dev/null; \
             rm -f "$TMP_C"; } || true
    if $POST_CLEANUP; then
        log "Post-run cleanup (--post-cleanup): removing test node..."
        psql_query "DELETE FROM node WHERE nodelabel = '${NODE_LABEL}'" 2>/dev/null || true
    fi
}
PROV=delta-v-provisiond
trap cleanup EXIT

# ── Pre-run cleanup (--pre-clean) ─────────────────────────────────
if $PRE_CLEAN; then
    log "Pre-run cleanup (--pre-clean): removing any prior test node..."
    psql_query "DELETE FROM node WHERE nodelabel = '${NODE_LABEL}'" 2>/dev/null || true
    docker rm -f "$TARGET_NAME" >/dev/null 2>&1 || true
    ok "Prior test state cleared"
    log ""
fi

# ── Prerequisite Checks ───────────────────────────────────────────
log "Checking prerequisites..."
command -v docker >/dev/null 2>&1 || err "docker not found."

REQUIRED_SERVICES="postgres kafka provisiond pollerd minion-gateway"
for svc in $REQUIRED_SERVICES; do
    if ! docker compose ps --status running --format '{{.Name}}' 2>/dev/null | grep -qw "$svc"; then
        err "Service '$svc' is not running. Deploy with: docker compose up -d"
    fi
done
if ! docker ps --format '{{.Names}}' 2>/dev/null | grep -qw "delta-v-minion"; then
    err "Minion container is not running (poll execution is Minion-only, NFR1)."
fi
ok "All required services running (including Minion + minion-gateway)"

# ══════════════════════════════════════════════════════════════════
# Phase 0: Start the throwaway TCP target on the compose network
# ══════════════════════════════════════════════════════════════════
log ""
log "Phase 0: starting TCP target '${TARGET_NAME}' on the compose network..."

# Attach the target to the same docker network the Minion uses so polls can reach it.
NET=$(docker inspect -f '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{"\n"}}{{end}}' delta-v-minion 2>/dev/null | head -1)
[ -n "$NET" ] || err "Could not determine the Minion's docker network"

docker rm -f "$TARGET_NAME" >/dev/null 2>&1 || true
# Long-lived container with no listener yet; the listener is started/stopped via docker exec
# so the container IP stays stable across the outage (open = kill socat, recover = restart it).
docker run -d --name "$TARGET_NAME" --network "$NET" --entrypoint sh "$TARGET_IMAGE" \
    -c 'while true; do sleep 3600; done' >/dev/null \
    || err "Failed to start target container (image ${TARGET_IMAGE})"

TARGET_IP=$(docker inspect -f "{{(index .NetworkSettings.Networks \"$NET\").IPAddress}}" "$TARGET_NAME" 2>/dev/null)
[ -n "$TARGET_IP" ] || err "Could not determine target container IP on network ${NET}"
ok "Target up at ${TARGET_IP}:${TARGET_PORT} on network ${NET}"

target_listener_up
sleep 2
ok "Listener started on ${TARGET_PORT}"

# ══════════════════════════════════════════════════════════════════
# Phase 1: Provision a node at the target with the active service
# ══════════════════════════════════════════════════════════════════
log ""
log "Phase 1: provisioning '${NODE_LABEL}' at ${TARGET_IP} with active '${SERVICE_NAME}'..."

# Deliver provisioning artifacts DIRECTLY into the running provisiond container.
# The host overlays/provisiond/etc/ path is NOT mounted into provisiond — config
# is baked into the image and imports/ is a named volume seeded by an init sidecar
# (see test-minion-rpc-e2e.sh + project_provisiond_e2e_slowness_root_cause). Writing
# to the host path silently does nothing; write into the container instead.
PROV=delta-v-provisiond

# 1. Foreign source — empty detectors (the service is declared in the requisition,
#    so no scan-time detection is needed; this also avoids the 17-detector default).
docker exec -i "$PROV" tee "/opt/deltav/etc/foreign-sources/${FOREIGN_SOURCE}.xml" >/dev/null <<FSEOF
<?xml version="1.0" encoding="UTF-8"?>
<foreign-source xmlns="http://xmlns.opennms.org/xsd/config/foreign-source" name="${FOREIGN_SOURCE}">
    <scan-interval>1d</scan-interval>
    <detectors/>
    <policies/>
</foreign-source>
FSEOF

# 2. Requisition with the throwaway target IP + the active service.
docker exec -i "$PROV" tee "/opt/deltav/etc/imports/${FOREIGN_SOURCE}.xml" >/dev/null <<REQEOF
<?xml version="1.0" encoding="UTF-8"?>
<model-import xmlns="http://xmlns.opennms.org/xsd/config/model-import"
              foreign-source="${FOREIGN_SOURCE}"
              date-stamp="$(date -u +%Y-%m-%dT%H:%M:%S.000Z)">
    <node foreign-id="restart-outage" node-label="${NODE_LABEL}">
        <interface ip-addr="${TARGET_IP}" status="1" managed="true" snmp-primary="N">
            <monitored-service service-name="${SERVICE_NAME}"/>
        </interface>
    </node>
</model-import>
REQEOF

# 3. Add a requisition-def for our foreign source to the in-container config so the
#    scheduler imports it. Edit via a docker cp round-trip (no reliance on in-container
#    sed). 0/30 cron = first import within 30s; the def + node are removed in cleanup.
if ! docker exec "$PROV" grep -q "import-name=\"${FOREIGN_SOURCE}\"" /opt/deltav/etc/provisiond-configuration.xml; then
    TMP_CFG="$(mktemp)"
    docker cp "${PROV}:/opt/deltav/etc/provisiond-configuration.xml" "$TMP_CFG"
    python3 - "$TMP_CFG" "$FOREIGN_SOURCE" <<'PY'
import sys
path, fs = sys.argv[1], sys.argv[2]
defn = (f'  <requisition-def import-name="{fs}"\n'
        f'                   import-url-resource="file:///opt/deltav/etc/imports/{fs}.xml">\n'
        f'    <cron-schedule>0/30 * * * * ?</cron-schedule>\n'
        f'  </requisition-def>\n')
s = open(path).read()
s = s.replace('</provisiond-configuration>', defn + '</provisiond-configuration>')
open(path, 'w').write(s)
PY
    docker cp "$TMP_CFG" "${PROV}:/opt/deltav/etc/provisiond-configuration.xml"
    rm -f "$TMP_CFG"
fi
ok "Requisition, foreign source, and provisiond-config delivered into the container"

log "  Restarting Provisiond to import the requisition..."
docker compose restart provisiond
wait_for_healthy delta-v-provisiond || err "Provisiond not healthy after restart"

# Gate on the monitored-service row existing (not just the node, and not outages — there are
# none yet while the target is UP), so we don't advance to Phase 2 before the service is schedulable.
SVC_QUERY="SELECT count(*) FROM ifservices s
            JOIN service svc ON s.serviceid = svc.serviceid
            JOIN ipinterface ip ON s.ipinterfaceid = ip.id
            JOIN node n ON ip.nodeid = n.nodeid
           WHERE n.nodelabel = '${NODE_LABEL}' AND svc.servicename = '${SERVICE_NAME}'"
if wait_for_db "$SVC_QUERY" "$PROVISION_TIMEOUT" "service '${SERVICE_NAME}' on '${NODE_LABEL}'"; then
    ok "Node + service '${SERVICE_NAME}' provisioned"
else
    fail "Service '${SERVICE_NAME}' on '${NODE_LABEL}' not provisioned within ${PROVISION_TIMEOUT}s"
    log "Results: $PASS passed, $FAIL failed"; exit 1
fi

# Restart Pollerd so it rebuilds its in-memory poll schedule with the new node ID,
# then let it run a couple of poll cycles against the (currently up) target.
log "Restarting Pollerd to pick up the new node, then letting it poll..."
docker compose restart pollerd
wait_for_healthy delta-v-pollerd && ok "Pollerd healthy" || fail "Pollerd not healthy"
log "Waiting 45s for schedule rebuild + initial polls (service should be UP)..."
sleep 45

# ══════════════════════════════════════════════════════════════════
# Phase 2: Open a real poll-driven outage
# ══════════════════════════════════════════════════════════════════
log ""
log "Phase 2: stopping the listener to open an outage (poll -> connection refused)..."
target_listener_down
ok "Listener stopped on ${TARGET_PORT}"

if wait_for_db "$(outage_query "AND o.ifregainedservice IS NULL")" "$OUTAGE_TIMEOUT" "open outage for ${SERVICE_NAME}"; then
    OUTAGE_ID=$(psql_query "SELECT o.outageid FROM outages o
                      JOIN ifservices s ON o.ifserviceid = s.id
                      JOIN service svc ON s.serviceid = svc.serviceid
                      JOIN ipinterface ip ON s.ipinterfaceid = ip.id
                      JOIN node n ON ip.nodeid = n.nodeid
                     WHERE n.nodelabel = '${NODE_LABEL}' AND svc.servicename = '${SERVICE_NAME}'
                       AND o.ifregainedservice IS NULL
                     ORDER BY o.outageid DESC LIMIT 1")
    ok "Outage OPENED for ${SERVICE_NAME} (outageid=${OUTAGE_ID})"
else
    fail "No open outage for ${SERVICE_NAME} within ${OUTAGE_TIMEOUT}s (was it actively polled?)"
    log "Results: $PASS passed, $FAIL failed"; exit 1
fi

# ══════════════════════════════════════════════════════════════════
# Phase 3: Restart Pollerd WHILE the outage is open
# ══════════════════════════════════════════════════════════════════
log ""
log "Phase 3: restarting Pollerd while outage ${OUTAGE_ID} is open..."
docker compose restart pollerd
wait_for_healthy delta-v-pollerd && ok "Pollerd restarted and healthy" || fail "Pollerd not healthy after restart"

# The outage must survive the restart unchanged (still the same open row).
STILL_OPEN=$(psql_query "SELECT count(*) FROM outages WHERE outageid = ${OUTAGE_ID} AND ifregainedservice IS NULL" 2>/dev/null || echo "0")
if [ "${STILL_OPEN:-0}" -eq 1 ]; then
    ok "Outage ${OUTAGE_ID} still open after restart (adopted, not orphaned)"
else
    fail "Outage ${OUTAGE_ID} not in the expected open state after restart"
fi

# ══════════════════════════════════════════════════════════════════
# Phase 4: Recover and assert the SAME outage closes (no duplicate)
# ══════════════════════════════════════════════════════════════════
log ""
log "Phase 4: restarting the listener to recover the service..."
target_listener_up
ok "Listener restarted on ${TARGET_PORT}"

if wait_for_db "SELECT count(*) FROM outages WHERE outageid = ${OUTAGE_ID} AND ifregainedservice IS NOT NULL" "$RESOLVE_TIMEOUT" "outage ${OUTAGE_ID} to close"; then
    ok "SAME outage ${OUTAGE_ID} CLOSED after restart+recovery (Poller init-path reconciliation ✓)"
else
    fail "Outage ${OUTAGE_ID} not closed within ${RESOLVE_TIMEOUT}s after recovery"
fi

# No orphan/duplicate: there must be exactly zero *other* open outages for this service.
OPEN_NOW=$(psql_query "$(outage_query "AND o.ifregainedservice IS NULL")" 2>/dev/null || echo "0")
if [ "${OPEN_NOW:-0}" -eq 0 ]; then
    ok "No orphan/duplicate open outage remains for ${SERVICE_NAME}"
else
    fail "Found ${OPEN_NOW} unexpected open outage(s) for ${SERVICE_NAME} after recovery"
fi

# ══════════════════════════════════════════════════════════════════
# Results
# ══════════════════════════════════════════════════════════════════
if $VERBOSE; then
    log ""
    log "── Outage rows for ${SERVICE_NAME} on ${NODE_LABEL} ──"
    psql_query "SELECT o.outageid, o.iflostservice, o.ifregainedservice FROM outages o
                  JOIN ifservices s ON o.ifserviceid = s.id
                  JOIN service svc ON s.serviceid = svc.serviceid
                  JOIN ipinterface ip ON s.ipinterfaceid = ip.id
                  JOIN node n ON ip.nodeid = n.nodeid
                 WHERE n.nodelabel = '${NODE_LABEL}' AND svc.servicename = '${SERVICE_NAME}'
                 ORDER BY o.outageid" 2>/dev/null || true
fi

log ""
log "Results: $PASS passed, $FAIL failed"
log ""
log "Validated (OVI #4 — active poll init-path reconciliation):"
log "  Outage opened by a real Minion poll (connection refused, not RPC timeout — NFR2)"
log "  Pollerd restarted while the outage was open → outage adopted, not orphaned"
log "  Same outage closed on recovery → no duplicate, no stuck-open outage"
[ "$FAIL" -eq 0 ] && exit 0 || exit 1
