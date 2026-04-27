#!/usr/bin/env bash
#
# test-enlinkd-e2e.sh — End-to-end Enlinkd link discovery test for Delta-V
#
# Provisions Containerlab cEOS nodes (on labbox) via the mhuot-labs requisition,
# verifies SNMP data gathering through the remote Minion, and waits for Enlinkd
# to discover layer-2/layer-3 links between the nodes.
#
# Topology (Containerlab on labbox):
#
#   sub1 (172.20.20.2) ── site1 (172.20.20.3) ── hub1 (172.20.20.4)
#                                                  │
#                          site2 (172.20.20.6) ── hub2 (172.20.20.5)
#
# All nodes are Arista cEOS containers with LLDP enabled.
# The Minion (minion-mhuot-labs) runs on labbox at location "mhuot-labs",
# connected to the same Docker bridge (clab) as the cEOS nodes.
#
# Usage:
#   ./test-enlinkd-e2e.sh              Run the test
#   ./test-enlinkd-e2e.sh --verbose    Show diagnostic queries on failure
#   ./test-enlinkd-e2e.sh --pre-clean  Full pre-run cleanup (DB + restart daemons + clear Kafka)
#   ./test-enlinkd-e2e.sh --post-cleanup  Delete test nodes and alarms after run
#   ./test-enlinkd-e2e.sh --skip-provision  Skip Phase 1 if nodes already exist
#
# Prerequisites:
#   - Delta-V deployed with full profile: ./deploy.sh up full
#   - Minion running on labbox at location "mhuot-labs"
#   - Containerlab cEOS topology running on labbox (172.20.20.0/24)
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
FOREIGN_SOURCE="mhuot-labs"
EXPECTED_NODES=5
PROVISION_TIMEOUT=300      # 5 min — remote Minion SNMP scanning can be slow
SNMP_IFACE_TIMEOUT=300     # 5 min — wait for snmpInterface records
ENLINKD_TIMEOUT=600        # 10 min — Enlinkd initial_sleep_time=60s + collection + topology
ENLINKD_POLL_INTERVAL=15   # seconds between DB checks

# Expected cEOS nodes
declare -A NODES=(
    [hub1]="172.20.20.4"
    [hub2]="172.20.20.5"
    [site1]="172.20.20.3"
    [site2]="172.20.20.6"
    [sub1]="172.20.20.2"
)

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

PROVISIOND_CONFIG="provisiond-overlay/etc/provisiond-configuration.xml"
PROVISIOND_CONFIG_BACKUP="$(mktemp -t enlinkd-provisiond-config.XXXXXX.xml)"

cleanup() {
    if $POST_CLEANUP; then
        log "Post-run cleanup (--post-cleanup): removing test data..."
        clean_all_nodes
        clean_all_alarms
    fi
    # Restore provisiond-configuration.xml from the committed state (captured
    # in Phase 0 before any mutation) so this test never leaves the working
    # tree mutilated for subsequent E2E runs that depend on the full set of
    # requisition-defs.
    if [ -f "${PROVISIOND_CONFIG_BACKUP}" ]; then
        cp "${PROVISIOND_CONFIG_BACKUP}" "${PROVISIOND_CONFIG}"
        rm -f "${PROVISIOND_CONFIG_BACKUP}"
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
    local poll_interval="${4:-$ENLINKD_POLL_INTERVAL}"
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
    psql_query "SELECT n.nodelabel, ip.ipaddr, ip.snmpprimarytype FROM ipinterface ip JOIN node n ON ip.nodeid = n.nodeid WHERE n.foreignsource = '${FOREIGN_SOURCE}' ORDER BY n.nodelabel" || true
    log ""
    log "Services:"
    psql_query "SELECT n.nodelabel, svc.servicename FROM ifservices s JOIN service svc ON s.serviceid = svc.serviceid JOIN ipinterface ip ON s.ipinterfaceid = ip.id JOIN node n ON ip.nodeid = n.nodeid WHERE n.foreignsource = '${FOREIGN_SOURCE}' ORDER BY n.nodelabel, svc.servicename" || true
    log ""
    log "SNMP Interfaces (sample):"
    psql_query "SELECT n.nodelabel, si.snmpifindex, si.snmpifname, si.snmpiftype FROM snmpinterface si JOIN node n ON si.nodeid = n.nodeid WHERE n.foreignsource = '${FOREIGN_SOURCE}' ORDER BY n.nodelabel, si.snmpifindex LIMIT 30" || true
    log ""
    log "LLDP Elements:"
    psql_query "SELECT n.nodelabel, le.lldpchassisid, le.lldpsysname FROM lldpelement le JOIN node n ON le.nodeid = n.nodeid WHERE n.foreignsource = '${FOREIGN_SOURCE}' ORDER BY n.nodelabel" || true
    log ""
    log "LLDP Links:"
    psql_query "SELECT n.nodelabel, ll.lldpportid, ll.lldpremchassisid, ll.lldpremsysname, ll.lldpremportid FROM lldplink ll JOIN node n ON ll.nodeid = n.nodeid WHERE n.foreignsource = '${FOREIGN_SOURCE}' ORDER BY n.nodelabel" || true
    log ""
    log "CDP Elements:"
    psql_query "SELECT n.nodelabel, ce.cdpglobaldeviceid FROM cdpelement ce JOIN node n ON ce.nodeid = n.nodeid WHERE n.foreignsource = '${FOREIGN_SOURCE}' ORDER BY n.nodelabel" || true
    log ""
    log "CDP Links:"
    psql_query "SELECT n.nodelabel, cl.cdpinterfacename, cl.cdpcachedeviceid, cl.cdpcachedeviceport FROM cdplink cl JOIN node n ON cl.nodeid = n.nodeid WHERE n.foreignsource = '${FOREIGN_SOURCE}' ORDER BY n.nodelabel" || true
}

wait_for_healthy() {
    local container="$1"
    local timeout="${2:-120}"
    local elapsed=0
    while [ $elapsed -lt "$timeout" ]; do
        HEALTH=$(docker inspect --format='{{.State.Health.Status}}' "$container" 2>/dev/null || echo "unknown")
        if [ "$HEALTH" = "healthy" ]; then return 0; fi
        sleep 5
        elapsed=$((elapsed + 5))
    done
    return 1
}

# ── Prerequisite Checks ───────────────────────────────────────────
log "Checking prerequisites..."

REQUIRED_SERVICES="postgres kafka provisiond enlinkd minion-gateway"
for svc in $REQUIRED_SERVICES; do
    if ! docker compose ps --status running --format '{{.Name}}' 2>/dev/null | grep -qw "$svc"; then
        err "Service '$svc' is not running. Deploy with: ./deploy.sh up full"
    fi
done
ok "Required services running (postgres, kafka, provisiond, enlinkd, minion-gateway)"

# v1.2.0-rc2 PR1: RPC channel migrated to gRPC bidi via minion-gateway.
# Default for opennms.minion.transport.rpc is "grpc" (matchIfMissing=true).
# Enlinkd's SNMP RPCs to the labbox Minion now flow through the gateway's
# bidi gRPC stream rather than the OpenNMS.mhuot-labs.rpc-request Kafka
# topic. Verify the stream is live before running the topology assertions
# that depend on it.
log "Checking gRPC RPC transport for location=${FOREIGN_SOURCE} (rc2 PR1)..."
RPC_STREAM_DEADLINE=$(( $(date +%s) + 30 ))
while (( $(date +%s) < RPC_STREAM_DEADLINE )); do
    if docker logs delta-v-minion-gateway 2>&1 | grep -q "RPC stream opened for minion=.* location=${FOREIGN_SOURCE}"; then
        break
    fi
    sleep 3
done
if ! docker logs delta-v-minion-gateway 2>&1 | grep -q "RPC stream opened for minion=.* location=${FOREIGN_SOURCE}"; then
    err "minion-gateway never logged 'RPC stream opened' for location=${FOREIGN_SOURCE}; gRPC RPC channel not live (is the labbox SSH tunnel up?)"
fi
ok "gRPC RPC stream live for location=${FOREIGN_SOURCE} (rc2 PR1)"

# ══════════════════════════════════════════════════════════════════
# Pre-run cleanup (--pre-clean): full reset for a pristine test run
# ══════════════════════════════════════════════════════════════════
if $PRE_CLEAN; then
    log ""
    log "Pre-run cleanup (--pre-clean): resetting DB, daemons, and Kafka topics..."

    # 1. Stop Enlinkd and Provisiond so they don't write while we clean
    log "  Stopping Enlinkd and Provisiond..."
    docker compose stop enlinkd provisiond 2>/dev/null || true

    # 2. Wipe ALL nodes from DB (FK-safe order) — not just mhuot-labs
    clean_all_nodes
    clean_all_alarms
    ok "Database cleaned"

    # 3. Delete mhuot-labs RPC Kafka topics so no stale requests linger
    log "  Clearing mhuot-labs Kafka RPC topics..."
    KAFKA_CONTAINER=$(docker compose ps -q kafka)
    # The RPC topic pattern is {instanceId}.{location}.rpc
    for topic in $(docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server localhost:9092 --list 2>/dev/null | grep -i "mhuot-labs" || true); do
        log "    Deleting topic: $topic"
        docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh \
            --bootstrap-server localhost:9092 --delete --topic "$topic" 2>/dev/null || true
    done
    ok "Kafka topics cleaned"

    # 4. Restart Enlinkd and Provisiond with fresh state
    log "  Restarting Enlinkd and Provisiond..."
    docker compose start enlinkd provisiond 2>/dev/null || true
    wait_for_healthy delta-v-provisiond 120 || err "Provisiond not healthy after restart"
    wait_for_healthy delta-v-enlinkd 120 || err "Enlinkd not healthy after restart"
    ok "Daemons restarted with clean state"

    log ""
fi

# ══════════════════════════════════════════════════════════════════
# Phase 0: Ensure provisioning configuration exists
# ══════════════════════════════════════════════════════════════════
log ""
log "Phase 0: Ensuring provisioning configuration..."

PROVISIOND_NEEDS_RESTART=false

# ── Requisition: provisiond-overlay/etc/imports/mhuot-labs.xml ──
# NOTE: The container sees provisiond-overlay/etc/imports/ (not etc/imports/).
# On macOS Docker Desktop the parent bind mount shadows the child.
mkdir -p provisiond-overlay/etc/imports
if [ ! -f "provisiond-overlay/etc/imports/mhuot-labs.xml" ]; then
    log "  Creating requisition: provisiond-overlay/etc/imports/mhuot-labs.xml"
    cat > "provisiond-overlay/etc/imports/mhuot-labs.xml" <<'REQEOF'
<model-import xmlns="http://xmlns.opennms.org/xsd/config/model-import"
              date-stamp="2026-03-23T00:00:00.000-07:00"
              foreign-source="mhuot-labs">
   <node location="mhuot-labs" foreign-id="hub1" node-label="hub1">
      <interface ip-addr="172.20.20.4" status="1" snmp-primary="P">
         <monitored-service service-name="SNMP"/>
         <monitored-service service-name="ICMP"/>
      </interface>
   </node>
   <node location="mhuot-labs" foreign-id="hub2" node-label="hub2">
      <interface ip-addr="172.20.20.5" status="1" snmp-primary="P">
         <monitored-service service-name="SNMP"/>
         <monitored-service service-name="ICMP"/>
      </interface>
   </node>
   <node location="mhuot-labs" foreign-id="site1" node-label="site1">
      <interface ip-addr="172.20.20.3" status="1" snmp-primary="P">
         <monitored-service service-name="SNMP"/>
         <monitored-service service-name="ICMP"/>
      </interface>
   </node>
   <node location="mhuot-labs" foreign-id="site2" node-label="site2">
      <interface ip-addr="172.20.20.6" status="1" snmp-primary="P">
         <monitored-service service-name="SNMP"/>
         <monitored-service service-name="ICMP"/>
      </interface>
   </node>
   <node location="mhuot-labs" foreign-id="sub1" node-label="sub1">
      <interface ip-addr="172.20.20.2" status="1" snmp-primary="P">
         <monitored-service service-name="SNMP"/>
         <monitored-service service-name="ICMP"/>
      </interface>
   </node>
</model-import>
REQEOF
    PROVISIOND_NEEDS_RESTART=true
    ok "Requisition created"
else
    ok "Requisition already exists"
fi

# ── Foreign source: provisiond-overlay/etc/foreign-sources/mhuot-labs.xml ──
mkdir -p provisiond-overlay/etc/foreign-sources
if [ ! -f "provisiond-overlay/etc/foreign-sources/mhuot-labs.xml" ]; then
    log "  Creating foreign source: mhuot-labs.xml (ICMP + SNMP detectors only)"
    cat > "provisiond-overlay/etc/foreign-sources/mhuot-labs.xml" <<'FSEOF'
<?xml version="1.0" encoding="UTF-8"?>
<foreign-source xmlns="http://xmlns.opennms.org/xsd/config/foreign-source" name="mhuot-labs">
    <scan-interval>1d</scan-interval>
    <detectors>
        <detector name="ICMP" class="org.opennms.netmgt.provision.detector.icmp.IcmpDetector"/>
        <detector name="SNMP" class="org.opennms.netmgt.provision.detector.snmp.SnmpDetector"/>
    </detectors>
    <policies/>
</foreign-source>
FSEOF
    PROVISIOND_NEEDS_RESTART=true
    ok "Foreign source created"
else
    ok "Foreign source already exists"
fi

# ── Provisiond config: provisiond-overlay/etc/provisiond-configuration.xml ──
# Ensure the mhuot-labs requisition-def is an *active* (uncommented) entry.
# The committed config has mhuot-labs wrapped in an XML comment block because
# the real lab devices at 172.20.20.x require VPN connectivity that most
# developers don't have. This test IS the labbox-dependent path, so we
# temporarily activate the requisition-def here and restore the committed
# state on EXIT (cleanup() trap).
cp "${PROVISIOND_CONFIG}" "${PROVISIOND_CONFIG_BACKUP}"
log "  Ensuring active mhuot-labs requisition-def in ${PROVISIOND_CONFIG}"
python3 - "${PROVISIOND_CONFIG}" <<'PYEOF'
import re, sys
path = sys.argv[1]
with open(path) as f: content = f.read()
active_pattern = re.compile(
    r'<requisition-def\s+import-name="mhuot-labs"[\s\S]+?</requisition-def>')
# Strip from any XML comment blocks so we can detect commented-out entries
stripped = re.sub(r'<!--[\s\S]*?-->', '', content)
if active_pattern.search(stripped):
    print("  (mhuot-labs already active — no change needed)")
    sys.exit(0)
snippet = (
    '  <requisition-def import-name="mhuot-labs"\n'
    '                   import-url-resource="file:///opt/deltav/etc/imports/mhuot-labs.xml">\n'
    '    <cron-schedule>0/30 * * * * ?</cron-schedule>\n'
    '  </requisition-def>\n'
)
marker = '</provisiond-configuration>'
if marker not in content:
    sys.exit(f"marker {marker!r} not found in {path}")
new_content = content.replace(marker, snippet + marker, 1)
with open(path, 'w') as f: f.write(new_content)
print("  (mhuot-labs requisition-def injected active)")
PYEOF
PROVISIOND_NEEDS_RESTART=true
ok "Provisiond configuration has active mhuot-labs requisition-def"

# ══════════════════════════════════════════════════════════════════
# Phase 0b: Clean ALL prior node data (lightweight — skipped if --pre-clean already ran)
# ══════════════════════════════════════════════════════════════════
if ! $PRE_CLEAN; then
    log ""
    log "Phase 0b: Cleaning ALL prior node data from database..."

    PRIOR_COUNT=$(psql_query "SELECT count(*) FROM node" || echo "0")
    if [ "${PRIOR_COUNT:-0}" -gt 0 ]; then
        clean_all_nodes
        ok "Prior test data cleaned (${PRIOR_COUNT} nodes removed)"
        PROVISIOND_NEEDS_RESTART=true
    else
        ok "No prior node data to clean"
    fi
else
    log ""
    log "Phase 0b: Skipped (already cleaned by --pre-clean)"
fi

# ══════════════════════════════════════════════════════════════════
# Phase 1: Node Provisioning
# ══════════════════════════════════════════════════════════════════
log ""
log "Phase 1: Provisioning ${EXPECTED_NODES} cEOS nodes from mhuot-labs requisition..."

EXISTING_NODES=$(psql_query "SELECT count(*) FROM node WHERE foreignsource = '${FOREIGN_SOURCE}'")
if [ "${EXISTING_NODES:-0}" -ge "$EXPECTED_NODES" ] && $SKIP_PROVISION; then
    log "  ${EXISTING_NODES} nodes already exist (--skip-provision)"
    ok "Nodes already provisioned"
else
    if [ "${EXISTING_NODES:-0}" -ge "$EXPECTED_NODES" ] && ! $PROVISIOND_NEEDS_RESTART; then
        log "  ${EXISTING_NODES} nodes already exist, verifying..."
    else
        # Restart Provisiond to pick up config changes and trigger the import
        log "  Restarting Provisiond to import requisition..."
        docker compose restart provisiond
        wait_for_healthy delta-v-provisiond 120 || err "Provisiond not healthy after restart"
        ok "Provisiond restarted and healthy"
    fi

    # Wait for all nodes to appear
    if wait_for_db \
        "SELECT CASE WHEN count(*) >= ${EXPECTED_NODES} THEN count(*) ELSE 0 END FROM node WHERE foreignsource = '${FOREIGN_SOURCE}'" \
        "$PROVISION_TIMEOUT" "${EXPECTED_NODES} nodes in database" 10; then
        ok "All ${EXPECTED_NODES} nodes provisioned"
    else
        # Check how many we got
        GOT=$(psql_query "SELECT count(*) FROM node WHERE foreignsource = '${FOREIGN_SOURCE}'" || echo "0")
        if [ "${GOT:-0}" -gt 0 ]; then
            fail "Only ${GOT}/${EXPECTED_NODES} nodes provisioned within ${PROVISION_TIMEOUT}s"
        else
            fail "No nodes provisioned within ${PROVISION_TIMEOUT}s"
            show_diagnostics
            log ""
            log "Results: $PASS passed, $FAIL failed"
            exit 1
        fi
    fi
fi

# Verify each expected node
for label in "${!NODES[@]}"; do
    ip="${NODES[$label]}"
    ROW=$(psql_query "SELECT n.nodeid FROM node n JOIN ipinterface ip ON n.nodeid = ip.nodeid WHERE n.foreignsource = '${FOREIGN_SOURCE}' AND n.foreignid = '${label}' AND ip.ipaddr = '${ip}' LIMIT 1")
    if [ -n "$ROW" ]; then
        ok "Node ${label} (${ip}) — nodeid=${ROW}"
    else
        fail "Node ${label} (${ip}) not found or IP mismatch"
    fi
done

# ══════════════════════════════════════════════════════════════════
# Phase 2: SNMP Service Detection & Interface Collection
# ══════════════════════════════════════════════════════════════════
log ""
log "Phase 2: Verifying SNMP data gathering via remote Minion..."

# Check SNMP service detected on all nodes
SNMP_SVC_QUERY="SELECT count(DISTINCT n.nodeid) FROM ifservices s JOIN service svc ON s.serviceid = svc.serviceid JOIN ipinterface ip ON s.ipinterfaceid = ip.id JOIN node n ON ip.nodeid = n.nodeid WHERE n.foreignsource = '${FOREIGN_SOURCE}' AND svc.servicename = 'SNMP'"
SNMP_SVC_THRESHOLD="SELECT CASE WHEN count(DISTINCT n.nodeid) >= ${EXPECTED_NODES} THEN count(DISTINCT n.nodeid) ELSE 0 END FROM ifservices s JOIN service svc ON s.serviceid = svc.serviceid JOIN ipinterface ip ON s.ipinterfaceid = ip.id JOIN node n ON ip.nodeid = n.nodeid WHERE n.foreignsource = '${FOREIGN_SOURCE}' AND svc.servicename = 'SNMP'"
if wait_for_db "$SNMP_SVC_THRESHOLD" "$PROVISION_TIMEOUT" "SNMP service on all nodes" 10; then
    SNMP_COUNT=$(psql_query "$SNMP_SVC_QUERY")
    if [ "${SNMP_COUNT:-0}" -ge "$EXPECTED_NODES" ]; then
        ok "SNMP service detected on all ${EXPECTED_NODES} nodes"
    else
        fail "SNMP service detected on ${SNMP_COUNT}/${EXPECTED_NODES} nodes"
    fi
else
    SNMP_COUNT=$(psql_query "$SNMP_SVC_QUERY" || echo "0")
    fail "SNMP service detected on only ${SNMP_COUNT}/${EXPECTED_NODES} nodes within ${PROVISION_TIMEOUT}s"
fi

# Check SNMP interfaces collected (cEOS typically has 5+ interfaces)
SNMP_IFACE_QUERY="SELECT count(DISTINCT n.nodeid) FROM snmpinterface si JOIN node n ON si.nodeid = n.nodeid WHERE n.foreignsource = '${FOREIGN_SOURCE}'"
SNMP_IFACE_THRESHOLD="SELECT CASE WHEN count(DISTINCT n.nodeid) >= ${EXPECTED_NODES} THEN count(DISTINCT n.nodeid) ELSE 0 END FROM snmpinterface si JOIN node n ON si.nodeid = n.nodeid WHERE n.foreignsource = '${FOREIGN_SOURCE}'"
if wait_for_db "$SNMP_IFACE_THRESHOLD" "$SNMP_IFACE_TIMEOUT" "SNMP interfaces on all nodes" 10; then
    IFACE_NODES=$(psql_query "$SNMP_IFACE_QUERY")
    TOTAL_IFACES=$(psql_query "SELECT count(*) FROM snmpinterface si JOIN node n ON si.nodeid = n.nodeid WHERE n.foreignsource = '${FOREIGN_SOURCE}'" || echo "0")
    if [ "${IFACE_NODES:-0}" -ge "$EXPECTED_NODES" ]; then
        ok "SNMP interfaces collected on all ${EXPECTED_NODES} nodes (${TOTAL_IFACES} total interfaces)"
    else
        fail "SNMP interfaces on ${IFACE_NODES}/${EXPECTED_NODES} nodes (${TOTAL_IFACES} total)"
    fi
else
    IFACE_NODES=$(psql_query "$SNMP_IFACE_QUERY" || echo "0")
    TOTAL_IFACES=$(psql_query "SELECT count(*) FROM snmpinterface si JOIN node n ON si.nodeid = n.nodeid WHERE n.foreignsource = '${FOREIGN_SOURCE}'" || echo "0")
    fail "SNMP interfaces on only ${IFACE_NODES}/${EXPECTED_NODES} nodes (${TOTAL_IFACES} total) within ${SNMP_IFACE_TIMEOUT}s"
fi

# Check sysObjectID was collected (proves SNMP GET worked via Minion)
SYSOBJECTID_QUERY="SELECT count(*) FROM node WHERE foreignsource = '${FOREIGN_SOURCE}' AND nodesysoid IS NOT NULL"
SYSOBJECTID_COUNT=$(psql_query "$SYSOBJECTID_QUERY" || echo "0")
if [ "${SYSOBJECTID_COUNT:-0}" -ge "$EXPECTED_NODES" ]; then
    ok "sysObjectID collected on all ${EXPECTED_NODES} nodes"
    if $VERBOSE; then
        log "  sysObjectIDs:"
        psql_query "SELECT nodelabel, nodesysoid FROM node WHERE foreignsource = '${FOREIGN_SOURCE}' ORDER BY nodelabel" || true
    fi
else
    fail "sysObjectID on ${SYSOBJECTID_COUNT}/${EXPECTED_NODES} nodes"
fi

# ══════════════════════════════════════════════════════════════════
# Phase 3: Enlinkd Link Discovery
# ══════════════════════════════════════════════════════════════════
log ""
log "Phase 3: Waiting for Enlinkd link discovery..."
log "  (Enlinkd initial_sleep_time=60s, then collects LLDP/CDP/OSPF/ISIS data)"

# Phase 3a: LLDP Elements — proves Enlinkd collected LLDP data from the node
LLDP_ELEM_QUERY="SELECT count(*) FROM lldpelement le JOIN node n ON le.nodeid = n.nodeid WHERE n.foreignsource = '${FOREIGN_SOURCE}'"
if wait_for_db "$LLDP_ELEM_QUERY" "$ENLINKD_TIMEOUT" "LLDP elements"; then
    LLDP_ELEMS=$(psql_query "$LLDP_ELEM_QUERY")
    ok "LLDP elements discovered on ${LLDP_ELEMS} nodes"
else
    LLDP_ELEMS=$(psql_query "$LLDP_ELEM_QUERY" || echo "0")
    fail "LLDP elements on only ${LLDP_ELEMS} nodes within ${ENLINKD_TIMEOUT}s"
    show_diagnostics
    log ""
    log "Results: $PASS passed, $FAIL failed"
    exit 1
fi

# Phase 3b: LLDP Links — proves Enlinkd discovered neighbor adjacencies
LLDP_LINK_QUERY="SELECT count(*) FROM lldplink ll JOIN node n ON ll.nodeid = n.nodeid WHERE n.foreignsource = '${FOREIGN_SOURCE}'"
if wait_for_db "$LLDP_LINK_QUERY" 120 "LLDP links"; then
    LLDP_LINKS=$(psql_query "$LLDP_LINK_QUERY")
    ok "LLDP links discovered: ${LLDP_LINKS} link entries"
else
    LLDP_LINKS=$(psql_query "$LLDP_LINK_QUERY" || echo "0")
    fail "Only ${LLDP_LINKS} LLDP links within timeout"
fi

# Phase 3c: CDP (optional — cEOS may or may not have CDP enabled)
CDP_LINK_QUERY="SELECT count(*) FROM cdplink cl JOIN node n ON cl.nodeid = n.nodeid WHERE n.foreignsource = '${FOREIGN_SOURCE}'"
CDP_LINKS=$(psql_query "$CDP_LINK_QUERY" || echo "0")
if [ "${CDP_LINKS:-0}" -gt 0 ]; then
    ok "CDP links discovered: ${CDP_LINKS} link entries"
else
    log "  [INFO] No CDP links (cEOS may not have CDP enabled — this is OK)"
fi

# Phase 3d: Count unique node pairs with LLDP links (bidirectional)
LLDP_PAIRS_QUERY="SELECT count(DISTINCT LEAST(ll.nodeid, n2.nodeid) || '-' || GREATEST(ll.nodeid, n2.nodeid))
FROM lldplink ll
JOIN node n1 ON ll.nodeid = n1.nodeid
JOIN lldpelement re ON ll.lldpremchassisid = re.lldpchassisid
JOIN node n2 ON re.nodeid = n2.nodeid
WHERE n1.foreignsource = '${FOREIGN_SOURCE}'
  AND n2.foreignsource = '${FOREIGN_SOURCE}'"
LLDP_PAIRS=$(psql_query "$LLDP_PAIRS_QUERY" 2>/dev/null || echo "0")
if [ "${LLDP_PAIRS:-0}" -gt 0 ]; then
    ok "LLDP topology: ${LLDP_PAIRS} unique node-pair links resolved"
else
    log "  [INFO] LLDP pair resolution returned 0 — links may exist but chassis ID matching pending"
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
log "  Phase 0: Config setup + DB cleanup (requisition, foreign source, provisiond-config)"
log "  Phase 1: Requisition import → ${EXPECTED_NODES} cEOS nodes provisioned"
log "  Phase 2: SNMP via remote Minion → services detected, interfaces collected"
log "  Phase 3: Enlinkd discovery → LLDP elements + links"
log "══════════════════════════════════════════════════════════════"
[ "$FAIL" -eq 0 ] && exit 0 || exit 1
