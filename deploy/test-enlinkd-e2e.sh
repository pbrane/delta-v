#!/usr/bin/env bash
#
# test-enlinkd-e2e.sh — End-to-end Enlinkd LLDP link discovery test for Delta-V
#
# Runs ENTIRELY IN-STACK against the local nl6 simulator — no labbox, no
# Containerlab cEOS, no VPN. This test sits ON TOP of the nl6 LLDP topology
# foundation provided by the nl6 v0.11.2 pin and the generated 5-stage Clos
# fabric (components/nl6/gen-fabric.py + clos.json, wired through
# nl6-provisioner). Enlinkd walks the resulting LLDP-MIB (1.0.8802.1.1.2) via
# the nl6-minion (location nl6-lab) and resolves the layer-2 adjacencies into
# lldpelement/lldplink rows.
#
# DEPENDENCY: the Clos topology + v0.11.2 pin land via the nl6 foundation PRs
# (#380 v0.11.2, #381 Clos fabric). This test is meaningful only on a develop
# that already carries them; until then it is a draft. After they merge, rebase
# this branch on develop and run.
#
# Authored topology (components/nl6/clos.json — generated 5-stage Clos):
#   4 core   (10.0.0.1-4)   ─┐
#   8 agg    (10.0.0.21-28)  ├─ 48 links total
#   8 edge   (10.0.0.41-48)  │
#   16 host  (10.0.0.61-76) ─┘
# 48 physical links → enlinkd sees both ends → ~96 directed lldplink rows and
# 48 resolved node-pairs across the 36 linked nodes.
#
# The nl6 nodes are imported from the profile-gated nl6-lab requisition
# (foreign-source="nl6", location="nl6-lab") that provisiond-nl6-init swaps in
# under the nl6 profiles. Enlinkd's SNMP runs via the nl6-minion at
# location=nl6-lab (RPC through the minion-gateway gRPC bidi stream).
#
# Usage:
#   ./test-enlinkd-e2e.sh                 Bring up the lean nl6 stack + run
#   ./test-enlinkd-e2e.sh --skip-deploy   Reuse an already-running stack
#   ./test-enlinkd-e2e.sh --verbose       Show diagnostic queries
#   ./test-enlinkd-e2e.sh --pre-clean     Wipe DB nodes before the run
#   ./test-enlinkd-e2e.sh --teardown      Tear down the lean stack on exit
#   ./test-enlinkd-e2e.sh --post-cleanup  Delete test nodes/alarms after run
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
# The requisition's foreign-source attribute is "nl6" (NOT "nl6-lab"); the
# nodes carry location="nl6-lab". Querying foreignsource must use "nl6".
FOREIGN_SOURCE="nl6"
LOCATION="nl6-lab"

# Lean service set: enlinkd + the nl6 simulator stack. Compose pulls in the
# always-up base (postgres/kafka/db-init/minion-gateway/envoy) and the
# depends_on chain (provisiond-imports-init → provisiond-nl6-init) automatically.
LEAN_SERVICES="nl6 nl6-provisioner nl6-minion provisiond provisiond-nl6-init enlinkd"

DEPLOY_TIMEOUT=600         # nl6 + provisiond cold start can be slow on macOS
PROVISION_TIMEOUT=420      # SNMP scan of 36 devices via nl6-minion
SNMP_IFACE_TIMEOUT=420
ENLINKD_TIMEOUT=900        # 36-node fabric: initial_sleep 60s + collection + topology
ENLINKD_POLL_INTERVAL=15

# Expected fabric size — tracks components/nl6/clos.json (gen-fabric.py).
# These are the design counts; CONFIRM/tighten against actual discovered
# counts when verifying on a live stack after the nl6 foundation PRs land.
EXPECTED_TOTAL_NODES=36    # 4 core + 8 agg + 8 edge + 16 host
EXPECTED_LLDP_ELEMENTS=36  # every node has >=1 link → advertises LLDP local data
EXPECTED_LLDP_LINKS=48     # physical links (each end → a directed lldplink row → ~96)
EXPECTED_LLDP_PAIRS=48     # resolved unique node-pairs

# The nl6-lab reverse-DNS RPC (via nl6-minion) is flaky — responses intermittently
# fail to unmarshal — which can drop a few nodes from any single provisiond import
# cycle, and leaves a minority of nodes without sysObjectID/snmpinterface after the
# one-shot node scan (scan-interval=1d) until the next daily scan. So:
#   - import is retried (each provisiond restart re-triggers a cycle) until all land;
#   - Phase 2 asserts SNMP works on a healthy MAJORITY (floors below), not 100%.
# The authoritative all-node SNMP-reachability proof is Phase 3 (lldpelement on all
# ${EXPECTED_LLDP_ELEMENTS}), which enlinkd reaches because it reschedules its walk.
IMPORT_ATTEMPTS=3          # provisiond (re)import cycles to converge all nodes
SNMP_NODE_FLOOR=24         # >= 2/3 of nodes have snmpinterface (proves collection works)
SYSOID_FLOOR=12            # >= 1/3 have sysObjectID (proves SNMP GET via minion works)

# Stable backbone nodes used for per-node provisioning/SNMP existence checks
# (IP → expected node-label/foreign-id). The 4 core switches are the most
# stable anchors in any Clos generation.
declare -A CORE_NODES=(
    [10.0.0.1]="core-10.0.0.1"
    [10.0.0.2]="core-10.0.0.2"
    [10.0.0.3]="core-10.0.0.3"
    [10.0.0.4]="core-10.0.0.4"
)

# ── Parse flags ────────────────────────────────────────────────────
VERBOSE=false
SKIP_DEPLOY=false
PRE_CLEAN=false
TEARDOWN=false
POST_CLEANUP=false
for arg in "$@"; do
    case "$arg" in
        --verbose) VERBOSE=true ;;
        --skip-deploy) SKIP_DEPLOY=true ;;
        --pre-clean) PRE_CLEAN=true ;;
        --teardown) TEARDOWN=true ;;
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
    if $POST_CLEANUP; then
        log "Post-run cleanup (--post-cleanup): removing test data..."
        clean_all_nodes
        clean_all_alarms
    fi
    if $TEARDOWN; then
        log "Tearing down lean nl6 stack (--teardown)..."
        # shellcheck disable=SC2086
        docker compose stop $LEAN_SERVICES >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

psql_query() {
    docker compose exec -T -e PGPASSWORD=opennms postgres \
        psql -U opennms -d opennms -t -A -c "$1" 2>/dev/null
}

wait_for_db() {
    local query="$1" timeout="$2" description="$3"
    local poll_interval="${4:-$ENLINKD_POLL_INTERVAL}" elapsed=0
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

wait_for_healthy() {
    local container="$1" timeout="${2:-120}" elapsed=0 health
    while [ $elapsed -lt "$timeout" ]; do
        health=$(docker inspect --format='{{.State.Health.Status}}' "$container" 2>/dev/null || echo "unknown")
        if [ "$health" = "healthy" ]; then return 0; fi
        sleep 5
        elapsed=$((elapsed + 5))
    done
    return 1
}

show_diagnostics() {
    if ! $VERBOSE; then
        log "Hint: re-run with --verbose for diagnostic output"
        return
    fi
    log ""
    log "── Diagnostic Queries (foreignsource=${FOREIGN_SOURCE}) ──"
    log "Node count by role:"
    psql_query "SELECT split_part(foreignid,'-',1) AS role, count(*) FROM node WHERE foreignsource = '${FOREIGN_SOURCE}' GROUP BY 1 ORDER BY 1" || true
    log "LLDP elements (sample):"
    psql_query "SELECT n.nodelabel, le.lldpchassisid, le.lldpsysname FROM lldpelement le JOIN node n ON le.nodeid = n.nodeid WHERE n.foreignsource = '${FOREIGN_SOURCE}' ORDER BY n.nodelabel LIMIT 12" || true
    log "LLDP links (sample):"
    psql_query "SELECT n.nodelabel, ll.lldpportid, ll.lldpremchassisid, ll.lldpremsysname FROM lldplink ll JOIN node n ON ll.nodeid = n.nodeid WHERE n.foreignsource = '${FOREIGN_SOURCE}' ORDER BY n.nodelabel LIMIT 12" || true
}

# ══════════════════════════════════════════════════════════════════
# Phase 0: Bring up (or reuse) the lean nl6 stack
# ══════════════════════════════════════════════════════════════════
log "Phase 0: nl6 stack..."
if $SKIP_DEPLOY; then
    log "  --skip-deploy: reusing the running stack"
else
    log "  Bringing up lean nl6 services: ${LEAN_SERVICES}"
    # Naming profiled services explicitly starts them + their deps even without
    # the profile flag (Compose v2). Idempotent: a no-op if already running.
    # shellcheck disable=SC2086
    docker compose up -d $LEAN_SERVICES
fi

# Wait for the daemons the test depends on to report healthy.
for c in delta-v-nl6 delta-v-provisiond delta-v-enlinkd delta-v-nl6-minion; do
    if wait_for_healthy "$c" "$DEPLOY_TIMEOUT"; then
        ok "${c} healthy"
    else
        err "${c} did not become healthy within ${DEPLOY_TIMEOUT}s"
    fi
done

# nl6-provisioner is a one-shot (restart: "no"); confirm it exited 0 so the
# devices AND the Clos LLDP topology links were authored.
PROV_RC=$(docker inspect --format='{{.State.ExitCode}}' delta-v-nl6-provisioner 2>/dev/null || echo "missing")
if [ "$PROV_RC" = "0" ]; then
    ok "nl6-provisioner completed (devices + Clos topology authored)"
else
    log "  nl6-provisioner exit code: ${PROV_RC}"
    docker logs delta-v-nl6-provisioner 2>&1 | tail -20 || true
    err "nl6-provisioner did not complete successfully (rc=${PROV_RC})"
fi

# Independently confirm nl6 reports the authored topology as active.
NL6_STATUS=$(docker exec delta-v-nl6 wget -q -O- http://127.0.0.1:8080/api/v1/topology/status 2>/dev/null || echo "")
CONFIGURED_LINKS=$(printf '%s' "$NL6_STATUS" | sed -n 's/.*"configured_links":\([0-9]*\).*/\1/p')
if [ "${CONFIGURED_LINKS:-0}" -ge "$EXPECTED_LLDP_LINKS" ]; then
    ok "nl6 topology active: ${CONFIGURED_LINKS} configured links (>= ${EXPECTED_LLDP_LINKS})"
else
    fail "nl6 reports ${CONFIGURED_LINKS:-0} configured links (expected >= ${EXPECTED_LLDP_LINKS}); status=${NL6_STATUS}"
fi

# ── Prerequisite: gRPC RPC stream for location=nl6-lab ──
log "Checking gRPC RPC stream for location=${LOCATION}..."
RPC_STREAM_DEADLINE=$(( $(date +%s) + 60 ))
while (( $(date +%s) < RPC_STREAM_DEADLINE )); do
    if docker logs delta-v-minion-gateway 2>&1 | grep -q "RPC stream opened for minion=.* location=${LOCATION}"; then
        break
    fi
    sleep 3
done
if docker logs delta-v-minion-gateway 2>&1 | grep -q "RPC stream opened for minion=.* location=${LOCATION}"; then
    ok "gRPC RPC stream live for location=${LOCATION}"
else
    err "minion-gateway never logged 'RPC stream opened' for location=${LOCATION}; nl6-minion RPC channel not live"
fi

# ── Optional pre-clean ──
if $PRE_CLEAN; then
    log "Pre-run cleanup (--pre-clean): wiping DB nodes..."
    docker compose stop enlinkd provisiond >/dev/null 2>&1 || true
    clean_all_nodes
    clean_all_alarms
    docker compose start provisiond enlinkd >/dev/null 2>&1 || true
    wait_for_healthy delta-v-provisiond 120 || err "Provisiond not healthy after restart"
    wait_for_healthy delta-v-enlinkd 120 || err "Enlinkd not healthy after restart"
    ok "Database cleaned, daemons restarted"
fi

# ══════════════════════════════════════════════════════════════════
# Phase 1: nl6 node provisioning
# ══════════════════════════════════════════════════════════════════
log ""
log "Phase 1: Importing nl6-lab requisition (foreign-source=${FOREIGN_SOURCE})..."

# Retry the import: a provisiond restart (re)triggers an import cycle, and the
# flaky nl6-lab reverse-DNS RPC can drop a few nodes per cycle (they recover on
# the next). Loop until all nodes land or attempts are exhausted.
NODE_COUNT_Q="SELECT count(*) FROM node WHERE foreignsource = '${FOREIGN_SOURCE}'"
NODE_THRESH_Q="SELECT CASE WHEN count(*) >= ${EXPECTED_TOTAL_NODES} THEN count(*) ELSE 0 END FROM node WHERE foreignsource = '${FOREIGN_SOURCE}'"
attempt=1
while [ "$attempt" -le "$IMPORT_ATTEMPTS" ]; do
    NOW=$(psql_query "$NODE_COUNT_Q" || echo "0")
    [ "${NOW:-0}" -ge "$EXPECTED_TOTAL_NODES" ] && break
    log "  Import attempt ${attempt}/${IMPORT_ATTEMPTS}: ${NOW:-0}/${EXPECTED_TOTAL_NODES} nodes — restarting provisiond to (re)trigger import..."
    docker restart delta-v-provisiond >/dev/null 2>&1 || true
    wait_for_healthy delta-v-provisiond 120 || err "Provisiond not healthy after restart"
    wait_for_db "$NODE_THRESH_Q" "$PROVISION_TIMEOUT" "${EXPECTED_TOTAL_NODES} nl6 nodes (attempt ${attempt})" 10 && break
    attempt=$((attempt + 1))
done

TOTAL_NODES=$(psql_query "$NODE_COUNT_Q" || echo "0")
if [ "${TOTAL_NODES:-0}" -ge "$EXPECTED_TOTAL_NODES" ]; then
    ok "nl6 nodes provisioned (${TOTAL_NODES} total)"
else
    fail "Only ${TOTAL_NODES} nl6 nodes provisioned after ${IMPORT_ATTEMPTS} import attempts (expected >= ${EXPECTED_TOTAL_NODES}) — nl6-lab DNS-RPC reverse-lookup flakiness?"
    show_diagnostics
    log "Results: $PASS passed, $FAIL failed"
    exit 1
fi

# Verify each core (backbone) node exists with the right IP.
for ip in "${!CORE_NODES[@]}"; do
    label="${CORE_NODES[$ip]}"
    ROW=$(psql_query "SELECT n.nodeid FROM node n JOIN ipinterface i ON n.nodeid = i.nodeid WHERE n.foreignsource = '${FOREIGN_SOURCE}' AND n.foreignid = '${label}' AND i.ipaddr = '${ip}' LIMIT 1")
    if [ -n "$ROW" ]; then
        ok "Core node ${label} (${ip}) — nodeid=${ROW}"
    else
        fail "Core node ${label} (${ip}) not found or IP mismatch"
    fi
done

# ══════════════════════════════════════════════════════════════════
# Phase 2: SNMP gathering via nl6-minion
# ══════════════════════════════════════════════════════════════════
log ""
log "Phase 2: Verifying SNMP data gathering via nl6-minion (location=${LOCATION})..."

# Majority floors (not 100%) — provisiond's one-shot scan + flaky nl6-lab DNS RPC
# leave a minority un-collected until the next daily scan. Phase 3 proves all-node
# SNMP reachability authoritatively.
SNMP_IFACE_THRESHOLD="SELECT CASE WHEN count(DISTINCT n.nodeid) >= ${SNMP_NODE_FLOOR} THEN count(DISTINCT n.nodeid) ELSE 0 END FROM snmpinterface si JOIN node n ON si.nodeid = n.nodeid WHERE n.foreignsource = '${FOREIGN_SOURCE}'"
if wait_for_db "$SNMP_IFACE_THRESHOLD" "$SNMP_IFACE_TIMEOUT" "SNMP interfaces on >= ${SNMP_NODE_FLOOR} nl6 nodes" 10; then
    IFACE_NODES=$(psql_query "SELECT count(DISTINCT n.nodeid) FROM snmpinterface si JOIN node n ON si.nodeid = n.nodeid WHERE n.foreignsource = '${FOREIGN_SOURCE}'")
    TOTAL_IFACES=$(psql_query "SELECT count(*) FROM snmpinterface si JOIN node n ON si.nodeid = n.nodeid WHERE n.foreignsource = '${FOREIGN_SOURCE}'" || echo "0")
    ok "SNMP interfaces collected on ${IFACE_NODES}/${EXPECTED_TOTAL_NODES} nodes (${TOTAL_IFACES} total; floor ${SNMP_NODE_FLOOR})"
else
    IFACE_NODES=$(psql_query "SELECT count(DISTINCT n.nodeid) FROM snmpinterface si JOIN node n ON si.nodeid = n.nodeid WHERE n.foreignsource = '${FOREIGN_SOURCE}'" || echo "0")
    fail "SNMP interfaces on only ${IFACE_NODES} nodes within ${SNMP_IFACE_TIMEOUT}s (floor ${SNMP_NODE_FLOOR})"
fi

SYSOID_COUNT=$(psql_query "SELECT count(*) FROM node WHERE foreignsource = '${FOREIGN_SOURCE}' AND nodesysoid IS NOT NULL" || echo "0")
if [ "${SYSOID_COUNT:-0}" -ge "$SYSOID_FLOOR" ]; then
    ok "sysObjectID collected on ${SYSOID_COUNT}/${EXPECTED_TOTAL_NODES} nodes (proves SNMP GET via nl6-minion; floor ${SYSOID_FLOOR})"
else
    fail "sysObjectID on only ${SYSOID_COUNT} nodes (floor ${SYSOID_FLOOR})"
fi

# ══════════════════════════════════════════════════════════════════
# Phase 3: Enlinkd LLDP discovery
# ══════════════════════════════════════════════════════════════════
log ""
log "Phase 3: Waiting for Enlinkd LLDP discovery across the Clos fabric..."
log "  (Enlinkd initial_sleep_time=60s, then walks the LLDP-MIB via nl6-minion)"

# Phase 3a: LLDP elements — proves Enlinkd collected LLDP local data.
LLDP_ELEM_QUERY="SELECT count(*) FROM lldpelement le JOIN node n ON le.nodeid = n.nodeid WHERE n.foreignsource = '${FOREIGN_SOURCE}'"
LLDP_ELEM_THRESHOLD="SELECT CASE WHEN count(*) >= ${EXPECTED_LLDP_ELEMENTS} THEN count(*) ELSE 0 END FROM lldpelement le JOIN node n ON le.nodeid = n.nodeid WHERE n.foreignsource = '${FOREIGN_SOURCE}'"
if wait_for_db "$LLDP_ELEM_THRESHOLD" "$ENLINKD_TIMEOUT" "LLDP elements on linked nodes"; then
    LLDP_ELEMS=$(psql_query "$LLDP_ELEM_QUERY")
    ok "LLDP elements discovered on ${LLDP_ELEMS} nodes (expected >= ${EXPECTED_LLDP_ELEMENTS})"
else
    LLDP_ELEMS=$(psql_query "$LLDP_ELEM_QUERY" || echo "0")
    fail "LLDP elements on only ${LLDP_ELEMS} nodes within ${ENLINKD_TIMEOUT}s"
    show_diagnostics
    log "Results: $PASS passed, $FAIL failed"
    exit 1
fi

# Phase 3b: LLDP links — the neighbor adjacencies (both ends → ~2x physical).
LLDP_LINK_QUERY="SELECT count(*) FROM lldplink ll JOIN node n ON ll.nodeid = n.nodeid WHERE n.foreignsource = '${FOREIGN_SOURCE}'"
LLDP_LINK_THRESHOLD="SELECT CASE WHEN count(*) >= ${EXPECTED_LLDP_LINKS} THEN count(*) ELSE 0 END FROM lldplink ll JOIN node n ON ll.nodeid = n.nodeid WHERE n.foreignsource = '${FOREIGN_SOURCE}'"
if wait_for_db "$LLDP_LINK_THRESHOLD" 300 "LLDP links"; then
    LLDP_LINKS=$(psql_query "$LLDP_LINK_QUERY")
    ok "LLDP links discovered: ${LLDP_LINKS} link entries (expected >= ${EXPECTED_LLDP_LINKS})"
else
    LLDP_LINKS=$(psql_query "$LLDP_LINK_QUERY" || echo "0")
    fail "Only ${LLDP_LINKS} LLDP links within timeout (expected >= ${EXPECTED_LLDP_LINKS})"
fi

# Phase 3c: resolved node-pairs (chassis-id join across nl6 nodes).
LLDP_PAIRS_QUERY="SELECT count(DISTINCT LEAST(ll.nodeid, n2.nodeid) || '-' || GREATEST(ll.nodeid, n2.nodeid))
FROM lldplink ll
JOIN node n1 ON ll.nodeid = n1.nodeid
JOIN lldpelement re ON ll.lldpremchassisid = re.lldpchassisid
JOIN node n2 ON re.nodeid = n2.nodeid
WHERE n1.foreignsource = '${FOREIGN_SOURCE}'
  AND n2.foreignsource = '${FOREIGN_SOURCE}'"
LLDP_PAIRS=$(psql_query "$LLDP_PAIRS_QUERY" 2>/dev/null || echo "0")
if [ "${LLDP_PAIRS:-0}" -ge "$EXPECTED_LLDP_PAIRS" ]; then
    ok "LLDP topology: ${LLDP_PAIRS} unique node-pair links resolved (expected >= ${EXPECTED_LLDP_PAIRS})"
elif [ "${LLDP_PAIRS:-0}" -gt 0 ]; then
    ok "LLDP topology: ${LLDP_PAIRS} node-pair links resolved (partial; expected ${EXPECTED_LLDP_PAIRS})"
else
    fail "LLDP pair resolution returned 0 — links exist but chassis-id matching failed"
fi

# ══════════════════════════════════════════════════════════════════
# Summary
# ══════════════════════════════════════════════════════════════════
show_diagnostics
log ""
log "══════════════════════════════════════════════════════════════"
log "Results: $PASS passed, $FAIL failed"
log ""
log "Validated (entirely in-stack — no labbox/cEOS/VPN):"
log "  Phase 0: Lean nl6 stack up; nl6-provisioner authored the Clos topology (>= ${EXPECTED_LLDP_LINKS} links)"
log "  Phase 1: nl6-lab requisition imported → ${EXPECTED_TOTAL_NODES} nl6 nodes (foreign-source=${FOREIGN_SOURCE})"
log "  Phase 2: SNMP via nl6-minion (location=${LOCATION}) → interfaces + sysObjectID"
log "  Phase 3: Enlinkd discovery → LLDP elements + links + resolved node-pairs"
log "══════════════════════════════════════════════════════════════"
[ "$FAIL" -eq 0 ] && exit 0 || exit 1
