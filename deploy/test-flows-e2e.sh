#!/usr/bin/env bash
#
# test-flows-e2e.sh — End-to-end flow ingestion test for Delta-V ClickHouse pipeline
#
# Verifies that enriched flow data flows from Telemetryd → Kafka → flow-enricher →
# Kafka (deltav-flows) → ClickHouse flows_raw, and that all four dimension
# materialized views (application, source_ip, conversation, dscp) are populated.
#
# Pipeline:
#   softflowd/exporter → Telemetryd (UDP 9999) → Kafka DeltaV.Sink.Flows →
#   flow-enricher (enrich + split) → Kafka deltav-flows →
#   ClickHouse Kafka engine (flows_kafka) → ingest MV → flows_raw →
#   dimension MVs (flows_by_application_1m, flows_by_source_ip_1m,
#                  flows_by_conversation_1m, flows_by_dscp_1m)
#
# Usage:
#   ./test-flows-e2e.sh              Run the test
#   ./test-flows-e2e.sh --verbose    Show diagnostic queries on failure
#   ./test-flows-e2e.sh --timeout=N  Override wait timeout (default 300s)
#
# Prerequisites:
#   - Delta-V deployed with ClickHouse: ./deploy.sh up full
#   - flow-enricher, telemetryd, clickhouse, kafka all running
#   - Flow data being generated (softflowd or similar sending to Minion)
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
CH_TIMEOUT=300           # 5 min — wait for flow data to appear in ClickHouse
CH_POLL_INTERVAL=10      # seconds between ClickHouse polls
EXPECTED_TABLES=("flows_raw" "flows_kafka" "flows_by_application_1m" "flows_by_source_ip_1m" "flows_by_conversation_1m" "flows_by_dscp_1m")
DIMENSION_TABLES=("application" "source_ip" "conversation" "dscp")

# ── Parse flags ────────────────────────────────────────────────────
VERBOSE=false
for arg in "$@"; do
  case "$arg" in
    --verbose) VERBOSE=true ;;
    --timeout=*) CH_TIMEOUT="${arg#--timeout=}" ;;
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

ch_query() {
  docker compose exec -T clickhouse clickhouse-client \
    --user deltav --password deltav \
    --format TabSeparated \
    -q "$1" 2>/dev/null
}

wait_for_ch() {
  local query="$1"
  local timeout="$2"
  local description="$3"
  local poll_interval="${4:-$CH_POLL_INTERVAL}"
  local elapsed=0

  log "Waiting for $description (timeout: ${timeout}s)..."
  while [ $elapsed -lt "$timeout" ]; do
    local result
    result=$(ch_query "$query" 2>/dev/null || echo "")
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
  log "ClickHouse tables in deltav:"
  ch_query "SHOW TABLES FROM deltav" || true
  log ""
  log "flows_raw total count:"
  ch_query "SELECT count() FROM deltav.flows_raw" || true
  log ""
  log "flows_raw recent count (last 10 min):"
  ch_query "SELECT count() FROM deltav.flows_raw WHERE timestamp > now() - INTERVAL 10 MINUTE" || true
  log ""
  log "flows_raw sample (last 10 min, 5 rows):"
  ch_query "SELECT timestamp, src_address, dst_address, application, exporter_node_id, num_bytes FROM deltav.flows_raw WHERE timestamp > now() - INTERVAL 10 MINUTE ORDER BY timestamp DESC LIMIT 5" || true
  log ""
  log "Dimension MV counts (last 10 min):"
  for dim in "${DIMENSION_TABLES[@]}"; do
    local count
    count=$(ch_query "SELECT count() FROM deltav.flows_by_${dim}_1m WHERE t_minute > now() - INTERVAL 10 MINUTE" 2>/dev/null || echo "0")
    log "  flows_by_${dim}_1m: ${count} rows"
  done
  log ""
  log "Enriched flows check (exporter_node_id > 0, last 10 min):"
  ch_query "SELECT count() FROM deltav.flows_raw WHERE exporter_node_id > 0 AND timestamp > now() - INTERVAL 10 MINUTE" || true
  log ""
  log "Kafka consumer lag (deltav-clickhouse-persister):"
  docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 \
    --describe --group deltav-clickhouse-persister 2>/dev/null || true
}

# ══════════════════════════════════════════════════════════════════
# Phase 1: Prerequisites
# ══════════════════════════════════════════════════════════════════
log ""
log "Phase 1: Prerequisite checks..."

REQUIRED_SERVICES="clickhouse kafka telemetryd flow-enricher minion"
for svc in $REQUIRED_SERVICES; do
  if ! docker compose ps --status running --format '{{.Name}}' 2>/dev/null | grep -qw "$svc"; then
    err "Service '$svc' is not running. Deploy with: ./deploy.sh up full"
  fi
done
ok "Required services running (clickhouse, kafka, telemetryd, flow-enricher, minion)"

# Verify ClickHouse is reachable and responsive
if ! ch_query "SELECT 1" > /dev/null 2>&1; then
  err "ClickHouse is not responding. Check: docker compose logs clickhouse"
fi
ok "ClickHouse is healthy and accepting queries"

# Verify DDL bootstrap completed — all expected tables must exist
log "Verifying DDL bootstrap (expected ${#EXPECTED_TABLES[@]} tables in deltav)..."
EXISTING_TABLES=$(ch_query "SHOW TABLES FROM deltav" 2>/dev/null || echo "")
for table in "${EXPECTED_TABLES[@]}"; do
  if echo "$EXISTING_TABLES" | grep -qw "$table"; then
    ok "Table exists: deltav.${table}"
  else
    err "Table missing: deltav.${table} — run clickhouse-init or check init logs"
  fi
done

# ══════════════════════════════════════════════════════════════════
# Pre-flight: confirm gateway-side flow stream opens
# ══════════════════════════════════════════════════════════════════

# The nl6 simulator fleet emits flows continuously — its 29 devices are
# round-robined across all four protocols (netflow9 / netflow5 / sflow / ipfix
# to minion:4729), so we don't need to drive synthetic traffic. Wait up to
# 10s for any of the four publish* methods to log a stream open.
# Capture+case avoids SIGPIPE under pipefail (per
# feedback_grep_q_sigpipe_in_pipefail).
GATEWAY_OPENED=""
for i in $(seq 1 10); do
    GATEWAY_LOG=$(docker compose logs minion-gateway 2>&1 || true)
    case "$GATEWAY_LOG" in
        *"Telemetry stream opened for minion="*) GATEWAY_OPENED=yes; break ;;
    esac
    sleep 1
done
if [ "$GATEWAY_OPENED" = "yes" ]; then
    ok "gRPC TelemetryService engaged: minion-gateway logged stream open"
else
    log "  (last 30 gateway log lines:)"
    docker compose logs minion-gateway --tail 30 2>&1 | sed 's/^/    /'
    err "Pre-flight timed out: minion-gateway never logged 'Telemetry stream opened'"
fi

# ══════════════════════════════════════════════════════════════════
# Phase 2: Wait for flow data in flows_raw
# ══════════════════════════════════════════════════════════════════
log ""
log "Phase 2: Waiting for flow data in ClickHouse flows_raw..."

RAW_QUERY="SELECT count() FROM deltav.flows_raw WHERE timestamp > now() - INTERVAL 10 MINUTE"
if wait_for_ch "$RAW_QUERY" "$CH_TIMEOUT" "flows_raw data (last 10 min)"; then
  RAW_COUNT=$(ch_query "$RAW_QUERY" 2>/dev/null || echo "0")
  ok "flows_raw has ${RAW_COUNT} flow records in the last 10 minutes"
else
  RAW_TOTAL=$(ch_query "SELECT count() FROM deltav.flows_raw" 2>/dev/null || echo "0")
  fail "No recent flows in flows_raw within ${CH_TIMEOUT}s (total rows: ${RAW_TOTAL})"
  show_diagnostics
  log ""
  echo "========================================"
  echo " Results: ${PASS} passed, ${FAIL} failed"
  echo "========================================"
  exit 1
fi

# Per-protocol assertions. Proves the nl6 fleet's datagrams travel the full
# Minion → Kafka → flow-enricher → ClickHouse path for each protocol. The
# netflow_version column is a String whose value comes from the FlowDocument
# protobuf enum name (NetflowVersion.V9 → "V9", .V5 → "V5", .SFLOW → "SFLOW",
# .IPFIX → "IPFIX"). All four enum names are confirmed in live flows_raw data;
# the nl6 fleet round-robins its 29 devices across all four protocols.
for proto in V9 V5 SFLOW IPFIX; do
  PROTO_QUERY="SELECT count() FROM deltav.flows_raw WHERE netflow_version = '${proto}' AND timestamp > now() - INTERVAL 10 MINUTE"
  if wait_for_ch "$PROTO_QUERY" 120 "flows_raw ${proto} rows (last 10 min)" 10; then
    PROTO_COUNT=$(ch_query "$PROTO_QUERY" 2>/dev/null || echo "0")
    ok "flows_raw has ${PROTO_COUNT} ${proto} records in the last 10 minutes"
  else
    PROTO_TOTAL=$(ch_query "SELECT count() FROM deltav.flows_raw WHERE netflow_version = '${proto}'" 2>/dev/null || echo "0")
    SEEN_VERSIONS=$(ch_query "SELECT DISTINCT netflow_version FROM deltav.flows_raw WHERE timestamp > now() - INTERVAL 10 MINUTE" 2>/dev/null | tr '\n' ',' || echo "")
    fail "No recent ${proto} rows in flows_raw within 120s (total ${proto} rows: ${PROTO_TOTAL}, versions seen recently: ${SEEN_VERSIONS:-none})"
  fi
done

# ══════════════════════════════════════════════════════════════════
# Phase 3: Dimension materialized views have data
# ══════════════════════════════════════════════════════════════════
log ""
log "Phase 3: Asserting dimension MVs are populated..."

for dim in "${DIMENSION_TABLES[@]}"; do
  MV_QUERY="SELECT count() FROM deltav.flows_by_${dim}_1m WHERE t_minute > now() - INTERVAL 10 MINUTE"
  if wait_for_ch "$MV_QUERY" 60 "flows_by_${dim}_1m data" 5; then
    MV_COUNT=$(ch_query "$MV_QUERY" 2>/dev/null || echo "0")
    ok "flows_by_${dim}_1m has ${MV_COUNT} aggregated rows in the last 10 minutes"
  else
    MV_TOTAL=$(ch_query "SELECT count() FROM deltav.flows_by_${dim}_1m" 2>/dev/null || echo "0")
    fail "flows_by_${dim}_1m has no recent data (total rows: ${MV_TOTAL})"
  fi
done

# ══════════════════════════════════════════════════════════════════
# Phase 4: Basic data integrity checks
# ══════════════════════════════════════════════════════════════════
log ""
log "Phase 4: Data integrity checks..."

# Enriched flows must have a non-zero exporter_node_id (proves flow-enricher ran)
ENRICHED_QUERY="SELECT count() FROM deltav.flows_raw WHERE exporter_node_id > 0 AND timestamp > now() - INTERVAL 10 MINUTE"
ENRICHED_COUNT=$(ch_query "$ENRICHED_QUERY" 2>/dev/null || echo "0")
if [ "${ENRICHED_COUNT:-0}" -gt 0 ]; then
  ok "Enriched flows confirmed: ${ENRICHED_COUNT} records with exporter_node_id > 0"
else
  fail "No enriched flows (exporter_node_id > 0) in the last 10 minutes — flow-enricher may not be running or NodeInfo lookup is failing"
fi

# src_address must be populated (proves L3 data was decoded)
SRC_ADDR_QUERY="SELECT count() FROM deltav.flows_raw WHERE src_address != toIPv6('::') AND timestamp > now() - INTERVAL 10 MINUTE"
SRC_ADDR_COUNT=$(ch_query "$SRC_ADDR_QUERY" 2>/dev/null || echo "0")
if [ "${SRC_ADDR_COUNT:-0}" -gt 0 ]; then
  ok "Source addresses populated: ${SRC_ADDR_COUNT} records with non-zero src_address"
else
  fail "No records with populated src_address in the last 10 minutes — flow decoding may be broken"
fi

# Application classification must have at least some non-empty labels
APP_QUERY="SELECT count() FROM deltav.flows_raw WHERE application != '' AND timestamp > now() - INTERVAL 10 MINUTE"
APP_COUNT=$(ch_query "$APP_QUERY" 2>/dev/null || echo "0")
if [ "${APP_COUNT:-0}" -gt 0 ]; then
  ok "Application classification active: ${APP_COUNT} records with non-empty application label"
else
  log "  [INFO] No application-classified flows yet — classification may be pending (non-fatal)"
fi

# ══════════════════════════════════════════════════════════════════
# Phase 5: Reverse-DNS enrichment (deltav.flows.dns.*)
# ══════════════════════════════════════════════════════════════════
log ""
log "Phase 5: Reverse-DNS enrichment checks..."

KIWI_IP="76.223.105.230"        # deltav.kiwi (AWS Global Accelerator) — stable PTR record
KIWI_CH="::ffff:${KIWI_IP}"

# deltav_dns_reverse_lookups_total{result=$1} from the flow-enricher actuator;
# empty $1 → grand total. Values are floats (e.g. 30359.0); callers integer-truncate.
fe_dns_metric() {
  local tag="$1" out
  out=$(docker compose exec -T flow-enricher sh -c \
    'wget -qO- http://localhost:8080/actuator/prometheus 2>/dev/null | grep "^deltav_dns_reverse_lookups_total"' 2>/dev/null)
  if [ -n "$tag" ]; then
    echo "$out" | awk -v t="result=\"$tag\"" 'index($0,t){print $2}' | head -1
  else
    echo "$out" | awk '{s+=$2} END {printf "%d", s+0}'
  fi
}
fe_wait_ready() {
  local i
  for i in $(seq 1 20); do
    docker compose exec -T flow-enricher sh -c \
      'wget -qO- http://localhost:8080/actuator/health 2>/dev/null | grep -q UP' 2>/dev/null && return 0
    sleep 10
  done
  return 1
}
# Inject $2 Netflow-v5 packets with dst IPv4 $1 to the Minion flow port (4729).
inject_netflow5() {
  python3 - "$1" "${2:-8}" <<'PYINJECT'
import socket, struct, sys, time
dst = bytes(int(x) for x in sys.argv[1].split('.')); count = int(sys.argv[2])
up = 100000; now = int(time.time())
hdr = struct.pack('!HHIIIIBBH', 5, 1, up, now, 0, 1, 0, 0, 0)
rec = (bytes([10,99,99,99]) + dst + bytes(4) + struct.pack('!HH',1,2)
       + struct.pack('!II',10,1500) + struct.pack('!II',up-1000,up)
       + struct.pack('!HH',40000,443) + struct.pack('!BBBB',0,0x10,6,0)
       + struct.pack('!HH',0,0) + struct.pack('!BBH',24,24,0))
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
for _ in range(count): s.sendto(hdr+rec, ('127.0.0.1', 4729)); time.sleep(0.3)
s.close()
PYINJECT
}

if ! docker compose exec -T flow-enricher sh -c \
     'wget -qO- http://localhost:8080/actuator/prometheus 2>/dev/null | grep -q deltav_dns'; then
  log "  [SKIP] flow-enricher image predates reverse-DNS (no deltav_dns metric) — skipping Phase 5"
else
  # 5A — resolver is active (has attempted reverse lookups on real flows)
  DNS_TOTAL=$(fe_dns_metric "")
  if [ "${DNS_TOTAL:-0}" -gt 0 ]; then
    ok "Reverse-DNS resolver active: ${DNS_TOTAL} reverse lookups attempted"
  else
    fail "Reverse-DNS resolver inactive: deltav_dns_reverse_lookups_total is 0 despite DNS enabled"
  fi

  # 5C — a public PTR-resolvable dst gets dst_hostname populated end-to-end (scope=all default).
  log "  Injecting a flow to ${KIWI_IP} (deltav.kiwi) and waiting for reverse-DNS..."
  inject_netflow5 "$KIWI_IP" 8
  KIWI_HOST=""
  for _i in $(seq 1 9); do
    KIWI_HOST=$(ch_query "SELECT dst_hostname FROM deltav.flows_raw WHERE dst_address = toIPv6('${KIWI_CH}') AND dst_hostname != '' ORDER BY timestamp DESC LIMIT 1")
    [ -n "$KIWI_HOST" ] && break
    sleep 10
  done
  if [ -n "$KIWI_HOST" ]; then
    ok "Public dst reverse-resolved: ${KIWI_IP} → dst_hostname='${KIWI_HOST}'"
  else
    fail "Public dst ${KIWI_IP} got no dst_hostname — Netty reverse-PTR path may be broken"
  fi

  # 5B — scope=private filters public IPs (cardinality guard) while STILL resolving the
  #      IPv4-mapped private nl6 IPs (::ffff:10.x). Toggle, observe, restore.
  log "  Toggling flow-enricher to DNS scope=private (CIDR gate + IPv4-mapped classification)..."
  DELTAV_FLOWS_DNS_SCOPE=private docker compose --profile demo up -d --force-recreate --no-deps flow-enricher >/dev/null 2>&1
  if fe_wait_ready; then
    # Metrics reset on recreate; wait for the fresh container to RESUME processing flows
    # (the enrichFlows binding only starts after the node-context cache re-bootstraps),
    # then take the fresh baselines.
    for _w in $(seq 1 18); do [ "$(fe_dns_metric '')" -gt 0 ] 2>/dev/null && break; sleep 10; done
    F0=$(fe_dns_metric filtered); F0=${F0%.*}; F0=${F0:-0}
    M0=$(fe_dns_metric miss); M0=${M0%.*}; M0=${M0:-0}
    inject_netflow5 "$KIWI_IP" 8     # public → must be filtered under scope=private
    sleep 30
    F1=$(fe_dns_metric filtered); F1=${F1%.*}; F1=${F1:-0}
    M1=$(fe_dns_metric miss); M1=${M1%.*}; M1=${M1:-0}
    KIWI_PRIV=$(ch_query "SELECT dst_hostname FROM deltav.flows_raw WHERE dst_address = toIPv6('${KIWI_CH}') AND dst_hostname != '' AND timestamp > now() - INTERVAL 1 MINUTE LIMIT 1")
    if [ "$F1" -gt "$F0" ] && [ -z "$KIWI_PRIV" ]; then
      ok "scope=private filters public IPs: filtered ${F0}→${F1}, deltav.kiwi left unresolved"
    else
      fail "scope=private did not filter the public IP (filtered ${F0}→${F1}, kiwi host='${KIWI_PRIV}')"
    fi
    if [ "$M1" -gt "$M0" ]; then
      ok "scope=private still resolves IPv4-mapped private nl6 IPs: miss ${M0}→${M1} (not mis-filtered)"
    else
      fail "scope=private appears to mis-filter IPv4-mapped private IPs (miss stalled: ${M0}→${M1})"
    fi
  else
    fail "flow-enricher did not become ready after scope=private recreate"
  fi
  log "  Restoring flow-enricher DNS scope=all..."
  DELTAV_FLOWS_DNS_SCOPE=all docker compose --profile demo up -d --force-recreate --no-deps flow-enricher >/dev/null 2>&1
  fe_wait_ready || log "  [WARN] flow-enricher slow to return after restore (check 'make status')"
fi

show_diagnostics

# ══════════════════════════════════════════════════════════════════
# Summary
# ══════════════════════════════════════════════════════════════════
echo ""
echo "========================================"
echo " Results: ${PASS} passed, ${FAIL} failed"
echo "========================================"
echo ""
echo "Validated:"
echo "  Phase 1: ClickHouse healthy + all DDL tables exist"
echo "  Phase 2: flows_raw receiving data (Minion → flow-enricher → ClickHouse)"
echo "  Phase 2: Per-protocol rows (Netflow v9 from softflowd, sFlow from hsflowd)"
echo "  Phase 3: All 4 dimension MVs populated (application, source_ip, conversation, dscp)"
echo "  Phase 4: Enrichment integrity (exporter_node_id, src_address)"
echo "  Phase 5: Reverse-DNS (resolver active, public dst→hostname, scope=private CIDR gate)"
[[ $FAIL -eq 0 ]] || exit 1
