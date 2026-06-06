#!/usr/bin/env bash
# Copyright (C) 2026 BeaconStrategists, Inc.
# Licensed under the GNU Affero General Public License v3.
#
# test-alerts-forwarder-e2e.sh — End-to-end alerts-forwarder pipeline test.
#
# Validates the v1.3.0 alarms-on-Kafka payoff:
#
#   alarmd → deltav-alarms-state-change (compacted Kafka topic, AlarmState proto,
#   reduction_key as message key) → alerts-forwarder (consumes + enriches against
#   deltav-node-context cache) → Alertmanager /api/v2/alerts (default sink).
#
# Triggers a real alarm via the same syslog "AWS Down" path that
# test-passive-e2e.sh uses (so we lean on a known-good event-conf entry and an
# already-provisioned node). Then asserts every hop:
#
#   1. alertmanager + alerts-forwarder containers running and /actuator/health UP
#   2. Alarm row appears in PG with our reduction_key
#   3. Same reduction_key appears as a Kafka message KEY on
#      deltav-alarms-state-change (binary protobuf value — key-only assertion)
#   4. Alertmanager /api/v2/alerts contains an active alert with the durable
#      foreignSource:foreignId composite node label, severity, description,
#      and status.state == active
#   5. After AWS Up syslog: alarm severity flips to 2 (CLEARED) in PG and the
#      Alertmanager alert is no longer active (status != active, or removed)
#   6. alerts-forwarder Micrometer counters incremented for both firing and
#      resolved outcomes
#
# Severity-filter assertion deliberately omitted — see the TODO below.
#
# Assumes the stack is already up with the metrics or metrics-e2e profile
# (alertmanager + alerts-forwarder live there). If not, fails fast.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
source "${SCRIPT_DIR}/test-lib.sh"

# ── Configuration ──────────────────────────────────────────────────
SYSLOG_HOST="localhost"
SYSLOG_PORT="1514"
NODE_LABEL="The Internet"
FOREIGN_SOURCE="cloud-services"
FOREIGN_ID="the-internet"
SERVICE_NAME="AWS"
ALARMS_TOPIC="deltav-alarms-state-change"
ALERTMANAGER_URL="http://localhost:9093"
ALARM_TIMEOUT=90
KAFKA_LOOKUP_TIMEOUT=60
ALERTMANAGER_POLL_TIMEOUT=45
RESOLVE_TIMEOUT=120
RESOLVE_AM_TIMEOUT=240

# ── Parse flags ────────────────────────────────────────────────────
VERBOSE=false
POST_CLEANUP=false
for arg in "$@"; do
    case "$arg" in
        --verbose) VERBOSE=true ;;
        --post-cleanup) POST_CLEANUP=true ;;
        --help|-h)
            cat <<EOF
Usage: $0 [--verbose] [--post-cleanup] [--help]

Validates the v1.3.0 alarms→Kafka→alerts-forwarder→Alertmanager pipeline.

Pre-requisites:
  Stack must be up with profile 'metrics' or 'metrics-e2e' (alertmanager +
  alerts-forwarder live there). Also requires the same provisioned
  "The Internet" node with passive AWS service that test-passive-e2e.sh
  sets up (run it once first, or run with the same stack that already
  has the cloud-services requisition imported).

Flags:
  --verbose         Dump extra diagnostics on the way out
  --post-cleanup    Delete the test alarm from PG after the run
EOF
            exit 0 ;;
    esac
done

# ── Helpers ────────────────────────────────────────────────────────
PASS=0
FAIL=0
TEST_TMPDIR=$(mktemp -d)
KAFKA_KEYS_LOG="$TEST_TMPDIR/kafka-keys.log"
KAFKA_CONSUMER_PID=""

log()  { echo "==> $*"; }
ok()   { echo "  [PASS] $*"; PASS=$((PASS + 1)); }
fail() { echo "  [FAIL] $*"; FAIL=$((FAIL + 1)); }
warn() { echo "  [WARN] $*"; }
err()  { echo "ERROR: $*" >&2; exit 2; }

cleanup() {
    # Stop our Kafka tail consumer (host-side process + the in-container one).
    if [ -n "$KAFKA_CONSUMER_PID" ]; then
        kill "$KAFKA_CONSUMER_PID" 2>/dev/null || true
    fi
    docker compose exec -T kafka sh -c 'for p in $(ps -eo pid,args 2>/dev/null | grep kafka-console-consumer | grep -v grep | awk "{print \$1}"); do kill "$p" 2>/dev/null; done' 2>/dev/null || true

    if $VERBOSE; then
        log ""
        log "── alerts-forwarder metrics snapshot ──"
        docker exec delta-v-alerts-forwarder-1 wget -qO- http://localhost:8080/actuator/prometheus 2>/dev/null \
            | grep -E '^deltav_alerts_forwarder' || true
        log ""
        log "── Alertmanager active alerts snapshot ──"
        curl -sf "${ALERTMANAGER_URL}/api/v2/alerts" 2>/dev/null | head -c 4000 || true
        echo
        log ""
        log "── Kafka keys captured (last 20) ──"
        tail -20 "$KAFKA_KEYS_LOG" 2>/dev/null || true
    fi

    if $POST_CLEANUP; then
        log "Post-run cleanup (--post-cleanup): removing test alarm..."
        psql_query "DELETE FROM alarms WHERE eventuei = 'uei.opennms.org/syslogd/cloud/serviceDown' AND reductionkey = '${REDUCTION_KEY:-__none__}'" 2>/dev/null || true
    fi

    rm -rf "$TEST_TMPDIR"
}
# Note (B2): the forwarder's bootstrap-replay design (AlarmStateKafkaConsumer seeks to beginning on
# every restart) means any poison record in the compacted topic can wedge the consumer across restarts. Tracked separately.
trap cleanup EXIT

psql_query() {
    docker compose exec -T -e PGPASSWORD=opennms postgres \
        psql -U opennms -d opennms -t -A -c "$1" 2>/dev/null
}

wait_for_db() {
    local query="$1"
    local timeout="$2"
    local description="$3"
    local elapsed=0
    log "Waiting for $description (timeout: ${timeout}s)..."
    while [ $elapsed -lt "$timeout" ]; do
        local result
        result=$(psql_query "$query" 2>/dev/null || echo "")
        if [ -n "$result" ] && [ "$result" != "0" ]; then
            return 0
        fi
        sleep 3
        elapsed=$((elapsed + 3))
    done
    return 1
}

send_syslog() {
    local pri="$1"
    local syslog_host="$2"
    local msg="$3"
    local timestamp
    # Regression check: the forwarder must handle non-UTC timestamps gracefully
    # via clamp (AlertmanagerAlarmSink.forward — Bug 1 fix). Syslog RFC3164
    # timestamps have no TZ info; OpenNMS interprets them as container-local UTC.
    # A non-UTC host sending local time would historically drift startsAt hours
    # into the future and trigger a 400 from Alertmanager. The clamp in Bug 1
    # eliminates that failure mode. This test intentionally uses the host's local
    # time (no -u flag) so that a non-UTC dev machine exercises the clamp path.
    timestamp=$(date '+%b %d %H:%M:%S')
    echo "<${pri}>${timestamp} ${syslog_host} ${msg}" | nc -u -w1 "$SYSLOG_HOST" "$SYSLOG_PORT"
}

# Poll Alertmanager /api/v2/alerts for up to N seconds until $1 (a jq filter
# expression that returns a non-empty result) matches. Returns 0 on match,
# 1 on timeout. The matched JSON is echoed to stdout on success.
wait_for_alertmanager_alert() {
    local jq_filter="$1"
    local timeout="$2"
    local description="$3"
    local elapsed=0
    # Log to stderr so the only thing on stdout is the matched JSON
    # (callers do ALERT_JSON=$(wait_for_alertmanager_alert ...)).
    log "Polling Alertmanager for $description (timeout: ${timeout}s)..." >&2
    while [ $elapsed -lt "$timeout" ]; do
        local body match
        body=$(curl -sf "${ALERTMANAGER_URL}/api/v2/alerts" 2>/dev/null || echo "[]")
        match=$(echo "$body" | jq -c "${jq_filter}" 2>/dev/null || echo "")
        if [ -n "$match" ] && [ "$match" != "null" ] && [ "$match" != "[]" ]; then
            echo "$match"
            return 0
        fi
        sleep 3
        elapsed=$((elapsed + 3))
    done
    return 1
}

# Start a long-running consumer that tails new keys appended to
# $ALARMS_TOPIC into $KAFKA_KEYS_LOG. Must be started BEFORE the trigger
# action so the new record is observed. --offset latest reads only records
# produced after subscription, which avoids slow scans of an old compacted
# topic that may contain thousands of historical keys.
start_kafka_key_tail() {
    log "Starting Kafka tail consumer on ${ALARMS_TOPIC} (latest only)..."
    : > "$KAFKA_KEYS_LOG"
    docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
        --bootstrap-server localhost:9092 \
        --topic "$ALARMS_TOPIC" \
        --consumer-property auto.offset.reset=latest \
        --group "test-alerts-forwarder-e2e-$$-$(date +%s)" \
        --formatter-property print.key=true \
        --formatter-property print.value=false \
        > "$KAFKA_KEYS_LOG" 2>/dev/null &
    KAFKA_CONSUMER_PID=$!
    # Give the consumer a few seconds to join its (single-member) group and
    # subscribe before any records get produced.
    sleep 5
}

# Wait up to $2 seconds for $1 (literal string) to appear as a key in the
# tail log written by start_kafka_key_tail.
wait_for_kafka_key() {
    local needle="$1"
    local timeout="$2"
    log "Waiting for key '${needle}' on ${ALARMS_TOPIC} (timeout: ${timeout}s)..."
    local elapsed=0 keys
    while [ $elapsed -lt "$timeout" ]; do
        # Deterministic scan: read the whole (compacted, small) topic from the
        # beginning and match the key, rather than tailing an ephemeral `latest`
        # consumer that can miss the record if it has not finished joining its
        # group before the AlarmState is produced (the 5s join was too tight).
        # Uses --property (not --formatter-property) like the other e2e scripts,
        # and a bash substring match (no `| grep -q`) so a hit can never trip
        # SIGPIPE under `set -o pipefail`.
        keys=$(docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
            --bootstrap-server localhost:9092 --topic "$ALARMS_TOPIC" \
            --from-beginning --max-messages 50000 --timeout-ms 5000 \
            --property print.key=true --property print.value=false 2>/dev/null || true)
        if [[ "$keys" == *"$needle"* ]]; then
            return 0
        fi
        sleep 2
        elapsed=$((elapsed + 7))
    done
    return 1
}

# ── Step 0: Prerequisite checks ────────────────────────────────────
log "Checking prerequisites..."
command -v nc >/dev/null 2>&1 || err "nc (netcat) not found."
command -v jq >/dev/null 2>&1 || err "jq not found (required for Alertmanager assertions)."
command -v curl >/dev/null 2>&1 || err "curl not found."

RUNNING=$(docker compose ps --status running --format '{{.Name}}' 2>/dev/null || true)
for svc in alertmanager alerts-forwarder kafka postgres alarmd syslogd eventtranslator minion-gateway; do
    if ! echo "$RUNNING" | grep -qw "$svc"; then
        err "Service '$svc' is not running. Bring up the stack with: docker compose --profile lite --profile metrics up -d"
    fi
done
ok "All required services running (incl. alertmanager + alerts-forwarder)"

# Make sure the cloud-services node exists. test-passive-e2e.sh provisions it;
# if it isn't here, the test cannot trigger an alarm via the syslog path.
EXISTING_NODE=$(psql_query "SELECT nodeid FROM node WHERE nodelabel = '${NODE_LABEL}' AND foreignsource = '${FOREIGN_SOURCE}' AND foreignid = '${FOREIGN_ID}' LIMIT 1")
if [ -z "$EXISTING_NODE" ]; then
    err "Node '${NODE_LABEL}' (foreignSource=${FOREIGN_SOURCE}) not provisioned. Run test-passive-e2e.sh first to seed it."
fi
NODE_ID="$EXISTING_NODE"
NODE_IP=$(psql_query "SELECT ip.ipaddr FROM ipinterface ip WHERE ip.nodeid = ${NODE_ID} LIMIT 1")
[ -n "$NODE_IP" ] || err "No IP interface found for node ${NODE_ID}"
log "Using nodeid=${NODE_ID}, ip=${NODE_IP}, expecting foreignSource:foreignId = ${FOREIGN_SOURCE}:${FOREIGN_ID}"

# Make sure the cloud serviceDown/serviceUp event-conf entries exist. These
# are inserted by test-passive-e2e.sh; we re-insert here so the test is
# self-contained against a stack where the node exists but the eventconf
# rows have been dropped.
CLOUD_SOURCE_EXISTS=$(psql_query "SELECT count(*) FROM eventconf_sources WHERE name = 'cloud.status.events'")
if [ "${CLOUD_SOURCE_EXISTS:-0}" -eq 0 ]; then
    log "Inserting cloud.status.events source + serviceDown/serviceUp event-conf rows..."
    psql_query "INSERT INTO eventconf_sources(id, name, description, vendor, file_order, enabled, event_count, created_time, last_modified, uploaded_by) VALUES (30, 'cloud.status.events', 'Cloud service status events (passive monitoring)', 'cloud', 30, true, 2, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'test-alerts-forwarder-e2e')" >/dev/null
fi

SVCDOWN_EXISTS=$(psql_query "SELECT count(*) FROM eventconf_events WHERE uei = 'uei.opennms.org/syslogd/cloud/serviceDown'")
if [ "${SVCDOWN_EXISTS:-0}" -eq 0 ]; then
    psql_query "INSERT INTO eventconf_events(id, source_id, uei, event_label, description, enabled, xml_content, created_time, last_modified, modified_by) VALUES (200, 30, 'uei.opennms.org/syslogd/cloud/serviceDown', 'Cloud Service Down', 'Cloud service down via syslog', true, '<event xmlns=\"http://xmlns.opennms.org/xsd/eventconf\">
   <uei>uei.opennms.org/syslogd/cloud/serviceDown</uei>
   <event-label>Cloud Service Down</event-label>
   <descr>Cloud service %parm[cloudService]% is down</descr>
   <logmsg dest=\"logndisplay\">Cloud service %parm[cloudService]% is down</logmsg>
   <severity>Minor</severity>
   <alarm-data reduction-key=\"%uei%:%dpname%:%nodeid%:%parm[cloudService]%\" alarm-type=\"1\" auto-clean=\"false\">
      <update-field field-name=\"severity\" update-on-reduction=\"true\"/>
   </alarm-data>
</event>', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'test-alerts-forwarder-e2e')" >/dev/null
fi

SVCUP_EXISTS=$(psql_query "SELECT count(*) FROM eventconf_events WHERE uei = 'uei.opennms.org/syslogd/cloud/serviceUp'")
if [ "${SVCUP_EXISTS:-0}" -eq 0 ]; then
    psql_query "INSERT INTO eventconf_events(id, source_id, uei, event_label, description, enabled, xml_content, created_time, last_modified, modified_by) VALUES (201, 30, 'uei.opennms.org/syslogd/cloud/serviceUp', 'Cloud Service Up', 'Cloud service up via syslog', true, '<event xmlns=\"http://xmlns.opennms.org/xsd/eventconf\">
   <uei>uei.opennms.org/syslogd/cloud/serviceUp</uei>
   <event-label>Cloud Service Up</event-label>
   <descr>Cloud service %parm[cloudService]% is up</descr>
   <logmsg dest=\"logndisplay\">Cloud service %parm[cloudService]% is up</logmsg>
   <severity>Normal</severity>
   <alarm-data reduction-key=\"%uei%:%dpname%:%nodeid%:%parm[cloudService]%\" alarm-type=\"2\" clear-key=\"uei.opennms.org/syslogd/cloud/serviceDown:%dpname%:%nodeid%:%parm[cloudService]%\"/>
</event>', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'test-alerts-forwarder-e2e')" >/dev/null
fi

# Determine if we just inserted (need restarts) vs already present (no restart).
if [ "${SVCDOWN_EXISTS:-0}" -eq 0 ] || [ "${SVCUP_EXISTS:-0}" -eq 0 ]; then
    log "Restarting syslogd, eventtranslator, alarmd to pick up new eventconf rows..."
    docker compose restart syslogd eventtranslator alarmd >/dev/null
    log "Waiting 30s for daemons to come back healthy..."
    sleep 30
fi
ok "Cloud event-conf entries present (serviceDown=Minor, serviceUp=Normal/clear)"

# ── Step 1: alertmanager + alerts-forwarder healthy ────────────────
log ""
log "Step 1: container health checks"

deadline=$((SECONDS + 30))
forwarder_up=false
while (( SECONDS < deadline )); do
    HEALTH=$(docker exec delta-v-alerts-forwarder-1 wget -qO- http://localhost:8080/actuator/health 2>/dev/null || true)
    if echo "$HEALTH" | grep -q '"status":"UP"'; then
        forwarder_up=true; break
    fi
    sleep 2
done
$forwarder_up && ok "alerts-forwarder /actuator/health = UP" || fail "alerts-forwarder /actuator/health did not reach UP in 30s"

# Alertmanager has its own /-/healthy endpoint and a v2 API; we already use
# the API in Step 4, so just probe it here.
if curl -sf "${ALERTMANAGER_URL}/api/v2/status" >/dev/null 2>&1; then
    ok "Alertmanager /api/v2/status reachable on :9093"
else
    fail "Alertmanager /api/v2/status not reachable on :9093"
fi

# Capture the alerts-forwarder counters BEFORE the trigger so the "forwarded"
# assertion is a delta, not an absolute (the stack has been up for hours).
# Guard every grep against set -o pipefail (no-match → exit 1 → killed script).
PRE_METRICS=$(docker exec delta-v-alerts-forwarder-1 wget -qO- http://localhost:8080/actuator/prometheus 2>/dev/null || true)
PRE_FIRING=$({ echo "$PRE_METRICS" | grep -E '^deltav_alerts_forwarder_forwarded_total\{outcome="firing"\}' || true; } | awk '{print $2}' | head -1)
PRE_RESOLVED=$({ echo "$PRE_METRICS" | grep -E '^deltav_alerts_forwarder_forwarded_total\{outcome="resolved"\}' || true; } | awk '{print $2}' | head -1)
PRE_FIRING="${PRE_FIRING:-0}"
PRE_RESOLVED="${PRE_RESOLVED:-0}"
log "Baseline counters: firing=${PRE_FIRING} resolved=${PRE_RESOLVED}"

# ── Step 2: Trigger AWS Down alarm ─────────────────────────────────
log ""
log "Step 2: Triggering AWS Down syslog (host=${NODE_IP})..."

# Make sure no prior cloud serviceDown alarm survives — would mask the assertion.
psql_query "DELETE FROM alarms WHERE eventuei IN ('uei.opennms.org/syslogd/cloud/serviceDown', 'uei.opennms.org/syslogd/cloud/serviceUp')" >/dev/null || true

# Start the Kafka tail BEFORE the trigger so we observe the new record.
start_kafka_key_tail

send_syslog 129 "$NODE_IP" "CLOUD-STATUS: Service ${SERVICE_NAME} is Down"
ok "Sent syslog 'CLOUD-STATUS: Service ${SERVICE_NAME} is Down' to Minion"

if wait_for_db "SELECT count(*) FROM alarms WHERE eventuei = 'uei.opennms.org/syslogd/cloud/serviceDown' AND alarmtype = 1" "$ALARM_TIMEOUT" "serviceDown alarm in PG"; then
    REDUCTION_KEY=$(psql_query "SELECT reductionkey FROM alarms WHERE eventuei = 'uei.opennms.org/syslogd/cloud/serviceDown' AND alarmtype = 1 ORDER BY alarmid DESC LIMIT 1")
    ALARM_SEVERITY=$(psql_query "SELECT severity FROM alarms WHERE reductionkey = '${REDUCTION_KEY}'")
    ok "Alarm row in PG (reductionkey=${REDUCTION_KEY}, severity=${ALARM_SEVERITY})"
else
    fail "serviceDown alarm not in PG within ${ALARM_TIMEOUT}s — pipeline broken upstream of alarmd"
    log "Results: $PASS passed, $FAIL failed"; exit 1
fi

# ── Step 3: Reduction key appears on deltav-alarms-state-change ────
log ""
log "Step 3: Reduction key on Kafka topic ${ALARMS_TOPIC}"

if wait_for_kafka_key "$REDUCTION_KEY" "$KAFKA_LOOKUP_TIMEOUT"; then
    ok "Reduction key '${REDUCTION_KEY}' published as Kafka message key"
else
    fail "Reduction key not seen on ${ALARMS_TOPIC} within ${KAFKA_LOOKUP_TIMEOUT}s"
    log "  Last 5 keys captured for diagnostics:"
    tail -5 "$KAFKA_KEYS_LOG" 2>/dev/null | sed 's/^/    /'
fi

# ── Step 4: Alertmanager has the active alert ──────────────────────
log ""
log "Step 4: Alertmanager /api/v2/alerts assertions"

# alerts-forwarder uses `uei` as a label. Filter on the UEI we just triggered.
JQ_ACTIVE_FILTER='[.[] | select(.labels.uei == "uei.opennms.org/syslogd/cloud/serviceDown" and .status.state == "active")][0]'

ALERT_JSON=""
if ALERT_JSON=$(wait_for_alertmanager_alert "$JQ_ACTIVE_FILTER" "$ALERTMANAGER_POLL_TIMEOUT" "active alert with uei=cloud/serviceDown"); then
    ok "Alertmanager has an active alert for uei.opennms.org/syslogd/cloud/serviceDown"
else
    fail "No active Alertmanager alert with uei=cloud/serviceDown within ${ALERTMANAGER_POLL_TIMEOUT}s"
    log "  Current Alertmanager state:"
    curl -sf "${ALERTMANAGER_URL}/api/v2/alerts" 2>/dev/null | head -c 2000 | sed 's/^/    /' || true
    echo
    log "Results: $PASS passed, $FAIL failed"; exit 1
fi

if $VERBOSE; then
    log "Matched alert JSON:"
    echo "$ALERT_JSON" | jq . 2>/dev/null | sed 's/^/    /' || echo "$ALERT_JSON"
fi

# labels.node must be the durable foreignSource:foreignId composite
NODE_LABEL_VAL=$(echo "$ALERT_JSON" | jq -r '.labels.node // ""')
if [[ "$NODE_LABEL_VAL" =~ ^[^:]+:[^:]+$ ]]; then
    ok "labels.node is composite '${NODE_LABEL_VAL}' (matches ^[^:]+:[^:]+\$)"
else
    fail "labels.node '${NODE_LABEL_VAL}' is not a foreignSource:foreignId composite"
fi

# Specifically the value we provisioned
if [ "$NODE_LABEL_VAL" = "${FOREIGN_SOURCE}:${FOREIGN_ID}" ]; then
    ok "labels.node == '${FOREIGN_SOURCE}:${FOREIGN_ID}' (matches provisioning)"
else
    fail "labels.node = '${NODE_LABEL_VAL}' does not match expected '${FOREIGN_SOURCE}:${FOREIGN_ID}'"
fi

# labels.severity must equal the OpenNMS severity name. serviceDown in
# eventconf is <severity>Minor</severity> → enum MINOR.
SEV=$(echo "$ALERT_JSON" | jq -r '.labels.severity // ""')
if [ "$SEV" = "MINOR" ]; then
    ok "labels.severity == 'MINOR' (matches eventconf serviceDown)"
else
    fail "labels.severity = '${SEV}', expected 'MINOR'"
fi

# annotations.description must be non-empty
DESC=$(echo "$ALERT_JSON" | jq -r '.annotations.description // ""')
if [ -n "$DESC" ]; then
    ok "annotations.description non-empty: '${DESC}'"
else
    fail "annotations.description is empty"
fi

# status.state == active (already filtered, but double-assert for clarity)
STATE=$(echo "$ALERT_JSON" | jq -r '.status.state // ""')
if [ "$STATE" = "active" ]; then
    ok "status.state == 'active'"
else
    fail "status.state = '${STATE}', expected 'active'"
fi

# ── Step 5: Resolve via AWS Up syslog ──────────────────────────────
log ""
log "Step 5: Sending AWS Up syslog and asserting resolve..."

send_syslog 134 "$NODE_IP" "CLOUD-STATUS: Service ${SERVICE_NAME} is Up"
ok "Sent syslog 'CLOUD-STATUS: Service ${SERVICE_NAME} is Up' to Minion"

if wait_for_db "SELECT count(*) FROM alarms WHERE reductionkey = '${REDUCTION_KEY}' AND severity = 2" "$RESOLVE_TIMEOUT" "alarm CLEARED in PG"; then
    ok "Alarm severity dropped to 2 (CLEARED) in PG"
else
    STILL=$(psql_query "SELECT alarmid, severity FROM alarms WHERE reductionkey = '${REDUCTION_KEY}' LIMIT 1")
    fail "Alarm not cleared in PG (still: ${STILL:-<gone>})"
fi

# Now poll Alertmanager: accept either (a) the alert vanished from the list,
# or (b) it's still present but status.state != "active" (endsAt in the past).
log "Polling Alertmanager for resolve (timeout: ${RESOLVE_AM_TIMEOUT}s)..."
resolved=false
elapsed=0
while [ $elapsed -lt "$RESOLVE_AM_TIMEOUT" ]; do
    BODY=$(curl -sf "${ALERTMANAGER_URL}/api/v2/alerts" 2>/dev/null || echo "[]")
    STILL_ACTIVE=$(echo "$BODY" | jq -c '[.[] | select(.labels.uei == "uei.opennms.org/syslogd/cloud/serviceDown" and .status.state == "active")] | length' 2>/dev/null || echo "0")
    if [ "${STILL_ACTIVE:-0}" = "0" ]; then
        resolved=true; break
    fi
    sleep 3
    elapsed=$((elapsed + 3))
done
if $resolved; then
    ok "Alertmanager no longer shows the alert as active"
else
    fail "Alertmanager still has alert active after ${RESOLVE_AM_TIMEOUT}s"
fi

# ── Step 6: Forwarder counters incremented ─────────────────────────
log ""
log "Step 6: alerts-forwarder counters incremented"

# Allow up to ~10s for the resolved counter to tick after the resolve POST.
sleep 5
METRICS=$(docker exec delta-v-alerts-forwarder-1 wget -qO- http://localhost:8080/actuator/prometheus 2>/dev/null || true)

# Every grep here is guarded with { ... || true; } to survive set -o pipefail
# when the metric line isn't present yet (no-match → grep exit 1 → SIGPIPE).
POST_FIRING=$({ echo "$METRICS" | grep -E '^deltav_alerts_forwarder_forwarded_total\{outcome="firing"\}' || true; } | awk '{print $2}' | head -1)
POST_RESOLVED=$({ echo "$METRICS" | grep -E '^deltav_alerts_forwarder_forwarded_total\{outcome="resolved"\}' || true; } | awk '{print $2}' | head -1)
POST_FIRING="${POST_FIRING:-0}"
POST_RESOLVED="${POST_RESOLVED:-0}"

DELTA_FIRING=$(awk -v a="$POST_FIRING" -v b="$PRE_FIRING" 'BEGIN {printf "%.0f", a - b}')
DELTA_RESOLVED=$(awk -v a="$POST_RESOLVED" -v b="$PRE_RESOLVED" 'BEGIN {printf "%.0f", a - b}')

if [ "$DELTA_FIRING" -ge 1 ] 2>/dev/null; then
    ok "forwarded_total{outcome=firing} incremented by ${DELTA_FIRING} (was ${PRE_FIRING}, now ${POST_FIRING})"
else
    fail "forwarded_total{outcome=firing} did not increment (was ${PRE_FIRING}, now ${POST_FIRING})"
fi

if [ "$DELTA_RESOLVED" -ge 1 ] 2>/dev/null; then
    ok "forwarded_total{outcome=resolved} incremented by ${DELTA_RESOLVED} (was ${PRE_RESOLVED}, now ${POST_RESOLVED})"
else
    fail "forwarded_total{outcome=resolved} did not increment (was ${PRE_RESOLVED}, now ${POST_RESOLVED})"
fi

CONSUMED=$({ echo "$METRICS" | grep -E '^deltav_alerts_forwarder_alarms_consumed_total\b' || true; } | awk '{print $2}' | head -1)
if [ -n "$CONSUMED" ] && awk -v c="$CONSUMED" 'BEGIN {exit !(c+0 > 0)}'; then
    ok "alarms_consumed_total > 0 (= ${CONSUMED})"
else
    fail "alarms_consumed_total not > 0 (= '${CONSUMED:-<missing>}')"
fi

# TODO: severity-filter assertion deferred — needs an event-conf entry with
# severity <= NORMAL to exercise the filter's reject path end-to-end.
# AlarmFilterTest provides unit coverage; this E2E gap is acceptable for rc.

# ── Results ────────────────────────────────────────────────────────
log ""
log "Results: $PASS passed, $FAIL failed"
log ""
log "Validated:"
log "  Step 1: alertmanager + alerts-forwarder healthy"
log "  Step 2: AWS Down syslog → alarmd → alarm row in PG"
log "  Step 3: reduction_key on ${ALARMS_TOPIC} (key-only — proto value not decoded)"
log "  Step 4: Alertmanager has active alert with foreignSource:foreignId node label, MINOR severity, non-empty description"
log "  Step 5: AWS Up syslog → alarm CLEARED in PG → Alertmanager alert no longer active"
log "  Step 6: alerts-forwarder forwarded_total{firing,resolved} both incremented"

[ "$FAIL" -eq 0 ] && exit 0 || exit 1
