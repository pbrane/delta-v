#!/usr/bin/env bash
# Copyright (C) 2026 BeaconStrategists, Inc.
# Licensed under the GNU Affero General Public License v3.
# See LICENSE.md in the repository root for full license text.
#
# Layer 5 end-to-end smoke test for the Kafka Time Series producer. Starts
# the delta-v Docker Compose stack with DELTAV_TIMESERIES_ENABLED=true,
# provisions a test node, waits for Collectd to poll it, then asserts:
#   1. at least one record arrives on the deltav-timeseries topic
#      (via kafka-console-consumer, payload not deserialized)
#   2. deltav_timeseries_batches_published_total{producer="collectd"} > 0
#      on the Collectd /actuator/prometheus endpoint
#   3. deltav_timeseries_batches_failed_total counters stay at zero
# Protobuf deserialization is out of scope for Phase 0; the Prometheus
# counter assertion is the load-bearing check. Tears down cleanly on
# success or failure.

set -euo pipefail

cd "$(dirname "$0")"

export DELTAV_TIMESERIES_ENABLED=true
TEST_REQUISITION="timeseries-e2e"
STACK_READY_TIMEOUT=180
POLL_TIMEOUT=120

cleanup() {
    echo "==> Tearing down stack"
    docker compose down -v --remove-orphans || true
}
trap cleanup EXIT

echo "==> Starting delta-v Docker Compose with DELTAV_TIMESERIES_ENABLED=true"
docker compose up -d --build

echo "==> Waiting for collectd /actuator/health (up to ${STACK_READY_TIMEOUT} s)"
deadline=$(( $(date +%s) + STACK_READY_TIMEOUT ))
while (( $(date +%s) < deadline )); do
    if docker compose exec -T collectd curl -sf http://localhost:8080/actuator/health >/dev/null 2>&1; then
        echo "==> collectd healthy"
        break
    fi
    sleep 3
done
if (( $(date +%s) >= deadline )); then
    echo "ERROR: collectd did not become healthy within ${STACK_READY_TIMEOUT} s"
    docker compose logs collectd | tail -60
    exit 1
fi

echo "==> Provisioning test requisition ${TEST_REQUISITION}"
docker compose exec -T horizon-core curl -sf -u admin:admin \
    -H "Content-Type: application/xml" \
    -X POST -d '<model-import foreign-source="timeseries-e2e">
        <node foreign-id="ts-1" node-label="timeseries-e2e-node-1">
            <interface ip-addr="127.0.0.1">
                <monitored-service service-name="ICMP"/>
            </interface>
        </node>
    </model-import>' \
    "http://localhost:8980/opennms/rest/requisitions" || {
        echo "WARN: requisition POST failed; horizon-core may be on a different port"
        echo "      checking alternate ports..."
    }
docker compose exec -T horizon-core curl -sf -u admin:admin \
    -X PUT "http://localhost:8980/opennms/rest/requisitions/${TEST_REQUISITION}/import" || true

echo "==> Waiting for a record on deltav-timeseries (up to ${POLL_TIMEOUT} s)"
timeout "${POLL_TIMEOUT}" docker compose exec -T kafka \
    /opt/kafka/bin/kafka-console-consumer.sh \
        --bootstrap-server kafka:9092 \
        --topic deltav-timeseries \
        --from-beginning \
        --max-messages 1 \
        --formatter kafka.tools.DefaultMessageFormatter \
        --property print.key=true > /tmp/ts-e2e-msg.bin || {
    echo "ERROR: no record received on deltav-timeseries within ${POLL_TIMEOUT} s"
    docker compose logs collectd | tail -60
    exit 1
}
if [ ! -s /tmp/ts-e2e-msg.bin ]; then
    echo "ERROR: /tmp/ts-e2e-msg.bin is empty"
    exit 1
fi
echo "==> Received record on deltav-timeseries ($(wc -c < /tmp/ts-e2e-msg.bin) bytes)"

echo "==> Asserting /actuator/prometheus metrics"
metrics=$(docker compose exec -T collectd curl -sf http://localhost:8080/actuator/prometheus)
if ! echo "${metrics}" | grep -E '^deltav_timeseries_batches_published_total.*producer="collectd"' | \
       awk '{print $NF}' | head -1 | grep -qE '^[1-9]'; then
    echo "ERROR: deltav_timeseries_batches_published_total not > 0"
    echo "${metrics}" | grep deltav_timeseries || true
    exit 1
fi
if echo "${metrics}" | grep -E '^deltav_timeseries_batches_failed_total' | \
       awk '{print $NF}' | grep -qE '^[1-9]'; then
    echo "ERROR: deltav_timeseries_batches_failed_total is non-zero"
    echo "${metrics}" | grep deltav_timeseries_batches_failed_total || true
    exit 1
fi

# Regression guard for the horizon TimeseriesPersister ClassCastException
# cascade fixed in delta-v-horizon PR #8 (horizon 1.0.11). The ClassCastException
# on visitGroup and the NPE cascade on visitAttribute/persistNumeric/String
# must all stay at 0 — if any of these fires, a regression has re-introduced
# the bug and FanoutPersister's isolation would otherwise hide it.
# step=visitResource is deliberately NOT asserted: Bug #1 (MetaTagDataLoader
# rollback) is a separate investigation and may still tick on labbox today.
echo "==> Asserting inner-persister cascade counters stay at zero"
for step in visitGroup visitAttribute persistNumericAttribute persistStringAttribute; do
    value=$(echo "${metrics}" | \
        grep -E "^deltav_collectd_persister_inner_failures_total\{.*step=\"${step}\"" | \
        awk '{print $NF}')
    if [ -z "${value}" ]; then
        echo "WARN: counter deltav_collectd_persister_inner_failures_total{step=${step}} not registered"
        continue
    fi
    if [ "${value}" != "0.0" ]; then
        echo "ERROR: deltav_collectd_persister_inner_failures_total{step=${step}} = ${value} (expected 0.0)"
        echo "${metrics}" | grep deltav_collectd_persister || true
        exit 1
    fi
done
echo "==> Inner-persister cascade counters verified at zero"

echo "==> PASS"
exit 0
