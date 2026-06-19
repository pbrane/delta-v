#!/usr/bin/env bash
# Copyright (C) 2026 BeaconStrategists, Inc.
# Licensed under the GNU Affero General Public License v3.
# See LICENSE.md in the repository root for full license text.
#
# Layer 5 end-to-end smoke test for the Kafka Time Series producer. Runs
# against a pre-deployed full stack (./deploy.sh up full), asserts:
#   1. at least one record arrives on the deltav-timeseries topic
#      (via kafka-console-consumer, payload not deserialized)
#   2. deltav_timeseries_batches_published_total{producer="collectd"} > 0
#      on the Collectd /actuator/prometheus endpoint
#   3. deltav_timeseries_batches_failed_total counters stay at zero
#   4. inner-persister cascade counters stay at zero (regression guard for
#      horizon TimeseriesPersister ClassCastException cascade fixed in
#      delta-v-horizon PR #8 / horizon 1.0.11)
#
# This test was rewritten (2026-04-22) to drop its previous dependency on
# the legacy horizon-core webapp REST API (which was removed from delta-v
# in PR #28 — project_webapp_removed). The committed full stack already
# imports multiple requisitions at startup (delta-v, cloud-services,
# rpc-canary, nl6-lab, perspective-test) so Collectd has
# plenty of nodes to poll without this test needing to provision its own.
#
# Prerequisites:
#   ./deploy.sh up full  (must be running with all daemons healthy)
#
# Usage:
#   ./test-timeseries-e2e.sh              Run the test
#   ./test-timeseries-e2e.sh --pre-clean  (no-op, kept for parity with other tests)
#
# Exit codes:
#   0 = all tests passed
#   1 = test failure
#   2 = prerequisite failure

set -euo pipefail

cd "$(dirname "$0")"

POLL_TIMEOUT=120
# Tempfile for kafka-console-consumer output; auto-cleaned on exit.
MSG_FILE=$(mktemp -t ts-e2e-msg.XXXXXX)

cleanup() {
    rm -f "${MSG_FILE}"
}
trap cleanup EXIT

# ── Prerequisites ─────────────────────────────────────────────────────────────
echo "==> Checking prerequisites..."
RUNNING=$(docker compose ps --status running --format '{{.Name}}' 2>/dev/null)
for svc in postgres kafka collectd provisiond minion; do
    if ! echo "$RUNNING" | grep -q "$svc"; then
        echo "ERROR: Service '$svc' is not running. Deploy with: ./deploy.sh up full" >&2
        exit 2
    fi
done
echo "==> Required services running (postgres, kafka, collectd, provisiond, minion)"

# ── Step 1: Wait for a record on deltav-timeseries ────────────────────────────
echo "==> Waiting for a record on deltav-timeseries (up to ${POLL_TIMEOUT} s)"
timeout "${POLL_TIMEOUT}" docker compose exec -T kafka \
    /opt/kafka/bin/kafka-console-consumer.sh \
        --bootstrap-server kafka:9092 \
        --topic deltav-timeseries \
        --from-beginning \
        --max-messages 1 \
        --property print.key=true > "${MSG_FILE}" 2>/dev/null || {
    echo "ERROR: no record received on deltav-timeseries within ${POLL_TIMEOUT} s"
    docker compose logs collectd | tail -60
    exit 1
}
if [ ! -s "${MSG_FILE}" ]; then
    echo "ERROR: ${MSG_FILE} is empty"
    exit 1
fi
echo "==> Received record on deltav-timeseries ($(wc -c < "${MSG_FILE}") bytes)"

# ── Step 2: Assert Collectd published counter ─────────────────────────────────
echo "==> Asserting /actuator/prometheus metrics"
metrics=$(docker compose exec -T collectd curl -sf http://localhost:8080/actuator/prometheus)
if ! echo "${metrics}" | grep -E '^deltav_timeseries_batches_published_total.*producer="collectd"' | \
       awk '{print $NF}' | head -1 | grep -qE '^[1-9]'; then
    echo "ERROR: deltav_timeseries_batches_published_total not > 0"
    echo "${metrics}" | grep deltav_timeseries || true
    exit 1
fi
echo "==> deltav_timeseries_batches_published_total > 0"

if echo "${metrics}" | grep -E '^deltav_timeseries_batches_failed_total' | \
       awk '{print $NF}' | grep -qE '^[1-9]'; then
    echo "ERROR: deltav_timeseries_batches_failed_total is non-zero"
    echo "${metrics}" | grep deltav_timeseries_batches_failed_total || true
    exit 1
fi
echo "==> deltav_timeseries_batches_failed_total = 0"

# ── Step 3: Regression guard for horizon TimeseriesPersister cascade ──────────
# Bugs fixed in delta-v-horizon PR #8 (horizon 1.0.11). The
# ClassCastException on visitGroup and the NPE cascade on
# visitAttribute/persistNumeric/String must all stay at 0 — if any of
# these fires, a regression has re-introduced the bug and
# FanoutPersister's isolation would otherwise hide it.
# step=visitResource is deliberately NOT asserted: Bug #1
# (MetaTagDataLoader rollback) is a separate investigation and may still
# tick today.
echo "==> Asserting inner-persister cascade counters stay at zero"
for step in visitGroup visitAttribute persistNumericAttribute persistStringAttribute completeResource; do
    value=$(echo "${metrics}" | \
        grep -E "^deltav_collectd_persister_inner_failures_total\{.*step=\"${step}\"" | \
        awk '{print $NF}')
    if [ -z "${value}" ]; then
        echo "  (counter step=${step} not yet registered — no cascade has fired)"
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
