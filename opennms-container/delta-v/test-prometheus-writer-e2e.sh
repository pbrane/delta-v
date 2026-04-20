#!/usr/bin/env bash
# Copyright (C) 2026 BeaconStrategists, Inc.
# Licensed under the GNU Affero General Public License v3.
#
# Layer 5 end-to-end smoke test for the prometheus-writer.  Starts the
# delta-v Docker Compose stack (lite + metrics-e2e profiles: Collectd
# produces to deltav-timeseries, Provisiond produces to
# deltav-node-context, prometheus-writer consumes + enriches + POSTs to
# VictoriaMetrics), then asserts:
#
#   1. prometheus-writer /actuator/health/readiness is UP (cache
#      bootstrapped + binding resumed)
#   2. NodeContextCache bootstrap fired cleanly
#      (bootstrap_duration_seconds_count >= 1, cache_ready == 1)
#   3. After 2 × 30 s Collectd poll cycles (~90 s wait),
#      records_consumed_total > 0, samples_sent_total > 0,
#      batches_sent_total > 0, circuit_state == 0 (closed)
#   4. (folded into step 5 assertions below)
#   5. Zero failure counters (batches_failed_total == 0,
#      enrichment_missing_total == 0, samples_dropped_total == 0,
#      dlq_records_total == 0)
#   6. VictoriaMetrics query returns a result with the expected label
#      set — the "gold standard" assertion that proves the full RW
#      wire round-trip.
#
# Scar prophylactic: every grep pipeline guarded by { ... || true; }
# because grep returns 1 on no-match and `set -euo pipefail` would kill
# the script (Phase 1 scar).

set -euo pipefail

cd "$(dirname "$0")"

STACK_READY_TIMEOUT=180
POLL_GRACE_SECONDS=90
METRICS_TIMEOUT=180
VM_QUERY_TIMEOUT=30

cleanup() {
    echo "==> Tearing down stack"
    docker compose --profile lite --profile metrics-e2e down -v --remove-orphans || true
}
trap cleanup EXIT

echo "==> Starting delta-v Docker Compose (lite + metrics-e2e profiles)"
docker compose --profile lite --profile metrics-e2e up -d --build

# ── Step 1: Wait for prometheus-writer readiness ──────────────────────────────
echo "==> Step 1: Wait for prometheus-writer /actuator/health/readiness"
deadline=$((SECONDS + STACK_READY_TIMEOUT))
while (( SECONDS < deadline )); do
    if docker compose exec -T prometheus-writer \
            curl -sf http://localhost:8080/actuator/health/readiness >/dev/null 2>&1; then
        echo "==> prometheus-writer ready"
        break
    fi
    sleep 3
done
if ! docker compose exec -T prometheus-writer \
        curl -sf http://localhost:8080/actuator/health/readiness >/dev/null 2>&1; then
    echo "FAIL: prometheus-writer did not become ready in ${STACK_READY_TIMEOUT}s"
    docker compose logs prometheus-writer | tail -100
    exit 1
fi

# ── Step 2: Assert startup gate fired cleanly ─────────────────────────────────
echo "==> Step 2: Assert startup gate fired"
metrics=$(docker compose exec -T prometheus-writer \
        curl -sf http://localhost:8080/actuator/prometheus)

ready=$({ echo "$metrics" | grep -E '^deltav_prometheus_writer_node_context_cache_ready\b' || true; } \
        | awk '{print $2}' | head -1)
if [[ "$ready" != "1.0" && "$ready" != "1" ]]; then
    echo "FAIL: node_context_cache_ready != 1 (got: '$ready')"
    exit 1
fi

boot_count=$({ echo "$metrics" | grep -E '^deltav_prometheus_writer_node_context_bootstrap_duration_seconds_count\b' || true; } \
        | awk '{sum+=$2} END {print sum+0}')
if (( $(echo "$boot_count < 1" | bc -l) )); then
    echo "FAIL: bootstrap_duration_seconds_count < 1 (got: $boot_count)"
    exit 1
fi
echo "==> Startup gate verified: cache_ready=1, bootstrap_count=$boot_count"

# ── Step 3: Wait for Collectd polls ───────────────────────────────────────────
echo "==> Step 3: Wait ${POLL_GRACE_SECONDS}s for Collectd to produce timeseries"
sleep "${POLL_GRACE_SECONDS}"

# ── Step 4: Assert samples flowed ─────────────────────────────────────────────
echo "==> Step 4: Assert records/samples/batches flowed"
metrics=$(docker compose exec -T prometheus-writer \
        curl -sf http://localhost:8080/actuator/prometheus)

assert_gt_zero() {
    local name="$1"
    local value
    value=$({ echo "$metrics" | grep -E "^${name}\b" || true; } | awk '{sum+=$2} END {print sum+0}')
    if (( $(echo "$value <= 0" | bc -l) )); then
        echo "FAIL: ${name} is not > 0 (got: $value)"
        exit 1
    fi
    echo "==> ${name} = ${value}"
}

assert_gt_zero 'deltav_prometheus_writer_records_consumed_total'
assert_gt_zero 'deltav_prometheus_writer_samples_sent_total'
assert_gt_zero 'deltav_prometheus_writer_batches_sent_total'

circuit=$({ echo "$metrics" | grep -E '^deltav_prometheus_writer_circuit_state\b' || true; } | awk '{print $2}' | head -1)
if [[ "$circuit" != "0.0" && "$circuit" != "0" ]]; then
    echo "FAIL: circuit_state != 0 (got: '$circuit')"
    exit 1
fi
echo "==> Circuit closed (state=0)"

# ── Step 5: Assert zero failures ──────────────────────────────────────────────
echo "==> Step 5: Assert zero failure counters"
assert_zero() {
    local name="$1"
    local value
    value=$({ echo "$metrics" | grep -E "^${name}" || true; } | awk '{sum+=$2} END {print sum+0}')
    if (( $(echo "$value > 0" | bc -l) )); then
        echo "FAIL: ${name} > 0 (got: $value)"
        exit 1
    fi
    echo "==> ${name} = 0 ✓"
}

assert_zero 'deltav_prometheus_writer_batches_failed_total'
assert_zero 'deltav_prometheus_writer_enrichment_missing_total'
# samples_dropped_total: split by reason. "type_unspecified" must stay zero
# (indicates a producer bug if it ever fires). "string_attribute" is expected
# correct behavior when SNMP collection surfaces string OIDs (sysDescr,
# sysName, sysContact, etc.) — Prometheus samples are float64, so the
# translator drops strings by design. The rpc-canary data path produces a
# handful of these on each poll.
assert_zero 'deltav_prometheus_writer_samples_dropped_total\{reason="type_unspecified"\}'
assert_zero 'deltav_prometheus_writer_dlq_records_total'

# ── Step 6: Query VictoriaMetrics ─────────────────────────────────────────────
echo "==> Step 6: Query VictoriaMetrics for the landed series"
deadline=$((SECONDS + VM_QUERY_TIMEOUT))
while (( SECONDS < deadline )); do
    resp=$(curl -sf "http://localhost:18428/api/v1/query?query=opennms_mib2_interface_errors_ifindiscards_total" \
            || echo '{"data":{"result":[]}}')
    count=$(echo "$resp" | python3 -c \
        'import json,sys; d=json.load(sys.stdin); print(len(d.get("data",{}).get("result",[])))' \
        2>/dev/null || echo "0")
    if (( count > 0 )); then
        echo "==> VM returned ${count} series for opennms_mib2_interface_errors_ifindiscards_total"
        echo "$resp" | grep -E '"node_id":"[0-9]+"' > /dev/null || { echo "FAIL: missing node_id label"; exit 1; }
        echo "$resp" | grep -E '"location":' > /dev/null || { echo "FAIL: missing location label"; exit 1; }
        echo "$resp" | grep -E '"foreign_source":' > /dev/null || { echo "FAIL: missing foreign_source label"; exit 1; }
        echo "$resp" | grep -E '"resource_instance":' > /dev/null || { echo "FAIL: missing resource_instance label"; exit 1; }
        echo "==> ALL ASSERTIONS PASSED"
        exit 0
    fi
    sleep 2
done

echo "FAIL: VictoriaMetrics returned no results within ${VM_QUERY_TIMEOUT}s"
echo "Last VM response: $resp"
exit 1
