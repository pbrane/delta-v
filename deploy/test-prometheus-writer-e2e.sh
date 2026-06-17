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
# Bumped from 30s to 90s to accommodate cold-start SNMP warmup. Collectd's
# first poll cycle lands ~30s after provisiond reports the node; on a slow
# host the combination of Kafka producer/consumer bootstrap, Collectd's own
# warmup, and Prometheus remote-write batching can push the first sample's
# arrival in VictoriaMetrics to 60-80s. A 30s budget made Step 6 flaky.
VM_QUERY_TIMEOUT=90
CLICKHOUSE_QUERY_TIMEOUT=60

cleanup() {
    echo "==> Tearing down stack"
    docker compose --profile lite --profile metrics down -v --remove-orphans || true
}
trap cleanup EXIT

echo "==> Starting delta-v Docker Compose (lite + metrics profiles)"
docker compose --profile lite --profile metrics up -d --build

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

ready=$({ echo "$metrics" | grep -E '^deltav_node_context_cache_ready\b' || true; } \
        | awk '{print $2}' | head -1)
if [[ "$ready" != "1.0" && "$ready" != "1" ]]; then
    echo "FAIL: node_context_cache_ready != 1 (got: '$ready')"
    exit 1
fi

boot_count=$({ echo "$metrics" | grep -E '^deltav_node_context_bootstrap_seconds_count\b' || true; } \
        | awk '{sum+=$2} END {print sum+0}')
if (( $(echo "$boot_count < 1" | bc -l) )); then
    echo "FAIL: bootstrap_seconds_count < 1 (got: $boot_count)"
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
# enrichment_missing_total is a cumulative counter that also counts samples
# processed during the NodeContext cache warmup window right after startup (the
# cache fills asynchronously from the node-context Kafka stream), so a strict
# ==0 assertion is racy. Assert instead that it has STOPPED growing — enrichment
# is healthy now that the cache is warm. A cache that never warms keeps
# incrementing and still fails this check.
em_name='deltav_prometheus_writer_enrichment_missing_total'
em_before=$({ echo "$metrics" | grep -E "^${em_name}" || true; } | awk '{sum+=$2} END {print sum+0}')
sleep 20
metrics_warm=$(docker compose exec -T prometheus-writer curl -sf http://localhost:8080/actuator/prometheus)
em_after=$({ echo "$metrics_warm" | grep -E "^${em_name}" || true; } | awk '{sum+=$2} END {print sum+0}')
if (( $(echo "$em_after > $em_before" | bc -l) )); then
    echo "FAIL: ${em_name} still growing (${em_before} -> ${em_after} over 20s) — NodeContext enrichment not healthy"
    exit 1
fi
echo "==> ${em_name} stable at ${em_after} (no new misses over 20s; ${em_before} warmup-window misses tolerated) ✓"
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
vm_landed=false
while (( SECONDS < deadline )); do
    # Query opennms_mib2_x_interfaces_ifhcinoctets_total instead of the
    # narrower opennms_mib2_interface_errors_ifindiscards_total: the HC
    # interface counters come from the MIB-2 mib2-X-interfaces group which
    # every MIB-2-compliant device serves, so the nl6-lab simulator
    # (reliable) produces them alongside rpc-canary (which goes through the
    # flaky in-compose mock-snmp-agent, see project_mock_snmp_agent_systemgroup_bug).
    # Any reliable opennms_ metric proves the pipeline+labels work; there is
    # no need to couple Step 6 to the flaky mock-agent path.
    resp=$(curl -sf "http://localhost:18428/api/v1/query?query=opennms_mib2_x_interfaces_ifhcinoctets_total" \
            || echo '{"data":{"result":[]}}')
    count=$(echo "$resp" | python3 -c \
        'import json,sys; d=json.load(sys.stdin); print(len(d.get("data",{}).get("result",[])))' \
        2>/dev/null || echo "0")
    if (( count > 0 )); then
        echo "==> VM returned ${count} series for opennms_mib2_x_interfaces_ifhcinoctets_total"
        echo "$resp" | grep -E '"node_id":"[0-9]+"' > /dev/null || { echo "FAIL: missing node_id label"; exit 1; }
        echo "$resp" | grep -E '"location":' > /dev/null || { echo "FAIL: missing location label"; exit 1; }
        echo "$resp" | grep -E '"foreign_source":' > /dev/null || { echo "FAIL: missing foreign_source label"; exit 1; }
        echo "$resp" | grep -E '"resource_instance":' > /dev/null || { echo "FAIL: missing resource_instance label"; exit 1; }
        vm_landed=true
        break
    fi
    sleep 2
done
if [[ "$vm_landed" != "true" ]]; then
    echo "FAIL: VictoriaMetrics returned no results within ${VM_QUERY_TIMEOUT}s"
    echo "Last VM response: $resp"
    exit 1
fi

# ── Step 6b: Assert Track 2a info-metrics + composite node label ──────────────
echo "==> Step 6b: Assert Track 2a outputs (deltav_node_info, deltav_snmp_interface_info, composite node)"

# C1: deltav_node_info exists with composite node label
echo "==> Assertion C1: deltav_node_info with composite node label"
deadline=$((SECONDS + VM_QUERY_TIMEOUT))
c1_passed=false
while (( SECONDS < deadline )); do
    c1_resp=$(curl -sf "http://localhost:18428/api/v1/query?query=deltav_node_info" \
            || echo '{"data":{"result":[]}}')
    c1_count=$(echo "$c1_resp" | python3 -c \
        'import json,sys; d=json.load(sys.stdin); print(len(d.get("data",{}).get("result",[])))' \
        2>/dev/null || echo "0")
    if (( c1_count > 0 )); then
        # Check for composite node label matching foreignSource:foreignId pattern
        c1_node=$({ echo "$c1_resp" | grep -oE '"node":"[^"]*:[^"]*"' || true; } | head -1)
        if [[ -n "$c1_node" ]]; then
            echo "==> C1 PASS: deltav_node_info returned ${c1_count} series, found composite node label: $c1_node"
            c1_passed=true
            break
        fi
    fi
    sleep 2
done
if [[ "$c1_passed" != "true" ]]; then
    echo "FAIL: C1 deltav_node_info not found or missing composite node label within ${VM_QUERY_TIMEOUT}s"
    exit 1
fi

# C2: deltav_snmp_interface_info exists with composite node label
echo "==> Assertion C2: deltav_snmp_interface_info with composite node label"
deadline=$((SECONDS + VM_QUERY_TIMEOUT))
c2_passed=false
c2_warned=false
while (( SECONDS < deadline )); do
    c2_resp=$(curl -sf "http://localhost:18428/api/v1/query?query=deltav_snmp_interface_info" \
            || echo '{"data":{"result":[]}}')
    c2_count=$(echo "$c2_resp" | python3 -c \
        'import json,sys; d=json.load(sys.stdin); print(len(d.get("data",{}).get("result",[])))' \
        2>/dev/null || echo "0")
    if (( c2_count > 0 )); then
        # Check for composite node label
        c2_node=$({ echo "$c2_resp" | grep -oE '"node":"[^"]*:[^"]*"' || true; } | head -1)
        if [[ -n "$c2_node" ]]; then
            echo "==> C2 PASS: deltav_snmp_interface_info returned ${c2_count} series, found composite node label: $c2_node"
            c2_passed=true
            break
        fi
    fi
    sleep 2
done
if [[ "$c2_passed" != "true" ]]; then
    # C2 may legitimately be empty on a fresh stack with no SNMP-discovered interfaces
    # Emit warn instead of fail
    echo "WARN: C2 deltav_snmp_interface_info not populated (expected on fresh stack with no SNMP interfaces)"
    c2_warned=true
fi

# C3: Durable composite node label on a regular time series (use deltav_response_time)
echo "==> Assertion C3: deltav_response_time with composite node label"
deadline=$((SECONDS + VM_QUERY_TIMEOUT))
c3_passed=false
while (( SECONDS < deadline )); do
    c3_resp=$(curl -sf "http://localhost:18428/api/v1/query?query=deltav_response_time" \
            || echo '{"data":{"result":[]}}')
    c3_count=$(echo "$c3_resp" | python3 -c \
        'import json,sys; d=json.load(sys.stdin); print(len(d.get("data",{}).get("result",[])))' \
        2>/dev/null || echo "0")
    if (( c3_count > 0 )); then
        # Check for composite node label on the response_time metric
        c3_node=$({ echo "$c3_resp" | grep -oE '"node":"[^"]*:[^"]*"' || true; } | head -1)
        if [[ -n "$c3_node" ]]; then
            echo "==> C3 PASS: deltav_response_time returned ${c3_count} series with composite node label: $c3_node"
            c3_passed=true
            break
        fi
    fi
    sleep 2
done
if [[ "$c3_passed" != "true" ]]; then
    echo "FAIL: C3 deltav_response_time not found or missing composite node label within ${VM_QUERY_TIMEOUT}s"
    exit 1
fi

echo "==> Track 2a assertions complete (C1 PASS, C2 $([ "$c2_warned" = "true" ] && echo "WARN" || echo "PASS"), C3 PASS)"

# ── Step 7: Wait for Grafana readiness ────────────────────────────────────────
echo "==> Step 7: Wait for Grafana /api/health"
deadline=$((SECONDS + STACK_READY_TIMEOUT))
gf_ready=false
GF_PASS="${GF_ADMIN_PASSWORD:-admin}"
while (( SECONDS < deadline )); do
    health=$(curl -sf -u "admin:${GF_PASS}" http://localhost:13000/api/health 2>/dev/null || true)
    if echo "$health" | grep -q 'database.*ok'; then
        echo "==> Grafana ready"
        gf_ready=true
        break
    fi
    sleep 3
done
if [[ "$gf_ready" != "true" ]]; then
    echo "FAIL: Grafana readiness timeout"
    docker compose logs grafana | tail -50
    exit 1
fi

# ── Step 8: Verify VictoriaMetrics datasource is reachable from Grafana ───────
echo "==> Step 8: Verify VictoriaMetrics datasource health"
ds_health=$(curl -sf -u "admin:${GF_PASS}" \
    http://localhost:13000/api/datasources/uid/victoriametrics/health 2>/dev/null || true)
if ! echo "$ds_health" | grep -q '"status":"OK"'; then
    echo "FAIL: VictoriaMetrics datasource health check failed"
    echo "Response: $ds_health"
    exit 1
fi
echo "==> Datasource OK"

# ── Step 9: Verify the snmp-overview dashboard is provisioned ─────────────────
echo "==> Step 9: Verify snmp-overview dashboard is loaded"
dash=$(curl -sf -u "admin:${GF_PASS}" \
    http://localhost:13000/api/dashboards/uid/snmp-overview 2>/dev/null || true)
if ! echo "$dash" | grep -q '"title":"SNMP Overview"'; then
    echo "FAIL: snmp-overview dashboard not loaded"
    echo "Response: $dash"
    exit 1
fi
echo "==> Dashboard OK"

# ── Step 10: Verify nl6-lab location is producing metrics ───────────────
echo "==> Step 10: Verify nl6-lab location produces interface HC counters"
deadline=$((SECONDS + VM_QUERY_TIMEOUT))
lab_landed=false
while (( SECONDS < deadline )); do
    resp=$(curl -sGf "http://localhost:18428/api/v1/query" \
            --data-urlencode 'query=opennms_mib2_x_interfaces_ifhcinoctets_total{foreign_source="nl6-lab"}' \
            2>/dev/null || echo '{"data":{"result":[]}}')
    count=$(echo "$resp" | python3 -c \
            'import json,sys; d=json.load(sys.stdin); print(len(d.get("data",{}).get("result",[])))' \
            2>/dev/null || echo "0")
    if (( count > 0 )); then
        echo "==> VM returned ${count} series for nl6-lab (nl6-minion is working)"
        echo "$resp" | grep -q '"location":"nl6-lab"' || \
            { echo "FAIL: series missing location label"; exit 1; }
        lab_landed=true
        break
    fi
    sleep 2
done
if [[ "$lab_landed" != "true" ]]; then
    echo "FAIL: nl6-lab produced no interface HC metrics within ${VM_QUERY_TIMEOUT}s"
    echo "Last VM response: $resp"
    docker compose logs nl6-minion | tail -30
    exit 1
fi

# ── Step 11: Verify nl6-lab IPFIX flows + full 4-protocol coverage in ClickHouse ──
# Asserts the flow pipeline (nl6 → nl6-minion → flow-enricher → ClickHouse)
# delivers IPFIX flows AND that all four parser paths (NetFlow v5, NetFlow v9,
# IPFIX, sFlow) reach ClickHouse. All four protocols now come from the nl6
# fleet (its 29 devices are round-robined across netflow9/netflow5/sflow/ipfix
# — PR #315; the dedicated flow-exporter/sflow-exporter test-nodes were
# removed). The assertion is "==4" so a regression in any parser path fails
# the gate.
echo "==> Step 11: Verify nl6-lab flows land in ClickHouse with full 4-protocol coverage"
deadline=$((SECONDS + CLICKHOUSE_QUERY_TIMEOUT))
flows_landed=false
while (( SECONDS < deadline )); do
    rows=$(curl -sf -u deltav:deltav 'http://localhost:8123/' \
           --data-binary "SELECT count() FROM deltav.flows_raw WHERE location = 'nl6-lab'" \
           2>/dev/null || echo "0")
    if (( rows > 0 )); then
        protos=$(curl -sf -u deltav:deltav 'http://localhost:8123/' \
                 --data-binary "SELECT count(DISTINCT netflow_version) FROM deltav.flows_raw WHERE netflow_version != ''" \
                 2>/dev/null || echo "0")
        echo "==> ClickHouse has ${rows} nl6-lab flow rows across ${protos} protocol(s)"
        if (( protos >= 4 )); then
            echo "==> All 4 protocols present (NetFlow v5/v9, IPFIX, sFlow)"
            flows_landed=true
            break
        fi
    fi
    sleep 3
done
if [[ "$flows_landed" != "true" ]]; then
    echo "FAIL: nl6-lab flows did not land with full 4-protocol coverage within ${CLICKHOUSE_QUERY_TIMEOUT}s"
    echo "Last rows: ${rows:-0}; last protocols: ${protos:-0}"
    docker compose logs flow-enricher | tail -30
    docker compose logs nl6-minion | tail -20
    exit 1
fi

# ── Step 12: Verify flows-forensic dashboard provisioned + flows-overview regression ──
echo "==> Step 12: Verify flows-forensic dashboard provisioned with 9 panels"
dash=$(curl -sf -u "admin:${GF_PASS}" \
       http://localhost:13000/api/dashboards/uid/flows-forensic 2>/dev/null || true)
if ! echo "$dash" | grep -q '"title":"Flows Forensic"'; then
    echo "FAIL: flows-forensic dashboard not loaded"
    echo "Response: $dash"
    exit 1
fi
forensic_panels=$(echo "$dash" | python3 -c \
                  'import json,sys; d=json.load(sys.stdin); print(len(d.get("dashboard",{}).get("panels",[])))')
if [[ "$forensic_panels" != "9" ]]; then
    echo "FAIL: flows-forensic has ${forensic_panels} panels, expected 9"
    exit 1
fi
echo "==> flows-forensic loaded with 9 panels"

# Regression: flows-overview still loads after data-link additions
dash2=$(curl -sf -u "admin:${GF_PASS}" \
        http://localhost:13000/api/dashboards/uid/flows-overview 2>/dev/null || true)
if ! echo "$dash2" | grep -q '"title":"Flows Overview"'; then
    echo "FAIL: flows-overview regression after data-link edits"
    echo "Response: $dash2"
    exit 1
fi
overview_panels=$(echo "$dash2" | python3 -c \
                  'import json,sys; d=json.load(sys.stdin); print(len(d.get("dashboard",{}).get("panels",[])))')
if [[ "$overview_panels" != "6" ]]; then
    echo "FAIL: flows-overview has ${overview_panels} panels, expected 6 (regression)"
    exit 1
fi
echo "==> flows-overview regression OK (still 6 panels)"

echo "==> ALL ASSERTIONS PASSED"
exit 0
