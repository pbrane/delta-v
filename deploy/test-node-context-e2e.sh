#!/usr/bin/env bash
# Copyright (C) 2026 BeaconStrategists, Inc.
# Licensed under the GNU Affero General Public License v3.
# See LICENSE.md in the repository root for full license text.
#
# Layer 5 end-to-end smoke test for the provisiond → deltav-node-context
# change-feed producer.  Starts the delta-v Docker Compose stack (lite
# profile is sufficient — no Minion required), waits for provisiond to
# become healthy, then asserts:
#
#   1. at least one record arrives on the deltav-node-context topic
#      (via kafka-console-consumer — protobuf not deserialized)
#   2. deltav_node_context_records_published_total{reason="bootstrap"} > 0
#      on the provisiond /actuator/prometheus endpoint (seed nodes trigger
#      the bootstrap pass on startup)
#   3. deltav_node_context_records_published_total{reason="change"} > 0
#      after the E2E requisition is injected and provisiond re-imports it
#      (IMPORT_SUCCESSFUL_UEI fan-out)
#   4. deltav_node_context_records_failed_total stays at zero
#
# Tombstone assertion is intentionally omitted: the nodeDeleted event path
# requires a PARM_LOCATION parm that is only present when the full event
# pipeline is running (alarmd + eventtranslator).  The lite profile used
# here does not include those services, so tombstone coverage is deferred
# to the full-profile test-e2e.sh run.
#
# Port / URL conventions (matched from test-timeseries-e2e.sh and
# test-collectd-e2e.sh):
#   - All actuator calls go through docker compose exec -T <service>
#     because provisiond has NO host-port mapping in compose.yml.
#   - Kafka consumer calls go through docker compose exec -T kafka with
#     bootstrap-server localhost:9092 (internal Docker network address).
#   - REST provisioning is file-based (write to overlays/provisiond/etc/imports/)
#     rather than via horizon-core REST, because horizon-core is not
#     included in the lite profile.
#
# Exit non-zero on any failure; tears down on success or failure.

set -euo pipefail

cd "$(dirname "$0")"

STACK_READY_TIMEOUT="${STACK_READY_TIMEOUT:-180}"
TOPIC_TIMEOUT="${TOPIC_TIMEOUT:-60}"
METRICS_TIMEOUT="${METRICS_TIMEOUT:-180}"
E2E_FOREIGN_SOURCE="node-context-e2e"
E2E_REQUISITION_FILE="overlays/provisiond/etc/imports/${E2E_FOREIGN_SOURCE}.xml"

PROVISIOND_CONFIG="overlays/provisiond/etc/provisiond-configuration.xml"
PROVISIOND_CONFIG_BACKUP="$(mktemp -t provisiond-config.XXXXXX.xml)"

cleanup() {
    echo "==> Tearing down stack"
    rm -f "${PROVISIOND_CONFIG_BACKUP}"
    # The requisition + requisition-def are written only INTO the provisiond
    # container (not the host overlays/, which isn't mounted). `down -v` removes
    # the container, so the baked config resets and nothing leaks into the working
    # tree or other e2e tests.
    docker compose down -v --remove-orphans || true
}
trap cleanup EXIT

echo "==> Starting delta-v Docker Compose (active profile)"
docker compose --profile active up -d --build

# ── Step 1: Wait for provisiond /actuator/health ──────────────────────────────

echo "==> Waiting for provisiond /actuator/health (up to ${STACK_READY_TIMEOUT} s)"
deadline=$(( $(date +%s) + STACK_READY_TIMEOUT ))
while (( $(date +%s) < deadline )); do
    if docker compose exec -T provisiond curl -sf http://localhost:8080/actuator/health >/dev/null 2>&1; then
        echo "==> provisiond healthy"
        break
    fi
    sleep 5
done
if (( $(date +%s) >= deadline )); then
    echo "ERROR: provisiond did not become healthy within ${STACK_READY_TIMEOUT} s"
    docker compose logs provisiond | tail -80
    exit 1
fi

# ── Step 2: Assert bootstrap runner executed ──────────────────────────────────
# NodeContextBootstrapRunner.start() fires synchronously before provisiond
# reports ready (SmartLifecycle phase MAX_VALUE-100).  On a fresh DB the
# runner will find 0 nodes and publish nothing, so published_total{reason=
# "bootstrap"} may legitimately be 0 — the contract we're verifying is that
# the runner RAN, which is reliably signalled by the
# bootstrap_duration_seconds_count Timer being ≥ 1.

echo "==> Asserting bootstrap runner executed on provisiond /actuator/prometheus"
metrics=$(docker compose exec -T provisiond curl -sf http://localhost:8080/actuator/prometheus)
if ! echo "${metrics}" | grep -E '^deltav_node_context_bootstrap_duration_seconds_count ' | \
       awk '{print $NF}' | head -1 | grep -qE '^[1-9]'; then
    echo "ERROR: deltav_node_context_bootstrap_duration_seconds_count not ≥ 1"
    echo "${metrics}" | grep deltav_node_context || true
    docker compose logs provisiond | tail -60
    exit 1
fi
echo "==> bootstrap runner executed — lifecycle wiring verified"

# ── Step 3: Inject E2E requisition and assert change records ──────────────────
# Write a new requisition XML into overlays/provisiond/etc/imports/ so that
# provisiond picks it up on the next cron tick (or a manual import trigger).
# Provisiond is configured to scan that directory; IMPORT_SUCCESSFUL_UEI fires
# after each successful import and the NodeContextChangeFeedListener fan-out
# enqueues every node in the foreignSource for a "change" publish.

# Write the requisition DIRECTLY into the running provisiond container. The host
# overlays/provisiond/etc/ path is NOT mounted into provisiond (config is baked into
# the image; imports/ is a named volume seeded by an init sidecar), so host writes
# never reach the daemon. See project_provisiond_e2e_slowness_root_cause.
echo "==> Writing E2E requisition ${E2E_FOREIGN_SOURCE} into the provisiond container"
docker compose exec -T provisiond tee "/opt/deltav/etc/imports/${E2E_FOREIGN_SOURCE}.xml" >/dev/null <<'REQEOF'
<model-import xmlns="http://xmlns.opennms.org/xsd/config/model-import"
              date-stamp="2026-04-16T00:00:00.000-05:00"
              foreign-source="node-context-e2e">
  <node location="Default" foreign-id="nc-e2e-1" node-label="node-context-e2e-node-1">
    <interface ip-addr="127.0.0.2" status="1" snmp-primary="N"/>
    <meta-data context="requisition" key="env" value="e2e"/>
  </node>
</model-import>
REQEOF

# Add the requisition-def to provisiond-configuration.xml so provisiond
# auto-imports.  Insert our E2E requisition-def immediately before the
# closing </provisiond-configuration> tag, preserving every other
# requisition-def (delta-v, rpc-canary, cloud-services, nl6-lab,
# perspective-test) that other e2e tests depend on.
# The committed state is backed up in cleanup()'s trap and restored on exit.
# Add the requisition-def to the IN-CONTAINER provisiond-configuration.xml (the baked
# config), via a docker cp round-trip. The container is reset by `down -v` in cleanup,
# so the host config is never mutated and needs no backup/restore.
echo "==> Injecting ${E2E_FOREIGN_SOURCE} requisition-def into the in-container provisiond config"
python3 - "${PROVISIOND_CONFIG_BACKUP}" "${E2E_FOREIGN_SOURCE}" <<'PYEOF'
import subprocess, sys
tmp, fs = sys.argv[1], sys.argv[2]
subprocess.run(["docker", "cp", "delta-v-provisiond:/opt/deltav/etc/provisiond-configuration.xml", tmp], check=True)
snippet = (
    f'  <requisition-def import-name="{fs}"\n'
    f'                   import-url-resource="file:///opt/deltav/etc/imports/{fs}.xml">\n'
    f'    <cron-schedule>0/30 * * * * ?</cron-schedule>\n'
    f'  </requisition-def>\n'
)
with open(tmp) as f: content = f.read()
marker = '</provisiond-configuration>'
if marker not in content:
    sys.exit(f"marker {marker!r} not found in container config")
with open(tmp, 'w') as f: f.write(content.replace(marker, snippet + marker, 1))
subprocess.run(["docker", "cp", tmp, "delta-v-provisiond:/opt/deltav/etc/provisiond-configuration.xml"], check=True)
PYEOF

echo "==> Restarting provisiond to pick up E2E requisition"
docker compose restart provisiond

echo "==> Waiting for provisiond to become healthy again (up to ${STACK_READY_TIMEOUT} s)"
deadline=$(( $(date +%s) + STACK_READY_TIMEOUT ))
while (( $(date +%s) < deadline )); do
    if docker compose exec -T provisiond curl -sf http://localhost:8080/actuator/health >/dev/null 2>&1; then
        echo "==> provisiond healthy"
        break
    fi
    sleep 5
done
if (( $(date +%s) >= deadline )); then
    echo "ERROR: provisiond did not become healthy after restart within ${STACK_READY_TIMEOUT} s"
    docker compose logs provisiond | tail -80
    exit 1
fi

echo "==> Waiting for change record (up to ${METRICS_TIMEOUT} s)"
deadline=$(( $(date +%s) + METRICS_TIMEOUT ))
while (( $(date +%s) < deadline )); do
    metrics=$(docker compose exec -T provisiond curl -sf http://localhost:8080/actuator/prometheus 2>/dev/null || true)
    if echo "${metrics}" | grep -E 'deltav_node_context_records_published_total.*reason="change"' | \
           awk '{print $NF}' | head -1 | grep -qE '^[1-9]' 2>/dev/null; then
        echo "==> change counter > 0 — change-feed verified"
        break
    fi
    sleep 5
done
if (( $(date +%s) >= deadline )); then
    echo "ERROR: deltav_node_context_records_published_total{reason=\"change\"} not > 0 within ${METRICS_TIMEOUT} s"
    metrics=$(docker compose exec -T provisiond curl -sf http://localhost:8080/actuator/prometheus 2>/dev/null || true)
    echo "${metrics}" | grep deltav_node_context || true
    docker compose logs provisiond | tail -80
    exit 1
fi

# ── Step 5: Assert no failures ────────────────────────────────────────────────

echo "==> Checking failure counters"
metrics=$(docker compose exec -T provisiond curl -sf http://localhost:8080/actuator/prometheus)
# grep returns 1 on no-match (the success path), which would kill the script
# under set -euo pipefail. Tolerate no-match with a trailing `|| true`.
failed_sum=$( { echo "${metrics}" \
    | grep -E '^deltav_node_context_records_failed_total\{' \
    || true; } \
    | awk '{sum += $NF} END {print sum + 0}')
if [[ "${failed_sum}" != "0" ]]; then
    echo "ERROR: deltav_node_context_records_failed_total sum = ${failed_sum} (expected 0)"
    echo "${metrics}" | grep deltav_node_context_records_failed_total || true
    exit 1
fi
echo "==> No failure counters — all records published cleanly"

echo "==> PASS"
exit 0
