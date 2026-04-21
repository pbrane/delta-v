# Flows Visibility Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Surface the existing Minion → flow-enricher → ClickHouse flow pipeline in Grafana via a new 6-panel Flows Overview dashboard, with l8opensim's 20 simulated devices producing NetFlow v5/v9, IPFIX, and sFlow traffic at `location=l8opensim-lab`.

**Architecture:** Three new config files (Grafana ClickHouse datasource YAML, flows-overview dashboard JSON, l8opensim flow-exporters config). Five compose services have their profile memberships widened from `[full]` to `[lite, full, metrics]`. l8opensim-provisioner gains a flow-enable step running after device creation. E2E gains Step 11 asserting flows land in ClickHouse across all four protocols.

**Tech Stack:** Docker Compose, l8opensim, Grafana 11.4.0 with `grafana-clickhouse-datasource` plugin, ClickHouse 25.8, VictoriaMetrics (unchanged, for SNMP side), bash + curl for the E2E script, python3 for JSON/YAML validation.

**Branch:** `feat/flows-visibility` (already created off develop tip `952c6102f78`, with the spec committed as `f14722076b3`).

**Spec reference:** `docs/superpowers/specs/2026-04-20-flows-visibility-design.md`.

**PR target:** `pbrane/delta-v` `develop`. Title prefix `feat(flows):`. **NEVER `OpenNMS/opennms`.**

**No Java rebuild needed** — this PR only touches docker-compose, grafana provisioning YAML, dashboard JSON, shell scripts, and the E2E script. flow-enricher / clickhouse-init images stay at their current versions.

---

## Pre-flight (one-time per session)

- [ ] **Verify branch and clean working tree**

Run: `git branch --show-current && git status --short`
Expected:
```
feat/flows-visibility
```
(no dirty files; if `provisiond-overlay/etc/imports/*.xml` files show modified, run `git checkout -- opennms-container/delta-v/provisiond-overlay/etc/imports/` to discard the requisition runtime drift per `feedback_provisiond_requisition_drift`.)

- [ ] **Verify spec is committed**

Run: `git log --oneline -4`
Expected: most recent non-gitignore commit is `f14722076b3 docs(spec): flows visibility — ClickHouse datasource + dashboard + l8opensim flow exporters`.

- [ ] **Verify Docker Desktop is running and stack is torn down**

Run: `docker ps --format '{{.Names}}' | grep delta-v | head -5`
Expected: empty output. If anything's running: `cd opennms-container/delta-v && docker compose --profile lite --profile full --profile metrics down -v --remove-orphans` first.

If any pre-flight check fails, stop and resolve before starting Task 1.

---

### Task 1: Spike l8opensim flow-exporter REST API + author `flow-exporters.json`

**Files:**
- Create: `opennms-container/delta-v/l8opensim/flow-exporters.json` (the 20-device flow config)
- Create (conditional): `opennms-container/delta-v/l8opensim/enable-flows.sh` (Scenario B only)

**Empirical question:** we know l8opensim supports NetFlow v5/v9, IPFIX, and sFlow v5 per its README and web UI's "Fleet" panel. We don't yet know the exact REST API shape for enabling per-device flow export. The task probes three scenarios:

- **Scenario A** — flow config accepted inline during `POST /api/v1/devices` as additional JSON fields.
- **Scenario B** — flow config requires a separate POST per device (e.g., `POST /api/v1/devices/{id}/exporters`).
- **Scenario C (fallback)** — only global flow config is supported (one collector, one protocol for all devices).

- [ ] **Step 1: Pull the image (cached from PR #183, just confirm)**

```
docker pull ghcr.io/labmonkeys-space/l8opensim@sha256:714b83cbdda904dd748bc850cb6f6977815dbfece0ca1393ded22eb24fe61b07
```
Expected: `Status: Image is up to date`.

- [ ] **Step 2: Run a temporary l8opensim instance in no-namespace mode**

```
docker run -d --rm --name l8-flow-spike \
  --cap-add=NET_ADMIN --cap-add=SYS_ADMIN \
  --device /dev/net/tun \
  -p 19161:161/udp -p 19081:8080/tcp \
  ghcr.io/labmonkeys-space/l8opensim@sha256:714b83cbdda904dd748bc850cb6f6977815dbfece0ca1393ded22eb24fe61b07 -no-namespace
```

Wait: `sleep 5 && docker logs l8-flow-spike 2>&1 | grep -E 'Web UI|server starting' | head -2`
Expected: `Web UI ...` and `server starting` lines.

- [ ] **Step 3: Create a test device first (prerequisite for per-device flow probes)**

```
curl -sf -X POST http://localhost:19081/api/v1/devices \
  -H "Content-Type: application/json" \
  -d '{"start_ip":"10.0.0.1","device_count":1,"netmask":"24","resource_file":"cisco_crs_x.json"}' \
  | python3 -m json.tool | head -20
```
Expected: JSON response with a device ID. Record the ID for Step 4.

- [ ] **Step 4: Probe Scenario A — inline flow fields on device creation**

Create a second test device with flow-config fields:
```
curl -sf -X POST http://localhost:19081/api/v1/devices \
  -H "Content-Type: application/json" \
  -d '{"start_ip":"10.0.0.2","device_count":1,"netmask":"24","resource_file":"cisco_crs_x.json",
       "flow_exporter_type":"netflow_v9","flow_collector_addr":"127.0.0.1:4729","flow_sampling_rate":1}' \
  | python3 -m json.tool | head -20
```

Then inspect the response:
```
curl -sf http://localhost:19081/api/v1/devices | python3 -m json.tool | head -40
```

Look for fields like `flow_exporter_type`, `netflow_version`, `flow_collector`, `exporters`, or similar on the returned device entry. If the simulator accepted the inline fields, Scenario A applies.

- [ ] **Step 5: Probe Scenario B — separate flow-config endpoint**

```
# Try the common "subresource" pattern
curl -sf -X POST http://localhost:19081/api/v1/devices/10.0.0.1/exporters \
  -H "Content-Type: application/json" \
  -d '{"type":"netflow_v9","collector":"127.0.0.1:4729","sampling_rate":1}' \
  | python3 -m json.tool | head -20

# Try an alternative "flow-config" endpoint
curl -sf -X POST http://localhost:19081/api/v1/devices/10.0.0.1/flow-config \
  -H "Content-Type: application/json" \
  -d '{"type":"netflow_v9","collector":"127.0.0.1:4729"}' \
  | python3 -m json.tool | head -20

# Try a top-level exporters endpoint
curl -sf http://localhost:19081/api/v1/exporters | python3 -m json.tool | head -20
```

Check HTTP status codes and any error messages pointing to the correct endpoint.

- [ ] **Step 6: Consult web UI + any auto-discovery**

```
# Often the UI's JS code exposes the API shape in a discoverable way.
curl -sf http://localhost:19081/web/app_api.js 2>/dev/null | grep -iE 'flow|exporter|netflow|sflow|ipfix' | head -20

# Check for swagger / openapi
curl -sf http://localhost:19081/api/v1/openapi.json 2>/dev/null | python3 -m json.tool | grep -iE 'flow|exporter' | head -30
curl -sf http://localhost:19081/swagger 2>/dev/null | head -20
```

Document what API shape actually works.

- [ ] **Step 7: Tear down the spike**

```
docker rm -f l8-flow-spike
```

- [ ] **Step 8: Author `flow-exporters.json` based on scenario outcome**

Create `opennms-container/delta-v/l8opensim/flow-exporters.json`. Format depends on scenario.

**Scenario A (inline fields):** This file is a companion to `devices.json` — the provisioner merges the flow-config keys into each device's POST payload. Format as a JSON array of 20 entries, one per device, mapping `start_ip` → flow config:

```json
[
  {"start_ip":"10.0.0.1","flow_exporter_type":"netflow_v5","flow_collector_addr":"127.0.0.1:4729","flow_sampling_rate":1},
  {"start_ip":"10.0.0.2","flow_exporter_type":"netflow_v5","flow_collector_addr":"127.0.0.1:4729","flow_sampling_rate":1},
  {"start_ip":"10.0.0.3","flow_exporter_type":"netflow_v5","flow_collector_addr":"127.0.0.1:4729","flow_sampling_rate":1},
  {"start_ip":"10.0.0.4","flow_exporter_type":"netflow_v5","flow_collector_addr":"127.0.0.1:4729","flow_sampling_rate":1},
  {"start_ip":"10.0.0.5","flow_exporter_type":"netflow_v5","flow_collector_addr":"127.0.0.1:4729","flow_sampling_rate":1},
  {"start_ip":"10.0.0.6","flow_exporter_type":"netflow_v9","flow_collector_addr":"127.0.0.1:4729","flow_sampling_rate":1},
  {"start_ip":"10.0.0.7","flow_exporter_type":"netflow_v9","flow_collector_addr":"127.0.0.1:4729","flow_sampling_rate":1},
  {"start_ip":"10.0.0.8","flow_exporter_type":"netflow_v9","flow_collector_addr":"127.0.0.1:4729","flow_sampling_rate":1},
  {"start_ip":"10.0.0.9","flow_exporter_type":"netflow_v9","flow_collector_addr":"127.0.0.1:4729","flow_sampling_rate":1},
  {"start_ip":"10.0.0.10","flow_exporter_type":"netflow_v9","flow_collector_addr":"127.0.0.1:4729","flow_sampling_rate":1},
  {"start_ip":"10.0.0.11","flow_exporter_type":"ipfix","flow_collector_addr":"127.0.0.1:4729","flow_sampling_rate":1},
  {"start_ip":"10.0.0.12","flow_exporter_type":"ipfix","flow_collector_addr":"127.0.0.1:4729","flow_sampling_rate":1},
  {"start_ip":"10.0.0.13","flow_exporter_type":"ipfix","flow_collector_addr":"127.0.0.1:4729","flow_sampling_rate":1},
  {"start_ip":"10.0.0.14","flow_exporter_type":"ipfix","flow_collector_addr":"127.0.0.1:4729","flow_sampling_rate":1},
  {"start_ip":"10.0.0.15","flow_exporter_type":"ipfix","flow_collector_addr":"127.0.0.1:4729","flow_sampling_rate":1},
  {"start_ip":"10.0.0.16","flow_exporter_type":"sflow","flow_collector_addr":"127.0.0.1:4729","flow_sampling_rate":1},
  {"start_ip":"10.0.0.17","flow_exporter_type":"sflow","flow_collector_addr":"127.0.0.1:4729","flow_sampling_rate":1},
  {"start_ip":"10.0.0.18","flow_exporter_type":"sflow","flow_collector_addr":"127.0.0.1:4729","flow_sampling_rate":1},
  {"start_ip":"10.0.0.19","flow_exporter_type":"sflow","flow_collector_addr":"127.0.0.1:4729","flow_sampling_rate":1},
  {"start_ip":"10.0.0.20","flow_exporter_type":"sflow","flow_collector_addr":"127.0.0.1:4729","flow_sampling_rate":1}
]
```

Replace the field names (`flow_exporter_type`, `flow_collector_addr`, `flow_sampling_rate`) with whatever the API actually accepts per Steps 4-6 findings. Also replace the enum values (`netflow_v5`, `netflow_v9`, `ipfix`, `sflow`) with whatever the API accepts.

**Scenario B (separate endpoint):** Same JSON array structure as Scenario A, but the keys map to the per-device POST body of the discovered endpoint.

**Scenario C (global only):** Reduce to a single-entry JSON array with the global collector + protocol:
```json
[
  {"flow_exporter_type":"netflow_v9","flow_collector_addr":"127.0.0.1:4729","flow_sampling_rate":1}
]
```
Note: under Scenario C, the spec's "all 4 protocols" assertion relaxes to `>= 1`. Update Task 9's Step 11 assertion accordingly.

- [ ] **Step 9 (conditional, Scenario B only): Author `enable-flows.sh`**

Skip if Scenario A or C — in A the config is merged into the existing `post-each.sh` POST; in C the single entry is applied once.

For Scenario B, create `opennms-container/delta-v/l8opensim/enable-flows.sh`:

```bash
#!/bin/sh
# Enable flow exporters on l8opensim devices after device creation.
# Reads flow-exporters.json from stdin (one JSON object per line — caller
# pre-splits via the same sed pipeline that post-each.sh uses).
#
# Args:
#   $1 = base URL of l8opensim REST API (e.g. http://127.0.0.1:8080)
#
# For each entry: POST to /api/v1/devices/{start_ip}/exporters (or the
# Task-1-discovered endpoint).
set -eu
base_url="$1"
enabled=0
failed=0
while IFS= read -r entry || [ -n "$entry" ]; do
    [ -z "$entry" ] && continue
    ip=$(echo "$entry" | grep -o '"start_ip":"[^"]*"' | cut -d'"' -f4)
    if [ -z "$ip" ]; then
        echo "SKIP: no start_ip in: $entry" >&2
        continue
    fi
    if curl -sf -X POST "$base_url/api/v1/devices/$ip/exporters" \
            -H "Content-Type: application/json" \
            -d "$entry" > /dev/null; then
        enabled=$((enabled + 1))
    else
        failed=$((failed + 1))
        echo "FAILED to enable flows for $ip: $entry" >&2
    fi
done
echo "enable-flows: enabled=$enabled failed=$failed"
[ "$failed" -eq 0 ] || exit 1
```

Update the endpoint path on line `curl -sf -X POST "$base_url/..."` to match Step 5's discovery. Make executable: `chmod +x opennms-container/delta-v/l8opensim/enable-flows.sh`.

Verify syntax: `sh -n opennms-container/delta-v/l8opensim/enable-flows.sh && echo OK`. Expected: `OK`.

- [ ] **Step 10: Validate JSON syntax**

```
python3 -c 'import json; d=json.load(open("opennms-container/delta-v/l8opensim/flow-exporters.json")); print(f"OK entries={len(d)}")'
```
Expected: `OK entries=20` (Scenario A or B) or `OK entries=1` (Scenario C).

- [ ] **Step 11: Commit**

```
git add opennms-container/delta-v/l8opensim/
git commit -m "feat(flows): add l8opensim flow-exporters.json + (if B) enable-flows.sh

flow-exporters.json holds the 20-device flow export spec: 5 devices each
across NetFlow v5, NetFlow v9, IPFIX, and sFlow v5 targeting
127.0.0.1:4729 (shared netns with minion-lab).

API shape verified empirically: <Scenario A, B, or C finding here + note
any fallback that applies>."
```

---

### Task 2: Widen compose profile membership for flow pipeline services

**Files:**
- Modify: `opennms-container/delta-v/docker-compose.yml`

Five services move from `profiles: [full]` to `profiles: [lite, full, metrics]`. This is five small edits.

- [ ] **Step 1: Identify the five services and current profile lines**

```
grep -n 'profiles: \[full\]' opennms-container/delta-v/docker-compose.yml
```

Expected: exactly these five matches (in this order by line number):
- line ~598 under `clickhouse:`
- line ~628 under `clickhouse-init:`
- line ~650 under `flow-enricher:`
- line ~676 under `flow-default-testnode-1:`
- line ~691 under `flow-sflow-testnode-1:`

If any other service shows `[full]`, do NOT touch it — those are intentional gates (e.g., additional full-only daemons).

- [ ] **Step 2: Edit each profile line via Edit tool, one at a time**

Use unique leading context (service name + one line of config above `profiles:`) to ensure each Edit targets only the intended match. For each of the 5 services, change:

```yaml
    profiles: [full]
```

to:

```yaml
    profiles: [lite, full, metrics]
```

- [ ] **Step 3: Validate YAML + confirm all 5 services are in the `lite` profile now**

```
cd opennms-container/delta-v
docker compose --profile lite --profile metrics config --services 2>&1 | sort > /tmp/lite-metrics-services.txt
cd ../..
grep -E '^(clickhouse|clickhouse-init|flow-enricher|flow-default-testnode-1|flow-sflow-testnode-1)$' /tmp/lite-metrics-services.txt
```
Expected: all 5 services listed. If any are missing, the Edit targeted the wrong match — revisit.

- [ ] **Step 4: Re-run compose config to confirm no YAML errors**

```
cd opennms-container/delta-v && docker compose --profile lite --profile metrics config > /dev/null && echo OK && cd ../..
```
Expected: `OK`.

- [ ] **Step 5: Commit**

```
git add opennms-container/delta-v/docker-compose.yml
git commit -m "feat(flows): widen profile membership for flow pipeline services

clickhouse, clickhouse-init, flow-enricher, flow-default-testnode-1,
flow-sflow-testnode-1 move from [full] → [lite, full, metrics]. The
default docker compose --profile lite --profile metrics demo path now
includes the full flow pipeline, matching the pattern set by l8opensim
and Grafana. Operators with tight resources can trim back to [full]
locally."
```

---

### Task 3: Add ClickHouse datasource provisioning YAML

**Files:**
- Create: `opennms-container/delta-v/grafana/provisioning/datasources/clickhouse.yml`

A sibling file to the existing `victoriametrics.yml`. Configures the ClickHouse datasource for Grafana's plugin-based auto-provisioning.

- [ ] **Step 1: Read the existing victoriametrics.yml to confirm style conventions**

```
cat opennms-container/delta-v/grafana/provisioning/datasources/victoriametrics.yml
```

Note the exact indentation and field placement (apiVersion, datasources list, etc).

- [ ] **Step 2: Create the new clickhouse.yml file**

Create `opennms-container/delta-v/grafana/provisioning/datasources/clickhouse.yml`:

```yaml
apiVersion: 1
datasources:
  - name: ClickHouse
    uid: clickhouse
    type: grafana-clickhouse-datasource
    access: proxy
    url: http://clickhouse:8123
    isDefault: false
    editable: false
    jsonData:
      defaultDatabase: deltav
      protocol: http
      username: deltav
    secureJsonData:
      password: deltav
```

- [ ] **Step 3: Validate YAML**

```
python3 -c 'import yaml; d=yaml.safe_load(open("opennms-container/delta-v/grafana/provisioning/datasources/clickhouse.yml")); print(f"OK uid={d[\"datasources\"][0][\"uid\"]} url={d[\"datasources\"][0][\"url\"]}")'
```
Expected: `OK uid=clickhouse url=http://clickhouse:8123`.

- [ ] **Step 4: Commit**

```
git add opennms-container/delta-v/grafana/provisioning/datasources/clickhouse.yml
git commit -m "feat(flows): provision ClickHouse datasource for Grafana

Mirrors the style of victoriametrics.yml. UID 'clickhouse', targeting
http://clickhouse:8123 via the grafana-clickhouse-datasource plugin
(installed by GF_INSTALL_PLUGINS in the next task). Username+password
placement may need tweaking based on plugin version — Task 7
verification gate validates via /api/datasources/uid/clickhouse/health."
```

---

### Task 4: Add `GF_INSTALL_PLUGINS` to Grafana service

**Files:**
- Modify: `opennms-container/delta-v/docker-compose.yml`

Add a single env var to the existing grafana service so the ClickHouse datasource plugin auto-installs on first boot.

- [ ] **Step 1: Locate the grafana environment block**

```
grep -n -A 5 '^  grafana:' opennms-container/delta-v/docker-compose.yml | head -30
```

Find the `environment:` key block (around line 760-780) and the healthcheck block that follows it.

- [ ] **Step 2: Add `GF_INSTALL_PLUGINS` via Edit tool**

Find the existing `environment:` block for grafana. A typical line in it is `GF_AUTH_ANONYMOUS_ENABLED: "true"` or similar. Use unique context: grab the last existing `GF_*` line and insert a new line after it.

For example, if the last env line is:
```yaml
      GF_USERS_DEFAULT_THEME: dark
```

Change to:
```yaml
      GF_USERS_DEFAULT_THEME: dark
      GF_INSTALL_PLUGINS: grafana-clickhouse-datasource
```

(Adjust the preceding line to whatever is actually last in the existing environment block.)

- [ ] **Step 3: Validate compose YAML**

```
cd opennms-container/delta-v && docker compose --profile lite --profile metrics config | grep -A 1 GF_INSTALL_PLUGINS && cd ../..
```
Expected: one match showing the value `grafana-clickhouse-datasource`.

- [ ] **Step 4: Commit**

```
git add opennms-container/delta-v/docker-compose.yml
git commit -m "feat(flows): install grafana-clickhouse-datasource plugin on boot

GF_INSTALL_PLUGINS env var triggers Grafana's boot-time plugin install.
First boot downloads the plugin (~10-15s); subsequent boots cache in the
grafana volume. Healthcheck start_period may need to bump from 30s to
60s if Task 7 measurement shows a race — defer to that task."
```

---

### Task 5: Build `flows-overview.json` dashboard

**Files:**
- Create: `opennms-container/delta-v/grafana/dashboards/flows-overview.json`

The 6-panel dashboard per spec Section 3. A sibling to `snmp-overview.json` in the same provisioning directory, auto-loaded on Grafana startup.

- [ ] **Step 1: Read snmp-overview.json to confirm top-level structure conventions**

```
python3 -c 'import json; d=json.load(open("opennms-container/delta-v/grafana/dashboards/snmp-overview.json")); print([k for k in d.keys()])'
```
Expected top-level keys: something like `['uid', 'title', 'panels', 'templating', 'time', 'schemaVersion', ...]`.

Also inspect indentation style: `head -30 opennms-container/delta-v/grafana/dashboards/snmp-overview.json`.

- [ ] **Step 2: Create `flows-overview.json` with the complete 6-panel structure**

Create `opennms-container/delta-v/grafana/dashboards/flows-overview.json`. Use the dashboard below — it's complete and ships as-is:

```json
{
  "uid": "flows-overview",
  "title": "Flows Overview",
  "schemaVersion": 39,
  "version": 1,
  "refresh": "30s",
  "time": { "from": "now-15m", "to": "now" },
  "tags": ["delta-v", "flows"],
  "templating": {
    "list": [
      {
        "name": "monitoring_location",
        "label": "Monitoring location",
        "type": "query",
        "datasource": { "type": "grafana-clickhouse-datasource", "uid": "clickhouse" },
        "query": "SELECT DISTINCT location FROM deltav.flows_raw WHERE $__timeFilter(timestamp)",
        "refresh": 1,
        "multi": true,
        "includeAll": true,
        "allValue": "%",
        "current": { "selected": true, "text": ["All"], "value": ["$__all"] }
      },
      {
        "name": "exporter_instance",
        "label": "Exporter",
        "type": "query",
        "datasource": { "type": "grafana-clickhouse-datasource", "uid": "clickhouse" },
        "query": "SELECT DISTINCT concat(exporter_node_foreign_source, '/', exporter_node_foreign_id) FROM deltav.flows_raw WHERE $__timeFilter(timestamp) AND location IN (${monitoring_location:sqlstring})",
        "refresh": 1,
        "multi": true,
        "includeAll": true,
        "allValue": "%",
        "current": { "selected": true, "text": ["All"], "value": ["$__all"] }
      },
      {
        "name": "application",
        "label": "Application",
        "type": "query",
        "datasource": { "type": "grafana-clickhouse-datasource", "uid": "clickhouse" },
        "query": "SELECT DISTINCT application FROM deltav.flows_raw WHERE $__timeFilter(timestamp) AND location IN (${monitoring_location:sqlstring})",
        "refresh": 1,
        "multi": true,
        "includeAll": true,
        "allValue": "%",
        "current": { "selected": true, "text": ["All"], "value": ["$__all"] }
      }
    ]
  },
  "panels": [
    {
      "id": 1,
      "type": "timeseries",
      "title": "Bandwidth over time, stacked by location",
      "gridPos": { "h": 8, "w": 24, "x": 0, "y": 0 },
      "datasource": { "type": "grafana-clickhouse-datasource", "uid": "clickhouse" },
      "fieldConfig": {
        "defaults": { "unit": "bps", "custom": { "drawStyle": "line", "fillOpacity": 40, "stacking": { "mode": "normal" } } },
        "overrides": []
      },
      "targets": [
        {
          "refId": "A",
          "rawSql": "SELECT $__timeInterval(timestamp) AS time, location, sum(num_bytes) * 8 / ($__interval_ms / 1000) AS bps FROM deltav.flows_raw WHERE $__timeFilter(timestamp) AND location IN (${monitoring_location:sqlstring}) AND application IN (${application:sqlstring}) GROUP BY time, location ORDER BY time",
          "format": 1
        }
      ]
    },
    {
      "id": 2,
      "type": "bargauge",
      "title": "Top applications by bandwidth",
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 8 },
      "datasource": { "type": "grafana-clickhouse-datasource", "uid": "clickhouse" },
      "fieldConfig": { "defaults": { "unit": "bytes" }, "overrides": [] },
      "options": { "orientation": "horizontal", "displayMode": "gradient" },
      "targets": [
        {
          "refId": "A",
          "rawSql": "SELECT application, sum(num_bytes) AS bytes FROM deltav.flows_raw WHERE $__timeFilter(timestamp) AND location IN (${monitoring_location:sqlstring}) GROUP BY application ORDER BY bytes DESC LIMIT 10",
          "format": 0
        }
      ]
    },
    {
      "id": 3,
      "type": "table",
      "title": "Top conversations",
      "gridPos": { "h": 8, "w": 12, "x": 12, "y": 8 },
      "datasource": { "type": "grafana-clickhouse-datasource", "uid": "clickhouse" },
      "targets": [
        {
          "refId": "A",
          "rawSql": "SELECT IPv6NumToString(src_address) AS \"Source\", IPv6NumToString(dst_address) AS \"Destination\", application AS \"Application\", sum(num_bytes) AS \"Bytes\", sum(num_packets) AS \"Packets\", count() AS \"Flow records\" FROM deltav.flows_raw WHERE $__timeFilter(timestamp) AND location IN (${monitoring_location:sqlstring}) AND application IN (${application:sqlstring}) GROUP BY src_address, dst_address, application ORDER BY \"Bytes\" DESC LIMIT 20",
          "format": 0
        }
      ]
    },
    {
      "id": 4,
      "type": "timeseries",
      "title": "Per-exporter flow rate",
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 16 },
      "datasource": { "type": "grafana-clickhouse-datasource", "uid": "clickhouse" },
      "fieldConfig": { "defaults": { "unit": "flows/sec" }, "overrides": [] },
      "targets": [
        {
          "refId": "A",
          "rawSql": "SELECT $__timeInterval(timestamp) AS time, concat(exporter_node_foreign_source, '/', exporter_node_foreign_id) AS exporter, count() / ($__interval_ms / 1000) AS flows_per_sec FROM deltav.flows_raw WHERE $__timeFilter(timestamp) AND location IN (${monitoring_location:sqlstring}) AND concat(exporter_node_foreign_source, '/', exporter_node_foreign_id) IN (${exporter_instance:sqlstring}) GROUP BY time, exporter ORDER BY time",
          "format": 1
        }
      ]
    },
    {
      "id": 5,
      "type": "piechart",
      "title": "Protocol & DSCP mix",
      "gridPos": { "h": 8, "w": 12, "x": 12, "y": 16 },
      "datasource": { "type": "grafana-clickhouse-datasource", "uid": "clickhouse" },
      "options": { "legend": { "displayMode": "table", "placement": "right" }, "pieType": "donut" },
      "targets": [
        {
          "refId": "A",
          "rawSql": "SELECT netflow_version AS label, count() AS records FROM deltav.flows_raw WHERE $__timeFilter(timestamp) AND location IN (${monitoring_location:sqlstring}) GROUP BY netflow_version",
          "format": 0
        },
        {
          "refId": "B",
          "rawSql": "SELECT concat('DSCP=', toString(dscp)) AS label, count() AS records FROM deltav.flows_raw WHERE $__timeFilter(timestamp) AND location IN (${monitoring_location:sqlstring}) AND dscp IS NOT NULL GROUP BY dscp",
          "format": 0
        }
      ]
    },
    {
      "id": 6,
      "type": "table",
      "title": "Flow sources inventory",
      "gridPos": { "h": 8, "w": 24, "x": 0, "y": 24 },
      "datasource": { "type": "grafana-clickhouse-datasource", "uid": "clickhouse" },
      "transformations": [
        {
          "id": "organize",
          "options": {
            "renameByName": {
              "exporter_node_foreign_source": "Foreign source",
              "exporter_node_foreign_id": "Foreign ID",
              "location": "Monitoring location",
              "host": "Exporter hostname",
              "last_seen": "Last seen",
              "flow_count": "Flows in window"
            }
          }
        }
      ],
      "targets": [
        {
          "refId": "A",
          "rawSql": "SELECT exporter_node_foreign_source, exporter_node_foreign_id, location, host, max(timestamp) AS last_seen, count() AS flow_count FROM deltav.flows_raw WHERE $__timeFilter(timestamp) AND location IN (${monitoring_location:sqlstring}) GROUP BY exporter_node_foreign_source, exporter_node_foreign_id, location, host ORDER BY flow_count DESC",
          "format": 0
        }
      ]
    }
  ]
}
```

- [ ] **Step 3: Validate JSON + panel/templating counts**

```
python3 << 'EOF'
import json
d = json.load(open("opennms-container/delta-v/grafana/dashboards/flows-overview.json"))
assert d["uid"] == "flows-overview", f"uid wrong: {d['uid']}"
assert len(d["panels"]) == 6, f"expected 6 panels, got {len(d['panels'])}"
assert len(d["templating"]["list"]) == 3, f"expected 3 templates, got {len(d['templating']['list'])}"
template_names = [v["name"] for v in d["templating"]["list"]]
assert template_names == ["monitoring_location", "exporter_instance", "application"], f"templates in wrong order: {template_names}"
# Every panel must reference the clickhouse datasource
for i, p in enumerate(d["panels"]):
    ds = p.get("datasource", {})
    assert ds.get("uid") == "clickhouse", f"panel {i+1} uses wrong datasource: {ds}"
# Panels 1-5 filter by monitoring_location in their SQL
for i in range(5):
    p = d["panels"][i]
    expr = p["targets"][0]["rawSql"]
    assert "${monitoring_location:sqlstring}" in expr, f"panel {i+1} missing monitoring_location filter"
# Panel 6 also uses monitoring_location in its SQL (no inventory without it)
panel6 = d["panels"][5]
assert "${monitoring_location:sqlstring}" in panel6["targets"][0]["rawSql"], "panel 6 missing monitoring_location filter"
# Panel 5 must have 2 targets (Protocol + DSCP)
assert len(d["panels"][4]["targets"]) == 2, "panel 5 should have 2 targets (netflow_version + dscp)"
print(f"OK uid=flows-overview panels=6 templates=3 all filter by monitoring_location panel5 has 2 targets")
EOF
```
Expected: `OK uid=flows-overview panels=6 templates=3 all filter by monitoring_location panel5 has 2 targets`.

- [ ] **Step 4: Commit**

```
git add opennms-container/delta-v/grafana/dashboards/flows-overview.json
git commit -m "feat(flows): add flows-overview dashboard JSON (6 panels + 3 templates)

Hero bandwidth-by-location timeseries + top apps + top conversations +
per-exporter flow rate + protocol/DSCP mix + flow sources inventory.
All queries target deltav.flows_raw directly; templates chain via
\${monitoring_location:sqlstring} for filter-dependent variables.
Panel 5 uses two queries (protocol variant + DSCP class) rendered on
one donut chart; plan allows splitting to two half-width panels if
cluttered (Task 7 verification judges)."
```

---

### Task 6: Wire `l8opensim-provisioner` to enable flow exporters

**Files:**
- Modify: `opennms-container/delta-v/docker-compose.yml`

The `l8opensim-provisioner` service currently POSTs `devices.json` after l8opensim is healthy. Extend its command to ALSO apply flow-exporter config after devices are created.

- [ ] **Step 1: Locate the l8opensim-provisioner command block**

```
grep -n -A 30 '^  l8opensim-provisioner:' opennms-container/delta-v/docker-compose.yml | head -40
```

Find the `command:` block with the embedded shell script (lines ~118-148 per Task 5 of #183's plan).

- [ ] **Step 2: Modify the command based on Task 1's scenario outcome**

**Scenario A (inline fields merged into devices.json POST):**

If Task 1 determined the flow config is merged inline, the existing `devices.json` is extended with flow fields (see Task 1 Step 8). The provisioner command stays roughly the same — `post-each.sh` handles the combined payload. Only change: the verification count may need to confirm flow config was applied. Add a diagnostic line:

Find the existing command block's verification section and add after `echo "provisioner: l8opensim now reports $$count devices"`:
```
        flow_count=$$(wget -q -O- http://127.0.0.1:8080/api/v1/devices | grep -oE '"flow_exporter_type":"[^"]+"' | wc -l)
        echo "provisioner: $$flow_count devices configured with flow exporters"
        [ "$$flow_count" -ge 20 ] || { echo "WARN expected >= 20 flow-configured devices got $$flow_count"; }
```
(`WARN` not `exit 1` for this check — the primary gate is the device count; flow config verification is diagnostic.)

**Scenario B (separate enable-flows.sh call):**

Chain `enable-flows.sh` AFTER `post-each.sh`. Find the line `cat /seed/devices.json \` and the sed pipeline that follows, then AFTER that `sh /seed/post-each.sh ...` call, add:

```
        echo "provisioner: enabling flow exporters on 20 devices"
        cat /seed/flow-exporters.json \
            | sed -e 's/^\[//' -e 's/\]$//' -e 's/},/}\n/g' \
            | sh /seed/enable-flows.sh http://127.0.0.1:8080
```

Match indentation to the surrounding lines (8 spaces for content inside the `|` block scalar — verify by looking at existing `cat /seed/devices.json` line).

Also update the count verification section to also confirm flow config:
```
        flow_count=$$(wget -q -O- http://127.0.0.1:8080/api/v1/devices/exporters 2>/dev/null | grep -o '"id"' | wc -l)
        echo "provisioner: $$flow_count flow exporters enabled"
        [ "$$flow_count" -ge 20 ] || { echo "FAIL expected >= 20 flow exporters got $$flow_count"; exit 1; }
```

(Endpoint path `/api/v1/devices/exporters` will be replaced with whatever Task 1 Step 5 discovered.)

**Scenario C (global-only config):**

Single POST at provisioner start to the global endpoint. After the devices-created verification, add:
```
        echo "provisioner: applying global flow-exporter config"
        curl -sf -X POST http://127.0.0.1:8080/api/v1/exporters \
            -H "Content-Type: application/json" \
            -d @/seed/flow-exporters.json || { echo "FAIL could not apply global flow config"; exit 1; }
        echo "provisioner: global flow config applied"
```

- [ ] **Step 3: Validate compose YAML**

```
cd opennms-container/delta-v && docker compose --profile lite --profile metrics config > /dev/null && echo OK && cd ../..
```
Expected: `OK`.

- [ ] **Step 4: Commit**

```
git add opennms-container/delta-v/docker-compose.yml
git commit -m "feat(flows): enable l8opensim flow exporters in provisioner

Extends l8opensim-provisioner command chain to apply flow-exporter
config via <Scenario A/B/C per Task 1 finding> after device creation.
The 20 devices split 5-5-5-5 across NetFlow v5/v9, IPFIX, sFlow v5
all target 127.0.0.1:4729 in the shared netns with minion-lab."
```

---

### Task 7: Verification gate — boot full pipeline, confirm plugin + datasource + l8opensim flows reach ClickHouse

**No file changes** — verification gate before adding the E2E assertion. Run the stack under `lite + metrics` and verify each pipeline link.

- [ ] **Step 1: Start the stack**

```
cd opennms-container/delta-v
docker compose --profile lite --profile metrics up -d --build 2>&1 | tail -10
cd ../..
```

- [ ] **Step 2: Wait for all services healthy, confirm new flow services included**

```
sleep 90
docker ps --format '{{.Names}} {{.Status}}' | grep -E 'delta-v-(clickhouse|flow-enricher|grafana|l8opensim)' | sort
```

Expected: all 4 `Up ... (healthy)`. If grafana shows "unhealthy" or stuck in "starting", it's likely the plugin download race — try:
```
docker compose -f opennms-container/delta-v/docker-compose.yml logs grafana 2>&1 | grep -iE 'plugin|install|error' | tail -20
```
If you see plugin download taking > 30s, note this for Step 8.

- [ ] **Step 3: Verify the grafana-clickhouse-datasource plugin is installed**

```
curl -sf -u "admin:admin" http://localhost:13000/api/plugins/grafana-clickhouse-datasource/settings 2>&1 | python3 -m json.tool | head -10
```
Expected: JSON with `"enabled": true` or similar. If 404, the plugin install failed or is still in progress — wait another 30s and retry.

- [ ] **Step 4: Verify the ClickHouse datasource is healthy via Grafana proxy**

```
curl -sf -u "admin:admin" http://localhost:13000/api/datasources/uid/clickhouse/health 2>&1 | python3 -m json.tool
```
Expected: JSON with `"status": "OK"` or equivalent. If the response shows authentication failure, the `username`/`password` placement in `clickhouse.yml` (Task 3) needs adjustment — check the plugin docs at `http://localhost:13000/plugins/grafana-clickhouse-datasource/` for the correct field layout, edit Task 3's YAML, rebuild, re-verify.

- [ ] **Step 5: Verify l8opensim-provisioner exited 0 with flow config applied**

```
docker compose -f opennms-container/delta-v/docker-compose.yml logs l8opensim-provisioner 2>&1 | tail -15
```
Expected (per Task 6 scenario):
- Scenario A: `provisioner: 20 devices configured with flow exporters`
- Scenario B: `provisioner: 20 flow exporters enabled`
- Scenario C: `provisioner: global flow config applied`

- [ ] **Step 6: Verify flows are landing in ClickHouse for l8opensim-lab location**

Wait a bit for first flow cycle:
```
sleep 30
curl -sf 'http://localhost:8123/?user=deltav&password=deltav' --data-urlencode \
     "query=SELECT count() FROM deltav.flows_raw WHERE location = 'l8opensim-lab'"
```
Expected: a number > 0 (typically 50-500 flows).

- [ ] **Step 7: Verify protocol coverage (under Scenarios A and B only)**

```
curl -sf 'http://localhost:8123/?user=deltav&password=deltav' --data-urlencode \
     "query=SELECT netflow_version, count() FROM deltav.flows_raw WHERE location = 'l8opensim-lab' GROUP BY netflow_version FORMAT TSV"
```

Expected (Scenarios A/B): four rows, one per `netflow_version` value covering NetFlow v5/v9, IPFIX, sFlow.
Expected (Scenario C): one row for the single chosen protocol.

If Scenario A/B and fewer than 4 values appear after 60s, surface to user — the 5-5-5-5 split is not reaching all four parsers.

- [ ] **Step 8: Verify the flows-overview dashboard loads**

```
curl -sf -u "admin:admin" http://localhost:13000/api/dashboards/uid/flows-overview 2>&1 | python3 -m json.tool | head -30
```
Expected: JSON containing `"title": "Flows Overview"` and `"panels":` with 6 entries.

- [ ] **Step 9: Tear down**

```
cd opennms-container/delta-v && docker compose --profile lite --profile metrics down -v --remove-orphans 2>&1 | tail -3 && cd ../..
git checkout -- opennms-container/delta-v/provisiond-overlay/etc/imports/ 2>&1 || true
git status --short
```
Expected: empty `git status` output.

(No commit. This task is verification only.)

If any step fails, stop and diagnose before proceeding to Task 8. The failure diagnoses are:
- Plugin not installed (Step 3): bump grafana healthcheck `start_period` from 30s to 60s in Task 4's docker-compose edit, or retry boot. Update Task 4.
- Datasource health fails (Step 4): fix `clickhouse.yml` auth fields (Task 3), re-verify.
- Fewer than 4 protocols (Step 7): revisit Task 1's flow-exporters.json — the API may not have split the way we expected. If fallback to Scenario C is needed, update Task 1 artifacts and Task 9's assertion.

---

### Task 8: Empirical first-flow-data latency measurement + possible POLL_GRACE adjustment

**No file changes** — empirical measurement that determines Task 9's `CLICKHOUSE_QUERY_TIMEOUT` default.

- [ ] **Step 1: Start the stack**

```
cd opennms-container/delta-v
docker compose --profile lite --profile metrics up -d --build 2>&1 | tail -10
cd ../..
```

- [ ] **Step 2: Measurement loop — time from stack start to first l8opensim-lab flow in ClickHouse AND all 4 protocols (or 1 under Scenario C)**

```
START=$(date +%s)
echo "stack started at: $(date -r $START)"
deadline=$((START + 300))   # 5-minute upper bound
first_data_t=""
all_protos_t=""
expected_protos=4
# Under Scenario C, set expected_protos=1
while (( $(date +%s) < deadline )); do
    elapsed=$(( $(date +%s) - START ))
    rows=$(curl -sf 'http://localhost:8123/?user=deltav&password=deltav' \
           --data-urlencode "query=SELECT count() FROM deltav.flows_raw WHERE location = 'l8opensim-lab'" \
           2>/dev/null || echo "0")
    protos=$(curl -sf 'http://localhost:8123/?user=deltav&password=deltav' \
           --data-urlencode "query=SELECT count(DISTINCT netflow_version) FROM deltav.flows_raw WHERE location = 'l8opensim-lab'" \
           2>/dev/null || echo "0")
    echo "[t+${elapsed}s] rows=$rows protos=$protos/$expected_protos"
    if (( rows > 0 )) && [ -z "$first_data_t" ]; then first_data_t="${elapsed}"; fi
    if (( protos == expected_protos )) && [ -z "$all_protos_t" ]; then
        all_protos_t="${elapsed}"
        echo "=== all protocols present at t+${all_protos_t}s ==="
        break
    fi
    sleep 10
done
echo "=== measurement complete: first_data=t+${first_data_t}s all_protos=t+${all_protos_t}s ==="
```

CAPTURE the `all_protos_t` value. This is the empirical basis for `CLICKHOUSE_QUERY_TIMEOUT` in Task 9.

**Expected:** 90-180s.

- [ ] **Step 3: Tear down**

```
cd opennms-container/delta-v && docker compose --profile lite --profile metrics down -v --remove-orphans 2>&1 | tail -3 && cd ../..
git checkout -- opennms-container/delta-v/provisiond-overlay/etc/imports/ 2>&1 || true
git status --short
```

- [ ] **Step 4: Translate measurement to `CLICKHOUSE_QUERY_TIMEOUT` value**

Because Step 11 runs AFTER Step 10 (which already waits POLL_GRACE_SECONDS=90s), `all_protos_t - 90` is the relevant budget. Map:

- `all_protos_t ≤ 90s`: Step 11 budget is negative — all protocols land before Step 10 completes. Set `CLICKHOUSE_QUERY_TIMEOUT=30` (safe baseline).
- `90 < all_protos_t ≤ 120s`: budget is 30s. Set `CLICKHOUSE_QUERY_TIMEOUT=60` (2× safety margin).
- `120 < all_protos_t ≤ 150s`: budget is 60s. Set `CLICKHOUSE_QUERY_TIMEOUT=90`.
- `150 < all_protos_t ≤ 180s`: budget is 90s. Set `CLICKHOUSE_QUERY_TIMEOUT=120`.
- `all_protos_t > 180s`: STOP. Surface to user. Something is wrong (flow-enricher consumer lag, Minion listener not subscribed, provisioner racing). Do NOT blindly bump `CLICKHOUSE_QUERY_TIMEOUT` beyond 120.

Record the `CLICKHOUSE_QUERY_TIMEOUT` value for Task 9.

(No commit. This task is verification + measurement only.)

---

### Task 9: Extend E2E — add Step 11 + `CLICKHOUSE_QUERY_TIMEOUT` variable

**Files:**
- Modify: `opennms-container/delta-v/test-prometheus-writer-e2e.sh`

Two edits:
1. Add `CLICKHOUSE_QUERY_TIMEOUT=<value from Task 8>` near the top of the script.
2. Insert Step 11 immediately before the final `echo "==> ALL ASSERTIONS PASSED"; exit 0`.

- [ ] **Step 1: Add `CLICKHOUSE_QUERY_TIMEOUT` variable near existing timeouts**

Find the existing line near top:
```
VM_QUERY_TIMEOUT=30
```
(around line 37 of the script).

Add immediately after it (use Edit with the two-line context):
```
VM_QUERY_TIMEOUT=30
CLICKHOUSE_QUERY_TIMEOUT=<VALUE_FROM_TASK_8>
```
Replace `<VALUE_FROM_TASK_8>` with the actual number (30, 60, 90, or 120).

- [ ] **Step 2: Insert Step 11 block**

Find this unique sequence (last 4 lines of the script):
```
echo "==> VM returned ${count} series for l8opensim-lab (Minion-lab is working)"
... (rest of Step 10's if-block ending) ...

echo "==> ALL ASSERTIONS PASSED"
exit 0
```

Use Edit with `old_string` = the Step 10 closing + blank + final echo/exit:
```bash
fi

echo "==> ALL ASSERTIONS PASSED"
exit 0
```

and `new_string` = the same block with Step 11 inserted between the `fi` and the `echo "==> ALL ASSERTIONS PASSED"`:

```bash
fi

# ── Step 11: Verify l8opensim-lab flows land in ClickHouse ────────────────────
echo "==> Step 11: Verify l8opensim-lab flows land in ClickHouse with all 4 protocols"
CLICKHOUSE_URL="http://localhost:8123/?user=deltav&password=deltav"
deadline=$((SECONDS + CLICKHOUSE_QUERY_TIMEOUT))
flows_landed=false
while (( SECONDS < deadline )); do
    rows=$(curl -sf "${CLICKHOUSE_URL}" --data-urlencode \
           "query=SELECT count() FROM deltav.flows_raw WHERE location = 'l8opensim-lab'" \
           2>/dev/null || echo "0")
    if (( rows > 0 )); then
        protos=$(curl -sf "${CLICKHOUSE_URL}" --data-urlencode \
                 "query=SELECT count(DISTINCT netflow_version) FROM deltav.flows_raw WHERE location = 'l8opensim-lab'" \
                 2>/dev/null || echo "0")
        echo "==> ClickHouse has ${rows} l8opensim-lab flow rows across ${protos} protocol(s)"
        if (( protos == 4 )); then
            echo "==> All 4 protocols (NetFlow v5/v9, IPFIX, sFlow) present"
            flows_landed=true
            break
        fi
    fi
    sleep 3
done
if [[ "$flows_landed" != "true" ]]; then
    echo "FAIL: l8opensim-lab flows did not land with full protocol coverage within ${CLICKHOUSE_QUERY_TIMEOUT}s"
    echo "Last rows: ${rows:-0}; last protocols: ${protos:-0}"
    docker compose logs flow-enricher | tail -30
    docker compose logs minion-lab | tail -20
    exit 1
fi

echo "==> ALL ASSERTIONS PASSED"
exit 0
```

**If Task 1 landed in Scenario C (single protocol):** change the `if (( protos == 4 ))` line to `if (( protos >= 1 ))`. Also update the surrounding comments and the `echo "==> All 4 protocols ..."` message to reflect the single-protocol reality.

- [ ] **Step 3: Verify script syntax**

```
bash -n opennms-container/delta-v/test-prometheus-writer-e2e.sh && echo OK
```
Expected: `OK`.

- [ ] **Step 4: Commit**

```
git add opennms-container/delta-v/test-prometheus-writer-e2e.sh
git commit -m "test(flows): add Step 11 asserting l8opensim-lab flows in ClickHouse

Asserts SELECT count() FROM deltav.flows_raw WHERE location='l8opensim-lab'
returns rows AND (under Scenarios A/B) all 4 protocols reach ClickHouse.
Catches: flow-enricher consumer lag, parser silent-drop bugs (like the
sflow bug history via project_flow_enricher_sflow_silent_drop), Minion-
lab flow-listener subscription failures, ClickHouse MV materialization
lag. CLICKHOUSE_QUERY_TIMEOUT=<VALUE> per Task 8 empirical measurement."
```

Replace `<VALUE>` in the commit message body with the actual CLICKHOUSE_QUERY_TIMEOUT number.

---

### Task 10: Full E2E verification gate

**No file changes** — runs the extended `test-prometheus-writer-e2e.sh` end-to-end.

- [ ] **Step 1: Run the full E2E**

```
bash opennms-container/delta-v/test-prometheus-writer-e2e.sh 2>&1 | tee /tmp/flows-e2e.log | grep -E "==> Step|FAIL|ALL ASSERTIONS|ClickHouse has|l8opensim-lab|records_consumed" | tail -40
```

Expected: all 11 steps pass. Final lines should include:
```
==> Dashboard OK
==> VM returned <N> series for l8opensim-lab (Minion-lab is working)
==> ClickHouse has <N> l8opensim-lab flow rows across 4 protocol(s)
==> All 4 protocols (NetFlow v5/v9, IPFIX, sFlow) present
==> ALL ASSERTIONS PASSED
```

Full run time: 4-6 minutes.

- [ ] **Step 2: If anything fails, debug from /tmp/flows-e2e.log**

Common failure modes:
- **Step 7 timeout (Grafana not ready)**: plugin-install race. Bump grafana healthcheck `start_period` from 30s to 60s in docker-compose.yml. Return to Task 4, re-run Task 7 gate.
- **Step 11 `count = 0`**: flows not reaching ClickHouse. Check `docker compose logs flow-enricher 2>&1 | tail -50` for consumer errors, `docker compose logs l8opensim-provisioner 2>&1 | tail -20` for flow-config POST failures, `docker exec delta-v-minion-lab netstat -lun | grep 4729` for the listener.
- **Step 11 protocols < 4**: one or more parsers silent-dropping. Check `docker compose logs flow-enricher 2>&1 | grep -iE 'parser|error|drop' | tail -30`.

- [ ] **Step 3: Discard requisition drift and teardown**

The E2E script auto-tears-down on exit. Verify:
```
docker ps --format '{{.Names}}' | grep delta-v | head -5
git checkout -- opennms-container/delta-v/provisiond-overlay/etc/imports/ 2>&1 || true
git status --short
```
Expected: both outputs empty.

(No commit. This is the final gate before push.)

---

### Task 11: Push branch + open PR

- [ ] **Step 1: Push branch**

```
git push -u origin feat/flows-visibility 2>&1 | tail -5
```
Expected: `* [new branch] feat/flows-visibility -> feat/flows-visibility`.

- [ ] **Step 2: Open the PR — `--repo pbrane/delta-v` is non-negotiable**

```
gh pr create --repo pbrane/delta-v --base develop --head feat/flows-visibility \
  --title "feat(flows): add Flows Overview dashboard + ClickHouse datasource + l8opensim flow exporters" \
  --body "$(cat <<'EOF'
## Summary

Surfaces the existing Minion → flow-enricher → ClickHouse flow pipeline in Grafana via a new 6-panel Flows Overview dashboard, with l8opensim's 20 simulated devices now producing telemetry at `location=l8opensim-lab`.

### What ships

- **Grafana ClickHouse datasource** — new `grafana/provisioning/datasources/clickhouse.yml` using the `grafana-clickhouse-datasource` plugin (auto-installed via `GF_INSTALL_PLUGINS`).
- **`flows-overview.json` dashboard** — 6 panels (bandwidth-by-location hero timeseries, top apps, top conversations, per-exporter flow rate, protocol+DSCP mix, flow sources inventory) with 3 template variables (`monitoring_location`, `exporter_instance`, `application`).
- **l8opensim flow exporters** — 20 devices split 5-5-5-5 across NetFlow v5/v9, IPFIX, sFlow v5, emitting to `127.0.0.1:4729` in the shared netns with `minion-lab`. (If Task 1 spike landed in Scenario C, fallback to single-protocol NetFlow v9 across all 20 devices — see PR description.)
- **Profile widening** — `clickhouse`, `clickhouse-init`, `flow-enricher`, `flow-default-testnode-1`, `flow-sflow-testnode-1` move from `[full]` → `[lite, full, metrics]`. Demo is single-command: `docker compose --profile lite --profile metrics up -d`.
- **E2E Step 11** — asserts flows land in `deltav.flows_raw` with `location='l8opensim-lab'` AND all 4 protocols present within `CLICKHOUSE_QUERY_TIMEOUT` (empirically set via Task 8).

### Backward compatibility

| Change | Impact |
|---|---|
| Profile widening for 5 flow-pipeline services | **Behavior change at upgrade** for `--profile lite` users. They now get ClickHouse + flow-enricher + 2 test-node containers they didn't have before. ~500 MiB RAM increase. Operators with tight resources can locally trim profiles back to `[full]`. |
| New Grafana datasource + dashboard | Additive. snmp-overview dashboard unaffected. |
| New `GF_INSTALL_PLUGINS` env var | First-boot adds ~10-15s plugin download; subsequent boots cache. |
| E2E Step 11 added | Stricter overall gate. Steps 1-10 still pass even if Step 11 fails. |

### Plan-Task-1 finding

l8opensim flow-exporter REST API shape: **<Scenario A / B / C from Task 1>**. Details in the spec; see \`docs/superpowers/specs/2026-04-20-flows-visibility-design.md\`.

Spec: \`docs/superpowers/specs/2026-04-20-flows-visibility-design.md\`. Plan: \`docs/superpowers/plans/2026-04-20-flows-visibility-plan.md\`.

## Test plan

- [x] Task 7 gate — compose boots with plugin installed, datasource healthy, l8opensim flows reaching ClickHouse.
- [x] Task 8 gate — empirical first-flow-data latency measurement.
- [x] Task 10 gate — full \`bash opennms-container/delta-v/test-prometheus-writer-e2e.sh\` passes all 11 steps.
- [ ] Manual visual verification (post-merge):
  - \`http://localhost:13000/d/flows-overview\` renders 6 panels.
  - Monitoring location dropdown lists \`Default\` AND \`l8opensim-lab\`.
  - Filter to \`l8opensim-lab\`: Panels 1-5 populate. Panel 6 shows 20 exporter rows.
  - Filter to \`Default\`: Panels 1-5 show test-node producer flows. Panel 6 shows 2 exporter rows.
  - Protocol/DSCP panel shows 4 slices under "All" (Scenarios A/B only).
  - \`snmp-overview\` dashboard still works end-to-end (regression check).

## Out of scope (queued follow-ups)

- **PR B** — vmagent scrape of \`/actuator/prometheus\` on flow-enricher + flow-processing self-monitoring dashboard.
- Forensic drilldown dashboard (pick-a-window + flow records table).
- Retiring \`flow-default-testnode-1\` / \`flow-sflow-testnode-1\` — possible once l8opensim can span multiple locations.
- Adding \`location\` to ClickHouse MVs — future schema migration for long-range queries.
- Upstream contribution to labmonkeys-space/l8opensim for DSCP traffic variation (if monochrome).
- \`project_flow_enricher_classpath_batch_cleanup\` — awaits next horizon release.

EOF
)" 2>&1 | tail -3
```

Expected: GitHub CLI prints the PR URL.

- [ ] **Step 3: Print the PR URL**

The `gh pr create` command prints the URL on success. Echo it back so the user can open it.

---

## Self-Review Checklist (run after writing the plan; not a separate task)

The following list pins what I checked when writing this plan. Re-verify if the plan is changed.

**1. Spec coverage**

| Spec section | Implementing task |
|---|---|
| Grafana ClickHouse datasource | Task 3 |
| `GF_INSTALL_PLUGINS` on grafana | Task 4 |
| New `flows-overview.json` dashboard | Task 5 |
| Profile widening of 5 services | Task 2 |
| l8opensim flow exporter config (20 devices, 5-5-5-5 split) | Task 1 + Task 6 |
| E2E Step 11 (rows + protocol coverage) | Task 9 |
| Scenario C fallback handling | Tasks 1, 6, 9 (explicit branches) |
| Plugin install race mitigation | Task 7 gate, references Task 4 |
| Task 1 empirical spike for API shape | Task 1 |
| First-flow-data empirical measurement | Task 8 |
| Full E2E green | Task 10 |
| PR opened `--repo pbrane/delta-v` | Task 11 |

All scope items have a task. Spec out-of-scope items (PR B, forensic dashboard, MV `location` addition, sink topic rename, classpath cleanup, DSCP traffic contribution, test-node retirement) are absent from the plan.

**2. Placeholder scan:** the only placeholder-style tokens in the plan are intentional:
- `<VALUE_FROM_TASK_8>` in Task 9 Step 1 — deliberate fill-in-from-upstream-task.
- `<VALUE>` in Task 9 Step 4 commit message — same.
- `<Scenario A/B/C from Task 1>` in Task 11 PR body — filled at PR-open time.
- Field names in Task 1's flow-exporters.json (`flow_exporter_type`, etc.) — marked as "Replace the field names ... with whatever the API actually accepts." Intentional empirical unknown.

No unintentional placeholders.

**3. Type/name consistency:**
- Datasource UID: `clickhouse` consistent across Tasks 3, 5, 7.
- Dashboard UID: `flows-overview` consistent across Tasks 5, 7, 10.
- Plugin name: `grafana-clickhouse-datasource` consistent across Tasks 3, 4, 7.
- Profile list `[lite, full, metrics]` consistent across Task 2 (5 instances) and Task 7 (invocation).
- Shared netns target `127.0.0.1:4729` consistent across Task 1 + Task 6.
- `CLICKHOUSE_QUERY_TIMEOUT` variable name consistent across Tasks 8, 9.
- `flows_raw` table + `location` column usage consistent across Tasks 5, 7, 8, 9.
- Step 11 assertion matches spec's Section 5 exactly.

All names match across tasks.
