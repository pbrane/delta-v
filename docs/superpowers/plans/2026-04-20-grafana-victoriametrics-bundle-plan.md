# Grafana + VictoriaMetrics Bundle Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a Grafana service to the delta-v docker-compose stack with a pre-provisioned VictoriaMetrics datasource and one starter SNMP overview dashboard, so an operator running `docker compose --profile lite --profile metrics up` can browse SNMP graphs at `http://localhost:13000/d/snmp-overview` within ~90 seconds.

**Architecture:** Configuration-only change (no Java touched). Grafana 11.4.0 container, filesystem provisioning via bind-mounted `grafana/provisioning/` and `grafana/dashboards/` directories. Profile rename: introduce `metrics` as the canonical name, retain `metrics-e2e` as alias on both the `victoriametrics` and the new `grafana` services. Existing E2E test extended with 3 new steps (Grafana health, datasource health, dashboard load).

**Tech Stack:** Docker Compose, Grafana 11.4.0 OSS, VictoriaMetrics (already pinned at v1.106.1), bash (for the E2E script extension).

**Branch:** `feat/grafana-victoriametrics-bundle` (already created off `develop` at `12a5479e3b6`, with the spec committed as `3fe6ca94478`).

**Spec reference:** `docs/superpowers/specs/2026-04-20-grafana-victoriametrics-bundle-design.md`.

**PR target:** `pbrane/delta-v` `develop`. Title prefix `feat(grafana):`. **NEVER `OpenNMS/opennms`.**

---

## Pre-flight (one-time per session)

- [ ] **Verify branch and clean working tree**

Run: `git branch --show-current && git status --short`
Expected:
```
feat/grafana-victoriametrics-bundle
```
(no dirty files)

- [ ] **Verify spec is committed**

Run: `git log --oneline -3`
Expected: most recent commit is `3fe6ca94478 docs(spec): grafana + victoriametrics dashboard bundle ...`

If either check fails, stop and resolve before starting Task 1.

---

### Task 1: Create the grafana/ directory tree

**Files (NEW directories — created via `mkdir`):**
- `opennms-container/delta-v/grafana/provisioning/datasources/`
- `opennms-container/delta-v/grafana/provisioning/dashboards/`
- `opennms-container/delta-v/grafana/dashboards/`

These directories will hold the provisioning manifests and dashboard JSON. Directories alone are not tracked by git — files added in subsequent tasks will register the structure.

- [ ] **Step 1: Create the three directories**

Run from repo root `/Users/david/development/src/opennms/delta-v`:
```
mkdir -p opennms-container/delta-v/grafana/provisioning/datasources
mkdir -p opennms-container/delta-v/grafana/provisioning/dashboards
mkdir -p opennms-container/delta-v/grafana/dashboards
```

- [ ] **Step 2: Verify the structure**

Run: `find opennms-container/delta-v/grafana -type d | sort`
Expected:
```
opennms-container/delta-v/grafana
opennms-container/delta-v/grafana/dashboards
opennms-container/delta-v/grafana/provisioning
opennms-container/delta-v/grafana/provisioning/dashboards
opennms-container/delta-v/grafana/provisioning/datasources
```

(No commit. Directories without files are not tracked. Task 3 will add the first files.)

---

### Task 2: Add Grafana service to docker-compose.yml + expand related profiles

**Files:**
- Modify: `opennms-container/delta-v/docker-compose.yml`

Three precise edits:
1. Expand `victoriametrics:` profile from `[metrics-e2e]` → `[metrics, metrics-e2e]`.
2. Expand `prometheus-writer:` profile from `[lite, full]` → `[lite, full, metrics]`.
3. Insert a new `grafana:` service block after the `victoriametrics:` block.
4. Add `grafanadata:` named volume in the `volumes:` section.

- [ ] **Step 1: Update `victoriametrics:` profile (line ~621)**

Locate this line:
```yaml
    profiles: [metrics-e2e]
```
inside the `victoriametrics:` block.

Replace with:
```yaml
    profiles: [metrics, metrics-e2e]
```

- [ ] **Step 2: Update `prometheus-writer:` profile (line ~601)**

Locate this line inside the `prometheus-writer:` block:
```yaml
    profiles: [lite, full]
```

Replace with:
```yaml
    profiles: [lite, full, metrics]
```

- [ ] **Step 3: Insert the new `grafana:` service**

After the closing of the `victoriametrics:` block (the line `      - "18428:8428"` on line ~636) and BEFORE the `volumes:` section header (line ~638), insert this new service block. Note: there must be exactly two spaces of indent on each line for the service definition (matching all other top-level services).

```yaml

  grafana:
    image: grafana/grafana:11.4.0
    profiles: [metrics, metrics-e2e]
    container_name: delta-v-grafana
    hostname: grafana
    depends_on:
      victoriametrics:
        condition: service_healthy
    environment:
      # Anonymous read-only access for demo. Anyone hitting localhost:13000 lands
      # in the Viewer role without login. Disable by setting GF_AUTH_ANONYMOUS_ENABLED=false.
      GF_AUTH_ANONYMOUS_ENABLED: "true"
      GF_AUTH_ANONYMOUS_ORG_ROLE: "Viewer"
      # Admin password override. Default 'admin' is fine for the demo stack.
      GF_SECURITY_ADMIN_PASSWORD: ${GF_ADMIN_PASSWORD:-admin}
      # Disable telemetry phone-home and update checks — this is a demo container.
      GF_ANALYTICS_REPORTING_ENABLED: "false"
      GF_ANALYTICS_CHECK_FOR_UPDATES: "false"
    volumes:
      - ./grafana/provisioning:/etc/grafana/provisioning:ro
      - ./grafana/dashboards:/var/lib/grafana/dashboards:ro
      - grafanadata:/var/lib/grafana
    healthcheck:
      test: ["CMD-SHELL", "wget -q -O- http://localhost:3000/api/health | grep -q '\"database\":\"ok\"'"]
      interval: 5s
      timeout: 3s
      retries: 12
      start_period: 10s
    ports:
      - "13000:3000"
```

- [ ] **Step 4: Add `grafanadata:` named volume**

Locate the `volumes:` section near the bottom of the file (line ~638). Add `grafanadata:` after the existing `clickhouse-data:` entry. The final volumes block should look like:

```yaml
volumes:
  pgdata:
  kafkadata:
  collectd-data:
  clickhouse-data:
  grafanadata:
```

- [ ] **Step 5: Validate the YAML**

Run: `docker compose -f opennms-container/delta-v/docker-compose.yml config --profile metrics --profile lite > /dev/null && echo OK`
Expected: `OK`. If yaml is malformed, `docker compose config` returns a parse error.

- [ ] **Step 6: Commit**

```
git add opennms-container/delta-v/docker-compose.yml
git commit -m "feat(grafana): add grafana service + expand profile membership for metrics path

Grafana 11.4.0 OSS pinned, anonymous Viewer access, host port 13000.
Profile expansion: victoriametrics gains 'metrics' alongside 'metrics-e2e';
prometheus-writer gains 'metrics' so the writer is co-resident with its target.
Both profile names continue to work — 'metrics-e2e' retained as alias."
```

---

### Task 3: Write the datasource provisioning manifest

**Files:**
- Create: `opennms-container/delta-v/grafana/provisioning/datasources/victoriametrics.yml`

- [ ] **Step 1: Create the datasource yaml**

```yaml
# Grafana datasource provisioning — auto-loaded at startup via the
# /etc/grafana/provisioning/datasources/ bind mount.
apiVersion: 1

datasources:
  - name: VictoriaMetrics
    uid: victoriametrics
    type: prometheus
    access: proxy
    url: http://victoriametrics:8428
    isDefault: true
    editable: false
    jsonData:
      # Match VictoriaMetrics's ingest cadence; collectd-default polls at 30s.
      timeInterval: 30s
      httpMethod: POST
      manageAlerts: false
      prometheusType: Prometheus
      prometheusVersion: 2.40.0
```

The `uid: victoriametrics` is non-negotiable — the dashboard JSON references the datasource by UID.

- [ ] **Step 2: Validate the yaml syntax**

Run: `docker run --rm -v $(pwd)/opennms-container/delta-v/grafana/provisioning/datasources:/check mikefarah/yq:4 'true' /check/victoriametrics.yml > /dev/null && echo OK`
Expected: `OK`. (If `yq` isn't preferred, `python3 -c 'import yaml; yaml.safe_load(open("opennms-container/delta-v/grafana/provisioning/datasources/victoriametrics.yml"))' && echo OK` works equivalently.)

- [ ] **Step 3: Commit**

```
git add opennms-container/delta-v/grafana/provisioning/datasources/victoriametrics.yml
git commit -m "feat(grafana): provision VictoriaMetrics datasource (uid=victoriametrics, default)"
```

---

### Task 4: Write the dashboard provisioning manifest

**Files:**
- Create: `opennms-container/delta-v/grafana/provisioning/dashboards/default.yml`

- [ ] **Step 1: Create the dashboard provider yaml**

```yaml
# Grafana dashboard provider — points at the bind-mounted directory where
# JSON dashboard files live. Reloads every 30s, allows UI edits but they
# don't persist across container restarts (the JSON file wins).
apiVersion: 1

providers:
  - name: delta-v-bundled
    orgId: 1
    folder: Delta-V
    type: file
    disableDeletion: false
    editable: true
    updateIntervalSeconds: 30
    allowUiUpdates: true
    options:
      path: /var/lib/grafana/dashboards
      foldersFromFilesStructure: false
```

- [ ] **Step 2: Validate the yaml syntax**

Run: `python3 -c 'import yaml; yaml.safe_load(open("opennms-container/delta-v/grafana/provisioning/dashboards/default.yml"))' && echo OK`
Expected: `OK`.

- [ ] **Step 3: Commit**

```
git add opennms-container/delta-v/grafana/provisioning/dashboards/default.yml
git commit -m "feat(grafana): provision dashboard provider (folder Delta-V, 30s reload)"
```

---

### Task 5: Write the SNMP overview dashboard JSON

**Files:**
- Create: `opennms-container/delta-v/grafana/dashboards/snmp-overview.json`

This is the largest artifact. The complete JSON below was hand-crafted from the spec's panel definitions (spec §5). It targets Grafana 11.x schema version 39. After creation, Task 5 verifies the JSON loads cleanly by booting Grafana and asking the API for the dashboard back.

**Panels covered (per spec §5):**
1. (0,0) Interface throughput in (bps, top 10) — `topk(10, rate(...ifhcinoctets_total[5m]) * 8)`
2. (0,1) Interface throughput out (bps, top 10) — symmetric on `ifhcoutoctets`
3. (0,2) Interface error rate — sum of in/out errors + discards rates
4. (1,0) CPU load per processor — `opennms_mib2_host_resources_processor_hrprocessorload`
5. (1,1) Storage utilization (%) — `100 * hrstorageused / hrstoragesize`
6. (1,2) Devices monitored (table) — `count by (instance, foreign_source, snmp_syscontact, snmp_syslocation)(...)` — **demos PR #180's labels**

- [ ] **Step 1: Create the dashboard JSON file**

```json
{
  "uid": "snmp-overview",
  "title": "SNMP Overview",
  "tags": ["delta-v", "snmp"],
  "timezone": "browser",
  "schemaVersion": 39,
  "version": 1,
  "refresh": "30s",
  "time": {
    "from": "now-15m",
    "to": "now"
  },
  "templating": {
    "list": [
      {
        "name": "instance",
        "label": "Instance",
        "type": "query",
        "datasource": {
          "type": "prometheus",
          "uid": "victoriametrics"
        },
        "query": {
          "query": "label_values(opennms_mib2_x_interfaces_ifhcinoctets_total, instance)",
          "refId": "PrometheusVariableQueryEditor-VariableQuery"
        },
        "refresh": 1,
        "regex": "",
        "sort": 1,
        "multi": true,
        "includeAll": true,
        "allValue": ".*",
        "current": {
          "selected": true,
          "text": ["All"],
          "value": ["$__all"]
        }
      },
      {
        "name": "foreign_source",
        "label": "Foreign source",
        "type": "query",
        "datasource": {
          "type": "prometheus",
          "uid": "victoriametrics"
        },
        "query": {
          "query": "label_values(opennms_mib2_x_interfaces_ifhcinoctets_total{instance=~\"$instance\"}, foreign_source)",
          "refId": "PrometheusVariableQueryEditor-VariableQuery"
        },
        "refresh": 1,
        "regex": "",
        "sort": 1,
        "multi": true,
        "includeAll": true,
        "allValue": ".*",
        "current": {
          "selected": true,
          "text": ["All"],
          "value": ["$__all"]
        }
      }
    ]
  },
  "panels": [
    {
      "id": 1,
      "title": "Interface throughput in (bps, top 10)",
      "type": "timeseries",
      "datasource": {
        "type": "prometheus",
        "uid": "victoriametrics"
      },
      "gridPos": {"h": 8, "w": 8, "x": 0, "y": 0},
      "targets": [
        {
          "refId": "A",
          "expr": "topk(10, rate(opennms_mib2_x_interfaces_ifhcinoctets_total{instance=~\"$instance\", foreign_source=~\"$foreign_source\"}[5m]) * 8)",
          "legendFormat": "{{instance}} · ifIndex {{resource_instance}}"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "bps",
          "custom": {
            "drawStyle": "line",
            "lineWidth": 2,
            "fillOpacity": 10
          }
        },
        "overrides": []
      },
      "options": {
        "legend": {"showLegend": true, "displayMode": "list", "placement": "bottom"},
        "tooltip": {"mode": "multi", "sort": "desc"}
      }
    },
    {
      "id": 2,
      "title": "Interface throughput out (bps, top 10)",
      "type": "timeseries",
      "datasource": {
        "type": "prometheus",
        "uid": "victoriametrics"
      },
      "gridPos": {"h": 8, "w": 8, "x": 8, "y": 0},
      "targets": [
        {
          "refId": "A",
          "expr": "topk(10, rate(opennms_mib2_x_interfaces_ifhcoutoctets_total{instance=~\"$instance\", foreign_source=~\"$foreign_source\"}[5m]) * 8)",
          "legendFormat": "{{instance}} · ifIndex {{resource_instance}}"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "bps",
          "custom": {
            "drawStyle": "line",
            "lineWidth": 2,
            "fillOpacity": 10
          }
        },
        "overrides": []
      },
      "options": {
        "legend": {"showLegend": true, "displayMode": "list", "placement": "bottom"},
        "tooltip": {"mode": "multi", "sort": "desc"}
      }
    },
    {
      "id": 3,
      "title": "Interface error rate (errors+discards/sec)",
      "type": "timeseries",
      "datasource": {
        "type": "prometheus",
        "uid": "victoriametrics"
      },
      "gridPos": {"h": 8, "w": 8, "x": 16, "y": 0},
      "targets": [
        {
          "refId": "A",
          "expr": "rate(opennms_mib2_interface_errors_ifinerrors_total{instance=~\"$instance\", foreign_source=~\"$foreign_source\"}[5m]) + rate(opennms_mib2_interface_errors_ifouterrors_total{instance=~\"$instance\", foreign_source=~\"$foreign_source\"}[5m]) + rate(opennms_mib2_interface_errors_ifindiscards_total{instance=~\"$instance\", foreign_source=~\"$foreign_source\"}[5m]) + rate(opennms_mib2_interface_errors_ifoutdiscards_total{instance=~\"$instance\", foreign_source=~\"$foreign_source\"}[5m])",
          "legendFormat": "{{instance}} · ifIndex {{resource_instance}}"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "cps",
          "thresholds": {
            "mode": "absolute",
            "steps": [
              {"color": "green", "value": null},
              {"color": "yellow", "value": 1},
              {"color": "red", "value": 10}
            ]
          },
          "custom": {
            "drawStyle": "line",
            "lineWidth": 2,
            "thresholdsStyle": {"mode": "line"}
          }
        },
        "overrides": []
      },
      "options": {
        "legend": {"showLegend": true, "displayMode": "list", "placement": "bottom"},
        "tooltip": {"mode": "multi", "sort": "desc"}
      }
    },
    {
      "id": 4,
      "title": "CPU load per processor (%)",
      "type": "timeseries",
      "datasource": {
        "type": "prometheus",
        "uid": "victoriametrics"
      },
      "gridPos": {"h": 8, "w": 8, "x": 0, "y": 8},
      "targets": [
        {
          "refId": "A",
          "expr": "opennms_mib2_host_resources_processor_hrprocessorload{instance=~\"$instance\", foreign_source=~\"$foreign_source\"}",
          "legendFormat": "{{instance}} · proc {{resource_instance}}"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "percent",
          "min": 0,
          "max": 100,
          "custom": {
            "drawStyle": "line",
            "lineWidth": 2,
            "fillOpacity": 10
          }
        },
        "overrides": []
      },
      "options": {
        "legend": {"showLegend": true, "displayMode": "list", "placement": "bottom"},
        "tooltip": {"mode": "multi", "sort": "desc"}
      }
    },
    {
      "id": 5,
      "title": "Storage utilization (%)",
      "type": "bargauge",
      "datasource": {
        "type": "prometheus",
        "uid": "victoriametrics"
      },
      "gridPos": {"h": 8, "w": 8, "x": 8, "y": 8},
      "targets": [
        {
          "refId": "A",
          "expr": "100 * opennms_mib2_host_resources_storage_hrstorageused{instance=~\"$instance\", foreign_source=~\"$foreign_source\"} / opennms_mib2_host_resources_storage_hrstoragesize{instance=~\"$instance\", foreign_source=~\"$foreign_source\"}",
          "legendFormat": "{{instance}} · storage {{resource_instance}}",
          "instant": true
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "percent",
          "min": 0,
          "max": 100,
          "thresholds": {
            "mode": "absolute",
            "steps": [
              {"color": "green", "value": null},
              {"color": "yellow", "value": 70},
              {"color": "red", "value": 90}
            ]
          }
        },
        "overrides": []
      },
      "options": {
        "orientation": "horizontal",
        "displayMode": "gradient",
        "showUnfilled": true
      }
    },
    {
      "id": 6,
      "title": "Devices monitored",
      "type": "table",
      "datasource": {
        "type": "prometheus",
        "uid": "victoriametrics"
      },
      "gridPos": {"h": 8, "w": 8, "x": 16, "y": 8},
      "targets": [
        {
          "refId": "A",
          "expr": "count by (instance, foreign_source, snmp_syscontact, snmp_syslocation) (rate(opennms_mib2_x_interfaces_ifhcinoctets_total[5m]))",
          "format": "table",
          "instant": true
        }
      ],
      "fieldConfig": {
        "defaults": {},
        "overrides": [
          {
            "matcher": {"id": "byName", "options": "Value"},
            "properties": [{"id": "custom.hidden", "value": true}]
          },
          {
            "matcher": {"id": "byName", "options": "Time"},
            "properties": [{"id": "custom.hidden", "value": true}]
          }
        ]
      },
      "options": {
        "showHeader": true
      },
      "transformations": [
        {
          "id": "organize",
          "options": {
            "renameByName": {
              "instance": "Instance",
              "foreign_source": "Foreign source",
              "snmp_syscontact": "SNMP sysContact",
              "snmp_syslocation": "SNMP sysLocation"
            }
          }
        }
      ]
    }
  ],
  "annotations": {
    "list": []
  },
  "links": [],
  "weekStart": ""
}
```

- [ ] **Step 2: Validate the JSON syntax**

Run: `python3 -c 'import json; json.load(open("opennms-container/delta-v/grafana/dashboards/snmp-overview.json"))' && echo OK`
Expected: `OK`. (Catches typos like trailing commas or unmatched braces before Grafana ever sees it.)

- [ ] **Step 3: Commit**

```
git add opennms-container/delta-v/grafana/dashboards/snmp-overview.json
git commit -m "feat(grafana): add SNMP overview starter dashboard (uid=snmp-overview)

Six panels covering interface throughput in/out (top-10), error rate,
CPU load per processor, storage utilization (%), and a devices-monitored
table that exercises the four operator-friendly labels added by #180
(instance, foreign_source, snmp_syscontact, snmp_syslocation).
Two template variables: instance and foreign_source. 30s refresh,
last-15-min default time range, Delta-V folder."
```

---

### Task 6: Verify the Grafana stack boots and dashboard provisions

**No file changes** — this task validates that Tasks 1-5 produce a working stack before we extend the E2E test.

The implementer brings up only the metrics-pipeline subset of the stack, confirms Grafana boots, the datasource provisions, and the dashboard loads. This is faster than running the full E2E (which adds Collectd polling + sample wait time).

- [ ] **Step 1: Bring up the metrics stack**

```
cd opennms-container/delta-v
docker compose --profile lite --profile metrics up -d --build
```

Expected: services start. Wait until docker compose ps shows `delta-v-grafana` as `Up (healthy)`.

- [ ] **Step 2: Confirm Grafana is healthy**

```
docker compose exec -T grafana wget -q -O- http://localhost:3000/api/health
```
Expected: JSON response containing `"database":"ok"`.

- [ ] **Step 3: Confirm the VictoriaMetrics datasource provisioned**

```
docker compose exec -T grafana wget -q -O- --user=admin --password=admin \
    http://localhost:3000/api/datasources/uid/victoriametrics
```
Expected: JSON containing `"name":"VictoriaMetrics"` and `"url":"http://victoriametrics:8428"`.

- [ ] **Step 4: Confirm the dashboard provisioned**

```
docker compose exec -T grafana wget -q -O- --user=admin --password=admin \
    http://localhost:3000/api/dashboards/uid/snmp-overview
```
Expected: JSON containing `"title":"SNMP Overview"` and `"uid":"snmp-overview"`.

- [ ] **Step 5: Tear down**

```
docker compose --profile lite --profile metrics down -v --remove-orphans
cd ../..
```

If any of steps 2-4 fail, **stop and debug**:
- `docker compose logs grafana | grep -E "(error|provisioning|dashboard)"` for provisioning errors.
- The most common failure is dashboard JSON schema rejection — the JSON in Task 5 was hand-crafted and may need minor adjustments. Watch for log lines like `failed to load dashboard` and fix the offending JSON field.
- If the JSON loads but the dashboard returns 404 from the API, check `docker compose exec grafana ls -la /var/lib/grafana/dashboards/` to confirm the bind mount is correct.

(No commit. This task is verification only.)

---

### Task 7: Extend test-prometheus-writer-e2e.sh

**Files:**
- Modify: `opennms-container/delta-v/test-prometheus-writer-e2e.sh`

Three changes:
1. Profile name `metrics-e2e` → `metrics` in the cleanup trap (line 41) and the up command (line 46).
2. Update the echo on line 45 to reflect the new name.
3. Restructure the end of step 6 so the script doesn't `exit 0` immediately on success — needs to continue to steps 7-9 first.
4. Insert steps 7-9 (Grafana health, datasource health, dashboard load) after step 6.

- [ ] **Step 1: Update profile names (lines 41, 45, 46)**

Replace this block (lines 39-46):
```bash
cleanup() {
    echo "==> Tearing down stack"
    docker compose --profile lite --profile metrics-e2e down -v --remove-orphans || true
}
trap cleanup EXIT

echo "==> Starting delta-v Docker Compose (lite + metrics-e2e profiles)"
docker compose --profile lite --profile metrics-e2e up -d --build
```

with:
```bash
cleanup() {
    echo "==> Tearing down stack"
    docker compose --profile lite --profile metrics down -v --remove-orphans || true
}
trap cleanup EXIT

echo "==> Starting delta-v Docker Compose (lite + metrics profiles)"
docker compose --profile lite --profile metrics up -d --build
```

- [ ] **Step 2: Restructure step 6 so it falls through to steps 7-9**

Replace this block (lines 141-164):
```bash
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
```

with the restructured form (changes: replace `exit 0` with `vm_landed=true; break`; add a sentinel guard after the loop; remove the trailing `exit 1` since steps 7-9 now follow):
```bash
# ── Step 6: Query VictoriaMetrics ─────────────────────────────────────────────
echo "==> Step 6: Query VictoriaMetrics for the landed series"
deadline=$((SECONDS + VM_QUERY_TIMEOUT))
vm_landed=false
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

# ── Step 7: Wait for Grafana readiness ────────────────────────────────────────
echo "==> Step 7: Wait for Grafana /api/health"
deadline=$((SECONDS + STACK_READY_TIMEOUT))
gf_ready=false
while (( SECONDS < deadline )); do
    health=$(docker compose exec -T grafana \
        wget -q -O- http://localhost:3000/api/health 2>/dev/null || true)
    if echo "$health" | grep -q '"database":"ok"'; then
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
ds_health=$(docker compose exec -T grafana \
    wget -q -O- --user=admin --password="${GF_ADMIN_PASSWORD:-admin}" \
    http://localhost:3000/api/datasources/uid/victoriametrics/health 2>/dev/null || true)
if ! echo "$ds_health" | grep -q '"status":"OK"'; then
    echo "FAIL: VictoriaMetrics datasource health check failed"
    echo "Response: $ds_health"
    exit 1
fi
echo "==> Datasource OK"

# ── Step 9: Verify the snmp-overview dashboard is provisioned ─────────────────
echo "==> Step 9: Verify snmp-overview dashboard is loaded"
dash=$(docker compose exec -T grafana \
    wget -q -O- --user=admin --password="${GF_ADMIN_PASSWORD:-admin}" \
    http://localhost:3000/api/dashboards/uid/snmp-overview 2>/dev/null || true)
if ! echo "$dash" | grep -q '"title":"SNMP Overview"'; then
    echo "FAIL: snmp-overview dashboard not loaded"
    echo "Response: $dash"
    exit 1
fi
echo "==> Dashboard OK"

echo "==> ALL ASSERTIONS PASSED"
exit 0
```

- [ ] **Step 3: Verify the script is syntactically valid**

Run: `bash -n opennms-container/delta-v/test-prometheus-writer-e2e.sh && echo OK`
Expected: `OK`. (`bash -n` parses without executing — catches syntax errors.)

- [ ] **Step 4: Commit**

```
git add opennms-container/delta-v/test-prometheus-writer-e2e.sh
git commit -m "test(grafana): extend e2e with Grafana health + datasource health + dashboard load

Renames the up/down profile from metrics-e2e to metrics (both still work as
aliases on the affected services). Restructures step 6 to fall through to
new steps 7-9 instead of exiting on success."
```

---

### Task 8: Update README.md with Grafana access blurb + fix profile reference

**Files:**
- Modify: `opennms-container/delta-v/README.md`

Two changes:
1. Update line 346-347's description of `metrics-e2e` to reflect the rename.
2. Insert a new "## Grafana access (demo mode)" section after the "Wire format frozen" section (line 399) and before "## Troubleshooting" (line 401).

- [ ] **Step 1: Update the Phase 2 profiles description**

Locate this block (lines 340-347):
```markdown
### Profiles

- **lite, full** — starts `prometheus-writer`. Point `PROMETHEUS_WRITER_REMOTE_WRITE_URL`
  at your TSDB (Mimir, VictoriaMetrics, Cortex, Thanos Receive, Prometheus with
  `--web.enable-remote-write-receiver`). Without a reachable target, the writer
  starts healthy, opens its circuit on first POST failure, and stays paused.
- **metrics-e2e** — adds a pinned `victoriametrics:v1.106.1` container for E2E.
  Not intended for production.
```

Replace with:
```markdown
### Profiles

- **lite, full** — starts `prometheus-writer`. Point `PROMETHEUS_WRITER_REMOTE_WRITE_URL`
  at your TSDB (Mimir, VictoriaMetrics, Cortex, Thanos Receive, Prometheus with
  `--web.enable-remote-write-receiver`). Without a reachable target, the writer
  starts healthy, opens its circuit on first POST failure, and stays paused.
- **metrics** (alias: **metrics-e2e**) — adds a pinned `victoriametrics:v1.106.1`
  container plus a Grafana service with a starter SNMP dashboard. The canonical
  demo command is `docker compose --profile lite --profile metrics up`. See the
  "Grafana access" section below.
```

- [ ] **Step 2: Insert the new "Grafana access" section**

Locate the "## Troubleshooting" line (line 401). Immediately BEFORE it, insert a blank line followed by:

```markdown
## Grafana access (demo mode)

After `docker compose --profile lite --profile metrics up -d`, open
`http://localhost:13000/d/snmp-overview`. Anonymous Viewer access is on by
default (no login required) and the SNMP Overview dashboard is pre-provisioned
with VictoriaMetrics as the datasource.

The first poll cycle takes ~30 seconds; allow ~90 seconds after `up` for
panels to populate with data from the bundled mock SNMP agent.

To log in as admin (e.g. to create custom dashboards):

- Username: `admin`
- Password: `admin` (override via `GF_ADMIN_PASSWORD` environment variable).

```

(There must be a trailing blank line before the `## Troubleshooting` header so the headers don't collide.)

- [ ] **Step 3: Verify the markdown still renders cleanly**

Run: `head -1 opennms-container/delta-v/README.md && echo "---" && grep -c "^## " opennms-container/delta-v/README.md`
Expected: First line `# OpenNMS Delta-V`, and the count of `##` (level 2) headers should be one higher than before (the new "Grafana access" section).

- [ ] **Step 4: Commit**

```
git add opennms-container/delta-v/README.md
git commit -m "docs(grafana): document operator access at localhost:13000 + canonical metrics profile"
```

---

### Task 9: Full E2E verification gate

**No file changes** — this is the gate that ensures everything works end-to-end before push.

- [ ] **Step 1: Run the extended E2E script**

```
bash opennms-container/delta-v/test-prometheus-writer-e2e.sh
```

Expected: exits 0, with the final lines reading `==> Dashboard OK` and `==> ALL ASSERTIONS PASSED`. The full script should take roughly 3-5 minutes (stack spinup + 90s poll grace + verification).

If the test fails at any step:
- **Step 1-6 failures**: pre-existing E2E behavior; investigate as you would for the existing test.
- **Step 7 (Grafana readiness)**: check `docker compose logs grafana` for boot errors. Increase `start_period` in the docker-compose healthcheck if Grafana needs longer than 10s on slow machines.
- **Step 8 (datasource health)**: usually means VictoriaMetrics is unreachable from inside the Grafana container. Confirm both are on the same default compose network (they should be — no special config needed).
- **Step 9 (dashboard load)**: the JSON failed to parse on Grafana's side. `docker compose logs grafana | grep -E "(error|dashboard)"` will show the line of the JSON it choked on.

(No commit. This is a gate.)

---

### Task 10: Push branch + open PR `--repo pbrane/delta-v`

- [ ] **Step 1: Push the branch with upstream tracking**

```
git push -u origin feat/grafana-victoriametrics-bundle
```
Expected: branch is created on `pbrane/delta-v` (this is the default `origin` remote per project convention).

- [ ] **Step 2: Open the PR — `--repo pbrane/delta-v` is non-negotiable**

```
gh pr create --repo pbrane/delta-v --base develop --head feat/grafana-victoriametrics-bundle \
  --title "feat(grafana): add Grafana service + VictoriaMetrics datasource + starter SNMP dashboard" \
  --body "$(cat <<'EOF'
## Summary

Closes the "graphs after `docker compose up`" gap in the delta-v stack. After this PR, `docker compose --profile lite --profile metrics up` brings up the full pipeline (mock-snmp-agent → Minion → Collectd → Kafka → prometheus-writer → VictoriaMetrics → Grafana), and `http://localhost:13000/d/snmp-overview` shows live SNMP graphs within ~90 seconds.

**Configuration-only PR — no Java touched.**

### What ships

- **`grafana` service** in docker-compose: Grafana 11.4.0 OSS, anonymous Viewer access, host port 13000, profiled `[metrics, metrics-e2e]`.
- **Filesystem provisioning** under `opennms-container/delta-v/grafana/`:
  - VictoriaMetrics datasource (UID `victoriametrics`, default).
  - One starter dashboard `snmp-overview.json` with 6 panels (interface throughput in/out, error rate, CPU per processor, storage utilization %, devices-monitored table).
- **Profile rename**: `metrics` is now the canonical name; `metrics-e2e` retained as alias on `victoriametrics` and `grafana`. `prometheus-writer` profile expanded to `[lite, full, metrics]` so the writer is co-resident with its target.
- **E2E test extension**: `test-prometheus-writer-e2e.sh` gains 3 new steps (Grafana health, datasource health, dashboard load) and is updated to use `--profile metrics`.

### Killer demo for #180's labels

The "Devices monitored" table panel pulls `instance`, `foreign_source`, `snmp_syscontact`, `snmp_syslocation` columns from a `count by (...)` query — operators see all four operator-friendly labels added by #180 in a single tabular view without writing PromQL.

### Depends on

- delta-v#180 (currently open) ships the labels the dashboard variables and the devices-monitored table reference. The dashboard loads cleanly without #180 (E2E passes), but the variables return empty values and the table is blank until #180 lands.

Spec: `docs/superpowers/specs/2026-04-20-grafana-victoriametrics-bundle-design.md`. Plan: `docs/superpowers/plans/2026-04-20-grafana-victoriametrics-bundle-plan.md`.

## Test plan

- [x] `bash opennms-container/delta-v/test-prometheus-writer-e2e.sh` — green (steps 1-9, including new Grafana asserts).
- [x] Manual visual: open `http://localhost:13000/d/snmp-overview`, confirm panels render with data after ~90s of polling.
- [ ] Post-merge: nothing — this is operator-facing tooling, no follow-up memo.
EOF
)"
```

Expected: GitHub CLI returns the PR URL.

- [ ] **Step 3: Print the PR URL**

The `gh pr create` command prints the URL on success. Echo it back so the user can open it.

---

## Self-Review Checklist (run after writing the plan; not a separate task)

The following list pins what I checked when writing this plan. Re-verify if the plan is changed.

**1. Spec coverage**

| Spec section | Implementing task |
|---|---|
| `grafana:` service in docker-compose, profile membership, healthcheck | Task 2 |
| `victoriametrics` profile expansion to include `metrics` | Task 2 |
| `prometheus-writer` profile expansion to include `metrics` | Task 2 |
| `grafanadata` named volume | Task 2 |
| Datasource provisioning yaml | Task 3 |
| Dashboard provider provisioning yaml | Task 4 |
| `snmp-overview.json` with 2 template variables + 6 panels | Task 5 |
| E2E rename + 3 new steps | Task 7 |
| README operator-facing blurb + Phase 2 profile description fix | Task 8 |
| Full E2E green | Task 9 |
| PR opened `--repo pbrane/delta-v` | Task 10 |

All scope items have an implementing task. Spec out-of-scope items (per-interface drilldown dashboard, ClickHouse datasource, alerting, visual regression, LDAP/SSO) are correctly absent from the plan.

**2. Placeholder scan**: no `TBD`, `TODO`, `fill in`, `add appropriate handling`, or "similar to Task N" references. All file content is provided in full.

**3. Type/name consistency**:
- Datasource `uid: victoriametrics` — Task 3 declares it; Task 5 dashboard JSON references it 7 times (2 variables + 5 panel target datasources); Task 7 E2E queries `/api/datasources/uid/victoriametrics/health`. All match.
- Dashboard `uid: snmp-overview` — Task 5 declares it; Task 7 queries `/api/dashboards/uid/snmp-overview`. All match.
- Profile `metrics` — Task 2 declares; Task 7 uses in cleanup + up; Task 8 documents.
- Container hostname `grafana` — Task 2 declares; Task 7 references via `docker compose exec -T grafana`.
- Host port `13000` — Task 2 declares; Task 8 README references.
- Metric names in dashboard JSON match the spec table exactly (verified against `mib2.xml` during spec writing).
