# Grafana + VictoriaMetrics Bundle — Design Spec

**Date:** 2026-04-20
**Branch:** `feat/grafana-victoriametrics-bundle` (already created off develop tip `12a5479e3b6`)
**Predecessor PR:** delta-v#180 (`feat/prometheus-writer-label-coverage`) — ships the `instance`, `foreign_id`, `snmp_syscontact`, `snmp_syslocation` labels this dashboard depends on. **This PR's dashboard variables and table panel reference labels added by #180.** If #180 is reverted, the SNMP Overview dashboard's `$instance` variable returns no values and panels render empty (not error) — the dashboard structure stays loadable and the E2E health check still passes, but visualization breaks until #180 re-lands.
**PR target:** pbrane/delta-v develop (NEVER OpenNMS/opennms).

## Goal

Make Delta-V's Collectd → Kafka → prometheus-writer → VictoriaMetrics pipeline operator-visible by adding a Grafana service to the docker-compose stack with a starter SNMP overview dashboard pre-provisioned. After this PR, `docker compose --profile lite --profile metrics up` produces a stack where `http://localhost:13000/d/snmp-overview` shows live SNMP graphs from the mock SNMP agent within ~90s of startup.

## Scope

### In Scope

- New `grafana/grafana:11.4.0` service in `opennms-container/delta-v/docker-compose.yml`, anonymous read-only access enabled, host port `13000`.
- New `grafana/` subdirectory with provisioning manifests (datasource + dashboard provider) and one starter dashboard JSON.
- Profile rename: introduce a new `metrics` profile that contains both `victoriametrics` and `grafana` (and the `prometheus-writer`, so it has a write target). Existing `metrics-e2e` profile retained as alias for backwards compatibility — both `victoriametrics` and `grafana` will list `[metrics, metrics-e2e]`.
- Extension of `test-prometheus-writer-e2e.sh` with three new steps verifying Grafana health, datasource health, and dashboard load.
- Documentation: brief operator-facing README addition explaining how to access the dashboard.

### Out of Scope

- Multiple dashboards (per-interface drill-down, node detail page, alarm view). Tracked as follow-up work — this PR establishes the foundation; further dashboards are iterative.
- Grafana alerting rules. Out of scope for the demo bundle; operators set up alerts against their production VM/Prometheus separately.
- LDAP / SSO / OAuth integration. Anonymous-Viewer is the demo default; production deployments configure auth via env-var override.
- Per-tenant Grafana org separation.
- Bundling a Prometheus alternative (the bundled VM is the only supported storage backend in this PR; operators pointing at their own Prometheus do so via the existing `PROMETHEUS_WRITER_REMOTE_WRITE_URL` env override and bring their own visualization).
- Visual regression tests (e.g. screenshot comparison). The E2E asserts services-are-healthy and dashboard-is-loaded; visual rendering of panels is verified manually before merge.

## Background

The Phase 2 E2E test (`opennms-container/delta-v/test-prometheus-writer-e2e.sh`) already proves: mock-snmp-agent → Minion poll → Collectd → `deltav-timeseries` Kafka topic → `prometheus-writer` (joined with provisiond's NodeContext) → VictoriaMetrics, ending with a successful PromQL query. PR #180 (currently open against develop) ships operator-friendly labels (`instance`, `foreign_id`, sanitized metadata keys) that Grafana templating naturally consumes.

The gap between "the pipeline works" and "an operator can see graphs" is purely the Grafana service plus a starter dashboard. There is no data-pipeline change in this PR.

**Profile-composition gap (corrected by this PR):** `prometheus-writer` is in profiles `[lite, full]` today, but its write target `victoriametrics` is in profile `[metrics-e2e]` only. So in default deployments, the writer's circuit breaker eventually opens because there's no destination. This PR aligns the writer and its target into the same profile (`metrics`), so any profile combination that includes the writer also includes a working target.

## Design

### 1. File layout

```
opennms-container/delta-v/
├── docker-compose.yml                       (MODIFIED — see §2)
│
├── grafana/                                 (NEW — bind-mounted into container)
│   ├── provisioning/
│   │   ├── datasources/
│   │   │   └── victoriametrics.yml          (NEW)
│   │   └── dashboards/
│   │       └── default.yml                  (NEW)
│   └── dashboards/
│       └── snmp-overview.json               (NEW)
│
├── test-prometheus-writer-e2e.sh            (MODIFIED — see §6)
│
└── README.md                                (MODIFIED — operator-facing access doc)
```

### 2. `docker-compose.yml` changes

Three changes:

**a. New `grafana` service** (inserted after `victoriametrics:` block):

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

**b. Update `victoriametrics` profile list** from `[metrics-e2e]` to `[metrics, metrics-e2e]`. Same file, only the `profiles:` line in the `victoriametrics` block changes.

**c. Update `prometheus-writer` profile list** from `[lite, full]` to `[lite, full, metrics]`. So when `--profile metrics` is selected alongside an upstream-data profile (`lite` or `full`), the writer is co-resident with its VM target. The canonical demo command is therefore `--profile lite --profile metrics` (matching the existing `lite + metrics-e2e` pattern of the test script). `--profile metrics` alone is intentionally NOT a complete-pipeline profile: it brings up VM + Grafana + writer, but `collectd` and `provisiond` (which produce the data) live in `lite`/`full`/`passive`. Operators picking only `--profile metrics` see Grafana with empty graphs, not a runtime error.

**d. New named volume** at the bottom of the `volumes:` section:

```yaml
  grafanadata:
```

### 3. Datasource provisioning manifest

`grafana/provisioning/datasources/victoriametrics.yml`:

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

**Why `uid: victoriametrics` is non-negotiable:** the dashboard JSON references the datasource by UID, not name. Pinning the UID makes the dashboard portable across Grafana installs and keeps the E2E datasource-health check (`/api/datasources/uid/victoriametrics/health`) deterministic.

**Why `editable: false`:** prevents accidental UI edits that would diverge from this YAML.

### 4. Dashboard provider manifest

`grafana/provisioning/dashboards/default.yml`:

```yaml
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

**`editable: true` on dashboards (vs. false on datasource):** operators CAN edit panels live to experiment, but the source-of-truth JSON wins on container restart (or every 30s reload).

### 5. Starter SNMP overview dashboard

`grafana/dashboards/snmp-overview.json`. Semantic spec — the implementation plan produces the actual JSON, hand-built then exported from a live Grafana to ensure schema correctness.

**Dashboard metadata:**
- Title: `SNMP Overview`
- UID: `snmp-overview`
- Folder: `Delta-V` (per the provisioner's `folder:` field)
- Refresh: `30s` (matches collectd default poll interval)
- Default time range: `Last 15 minutes`
- Tags: `["delta-v", "snmp"]`

**Two template variables:**

| Variable | Type | Query | Multi | Include All | Default |
|---|---|---|---|---|---|
| `instance` | Query | `label_values(opennms_mib2_x_interfaces_ifhcinoctets_total, instance)` | yes | yes | All |
| `foreign_source` | Query | `label_values(opennms_mib2_x_interfaces_ifhcinoctets_total{instance=~"$instance"}, foreign_source)` | yes | yes | All |

**Six panels** (3-column × 2-row grid):

| Pos | Panel title | Query (templated) | Visualization |
|---|---|---|---|
| (0,0) | **Interface throughput in (bps, top 10)** | `topk(10, rate(opennms_mib2_x_interfaces_ifhcinoctets_total{instance=~"$instance", foreign_source=~"$foreign_source"}[5m]) * 8)` | Time series; legend `{{instance}} · ifIndex {{resource_instance}}`; unit `bits/s`. |
| (0,1) | **Interface throughput out (bps, top 10)** | `topk(10, rate(opennms_mib2_x_interfaces_ifhcoutoctets_total{instance=~"$instance", foreign_source=~"$foreign_source"}[5m]) * 8)` | Time series; same legend pattern; unit `bits/s`. |
| (0,2) | **Interface error rate (errors+discards/sec)** | `rate(opennms_mib2_interface_errors_ifinerrors_total{instance=~"$instance",foreign_source=~"$foreign_source"}[5m]) + rate(opennms_mib2_interface_errors_ifouterrors_total{instance=~"$instance",foreign_source=~"$foreign_source"}[5m]) + rate(opennms_mib2_interface_errors_ifindiscards_total{instance=~"$instance",foreign_source=~"$foreign_source"}[5m]) + rate(opennms_mib2_interface_errors_ifoutdiscards_total{instance=~"$instance",foreign_source=~"$foreign_source"}[5m])` | Time series with thresholds (yellow > 1/s, red > 10/s); unit `errors/s`. |
| (1,0) | **CPU load per processor** | `opennms_mib2_host_resources_processor_hrprocessorload{instance=~"$instance", foreign_source=~"$foreign_source"}` | Time series; legend `{{instance}} · proc {{resource_instance}}`; unit `percent (0-100)`. |
| (1,1) | **Storage utilization (%)** | `100 * opennms_mib2_host_resources_storage_hrstorageused{instance=~"$instance", foreign_source=~"$foreign_source"} / opennms_mib2_host_resources_storage_hrstoragesize{instance=~"$instance", foreign_source=~"$foreign_source"}` | Bar gauge; legend `{{instance}} · storage {{resource_instance}}`; thresholds at 70 (yellow) / 90 (red). |
| (1,2) | **Devices monitored** | `count by (instance, foreign_source, snmp_syscontact, snmp_syslocation) (rate(opennms_mib2_x_interfaces_ifhcinoctets_total[5m]))` | Table; columns shown: `instance`, `foreign_source`, `snmp_syscontact`, `snmp_syslocation`. Hide the `Value` column. **This panel is the killer demo for PR #180's labels** — operators see all four operator-friendly labels in a single table without having to know any PromQL. |

**Pinned metric names (sanitized via existing `NameSanitizer`):**

| Source group + attribute | Sanitized PromQL name |
|---|---|
| `mib2-X-interfaces.ifHCInOctets` (Counter64) | `opennms_mib2_x_interfaces_ifhcinoctets_total` |
| `mib2-X-interfaces.ifHCOutOctets` (Counter64) | `opennms_mib2_x_interfaces_ifhcoutoctets_total` |
| `mib2-interface-errors.ifInErrors` (counter) | `opennms_mib2_interface_errors_ifinerrors_total` |
| `mib2-interface-errors.ifOutErrors` | `opennms_mib2_interface_errors_ifouterrors_total` |
| `mib2-interface-errors.ifInDiscards` | `opennms_mib2_interface_errors_ifindiscards_total` |
| `mib2-interface-errors.ifOutDiscards` | `opennms_mib2_interface_errors_ifoutdiscards_total` |
| `mib2-host-resources-processor.hrProcessorLoad` (Gauge32) | `opennms_mib2_host_resources_processor_hrprocessorload` |
| `mib2-host-resources-storage.hrStorageUsed` (gauge) | `opennms_mib2_host_resources_storage_hrstorageused` |
| `mib2-host-resources-storage.hrStorageSize` (gauge) | `opennms_mib2_host_resources_storage_hrstoragesize` |

Naming derivation (per `TimeseriesToPromTranslator.buildMetricName`): `"opennms_" + sanitize(groupName) + "_" + sanitize(attrName)`, plus `_total` suffix for counters. `NameSanitizer` lowercases everything and replaces non-alphanumerics with `_`.

### 6. E2E test extension

`opennms-container/delta-v/test-prometheus-writer-e2e.sh` modifications:

**Profile rename** (lines 41 and 46): change `--profile metrics-e2e` to `--profile metrics`. Since both `victoriametrics` and `grafana` list both profile names, the cleanup `down -v` still works.

**Three new steps after the existing step 6 (VictoriaMetrics gold-standard query):**

**Step 7 — Wait for Grafana readiness:**

```bash
echo "==> Step 7: Wait for Grafana /api/health"
deadline=$((SECONDS + STACK_READY_TIMEOUT))
while (( SECONDS < deadline )); do
    health=$(docker compose exec -T grafana \
        wget -q -O- http://localhost:3000/api/health 2>/dev/null || true)
    if echo "$health" | grep -q '"database":"ok"'; then
        echo "==> Grafana ready"
        break
    fi
    sleep 3
done
if ! echo "${health:-}" | grep -q '"database":"ok"'; then
    echo "FAIL: Grafana readiness timeout"
    exit 1
fi
```

**Step 8 — Datasource health:**

```bash
echo "==> Step 8: Verify VictoriaMetrics datasource is reachable from Grafana"
ds_health=$(docker compose exec -T grafana \
    wget -q -O- --user=admin --password="${GF_ADMIN_PASSWORD:-admin}" \
    http://localhost:3000/api/datasources/uid/victoriametrics/health || true)
if ! echo "$ds_health" | grep -q '"status":"OK"'; then
    echo "FAIL: VictoriaMetrics datasource health check failed"
    echo "Response: $ds_health"
    exit 1
fi
echo "==> Datasource OK"
```

**Step 9 — Dashboard loaded:**

```bash
echo "==> Step 9: Verify snmp-overview dashboard is provisioned"
dash=$(docker compose exec -T grafana \
    wget -q -O- --user=admin --password="${GF_ADMIN_PASSWORD:-admin}" \
    http://localhost:3000/api/dashboards/uid/snmp-overview || true)
if ! echo "$dash" | grep -q '"title":"SNMP Overview"'; then
    echo "FAIL: snmp-overview dashboard not loaded"
    echo "Response: $dash"
    exit 1
fi
echo "==> Dashboard OK"
```

**What this catches at PR time:**

| Catches | Doesn't catch (acceptable for this PR) |
|---|---|
| Grafana service won't start | Panel queries return no data |
| Provisioning YAML malformed | A panel JSON typo that doesn't break loading |
| Datasource UID mismatch | Variable query syntax errors |
| Datasource can't reach VM | Stale/incorrect metric names in panels |
| Dashboard JSON malformed | Visual regressions (color, layout) |

The "doesn't catch" column is verified manually by opening the dashboard in a browser at `localhost:13000` after the test passes. The PR description includes a "smoke-tested visually against mock-snmp-agent" line.

### 7. README addition

Add a short section to `opennms-container/delta-v/README.md` (after the existing profile documentation, before the troubleshooting section if any):

```markdown
## SNMP graphs in Grafana (demo mode)

After `docker compose --profile lite --profile metrics up -d`, open
`http://localhost:13000/d/snmp-overview`. Anonymous Viewer access is on by
default (no login required) and the SNMP Overview dashboard is pre-provisioned
with VictoriaMetrics as the datasource.

The first poll cycle takes ~30 seconds; allow ~90 seconds after `up` for
panels to populate with data from the bundled mock SNMP agent.

To log in as admin (e.g. to create custom dashboards):
- Username: `admin`
- Password: `admin` (override via `GF_ADMIN_PASSWORD` env var).
```

## Edge cases & error handling

| Scenario | Handling |
|---|---|
| Operator runs `--profile metrics-e2e` (legacy name) | Both VM and Grafana still come up — they're listed under both profiles. Test script also updated to use `--profile metrics` going forward. |
| Operator runs `--profile lite` only (no metrics) | prometheus-writer comes up, finds no VM target, circuit breaker opens. Same behavior as today (this PR doesn't change `lite` profile membership). Operator sees DLQ + circuit metrics climb in `/actuator/prometheus` but no Grafana. |
| Grafana starts before VM is healthy | `depends_on: victoriametrics: service_healthy` blocks Grafana startup until VM passes its healthcheck. |
| Operator changes `prometheus-writer.labels.instance-source` to `FOREIGN_ID` or `NODE_ID` | Dashboard variables and panels still work — they reference the `instance` label by name, not by content. Legends will display the new format. |
| `snmp_syscontact` / `snmp_syslocation` are empty (SNMP discovery hasn't populated them) | Devices-monitored table still renders with empty cells in those columns. Operator sees the column structure even before metadata fills in. |
| Anonymous access undesirable | Set `GF_AUTH_ANONYMOUS_ENABLED=false` in env; operators land on the login page. |

## Performance

| Resource | Cost |
|---|---|
| Grafana container memory | ~150-200 MB resident, ~250 MB peak (typical Grafana 11 OSS) |
| Grafana container startup time | ~5-15 seconds to readiness (provisioning happens during startup) |
| Stack startup additional cost (vs. today's `metrics-e2e`) | +5-15s for Grafana, +0s for VM (already in `metrics-e2e`) |
| Disk: bind-mounted dashboard JSON | ~50 KB |
| Disk: `grafanadata` volume (admin password, session state) | ~5-10 MB after first boot |

Negligible for the demo use case.

## Backward compatibility

| Change | Impact |
|---|---|
| New `metrics` profile alias | Additive. `--profile metrics-e2e` continues to work for any existing scripts/docs that reference it. |
| `victoriametrics` profile expanded | Additive (`[metrics-e2e]` → `[metrics, metrics-e2e]`). |
| `prometheus-writer` profile expanded | Additive (`[lite, full]` → `[lite, full, metrics]`). The writer now appears in `--profile metrics` too. |
| New `grafana` service | Purely additive. Operators not opting into `metrics`/`metrics-e2e` see no change. |
| `test-prometheus-writer-e2e.sh` profile name change | Internal test script — operators don't run it. CI pipelines pointing at this script may need a single-line update if they hardcoded the profile name. |

No PromQL query breaks. No existing label changes (this PR depends on PR #180's labels but doesn't add or modify any).

## Testing strategy

**Automated (CI):** the extended `test-prometheus-writer-e2e.sh` (steps 1-9) runs the full pipeline including Grafana health checks and dashboard load.

**Manual (before merge):**
1. `cd opennms-container/delta-v && docker compose --profile lite --profile metrics up -d --build`
2. Wait ~90s for poll cycles to flow.
3. Open `http://localhost:13000/d/snmp-overview` in a browser.
4. Confirm: device picker dropdown populated; bps in/out panels show non-zero rates; CPU/storage panels show host-resources data; devices-monitored table shows the snmp-agent host with foreign_source populated.
5. Add a "smoke-tested visually 2026-04-XX" line to the PR description.

**No unit tests, no module rebuild required.** This PR is configuration and JSON only — no Java code touched, so `./mvnw -pl core/prometheus-writer verify` is unchanged.

## Success criteria

- [x] `grafana:11.4.0` service in docker-compose, profile `[metrics, metrics-e2e]`, healthcheck pinned to `/api/health` returning `database: ok`.
- [x] VictoriaMetrics datasource provisioned with UID `victoriametrics`, isDefault, editable=false.
- [x] One starter dashboard `snmp-overview.json` with 2 template variables (`instance`, `foreign_source`) and 6 panels covering interface throughput, errors, CPU, storage, and a labels-demonstration devices table.
- [x] Profile rename: `metrics` is the canonical new name; `metrics-e2e` retained as alias.
- [x] `prometheus-writer` profile expanded to include `metrics` (so the writer + target co-reside).
- [x] `test-prometheus-writer-e2e.sh` extended with steps 7-9 (Grafana health + datasource health + dashboard load) and profile name updated to `metrics`.
- [x] README updated with a 5-line operator-facing access blurb.
- [x] PR opened `--repo pbrane/delta-v --base develop`. Title prefix `feat(grafana):` (proposed; no adjacent convention since this is the first Grafana-related PR).

## After this PR (queued follow-ups)

- **Per-interface drill-down dashboard.** Click an interface row in the SNMP Overview, jump to a single-interface page with throughput, errors, ifAlias (when ifAlias is exposed via the proto follow-up tracked under `project_prometheus_label_coverage_gaps`).
- **Node detail dashboard.** All host-resources data for a single device.
- **Datasource for ClickHouse flows.** Delta-V already has flow-enricher → ClickHouse (per memory). A second Grafana datasource pointing at ClickHouse would let one dashboard reach metrics + flows.
- **Anonymous-access toggle in README.** Document the env-var override pattern more prominently for production deployments.
- **Visual regression test** (image diff against a baseline screenshot). Currently out of scope — defer until the dashboard set is large enough that visual drift is a real risk.
