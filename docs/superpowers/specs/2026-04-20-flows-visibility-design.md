# Flows Visibility — Design Spec

**Date:** 2026-04-20
**Branch:** `feat/flows-visibility` (off develop tip `952c6102f78`)
**Predecessor PRs:** delta-v#181 (Grafana bundle), #183 (l8opensim mock-lab), #139-#161 (flow-enricher pipeline), #145 (ClickHouse persistence)
**PR target:** pbrane/delta-v develop. **NEVER OpenNMS/opennms.** Title prefix: `feat(flows):`.

## Goal

Deliver a self-contained "open Grafana, see flows" demo on top of the existing Minion → flow-enricher → ClickHouse pipeline. Operators run `docker compose --profile lite --profile metrics up -d`, open `http://localhost:13000/d/flows-overview`, and see a populated 6-panel Flows Overview dashboard with working multi-location filtering, protocol-mix visualization, and a devices-monitored-style exporter inventory.

This PR surfaces existing-but-latent capability rather than building new capability. flow-enricher already handles all four flow protocols (PRs #157/#156), ClickHouse's `flows_raw` schema already carries `location` (schema line 57), the provisiond node-context producer (PR #171) already enriches l8opensim nodes because PR #183 registered them. What this PR adds: (1) Grafana ClickHouse datasource + plugin, (2) dashboard JSON, (3) profile widening so flows ship with the default demo, (4) l8opensim flow-exporter config producing telemetry at `location=l8opensim-lab`.

## Scope

### In Scope

- **Grafana ClickHouse datasource**: new provisioning YAML at `opennms-container/delta-v/grafana/provisioning/datasources/clickhouse.yml`, UID `clickhouse`, pointing at `http://clickhouse:8123` via the `grafana-clickhouse-datasource` plugin (auto-installed via `GF_INSTALL_PLUGINS` on the grafana service).
- **New dashboard** `opennms-container/delta-v/grafana/dashboards/flows-overview.json`:
  - UID: `flows-overview`.
  - 6 panels: hero bandwidth-by-location timeseries, top apps, top conversations, per-exporter flow rate, protocol+DSCP mix, flow sources inventory.
  - 3 template variables: `monitoring_location`, `exporter_instance`, `application` — each multi-select with `includeAll=true`.
- **Compose profile widening**: `clickhouse`, `clickhouse-init`, `flow-enricher`, `flow-default-testnode-1`, `flow-sflow-testnode-1` move from `[full]` → `[lite, full, metrics]`. No new containers; no network rewiring.
- **l8opensim flow exporter config**: 20 simulator devices split 5-5-5-5 across NetFlow v5 / v9 / IPFIX / sFlow, emitting to `127.0.0.1:4729` in the shared l8opensim+minion-lab network namespace. New file `opennms-container/delta-v/l8opensim/flow-exporters.json` (20-entry JSON array). Wiring mechanism (inline device-creation fields vs separate POST) determined by plan-phase API spike.
- **E2E extension**: `test-prometheus-writer-e2e.sh` gains Step 11 asserting `SELECT count() FROM deltav.flows_raw WHERE location = 'l8opensim-lab'` > 0 AND `count(DISTINCT netflow_version) = 4` within `CLICKHOUSE_QUERY_TIMEOUT=60s`.

### Out of Scope

- **PR B** (next session): vmagent scraping of `/actuator/prometheus` on flow-enricher + flow-processing self-monitoring dashboard.
- **Forensic drilldown dashboard** (option B from brainstorming Q1): deferred.
- **Retiring `flow-default-testnode-1` + `flow-sflow-testnode-1`**: kept to provide Default-location flow traffic. Retirement possible in a future PR once l8opensim can span locations.
- **ClickHouse MV additions for `location`**: existing MVs (`flows_by_application_1m`, etc.) don't carry the `location` column. Dashboard queries `flows_raw` directly — plenty fast for dashboard time ranges. Adding `location` to MVs is a future schema migration.
- **`project_sink_topic_rename`** (`OpenNMS.Sink.*` → `DeltaV.Sink.*`): untouched.
- **`project_flow_enricher_classpath_batch_cleanup`** (148 exclusions, core.model-api pom diet): untouched, awaits next horizon release.
- **DSCP traffic variation at l8opensim**: if the simulator emits only DSCP=0, Panel 5's DSCP sub-chart is monochrome. Upstream contribution opportunity; do not block merge.

## Background

PR #183 shipped the canonical in-compose mock environment (l8opensim + 20 devices at `location=l8opensim-lab`) with a working snmp-overview dashboard and a Step 10 E2E assertion that SNMP metrics flow end-to-end from the l8opensim-lab location. Manual verification of the snmp-overview dashboard confirmed the expected result: operators see 20 lab devices in the Devices-monitored panel, filtered by Monitoring location.

What #183 deliberately deferred (as three queued follow-ups): wiring l8opensim's flow exporters (NetFlow v5/v9, IPFIX, sFlow v5), SNMP trap exporter, and UDP syslog exporter. This PR picks up the first of those three — flow exporters — and pairs it with a dashboard that makes the flow data visible alongside the SNMP data.

The flow pipeline itself is production-validated: PRs #139-#161 shipped flow-enricher through Phase 2 (full per-protocol parsing via horizon UdpParser + capturing dispatcher + SCS consumer), with 18/18 E2E tests green and live hsflowd verification. PR #145 shipped ClickHouse persistence via the Kafka engine + 4 dimension MVs. PR #171 shipped the provisiond node-context producer so flow-enricher has enrichment data. PR #181 shipped Grafana + VictoriaMetrics + the provisioning scaffold.

Nothing in this PR changes how flows are parsed, enriched, or stored. The only new pipeline participant is Grafana's ClickHouse datasource plugin — everything else is configuration and content.

## Architecture

### Service topology changes

All compose changes are profile widenings and one grafana env-var addition. No new containers, no new networks, no new `depends_on` edges.

| Service | Current profile(s) | New profile(s) | Change |
|---|---|---|---|
| `clickhouse` | `[full]` | `[lite, full, metrics]` | widen |
| `clickhouse-init` | `[full]` | `[lite, full, metrics]` | widen |
| `flow-enricher` | `[full]` | `[lite, full, metrics]` | widen |
| `flow-default-testnode-1` | `[full]` | `[lite, full, metrics]` | widen |
| `flow-sflow-testnode-1` | `[full]` | `[lite, full, metrics]` | widen |
| `grafana` | `[metrics, metrics-e2e]` | unchanged | add `GF_INSTALL_PLUGINS` env var |

### Grafana datasource provisioning

**New file** `opennms-container/delta-v/grafana/provisioning/datasources/clickhouse.yml`:

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

(Exact field names confirmed during plan's Task 1 plugin-install verification. The `grafana-clickhouse-datasource` plugin's YAML config shape may put `username` under `jsonData` or `secureJsonData` — plan task validates via `GET /api/datasources/uid/clickhouse/health`.)

**Grafana service edits** in `docker-compose.yml`:

```yaml
  grafana:
    # ... existing config ...
    environment:
      # ... existing GF_* vars ...
      GF_INSTALL_PLUGINS: grafana-clickhouse-datasource
    healthcheck:
      # May need start_period bump from 30s → 60s if plugin-install races readiness.
      # Empirically measured during plan-phase Task 7 verification gate.
      start_period: 30s   # or 60s post-measurement
```

### l8opensim flow exporter wiring

**Target topology.** All 20 devices emit to `127.0.0.1:4729` (loopback inside the shared l8opensim+minion-lab netns). The per-device TUN IP (10.0.0.N) becomes the exporter IP in the flow header; flow-enricher's NodeInfo lookup attributes each flow to the correct node via the (location=l8opensim-lab, ip-addr=10.0.0.N) → node_id mapping already populated by PR #183's requisition + PR #171's node-context producer.

**Protocol split across the 20 devices:**

| IP range | Protocol | Rationale |
|---|---|---|
| 10.0.0.1 – 10.0.0.5 | NetFlow v5 | oldest variant; coverage for legacy environments |
| 10.0.0.6 – 10.0.0.10 | NetFlow v9 | most common; primary production variant |
| 10.0.0.11 – 10.0.0.15 | IPFIX | modern NetFlow successor |
| 10.0.0.16 – 10.0.0.20 | sFlow v5 | distinct protocol family, different parser path |

All four converge on UDP :4729 because Minion's dispatcher inspects the packet header and routes to the correct parser.

**New file** `opennms-container/delta-v/l8opensim/flow-exporters.json`: 20-entry JSON array with per-device config (IP, protocol, collector, sampling). Exact field names determined by plan's Task 1 API spike.

**Plan-Task-1 scenario handling:**

- **Scenario A** — l8opensim accepts flow config as inline fields on `POST /api/v1/devices`. Then `devices.json` (from #183) is *extended* with new keys per entry, and `post-each.sh` ships the combined payload.
- **Scenario B** — flow config requires a separate API call per device. Then a new script `enable-flows.sh` runs after `post-each.sh` in the `l8opensim-provisioner` command chain; it iterates the 20 devices and sends one flow-config POST each.
- **Scenario C (fallback)** — l8opensim supports only global flow config (one collector, one protocol for all). Then the protocol split collapses to single-protocol NetFlow v9 across all 20 devices. Panel 5's Protocol sub-chart becomes monochrome; everything else works identically. Spec accommodates this without design rework.

**Traffic generation.** l8opensim's internal traffic generator produces synthetic device-to-device traffic continuously — flow records emerge without external ping drivers. Sampling rate: 1 (every packet), matching `flow-sflow-testnode-1`'s `SAMPLING_RATE=1` pattern for low-volume demo visibility.

### Dashboard design

**File:** `opennms-container/delta-v/grafana/dashboards/flows-overview.json`. UID: `flows-overview`. All queries target `deltav.flows_raw` directly (fast for dashboard time ranges; pre-aggregated MVs reserved for future longer-range panels).

**Template variables** (rendered top of dashboard, order matters for chaining):

| Name | Label | Query | Multi / Include All |
|---|---|---|---|
| `monitoring_location` | Monitoring location | `SELECT DISTINCT location FROM deltav.flows_raw WHERE $__timeFilter(timestamp)` | yes / yes |
| `exporter_instance` | Exporter | `SELECT DISTINCT concat(exporter_node_foreign_source, '/', exporter_node_foreign_id) FROM deltav.flows_raw WHERE $__timeFilter(timestamp) AND location IN (${monitoring_location:sqlstring})` | yes / yes |
| `application` | Application | `SELECT DISTINCT application FROM deltav.flows_raw WHERE $__timeFilter(timestamp) AND location IN (${monitoring_location:sqlstring})` | yes / yes |

All three: `allValue='%'` (ClickHouse LIKE wildcard) so "All" passes through correctly.

**Panel 1 — Bandwidth over time, stacked by location** (hero, time series, stacked area, 24-wide)
```sql
SELECT
  $__timeInterval(timestamp) AS time,
  location,
  sum(num_bytes) * 8 / ($__interval_ms / 1000) AS bps
FROM deltav.flows_raw
WHERE $__timeFilter(timestamp)
  AND location IN (${monitoring_location:sqlstring})
  AND application IN (${application:sqlstring})
GROUP BY time, location
ORDER BY time
```
Unit: `bps`. Legend: `{{location}}`.

**Panel 2 — Top applications** (bar gauge, horizontal, top 10, 12-wide)
```sql
SELECT application, sum(num_bytes) AS bytes
FROM deltav.flows_raw
WHERE $__timeFilter(timestamp) AND location IN (${monitoring_location:sqlstring})
GROUP BY application ORDER BY bytes DESC LIMIT 10
```
Unit: bytes.

**Panel 3 — Top conversations** (table, 20 rows, 12-wide)
```sql
SELECT
  IPv6NumToString(src_address) AS "Source",
  IPv6NumToString(dst_address) AS "Destination",
  application AS "Application",
  sum(num_bytes) AS "Bytes",
  sum(num_packets) AS "Packets",
  count() AS "Flow records"
FROM deltav.flows_raw
WHERE $__timeFilter(timestamp)
  AND location IN (${monitoring_location:sqlstring})
  AND application IN (${application:sqlstring})
GROUP BY src_address, dst_address, application
ORDER BY "Bytes" DESC LIMIT 20
```

**Panel 4 — Per-exporter flow rate** (time series, one line per exporter, 12-wide)
```sql
SELECT
  $__timeInterval(timestamp) AS time,
  concat(exporter_node_foreign_source, '/', exporter_node_foreign_id) AS exporter,
  count() / ($__interval_ms / 1000) AS flows_per_sec
FROM deltav.flows_raw
WHERE $__timeFilter(timestamp)
  AND location IN (${monitoring_location:sqlstring})
  AND concat(exporter_node_foreign_source, '/', exporter_node_foreign_id) IN (${exporter_instance:sqlstring})
GROUP BY time, exporter ORDER BY time
```
Legend: `{{exporter}}`.

**Panel 5 — Protocol & DSCP mix** (pie chart with two queries, 12-wide)

Query A (protocol variant):
```sql
SELECT netflow_version, count() AS records
FROM deltav.flows_raw
WHERE $__timeFilter(timestamp) AND location IN (${monitoring_location:sqlstring})
GROUP BY netflow_version
```

Query B (DSCP class):
```sql
SELECT toString(dscp) AS dscp_class, count() AS records
FROM deltav.flows_raw
WHERE $__timeFilter(timestamp) AND location IN (${monitoring_location:sqlstring})
  AND dscp IS NOT NULL
GROUP BY dscp_class
```

Grafana pie chart configured with two data frames. If two-query rendering is visually cluttered, plan task 5 splits into two half-width panels instead.

**Panel 6 — Flow sources inventory** (table, 24-wide, mirrors snmp-overview's devices-monitored)
```sql
SELECT
  exporter_node_foreign_source AS "Foreign source",
  exporter_node_foreign_id AS "Foreign ID",
  location AS "Monitoring location",
  host AS "Exporter hostname",
  max(timestamp) AS "Last seen",
  count() AS "Flows in window"
FROM deltav.flows_raw
WHERE $__timeFilter(timestamp)
  AND location IN (${monitoring_location:sqlstring})
GROUP BY exporter_node_foreign_source, exporter_node_foreign_id, location, host
ORDER BY "Flows in window" DESC
```

**Layout** (Grafana grid, 24-column):
- Row 1: Panel 1 (24w × 8h)
- Row 2: Panel 2 (12w × 8h) | Panel 3 (12w × 8h)
- Row 3: Panel 4 (12w × 8h) | Panel 5 (12w × 8h)
- Row 4: Panel 6 (24w × 8h)

### E2E test changes

**File:** `opennms-container/delta-v/test-prometheus-writer-e2e.sh`.

**Step 11** inserted between Step 10 (l8opensim-lab SNMP assertion) and the final `exit 0`:

```bash
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
```

New variable near top of script: `CLICKHOUSE_QUERY_TIMEOUT=60`.

**Protocol count of 4 is only required under Scenarios A/B.** If plan's Task 1 falls back to Scenario C (single-protocol global), the assertion relaxes to `protos >= 1` — noted in the plan so the subagent implementing Task 10 adjusts appropriately.

## Edge cases & error handling

| Scenario | Handling |
|---|---|
| `grafana-clickhouse-datasource` plugin download fails on first boot | `GF_INSTALL_PLUGINS` retries automatically on restart. Healthcheck `start_period` bump to 60s (determined by plan Task 7) avoids readiness races. |
| l8opensim flow exporter config POST fails on some devices (network blip) | `l8opensim-provisioner` already exits non-zero on any POST failure via `post-each.sh`'s existing failure counting. Operator gets loud failure and re-runs `docker compose restart l8opensim-provisioner`. |
| Plan Task 1 discovers global-only flow config (Scenario C) | Protocol panel goes monochrome; E2E Step 11 assertion relaxes to `protos >= 1`. All other panels work unchanged. |
| Flow-enricher drops sFlow silently (regression of `project_flow_enricher_sflow_silent_drop` pattern) | Step 11's `count(DISTINCT netflow_version) = 4` assertion catches this loudly — exactly the kind of regression the #183 Step 10 assertion catches for SNMP. |
| ClickHouse data volume fills the grafanadata/clickhousedata Docker volume | `flows_raw` has 14-day TTL (`DELTAV_CLICKHOUSE_FLOWS_RAW_TTL_DAYS=14`). At low demo volumes this is negligible (<500 MB). Documented in PR description as a footprint consideration. |
| Operator runs only `--profile full` without `--profile metrics` | grafana + victoriametrics absent. Flow pipeline still runs (clickhouse + flow-enricher are in `[lite, full, metrics]`) but no dashboard. Existing expected behavior; PR description mentions. |
| Operator wants to disable the extra compose footprint | Trim `clickhouse`, `clickhouse-init`, `flow-enricher`, `flow-default-testnode-1`, `flow-sflow-testnode-1` profile lists back to `[full]` locally. README note covers this as an opt-out path. |

## Performance

| Resource | Cost |
|---|---|
| ClickHouse container memory (running + 14-day TTL flows) | ~300-500 MiB RAM. Negligible disk at demo volumes (<500 MB over 14 days). |
| flow-enricher memory | ~512 MiB RAM (JAVA_OPTS `-Xmx512m`). |
| Grafana plugin install (first boot) | ~10-15s download + unpack. Subsequent boots cache in the grafana volume. |
| First flow landing (stack up → Step 11 green) | Expected 90-180s. Plan's Task 8 measures empirically; if > 180s, diagnose before bumping `CLICKHOUSE_QUERY_TIMEOUT`. |
| Dashboard query cost | All 6 panels query `flows_raw` with time-range predicates + LowCardinality filters. <100ms per query at demo volumes. |

## Backward compatibility

| Change | Impact |
|---|---|
| Profile widening of `clickhouse` + 4 other services | **Behavior change at upgrade for `--profile lite` users.** Operators on `lite + metrics` now get ClickHouse + flow-enricher + 2 test-node containers they didn't have before. ~500 MiB RAM increase. Documented in PR description; opt-out via local profile edit if constrained. |
| New Grafana datasource + dashboard | Additive. Existing `snmp-overview` dashboard unaffected. Dashboard provisioning reload picks up the new file without operator action. |
| New `GF_INSTALL_PLUGINS` env var on grafana | Additive — Grafana auto-installs on first boot, caches thereafter. No impact on operators who already had the plugin manually installed (unusual). |
| New `l8opensim/flow-exporters.json` + `enable-flows.sh` | Additive — new files under the `opennms-container/delta-v/l8opensim/` directory introduced in #183. |
| E2E Step 11 added | Additive. Steps 1-10 still pass even if Step 11 fails. Stricter overall gate. |

## Testing strategy

### Automated (CI)

`bash opennms-container/delta-v/test-prometheus-writer-e2e.sh` runs Steps 1-11. All must pass for green.

- Steps 1-10 unchanged from #183.
- Step 11 new (l8opensim-lab flows in ClickHouse + 4-protocol coverage assertion).

### Manual (before merge)

1. `docker compose --profile lite --profile metrics up -d --build`.
2. Wait 2-3 minutes.
3. Open `http://localhost:13000/d/flows-overview`.
4. Confirm 3 template variables rendered: Monitoring location, Exporter, Application.
5. `Monitoring location` dropdown contains `Default` AND `l8opensim-lab`.
6. Filter to `l8opensim-lab`: Panels 1-5 populate. Panel 6 shows 20 exporters.
7. Filter to `Default`: Panels 1-5 show data from `flow-default-testnode-1` + `flow-sflow-testnode-1`. Panel 6 shows those 2 exporters.
8. Protocol/DSCP pie (Panel 5) shows 4 slices for protocol variant when "All" selected.
9. Regression check: `snmp-overview` dashboard still works end-to-end.

### Verification gates (plan-phase)

- Task 1 (API spike) — confirm l8opensim flow-config mechanism (A/B/C scenario).
- Task 7 (compose boot gate) — stack boots with full profile combination, clickhouse-init completes, flow-enricher reaches actuator UP, grafana plugin installed.
- Task 8 (first-flow latency) — empirical measurement for Step 11 timeout.
- Task 11 (full E2E gate) — all 11 steps green.

## Success criteria

- [ ] `docker compose --profile lite --profile metrics up -d` brings up the full pipeline including ClickHouse + flow-enricher + l8opensim flow exporters.
- [ ] `http://localhost:13000/d/flows-overview` renders 6 panels with data within 3 minutes of `up`.
- [ ] `monitoring_location` dropdown shows both `Default` and `l8opensim-lab`.
- [ ] E2E `test-prometheus-writer-e2e.sh` exits 0 with Step 11 reporting `count(DISTINCT netflow_version) = 4`.
- [ ] PR opened `--repo pbrane/delta-v --base develop` with title prefix `feat(flows):`.
- [ ] PR description calls out the profile-widening footprint change as an upgrade-time behavior change.
- [ ] Post-merge memory entries filed: `project_flows_visibility_done` (with merge SHA), update `project_grafana_dashboard_bundle` (note 2nd dashboard + ClickHouse datasource), update `project_clickhouse_phase2_done` (note operator-visible via flows-overview).

## After this PR (queued follow-ups)

- **PR B** (next-session): vmagent scraping of `/actuator/prometheus` on flow-enricher + flow-processing self-monitoring dashboard. Covers Q4 from brainstorming.
- **Forensic drilldown dashboard** — pick-a-time-window + drill into flow records table.
- **Protocol 5 panel split** — if two-query pie looks cluttered, split into two half-width panels (decided during plan Task 5).
- **Retire `flow-default-testnode-1` + `flow-sflow-testnode-1`** — possible once l8opensim can span multiple locations.
- **Add `location` to ClickHouse MVs** — future schema migration to speed up long-range queries.
- **Upstream contribution to labmonkeys-space/l8opensim** — if DSCP traffic variation proves monochrome, contribute a DSCP-varying traffic model.
- **SNMP trap + UDP syslog wiring** from l8opensim — two more PRs following the same l8opensim-integration pattern.
- **`project_flow_enricher_classpath_batch_cleanup`** — 148 exclusions sweep at next horizon release.
- **`project_sink_topic_rename`** — `OpenNMS.Sink.*` → `DeltaV.Sink.*` when decoupled from flow-enricher config.
