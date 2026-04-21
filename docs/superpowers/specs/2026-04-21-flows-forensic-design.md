# Flows Forensic Drilldown — Design Spec

**Date:** 2026-04-21
**Branch:** `feat/flows-forensic` (off develop tip `72c7a266d84`)
**Predecessor PRs:** delta-v#184 (Flows Overview dashboard + ClickHouse datasource), #145 (ClickHouse persistence), #181 (Grafana bundle)
**PR target:** pbrane/delta-v develop. **NEVER OpenNMS/opennms.** Title prefix: `feat(flows):`.

## Goal

Deliver a forensic flow investigation dashboard that complements the existing Flows Overview (PR #184). Operator clicks a row/cell in Flows Overview → forensic dashboard opens with that row's context pre-filled. Operator can also open it standalone, filter by hand, then self-pivot via clicks on top-N panels. A bounded raw-flow-records table at the bottom (last 60 minutes, LIMIT 1000) answers "exactly which flows."

This PR's value is the **investigative workflow**: top-N aggregations orient the operator quickly, drill-links collapse navigation friction, and the raw records table provides the smoking-gun evidence layer when aggregation isn't enough.

## Scope

### In Scope

- **New dashboard** `opennms-container/delta-v/grafana/dashboards/flows-forensic.json`:
  - UID: `flows-forensic`. Title: `Flows Forensic`.
  - 9 panels: volume timeline (stacked by app), top src IPs, top dst IPs, top conversations (with protocol+ports), L4 protocol mix, DSCP mix, TCP flag distribution, locality matrix, raw flow records. (Protocol and DSCP as two separate panels — combining different dimensions in one pie chart produces confusing tooltips and legends per review feedback.)
  - 9 template variables: `monitoring_location`, `exporter_instance`, `application`, `src_address`, `dst_address`, `l4_protocol`, `src_port`, `dst_port`, plus dashboard time range. All variables set `allowCustomValue: true` so operators can type IPs/ports not in the top-100 populating-query result.
  - Default time range: `Last 1 hour`. Refresh: `30s`.

- **Drill-OUT data links on `flows-overview.json`** (4 panels gain `links: [{...}]`):
  - Panel 2 (Top apps) → Forensic with `application` set
  - Panel 3 (Top conversations) → Forensic with `src_address` + `dst_address` + `application` set
  - Panel 4 (Per-exporter rate) → Forensic with `exporter_instance` set
  - Panel 6 (Flow sources inventory) → Forensic with `exporter_instance` + `monitoring_location` set
  - Panels 1 (bandwidth timeline) and 5 (Protocol mix) skipped — no clear drill target.

- **Self-pivot data links within forensic dashboard** (4 panels):
  - Top src IPs → click row → narrows `src_address`
  - Top dst IPs → click row → narrows `dst_address`
  - Top conversations → click row → narrows `src_address` + `dst_address` + `application`
  - Raw records → click row → narrows `src_address` + `dst_address`

- **Raw records bounded to last 60 minutes via panel-level `relativeTimeRange` override** (independent of dashboard time range — the records table stays cheap even if the dashboard time range is wider).

- **E2E Step 12** asserts forensic dashboard provisions cleanly (`GET /api/dashboards/uid/flows-forensic` returns title + 9 panels) AND verifies `flows-overview.json` regression (still loads after data-link edits).

### Out of Scope

- **PR B** (still queued from earlier brainstorm) — vmagent scraping `/actuator/prometheus` on flow-enricher + flow-processing self-monitoring dashboard.
- **sFlow silent-drop investigation** — tracked in `project_sflow_silent_drop_v2`. Independent investigation PR.
- **Dynamic Panel-1 application list** — current fixed pivot is acceptable; new apps from the classifier (PR #184 added 10) bucket into "unknown" until Panel 1 SQL is updated. Dynamic version is a follow-up.
- **ClickHouse MV `location` column** — out of scope for both this PR and PR #184. Future schema migration.
- **Geo / map panel** — locality matrix uses categorical fields, not coordinates. World-map panels deferred.
- **Per-VLAN drill / per-AS drill** — `vlan` + `src_as` columns exist in `flows_raw` but no panel uses them in v1.
- **Alert rules** — investigative dashboard, not alerting surface.
- **Saved investigation views** — Grafana has dashboard variables but doesn't bookmark filter combinations natively.
- **Browser-driven UX automation tests** — manual checklist only for the interactive drill mechanics. (No Playwright/Selenium per `project_l8opensim_mock_lab_done` precedent.)

## Background

PR #184 shipped the Flows Overview dashboard — a "weather map" that answers "what's happening on my network right now?" via 6 aggregated panels (bandwidth-by-location timeline, top apps, top conversations, per-exporter rate, protocol mix, flow sources inventory).

The deferred follow-up was a **forensic** dashboard answering the next-natural question: "What caused the spike at 2:30pm?" or "Why is l8opensim-aggregate suddenly emitting RoCE-v2 traffic?" That requires:
- A way to enter the dashboard with context pre-set (drill-OUT from Flows Overview panels)
- A way to keep narrowing without leaving the dashboard (self-pivot on top-N rows)
- A view of raw flow records (the smoking-gun layer) bounded to keep query cost predictable

This PR delivers all three via Grafana's standard data-link mechanism — no new datasource, no schema changes, no flow-enricher changes.

## Architecture

### Net new artifacts

One file added, one file edited:
- New: `opennms-container/delta-v/grafana/dashboards/flows-forensic.json` (~250-300 lines JSON).
- Edited: `opennms-container/delta-v/grafana/dashboards/flows-overview.json` — adds `"links"` arrays on 4 panels, ~60 lines added across the file.

No other changes. No compose changes (Grafana auto-reloads dashboards every 30s from the existing bind mount). No flow-enricher changes. No ClickHouse schema changes. No new datasource — reuses the `clickhouse` UID datasource provisioned in PR #184.

### Data flow

Identical to flows-overview: Grafana → grafana-clickhouse-datasource plugin → HTTP `clickhouse:8123` → `deltav.flows_raw`. Each panel is one ClickHouse SQL query against `flows_raw` with `WHERE` clauses driven by template variables.

### URL-param drill mechanism

Grafana's standard data-link URL form:
```
/d/flows-forensic?var-application=${__data.fields.Application}&var-monitoring_location=${monitoring_location:queryparam}&from=${__from}&to=${__to}
```

Variables not specified in the URL stay at their `includeAll` default ("All"). Time range carries via `from`/`to` URL params (Grafana standard). The `${__data.fields["..."]}` syntax extracts the clicked row/cell value.

### Self-pivot mechanism

Same URL-param form, but the data link's URL is `/d/flows-forensic?var-src_address=${__data.fields.Source}` (for the Top src IPs panel) — same dashboard, just with one variable narrowed. Grafana navigates in-place when the URL is the current dashboard. Existing variables in the URL are preserved (verified during plan-task verification gate).

### Template variables (9 total)

| Variable | Source query | Multi/All | Chains on |
|---|---|---|---|
| `monitoring_location` | `SELECT DISTINCT location FROM deltav.flows_raw WHERE $__timeFilter(timestamp)` | yes / yes | — |
| `exporter_instance` | `... WHERE $__timeFilter AND match(location, '${monitoring_location:regex}')` | yes / yes | location |
| `application` | `... WHERE $__timeFilter AND match(location, '${monitoring_location:regex}')` | yes / yes | location |
| `l4_protocol` | static custom: `6=TCP`, `17=UDP`, `1=ICMP`, `50=ESP` | yes / yes | — |
| `src_address` | `SELECT DISTINCT IPv6NumToString(src_address) FROM ... WHERE chained LIMIT 100` | yes / yes | location, exporter |
| `dst_address` | similar to src_address | yes / yes | location, exporter |
| `src_port` | `SELECT DISTINCT src_port FROM ... WHERE chained AND src_port IS NOT NULL LIMIT 100` | yes / yes | location, exporter |
| `dst_port` | similar | yes / yes | location, exporter |

Variables that don't chain on time (`monitoring_location`, `l4_protocol`) pull from the dashboard's full time range. Variables that chain narrow as filters tighten — keeps dropdowns small even at high cardinality.

**All variables set `allowCustomValue: true`** (Grafana 11+ feature). When an operator needs to filter by an IP or port not in the top-100 populating-query result, they can type or paste it into the dropdown and the WHERE clause's `match(field, :regex)` or `IN (:csv)` picks it up. Mitigates the `LIMIT 100` tradeoff in the populating queries.

### Filter strategy

Every panel's WHERE clause uses the same template-variable chain so all panels respect all filters consistently:

```sql
WHERE $__timeFilter(timestamp)
  AND match(location, '${monitoring_location:regex}')
  AND match(concat(exporter_node_foreign_source, '/', exporter_node_foreign_id), '${exporter_instance:regex}')
  AND match(application, '${application:regex}')
  AND (length('${l4_protocol:csv}') = 0 OR protocol IN (${l4_protocol:csv}))
  AND match(IPv6NumToString(src_address), '${src_address:regex}')
  AND match(IPv6NumToString(dst_address), '${dst_address:regex}')
  AND (length('${src_port:csv}') = 0 OR src_port IN (${src_port:csv}))
  AND (length('${dst_port:csv}') = 0 OR dst_port IN (${dst_port:csv}))
```

The `match() + :regex` pattern (per `feedback_grafana_clickhouse_dashboard_patterns`) handles "All" cleanly. The `IN (...)` for ports/protocol uses the `:csv` formatter since they're numeric.

This shared `WHERE` is referred to as `${WHERE}` in the panel SQL below.

## Panel design (9 panels)

24-column Grafana grid layout (9 panels):

- Row 1 (y=0, h=8): Panel 1 — Volume timeline (24w)
- Row 2 (y=8, h=8): Panel 2 — Top src IPs (12w) | Panel 3 — Top dst IPs (12w)
- Row 3 (y=16, h=8): Panel 4 — Top conversations (12w) | Panel 5 — L4 protocol mix (12w)
- Row 4 (y=24, h=8): Panel 6 — DSCP mix (12w) | Panel 7 — TCP flag distribution (12w)
- Row 5 (y=32, h=8): Panel 8 — Locality matrix (24w)
- Row 6 (y=40, h=12): Panel 9 — Raw flow records (24w)

Total dashboard height: ~52 rows × 30px = ~1560px. Long but matches forensic intent (scroll to records).

### Panel 1 — Volume timeline, stacked by application (24w × 8h, hero)

```sql
SELECT
  $__timeInterval(timestamp) AS time,
  sumIf(num_bytes, application = 'HTTPS') * 8 / ($__interval_ms / 1000) AS "HTTPS",
  sumIf(num_bytes, application = 'iSCSI') * 8 / ($__interval_ms / 1000) AS "iSCSI",
  sumIf(num_bytes, application = 'NFS')   * 8 / ($__interval_ms / 1000) AS "NFS",
  sumIf(num_bytes, application = 'SMB')   * 8 / ($__interval_ms / 1000) AS "SMB",
  sumIf(num_bytes, application = 'RoCE-v2') * 8 / ($__interval_ms / 1000) AS "RoCE-v2",
  sumIf(num_bytes, application = 'unknown') * 8 / ($__interval_ms / 1000) AS "unknown"
FROM deltav.flows_raw WHERE ${WHERE} GROUP BY time ORDER BY time
```

Wide-format pivot per `feedback_grafana_clickhouse_dashboard_patterns`. Pre-known apps; new ones land in "unknown" until Panel-1 SQL is updated. Acceptable trade — flows-overview's bandwidth-by-location panel handles dynamic locations differently because there are only 2 of them.

Unit: `bps`. Stacked area, fillOpacity 40.

### Panel 2 — Top src IPs (12w × 8h, table)

```sql
SELECT IPv6NumToString(src_address) AS "Source",
       sum(num_bytes) AS "Bytes",
       sum(num_packets) AS "Packets",
       count() AS "Flow records"
FROM deltav.flows_raw WHERE ${WHERE}
GROUP BY src_address
ORDER BY "Bytes" DESC LIMIT 20
```

Self-pivot data link: clicking a row sets `var-src_address=${__data.fields.Source}`. Field overrides: Bytes → unit `bytes`, Packets / Flow records → unit `short`.

### Panel 3 — Top dst IPs (12w × 8h, table)

Symmetric to Panel 2 with `dst_address`. Self-pivot sets `var-dst_address`.

### Panel 4 — Top conversations (12w × 8h, table — wider columns than flows-overview's version)

```sql
SELECT IPv6NumToString(src_address) AS "Source",
       src_port AS "Src port",
       IPv6NumToString(dst_address) AS "Destination",
       dst_port AS "Dst port",
       application AS "App",
       CASE protocol WHEN 6 THEN 'TCP' WHEN 17 THEN 'UDP' WHEN 1 THEN 'ICMP'
                     WHEN 50 THEN 'ESP' ELSE concat('proto=', toString(protocol)) END AS "L4",
       sum(num_bytes) AS "Bytes",
       count() AS "Records"
FROM deltav.flows_raw WHERE ${WHERE}
GROUP BY src_address, src_port, dst_address, dst_port, application, protocol
ORDER BY "Bytes" DESC LIMIT 20
```

Self-pivot data link: clicking a row sets `var-src_address` + `var-dst_address` + `var-application`.

### Panel 5 — L4 protocol mix (12w × 8h, donut)

```sql
SELECT
  countIf(protocol = 6)  AS "TCP",
  countIf(protocol = 17) AS "UDP",
  countIf(protocol = 1)  AS "ICMP",
  countIf(protocol = 50) AS "ESP",
  countIf(protocol NOT IN (1, 6, 17, 50) AND protocol IS NOT NULL) AS "Other"
FROM deltav.flows_raw WHERE ${WHERE}
```

### Panel 6 — DSCP mix (12w × 8h, donut)

```sql
SELECT
  countIf(dscp = 0)  AS "BE (0)",
  countIf(dscp = 8)  AS "CS1 (8)",
  countIf(dscp = 10) AS "AF11 (10)",
  countIf(dscp = 26) AS "AF31 (26)",
  countIf(dscp = 46) AS "EF (46)",
  countIf(dscp NOT IN (0, 8, 10, 26, 46) AND dscp IS NOT NULL) AS "Other DSCP"
FROM deltav.flows_raw WHERE ${WHERE} AND dscp IS NOT NULL
```

Kept as two separate panels (not combined) — per review feedback, combining two different aggregation dimensions in a single pie chart produces confusing tooltips and legends. Two donuts side-by-side read cleanly.

### Panel 7 — TCP flag distribution (12w × 8h, bar chart)

Only meaningful for TCP-filtered queries but works without:
```sql
SELECT
  countIf(bitTest(toUInt8(tcp_flags), 1)) AS "SYN",
  countIf(bitTest(toUInt8(tcp_flags), 4)) AS "ACK",
  countIf(bitTest(toUInt8(tcp_flags), 2)) AS "RST",
  countIf(bitTest(toUInt8(tcp_flags), 0)) AS "FIN",
  countIf(bitTest(toUInt8(tcp_flags), 3)) AS "PSH",
  countIf(bitTest(toUInt8(tcp_flags), 5)) AS "URG",
  countIf(bitTest(toUInt8(tcp_flags), 6)) AS "ECE",
  countIf(bitTest(toUInt8(tcp_flags), 7)) AS "CWR"
FROM deltav.flows_raw WHERE ${WHERE} AND protocol = 6 AND tcp_flags IS NOT NULL
```

TCP flag bit positions per RFC 793. Wide-pivot for bar chart. Empty when no TCP traffic in filter.

### Panel 8 — Locality matrix (24w × 8h, table)

```sql
SELECT src_locality AS "Src locality",
       dst_locality AS "Dst locality",
       count() AS "Flow records",
       sum(num_bytes) AS "Bytes"
FROM deltav.flows_raw WHERE ${WHERE}
GROUP BY src_locality, dst_locality
ORDER BY "Bytes" DESC
```

Field overrides: Bytes → `bytes`, Flow records → `short`.

### Panel 9 — Raw flow records (24w × 12h, table, panel-level time override)

`relativeTimeRange: { from: 3600, to: 0 }` = last 60 minutes regardless of dashboard time.

```sql
SELECT
  formatDateTime(timestamp, '%Y-%m-%d %H:%i:%S') AS "Time",
  CASE protocol WHEN 6 THEN 'TCP' WHEN 17 THEN 'UDP' WHEN 1 THEN 'ICMP'
                WHEN 50 THEN 'ESP' ELSE toString(protocol) END AS "L4",
  IPv6NumToString(src_address) AS "Src",
  src_port AS "Sport",
  IPv6NumToString(dst_address) AS "Dst",
  dst_port AS "Dport",
  application AS "App",
  num_bytes AS "Bytes",
  num_packets AS "Pkts",
  exporter_node_foreign_id AS "Exporter",
  netflow_version AS "Wire fmt"
FROM deltav.flows_raw
WHERE timestamp >= now() - INTERVAL 60 MINUTE
  AND match(location, '${monitoring_location:regex}')
  AND match(application, '${application:regex}')
  AND (length('${l4_protocol:csv}') = 0 OR protocol IN (${l4_protocol:csv}))
  AND match(IPv6NumToString(src_address), '${src_address:regex}')
  AND match(IPv6NumToString(dst_address), '${dst_address:regex}')
ORDER BY timestamp DESC
LIMIT 1000
```

Self-pivot data link: clicking a row sets `var-src_address` + `var-dst_address`. Field overrides on Bytes → `bytes`, Pkts → `short`.

## Drill-link wiring

### Drill-OUT — modifications to `flows-overview.json`

Each affected panel gains a `links` array.

**Panel 2 (Top apps) on flows-overview** — clicking a bar drills into Forensic with that app pre-selected:
```json
"links": [
  {
    "title": "Drill into forensic view",
    "url": "/d/flows-forensic?var-application=${__data.fields.application}&var-monitoring_location=${monitoring_location:queryparam}&from=${__from}&to=${__to}",
    "targetBlank": false
  }
]
```

**Panel 3 (Top conversations) on flows-overview** — clicking a row drills with src+dst+app:
```json
"links": [
  {
    "title": "Drill into forensic view",
    "url": "/d/flows-forensic?var-src_address=${__data.fields.Source}&var-dst_address=${__data.fields.Destination}&var-application=${__data.fields.Application}&var-monitoring_location=${monitoring_location:queryparam}&from=${__from}&to=${__to}",
    "targetBlank": false
  }
]
```

**Panel 4 (Per-exporter rate) on flows-overview** — clicking a series drills with exporter selected:
```json
"links": [
  {
    "title": "Drill into forensic view",
    "url": "/d/flows-forensic?var-exporter_instance=${__field.labels.exporter}&var-monitoring_location=${monitoring_location:queryparam}&from=${__from}&to=${__to}",
    "targetBlank": false
  }
]
```

**Panel 6 (Flow sources inventory) on flows-overview** — clicking a row drills with both exporter + location pre-set:
```json
"links": [
  {
    "title": "Drill into forensic view",
    "url": "/d/flows-forensic?var-exporter_instance=${__data.fields[\"Foreign source\"]}/${__data.fields[\"Foreign ID\"]}&var-monitoring_location=${__data.fields[\"Monitoring location\"]}&from=${__from}&to=${__to}",
    "targetBlank": false
  }
]
```

(The `${__data.fields["Foreign source"]}/${__data.fields["Foreign ID"]}` constructs the same `concat(...)` string the forensic dashboard's `exporter_instance` variable expects.)

### Self-pivot — links inline in `flows-forensic.json` panels

**Critical**: Grafana data-link URLs **replace** the query string on navigation — they don't merge with existing `var-*` params. To safely accumulate filters (click Top src IPs, then click Top dst IPs, and preserve both), every self-pivot URL must explicitly re-pass all currently-selected variables via `${variable_name}` Grafana syntax. Each URL below carries the full 9-variable set: the one being newly narrowed plus the other eight preserved from current state.

**Panel 2 (Top src IPs)** — clicking a row narrows `src_address` while preserving all others:
```json
"links": [
  {
    "title": "Pivot: filter to this source",
    "url": "/d/flows-forensic?var-src_address=${__data.fields.Source}&var-monitoring_location=${monitoring_location:queryparam}&var-exporter_instance=${exporter_instance:queryparam}&var-application=${application:queryparam}&var-l4_protocol=${l4_protocol:queryparam}&var-dst_address=${dst_address:queryparam}&var-src_port=${src_port:queryparam}&var-dst_port=${dst_port:queryparam}&from=${__from}&to=${__to}",
    "targetBlank": false
  }
]
```

**Panel 3 (Top dst IPs)** — symmetric pattern, narrows `dst_address`:
```json
"links": [
  {
    "title": "Pivot: filter to this destination",
    "url": "/d/flows-forensic?var-dst_address=${__data.fields.Destination}&var-monitoring_location=${monitoring_location:queryparam}&var-exporter_instance=${exporter_instance:queryparam}&var-application=${application:queryparam}&var-l4_protocol=${l4_protocol:queryparam}&var-src_address=${src_address:queryparam}&var-src_port=${src_port:queryparam}&var-dst_port=${dst_port:queryparam}&from=${__from}&to=${__to}",
    "targetBlank": false
  }
]
```

**Panel 4 (Top conversations)** — clicking narrows src+dst+application together:
```json
"links": [
  {
    "title": "Pivot: filter to this conversation",
    "url": "/d/flows-forensic?var-src_address=${__data.fields.Source}&var-dst_address=${__data.fields.Destination}&var-application=${__data.fields.App}&var-monitoring_location=${monitoring_location:queryparam}&var-exporter_instance=${exporter_instance:queryparam}&var-l4_protocol=${l4_protocol:queryparam}&var-src_port=${src_port:queryparam}&var-dst_port=${dst_port:queryparam}&from=${__from}&to=${__to}",
    "targetBlank": false
  }
]
```

**Panel 9 (Raw records)** — clicking narrows src+dst:
```json
"links": [
  {
    "title": "Pivot: filter to this src↔dst pair",
    "url": "/d/flows-forensic?var-src_address=${__data.fields.Src}&var-dst_address=${__data.fields.Dst}&var-monitoring_location=${monitoring_location:queryparam}&var-exporter_instance=${exporter_instance:queryparam}&var-application=${application:queryparam}&var-l4_protocol=${l4_protocol:queryparam}&var-src_port=${src_port:queryparam}&var-dst_port=${dst_port:queryparam}&from=${__from}&to=${__to}",
    "targetBlank": false
  }
]
```

The `${var:queryparam}` formatter correctly serializes multi-value variables (e.g., multiple locations selected) as repeated `var-X=A&var-X=B` URL params. Used throughout for consistency.

### Variable preservation

When Grafana navigates from `/d/flows-overview?var-X=...` to `/d/flows-forensic?var-X=...`, only variables present as `var-...` URL params are set. Others fall to their `includeAll` default ("All"). This is the desired behavior — drilling inherits CONTEXT, not full filter state.

## Edge cases & error handling

| Scenario | Handling |
|---|---|
| Operator opens dashboard with no URL params | All template vars default to "All". 60-min raw records table renders normally. Aggregated panels query 1-hour window with no filters; ClickHouse handles fine at demo volumes. |
| Operator drills with `var-application=HTTPS` but HTTPS has zero rows in the time window | Panels render empty with Grafana's "No data" indicator. Not a failure — operator narrows time range or clears filter. |
| Operator clicks a Top src IPs row that's already in the `src_address` filter | URL becomes `?var-src_address=X&var-src_address=X` (Grafana-deduplicated to single value). No-op. |
| Operator clicks Top src IPs then Top dst IPs in succession | Second URL preserves first's `var-src_address` AND adds `var-dst_address`. Verified during plan-task gate; if Grafana clears existing vars on click, fallback in plan = data link URL includes ALL current variables explicitly. |
| Panel 1's hardcoded app list misses a new app from the classifier | New flows bucket into "unknown" series. Operator sees the spike in the unknown band. Documented in Panel 1's description with an `// add new apps as they appear` comment in the SQL. |
| `length('${l4_protocol:csv}')` returns 0 when "All" selected | Filter falls back to allowing all protocols (the short-circuit `length=0 OR ...` clause). |
| Raw records table query exceeds 1000 rows | LIMIT 1000 caps it. Operator narrows time range or filters. UI shows "More than 1000 rows; refine filters." (Grafana table panel native behavior.) |

## Performance

| Resource | Cost |
|---|---|
| Dashboard panels per query | <100ms at demo volumes (~1500 flows/sec for 1-hour = 5.4M rows; ClickHouse partition-pruning + ORDER BY clause = sub-second). |
| Raw records table query | Bounded to 60 min × LIMIT 1000 — fast regardless of total table size. |
| Template-variable populating queries | LIMIT 100 each; <50ms. |
| Browser dashboard load time | ~2s for first render (9 panels parallel-query; bounded by Grafana's default concurrent-query limit of 6). Subsequent variable-change re-render: ~500ms. |
| ClickHouse memory | Each panel query: ~10-50 MB peak. 8 panels parallel: bounded by Grafana's per-dashboard concurrent-query limit (default 6). |

## Backward compatibility

| Change | Impact |
|---|---|
| New `flows-forensic.json` dashboard | Additive. snmp-overview / flows-overview unaffected. Auto-loaded by existing default.yml provisioning provider. |
| `links: [...]` arrays added to 4 flows-overview panels | Additive within the JSON. Existing flows-overview behavior unchanged for operators who don't click the new links. Tested via Step 12's regression check. |
| E2E Step 12 added | Additive. Steps 1-11 still pass even if Step 12 fails. Stricter overall gate. |
| No compose changes | Zero impact on services, profiles, or images. |

## Testing strategy

### Manual (primary)

Drill links + self-pivot are fundamentally interactive UX; can't be fully automated without browser-driving (out of scope per `project_l8opensim_mock_lab_done`'s "no Playwright" pattern).

Manual checklist:

1. Open `http://localhost:13000/d/flows-overview` → wait for data populate.
2. Click a row in **Top conversations** → destination is `http://localhost:13000/d/flows-forensic?...` with `var-src_address`, `var-dst_address`, `var-application` populated → forensic dashboard renders with those three filters applied.
3. Click a bar in **Top apps** → forensic opens with `var-application` only.
4. Click a series in **Per-exporter rate** → forensic opens with `var-exporter_instance` only.
5. Click a row in **Flow sources inventory** → forensic opens with `var-exporter_instance` + `var-monitoring_location`.
6. From forensic, click a row in **Top src IPs** → URL updates with `var-src_address`, dashboard re-renders with that filter narrowing all panels.
7. Same for **Top dst IPs**, **Top conversations**, **Raw records** self-pivots.
8. Open `http://localhost:13000/d/flows-forensic` directly (no URL params) → all 8 panels render with "All" filters; raw records table shows last 60 minutes only even if dashboard time = "Last 24 hours."
9. Verify `flows-overview` regression — its own filter dropdowns still work after data-link addition.

### Automated (E2E Step 12)

Extends existing `test-prometheus-writer-e2e.sh`. Asserts the dashboard provisions cleanly and has the expected shape. Doesn't exercise drill-links (needs browser) but proves the JSON validates and Grafana loads it:

```bash
# ── Step 12: Verify flows-forensic dashboard provisioned with 8 panels ────────
echo "==> Step 12: Verify flows-forensic dashboard provisioned"
dash=$(curl -sf -u "admin:${GF_PASS}" \
       http://localhost:13000/api/dashboards/uid/flows-forensic 2>/dev/null || true)
if ! echo "$dash" | grep -q '"title":"Flows Forensic"'; then
    echo "FAIL: flows-forensic dashboard not loaded"
    echo "Response: $dash"
    exit 1
fi
panels=$(echo "$dash" | python3 -c \
         'import json,sys; d=json.load(sys.stdin); print(len(d.get("dashboard",{}).get("panels",[])))')
if [ "$panels" != "9" ]; then
    echo "FAIL: flows-forensic has ${panels} panels, expected 9"
    exit 1
fi
echo "==> flows-forensic loaded with 9 panels"

# Sanity check that flows-overview still loads (regression for the data-link edits)
dash2=$(curl -sf -u "admin:${GF_PASS}" \
        http://localhost:13000/api/dashboards/uid/flows-overview 2>/dev/null || true)
if ! echo "$dash2" | grep -q '"title":"Flows Overview"'; then
    echo "FAIL: flows-overview dashboard regression after data-link edits"
    exit 1
fi
echo "==> flows-overview regression OK"
```

Inserted between Step 11 (l8opensim-lab flows in ClickHouse) and the final `echo "==> ALL ASSERTIONS PASSED"`.

### Plan-phase verification gates (analogous to PR #184's pattern)

- Task 1 spike: validate JSON syntax + dashboard structural assertions in Python before commit.
- Task 4 verification gate: boot stack, GET each dashboard via Grafana API, confirm both load + 8/6 panels respectively. Manual drill-link click-through.
- Task 5 verification gate: full E2E (12 steps green).

### What we're NOT testing (intentional scope cuts)

- Self-pivot URL parameter substitution at runtime — that's Grafana's job. If the link's URL template is structurally correct, Grafana handles substitution.
- Performance at high flow volumes — l8opensim's ~1500 flows/sec is enough to populate the demo; real-world tuning is a follow-up.
- Cross-browser data link behavior — manual verification on the user's own browser is sufficient.

## Risks for plan execution

1. **Panel 1's hardcoded application list will drift.** The wide-format pivot needs explicit `sumIf(application = 'X')` per app. New apps from the classifier (PR #184 added 10) get bucketed into `"unknown"` until Panel 1 is updated. Mitigation: documented as a known limitation in the panel description; future enhancement could be a 2-step query (template-variable-driven app list → pivot SQL generated from it). Acceptable for v1.

2. **TCP flag bit positions in Panel 6 require validation.** Per RFC 793 the bits are SYN=1, FIN=0, RST=2, PSH=3, ACK=4, URG=5, ECE=6, CWR=7. ClickHouse `bitTest()` is 0-indexed. The mapping in Panel 6 follows this convention. Plan Task 1 spike validates by emitting known TCP flag combinations and querying them back — if any panel column returns wrong counts, the bit-position table needs adjustment.

3. **`${__data.fields[...]}` URL-template syntax in Grafana 11.4.0.** Documented for 9.5+ but version-specific quirks possible. Plan Task 4 (verification gate) clicks each drill-link manually before commit; if any link doesn't substitute variables correctly, fall back to `${__cell:N}` (column-index form) which is more universally supported.

4. **Self-pivot variable accumulation — RESOLVED by explicit URL re-passing.** Grafana data-link URLs replace the query string on navigation rather than merging. Each self-pivot URL in Section "Drill-link wiring" explicitly re-passes all 8 other template variables via `${var_name:queryparam}` so subsequent clicks accumulate filters correctly. Plan Task 4 verification still clicks each link manually to confirm the pattern works on Grafana 11.4.0.

5. **High-cardinality template variable queries — MITIGATED by `allowCustomValue: true`.** Populating queries use `LIMIT 100`; if an operator needs to filter by an IP/port outside the top-100, they can type or paste it into the variable dropdown (Grafana 11+ feature). The WHERE clause's `match(field, :regex)` or `IN (:csv)` picks up the typed value. Still worth noting: operators may not discover this UX without documentation in the dashboard description panel.

## Success criteria

- [ ] `bash opennms-container/delta-v/test-prometheus-writer-e2e.sh` exits 0 with all 12 steps green.
- [ ] Manual drill-link verification: each of the 4 flows-overview drill-OUT links opens flows-forensic with expected variables pre-set.
- [ ] Manual self-pivot verification: each of the 4 flows-forensic self-pivot links updates the URL and re-renders panels.
- [ ] flows-overview regression: existing dropdowns + panels still work after data-link edits.
- [ ] PR opened `--repo pbrane/delta-v --base develop` with title prefix `feat(flows):`.

## After this PR (queued follow-ups)

- **PR B** — vmagent scraping `/actuator/prometheus` on flow-enricher + flow-processing self-monitoring dashboard.
- **sFlow silent-drop investigation** — `project_sflow_silent_drop_v2` open issue.
- **Dynamic Panel-1 app list** — replace hardcoded `sumIf(application = 'X')` with a template-variable-driven generator.
- **ClickHouse MV `location` column** — schema migration to enable longer-range dashboard queries via MVs.
- **Per-VLAN / Per-AS panels** — when operator demand emerges.
- **Geo / map panel** — requires Geo-IP enrichment of `flows_raw` (provisiond-side or flow-enricher-side).
