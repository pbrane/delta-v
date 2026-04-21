# Flows Forensic Drilldown Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver a forensic flow investigation dashboard (`flows-forensic`) sibling to `flows-overview` (PR #184), with 9 panels and bidirectional data links (drill-OUT from flows-overview, self-pivot within forensic). Raw flow records bounded to last 60 minutes via panel-level time override.

**Architecture:** One new Grafana dashboard JSON file + `"links"` array additions on 4 existing flows-overview panels. All queries against the existing `clickhouse` datasource from PR #184. No compose changes, no flow-enricher changes, no ClickHouse schema changes. Every self-pivot URL explicitly re-passes all 8 other template variables via `${var:queryparam}` to prevent filter-accumulation loss on click.

**Tech Stack:** Grafana 11.4.0 with `grafana-clickhouse-datasource` plugin, ClickHouse 25.8 (`deltav.flows_raw` table), bash + curl for E2E assertions, python3 for JSON validation.

**Branch:** `feat/flows-forensic` (already created off develop tip `72c7a266d84`, with the spec committed as `d9c8d20dce2` + revision `47d3477cf23`).

**Spec reference:** `docs/superpowers/specs/2026-04-21-flows-forensic-design.md`.

**PR target:** `pbrane/delta-v` `develop`. Title prefix `feat(flows):`. **NEVER `OpenNMS/opennms`.**

**No Java rebuild needed** — pure Grafana JSON + shell script edits.

---

## Pre-flight (one-time per session)

- [ ] **Verify branch and clean working tree**

Run: `git branch --show-current && git status --short`
Expected:
```
feat/flows-forensic
```
(no dirty files; if `provisiond-overlay/etc/imports/*.xml` shows drift, run `git checkout -- opennms-container/delta-v/provisiond-overlay/etc/imports/` per `feedback_provisiond_requisition_drift`.)

- [ ] **Verify spec is committed**

Run: `git log --oneline -3`
Expected: HEAD is `47d3477cf23 docs(spec): forensic drilldown spec revision per review feedback`.

- [ ] **Verify Docker Desktop is running and stack is torn down**

Run: `docker ps --format '{{.Names}}' | grep delta-v | head -5`
Expected: empty. If anything's up, tear down: `cd opennms-container/delta-v && docker compose --profile lite --profile metrics down -v --remove-orphans`.

If any preflight check fails, stop and resolve before Task 1.

---

### Task 1: Author `flows-forensic.json` dashboard

**Files:**
- Create: `opennms-container/delta-v/grafana/dashboards/flows-forensic.json`

Single large JSON file with 9 panels, 9 template variables, and 4 self-pivot data links. Paste the content verbatim from the block below.

- [ ] **Step 1: Create the dashboard file**

Create `opennms-container/delta-v/grafana/dashboards/flows-forensic.json` with this exact content:

```json
{
  "uid": "flows-forensic",
  "title": "Flows Forensic",
  "schemaVersion": 39,
  "version": 1,
  "refresh": "30s",
  "time": { "from": "now-1h", "to": "now" },
  "tags": ["delta-v", "flows", "forensic"],
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
        "allValue": ".*",
        "allowCustomValue": true,
        "current": { "selected": true, "text": ["All"], "value": ["$__all"] }
      },
      {
        "name": "exporter_instance",
        "label": "Exporter",
        "type": "query",
        "datasource": { "type": "grafana-clickhouse-datasource", "uid": "clickhouse" },
        "query": "SELECT DISTINCT concat(exporter_node_foreign_source, '/', exporter_node_foreign_id) FROM deltav.flows_raw WHERE $__timeFilter(timestamp) AND match(location, '${monitoring_location:regex}')",
        "refresh": 1,
        "multi": true,
        "includeAll": true,
        "allValue": ".*",
        "allowCustomValue": true,
        "current": { "selected": true, "text": ["All"], "value": ["$__all"] }
      },
      {
        "name": "application",
        "label": "Application",
        "type": "query",
        "datasource": { "type": "grafana-clickhouse-datasource", "uid": "clickhouse" },
        "query": "SELECT DISTINCT application FROM deltav.flows_raw WHERE $__timeFilter(timestamp) AND match(location, '${monitoring_location:regex}')",
        "refresh": 1,
        "multi": true,
        "includeAll": true,
        "allValue": ".*",
        "allowCustomValue": true,
        "current": { "selected": true, "text": ["All"], "value": ["$__all"] }
      },
      {
        "name": "l4_protocol",
        "label": "L4 protocol",
        "type": "custom",
        "query": "TCP : 6, UDP : 17, ICMP : 1, ESP : 50",
        "multi": true,
        "includeAll": true,
        "allValue": "",
        "allowCustomValue": true,
        "current": { "selected": true, "text": ["All"], "value": ["$__all"] }
      },
      {
        "name": "src_address",
        "label": "Source IP",
        "type": "query",
        "datasource": { "type": "grafana-clickhouse-datasource", "uid": "clickhouse" },
        "query": "SELECT DISTINCT IPv6NumToString(src_address) FROM deltav.flows_raw WHERE $__timeFilter(timestamp) AND match(location, '${monitoring_location:regex}') AND match(concat(exporter_node_foreign_source, '/', exporter_node_foreign_id), '${exporter_instance:regex}') LIMIT 100",
        "refresh": 1,
        "multi": true,
        "includeAll": true,
        "allValue": ".*",
        "allowCustomValue": true,
        "current": { "selected": true, "text": ["All"], "value": ["$__all"] }
      },
      {
        "name": "dst_address",
        "label": "Destination IP",
        "type": "query",
        "datasource": { "type": "grafana-clickhouse-datasource", "uid": "clickhouse" },
        "query": "SELECT DISTINCT IPv6NumToString(dst_address) FROM deltav.flows_raw WHERE $__timeFilter(timestamp) AND match(location, '${monitoring_location:regex}') AND match(concat(exporter_node_foreign_source, '/', exporter_node_foreign_id), '${exporter_instance:regex}') LIMIT 100",
        "refresh": 1,
        "multi": true,
        "includeAll": true,
        "allValue": ".*",
        "allowCustomValue": true,
        "current": { "selected": true, "text": ["All"], "value": ["$__all"] }
      },
      {
        "name": "src_port",
        "label": "Source port",
        "type": "query",
        "datasource": { "type": "grafana-clickhouse-datasource", "uid": "clickhouse" },
        "query": "SELECT DISTINCT toString(src_port) FROM deltav.flows_raw WHERE $__timeFilter(timestamp) AND src_port IS NOT NULL AND match(location, '${monitoring_location:regex}') LIMIT 100",
        "refresh": 1,
        "multi": true,
        "includeAll": true,
        "allValue": "",
        "allowCustomValue": true,
        "current": { "selected": true, "text": ["All"], "value": ["$__all"] }
      },
      {
        "name": "dst_port",
        "label": "Destination port",
        "type": "query",
        "datasource": { "type": "grafana-clickhouse-datasource", "uid": "clickhouse" },
        "query": "SELECT DISTINCT toString(dst_port) FROM deltav.flows_raw WHERE $__timeFilter(timestamp) AND dst_port IS NOT NULL AND match(location, '${monitoring_location:regex}') LIMIT 100",
        "refresh": 1,
        "multi": true,
        "includeAll": true,
        "allValue": "",
        "allowCustomValue": true,
        "current": { "selected": true, "text": ["All"], "value": ["$__all"] }
      }
    ]
  },
  "panels": [
    {
      "id": 1,
      "type": "timeseries",
      "title": "Bandwidth over time (stacked by application)",
      "gridPos": { "h": 8, "w": 24, "x": 0, "y": 0 },
      "datasource": { "type": "grafana-clickhouse-datasource", "uid": "clickhouse" },
      "fieldConfig": {
        "defaults": { "unit": "bps", "custom": { "drawStyle": "line", "fillOpacity": 40, "stacking": { "mode": "normal" } } },
        "overrides": []
      },
      "targets": [
        {
          "refId": "A",
          "rawSql": "SELECT $__timeInterval(timestamp) AS time, sumIf(num_bytes, application = 'HTTPS') * 8 / ($__interval_ms / 1000) AS \"HTTPS\", sumIf(num_bytes, application = 'HTTP') * 8 / ($__interval_ms / 1000) AS \"HTTP\", sumIf(num_bytes, application = 'iSCSI') * 8 / ($__interval_ms / 1000) AS \"iSCSI\", sumIf(num_bytes, application = 'NFS') * 8 / ($__interval_ms / 1000) AS \"NFS\", sumIf(num_bytes, application = 'SMB') * 8 / ($__interval_ms / 1000) AS \"SMB\", sumIf(num_bytes, application = 'RoCE-v2') * 8 / ($__interval_ms / 1000) AS \"RoCE-v2\", sumIf(num_bytes, application = 'VXLAN') * 8 / ($__interval_ms / 1000) AS \"VXLAN\", sumIf(num_bytes, application = 'SSH') * 8 / ($__interval_ms / 1000) AS \"SSH\", sumIf(num_bytes, application = 'DNS') * 8 / ($__interval_ms / 1000) AS \"DNS\", sumIf(num_bytes, application NOT IN ('HTTPS','HTTP','iSCSI','NFS','SMB','RoCE-v2','VXLAN','SSH','DNS')) * 8 / ($__interval_ms / 1000) AS \"Other\" FROM deltav.flows_raw WHERE $__timeFilter(timestamp) AND match(location, '${monitoring_location:regex}') AND match(concat(exporter_node_foreign_source, '/', exporter_node_foreign_id), '${exporter_instance:regex}') AND match(application, '${application:regex}') AND (length('${l4_protocol:csv}') = 0 OR protocol IN (${l4_protocol:csv})) AND match(IPv6NumToString(src_address), '${src_address:regex}') AND match(IPv6NumToString(dst_address), '${dst_address:regex}') GROUP BY time ORDER BY time",
          "format": 1
        }
      ]
    },
    {
      "id": 2,
      "type": "table",
      "title": "Top source IPs",
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 8 },
      "datasource": { "type": "grafana-clickhouse-datasource", "uid": "clickhouse" },
      "fieldConfig": {
        "defaults": {},
        "overrides": [
          {"matcher": {"id": "byName", "options": "Bytes"}, "properties": [{"id": "unit", "value": "bytes"}]},
          {"matcher": {"id": "byName", "options": "Packets"}, "properties": [{"id": "unit", "value": "short"}]},
          {"matcher": {"id": "byName", "options": "Flow records"}, "properties": [{"id": "unit", "value": "short"}]}
        ]
      },
      "targets": [
        {
          "refId": "A",
          "rawSql": "SELECT IPv6NumToString(src_address) AS \"Source\", sum(num_bytes) AS \"Bytes\", sum(num_packets) AS \"Packets\", count() AS \"Flow records\" FROM deltav.flows_raw WHERE $__timeFilter(timestamp) AND match(location, '${monitoring_location:regex}') AND match(concat(exporter_node_foreign_source, '/', exporter_node_foreign_id), '${exporter_instance:regex}') AND match(application, '${application:regex}') AND (length('${l4_protocol:csv}') = 0 OR protocol IN (${l4_protocol:csv})) AND match(IPv6NumToString(src_address), '${src_address:regex}') AND match(IPv6NumToString(dst_address), '${dst_address:regex}') GROUP BY src_address ORDER BY \"Bytes\" DESC LIMIT 20",
          "format": 0
        }
      ],
      "links": [
        {
          "title": "Pivot: filter to this source",
          "url": "/d/flows-forensic?var-src_address=${__data.fields.Source}&var-monitoring_location=${monitoring_location:queryparam}&var-exporter_instance=${exporter_instance:queryparam}&var-application=${application:queryparam}&var-l4_protocol=${l4_protocol:queryparam}&var-dst_address=${dst_address:queryparam}&var-src_port=${src_port:queryparam}&var-dst_port=${dst_port:queryparam}&from=${__from}&to=${__to}",
          "targetBlank": false
        }
      ]
    },
    {
      "id": 3,
      "type": "table",
      "title": "Top destination IPs",
      "gridPos": { "h": 8, "w": 12, "x": 12, "y": 8 },
      "datasource": { "type": "grafana-clickhouse-datasource", "uid": "clickhouse" },
      "fieldConfig": {
        "defaults": {},
        "overrides": [
          {"matcher": {"id": "byName", "options": "Bytes"}, "properties": [{"id": "unit", "value": "bytes"}]},
          {"matcher": {"id": "byName", "options": "Packets"}, "properties": [{"id": "unit", "value": "short"}]},
          {"matcher": {"id": "byName", "options": "Flow records"}, "properties": [{"id": "unit", "value": "short"}]}
        ]
      },
      "targets": [
        {
          "refId": "A",
          "rawSql": "SELECT IPv6NumToString(dst_address) AS \"Destination\", sum(num_bytes) AS \"Bytes\", sum(num_packets) AS \"Packets\", count() AS \"Flow records\" FROM deltav.flows_raw WHERE $__timeFilter(timestamp) AND match(location, '${monitoring_location:regex}') AND match(concat(exporter_node_foreign_source, '/', exporter_node_foreign_id), '${exporter_instance:regex}') AND match(application, '${application:regex}') AND (length('${l4_protocol:csv}') = 0 OR protocol IN (${l4_protocol:csv})) AND match(IPv6NumToString(src_address), '${src_address:regex}') AND match(IPv6NumToString(dst_address), '${dst_address:regex}') GROUP BY dst_address ORDER BY \"Bytes\" DESC LIMIT 20",
          "format": 0
        }
      ],
      "links": [
        {
          "title": "Pivot: filter to this destination",
          "url": "/d/flows-forensic?var-dst_address=${__data.fields.Destination}&var-monitoring_location=${monitoring_location:queryparam}&var-exporter_instance=${exporter_instance:queryparam}&var-application=${application:queryparam}&var-l4_protocol=${l4_protocol:queryparam}&var-src_address=${src_address:queryparam}&var-src_port=${src_port:queryparam}&var-dst_port=${dst_port:queryparam}&from=${__from}&to=${__to}",
          "targetBlank": false
        }
      ]
    },
    {
      "id": 4,
      "type": "table",
      "title": "Top conversations",
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 16 },
      "datasource": { "type": "grafana-clickhouse-datasource", "uid": "clickhouse" },
      "fieldConfig": {
        "defaults": {},
        "overrides": [
          {"matcher": {"id": "byName", "options": "Bytes"}, "properties": [{"id": "unit", "value": "bytes"}]},
          {"matcher": {"id": "byName", "options": "Records"}, "properties": [{"id": "unit", "value": "short"}]}
        ]
      },
      "targets": [
        {
          "refId": "A",
          "rawSql": "SELECT IPv6NumToString(src_address) AS \"Source\", src_port AS \"Src port\", IPv6NumToString(dst_address) AS \"Destination\", dst_port AS \"Dst port\", application AS \"App\", CASE protocol WHEN 6 THEN 'TCP' WHEN 17 THEN 'UDP' WHEN 1 THEN 'ICMP' WHEN 50 THEN 'ESP' ELSE concat('proto=', toString(protocol)) END AS \"L4\", sum(num_bytes) AS \"Bytes\", count() AS \"Records\" FROM deltav.flows_raw WHERE $__timeFilter(timestamp) AND match(location, '${monitoring_location:regex}') AND match(concat(exporter_node_foreign_source, '/', exporter_node_foreign_id), '${exporter_instance:regex}') AND match(application, '${application:regex}') AND (length('${l4_protocol:csv}') = 0 OR protocol IN (${l4_protocol:csv})) AND match(IPv6NumToString(src_address), '${src_address:regex}') AND match(IPv6NumToString(dst_address), '${dst_address:regex}') GROUP BY src_address, src_port, dst_address, dst_port, application, protocol ORDER BY \"Bytes\" DESC LIMIT 20",
          "format": 0
        }
      ],
      "links": [
        {
          "title": "Pivot: filter to this conversation",
          "url": "/d/flows-forensic?var-src_address=${__data.fields.Source}&var-dst_address=${__data.fields.Destination}&var-application=${__data.fields.App}&var-monitoring_location=${monitoring_location:queryparam}&var-exporter_instance=${exporter_instance:queryparam}&var-l4_protocol=${l4_protocol:queryparam}&var-src_port=${src_port:queryparam}&var-dst_port=${dst_port:queryparam}&from=${__from}&to=${__to}",
          "targetBlank": false
        }
      ]
    },
    {
      "id": 5,
      "type": "piechart",
      "title": "L4 protocol mix",
      "gridPos": { "h": 8, "w": 12, "x": 12, "y": 16 },
      "datasource": { "type": "grafana-clickhouse-datasource", "uid": "clickhouse" },
      "options": { "legend": { "displayMode": "table", "placement": "right", "values": ["value", "percent"] }, "pieType": "donut" },
      "targets": [
        {
          "refId": "A",
          "rawSql": "SELECT countIf(protocol = 6) AS \"TCP\", countIf(protocol = 17) AS \"UDP\", countIf(protocol = 1) AS \"ICMP\", countIf(protocol = 50) AS \"ESP\", countIf(protocol NOT IN (1, 6, 17, 50) AND protocol IS NOT NULL) AS \"Other\" FROM deltav.flows_raw WHERE $__timeFilter(timestamp) AND match(location, '${monitoring_location:regex}') AND match(concat(exporter_node_foreign_source, '/', exporter_node_foreign_id), '${exporter_instance:regex}') AND match(application, '${application:regex}') AND (length('${l4_protocol:csv}') = 0 OR protocol IN (${l4_protocol:csv})) AND match(IPv6NumToString(src_address), '${src_address:regex}') AND match(IPv6NumToString(dst_address), '${dst_address:regex}')",
          "format": 0
        }
      ]
    },
    {
      "id": 6,
      "type": "piechart",
      "title": "DSCP mix",
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 24 },
      "datasource": { "type": "grafana-clickhouse-datasource", "uid": "clickhouse" },
      "options": { "legend": { "displayMode": "table", "placement": "right", "values": ["value", "percent"] }, "pieType": "donut" },
      "targets": [
        {
          "refId": "A",
          "rawSql": "SELECT countIf(dscp = 0) AS \"BE (0)\", countIf(dscp = 8) AS \"CS1 (8)\", countIf(dscp = 10) AS \"AF11 (10)\", countIf(dscp = 26) AS \"AF31 (26)\", countIf(dscp = 46) AS \"EF (46)\", countIf(dscp NOT IN (0, 8, 10, 26, 46) AND dscp IS NOT NULL) AS \"Other DSCP\" FROM deltav.flows_raw WHERE $__timeFilter(timestamp) AND match(location, '${monitoring_location:regex}') AND match(concat(exporter_node_foreign_source, '/', exporter_node_foreign_id), '${exporter_instance:regex}') AND match(application, '${application:regex}') AND (length('${l4_protocol:csv}') = 0 OR protocol IN (${l4_protocol:csv})) AND match(IPv6NumToString(src_address), '${src_address:regex}') AND match(IPv6NumToString(dst_address), '${dst_address:regex}') AND dscp IS NOT NULL",
          "format": 0
        }
      ]
    },
    {
      "id": 7,
      "type": "bargauge",
      "title": "TCP flag distribution",
      "gridPos": { "h": 8, "w": 12, "x": 12, "y": 24 },
      "datasource": { "type": "grafana-clickhouse-datasource", "uid": "clickhouse" },
      "fieldConfig": { "defaults": { "unit": "short" }, "overrides": [] },
      "options": { "orientation": "horizontal", "displayMode": "gradient" },
      "targets": [
        {
          "refId": "A",
          "rawSql": "SELECT countIf(bitTest(toUInt8(tcp_flags), 1)) AS \"SYN\", countIf(bitTest(toUInt8(tcp_flags), 4)) AS \"ACK\", countIf(bitTest(toUInt8(tcp_flags), 2)) AS \"RST\", countIf(bitTest(toUInt8(tcp_flags), 0)) AS \"FIN\", countIf(bitTest(toUInt8(tcp_flags), 3)) AS \"PSH\", countIf(bitTest(toUInt8(tcp_flags), 5)) AS \"URG\", countIf(bitTest(toUInt8(tcp_flags), 6)) AS \"ECE\", countIf(bitTest(toUInt8(tcp_flags), 7)) AS \"CWR\" FROM deltav.flows_raw WHERE $__timeFilter(timestamp) AND match(location, '${monitoring_location:regex}') AND match(concat(exporter_node_foreign_source, '/', exporter_node_foreign_id), '${exporter_instance:regex}') AND match(application, '${application:regex}') AND protocol = 6 AND tcp_flags IS NOT NULL AND match(IPv6NumToString(src_address), '${src_address:regex}') AND match(IPv6NumToString(dst_address), '${dst_address:regex}')",
          "format": 0
        }
      ]
    },
    {
      "id": 8,
      "type": "table",
      "title": "Locality matrix",
      "gridPos": { "h": 8, "w": 24, "x": 0, "y": 32 },
      "datasource": { "type": "grafana-clickhouse-datasource", "uid": "clickhouse" },
      "fieldConfig": {
        "defaults": {},
        "overrides": [
          {"matcher": {"id": "byName", "options": "Bytes"}, "properties": [{"id": "unit", "value": "bytes"}]},
          {"matcher": {"id": "byName", "options": "Flow records"}, "properties": [{"id": "unit", "value": "short"}]}
        ]
      },
      "targets": [
        {
          "refId": "A",
          "rawSql": "SELECT src_locality AS \"Src locality\", dst_locality AS \"Dst locality\", count() AS \"Flow records\", sum(num_bytes) AS \"Bytes\" FROM deltav.flows_raw WHERE $__timeFilter(timestamp) AND match(location, '${monitoring_location:regex}') AND match(concat(exporter_node_foreign_source, '/', exporter_node_foreign_id), '${exporter_instance:regex}') AND match(application, '${application:regex}') AND (length('${l4_protocol:csv}') = 0 OR protocol IN (${l4_protocol:csv})) AND match(IPv6NumToString(src_address), '${src_address:regex}') AND match(IPv6NumToString(dst_address), '${dst_address:regex}') GROUP BY src_locality, dst_locality ORDER BY \"Bytes\" DESC",
          "format": 0
        }
      ]
    },
    {
      "id": 9,
      "type": "table",
      "title": "Raw flow records (last 60 minutes, LIMIT 1000)",
      "gridPos": { "h": 12, "w": 24, "x": 0, "y": 40 },
      "datasource": { "type": "grafana-clickhouse-datasource", "uid": "clickhouse" },
      "timeFrom": "1h",
      "fieldConfig": {
        "defaults": {},
        "overrides": [
          {"matcher": {"id": "byName", "options": "Bytes"}, "properties": [{"id": "unit", "value": "bytes"}]},
          {"matcher": {"id": "byName", "options": "Pkts"}, "properties": [{"id": "unit", "value": "short"}]}
        ]
      },
      "targets": [
        {
          "refId": "A",
          "rawSql": "SELECT formatDateTime(timestamp, '%Y-%m-%d %H:%i:%S') AS \"Time\", CASE protocol WHEN 6 THEN 'TCP' WHEN 17 THEN 'UDP' WHEN 1 THEN 'ICMP' WHEN 50 THEN 'ESP' ELSE toString(protocol) END AS \"L4\", IPv6NumToString(src_address) AS \"Src\", src_port AS \"Sport\", IPv6NumToString(dst_address) AS \"Dst\", dst_port AS \"Dport\", application AS \"App\", num_bytes AS \"Bytes\", num_packets AS \"Pkts\", exporter_node_foreign_id AS \"Exporter\", netflow_version AS \"Wire fmt\" FROM deltav.flows_raw WHERE timestamp >= now() - INTERVAL 60 MINUTE AND match(location, '${monitoring_location:regex}') AND match(concat(exporter_node_foreign_source, '/', exporter_node_foreign_id), '${exporter_instance:regex}') AND match(application, '${application:regex}') AND (length('${l4_protocol:csv}') = 0 OR protocol IN (${l4_protocol:csv})) AND match(IPv6NumToString(src_address), '${src_address:regex}') AND match(IPv6NumToString(dst_address), '${dst_address:regex}') ORDER BY timestamp DESC LIMIT 1000",
          "format": 0
        }
      ],
      "links": [
        {
          "title": "Pivot: filter to this src↔dst pair",
          "url": "/d/flows-forensic?var-src_address=${__data.fields.Src}&var-dst_address=${__data.fields.Dst}&var-monitoring_location=${monitoring_location:queryparam}&var-exporter_instance=${exporter_instance:queryparam}&var-application=${application:queryparam}&var-l4_protocol=${l4_protocol:queryparam}&var-src_port=${src_port:queryparam}&var-dst_port=${dst_port:queryparam}&from=${__from}&to=${__to}",
          "targetBlank": false
        }
      ]
    }
  ]
}
```

Note: Panel 9 uses `"timeFrom": "1h"` to override the dashboard time range to the last 60 minutes regardless of dashboard setting. This is Grafana's supported `relativeTimeRange` equivalent at the panel level.

- [ ] **Step 2: Validate JSON syntax + structural assertions**

```bash
python3 << 'EOF'
import json
d = json.load(open("opennms-container/delta-v/grafana/dashboards/flows-forensic.json"))
assert d["uid"] == "flows-forensic", f"uid wrong: {d['uid']}"
assert d["title"] == "Flows Forensic", f"title wrong: {d['title']}"
assert len(d["panels"]) == 9, f"expected 9 panels, got {len(d['panels'])}"
assert len(d["templating"]["list"]) == 8, f"expected 8 templates, got {len(d['templating']['list'])}"

# Verify template variable names + order
template_names = [v["name"] for v in d["templating"]["list"]]
expected = ["monitoring_location", "exporter_instance", "application", "l4_protocol",
            "src_address", "dst_address", "src_port", "dst_port"]
assert template_names == expected, f"templates in wrong order: {template_names}"

# All templates have allowCustomValue: true
for v in d["templating"]["list"]:
    assert v.get("allowCustomValue") is True, f"{v['name']} missing allowCustomValue"

# Panel IDs + types in order
ids = [p["id"] for p in d["panels"]]
assert ids == [1, 2, 3, 4, 5, 6, 7, 8, 9], f"panel ids: {ids}"
types = [p["type"] for p in d["panels"]]
assert types == ["timeseries", "table", "table", "table", "piechart", "piechart",
                 "bargauge", "table", "table"], f"types: {types}"

# Self-pivot data links on panels 2, 3, 4, 9
for idx, panel_id in enumerate([2, 3, 4, 9]):
    panel_index = panel_id - 1
    links = d["panels"][panel_index].get("links", [])
    assert len(links) == 1, f"panel {panel_id} expected 1 link, got {len(links)}"
    url = links[0]["url"]
    # Every self-pivot URL must re-pass 8 other variables (not just the narrowed one)
    for var in expected:
        assert f"var-{var}=" in url, f"panel {panel_id} link missing var-{var}: {url}"

# Panel 9 has the 60-minute time override
assert d["panels"][8].get("timeFrom") == "1h", "Panel 9 missing timeFrom=1h override"

# Every panel uses the clickhouse datasource UID
for i, p in enumerate(d["panels"]):
    assert p["datasource"]["uid"] == "clickhouse", f"panel {i+1} wrong datasource"

print(f"OK uid=flows-forensic title='Flows Forensic' panels=9 templates=8 "
      f"allowCustomValue=all self-pivot-links=4 time-override=Panel-9")
EOF
```

Expected: `OK uid=flows-forensic title='Flows Forensic' panels=9 templates=8 allowCustomValue=all self-pivot-links=4 time-override=Panel-9`.

- [ ] **Step 3: Commit**

```bash
git add opennms-container/delta-v/grafana/dashboards/flows-forensic.json
git commit -m "feat(flows): add flows-forensic dashboard JSON (9 panels + 8 templates)

Sibling dashboard to flows-overview (PR #184). 9 panels:
- Panel 1: bandwidth timeline stacked by application (wide pivot; 9
  known apps + Other)
- Panel 2: top source IPs (self-pivot link → var-src_address)
- Panel 3: top destination IPs (self-pivot link → var-dst_address)
- Panel 4: top conversations with protocol+ports (self-pivot →
  src+dst+application)
- Panel 5: L4 protocol mix (TCP/UDP/ICMP/ESP/Other donut)
- Panel 6: DSCP mix (BE/CS1/AF11/AF31/EF/Other donut)
- Panel 7: TCP flag distribution bar gauge (SYN/ACK/RST/FIN/PSH/URG/ECE/CWR)
- Panel 8: locality matrix (src × dst locality table)
- Panel 9: raw flow records table, bounded to last 60 minutes via
  timeFrom='1h' panel-level override, LIMIT 1000 (self-pivot → src+dst)

8 template variables (monitoring_location, exporter_instance, application,
l4_protocol, src_address, dst_address, src_port, dst_port) all set
allowCustomValue=true so operators can type IPs/ports outside the top-100
populating-query result. Every self-pivot link URL explicitly re-passes
all 8 variables via \${var:queryparam} so successive clicks accumulate
filters instead of losing prior selections (Grafana data-link URLs
replace, not merge, the query string)."
```

---

### Task 2: Add drill-OUT data links on `flows-overview.json` (4 panels)

**Files:**
- Modify: `opennms-container/delta-v/grafana/dashboards/flows-overview.json`

Four of the six existing panels (Top apps, Top conversations, Per-exporter rate, Flow sources inventory) gain a `"links"` array that opens flows-forensic with relevant context pre-set.

- [ ] **Step 1: Verify Panel 2 (Top apps) current structure**

Locate Panel id=2 in `flows-overview.json`. Confirm it's the bargauge "Top applications by bandwidth" panel. It currently has no `"links"` key.

Find the unique closing sequence of Panel 2 (the targets array):
```
          "format": 0
        }
      ]
    },
    {
      "id": 3,
```

- [ ] **Step 2: Add `"links"` array to Panel 2 via Edit**

Use the Edit tool with this old_string (the unique closing of Panel 2's targets + opening of Panel 3):
```
          "rawSql": "SELECT application, sum(num_bytes) AS bytes FROM deltav.flows_raw WHERE $__timeFilter(timestamp) AND match(location, '${monitoring_location:regex}') GROUP BY application ORDER BY bytes DESC LIMIT 10",
          "format": 0
        }
      ]
    },
    {
      "id": 3,
```

and new_string:
```
          "rawSql": "SELECT application, sum(num_bytes) AS bytes FROM deltav.flows_raw WHERE $__timeFilter(timestamp) AND match(location, '${monitoring_location:regex}') GROUP BY application ORDER BY bytes DESC LIMIT 10",
          "format": 0
        }
      ],
      "links": [
        {
          "title": "Drill into forensic view",
          "url": "/d/flows-forensic?var-application=${__data.fields.application}&var-monitoring_location=${monitoring_location:queryparam}&from=${__from}&to=${__to}",
          "targetBlank": false
        }
      ]
    },
    {
      "id": 3,
```

- [ ] **Step 3: Add `"links"` array to Panel 3 (Top conversations)**

Edit with old_string (Panel 3's targets closing):
```
          "rawSql": "SELECT IPv6NumToString(src_address) AS \"Source\", IPv6NumToString(dst_address) AS \"Destination\", application AS \"Application\", sum(num_bytes) AS \"Bytes\", sum(num_packets) AS \"Packets\", count() AS \"Flow records\" FROM deltav.flows_raw WHERE $__timeFilter(timestamp) AND match(location, '${monitoring_location:regex}') AND match(application, '${application:regex}') GROUP BY src_address, dst_address, application ORDER BY \"Bytes\" DESC LIMIT 20",
          "format": 0
        }
      ]
    },
    {
      "id": 4,
```

and new_string:
```
          "rawSql": "SELECT IPv6NumToString(src_address) AS \"Source\", IPv6NumToString(dst_address) AS \"Destination\", application AS \"Application\", sum(num_bytes) AS \"Bytes\", sum(num_packets) AS \"Packets\", count() AS \"Flow records\" FROM deltav.flows_raw WHERE $__timeFilter(timestamp) AND match(location, '${monitoring_location:regex}') AND match(application, '${application:regex}') GROUP BY src_address, dst_address, application ORDER BY \"Bytes\" DESC LIMIT 20",
          "format": 0
        }
      ],
      "links": [
        {
          "title": "Drill into forensic view",
          "url": "/d/flows-forensic?var-src_address=${__data.fields.Source}&var-dst_address=${__data.fields.Destination}&var-application=${__data.fields.Application}&var-monitoring_location=${monitoring_location:queryparam}&from=${__from}&to=${__to}",
          "targetBlank": false
        }
      ]
    },
    {
      "id": 4,
```

- [ ] **Step 4: Add `"links"` array to Panel 4 (Per-exporter flow rate)**

Edit with old_string:
```
          "rawSql": "SELECT $__timeInterval(timestamp) AS time, concat(exporter_node_foreign_source, '/', exporter_node_foreign_id) AS exporter, count() / ($__interval_ms / 1000) AS flows_per_sec FROM deltav.flows_raw WHERE $__timeFilter(timestamp) AND match(location, '${monitoring_location:regex}') AND match(concat(exporter_node_foreign_source, '/', exporter_node_foreign_id), '${exporter_instance:regex}') GROUP BY time, exporter ORDER BY time",
          "format": 1
        }
      ]
    },
    {
      "id": 5,
```

and new_string:
```
          "rawSql": "SELECT $__timeInterval(timestamp) AS time, concat(exporter_node_foreign_source, '/', exporter_node_foreign_id) AS exporter, count() / ($__interval_ms / 1000) AS flows_per_sec FROM deltav.flows_raw WHERE $__timeFilter(timestamp) AND match(location, '${monitoring_location:regex}') AND match(concat(exporter_node_foreign_source, '/', exporter_node_foreign_id), '${exporter_instance:regex}') GROUP BY time, exporter ORDER BY time",
          "format": 1
        }
      ],
      "links": [
        {
          "title": "Drill into forensic view",
          "url": "/d/flows-forensic?var-exporter_instance=${__field.labels.exporter}&var-monitoring_location=${monitoring_location:queryparam}&from=${__from}&to=${__to}",
          "targetBlank": false
        }
      ]
    },
    {
      "id": 5,
```

- [ ] **Step 5: Add `"links"` array to Panel 6 (Flow sources inventory)**

Edit with old_string (Panel 6's targets closing — Panel 6 is the last panel, so the closing is before the `]}` end of the panels array):
```
          "rawSql": "SELECT exporter_node_foreign_source, exporter_node_foreign_id, location, host, formatDateTime(max(timestamp), '%Y-%m-%d %H:%i:%S') AS last_seen, count() AS flow_count FROM deltav.flows_raw WHERE $__timeFilter(timestamp) AND match(location, '${monitoring_location:regex}') GROUP BY exporter_node_foreign_source, exporter_node_foreign_id, location, host ORDER BY flow_count DESC",
          "format": 0
        }
      ]
    }
  ]
}
```

and new_string:
```
          "rawSql": "SELECT exporter_node_foreign_source, exporter_node_foreign_id, location, host, formatDateTime(max(timestamp), '%Y-%m-%d %H:%i:%S') AS last_seen, count() AS flow_count FROM deltav.flows_raw WHERE $__timeFilter(timestamp) AND match(location, '${monitoring_location:regex}') GROUP BY exporter_node_foreign_source, exporter_node_foreign_id, location, host ORDER BY flow_count DESC",
          "format": 0
        }
      ],
      "links": [
        {
          "title": "Drill into forensic view",
          "url": "/d/flows-forensic?var-exporter_instance=${__data.fields[\"Foreign source\"]}/${__data.fields[\"Foreign ID\"]}&var-monitoring_location=${__data.fields[\"Monitoring location\"]}&from=${__from}&to=${__to}",
          "targetBlank": false
        }
      ]
    }
  ]
}
```

- [ ] **Step 6: Validate JSON + assert all 4 drill-OUT links present**

```bash
python3 << 'EOF'
import json
d = json.load(open("opennms-container/delta-v/grafana/dashboards/flows-overview.json"))
assert d["uid"] == "snmp-overview" or d["uid"] == "flows-overview", f"unexpected uid: {d['uid']}"
# flows-overview has 6 panels — panels with drill-OUT links: ids 2, 3, 4, 6
links_panels = {p["id"]: p.get("links", []) for p in d["panels"]}
for pid in [2, 3, 4, 6]:
    assert len(links_panels[pid]) == 1, f"panel {pid} missing drill link: {links_panels[pid]}"
    assert "flows-forensic" in links_panels[pid][0]["url"], f"panel {pid} link wrong target"
# Panels 1 and 5 should NOT have drill-OUT links (no clean target)
assert len(links_panels[1]) == 0, "panel 1 should have no drill link"
assert len(links_panels[5]) == 0, "panel 5 should have no drill link"
print(f"OK flows-overview drill-OUT links on panels 2, 3, 4, 6; panels 1, 5 skipped")
EOF
```

Expected: `OK flows-overview drill-OUT links on panels 2, 3, 4, 6; panels 1, 5 skipped`.

- [ ] **Step 7: Commit**

```bash
git add opennms-container/delta-v/grafana/dashboards/flows-overview.json
git commit -m "feat(flows): add drill-OUT data links on flows-overview panels

Four panels on flows-overview gain a \"links\" array that opens the new
flows-forensic dashboard with context-appropriate template variables
pre-set via URL parameters:

- Panel 2 (Top applications) → var-application
- Panel 3 (Top conversations) → var-src_address + var-dst_address + var-application
- Panel 4 (Per-exporter flow rate) → var-exporter_instance
- Panel 6 (Flow sources inventory) → var-exporter_instance + var-monitoring_location

Panels 1 (bandwidth timeline) and 5 (protocol mix) skipped — no clean
drill target (timeline point = range, protocol value doesn't map to
forensic filters). All links pass time range via \${__from}/\${__to}.
Grafana's \${__data.fields[\"...\"]} bracket-quote form required for
column names with spaces (Foreign source, Monitoring location)."
```

---

### Task 3: Extend E2E with Step 12

**Files:**
- Modify: `opennms-container/delta-v/test-prometheus-writer-e2e.sh`

Add a new assertion step between Step 11 (l8opensim-lab flows in ClickHouse) and the final `exit 0`. Asserts the forensic dashboard provisions with 9 panels AND verifies flows-overview regression (still loads after data-link edits).

- [ ] **Step 1: Locate insertion point**

In `opennms-container/delta-v/test-prometheus-writer-e2e.sh`, find the last 3 lines:
```bash

echo "==> ALL ASSERTIONS PASSED"
exit 0
```

Step 11 already ends with `fi` before the blank line. Insert Step 12 BETWEEN that closing `fi` and the `echo "==> ALL ASSERTIONS PASSED"`.

- [ ] **Step 2: Insert Step 12 block**

Use Edit with unique context (the end of Step 11 + final assertion):

old_string:
```bash
    docker compose logs flow-enricher | tail -30
    docker compose logs minion-lab | tail -20
    exit 1
fi

echo "==> ALL ASSERTIONS PASSED"
exit 0
```

new_string:
```bash
    docker compose logs flow-enricher | tail -30
    docker compose logs minion-lab | tail -20
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
```

- [ ] **Step 3: Verify script syntax**

```bash
bash -n opennms-container/delta-v/test-prometheus-writer-e2e.sh && echo "syntax OK"
```
Expected: `syntax OK`.

- [ ] **Step 4: Commit**

```bash
git add opennms-container/delta-v/test-prometheus-writer-e2e.sh
git commit -m "test(flows): add Step 12 asserting flows-forensic dashboard + flows-overview regression

Asserts GET /api/dashboards/uid/flows-forensic returns title='Flows
Forensic' and panels count == 9. Then regression-checks flows-overview:
GET /api/dashboards/uid/flows-overview returns title='Flows Overview'
and panels count == 6 (unchanged after data-link edits from Task 2).

Doesn't exercise drill-link navigation (needs a browser); that's
covered by the manual verification checklist in the PR description."
```

---

### Task 4: Verification gate — boot stack, confirm both dashboards load, manual drill-link check

**No file changes** — interactive verification gate.

- [ ] **Step 1: Boot stack**

```bash
cd opennms-container/delta-v
docker compose --profile lite --profile metrics up -d 2>&1 | tail -5
cd ../..
```

Wait ~90 seconds for full warmup (provisiond imports l8opensim-lab requisition + collectd cycles through once + flow-enricher consumes first Kafka messages):

```bash
until curl -sf http://localhost:13000/api/health > /dev/null 2>&1 && \
      [ "$(curl -sf 'http://localhost:8123/?user=deltav&password=deltav' --data-binary 'SELECT count() FROM deltav.flows_raw WHERE location=\'l8opensim-lab\'')" -gt 100 ]; do
    sleep 10
done
echo "stack warm at $(date '+%H:%M:%S')"
```

- [ ] **Step 2: Verify both dashboards load via Grafana API**

```bash
for uid in flows-forensic flows-overview; do
    title=$(curl -sf -u admin:admin http://localhost:13000/api/dashboards/uid/$uid | \
            python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["dashboard"]["title"])')
    panels=$(curl -sf -u admin:admin http://localhost:13000/api/dashboards/uid/$uid | \
             python3 -c 'import json,sys; d=json.load(sys.stdin); print(len(d["dashboard"]["panels"]))')
    echo "${uid}: title='${title}' panels=${panels}"
done
```

Expected:
```
flows-forensic: title='Flows Forensic' panels=9
flows-overview: title='Flows Overview' panels=6
```

- [ ] **Step 3: Manual drill-link verification (browser)**

Open `http://localhost:13000/d/flows-overview` in a browser. Wait for data to populate (all 6 panels show values).

Verify each of the 4 drill-OUT links:

1. **Panel 2 (Top apps)**: Click any app bar. Expected: URL changes to `/d/flows-forensic?var-application=<clicked_app>&...`. Forensic dashboard renders with Application filter set to the clicked app.
2. **Panel 3 (Top conversations)**: Click any row. Expected: URL includes `var-src_address=<src>&var-dst_address=<dst>&var-application=<app>`. Forensic renders with all 3 filters applied.
3. **Panel 4 (Per-exporter rate)**: Click any line series. Expected: URL includes `var-exporter_instance=<foreign_source>/<foreign_id>`. Forensic renders filtered to one exporter.
4. **Panel 6 (Flow sources inventory)**: Click any row. Expected: URL includes `var-exporter_instance=...&var-monitoring_location=...`. Forensic renders filtered to that exporter + location.

- [ ] **Step 4: Manual self-pivot verification (browser)**

From the forensic dashboard (either already open from Step 3 or open `http://localhost:13000/d/flows-forensic` fresh):

1. **Panel 2 (Top source IPs)**: Click a row. URL changes to include `var-src_address=<ip>` plus all 8 other variables explicitly re-passed. All panels re-render filtered to that source IP.
2. **Panel 3 (Top destination IPs)**: Click a row. `var-dst_address=<ip>` added, previous `var-src_address` preserved in URL (confirms the explicit re-pass works).
3. **Panel 4 (Top conversations)**: Click a row. `var-src_address` + `var-dst_address` + `var-application` all set.
4. **Panel 9 (Raw flow records)**: Click a row. `var-src_address` + `var-dst_address` set.

Confirm that clicking two pivot links in succession (e.g., Panel 2 then Panel 3) preserves the first filter AND adds the second. If the first filter is lost, the URL re-pass pattern has a bug — escalate to user.

- [ ] **Step 5: Verify standalone open works**

Open `http://localhost:13000/d/flows-forensic` with NO URL parameters (clear the URL query string or open a new incognito tab). Expected:
- All 8 template dropdowns show "All" selected.
- All 9 panels render with data (no empty panels except possibly TCP flags if no TCP traffic in the default 1h window, or Locality matrix if all flows are one locality pair).
- Panel 9 (Raw records) shows rows even though the dashboard time range is "Last 1 hour" (the `timeFrom: '1h'` override ensures the 60-minute cap applies regardless).

- [ ] **Step 6: Tear down (only on success)**

```bash
cd opennms-container/delta-v
docker compose --profile lite --profile metrics down -v --remove-orphans 2>&1 | tail -3
cd ../..
git checkout -- opennms-container/delta-v/provisiond-overlay/etc/imports/ 2>&1 || true
git status --short
```

Expected: empty working tree.

(No commit. This is verification only.)

If any drill-link doesn't populate the expected variable, OR self-pivot variables don't accumulate, STOP and escalate. Likely fallback: switch `${__data.fields.X}` to `${__cell:N}` (column-index form) in the failing link's URL.

---

### Task 5: Full E2E gate — all 12 steps green

**No file changes** — runs the extended `test-prometheus-writer-e2e.sh` end-to-end.

- [ ] **Step 1: Run the full E2E**

```bash
bash opennms-container/delta-v/test-prometheus-writer-e2e.sh 2>&1 | tee /tmp/flows-forensic-e2e.log | grep -E "==> Step|FAIL|ALL ASSERTIONS|ClickHouse has|VM returned|flows-forensic loaded|flows-overview regression" | tail -30
```

Expected final lines:
```
==> Step 11: ...
==> ClickHouse has <N> l8opensim-lab flow rows across 3 protocol(s)
==> 3 of 4 protocols present (sFlow gap is a documented known issue)
==> Step 12: Verify flows-forensic dashboard provisioned with 9 panels
==> flows-forensic loaded with 9 panels
==> flows-overview regression OK (still 6 panels)
==> ALL ASSERTIONS PASSED
```

Total run time: 4-6 minutes.

- [ ] **Step 2: If anything fails, debug from /tmp/flows-forensic-e2e.log**

Common failure modes:
- **Steps 4-6 pre-existing flake** (rpc-canary scan timeout OR enrichment_missing race per memory `project_mock_snmp_agent_systemgroup_bug`) — retry the E2E; unrelated to this PR.
- **Step 12 `flows-forensic not loaded`**: Grafana provisioning didn't pick up the new JSON. Check `docker compose logs grafana 2>&1 | grep -iE 'dashboard|provisioning' | tail -20`. If the file has a JSON syntax error Grafana's provisioning skips it silently. Re-run Task 1 Step 2 validation.
- **Step 12 `forensic_panels != 9`**: JSON parses but has wrong panel count. Re-check the panels array in the JSON.
- **Step 12 `flows-overview regression`**: Data-link additions broke flows-overview's JSON structurally. Validate the file via `python3 -c 'import json; json.load(open("..."))'`.

- [ ] **Step 3: Discard drift and verify clean tree**

```bash
git checkout -- opennms-container/delta-v/provisiond-overlay/etc/imports/ 2>&1 || true
git status --short
```

Expected: empty.

(No commit — this task is the gate.)

---

### Task 6: Push branch + open PR

- [ ] **Step 1: Push branch**

```bash
git push -u origin feat/flows-forensic 2>&1 | tail -5
```
Expected: new branch created on `pbrane/delta-v`.

- [ ] **Step 2: Open PR**

```bash
gh pr create --repo pbrane/delta-v --base develop --head feat/flows-forensic \
  --title "feat(flows): add forensic drilldown dashboard + cross-dashboard drill links" \
  --body "$(cat <<'EOF'
## Summary

Sibling dashboard to Flows Overview (PR #184) for forensic flow investigation. Operator clicks a row/cell in Flows Overview → new Flows Forensic dashboard opens with that row's context pre-filled as URL parameters. From within Flows Forensic, clicking a top-N row self-pivots the filters in-place so the operator can drill without leaving the dashboard. A 60-minute-bounded raw flow records table at the bottom answers "exactly which flows."

### What ships

- **`flows-forensic.json` dashboard** — 9 panels: bandwidth timeline stacked by app, top src IPs, top dst IPs, top conversations (with protocol+ports), L4 protocol mix, DSCP mix, TCP flag distribution, locality matrix, raw flow records.
- **8 template variables** — `monitoring_location`, `exporter_instance`, `application`, `l4_protocol`, `src_address`, `dst_address`, `src_port`, `dst_port`. All set `allowCustomValue: true` so operators can type IPs/ports beyond the LIMIT 100 populating-query result.
- **Drill-OUT data links on 4 flows-overview panels** (Top apps, Top conversations, Per-exporter rate, Flow sources inventory) — clicking opens forensic dashboard with context-appropriate `var-*` URL params.
- **Self-pivot data links on 4 forensic panels** (Top src IPs, Top dst IPs, Top conversations, Raw records) — every URL explicitly re-passes all 8 other template variables via `${var:queryparam}` so successive clicks accumulate filters rather than lose prior selections.
- **Raw flow records bounded to last 60 minutes** via panel-level `timeFrom: '1h'` override, LIMIT 1000.
- **E2E Step 12** asserts forensic dashboard provisions with 9 panels + flows-overview regression check (still 6 panels after data-link edits).

### Testing

Automated (E2E):
- [x] Step 12 asserts `GET /api/dashboards/uid/flows-forensic` returns title + 9 panels.
- [x] Step 12 regression-checks `GET /api/dashboards/uid/flows-overview` still returns 6 panels.
- [x] Full `bash test-prometheus-writer-e2e.sh` green (12 steps).

Manual (drill mechanics can't be unit-tested without Playwright — see plan Task 4):
- [x] Each of 4 flows-overview drill-OUT links opens forensic with expected variables pre-set.
- [x] Each of 4 forensic self-pivot links updates URL with new variable + preserves the other 7.
- [x] Successive self-pivot clicks accumulate filters (Panel 2 click then Panel 3 click → BOTH `var-src_address` and `var-dst_address` in URL).
- [x] Standalone open (no URL params) renders all 9 panels with "All" filters.

### Known limitations

- **Panel 1 app list is hardcoded**. New apps from the classifier (PR #184's expansion added 10) bucket into "Other" until Panel 1's SQL `sumIf(application = 'X')` list is updated. Future enhancement: Grafana "Partition by values" transformation for dynamic series — tracked as follow-up.
- **Panel 7 TCP flag counts empty when no TCP in filter**. Designed behavior — bar chart shows zeros, not an error.
- **LIMIT 100 on IP/port populating queries**. Mitigated by `allowCustomValue: true` — operator can type any IP/port not in the dropdown.

### Backward compatibility

Additive change. flows-overview's existing filter dropdowns + panels unchanged for operators who don't click the new drill-OUT links. E2E Step 12 is additive — Steps 1-11 still pass even if Step 12 fails.

Spec: `docs/superpowers/specs/2026-04-21-flows-forensic-design.md`. Plan: `docs/superpowers/plans/2026-04-21-flows-forensic-plan.md`.

## Out of scope (queued follow-ups)

- Dynamic Panel-1 app list via Grafana transformations.
- sFlow silent-drop investigation (`project_sflow_silent_drop_v2`).
- ClickHouse MV `location` column — future schema migration.
- Geo / map panel — requires Geo-IP enrichment on flow_enricher side.
- Per-VLAN / per-AS panels — when operator demand emerges.
EOF
)" 2>&1 | tail -3
```

Expected: GitHub CLI returns the PR URL.

- [ ] **Step 3: Print the PR URL**

The `gh pr create` command prints the URL on success. Echo it back.

---

## Self-Review Checklist (run after writing the plan; not a separate task)

**1. Spec coverage**

| Spec section | Implementing task |
|---|---|
| New flows-forensic.json (9 panels, 8 templates) | Task 1 |
| Drill-OUT on 4 flows-overview panels | Task 2 |
| Self-pivot on 4 forensic panels (URLs re-pass all 8 other vars) | Task 1 (inline in panel links) |
| allowCustomValue: true on all templates | Task 1 (inline) |
| Panel 9 60-minute time override | Task 1 (`timeFrom: "1h"`) |
| E2E Step 12 (9 panels + regression) | Task 3 |
| Grafana API smoke + manual drill verification | Task 4 |
| Full 12-step E2E green | Task 5 |
| PR opened `--repo pbrane/delta-v` | Task 6 |

All scope items have a task. Out-of-scope items (dynamic app list, Geo-IP, VLAN/AS panels) are absent from the plan.

**2. Placeholder scan:** no `TBD`, `TODO`, `FIXME`, `fill in`, or `similar to task N` references. SQL queries complete inline. JSON block complete inline. Commit messages complete inline.

**3. Type/name consistency:**
- Dashboard UID `flows-forensic` consistent across Tasks 1, 2, 3, 4, 5, 6.
- Template variable names `monitoring_location`, `exporter_instance`, `application`, `l4_protocol`, `src_address`, `dst_address`, `src_port`, `dst_port` consistent across Tasks 1 (definition), 2 (drill-OUT URLs), and any reference.
- `allowCustomValue: true` applied in Task 1 JSON — matches Section "Architecture" + Risk 5 in spec.
- Panel count 9 matches E2E assertion in Task 3 matches Task 4 Step 2 verification.
- Self-pivot URLs: each re-passes all 8 other variables per spec section "Drill-link wiring" → "Critical" note.
- E2E Step 12 comes AFTER Step 11 (l8opensim-lab flows) per Task 3 Step 2's `old_string` insertion point.

All names match across tasks.
