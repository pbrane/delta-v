# Flow Document: exporter `node_label` + input/output `if_name`

**Date:** 2026-05-31
**Status:** Approved design
**Branch:** `feat/flow-doc-node-label-ifname`

## Goal

Persist the exporter's **node label** and the **input/output SNMP interface names** on each flow record, so flow queries and dashboards can show human-readable device + interface identity without joining back to node-context/DB data.

## Motivation

Today the FlowDocument carries stable identifiers only — `NodeInfo{node_id, foreign_source, foreign_id, categories}` and the numeric `input/output_snmp_ifindex`. The human-readable `node_label` / `if_name` live in the DB and require a join. With per-device flow exporter attribution now working (#319), each nl6 device flows as its own exporter node with SNMP interfaces, so on-flow `exporter_node_label` + `input/output_if_name` become directly useful (e.g. the native Grafana Exporter axis can show `cisco-catalyst-9500-10.0.0.1` + `Gi0/3`).

## Confirmed scope decisions

- **`node_label`** — populated on **all three** `NodeInfo` (exporter/src/dest) in the protobuf (free; the label already rides in the node lookup), but **persisted only** as `exporter_node_label` in ClickHouse.
- **`if_name`** — **both** `input_if_name` + `output_if_name`, resolved from the **exporter** node's `snmpinterface` by the flow's input/output ifIndex.
- **Migration** — fully idempotent `ALTER` (existing deployments self-upgrade on boot; fresh deploys get everything from `CREATE`).

## Architecture & single source of truth

`core/flow-enricher/src/main/proto/deltav-flows.proto` is the **single** proto source. It is consumed by:
1. the **flow-enricher** (Java protobuf codegen), and
2. the **ClickHouse** image — `build.sh do_clickhouse_image` bakes this exact file to `/delta-v/proto/deltav-flows.proto`, and `flows_kafka` parses the Kafka topic with `kafka_format = 'ProtobufSingle'`, `kafka_schema = 'deltav-flows.proto:FlowDocument'`.

So one proto edit propagates to both sides; no duplicate proto to sync. ProtobufSingle maps proto fields to ClickHouse columns **by name**, so column order is irrelevant and extra proto fields not present as columns are simply skipped.

The ingest path is three ClickHouse objects:
`flows_kafka` (Kafka engine, parses protobuf) → `flows_ingest` (MATERIALIZED VIEW `TO flows_raw`) → `flows_raw` (MergeTree, queried table).

## Components

### 1. Protobuf — `core/flow-enricher/src/main/proto/deltav-flows.proto`

Additive only (API version stays 1; wire-compatible; old consumers ignore new fields). Current max FlowDocument field is **47**; fields **25** and **44** are reserved.

```proto
message NodeInfo {
    uint32 node_id = 1;
    string foreign_source = 2;
    string foreign_id = 3;
    repeated string categories = 4;
    string node_label = 5;          // NEW — OnmsNode.label
}

message FlowDocument {
    // ... existing fields ...
    string input_if_name  = 48;     // NEW — exporter snmpinterface.snmpifname for input_snmp_ifindex
    string output_if_name = 49;     // NEW — exporter snmpinterface.snmpifname for output_snmp_ifindex
}
```

### 2. Enricher — `core/flow-enricher`

**`node_label` (extend `JdbcNodeInfoLookup` + `FlowToDocumentMapper`):**
- Add `nodelabel` to both SELECTs in `JdbcNodeInfoLookup`:
  - by-IP: `SELECT n.nodeid, n.nodelabel, n.foreignsource, n.foreignid, n.location FROM node n JOIN ipinterface i ON n.nodeid = i.nodeid WHERE i.ipaddr = ? ...`
  - by-nodeId: `SELECT nodeid, nodelabel, foreignsource, foreignid, location FROM node WHERE nodeid = ?`
- Add `String nodeLabel` to the `JdbcNodeInfoLookup.NodeInfo` record + row mapper.
- `FlowToDocumentMapper.toProtoNodeInfo(...)`: set `node_label` when non-empty. Because this method builds all three NodeInfo sub-messages, `node_label` is populated uniformly for exporter/src/dest.

**`if_name` (new `JdbcSnmpInterfaceLookup`):**
- New class mirroring `JdbcNodeInfoLookup`'s structure (Caffeine cache, same JDBC template):
  - SQL: `SELECT snmpifname FROM snmpinterface WHERE nodeid = ? AND snmpifindex = ?`
  - Cache key: a `record IfKey(int nodeId, int ifIndex)`; value `Optional<String>`.
  - `String lookupIfName(int nodeId, int ifIndex)` → ifName or `null`.
- Bean wired in `FlowEnricherConfiguration` (same datasource as the node lookup) and passed into `FlowEnrichmentFunction` → `FlowToDocumentMapper`.
- In `FlowToDocumentMapper`, after the exporter node + ifindexes are set, resolve from the **exporter** node:
  - `input_if_name`  = `lookupIfName(exporterNodeId, inputSnmpIfindex)` when `exporterNodeId > 0` and the input ifindex is present.
  - `output_if_name` = `lookupIfName(exporterNodeId, outputSnmpIfindex)` when `exporterNodeId > 0` and the output ifindex is present.
  - Set the proto fields only when the lookup returns a non-empty name; otherwise leave empty (unprovisioned exporter, missing interface, or null ifindex).
  - `exporterNodeId` comes from the resolved exporter `NodeInfo` (0/absent → skip both).

### 3. ClickHouse DDL

**`02-flows-raw.sql`** — add to `CREATE TABLE deltav.flows_raw` (all `LowCardinality(String)` — bounded by node/interface counts; none are ORDER BY columns):
```
exporter_node_label LowCardinality(String),
input_if_name       LowCardinality(String),
output_if_name      LowCardinality(String),
```
Then idempotent migration for existing tables:
```sql
ALTER TABLE deltav.flows_raw ADD COLUMN IF NOT EXISTS exporter_node_label LowCardinality(String) AFTER exporter_node_categories;
ALTER TABLE deltav.flows_raw ADD COLUMN IF NOT EXISTS input_if_name  LowCardinality(String) AFTER input_snmp_ifindex;
ALTER TABLE deltav.flows_raw ADD COLUMN IF NOT EXISTS output_if_name LowCardinality(String) AFTER output_snmp_ifindex;
```

**`03-flows-kafka.sql`** and **`20-flows-ingest.sql`** — migrate by **DROP + recreate**, not `ALTER`.

Both objects are **stateless**: `flows_kafka` is a Kafka-engine consumer view (no stored rows) and `flows_ingest` is a streaming MV (no stored rows). ClickHouse `ALTER` on a Kafka-engine table — especially `MODIFY COLUMN` on a `Tuple` to add a sub-field — is version-sensitive and finicky. Recreating is the cleaner, more predictable path for stateless ingest objects, so it is the **primary** approach (not a fallback):

- `flows_kafka`: add `node_label String` to the **`exporter_node` Tuple** (src/dest tuples unchanged; their proto `node_label` is simply not extracted) and add top-level `input_if_name String`, `output_if_name String`. Migrate with `DROP TABLE IF EXISTS deltav.flows_kafka` then the full `CREATE`. The consumer rejoins group `deltav-clickhouse-persister` at its **committed offset**, so no message loss — just a brief re-attach.
- `flows_ingest`: add `exporter_node.node_label AS exporter_node_label`, `input_if_name AS input_if_name`, `output_if_name AS output_if_name` to the SELECT. Migrate with `DROP VIEW IF EXISTS deltav.flows_ingest` then the full `CREATE MATERIALIZED VIEW ... TO deltav.flows_raw AS SELECT ...`.

**Ordering (important):** drop the MV **before** recreating `flows_kafka` (so no MV is left attached to a table being dropped), then recreate `flows_kafka`, then recreate the MV. The init files run in numeric order (`02` → `03` → `20`), so the implementation must drop `flows_ingest` at the **top of `03-flows-kafka.sql`** (before the kafka `DROP`/`CREATE`) and recreate it in `20-flows-ingest.sql`. Because both are unconditional `DROP`+`CREATE` (no `IF NOT EXISTS` guard on the create), the init is self-updating on every boot and idempotent in effect (the recreated objects always match the current proto/columns).

### 4. Testing

- **`FlowToDocumentMapperTest`** (extend): exporter `node_label` set on the exporter NodeInfo; `input_if_name`/`output_if_name` resolved via a stubbed `JdbcSnmpInterfaceLookup`; all three empty when the exporter is unresolved (`nodeId=0`) or the lookup returns null.
- **`JdbcSnmpInterfaceLookupTest`** (new): cache hit/miss, returns null on missing `(nodeid, ifindex)`, caches negatives.
- **Manual / E2E:** after a deploy, `SELECT exporter_node_label, input_if_name, output_if_name, host FROM deltav.flows_raw WHERE host LIKE '10.0.0.%' LIMIT 10` shows populated label + interface names for the nl6 device exporters (post-#319 they resolve to nodes with SNMP interfaces). Confirm the migration (`flows_raw` additive `ALTER` + `flows_kafka`/`flows_ingest` recreate) upgrades an existing `clickhouse-data` volume without a reset, and that the Kafka consumer resumes at its committed offset.

## Out of scope

- Persisting `src_node_label` / `dest_node_label` (they ride in the protobuf already; a future change only needs the columns + ingest mappings — no proto/enricher change).
- `if_descr` / `if_alias` / `if_speed` (only `if_name`).
- Grafana dashboard changes to surface the new fields (separate follow-up).
- Backfilling existing `flows_raw` rows (new columns default to empty for pre-existing data).

## Risks / notes

- **Cardinality:** all three new columns are `LowCardinality(String)`; node labels and interface names are low-distinct, so storage and merge cost are minimal. None participate in the primary key / `ORDER BY`.
- **Interface lookup volume:** the `snmpinterface` lookup is per-flow but Caffeine-cached by `(nodeId, ifIndex)`, so steady-state hit rate is high (the device fleet's interface set is small and stable), mirroring the existing node-id cache.
- **`if_name` only populates** for resolved exporters that have SNMP interfaces; unprovisioned exporters yield empty strings (same graceful-empty behavior as the existing node fields).
