# Shared `node-context-consumer` library + flow-enricher migration off JDBC

**Date:** 2026-06-01
**Status:** Approved design
**Branch:** `feat/node-context-consumer-migration`

## Goal

Get the **database out of the flow-enricher's per-flow hot path**. Today the enricher resolves node/interface identity via JDBC-to-PostgreSQL (`JdbcNodeInfoLookup` by IP, `JdbcSnmpInterfaceLookup` by `(nodeId, ifIndex)`), Caffeine-cached. Even cached, this hits PostgreSQL on every cache miss — and the **src/dst-IP lookups are high-cardinality** (internet-facing destination IPs are effectively unbounded), so misses trickle continuously at production flow velocity.

Migrate the enricher to consume the **`deltav-node-context` Kafka stream** (which `provisiond` already publishes and `alerts-forwarder` + `prometheus-writer` already consume) into an in-memory cache, making enrichment a pure map lookup with **zero DB in the hot path**. In the process, **eliminate the ~570 lines of duplicated consumer code** by extracting a shared library.

## Motivation & current state

- `provisiond` **publishes** `NodeContext` (proto `deltav-node-context.proto`, API v1 FROZEN) to topic `deltav-node-context`. It carries everything the enricher needs:
  - `node_id`, `location`, `node_label`, `foreign_source`, `foreign_id`, `categories`
  - `interface_metadata` — `map<string ip, InterfaceContext>` (→ build an **IP→node** index)
  - `snmp_interface_metadata` — `map<int32 ifIndex, SnmpInterfaceContext>` with `if_name` (→ **`(nodeId, ifIndex)→if_name`**)
- `alerts-forwarder` and `prometheus-writer` each carry an **identical** package-local copy of `NodeContextCache` (88 lines) + `NodeContextKafkaBootstrap` (195 lines) — keyed by `{location}@{node_id}`, bootstrapped by recording the per-partition end-offset HWM, seeking to earliest, draining to HWM, marking ready, then live-tailing. There is **no shared library**.
- The flow-enricher would be the **third** consumer — the inflection point to DRY this up.

## Scope decisions (confirmed)

- **Extract a shared library** and refactor the two existing consumers onto it, then add the flow-enricher (the ambitious option, not a third copy).
- **Complete lookup API in the shared cache** — `getByIp` + `ifName` live in `NodeContextCache` for all consumers (the IP index is cheap; a complete node-context lookup is the right abstraction). The two existing services carry an IP index they never read; cost is one bounded hashmap.
- **Populate `categories`** from `NodeContext.categories` (free win — the proto field and the `flows_raw` columns already exist; the JDBC path leaves them empty).
- **Gate flow processing until the cache is bootstrapped** — do not start the enrichFlows consumer until `NodeContextCacheReadyEvent` fires (we *can* know readiness via the Kafka HWM, unlike the old DB cold-start).

## Components

### 1. New module `core/node-context-consumer`

A small library depending on `deltav-kafka-contracts` (the `NodeContext` proto) and the Kafka client + Spring. Promotes the duplicated machinery to one home:

- **`NodeContextCache`** — in-memory store with a complete lookup API. Internally:
  - `ConcurrentHashMap<String, NodeContext> byKey` — keyed `{location}@{node_id}` (unchanged primary key).
  - `ConcurrentHashMap<Integer, NodeContext> byNodeId` — replaces today's linear `findByNodeId` scan with O(1).
  - `ConcurrentHashMap<String, NodeContext> byIp` — built from each record's `interface_metadata` keys (NEW).
  - API: `Optional<NodeContext> getByKey(String)`, `Optional<NodeContext> getByNodeId(int)`, `Optional<NodeContext> getByIp(String ipNormalized)`, `Optional<String> ifName(int nodeId, int ifIndex)` (reads `getByNodeId(nodeId).snmp_interface_metadata[ifIndex].if_name`), `boolean isReady()`, `int size()`, `Collection<NodeContext> snapshot()`.
  - `applyUpdate(key, value)` maintains **all three** indexes; when `value.deleted == true`, removes the node from `byKey`/`byNodeId` **and removes all of that node's IPs from `byIp`** (so tombstones don't leave stale IP entries).
  - **IP-conflict policy:** if two nodes claim the same IP (e.g. discovery duplicates), `byIp` is **last-update-wins** — mirrors the old JDBC `WHERE ipaddr = ? LIMIT 1` arbitrariness. Documented, not an error.
- **`NodeContextKafkaBootstrap`** — the existing HWM→seek-earliest→drain→`markReady`→live-tail consumer, moved verbatim (logic unchanged). Publishes `NodeContextCacheReadyEvent` when the drain completes.
- **`NodeContextCacheReadyEvent`** + **`NodeContextCacheHealthIndicator`** — promoted to the shared module (readiness gating + actuator health; prometheus-writer already has the health indicator, alerts-forwarder has the ready event — the shared module is the union).
- A `@Configuration` (or `@AutoConfiguration`) so a consuming service wires the cache + bootstrap with one import + two properties (`spring.kafka.bootstrap-servers`, the topic name with a `deltav-node-context` default).

### 2. Refactor the two existing consumers

- alerts-forwarder + prometheus-writer: **delete** their `…/nodecontext/` package-local copies, depend on `node-context-consumer`, update imports + bean wiring to the shared types. The two copies are verified line-identical (88/195), so behavior parity is straightforward; their existing tests are the regression net. prometheus-writer's `NodeContextCacheHealthIndicator` and alerts-forwarder's `NodeContextCacheReadyEvent` are now satisfied by the shared module.

### 3. flow-enricher migration

- Add the `node-context-consumer` dependency; wire `NodeContextCache` + `NodeContextKafkaBootstrap` (the consumer for `deltav-node-context`).
- In `FlowEnrichmentFunction`, replace:
  - `nodeInfoLookup.lookupByIpAddress(ip)` → `nodeContextCache.getByIp(normalize(ip))` (exporter, src, dst).
  - `snmpInterfaceLookup.lookupIfName(nodeId, ifIndex)` → `nodeContextCache.ifName(nodeId, ifIndex)`.
  - Map `NodeContext` → the mapper's inputs (node_id, foreign_source, foreign_id, **node_label**, **categories**, if_name).
- **Delete `JdbcNodeInfoLookup` + `JdbcSnmpInterfaceLookup`** and their beans.
- **Keep** the `DataSource` and `InterfaceMarkingCache` — it *writes* `UPDATE snmpinterface SET hasflows = true …` (a side-effect, not a lookup). NodeContext is read-only inventory and cannot replace a write. The `DataSource` bean stays solely for this.
- **`categories`:** `FlowToDocumentMapper.toProtoNodeInfo` now sets `categories` from the resolved `NodeContext.categories` (the proto `NodeInfo.categories` field 4 and `flows_raw.{exporter,src,dest}_node_categories` columns already exist; the MV ingest already maps them — no schema change).

### 4. Readiness gating

- Set the enrichFlows input binding to **`auto-startup: false`** (Spring Cloud Stream binding property).
- A listener on **`NodeContextCacheReadyEvent`** starts the binding (via `BindingsLifecycleController` / the binding's `Lifecycle.start()`), so flow consumption begins only after the NodeContext cache has drained to HWM.
- Result: the first flow processed has the fully-populated inventory available — strictly better than the old enrich-with-empty cold-start (which existed only because DB readiness couldn't be known). Liveness/health: the existing `NodeContextCacheHealthIndicator` reports not-ready during bootstrap.

### 5. IP normalization (mandated)

`getByIp` keys and the producer's `interface_metadata` keys **must** use the same canonical string form.

- **The producer contract (verified):** `provisiond` builds the keys in `core/daemon-boot-provisiond/src/main/java/org/deltav/netmgt/provision/nodecontext/NodeToProtobufTranslator.java` — line 70 `String ipKey = iface.getIpAddressAsString();` then `putInterfaceMetadata(ipKey, …)`. `OnmsIpInterface.getIpAddressAsString()` delegates to `java.net.InetAddress.getHostAddress()`, so the keys are **IPv4 dotted-decimal without leading zeros** (e.g. `10.0.0.1`) and **compressed IPv6** (e.g. `2001:db8::1`, not fully expanded).
- **The consumer mandate:** the `node-context-consumer` library MUST normalize every incoming flow IP string (exporter, src, dst) with `InetAddress.getByName(ipString).getHostAddress()` before calling `getByIp(...)`, producing exactly the producer's canonical form. The `byIp` index keys are taken straight from `interface_metadata` (already canonical per the producer contract); a defensive re-normalization of the map keys at consume time is cheap and guards against any non-canonical producer drift. (Flow source/dest addresses parsed from NetFlow/IPFIX may arrive IPv4-mapped-IPv6 or otherwise non-canonical, so consumer-side normalization is the load-bearing step.)
- **Testing:** a unit test MUST assert that functionally equivalent but textually divergent IP strings — e.g. `10.0.0.1` vs `010.000.000.001`, and an expanded vs compressed IPv6 pair (`2001:db8:0:0:0:0:0:1` vs `2001:db8::1`), and an IPv4-mapped-IPv6 vs plain IPv4 — all normalize to the identical canonical string and yield a `getByIp` cache hit against an `interface_metadata` entry keyed in the producer's form.

## Net result

- **Zero database in the per-flow hot path.** The high-cardinality src/dst-IP JDBC lookups are gone.
- **~570 lines of duplication removed**; one canonical NodeContext consumer with a complete lookup API.
- `node_label`, `if_name`, **and `categories`** all sourced from the stream.
- `DataSource` survives only for the `hasflows` write (`InterfaceMarkingCache`).
- Cold-start replaced by a **bounded, observable** bootstrap that gates ingest until ready.

## Testing

- **`node-context-consumer`:** `NodeContextCache` unit tests — `getByIp`, `ifName`, O(1) `getByNodeId`, multi-IP nodes, **tombstone eviction removes IPs**, **IP-conflict last-wins**, IP normalization (divergent forms → same entry). `NodeContextKafkaBootstrap` tests — drain-to-HWM marks ready + fires the event, live-tail applies updates/tombstones (embedded/mock Kafka per existing patterns).
- **alerts-forwarder + prometheus-writer:** their existing test suites pass unchanged against the shared types (behavior parity is the acceptance bar).
- **flow-enricher:** enrichment unit tests against an **in-memory `NodeContextCache`** (no DB) — `node_label`/`if_name`/`categories` populate; unresolved IP → empty; gating test asserts the binding does not start until `NodeContextCacheReadyEvent`. `FlowToDocumentMapper` unchanged.
- **E2E (next deploy):** flows show populated `exporter_node_label` / `input_if_name` / `output_if_name` / `*_categories` with the enricher holding **no PostgreSQL connection for lookups** (only the `hasflows` write path).

## Build order (one spec, sequenced)

1. Create `core/node-context-consumer` (cache + bootstrap + ready event + health indicator + config) with its tests.
2. Refactor alerts-forwarder, then prometheus-writer, onto it (delete copies; verify their tests).
3. Migrate flow-enricher (wire cache, replace the two JDBC lookups, delete them, populate categories, add readiness gating).

## Out of scope

- Changing the `NodeContext` proto or the publisher (`provisiond`) — consume only.
- Removing the `DataSource` from flow-enricher (retained for `InterfaceMarkingCache`'s `hasflows` write).
- `if_descr` / `if_alias` / `if_speed` (only `if_name`, matching the just-shipped fields).
- Dashboard work to surface `categories`.
