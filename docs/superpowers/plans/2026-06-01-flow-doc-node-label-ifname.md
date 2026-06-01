# Flow Document: exporter node_label + input/output if_name — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist the exporter's `node_label` and the input/output SNMP interface names (`if_name`) on each flow record, so flow queries/dashboards show human-readable device + interface identity without a join.

**Architecture:** Add three fields to the single-source `deltav-flows.proto` (consumed by both the Java enricher and ClickHouse's `ProtobufSingle` parser). The enricher resolves `node_label` via the existing `JdbcNodeInfoLookup` and `if_name` via a new cached `JdbcSnmpInterfaceLookup` (keyed by the exporter's `(nodeId, ifIndex)`); the caller passes resolved values into the pure `FlowToDocumentMapper`. ClickHouse gains 3 `LowCardinality(String)` columns via additive `ALTER` on `flows_raw` plus DROP+recreate of the stateless `flows_kafka` + `flows_ingest` objects.

**Tech Stack:** Java 21, Spring Boot, Spring `JdbcTemplate`, Caffeine cache, protobuf, JUnit 5 + AssertJ + Mockito, ClickHouse SQL (Kafka engine + MergeTree + materialized view).

**Spec:** `docs/superpowers/specs/2026-05-31-flow-doc-node-label-ifname-design.md`

**Reference — exact current code:**
- Proto: `core/flow-enricher/src/main/proto/deltav-flows.proto` (NodeInfo fields 1-4; FlowDocument max field 47; 25 & 44 reserved)
- `core/flow-enricher/src/main/java/org/deltav/flows/enricher/enrichment/JdbcNodeInfoLookup.java`
- `core/flow-enricher/src/main/java/org/deltav/flows/enricher/mapping/FlowToDocumentMapper.java`
- `core/flow-enricher/src/main/java/org/deltav/flows/enricher/FlowEnrichmentFunction.java`
- `core/flow-enricher/src/main/java/org/deltav/flows/enricher/FlowEnricherConfiguration.java`
- DDL: `opennms-container/delta-v/clickhouse/init/{02-flows-raw,03-flows-kafka,20-flows-ingest}.sql`

**Build/test commands:**
- Module build: `./mvnw -q --projects :org.deltav.core.flow-enricher -am -DskipTests install`
- Module tests: `./mvnw -q --projects :org.deltav.core.flow-enricher test`
- (Confirm the exact module artifactId with `grep '<artifactId>' core/flow-enricher/pom.xml` if the `--projects` selector errors.)

---

### Task 1: Add the three proto fields

**Files:**
- Modify: `core/flow-enricher/src/main/proto/deltav-flows.proto`

- [ ] **Step 1: Add `node_label` to `NodeInfo`**

Change the `NodeInfo` message from:
```proto
message NodeInfo {
    uint32 node_id = 1;
    string foreign_source = 2;
    string foreign_id = 3;
    repeated string categories = 4;
}
```
to:
```proto
message NodeInfo {
    uint32 node_id = 1;
    string foreign_source = 2;
    string foreign_id = 3;
    repeated string categories = 4;
    // Human-readable node label (OnmsNode.label). Populated for all three
    // NodeInfo sub-messages by the enricher; only the exporter's is persisted
    // to ClickHouse today.
    string node_label = 5;
}
```

- [ ] **Step 2: Add `input_if_name` / `output_if_name` to `FlowDocument`**

In the `FlowDocument` message, immediately after the last field `google.protobuf.UInt32Value ecn = 47;` and before the closing `}`, add:
```proto
    // Resolved SNMP interface names for the exporter's input/output ifIndex
    // (snmpinterface.snmpifname). Empty when the exporter is unresolved or the
    // interface is unknown. Fields 25 and 44 are reserved (formerly removed).
    string input_if_name = 48;
    string output_if_name = 49;
```

- [ ] **Step 3: Build to regenerate protobuf Java sources**

Run: `./mvnw -q --projects :org.deltav.core.flow-enricher -am -DskipTests install`
Expected: BUILD SUCCESS; generated `FlowDocumentProtos` now has `setNodeLabel`, `setInputIfName`, `setOutputIfName`.

- [ ] **Step 4: Commit**

```bash
git add core/flow-enricher/src/main/proto/deltav-flows.proto
git commit -m "feat(flows): add node_label + input/output if_name to FlowDocument proto"
```

---

### Task 2: Carry `node_label` through `JdbcNodeInfoLookup`

**Files:**
- Modify: `core/flow-enricher/src/main/java/org/deltav/flows/enricher/enrichment/JdbcNodeInfoLookup.java`
- Test: `core/flow-enricher/src/test/java/org/deltav/flows/enricher/enrichment/JdbcNodeInfoLookupTest.java` (create if absent)

- [ ] **Step 1: Write the failing test**

Create/append `JdbcNodeInfoLookupTest.java`:
```java
package org.deltav.flows.enricher.enrichment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

class JdbcNodeInfoLookupTest {

    @Test
    void lookupByNodeIdCarriesNodeLabel() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        JdbcNodeInfoLookup.NodeInfo row =
                new JdbcNodeInfoLookup.NodeInfo(7, "nl6", "dev-7", "nl6-lab", "cisco-7");
        when(jdbc.query(any(String.class), any(RowMapper.class), eq(7)))
                .thenReturn(List.of(row));

        JdbcNodeInfoLookup lookup = new JdbcNodeInfoLookup(jdbc, Duration.ofMinutes(5));
        JdbcNodeInfoLookup.NodeInfo result = lookup.lookupByNodeId(7);

        assertThat(result).isNotNull();
        assertThat(result.nodeLabel()).isEqualTo("cisco-7");
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `./mvnw -q --projects :org.deltav.core.flow-enricher test -Dtest=JdbcNodeInfoLookupTest`
Expected: COMPILE FAILURE — `NodeInfo` has no 5-arg constructor / no `nodeLabel()` accessor.

- [ ] **Step 3: Add `nodeLabel` to the record, SQL, and row mapper**

In `JdbcNodeInfoLookup.java`:

Change the record (append `nodeLabel` last to minimize disruption):
```java
public record NodeInfo(int nodeId, String foreignSource, String foreignId, String location, String nodeLabel) {}
```

Change `SQL_BY_IP`:
```java
private static final String SQL_BY_IP =
        "SELECT n.nodeid, n.foreignsource, n.foreignid, n.location, n.nodelabel " +
        "FROM node n JOIN ipinterface i ON n.nodeid = i.nodeid " +
        "WHERE i.ipaddr = ? LIMIT 1";
```

Change `SQL_BY_NODE_ID`:
```java
private static final String SQL_BY_NODE_ID =
        "SELECT nodeid, foreignsource, foreignid, location, nodelabel FROM node WHERE nodeid = ?";
```

Change `ROW_MAPPER`:
```java
private static final RowMapper<NodeInfo> ROW_MAPPER = (rs, rowNum) -> new NodeInfo(
        rs.getInt("nodeid"),
        rs.getString("foreignsource"),
        rs.getString("foreignid"),
        rs.getString("location"),
        rs.getString("nodelabel"));
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `./mvnw -q --projects :org.deltav.core.flow-enricher test -Dtest=JdbcNodeInfoLookupTest`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add core/flow-enricher/src/main/java/org/deltav/flows/enricher/enrichment/JdbcNodeInfoLookup.java \
        core/flow-enricher/src/test/java/org/deltav/flows/enricher/enrichment/JdbcNodeInfoLookupTest.java
git commit -m "feat(flows): carry node_label through JdbcNodeInfoLookup"
```

---

### Task 3: New `JdbcSnmpInterfaceLookup` (resolve `snmpifname`)

**Files:**
- Create: `core/flow-enricher/src/main/java/org/deltav/flows/enricher/enrichment/JdbcSnmpInterfaceLookup.java`
- Test: `core/flow-enricher/src/test/java/org/deltav/flows/enricher/enrichment/JdbcSnmpInterfaceLookupTest.java`

- [ ] **Step 1: Write the failing test**

Create `JdbcSnmpInterfaceLookupTest.java`:
```java
package org.deltav.flows.enricher.enrichment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

class JdbcSnmpInterfaceLookupTest {

    @Test
    void returnsIfNameAndCachesIt() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.queryForList(anyString(), eq(String.class), eq(5), eq(3)))
                .thenReturn(List.of("Gi0/3"));

        JdbcSnmpInterfaceLookup lookup = new JdbcSnmpInterfaceLookup(jdbc, Duration.ofMinutes(5));

        assertThat(lookup.lookupIfName(5, 3)).isEqualTo("Gi0/3");
        assertThat(lookup.lookupIfName(5, 3)).isEqualTo("Gi0/3"); // second call cached
        verify(jdbc, times(1)).queryForList(anyString(), eq(String.class), eq(5), eq(3));
    }

    @Test
    void returnsNullWhenInterfaceMissing() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.queryForList(anyString(), eq(String.class), eq(5), eq(99)))
                .thenReturn(List.of());

        JdbcSnmpInterfaceLookup lookup = new JdbcSnmpInterfaceLookup(jdbc, Duration.ofMinutes(5));
        assertThat(lookup.lookupIfName(5, 99)).isNull();
    }

    @Test
    void skipsQueryForUnresolvedNode() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        JdbcSnmpInterfaceLookup lookup = new JdbcSnmpInterfaceLookup(jdbc, Duration.ofMinutes(5));
        assertThat(lookup.lookupIfName(0, 3)).isNull();
        verifyNoInteractions(jdbc);
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `./mvnw -q --projects :org.deltav.core.flow-enricher test -Dtest=JdbcSnmpInterfaceLookupTest`
Expected: COMPILE FAILURE — `JdbcSnmpInterfaceLookup` does not exist.

- [ ] **Step 3: Create the lookup class**

Create `JdbcSnmpInterfaceLookup.java`:
```java
/*
 * Copyright (C) 2026 BeaconStrategists, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.deltav.flows.enricher.enrichment;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * JDBC-based resolver for SNMP interface names. Maps a flow's exporter
 * {@code (nodeId, ifIndex)} to {@code snmpinterface.snmpifname}.
 *
 * <p>Backed by a Caffeine cache keyed by {@link IfKey}; negative results are
 * cached as {@link Optional#empty()} so unprovisioned interfaces do not retry
 * the database on every flow. Mirrors {@link JdbcNodeInfoLookup}.
 */
public class JdbcSnmpInterfaceLookup {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcSnmpInterfaceLookup.class);

    private static final String SQL =
            "SELECT snmpifname FROM snmpinterface WHERE nodeid = ? AND snmpifindex = ? LIMIT 1";

    /** Cache key: exporter node id + SNMP ifIndex. */
    public record IfKey(int nodeId, int ifIndex) {}

    private final JdbcTemplate jdbc;
    private final Cache<IfKey, Optional<String>> cache;

    public JdbcSnmpInterfaceLookup(JdbcTemplate jdbc, Duration cacheTtl) {
        this.jdbc = jdbc;
        this.cache = Caffeine.newBuilder()
                .expireAfterWrite(cacheTtl)
                .maximumSize(50_000)
                .build();
    }

    /**
     * @return the SNMP interface name, or {@code null} when the node is
     *         unresolved ({@code nodeId <= 0}), the interface is unknown, or
     *         the name is empty.
     */
    public String lookupIfName(int nodeId, int ifIndex) {
        if (nodeId <= 0) {
            return null;
        }
        return cache.get(new IfKey(nodeId, ifIndex), this::query).orElse(null);
    }

    private Optional<String> query(IfKey key) {
        try {
            List<String> results = jdbc.queryForList(SQL, String.class, key.nodeId(), key.ifIndex());
            if (results.isEmpty()) {
                return Optional.empty();
            }
            String name = results.getFirst();
            return (name == null || name.isEmpty()) ? Optional.empty() : Optional.of(name);
        } catch (Exception e) {
            LOG.debug("Interface name lookup failed for node {} ifindex {}: {}",
                    key.nodeId(), key.ifIndex(), e.getMessage());
            return Optional.empty();
        }
    }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `./mvnw -q --projects :org.deltav.core.flow-enricher test -Dtest=JdbcSnmpInterfaceLookupTest`
Expected: PASS (all three tests).

- [ ] **Step 5: Commit**

```bash
git add core/flow-enricher/src/main/java/org/deltav/flows/enricher/enrichment/JdbcSnmpInterfaceLookup.java \
        core/flow-enricher/src/test/java/org/deltav/flows/enricher/enrichment/JdbcSnmpInterfaceLookupTest.java
git commit -m "feat(flows): add JdbcSnmpInterfaceLookup for exporter interface names"
```

---

### Task 4: Populate the new proto fields in `FlowToDocumentMapper`

**Files:**
- Modify: `core/flow-enricher/src/main/java/org/deltav/flows/enricher/mapping/FlowToDocumentMapper.java`
- Test: `core/flow-enricher/src/test/java/org/deltav/flows/enricher/mapping/FlowToDocumentMapperTest.java`

> NOTE: `map(...)` gains two trailing params `String inputIfName, String outputIfName`. Every existing `mapper.map(...)` call in the test must add these two args, and every `new JdbcNodeInfoLookup.NodeInfo(...)` in the test must add the trailing `nodeLabel` arg (from Task 2). Update them in this task.

- [ ] **Step 1: Write the failing test**

Append to `FlowToDocumentMapperTest.java` (use the same `Flow` mock/builder the existing tests use; the snippet below assumes a minimal `flow` from `@BeforeEach`):
```java
    @Test
    void setsExporterNodeLabelAndInterfaceNames() {
        JdbcNodeInfoLookup.NodeInfo exporter =
                new JdbcNodeInfoLookup.NodeInfo(7, "nl6", "dev-7", "nl6-lab", "cisco-7");

        FlowDocumentProtos.FlowDocument doc = mapper.map(
                flow, exporter, null, null,
                "HTTPS", "PRIVATE", "PUBLIC", "PRIVATE",
                "10.0.0.7", "nl6-lab", 0L,
                "Gi0/1", "Gi0/2");

        assertThat(doc.getExporterNode().getNodeLabel()).isEqualTo("cisco-7");
        assertThat(doc.getInputIfName()).isEqualTo("Gi0/1");
        assertThat(doc.getOutputIfName()).isEqualTo("Gi0/2");
    }

    @Test
    void leavesInterfaceNamesEmptyWhenNull() {
        FlowDocumentProtos.FlowDocument doc = mapper.map(
                flow, null, null, null,
                "HTTPS", "PRIVATE", "PUBLIC", "PRIVATE",
                "10.0.0.7", "nl6-lab", 0L,
                null, null);

        assertThat(doc.getInputIfName()).isEmpty();
        assertThat(doc.getOutputIfName()).isEmpty();
    }
```

Also update the two pre-existing `mapper.map(...)` calls (around lines 61 and 84) to add `, null, null` as the final two arguments, and any existing `new JdbcNodeInfoLookup.NodeInfo(...)` constructions to add a trailing label arg (e.g. `, "label"`).

- [ ] **Step 2: Run the test to verify it fails**

Run: `./mvnw -q --projects :org.deltav.core.flow-enricher test -Dtest=FlowToDocumentMapperTest`
Expected: COMPILE FAILURE — `map(...)` has no overload taking the two trailing `String` args.

- [ ] **Step 3: Extend `map(...)` and `toProtoNodeInfo(...)`**

In `FlowToDocumentMapper.java`:

(a) Add two params to the `map(...)` signature — change the final line of the parameter list from:
```java
            long clockCorrection) {
```
to:
```java
            long clockCorrection,
            String inputIfName,
            String outputIfName) {
```
Update the method Javadoc to document `inputIfName` / `outputIfName` ("resolved exporter SNMP interface names; null/empty leaves the proto field unset").

(b) Set the interface-name fields. Immediately after the existing `host`/`location` block (the `if (location != null && !location.isEmpty()) { builder.setLocation(location); }` block), add:
```java
        // Exporter SNMP interface names (resolved by the caller from the
        // exporter node's snmpinterface). proto3 scalar strings default to "",
        // so only write non-empty values.
        if (inputIfName != null && !inputIfName.isEmpty()) {
            builder.setInputIfName(inputIfName);
        }
        if (outputIfName != null && !outputIfName.isEmpty()) {
            builder.setOutputIfName(outputIfName);
        }
```

(c) Set `node_label` in `toProtoNodeInfo(...)`. After the `foreign_id` block and before the `return b.build();`, add:
```java
        if (info.nodeLabel() != null && !info.nodeLabel().isEmpty()) {
            b.setNodeLabel(info.nodeLabel());
        }
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `./mvnw -q --projects :org.deltav.core.flow-enricher test -Dtest=FlowToDocumentMapperTest`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add core/flow-enricher/src/main/java/org/deltav/flows/enricher/mapping/FlowToDocumentMapper.java \
        core/flow-enricher/src/test/java/org/deltav/flows/enricher/mapping/FlowToDocumentMapperTest.java
git commit -m "feat(flows): map exporter node_label + input/output if_name into FlowDocument"
```

---

### Task 5: Wire the interface lookup into config + caller

**Files:**
- Modify: `core/flow-enricher/src/main/java/org/deltav/flows/enricher/FlowEnricherConfiguration.java`
- Modify: `core/flow-enricher/src/main/java/org/deltav/flows/enricher/FlowEnrichmentFunction.java`

> This wiring is integration-level (Spring beans + Kafka stream function); it is verified by compilation + the existing context/integration tests rather than a new unit test.

- [ ] **Step 1: Add the `JdbcSnmpInterfaceLookup` bean**

In `FlowEnricherConfiguration.java`, add the import:
```java
import org.deltav.flows.enricher.enrichment.JdbcSnmpInterfaceLookup;
```
and a bean (place it next to the existing `jdbcNodeInfoLookup` bean, reusing the same `flowEnricherJdbcTemplate`):
```java
    @Bean
    JdbcSnmpInterfaceLookup jdbcSnmpInterfaceLookup(
            JdbcTemplate flowEnricherJdbcTemplate,
            @Value("${deltav.flows.interface-lookup.cache-ttl:5m}") Duration cacheTtl) {
        return new JdbcSnmpInterfaceLookup(flowEnricherJdbcTemplate, cacheTtl);
    }
```
If `FlowEnrichmentFunction` is itself constructed by an `@Bean` method in this class, add the new lookup as a parameter to that method and pass it to the constructor (see Step 2).

- [ ] **Step 2: Inject + use it in `FlowEnrichmentFunction`**

In `FlowEnrichmentFunction.java`:

(a) Import + field:
```java
import org.deltav.flows.enricher.enrichment.JdbcSnmpInterfaceLookup;
```
Add a field alongside `private final JdbcNodeInfoLookup nodeInfoLookup;`:
```java
    private final JdbcSnmpInterfaceLookup snmpInterfaceLookup;
```
Add it to the constructor parameter list (next to `JdbcNodeInfoLookup nodeInfoLookup`) and assign `this.snmpInterfaceLookup = snmpInterfaceLookup;`.

(b) Resolve the names in the existing exporter block. That block currently reads (around lines 220-227):
```java
        if (exporterNodeInfo != null) {
            Integer inputIfIndex = flow.getInputSnmp();
            if (inputIfIndex != null) {
                interfaceMarkingCache.markIfNeeded(exporterNodeInfo.nodeId(), inputIfIndex);
            }
            Integer outputIfIndex = flow.getOutputSnmp();
            if (outputIfIndex != null) {
                interfaceMarkingCache.markIfNeeded(exporterNodeInfo.nodeId(), outputIfIndex);
            }
        }
```
Introduce two locals initialised to `null` *before* this block, and resolve them inside it:
```java
        String inputIfName = null;
        String outputIfName = null;
        if (exporterNodeInfo != null) {
            Integer inputIfIndex = flow.getInputSnmp();
            if (inputIfIndex != null) {
                interfaceMarkingCache.markIfNeeded(exporterNodeInfo.nodeId(), inputIfIndex);
                inputIfName = snmpInterfaceLookup.lookupIfName(exporterNodeInfo.nodeId(), inputIfIndex);
            }
            Integer outputIfIndex = flow.getOutputSnmp();
            if (outputIfIndex != null) {
                interfaceMarkingCache.markIfNeeded(exporterNodeInfo.nodeId(), outputIfIndex);
                outputIfName = snmpInterfaceLookup.lookupIfName(exporterNodeInfo.nodeId(), outputIfIndex);
            }
        }
```

(c) Pass them to the mapper. The `flowToDocumentMapper.map(...)` call (around line 236) currently ends with `... clockCorrection)`. Add the two new trailing args matching Task 4's signature, e.g.:
```java
        FlowDocumentProtos.FlowDocument doc = flowToDocumentMapper.map(
                flow,
                exporterNodeInfo,
                srcNodeInfo,
                dstNodeInfo,
                /* ...existing args through clockCorrection... */,
                inputIfName,
                outputIfName);
```
(Insert `inputIfName, outputIfName` as the final two arguments, preserving every existing argument in between.)

- [ ] **Step 3: Build the module**

Run: `./mvnw -q --projects :org.deltav.core.flow-enricher -am test`
Expected: BUILD SUCCESS, all existing + new tests pass.

- [ ] **Step 4: Commit**

```bash
git add core/flow-enricher/src/main/java/org/deltav/flows/enricher/FlowEnricherConfiguration.java \
        core/flow-enricher/src/main/java/org/deltav/flows/enricher/FlowEnrichmentFunction.java
git commit -m "feat(flows): resolve exporter interface names and pass to the mapper"
```

---

### Task 6: ClickHouse schema — columns, kafka table, ingest view

**Files:**
- Modify: `opennms-container/delta-v/clickhouse/init/02-flows-raw.sql`
- Modify: `opennms-container/delta-v/clickhouse/init/03-flows-kafka.sql`
- Modify: `opennms-container/delta-v/clickhouse/init/20-flows-ingest.sql`

> CRITICAL: the materialized view inserts into `flows_raw` **by column position**. The new `flows_raw` columns and the new MV `SELECT` expressions MUST appear in the same order: `exporter_node_label` directly after `exporter_node_categories`; `input_if_name` + `output_if_name` directly after `output_snmp_ifindex`.

- [ ] **Step 1: `02-flows-raw.sql` — add columns to CREATE + idempotent ALTERs**

In the `CREATE TABLE deltav.flows_raw` column list, change the exporter-node block so it reads (add `exporter_node_label` after `exporter_node_categories`, and `input_if_name` / `output_if_name` after `output_snmp_ifindex`):
```sql
    -- Exporter node inventory (from NodeInfo exporter_node)
    exporter_node_id               UInt32,
    exporter_node_foreign_source   LowCardinality(String),
    exporter_node_foreign_id       String,
    exporter_node_categories       Array(LowCardinality(String)),
    exporter_node_label            LowCardinality(String),
    input_snmp_ifindex             UInt32,
    output_snmp_ifindex            Nullable(UInt32),
    input_if_name                  LowCardinality(String),
    output_if_name                 LowCardinality(String),
```
Then, at the **end of the file** (after the `CREATE TABLE ... SETTINGS ...;` statement), append idempotent migrations for existing tables:
```sql

-- Additive migration for clusters created before these columns existed.
-- ADD COLUMN IF NOT EXISTS is a no-op once applied. Positions are chosen to
-- match the flows_ingest SELECT order (the MV inserts by position).
ALTER TABLE deltav.flows_raw ADD COLUMN IF NOT EXISTS exporter_node_label LowCardinality(String) AFTER exporter_node_categories;
ALTER TABLE deltav.flows_raw ADD COLUMN IF NOT EXISTS input_if_name  LowCardinality(String) AFTER output_snmp_ifindex;
ALTER TABLE deltav.flows_raw ADD COLUMN IF NOT EXISTS output_if_name LowCardinality(String) AFTER input_if_name;
```

- [ ] **Step 2: `03-flows-kafka.sql` — drop the MV, recreate the kafka table with the new fields**

Because `flows_kafka` and `flows_ingest` are stateless and `ALTER` on a Kafka-engine table (especially `Tuple` evolution) is finicky, recreate them. Drop the MV **first** (it reads `flows_kafka`), then DROP+CREATE the kafka table.

At the **top** of `03-flows-kafka.sql`, before the `CREATE TABLE`, add:
```sql
-- flows_ingest reads flows_kafka; drop it before recreating the kafka table so
-- no MV is left attached to a dropped table. It is recreated in 20-flows-ingest.sql.
DROP VIEW IF EXISTS deltav.flows_ingest;
DROP TABLE IF EXISTS deltav.flows_kafka;
```
Change `CREATE TABLE IF NOT EXISTS deltav.flows_kafka` to `CREATE TABLE deltav.flows_kafka` (unconditional, since we just dropped it). Add `node_label String` to the `exporter_node` Tuple and add the two top-level `String` columns after `output_snmp_ifindex`. The exporter-node tuple + ifindex block becomes:
```sql
    exporter_node           Tuple(
        node_id             UInt32,
        foreign_source      String,
        foreign_id          String,
        categories          Array(String),
        node_label          String
    ),
    dest_node               Tuple(
        node_id             UInt32,
        foreign_source      String,
        foreign_id          String,
        categories          Array(String)
    ),

    -- SNMP ifindex lives at the top level of the proto, not in NodeInfo
    input_snmp_ifindex      Tuple(value UInt32),
    output_snmp_ifindex     Tuple(value UInt32),

    -- Resolved interface names (top-level proto strings)
    input_if_name           String,
    output_if_name          String
```
(Leave `src_node` and `dest_node` tuples without `node_label` — the proto carries it but ClickHouse only extracts the exporter's.)

- [ ] **Step 3: `20-flows-ingest.sql` — recreate the MV with the new projections in matching order**

Change `CREATE MATERIALIZED VIEW IF NOT EXISTS deltav.flows_ingest` to `CREATE MATERIALIZED VIEW deltav.flows_ingest` (it was dropped in `03`). Add `exporter_node.node_label AS exporter_node_label` after the `exporter_node_categories` projection, and `input_if_name` / `output_if_name` after the `output_snmp_ifindex` projection. The exporter block of the SELECT becomes:
```sql
    ifNull(exporter_node.node_id, 0)                 AS exporter_node_id,
    exporter_node.foreign_source                     AS exporter_node_foreign_source,
    exporter_node.foreign_id                         AS exporter_node_foreign_id,
    exporter_node.categories                         AS exporter_node_categories,
    exporter_node.node_label                         AS exporter_node_label,
    ifNull(input_snmp_ifindex.value, 0)              AS input_snmp_ifindex,
    toNullable(output_snmp_ifindex.value)            AS output_snmp_ifindex,
    input_if_name                                    AS input_if_name,
    output_if_name                                   AS output_if_name,
```
(The `src_node` / `dest_node` projection blocks that follow are unchanged.)

- [ ] **Step 4: Validate the SQL parses (syntax check)**

Run a throwaway ClickHouse to validate the three files parse cleanly (multi-statement, `--multiquery`):
```bash
cd opennms-container/delta-v
docker run --rm -v "$PWD/clickhouse/init:/init:ro" clickhouse/clickhouse-server:latest \
  sh -c 'clickhouse local --multiquery --query "$(sed s/\${DELTAV_CLICKHOUSE_FLOWS_RAW_TTL_DAYS}/30/ /init/01-database.sql /init/02-flows-raw.sql)"' 2>&1 | tail -5 || true
```
Expected: no syntax errors on `02-flows-raw.sql` (the Kafka/MV files need a running broker, so they are validated end-to-end in Task 7, not here). At minimum, eyeball that column lists in `02`/`03`/`20` are in the matching order described above.

- [ ] **Step 5: Commit**

```bash
git add opennms-container/delta-v/clickhouse/init/02-flows-raw.sql \
        opennms-container/delta-v/clickhouse/init/03-flows-kafka.sql \
        opennms-container/delta-v/clickhouse/init/20-flows-ingest.sql
git commit -m "feat(flows): persist exporter_node_label + input/output_if_name in ClickHouse"
```

---

### Task 7: End-to-end verification

**Files:** none (verification only)

- [ ] **Step 1: Build the flow-enricher + clickhouse images**

Run:
```bash
cd opennms-container/delta-v
docker buildx build --load -t deltav/flow-enricher:dev -f Dockerfile.daemon-per \
  --build-arg DAEMON_NAME=flow-enricher --build-arg DAEMON_BASE_IMAGE=deltav/daemon-base \
  --build-arg VERSION=dev --build-arg MAIN_CLASS="$(cat staging/flow-enricher/.main_class 2>/dev/null || echo SKIP)" . 2>&1 | tail -3
```
(Or simply `make daemon-image DAEMON=flow-enricher` if staging is already prepared, plus a clickhouse image rebuild so the baked proto is current.)
Expected: both images build; the clickhouse image bakes the updated `deltav-flows.proto`.

- [ ] **Step 2: Deploy and confirm flows carry the new fields**

Bring up a stack (`make up PROFILE=demo`), wait for the enricher node-context cache to warm (~3-5 min), then query:
```bash
docker exec delta-v-clickhouse clickhouse-client -u deltav --password deltav --query \
  "SELECT host, exporter_node_label, input_if_name, output_if_name FROM deltav.flows_raw WHERE host LIKE '10.0.0.%' AND timestamp > now()-INTERVAL 10 MINUTE LIMIT 10 FORMAT PrettyCompact"
```
Expected: rows show a populated `exporter_node_label` (e.g. `cisco-catalyst-9500-10.0.0.1`) and, where the exporter's `snmpinterface` rows exist, populated `input_if_name` / `output_if_name`. Empty strings are acceptable for exporters without resolved SNMP interfaces.

- [ ] **Step 3: Confirm the idempotent migration on an existing volume**

Re-run the clickhouse-init against a pre-existing `clickhouse-data` volume (i.e. without wiping it) and confirm `DESCRIBE deltav.flows_raw` lists the three new columns and that ingest continues (no consumer-group reset errors in the clickhouse logs).

- [ ] **Step 4: Final review commit (if any verification fixes were needed)**

Commit any fixes discovered during verification with a `fix(flows): …` message.

---

## Self-Review

**Spec coverage:**
- Proto: `node_label`=5, `input_if_name`=48, `output_if_name`=49 → Task 1. ✓
- Enricher `node_label` (JdbcNodeInfoLookup + toProtoNodeInfo) → Tasks 2, 4. ✓
- Enricher `if_name` (new JdbcSnmpInterfaceLookup + caller wiring) → Tasks 3, 5. ✓
- ClickHouse columns + idempotent ALTER (flows_raw) + recreate (kafka, MV) → Task 6. ✓
- node_label populated on all three NodeInfo, persisted exporter-only → Task 4 (toProtoNodeInfo is shared) + Task 6 (only `exporter_node_label` column). ✓
- if_name from the exporter node, input+output → Task 5 resolves from `exporterNodeInfo.nodeId()`. ✓
- LowCardinality(String); migration ordering (position-based MV) → Task 6 (explicit ordering note). ✓
- Tests → Tasks 2, 3, 4. ✓

**Type consistency:** `NodeInfo` record gains trailing `String nodeLabel` (Task 2), consumed via `info.nodeLabel()` (Task 4); `map(...)` gains trailing `String inputIfName, String outputIfName` (Task 4), supplied by the caller (Task 5) and by the tests (Task 4). `JdbcSnmpInterfaceLookup.lookupIfName(int,int)` returns `String`/null, used directly. Proto accessors `getNodeLabel()/getInputIfName()/getOutputIfName()` match the proto field names. Consistent.

**No placeholders:** every code step shows complete code; SQL column order is explicit; verification queries are concrete.
