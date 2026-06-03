# Shared node-context-consumer + flow-enricher JDBC migration — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract the duplicated NodeContext Kafka consumer into a shared `core/node-context-consumer` library with a complete lookup API (`getByIp` + `ifName`), refactor alerts-forwarder + prometheus-writer onto it, and migrate the flow-enricher off JDBC so per-flow enrichment is a pure in-memory map lookup (zero DB in the hot path).

**Architecture:** A shared module owns `NodeContextCache` (three indexes: `{location}@{node_id}` key, `node_id`, and normalized-IP), `NodeContextKafkaBootstrap` (HWM→seek-earliest→drain→ready→live-tail), `NodeContextCacheReadyEvent`, `NodeContextCacheHealthIndicator`, and a Spring `@AutoConfiguration` wiring them as `@Bean`s. The flow-enricher consumes the cache for exporter/src/dst (by IP) and `if_name` (by node+ifIndex), gates its flow binding on `NodeContextCacheReadyEvent`, and keeps its `DataSource` only for `InterfaceMarkingCache`'s `hasflows` write.

**Tech Stack:** Java 21, Spring Boot, Spring Cloud Stream (Kafka binder), Apache Kafka client, protobuf (`org.deltav.timeseries.proto.NodeContext`), JUnit 5 + AssertJ + Mockito, Testcontainers-Kafka.

**Spec:** `docs/superpowers/specs/2026-06-01-node-context-consumer-migration-design.md`

**Reference facts (verified):**
- Proto java package: `org.deltav.timeseries.proto` (`NodeContext`, `InterfaceContext`, `SnmpInterfaceContext`). Accessors: `getNodeId()`, `getLocation()`, `getNodeLabel()`, `getForeignSource()`, `getForeignId()`, `getCategoriesList()`, `getInterfaceMetadataMap()` (`Map<String,InterfaceContext>`), `getSnmpInterfaceMetadataMap()` (`Map<Integer,SnmpInterfaceContext>`), `getDeleted()`; `SnmpInterfaceContext.getIfName()`.
- kafka-contracts dep artifactId: `org.opennms.core.deltav-kafka-contracts`.
- Producer IP key contract: `NodeToProtobufTranslator:70` `iface.getIpAddressAsString()` → `InetAddress.getHostAddress()` (IPv4 no leading zeros, compressed IPv6).
- Existing consumer copies (line-identical 88/195): `core/{alerts-forwarder,prometheus-writer}/src/main/java/org/deltav/{alerts/forwarder,prometheus/writer}/nodecontext/`. prometheus-writer additionally has `NodeContextCacheHealthIndicator`; alerts-forwarder + prometheus-writer both have `NodeContextCacheReadyEvent`.
- flow-enricher SCS: function `enrichFlows`, input binding `enrichFlows-in-0` (group `deltav-flow-enricher`), output `enrichFlows-out-0`. Config: `core/flow-enricher/src/main/resources/application.yml`.

**Module build selector:** the new module's artifactId is `org.opennms.core.node-context-consumer` (see Task 1). Build a module with `./mvnw -q --projects :<artifactId> -am ...`.

---

## Phase A — the shared `core/node-context-consumer` module

### Task 1: Create the module skeleton

**Files:**
- Create: `core/node-context-consumer/pom.xml`
- Modify: `core/pom.xml` (or the root reactor `<modules>` list — locate with `grep -rl '<module>.*alerts-forwarder' core/pom.xml pom.xml`)

- [ ] **Step 1: Create `core/node-context-consumer/pom.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>org.opennms.core</groupId>
        <artifactId>delta-v-parent</artifactId>
        <version>1.3.0-rc7</version>
        <relativePath>../../pom.xml</relativePath>
    </parent>

    <artifactId>org.opennms.core.node-context-consumer</artifactId>
    <name>Delta-V :: Node Context Consumer</name>
    <packaging>jar</packaging>

    <dependencies>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-actuator</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-autoconfigure</artifactId>
        </dependency>
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-clients</artifactId>
        </dependency>
        <dependency>
            <groupId>org.opennms.core</groupId>
            <artifactId>org.opennms.core.deltav-kafka-contracts</artifactId>
            <version>${project.version}</version>
        </dependency>
        <dependency>
            <groupId>com.google.protobuf</groupId>
            <artifactId>protobuf-java</artifactId>
        </dependency>
        <dependency>
            <groupId>io.micrometer</groupId>
            <artifactId>micrometer-core</artifactId>
        </dependency>

        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>
</project>
```
(Confirm exact `<version>`, group coordinates, and whether the parent pins versions by copying the matching lines from `core/alerts-forwarder/pom.xml`. Use the same `delta-v-parent` version as the rest of the reactor — currently `1.3.0-rc7`.)

- [ ] **Step 2: Register the module in the reactor**

Find the modules list (`grep -n 'alerts-forwarder' core/pom.xml`) and add, next to the other `core/*` modules:
```xml
        <module>node-context-consumer</module>
```
(If `core/*` modules are listed in the root `pom.xml` instead, add it there in the same form the siblings use.)

- [ ] **Step 3: Verify the module resolves in the reactor**

Run: `./mvnw -q --projects :org.opennms.core.node-context-consumer -am -DskipTests install`
Expected: BUILD SUCCESS (empty module compiles).

- [ ] **Step 4: Commit**

```bash
git add core/node-context-consumer/pom.xml core/pom.xml pom.xml
git commit -m "feat(node-context): scaffold core/node-context-consumer module"
```

---

### Task 2: Move the bootstrap + ready event verbatim into the module

These are pure relocations — the only change is the `package` line (and the `NodeContextCache` import resolving within the new package). Source of truth is the alerts-forwarder copy.

**Files:**
- Create: `core/node-context-consumer/src/main/java/org/deltav/nodecontext/NodeContextKafkaBootstrap.java` (copy of `core/alerts-forwarder/.../nodecontext/NodeContextKafkaBootstrap.java`)
- Create: `core/node-context-consumer/src/main/java/org/deltav/nodecontext/NodeContextCacheReadyEvent.java` (copy of the alerts-forwarder copy)

- [ ] **Step 1: Copy both files into the new package**

```bash
mkdir -p core/node-context-consumer/src/main/java/org/deltav/nodecontext
cp core/alerts-forwarder/src/main/java/org/deltav/alerts/forwarder/nodecontext/NodeContextKafkaBootstrap.java \
   core/node-context-consumer/src/main/java/org/deltav/nodecontext/NodeContextKafkaBootstrap.java
cp core/alerts-forwarder/src/main/java/org/deltav/alerts/forwarder/nodecontext/NodeContextCacheReadyEvent.java \
   core/node-context-consumer/src/main/java/org/deltav/nodecontext/NodeContextCacheReadyEvent.java
```

- [ ] **Step 2: Rewrite the `package` declaration and drop the `@Component`**

In both new files change the first `package …;` line to:
```java
package org.deltav.nodecontext;
```
In `NodeContextKafkaBootstrap.java`, remove the `@Component` annotation and its `import org.springframework.stereotype.Component;` (it will be declared as a `@Bean` by the auto-config in Task 5 so it is portable across services). Leave **all other logic byte-for-byte unchanged** (constructor, `run()`, HWM/seek/drain loop, `markReady()`, the `NodeContextCacheReadyEvent` publish). The `NodeContextCache` reference now resolves to the class created in Task 3 (same package).

- [ ] **Step 3: Compile**

Run: `./mvnw -q --projects :org.opennms.core.node-context-consumer -am -DskipTests install`
Expected: FAIL — `NodeContextCache` does not yet exist in this package (created in Task 3). This is expected; proceed.

- [ ] **Step 4: Commit (after Task 3 compiles)** — defer the commit to Task 3's commit so the module compiles. Mark this task done once Task 3 builds.

---

### Task 3: Move + extend `NodeContextCache` (the three indexes + lookup API)

**Files:**
- Create: `core/node-context-consumer/src/main/java/org/deltav/nodecontext/NodeContextCache.java`
- Create: `core/node-context-consumer/src/main/java/org/deltav/nodecontext/IpNormalizer.java`
- Test: `core/node-context-consumer/src/test/java/org/deltav/nodecontext/NodeContextCacheTest.java`

- [ ] **Step 1: Write `IpNormalizer` (used by the cache and by consumers)**

Create `IpNormalizer.java`:
```java
/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.nodecontext;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Canonicalizes IP address strings to the same form provisiond uses for
 * NodeContext.interface_metadata keys: OnmsIpInterface.getIpAddressAsString()
 * -> InetAddress.getHostAddress() (IPv4 dotted-decimal without leading zeros,
 * compressed IPv6). All getByIp lookups and index keys MUST pass through here.
 */
public final class IpNormalizer {
    private IpNormalizer() {}

    /** @return the canonical host-address form, or {@code null} if the input is null/blank/unparseable. */
    public static String normalize(String ip) {
        if (ip == null || ip.isBlank()) {
            return null;
        }
        try {
            // getByName parses literals without DNS for valid IPv4/IPv6 text.
            return InetAddress.getByName(ip.trim()).getHostAddress();
        } catch (UnknownHostException e) {
            return null;
        }
    }
}
```

- [ ] **Step 2: Write the failing cache test**

Create `NodeContextCacheTest.java`:
```java
/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.nodecontext;

import static org.assertj.core.api.Assertions.assertThat;

import org.deltav.timeseries.proto.InterfaceContext;
import org.deltav.timeseries.proto.NodeContext;
import org.deltav.timeseries.proto.SnmpInterfaceContext;
import org.junit.jupiter.api.Test;

class NodeContextCacheTest {

    private static NodeContext node(int id, String location, String label) {
        return NodeContext.newBuilder()
                .setNodeId(id).setLocation(location).setNodeLabel(label)
                .build();
    }

    private static String key(String location, int id) {
        return location + "@" + id;
    }

    @Test
    void getByNodeIdAndByKeyResolve() {
        NodeContextCache c = new NodeContextCache();
        NodeContext n = node(7, "Default", "router-7");
        c.applyUpdate(key("Default", 7), n);

        assertThat(c.getByKey("Default@7")).contains(n);
        assertThat(c.getByNodeId(7)).contains(n);
    }

    @Test
    void getByIpUsesInterfaceMetadataAndNormalizes() {
        NodeContextCache c = new NodeContextCache();
        NodeContext n = node(7, "Default", "router-7").toBuilder()
                .putInterfaceMetadata("10.0.0.1", InterfaceContext.getDefaultInstance())
                .build();
        c.applyUpdate(key("Default", 7), n);

        // Divergent textual forms all canonicalize to 10.0.0.1 and hit.
        assertThat(c.getByIp("10.0.0.1")).contains(n);
        assertThat(c.getByIp("010.000.000.001")).contains(n);
        assertThat(c.getByIp("::ffff:10.0.0.1")).contains(n); // IPv4-mapped IPv6
        assertThat(c.getByIp("198.51.100.9")).isEmpty();
    }

    @Test
    void getByIpHandlesCompressedVsExpandedIpv6() {
        NodeContextCache c = new NodeContextCache();
        NodeContext n = node(8, "Default", "v6").toBuilder()
                .putInterfaceMetadata("2001:db8::1", InterfaceContext.getDefaultInstance())
                .build();
        c.applyUpdate(key("Default", 8), n);

        assertThat(c.getByIp("2001:db8:0:0:0:0:0:1")).contains(n); // expanded
        assertThat(c.getByIp("2001:DB8::1")).contains(n);          // upper-case
    }

    @Test
    void ifNameResolvesFromSnmpInterfaceMetadata() {
        NodeContextCache c = new NodeContextCache();
        NodeContext n = node(7, "Default", "router-7").toBuilder()
                .putSnmpInterfaceMetadata(3, SnmpInterfaceContext.newBuilder()
                        .setIfIndex(3).setIfName("Gi0/3").build())
                .build();
        c.applyUpdate(key("Default", 7), n);

        assertThat(c.ifName(7, 3)).contains("Gi0/3");
        assertThat(c.ifName(7, 99)).isEmpty();   // unknown ifIndex
        assertThat(c.ifName(404, 3)).isEmpty();  // unknown node
    }

    @Test
    void tombstoneEvictsKeyNodeIdAndIps() {
        NodeContextCache c = new NodeContextCache();
        NodeContext n = node(7, "Default", "router-7").toBuilder()
                .putInterfaceMetadata("10.0.0.1", InterfaceContext.getDefaultInstance())
                .build();
        c.applyUpdate(key("Default", 7), n);
        assertThat(c.getByIp("10.0.0.1")).isPresent();

        c.applyUpdate(key("Default", 7), NodeContext.newBuilder()
                .setNodeId(7).setLocation("Default").setDeleted(true).build());

        assertThat(c.getByKey("Default@7")).isEmpty();
        assertThat(c.getByNodeId(7)).isEmpty();
        assertThat(c.getByIp("10.0.0.1")).isEmpty();
    }

    @Test
    void ipConflictIsLastWriteWins() {
        NodeContextCache c = new NodeContextCache();
        NodeContext a = node(1, "Default", "a").toBuilder()
                .putInterfaceMetadata("10.0.0.30", InterfaceContext.getDefaultInstance()).build();
        NodeContext b = node(2, "Default", "b").toBuilder()
                .putInterfaceMetadata("10.0.0.30", InterfaceContext.getDefaultInstance()).build();
        c.applyUpdate(key("Default", 1), a);
        c.applyUpdate(key("Default", 2), b);

        assertThat(c.getByIp("10.0.0.30")).contains(b); // last writer wins
        // Removing the loser does not clobber the winner's IP entry.
        c.applyUpdate(key("Default", 1), NodeContext.newBuilder()
                .setNodeId(1).setLocation("Default").setDeleted(true).build());
        assertThat(c.getByIp("10.0.0.30")).contains(b);
    }

    @Test
    void updateRemovesStaleIpsFromPreviousVersion() {
        NodeContextCache c = new NodeContextCache();
        c.applyUpdate(key("Default", 7), node(7, "Default", "r").toBuilder()
                .putInterfaceMetadata("10.0.0.1", InterfaceContext.getDefaultInstance()).build());
        // Re-publish node 7 with a different IP.
        c.applyUpdate(key("Default", 7), node(7, "Default", "r").toBuilder()
                .putInterfaceMetadata("10.0.0.2", InterfaceContext.getDefaultInstance()).build());

        assertThat(c.getByIp("10.0.0.1")).isEmpty();    // stale IP gone
        assertThat(c.getByIp("10.0.0.2")).isPresent();
    }
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `./mvnw -q --projects :org.opennms.core.node-context-consumer test -Dtest=NodeContextCacheTest`
Expected: COMPILE FAILURE — `NodeContextCache` does not exist.

- [ ] **Step 4: Write the extended cache**

Create `NodeContextCache.java`:
```java
/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.nodecontext;

import org.deltav.timeseries.proto.NodeContext;
import org.deltav.timeseries.proto.SnmpInterfaceContext;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * In-memory cache of {@link NodeContext} records with a complete lookup API:
 * by {@code {location}@{node_id}} key, by {@code node_id}, and by normalized IP
 * (from {@code interface_metadata}). Reads are lock-free {@link ConcurrentHashMap}
 * gets on the flow hot path; all mutation is confined to the single Kafka
 * consumer thread owned by {@link NodeContextKafkaBootstrap}.
 *
 * <p>IP-conflict policy: if two nodes claim the same IP, the IP index is
 * last-write-wins (mirrors the old JDBC {@code WHERE ipaddr = ? LIMIT 1}).
 */
public class NodeContextCache {

    private final ConcurrentHashMap<String, NodeContext> byKey = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, NodeContext> byNodeId = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, NodeContext> byIp = new ConcurrentHashMap<>();
    private final AtomicBoolean ready = new AtomicBoolean(false);

    public Optional<NodeContext> getByKey(String key) {
        return Optional.ofNullable(byKey.get(key));
    }

    public Optional<NodeContext> getByNodeId(int nodeId) {
        return Optional.ofNullable(byNodeId.get(nodeId));
    }

    /** @param ip a raw address string; it is normalized internally before lookup. */
    public Optional<NodeContext> getByIp(String ip) {
        String norm = IpNormalizer.normalize(ip);
        return norm == null ? Optional.empty() : Optional.ofNullable(byIp.get(norm));
    }

    /** @return the SNMP interface name for the node's ifIndex, or empty when unknown. */
    public Optional<String> ifName(int nodeId, int ifIndex) {
        NodeContext n = byNodeId.get(nodeId);
        if (n == null) {
            return Optional.empty();
        }
        SnmpInterfaceContext sic = n.getSnmpInterfaceMetadataMap().get(ifIndex);
        if (sic == null || sic.getIfName().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(sic.getIfName());
    }

    /**
     * Apply a record from {@code deltav-node-context}. On {@code deleted=true},
     * evicts the node from all three indexes (looking up the prior record to
     * find its IPs, since a tombstone usually omits interface_metadata).
     * On update, removes the prior version's IPs before indexing the new one.
     */
    public void applyUpdate(String key, NodeContext value) {
        NodeContext prior = byKey.get(key);
        if (prior != null) {
            // Drop the prior version's IP entries (only if still mapped to it).
            for (String ip : prior.getInterfaceMetadataMap().keySet()) {
                String norm = IpNormalizer.normalize(ip);
                if (norm != null) {
                    byIp.remove(norm, prior);
                }
            }
        }
        if (value.getDeleted()) {
            byKey.remove(key);
            if (prior != null) {
                byNodeId.remove(prior.getNodeId(), prior);
            } else {
                byNodeId.remove(value.getNodeId());
            }
            return;
        }
        byKey.put(key, value);
        byNodeId.put(value.getNodeId(), value);
        for (String ip : value.getInterfaceMetadataMap().keySet()) {
            String norm = IpNormalizer.normalize(ip);
            if (norm != null) {
                byIp.put(norm, value);
            }
        }
    }

    public int size() {
        return byKey.size();
    }

    public boolean isReady() {
        return ready.get();
    }

    public void markReady() {
        ready.set(true);
    }

    /** Immutable snapshot of every cached entry. O(n); not for the hot path. */
    public Collection<NodeContext> snapshot() {
        return List.copyOf(byKey.values());
    }
}
```

> NOTE: the previous consumers called `get(key)` and `findByNodeId(nodeId)`. The shared API renames these to `getByKey` / `getByNodeId`. Tasks 6 and 7 update the two services' call sites accordingly. `put`/`remove` are dropped from the public API (mutation is internal via `applyUpdate`); if `NodeContextKafkaBootstrap` called `put`/`remove` directly, change it to call `applyUpdate(key, value)` (it already builds tombstone records with `deleted=true`).

- [ ] **Step 5: Run the cache test to verify it passes**

Run: `./mvnw -q --projects :org.opennms.core.node-context-consumer test -Dtest=NodeContextCacheTest`
Expected: PASS (all cases). Also build the module to confirm the moved `NodeContextKafkaBootstrap` (Task 2) compiles against the new cache: `./mvnw -q --projects :org.opennms.core.node-context-consumer -am -DskipTests install` → BUILD SUCCESS. If the bootstrap referenced `get`/`findByNodeId`/`put`/`remove`, update those references to the new API (`getByKey`/`getByNodeId`/`applyUpdate`).

- [ ] **Step 6: Commit (Tasks 2 + 3 together — first compiling state)**

```bash
git add core/node-context-consumer/src/main/java core/node-context-consumer/src/test
git commit -m "feat(node-context): shared NodeContextCache with IP + nodeId indexes, bootstrap, ready event"
```

---

### Task 4: Move the health indicator into the module

**Files:**
- Create: `core/node-context-consumer/src/main/java/org/deltav/nodecontext/NodeContextCacheHealthIndicator.java` (copy of `core/prometheus-writer/.../nodecontext/NodeContextCacheHealthIndicator.java`)

- [ ] **Step 1: Copy + re-package + de-`@Component`**

```bash
cp core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/nodecontext/NodeContextCacheHealthIndicator.java \
   core/node-context-consumer/src/main/java/org/deltav/nodecontext/NodeContextCacheHealthIndicator.java
```
Change its `package` to `org.deltav.nodecontext;`, remove `@Component` (declared as a `@Bean` in Task 5), and ensure any `cache.isReady()` / `cache.size()` calls match the shared `NodeContextCache` API (they do — both methods are unchanged).

- [ ] **Step 2: Build**

Run: `./mvnw -q --projects :org.opennms.core.node-context-consumer -am -DskipTests install`
Expected: BUILD SUCCESS.

- [ ] **Step 3: Commit**

```bash
git add core/node-context-consumer/src/main/java/org/deltav/nodecontext/NodeContextCacheHealthIndicator.java
git commit -m "feat(node-context): move NodeContextCacheHealthIndicator into the shared module"
```

---

### Task 5: Auto-configuration so consumers wire it with one dependency

**Files:**
- Create: `core/node-context-consumer/src/main/java/org/deltav/nodecontext/NodeContextConsumerAutoConfiguration.java`
- Create: `core/node-context-consumer/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`

- [ ] **Step 1: Write the auto-configuration**

Create `NodeContextConsumerAutoConfiguration.java`. The bootstrap/health-indicator constructor signatures must match the moved classes — copy the exact constructor parameter lists from `NodeContextKafkaBootstrap` (Task 2) and `NodeContextCacheHealthIndicator` (Task 4) and forward the same beans/`@Value`s:
```java
/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.nodecontext;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;

/**
 * Wires the shared NodeContext consumer for any service that depends on this
 * module. Consumers need only the dependency plus
 * {@code spring.kafka.bootstrap-servers} (and optionally a topic override) —
 * no component scan of this package required.
 */
@AutoConfiguration
public class NodeContextConsumerAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public NodeContextCache nodeContextCache() {
        return new NodeContextCache();
    }

    @Bean
    @ConditionalOnMissingBean
    public NodeContextCacheHealthIndicator nodeContextCacheHealthIndicator(NodeContextCache cache) {
        return new NodeContextCacheHealthIndicator(cache);
    }

    @Bean
    @ConditionalOnMissingBean
    public NodeContextKafkaBootstrap nodeContextKafkaBootstrap(
            NodeContextCache cache,
            MeterRegistry meterRegistry,
            ApplicationEventPublisher eventPublisher,
            org.springframework.core.env.Environment env) {
        // Reproduce the moved bootstrap's exact constructor invocation. Replace
        // the args below with the parameter list copied from
        // NodeContextKafkaBootstrap (Task 2) — e.g. it takes
        // (NodeContextCache, MeterRegistry, @Value bootstrapServers, ...).
        return new NodeContextKafkaBootstrap(cache, meterRegistry,
                env.getProperty("spring.kafka.bootstrap-servers", "localhost:9092"));
    }
}
```
(The bootstrap currently self-starts its thread from an `@PostConstruct`/constructor in the `@Component` form. Keep that lifecycle: if it started the thread in a `@PostConstruct`, leave the `@PostConstruct` method on the class so the `@Bean` instance still starts. If it relied on `@Component` + `implements Runnable` + an external starter, add a `@PostConstruct void start()` that launches the thread, matching the original behavior exactly.)

- [ ] **Step 2: Register the auto-configuration**

Create `…/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports` containing exactly:
```
org.deltav.nodecontext.NodeContextConsumerAutoConfiguration
```

- [ ] **Step 3: Build**

Run: `./mvnw -q --projects :org.opennms.core.node-context-consumer -am -DskipTests install`
Expected: BUILD SUCCESS.

- [ ] **Step 4: Commit**

```bash
git add core/node-context-consumer/src/main/java/org/deltav/nodecontext/NodeContextConsumerAutoConfiguration.java \
        core/node-context-consumer/src/main/resources
git commit -m "feat(node-context): auto-configuration for one-dependency wiring"
```

---

## Phase B — refactor the existing consumers onto the shared module

### Task 6: Refactor alerts-forwarder onto the shared module

**Files:**
- Modify: `core/alerts-forwarder/pom.xml` (add the shared dep)
- Delete: `core/alerts-forwarder/src/main/java/org/deltav/alerts/forwarder/nodecontext/{NodeContextCache,NodeContextKafkaBootstrap,NodeContextCacheReadyEvent}.java`
- Modify: every alerts-forwarder file importing those classes (find with `grep -rl 'forwarder.nodecontext\|NodeContextCache\|NodeContextKafkaBootstrap\|NodeContextCacheReadyEvent' core/alerts-forwarder/src/main`)

- [ ] **Step 1: Add the shared dependency to `core/alerts-forwarder/pom.xml`**

```xml
        <dependency>
            <groupId>org.opennms.core</groupId>
            <artifactId>org.opennms.core.node-context-consumer</artifactId>
            <version>${project.version}</version>
        </dependency>
```

- [ ] **Step 2: Delete the package-local copies**

```bash
git rm core/alerts-forwarder/src/main/java/org/deltav/alerts/forwarder/nodecontext/NodeContextCache.java \
       core/alerts-forwarder/src/main/java/org/deltav/alerts/forwarder/nodecontext/NodeContextKafkaBootstrap.java \
       core/alerts-forwarder/src/main/java/org/deltav/alerts/forwarder/nodecontext/NodeContextCacheReadyEvent.java
```
(If `NodeContextCacheHealthIndicator` exists here too, delete it as well.)

- [ ] **Step 3: Fix imports + renamed methods**

In each importing file replace `import org.deltav.alerts.forwarder.nodecontext.X;` with `import org.deltav.nodecontext.X;`. Update call sites from the old API: `cache.get(k)` → `cache.getByKey(k)`, `cache.findByNodeId(id)` → `cache.getByNodeId(id)`. (The shared auto-config provides the beans; remove any `@ComponentScan` entry that existed only to pick up the old `nodecontext` package, and any local `@Bean` that duplicated the cache/bootstrap.)

- [ ] **Step 4: Build + run alerts-forwarder tests (parity is the bar)**

Run: `./mvnw -q --projects :org.opennms.core.alerts-forwarder -am test`
Expected: BUILD SUCCESS, all existing tests pass unchanged.

- [ ] **Step 5: Commit**

```bash
git add core/alerts-forwarder
git commit -m "refactor(alerts-forwarder): consume shared node-context-consumer"
```

---

### Task 7: Refactor prometheus-writer onto the shared module

**Files:**
- Modify: `core/prometheus-writer/pom.xml`
- Delete: `core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/nodecontext/{NodeContextCache,NodeContextKafkaBootstrap,NodeContextCacheReadyEvent,NodeContextCacheHealthIndicator}.java`
- Modify: importing files (`grep -rl 'writer.nodecontext\|NodeContextCache\|NodeContextKafkaBootstrap' core/prometheus-writer/src/main`)

- [ ] **Step 1: Add the shared dependency** (same XML as Task 6 Step 1) to `core/prometheus-writer/pom.xml`.

- [ ] **Step 2: Delete the package-local copies**

```bash
git rm core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/nodecontext/NodeContextCache.java \
       core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/nodecontext/NodeContextKafkaBootstrap.java \
       core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/nodecontext/NodeContextCacheReadyEvent.java \
       core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/nodecontext/NodeContextCacheHealthIndicator.java
```

- [ ] **Step 3: Fix imports + renamed methods** — same as Task 6 Step 3 (`import org.deltav.nodecontext.X`, `get`→`getByKey`, `findByNodeId`→`getByNodeId`), and remove any local `@Bean`/`@ComponentScan` that duplicated the now-auto-configured beans.

- [ ] **Step 4: Build + run prometheus-writer tests**

Run: `./mvnw -q --projects :org.opennms.core.prometheus-writer -am test`
Expected: BUILD SUCCESS, existing tests pass. (Note: the prometheus-writer test memory mentions a `NodeContextKafkaBootstrap` startup race; if a pre-existing flaky test surfaces, re-run once — do not "fix" by changing the shared logic, which is byte-identical to before.)

- [ ] **Step 5: Commit**

```bash
git add core/prometheus-writer
git commit -m "refactor(prometheus-writer): consume shared node-context-consumer"
```

---

## Phase C — migrate the flow-enricher off JDBC

### Task 8: Wire the shared cache into the flow-enricher

**Files:**
- Modify: `core/flow-enricher/pom.xml` (add shared dep)
- Modify: `core/flow-enricher/src/main/resources/application.yml` (bootstrap-servers already present for the SCS binder; ensure the consumer can read it)

- [ ] **Step 1: Add the shared dependency** (same XML as Task 6 Step 1) to `core/flow-enricher/pom.xml`.

- [ ] **Step 2: Build to confirm the auto-config beans are available**

Run: `./mvnw -q --projects :org.deltav.flows.flow-enricher -am -DskipTests install`
Expected: BUILD SUCCESS. The `NodeContextCache` + bootstrap beans are now present via auto-config (they consume `spring.kafka.bootstrap-servers`, which the flow-enricher already sets for the Kafka binder).

- [ ] **Step 3: Commit**

```bash
git add core/flow-enricher/pom.xml core/flow-enricher/src/main/resources/application.yml
git commit -m "feat(flows): add node-context-consumer dependency to flow-enricher"
```

---

### Task 9: Replace the JDBC lookups with the cache (+ categories)

**Files:**
- Modify: `core/flow-enricher/src/main/java/org/deltav/flows/enricher/FlowEnrichmentFunction.java`
- Modify: `core/flow-enricher/src/main/java/org/deltav/flows/enricher/FlowEnricherConfiguration.java`
- Modify: `core/flow-enricher/src/main/java/org/deltav/flows/enricher/mapping/FlowToDocumentMapper.java`
- Delete: `core/flow-enricher/src/main/java/org/deltav/flows/enricher/enrichment/JdbcNodeInfoLookup.java`, `JdbcSnmpInterfaceLookup.java`
- Test: `core/flow-enricher/src/test/java/org/deltav/flows/enricher/mapping/FlowToDocumentMapperTest.java`

- [ ] **Step 1: Add a categories-setting test (failing)**

Append to `FlowToDocumentMapperTest.java` — the mapper's `NodeInfo` carrier must now carry categories. The mapper currently takes `JdbcNodeInfoLookup.NodeInfo`; this task introduces a small `NodeIdentity` record (below) so the mapper no longer depends on the deleted JDBC class. Test:
```java
    @Test
    void setsExporterNodeCategories() {
        FlowToDocumentMapper.NodeIdentity exporter = new FlowToDocumentMapper.NodeIdentity(
                7, "nl6", "dev-7", "cisco-7", java.util.List.of("Production", "Routers"));

        FlowDocumentProtos.FlowDocument doc = mapper.map(
                flow, exporter, null, null,
                "HTTPS", "PRIVATE", "PUBLIC", "PRIVATE",
                "10.0.0.7", "nl6-lab", 0L, "Gi0/1", "Gi0/2");

        assertThat(doc.getExporterNode().getCategoriesList()).containsExactly("Production", "Routers");
        assertThat(doc.getExporterNode().getNodeLabel()).isEqualTo("cisco-7");
    }
```

- [ ] **Step 2: Run to verify it fails**

Run: `./mvnw -q --projects :org.deltav.flows.flow-enricher test -Dtest=FlowToDocumentMapperTest`
Expected: COMPILE FAILURE — `FlowToDocumentMapper.NodeIdentity` does not exist.

- [ ] **Step 3: Introduce `NodeIdentity` + set categories in the mapper**

In `FlowToDocumentMapper.java`:
- Add a nested record decoupled from JDBC:
```java
    /** Resolved node identity passed to the mapper (decoupled from the data source). */
    public record NodeIdentity(int nodeId, String foreignSource, String foreignId,
                               String nodeLabel, java.util.List<String> categories) {}
```
- Change the three `map(...)` parameter types from `JdbcNodeInfoLookup.NodeInfo` to `NodeIdentity` (both the 13-arg and 11-arg overloads), and update `toProtoNodeInfo` to take `NodeIdentity` and additionally set categories:
```java
    private static FlowDocumentProtos.NodeInfo toProtoNodeInfo(NodeIdentity info) {
        FlowDocumentProtos.NodeInfo.Builder b = FlowDocumentProtos.NodeInfo.newBuilder();
        b.setNodeId(info.nodeId());
        if (info.foreignSource() != null) b.setForeignSource(info.foreignSource());
        if (info.foreignId() != null) b.setForeignId(info.foreignId());
        if (info.nodeLabel() != null && !info.nodeLabel().isEmpty()) b.setNodeLabel(info.nodeLabel());
        if (info.categories() != null) b.addAllCategories(info.categories());
        return b.build();
    }
```
- Remove the `import …JdbcNodeInfoLookup;` from the mapper. Update the existing mapper tests' `new JdbcNodeInfoLookup.NodeInfo(...)` constructions to `new FlowToDocumentMapper.NodeIdentity(nodeId, fs, fid, label, List.of())` (categories empty where not asserted).

- [ ] **Step 4: Resolve from the cache in `FlowEnrichmentFunction`**

In `FlowEnrichmentFunction.java`:
- Replace the `JdbcNodeInfoLookup nodeInfoLookup` + `JdbcSnmpInterfaceLookup snmpInterfaceLookup` fields/ctor-params with a single `NodeContextCache nodeContextCache` (import `org.deltav.nodecontext.NodeContextCache` and `org.deltav.timeseries.proto.NodeContext`).
- Add a private mapper from `NodeContext` → `FlowToDocumentMapper.NodeIdentity`:
```java
    private static FlowToDocumentMapper.NodeIdentity identity(NodeContext n) {
        return new FlowToDocumentMapper.NodeIdentity(
                n.getNodeId(), n.getForeignSource(), n.getForeignId(),
                n.getNodeLabel(), n.getCategoriesList());
    }
```
- Replace exporter/src/dst resolution: `nodeInfoLookup.lookupByIpAddress(ip)` → `nodeContextCache.getByIp(ip).map(FlowEnrichmentFunction::identity).orElse(null)`.
- Replace the exporter interface-name block: `snmpInterfaceLookup.lookupIfName(nodeId, ifIndex)` → `nodeContextCache.ifName(nodeId, ifIndex).orElse(null)`. The `nodeId` comes from the resolved exporter identity (`exporterIdentity.nodeId()`), only when non-null. `InterfaceMarkingCache.markIfNeeded(...)` calls and the `DataSource` stay exactly as they are.
- Pass `NodeIdentity` values into `flowToDocumentMapper.map(...)` (signature already uses the mapper's identity type after Step 3).

- [ ] **Step 5: Delete the JDBC lookups + their beans**

```bash
git rm core/flow-enricher/src/main/java/org/deltav/flows/enricher/enrichment/JdbcNodeInfoLookup.java \
       core/flow-enricher/src/main/java/org/deltav/flows/enricher/enrichment/JdbcSnmpInterfaceLookup.java \
       core/flow-enricher/src/test/java/org/deltav/flows/enricher/enrichment/JdbcNodeInfoLookupTest.java \
       core/flow-enricher/src/test/java/org/deltav/flows/enricher/enrichment/JdbcSnmpInterfaceLookupTest.java
```
In `FlowEnricherConfiguration.java`: delete the `jdbcNodeInfoLookup` + `jdbcSnmpInterfaceLookup` `@Bean`s and replace the `flowEnrichmentFunction(...)` bean's `JdbcNodeInfoLookup`/`JdbcSnmpInterfaceLookup` params with a `NodeContextCache nodeContextCache` param, passed to the constructor. **Keep** the `flowEnricherJdbcTemplate` + `interfaceMarkingCache` beans (the `hasflows` write path). Update `FlowEnrichmentFunctionTest` to construct `FlowEnrichmentFunction` with a `mock(NodeContextCache.class)` in place of the two removed mocks, and any `NodeIdentity`/`NodeContext` stubs it needs.

- [ ] **Step 6: Run the module tests**

Run: `./mvnw -q --projects :org.deltav.flows.flow-enricher test`
Expected: BUILD SUCCESS, all tests pass (mapper categories + the migrated function tests).

- [ ] **Step 7: Commit**

```bash
git add core/flow-enricher
git commit -m "feat(flows): enrich from NodeContext cache, delete JDBC lookups, populate categories"
```

---

### Task 10: Gate flow ingest on cache readiness

**Files:**
- Modify: `core/flow-enricher/src/main/resources/application.yml`
- Create: `core/flow-enricher/src/main/java/org/deltav/flows/enricher/NodeContextReadinessGate.java`
- Test: `core/flow-enricher/src/test/java/org/deltav/flows/enricher/NodeContextReadinessGateTest.java`

- [ ] **Step 1: Disable auto-startup on the input binding**

In `application.yml`, under `spring.cloud.stream.bindings.enrichFlows-in-0`, add:
```yaml
        enrichFlows-in-0:
          # ...existing destination/group...
          consumer:
            auto-startup: false
```
(Merge into the existing `enrichFlows-in-0` block; keep `destination` and `group`.)

- [ ] **Step 2: Write the gate test (failing)**

Create `NodeContextReadinessGateTest.java`:
```java
/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.flows.enricher;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.deltav.nodecontext.NodeContextCacheReadyEvent;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.stream.binding.BindingsLifecycleController;
import org.springframework.cloud.stream.binding.BindingsLifecycleController.State;

class NodeContextReadinessGateTest {

    @Test
    void startsInputBindingWhenCacheBecomesReady() {
        BindingsLifecycleController controller = mock(BindingsLifecycleController.class);
        NodeContextReadinessGate gate = new NodeContextReadinessGate(controller);

        gate.onReady(new NodeContextCacheReadyEvent(this));

        verify(controller).changeState("enrichFlows-in-0", State.STARTED);
    }
}
```
(Confirm the `NodeContextCacheReadyEvent` constructor signature from the moved class — if it is `new NodeContextCacheReadyEvent(Object source)`, the test matches; adjust if it differs.)

- [ ] **Step 3: Run to verify it fails**

Run: `./mvnw -q --projects :org.deltav.flows.flow-enricher test -Dtest=NodeContextReadinessGateTest`
Expected: COMPILE FAILURE — `NodeContextReadinessGate` does not exist.

- [ ] **Step 4: Write the gate**

Create `NodeContextReadinessGate.java`:
```java
/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.flows.enricher;

import org.deltav.nodecontext.NodeContextCacheReadyEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.binding.BindingsLifecycleController;
import org.springframework.cloud.stream.binding.BindingsLifecycleController.State;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Holds the flow ingest binding ({@code enrichFlows-in-0}, auto-startup=false)
 * until the NodeContext cache has drained to the Kafka HWM, so the first flow
 * processed sees the full node/interface inventory.
 */
@Component
public class NodeContextReadinessGate {

    private static final Logger LOG = LoggerFactory.getLogger(NodeContextReadinessGate.class);
    private static final String INPUT_BINDING = "enrichFlows-in-0";

    private final BindingsLifecycleController bindings;

    public NodeContextReadinessGate(BindingsLifecycleController bindings) {
        this.bindings = bindings;
    }

    @EventListener
    public void onReady(NodeContextCacheReadyEvent event) {
        LOG.info("NodeContext cache ready — starting flow ingest binding {}", INPUT_BINDING);
        bindings.changeState(INPUT_BINDING, State.STARTED);
    }
}
```

- [ ] **Step 5: Run the gate test to verify it passes**

Run: `./mvnw -q --projects :org.deltav.flows.flow-enricher test -Dtest=NodeContextReadinessGateTest`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add core/flow-enricher
git commit -m "feat(flows): gate flow ingest until NodeContext cache is bootstrapped"
```

---

### Task 11: Full reactor build + cross-module verification

**Files:** none (verification)

- [ ] **Step 1: Build the three modules + run their tests**

Run: `./mvnw -q --projects :org.opennms.core.node-context-consumer,:org.opennms.core.alerts-forwarder,:org.opennms.core.prometheus-writer,:org.deltav.flows.flow-enricher -am test`
Expected: BUILD SUCCESS; all four modules' tests pass.

- [ ] **Step 2: Confirm no remaining references to the deleted classes**

Run:
```bash
grep -rn 'JdbcNodeInfoLookup\|JdbcSnmpInterfaceLookup' core/flow-enricher/src && echo "LEFTOVER" || echo "clean"
grep -rln 'forwarder.nodecontext\.\|writer.nodecontext\.' core/alerts-forwarder/src core/prometheus-writer/src && echo "LEFTOVER" || echo "clean"
```
Expected: both print `clean`.

- [ ] **Step 3: Commit any fixups**, then this plan is complete; finish via `superpowers:finishing-a-development-branch`.

> **Deferred E2E (next deploy/rc):** rebuild flow-enricher + the two services, deploy, confirm flows still carry populated `exporter_node_label` / `input_if_name` / `output_if_name` **plus** `*_categories`, and that the flow-enricher holds no PostgreSQL connection for lookups (only the `hasflows` write). Confirm alerts-forwarder + prometheus-writer node enrichment unchanged.

---

## Self-Review

**Spec coverage:**
- Shared module + complete API (`getByIp`, `ifName`, O(1) `getByNodeId`) → Tasks 1–5. ✓
- Move bootstrap/ready event/health indicator verbatim → Tasks 2, 4. ✓
- Auto-config one-dependency wiring → Task 5. ✓
- Refactor alerts-forwarder + prometheus-writer → Tasks 6, 7. ✓
- flow-enricher: replace both JDBC lookups, delete them, keep DataSource for `hasflows` → Task 9. ✓
- categories from NodeContext → Task 9. ✓
- Readiness gating via `auto-startup:false` + `NodeContextCacheReadyEvent` → Task 10. ✓
- IP normalization mandate (canonical `InetAddress.getHostAddress`, divergent-pair tests) → Tasks 3 (`IpNormalizer` + cache tests). ✓
- Tombstone IP eviction, IP-conflict last-wins → Task 3 tests. ✓

**Type consistency:** `NodeContextCache` API (`getByKey`/`getByNodeId`/`getByIp`/`ifName`/`applyUpdate`/`isReady`/`markReady`/`snapshot`) is defined in Task 3 and consumed identically in Tasks 6, 7, 9. `FlowToDocumentMapper.NodeIdentity(nodeId, foreignSource, foreignId, nodeLabel, categories)` is defined in Task 9 Step 3 and used in Step 4 + the tests. `IpNormalizer.normalize` returns `String`/null, used in the cache + (mandated) at consumer lookups. `NodeContextReadinessGate` uses `BindingsLifecycleController.changeState("enrichFlows-in-0", State.STARTED)`.

**Open verification flagged for the implementer (not placeholders — facts to confirm against the moved code):** the exact constructor parameter list + start lifecycle of `NodeContextKafkaBootstrap` (Task 5 forwards them) and the `NodeContextCacheReadyEvent` constructor shape (Task 10 test). Both are in files this plan moves verbatim, so the signatures are already in-repo.
