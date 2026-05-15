# Pollerd/PerspectivePollerd Timeseries Publisher Unification — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Collapse the two ~95%-identical Phase 3 response-time publishers (Pollerd's `PollResultPublisher`, PerspectivePollerd's `PerspectiveResponseTimePublisher`) into one shared `ResponseTimePublisher` in a new `core/poller-timeseries-common` module, with no behavior change, and add the missing Pollerd Grafana dashboard.

**Architecture:** A new horizon-decoupled library module holds a neutral `ResponseTimeSample` record and a single `ResponseTimePublisher`. Each daemon's existing instrumented hook (`InstrumentedPollContext`, `InstrumentedPerspectivePollerd`) keeps its daemon-specific shape but adapts its horizon poll object into a `ResponseTimeSample` and calls the shared publisher. The two duplicate publisher classes are deleted.

**Tech Stack:** Java 21, Maven (bundled `./mvnw`), Spring Boot 4, Spring Cloud Stream (Kafka binder), Micrometer, protobuf (`deltav-kafka-contracts`), JUnit 5 + Mockito + AssertJ, Grafana/VictoriaMetrics.

**Spec:** `docs/superpowers/specs/2026-05-15-poller-timeseries-unification-design.md`

**Branch:** Implement on `feat/v1.2.0-poller-timeseries-unification` off `develop`. PR against `pbrane/delta-v` base `develop` (never `OpenNMS/*`).

---

## File structure

**Created:**
- `core/poller-timeseries-common/pom.xml` — new library module
- `core/poller-timeseries-common/src/main/java/org/deltav/poller/timeseries/ResponseTimeSample.java` — neutral value record
- `core/poller-timeseries-common/src/main/java/org/deltav/poller/timeseries/ResponseTimePublisher.java` — shared publisher
- `core/poller-timeseries-common/src/test/java/org/deltav/poller/timeseries/ResponseTimePublisherTest.java` — unit test (first to exist for this code)
- `opennms-container/delta-v/grafana/dashboards/pollerd-monitoring.json` — Pollerd latency dashboard

**Modified:**
- `pom.xml` — register the new module
- `core/daemon-boot-pollerd/pom.xml` — add dependency on new module
- `core/daemon-boot-pollerd/src/main/java/org/deltav/netmgt/poller/boot/InstrumentedPollContext.java` — adapt + call shared publisher
- `core/daemon-boot-pollerd/src/main/java/org/deltav/netmgt/poller/boot/PollerdTimeseriesConfiguration.java` — produce shared publisher bean
- `core/daemon-boot-pollerd/src/main/java/org/deltav/netmgt/poller/boot/PollerdDaemonConfiguration.java` — `ObjectProvider` type swap
- `core/daemon-boot-perspectivepollerd/pom.xml` — add dependency on new module
- `core/daemon-boot-perspectivepollerd/src/main/java/org/deltav/netmgt/perspectivepoller/boot/InstrumentedPerspectivePollerd.java` — adapt + call shared publisher
- `core/daemon-boot-perspectivepollerd/src/main/java/org/deltav/netmgt/perspectivepoller/boot/PerspectivePollerdTimeseriesConfiguration.java` — produce shared publisher bean
- `core/daemon-boot-perspectivepollerd/src/main/java/org/deltav/netmgt/perspectivepoller/boot/PerspectivePollerdDaemonConfiguration.java` — `ObjectProvider` type swap
- `docs/plans/2026-04-23-v1.2.0-release-plan.md` — correct stale Track 4 status

**Deleted:**
- `core/daemon-boot-pollerd/src/main/java/org/deltav/netmgt/poller/boot/PollResultPublisher.java`
- `core/daemon-boot-perspectivepollerd/src/main/java/org/deltav/netmgt/perspectivepoller/boot/PerspectiveResponseTimePublisher.java`

---

## Task 1: Create the `poller-timeseries-common` module skeleton

**Files:**
- Create: `core/poller-timeseries-common/pom.xml`
- Modify: `pom.xml` (root, `<modules>` list)

- [ ] **Step 1: Create the module pom**

Create `core/poller-timeseries-common/pom.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>org.deltav</groupId>
        <artifactId>delta-v-parent</artifactId>
        <version>1.2.0-rc2.2</version>
        <relativePath>../../pom.xml</relativePath>
    </parent>

    <groupId>org.deltav.core</groupId>
    <artifactId>org.opennms.core.poller-timeseries-common</artifactId>
    <name>OpenNMS :: Core :: Poller Timeseries Common</name>
    <description>Shared response-time publishing for Pollerd and PerspectivePollerd:
        a neutral ResponseTimeSample value type and the ResponseTimePublisher that
        emits TimeseriesBatch protobuf records to the deltav-timeseries Kafka topic.</description>

    <dependencies>
        <dependency>
            <groupId>org.deltav.core</groupId>
            <artifactId>org.opennms.core.deltav-kafka-contracts</artifactId>
            <version>${project.version}</version>
        </dependency>
        <dependency>
            <groupId>org.springframework.cloud</groupId>
            <artifactId>spring-cloud-stream</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.kafka</groupId>
            <artifactId>spring-kafka</artifactId>
        </dependency>
        <dependency>
            <groupId>io.micrometer</groupId>
            <artifactId>micrometer-core</artifactId>
        </dependency>
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-api</artifactId>
        </dependency>

        <!-- Test -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>
</project>
```

Note: `spring-boot-starter-test` (test scope) brings JUnit 5, Mockito, AssertJ, and `junit-platform-launcher` (required by surefire 3.5.4 per memory `feedback_boot4_junit_platform_launcher`). No `<version>` tags on dependencies — the `delta-v-parent` BOM manages them.

- [ ] **Step 2: Register the module in the root pom**

In `pom.xml` (repo root), the `<modules>` list has `core/opennms-model-jakarta` followed by a blank line then `core/daemon-boot-alarmd`. Add the new module on its own line immediately after `core/opennms-model-jakarta` (keeps the list alphabetical: `o` < `p`):

```xml
    <module>core/opennms-model-jakarta</module>
    <module>core/poller-timeseries-common</module>
```

- [ ] **Step 3: Create the source directory so Maven recognizes the module**

Run: `mkdir -p core/poller-timeseries-common/src/main/java/org/deltav/poller/timeseries core/poller-timeseries-common/src/test/java/org/deltav/poller/timeseries`

- [ ] **Step 4: Verify the module builds (empty)**

Run: `./mvnw -q -pl :org.opennms.core.poller-timeseries-common -am install -DskipTests`
Expected: `BUILD SUCCESS`. The module compiles with no sources yet.

- [ ] **Step 5: Commit**

```bash
git add core/poller-timeseries-common/pom.xml pom.xml
git commit -m "feat(poller-timeseries): add core/poller-timeseries-common module skeleton"
```

---

## Task 2: Create the `ResponseTimeSample` record

**Files:**
- Create: `core/poller-timeseries-common/src/main/java/org/deltav/poller/timeseries/ResponseTimeSample.java`

- [ ] **Step 1: Write the record**

Create `ResponseTimeSample.java`:

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
package org.deltav.poller.timeseries;

import org.deltav.timeseries.proto.ProducerType;

/**
 * Daemon-neutral value object describing one completed service poll's
 * response-time measurement. Pollerd and PerspectivePollerd each adapt their
 * horizon-specific poll objects into this record, which the shared
 * {@link ResponseTimePublisher} consumes.
 *
 * @param nodeId         monitored node id
 * @param serviceName    monitored service name (e.g. "ICMP", "HTTP-8080")
 * @param location       Minion location (Pollerd) or perspective location
 *                       (PerspectivePollerd) that ran the poll
 * @param responseTimeMs measured response time, milliseconds
 * @param timestampMs    poll completion timestamp, epoch millis
 * @param producerType   protobuf producer discriminator
 * @param producerLabel  Micrometer {@code producer} tag value
 */
public record ResponseTimeSample(
        int nodeId,
        String serviceName,
        String location,
        double responseTimeMs,
        long timestampMs,
        ProducerType producerType,
        String producerLabel) {
}
```

- [ ] **Step 2: Verify it compiles**

Run: `./mvnw -q -pl :org.opennms.core.poller-timeseries-common -am compile`
Expected: `BUILD SUCCESS`.

- [ ] **Step 3: Commit**

```bash
git add core/poller-timeseries-common/src/main/java/org/deltav/poller/timeseries/ResponseTimeSample.java
git commit -m "feat(poller-timeseries): add ResponseTimeSample neutral value record"
```

---

## Task 3: Create the shared `ResponseTimePublisher` (TDD)

**Files:**
- Create: `core/poller-timeseries-common/src/test/java/org/deltav/poller/timeseries/ResponseTimePublisherTest.java`
- Create: `core/poller-timeseries-common/src/main/java/org/deltav/poller/timeseries/ResponseTimePublisher.java`

- [ ] **Step 1: Write the failing test**

Create `ResponseTimePublisherTest.java`:

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
package org.deltav.poller.timeseries;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.deltav.timeseries.proto.ProducerType;
import org.deltav.timeseries.proto.TimeseriesBatch;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;

class ResponseTimePublisherTest {

    private ResponseTimeSample sample(ProducerType producer, String label) {
        return new ResponseTimeSample(42, "ICMP", "Default", 12.5, 1_700_000_000_000L,
                producer, label);
    }

    @Test
    void publishesBatchWithCorrectFieldsKeyAndProducer() throws Exception {
        StreamBridge streamBridge = mock(StreamBridge.class);
        when(streamBridge.send(any(String.class), any())).thenReturn(true);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        ResponseTimePublisher publisher = new ResponseTimePublisher(streamBridge, registry);

        publisher.publish(sample(ProducerType.PRODUCER_POLLERD, "pollerd"));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Message<byte[]>> msg = ArgumentCaptor.forClass(Message.class);
        verify(streamBridge).send(eq("publishTimeseries-out-0"), msg.capture());

        byte[] key = (byte[]) msg.getValue().getHeaders().get(KafkaHeaders.KEY);
        assertThat(new String(key, StandardCharsets.UTF_8)).isEqualTo("Default@42");

        TimeseriesBatch batch = TimeseriesBatch.parseFrom(msg.getValue().getPayload());
        assertThat(batch.getNodeId()).isEqualTo(42);
        assertThat(batch.getLocation()).isEqualTo("Default");
        assertThat(batch.getProducer()).isEqualTo(ProducerType.PRODUCER_POLLERD);
        assertThat(batch.getResourcesCount()).isEqualTo(1);
        assertThat(batch.getResources(0).getGroups(0).getAttributes(0).getNumeric())
                .isEqualTo(12.5);

        assertThat(registry.counter("deltav_timeseries_batches_published_total",
                "location", "Default", "producer", "pollerd").count()).isEqualTo(1.0);
    }

    @Test
    void perspectiveProducerIsCarriedThrough() throws Exception {
        StreamBridge streamBridge = mock(StreamBridge.class);
        when(streamBridge.send(any(String.class), any())).thenReturn(true);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        ResponseTimePublisher publisher = new ResponseTimePublisher(streamBridge, registry);

        publisher.publish(sample(ProducerType.PRODUCER_PERSPECTIVE_POLLERD, "perspectivepollerd"));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Message<byte[]>> msg = ArgumentCaptor.forClass(Message.class);
        verify(streamBridge).send(eq("publishTimeseries-out-0"), msg.capture());
        TimeseriesBatch batch = TimeseriesBatch.parseFrom(msg.getValue().getPayload());
        assertThat(batch.getProducer()).isEqualTo(ProducerType.PRODUCER_PERSPECTIVE_POLLERD);
    }

    @Test
    void failedSendIncrementsFailureCounter() {
        StreamBridge streamBridge = mock(StreamBridge.class);
        when(streamBridge.send(any(String.class), any())).thenReturn(false);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        ResponseTimePublisher publisher = new ResponseTimePublisher(streamBridge, registry);

        publisher.publish(sample(ProducerType.PRODUCER_POLLERD, "pollerd"));

        assertThat(registry.counter("deltav_timeseries_batches_failed_total",
                "location", "Default", "producer", "pollerd",
                "reason", "kafka_send_error").count()).isEqualTo(1.0);
        assertThat(registry.counter("deltav_timeseries_batches_published_total",
                "location", "Default", "producer", "pollerd").count()).isEqualTo(0.0);
    }

    @Test
    void nanResponseTimeIsSkipped() {
        StreamBridge streamBridge = mock(StreamBridge.class);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        ResponseTimePublisher publisher = new ResponseTimePublisher(streamBridge, registry);

        publisher.publish(new ResponseTimeSample(1, "ICMP", "Default", Double.NaN,
                1_700_000_000_000L, ProducerType.PRODUCER_POLLERD, "pollerd"));

        verify(streamBridge, never()).send(any(String.class), any());
    }

    @Test
    void nullSampleIsSafelyIgnored() {
        StreamBridge streamBridge = mock(StreamBridge.class);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        ResponseTimePublisher publisher = new ResponseTimePublisher(streamBridge, registry);

        publisher.publish(null);

        verify(streamBridge, never()).send(any(String.class), any());
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `./mvnw -q -pl :org.opennms.core.poller-timeseries-common -am test`
Expected: FAIL — compilation error, `ResponseTimePublisher` does not exist.

- [ ] **Step 3: Write the `ResponseTimePublisher` implementation**

Create `ResponseTimePublisher.java`:

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
package org.deltav.poller.timeseries;

import java.nio.charset.StandardCharsets;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import org.deltav.timeseries.proto.Attribute;
import org.deltav.timeseries.proto.AttributeGroup;
import org.deltav.timeseries.proto.AttributeType;
import org.deltav.timeseries.proto.Resource;
import org.deltav.timeseries.proto.TimeseriesBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

/**
 * Shared publisher of per-poll response-time samples to the
 * {@code deltav-timeseries} Kafka topic as {@link TimeseriesBatch} protobuf
 * records, keyed {@code "{location}@{nodeId}"}.
 *
 * <p>Used by both Pollerd and PerspectivePollerd. Each daemon adapts its
 * horizon-specific poll object into a {@link ResponseTimeSample}; this class
 * holds the entire publish mechanism, identical for both. Producer identity
 * (protobuf {@code ProducerType} + Micrometer {@code producer} label) is
 * carried on the sample, supplied by the daemon-side adapter.
 *
 * <p>Each batch carries a single {@link Resource}
 * ({@code resource_id = node[N].monitoredService[svc]}) with a single
 * GAUGE {@link Attribute} holding the response time in milliseconds.
 *
 * <p>Error-isolated: never throws. Failures bump
 * {@code deltav_timeseries_batches_failed_total} and the caller continues.
 */
public class ResponseTimePublisher {

    private static final Logger LOG = LoggerFactory.getLogger(ResponseTimePublisher.class);
    static final String BINDING_NAME = "publishTimeseries-out-0";
    static final String GROUP_NAME = "response-time";
    static final String ATTRIBUTE_NAME = "response";
    static final String RESOURCE_TYPE = "monitoredService";

    private final StreamBridge streamBridge;
    private final MeterRegistry meterRegistry;

    public ResponseTimePublisher(StreamBridge streamBridge, MeterRegistry meterRegistry) {
        this.streamBridge = streamBridge;
        this.meterRegistry = meterRegistry;
    }

    /**
     * Publishes one response-time sample. Returns silently for a null sample
     * or a NaN response time (defensive — daemon adapters already guard).
     */
    public void publish(ResponseTimeSample sample) {
        if (sample == null || Double.isNaN(sample.responseTimeMs())) {
            return;
        }
        String location = sample.location() != null ? sample.location() : "Default";
        String producer = sample.producerLabel();
        Timer.Sample timer = Timer.start(meterRegistry);
        try {
            TimeseriesBatch batch = buildBatch(sample, location);
            byte[] payload;
            try {
                payload = batch.toByteArray();
            } catch (RuntimeException ex) {
                LOG.warn("Serialization failed for node {} svc {}; dropping",
                        sample.nodeId(), sample.serviceName(), ex);
                meterRegistry.counter("deltav_timeseries_batches_failed_total",
                        "location", location, "producer", producer,
                        "reason", "serialization_error").increment();
                return;
            }
            byte[] key = (location + "@" + sample.nodeId()).getBytes(StandardCharsets.UTF_8);
            Message<byte[]> message = MessageBuilder.withPayload(payload)
                    .setHeader(KafkaHeaders.KEY, key)
                    .build();
            boolean sent;
            try {
                sent = streamBridge.send(BINDING_NAME, message);
            } catch (RuntimeException ex) {
                LOG.warn("streamBridge.send threw for node {} svc {}",
                        sample.nodeId(), sample.serviceName(), ex);
                meterRegistry.counter("deltav_timeseries_batches_failed_total",
                        "location", location, "producer", producer,
                        "reason", "kafka_send_error").increment();
                return;
            }
            if (!sent) {
                meterRegistry.counter("deltav_timeseries_batches_failed_total",
                        "location", location, "producer", producer,
                        "reason", "kafka_send_error").increment();
                return;
            }
            meterRegistry.counter("deltav_timeseries_batches_published_total",
                    "location", location, "producer", producer).increment();
        } finally {
            timer.stop(Timer.builder("deltav_timeseries_publish_duration_seconds")
                    .tags("location", location, "producer", producer)
                    .register(meterRegistry));
        }
    }

    private TimeseriesBatch buildBatch(ResponseTimeSample sample, String location) {
        String svc = sample.serviceName() != null ? sample.serviceName() : "";
        String resourceId = "node[" + sample.nodeId() + "].monitoredService[" + svc + "]";
        Attribute responseAttr = Attribute.newBuilder()
                .setName(ATTRIBUTE_NAME)
                .setNumeric(sample.responseTimeMs())
                .setType(AttributeType.ATTRIBUTE_TYPE_GAUGE)
                .build();
        AttributeGroup group = AttributeGroup.newBuilder()
                .setName(GROUP_NAME)
                .addAttributes(responseAttr)
                .build();
        Resource resource = Resource.newBuilder()
                .setResourceId(resourceId)
                .setType(RESOURCE_TYPE)
                .setInstance(svc)
                .addGroups(group)
                .build();
        return TimeseriesBatch.newBuilder()
                .setTimestampMs(sample.timestampMs())
                .setNodeId(sample.nodeId())
                .setLocation(location)
                .setCollectionPackage(svc)
                .setProducer(sample.producerType())
                .addResources(resource)
                .build();
    }
}
```

Note vs. the originals: `buildBatch` uses the null-guarded `svc` for `resourceId` as well as `instance`/`collectionPackage`. The originals left `resourceId` unguarded (it would emit the literal `null`). Service name is never null on a real poll, so this is unreachable in practice — the consolidation just makes the three uses consistent.

- [ ] **Step 4: Run the test to verify it passes**

Run: `./mvnw -q -pl :org.opennms.core.poller-timeseries-common -am test`
Expected: PASS — 5 tests green.

- [ ] **Step 5: Commit**

```bash
git add core/poller-timeseries-common/src/main/java/org/deltav/poller/timeseries/ResponseTimePublisher.java core/poller-timeseries-common/src/test/java/org/deltav/poller/timeseries/ResponseTimePublisherTest.java
git commit -m "feat(poller-timeseries): add shared ResponseTimePublisher with unit tests"
```

---

## Task 4: Migrate Pollerd to the shared publisher

**Files:**
- Modify: `core/daemon-boot-pollerd/pom.xml`
- Modify: `core/daemon-boot-pollerd/src/main/java/org/deltav/netmgt/poller/boot/InstrumentedPollContext.java`
- Modify: `core/daemon-boot-pollerd/src/main/java/org/deltav/netmgt/poller/boot/PollerdTimeseriesConfiguration.java`
- Modify: `core/daemon-boot-pollerd/src/main/java/org/deltav/netmgt/poller/boot/PollerdDaemonConfiguration.java`
- Delete: `core/daemon-boot-pollerd/src/main/java/org/deltav/netmgt/poller/boot/PollResultPublisher.java`

- [ ] **Step 1: Add the module dependency to the Pollerd pom**

In `core/daemon-boot-pollerd/pom.xml`, inside `<dependencies>`, add (next to the other `org.deltav.core` intra-reactor deps such as `org.opennms.core.daemon-common`):

```xml
        <dependency>
            <groupId>org.deltav.core</groupId>
            <artifactId>org.opennms.core.poller-timeseries-common</artifactId>
            <version>${project.version}</version>
        </dependency>
```

- [ ] **Step 2: Rewrite `InstrumentedPollContext.java`**

Replace the entire body of `InstrumentedPollContext.java` (keep the license header) with:

```java
package org.deltav.netmgt.poller.boot;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import org.deltav.poller.timeseries.ResponseTimePublisher;
import org.deltav.poller.timeseries.ResponseTimeSample;
import org.deltav.timeseries.proto.ProducerType;
import org.opennms.core.tsid.TsidFactory;
import org.opennms.netmgt.config.PollerConfig;
import org.opennms.netmgt.events.api.EventIpcManager;
import org.opennms.netmgt.icmp.proxy.LocationAwarePingClient;
import org.opennms.netmgt.poller.PollStatus;
import org.opennms.netmgt.poller.QueryManager;
import org.opennms.netmgt.poller.pollables.PollEvent;
import org.opennms.netmgt.poller.pollables.PollableService;

/**
 * {@link StandalonePollContext} subclass that increments {@code deltav_pollerd_*}
 * Micrometer counters on every poll completion and outage transition, and
 * optionally publishes per-poll response-time samples to the
 * {@code deltav-timeseries} Kafka topic via the shared
 * {@link ResponseTimePublisher}.
 *
 * <p>The publisher is wired only when {@code deltav.timeseries.enabled=true};
 * otherwise it is null and Phase 3 publishing is a no-op (Phase 2 counters
 * still fire). Producer identity for the shared publisher is supplied here:
 * {@code PRODUCER_POLLERD} / {@code "pollerd"}.
 */
public class InstrumentedPollContext extends StandalonePollContext {

    private static final ProducerType PRODUCER_TYPE = ProducerType.PRODUCER_POLLERD;
    private static final String PRODUCER_LABEL = "pollerd";

    private final MeterRegistry registry;
    private final ResponseTimePublisher publisher;
    private final Timer pollDuration;

    public InstrumentedPollContext(EventIpcManager eventManager, PollerConfig pollerConfig,
                                   QueryManager queryManager, LocationAwarePingClient pingClient,
                                   TsidFactory tsidFactory, String localHostName, String name,
                                   MeterRegistry registry, ResponseTimePublisher publisher) {
        super(eventManager, pollerConfig, queryManager, pingClient, tsidFactory, localHostName, name);
        this.registry = registry;
        this.publisher = publisher;
        this.pollDuration = registry.timer(PollerdDomainMetrics.POLL_DURATION);
    }

    @Override
    public void trackPoll(PollableService service, PollStatus status) {
        super.trackPoll(service, status);
        recordPoll(service, status);
        if (publisher != null && service != null && hasResponseTime(status)) {
            publisher.publish(toSample(service, status));
        }
    }

    @Override
    public void openOutage(PollableService service, PollEvent event) {
        super.openOutage(service, event);
        registry.counter(PollerdDomainMetrics.OUTAGES_OPENED,
                PollerdDomainMetrics.TAG_LOCATION, location(service)).increment();
    }

    @Override
    public void resolveOutage(PollableService service, PollEvent event) {
        super.resolveOutage(service, event);
        registry.counter(PollerdDomainMetrics.OUTAGES_RESOLVED,
                PollerdDomainMetrics.TAG_LOCATION, location(service)).increment();
    }

    private void recordPoll(PollableService service, PollStatus status) {
        String location = location(service);
        String result = status != null && status.getStatusName() != null
                ? status.getStatusName().toLowerCase() : "unknown";
        registry.counter(PollerdDomainMetrics.POLLS_COMPLETED,
                PollerdDomainMetrics.TAG_LOCATION, location,
                PollerdDomainMetrics.TAG_RESULT, result).increment();
        if (status != null && status.getResponseTime() != null) {
            pollDuration.record((long) (status.getResponseTime() * 1_000_000.0),
                    java.util.concurrent.TimeUnit.NANOSECONDS);
        }
    }

    private static boolean hasResponseTime(PollStatus status) {
        return status != null && status.getResponseTime() != null
                && !Double.isNaN(status.getResponseTime());
    }

    private static ResponseTimeSample toSample(PollableService service, PollStatus status) {
        long timestampMs = status.getTimestamp() != null
                ? status.getTimestamp().getTime() : System.currentTimeMillis();
        String location = service.getNodeLocation() != null ? service.getNodeLocation() : "Default";
        return new ResponseTimeSample(
                service.getNodeId(),
                service.getSvcName(),
                location,
                status.getResponseTime(),
                timestampMs,
                PRODUCER_TYPE,
                PRODUCER_LABEL);
    }

    private static String location(PollableService service) {
        if (service == null) return "unknown";
        String l = service.getNodeLocation();
        return l != null ? l : "Default";
    }
}
```

- [ ] **Step 3: Rewrite the publisher bean in `PollerdTimeseriesConfiguration.java`**

In `PollerdTimeseriesConfiguration.java`: change the import `org.deltav.timeseries.proto.ProducerType` to `org.deltav.poller.timeseries.ResponseTimePublisher`. Leave the `deltavTimeseriesTopic` `@Bean` unchanged. Replace the `pollResultPublisher` `@Bean` method with:

```java
    @Bean
    public ResponseTimePublisher responseTimePublisher(StreamBridge streamBridge,
                                                       MeterRegistry meterRegistry) {
        return new ResponseTimePublisher(streamBridge, meterRegistry);
    }
```

- [ ] **Step 4: Update the `ObjectProvider` type in `PollerdDaemonConfiguration.java`**

In `PollerdDaemonConfiguration.java`: the `pollContext` `@Bean` method (around line 158-170) declares a parameter `ObjectProvider<PollResultPublisher> publisherProvider`. Change the generic type to `ResponseTimePublisher`:

```java
                                  ObjectProvider<ResponseTimePublisher> publisherProvider) {
```

Update the import: replace any `import` of `org.deltav.netmgt.poller.boot.PollResultPublisher` (if present — it is same-package, so likely no explicit import) and add `import org.deltav.poller.timeseries.ResponseTimePublisher;`. Also update the javadoc reference to `PollResultPublisher` in that method's comment to `ResponseTimePublisher`. The method body (`new InstrumentedPollContext(... publisherProvider.getIfAvailable())`) needs no change — the argument type now matches the constructor.

- [ ] **Step 5: Delete the old publisher**

```bash
git rm core/daemon-boot-pollerd/src/main/java/org/deltav/netmgt/poller/boot/PollResultPublisher.java
```

- [ ] **Step 6: Build Pollerd to verify it compiles**

Run: `./mvnw -q -pl :org.opennms.core.daemon-boot-pollerd -am clean install -DskipTests`
Expected: `BUILD SUCCESS`. (`clean` is required — adding a dependency to a Spring Boot module needs a clean repackage per memory `feedback_spring_boot_repackage_needs_clean`.)

- [ ] **Step 7: Commit**

```bash
git add core/daemon-boot-pollerd/pom.xml core/daemon-boot-pollerd/src/main/java/org/deltav/netmgt/poller/boot/InstrumentedPollContext.java core/daemon-boot-pollerd/src/main/java/org/deltav/netmgt/poller/boot/PollerdTimeseriesConfiguration.java core/daemon-boot-pollerd/src/main/java/org/deltav/netmgt/poller/boot/PollerdDaemonConfiguration.java
git commit -m "refactor(pollerd): use shared ResponseTimePublisher, delete PollResultPublisher"
```

---

## Task 5: Migrate PerspectivePollerd to the shared publisher

**Files:**
- Modify: `core/daemon-boot-perspectivepollerd/pom.xml`
- Modify: `core/daemon-boot-perspectivepollerd/src/main/java/org/deltav/netmgt/perspectivepoller/boot/InstrumentedPerspectivePollerd.java`
- Modify: `core/daemon-boot-perspectivepollerd/src/main/java/org/deltav/netmgt/perspectivepoller/boot/PerspectivePollerdTimeseriesConfiguration.java`
- Modify: `core/daemon-boot-perspectivepollerd/src/main/java/org/deltav/netmgt/perspectivepoller/boot/PerspectivePollerdDaemonConfiguration.java`
- Delete: `core/daemon-boot-perspectivepollerd/src/main/java/org/deltav/netmgt/perspectivepoller/boot/PerspectiveResponseTimePublisher.java`

- [ ] **Step 1: Add the module dependency to the PerspectivePollerd pom**

In `core/daemon-boot-perspectivepollerd/pom.xml`, inside `<dependencies>`, add:

```xml
        <dependency>
            <groupId>org.deltav.core</groupId>
            <artifactId>org.opennms.core.poller-timeseries-common</artifactId>
            <version>${project.version}</version>
        </dependency>
```

- [ ] **Step 2: Edit `InstrumentedPerspectivePollerd.java`**

In `InstrumentedPerspectivePollerd.java`, make these changes:

Add imports:
```java
import org.deltav.poller.timeseries.ResponseTimePublisher;
import org.deltav.poller.timeseries.ResponseTimeSample;
import org.deltav.timeseries.proto.ProducerType;
```

Change the field declaration:
```java
    private final ResponseTimePublisher publisher;
```

Change the constructor's last parameter type from `PerspectiveResponseTimePublisher publisher` to `ResponseTimePublisher publisher` (the assignment `this.publisher = publisher;` is unchanged).

Add two producer-identity constants alongside the existing fields:
```java
    private static final ProducerType PRODUCER_TYPE = ProducerType.PRODUCER_PERSPECTIVE_POLLERD;
    private static final String PRODUCER_LABEL = "perspectivepollerd";
```

Replace the `persistResponseTimeData` override's publisher call. The current method is:
```java
    @Override
    public void persistResponseTimeData(PerspectivePolledService polledService, PollStatus pollStatus) {
        try {
            super.persistResponseTimeData(polledService, pollStatus);
        } finally {
            recordPoll(polledService, pollStatus);
            if (publisher != null) {
                publisher.publish(polledService, pollStatus);
            }
        }
    }
```
Change the publisher branch to adapt and guard:
```java
    @Override
    public void persistResponseTimeData(PerspectivePolledService polledService, PollStatus pollStatus) {
        try {
            super.persistResponseTimeData(polledService, pollStatus);
        } finally {
            recordPoll(polledService, pollStatus);
            if (publisher != null && polledService != null && hasResponseTime(pollStatus)) {
                publisher.publish(toSample(polledService, pollStatus));
            }
        }
    }
```

Add the two private helpers at the end of the class:
```java
    private static boolean hasResponseTime(PollStatus status) {
        return status != null && status.getResponseTime() != null
                && !Double.isNaN(status.getResponseTime());
    }

    private static ResponseTimeSample toSample(PerspectivePolledService svc, PollStatus status) {
        long timestampMs = status.getTimestamp() != null
                ? status.getTimestamp().getTime() : System.currentTimeMillis();
        String perspective = svc.getPerspectiveLocation() != null
                ? svc.getPerspectiveLocation() : "Default";
        return new ResponseTimeSample(
                svc.getNodeId(),
                svc.getServiceName(),
                perspective,
                status.getResponseTime(),
                timestampMs,
                PRODUCER_TYPE,
                PRODUCER_LABEL);
    }
```

Leave `recordPoll` and the rest of the class unchanged.

- [ ] **Step 3: Rewrite the publisher bean in `PerspectivePollerdTimeseriesConfiguration.java`**

In `PerspectivePollerdTimeseriesConfiguration.java`: add import `import org.deltav.poller.timeseries.ResponseTimePublisher;`. Leave the `deltavTimeseriesTopic` `@Bean` unchanged. Replace the `perspectiveResponseTimePublisher` `@Bean` method with:

```java
    @Bean
    public ResponseTimePublisher responseTimePublisher(StreamBridge streamBridge,
                                                       MeterRegistry meterRegistry) {
        return new ResponseTimePublisher(streamBridge, meterRegistry);
    }
```

- [ ] **Step 4: Update `PerspectivePollerdDaemonConfiguration.java`**

In `PerspectivePollerdDaemonConfiguration.java`, the `perspectivePollerd` `@Bean` method (around line 160-184):
- Change the parameter `ObjectProvider<PerspectiveResponseTimePublisher> publisherProvider` to `ObjectProvider<ResponseTimePublisher> publisherProvider`.
- Change the local variable line `PerspectiveResponseTimePublisher publisher = publisherProvider.getIfAvailable();` to `ResponseTimePublisher publisher = publisherProvider.getIfAvailable();`.
- Add import `import org.deltav.poller.timeseries.ResponseTimePublisher;`; remove any explicit import of `PerspectiveResponseTimePublisher` (same-package — likely none).

The `new InstrumentedPerspectivePollerd(... publisher)` call needs no change — the argument type now matches.

- [ ] **Step 5: Delete the old publisher**

```bash
git rm core/daemon-boot-perspectivepollerd/src/main/java/org/deltav/netmgt/perspectivepoller/boot/PerspectiveResponseTimePublisher.java
```

- [ ] **Step 6: Build PerspectivePollerd to verify it compiles**

Run: `./mvnw -q -pl :org.opennms.core.daemon-boot-perspectivepollerd -am clean install -DskipTests`
Expected: `BUILD SUCCESS`.

- [ ] **Step 7: Commit**

```bash
git add core/daemon-boot-perspectivepollerd/pom.xml core/daemon-boot-perspectivepollerd/src/main/java/org/deltav/netmgt/perspectivepoller/boot/InstrumentedPerspectivePollerd.java core/daemon-boot-perspectivepollerd/src/main/java/org/deltav/netmgt/perspectivepoller/boot/PerspectivePollerdTimeseriesConfiguration.java core/daemon-boot-perspectivepollerd/src/main/java/org/deltav/netmgt/perspectivepoller/boot/PerspectivePollerdDaemonConfiguration.java
git commit -m "refactor(perspectivepollerd): use shared ResponseTimePublisher, delete PerspectiveResponseTimePublisher"
```

---

## Task 6: Add the Pollerd Grafana dashboard

**Files:**
- Create: `opennms-container/delta-v/grafana/dashboards/pollerd-monitoring.json`

- [ ] **Step 1: Create the dashboard JSON**

Create `opennms-container/delta-v/grafana/dashboards/pollerd-monitoring.json` — mirrors `perspective-monitoring.json`, retargeted to direct Pollerd (`producer="pollerd"`, location not perspective):

```json
{
  "annotations": {
    "list": [
      {
        "builtIn": 1,
        "datasource": { "type": "grafana", "uid": "-- Grafana --" },
        "enable": true,
        "hide": true,
        "iconColor": "rgba(0, 211, 255, 1)",
        "name": "Annotations & Alerts",
        "type": "dashboard"
      }
    ]
  },
  "description": "Direct Pollerd service-poll latency and throughput. Panels query the Phase 3 publisher series (deltav-timeseries Kafka topic → prometheus-writer → VictoriaMetrics) and Pollerd's /actuator/prometheus counters.",
  "editable": true,
  "fiscalYearStartMonth": 0,
  "graphTooltip": 0,
  "id": null,
  "links": [],
  "panels": [
    {
      "datasource": { "type": "prometheus", "uid": "victoriametrics" },
      "fieldConfig": {
        "defaults": {
          "color": { "mode": "palette-classic" },
          "custom": {
            "drawStyle": "line",
            "fillOpacity": 10,
            "lineWidth": 2,
            "pointSize": 5,
            "showPoints": "auto",
            "spanNulls": false,
            "stacking": { "group": "A", "mode": "none" },
            "axisLabel": "response time",
            "axisPlacement": "auto"
          },
          "mappings": [],
          "thresholds": { "mode": "absolute", "steps": [{ "color": "green", "value": null }] },
          "unit": "ms"
        },
        "overrides": []
      },
      "gridPos": { "h": 9, "w": 16, "x": 0, "y": 0 },
      "id": 1,
      "options": {
        "legend": { "displayMode": "list", "placement": "bottom", "showLegend": true, "calcs": ["mean", "max"] },
        "tooltip": { "mode": "multi", "sort": "desc" }
      },
      "targets": [
        {
          "datasource": { "type": "prometheus", "uid": "victoriametrics" },
          "expr": "opennms_response_time_response{producer=\"pollerd\"}",
          "instant": false,
          "legendFormat": "{{location}} → {{node_label}}/{{resource_instance}}",
          "refId": "A"
        }
      ],
      "title": "Service response time (ms)",
      "type": "timeseries",
      "description": "Each line is one (location, target service) pair as measured by direct Pollerd polls."
    },
    {
      "datasource": { "type": "prometheus", "uid": "victoriametrics" },
      "fieldConfig": {
        "defaults": {
          "color": { "mode": "thresholds" },
          "mappings": [],
          "thresholds": {
            "mode": "absolute",
            "steps": [
              { "color": "green", "value": null },
              { "color": "yellow", "value": 100 },
              { "color": "red", "value": 500 }
            ]
          },
          "unit": "ms"
        },
        "overrides": []
      },
      "gridPos": { "h": 9, "w": 8, "x": 16, "y": 0 },
      "id": 2,
      "options": {
        "colorMode": "value",
        "graphMode": "area",
        "justifyMode": "auto",
        "orientation": "horizontal",
        "reduceOptions": { "calcs": ["lastNotNull"], "fields": "", "values": false },
        "textMode": "auto"
      },
      "targets": [
        {
          "datasource": { "type": "prometheus", "uid": "victoriametrics" },
          "expr": "avg by (location) (opennms_response_time_response{producer=\"pollerd\"})",
          "instant": true,
          "legendFormat": "{{location}}",
          "refId": "A"
        }
      ],
      "title": "Latest avg response time by location",
      "type": "stat"
    },
    {
      "datasource": { "type": "prometheus", "uid": "victoriametrics" },
      "fieldConfig": {
        "defaults": {
          "color": { "mode": "palette-classic" },
          "custom": {
            "drawStyle": "line",
            "fillOpacity": 10,
            "lineWidth": 2,
            "pointSize": 5,
            "showPoints": "auto",
            "stacking": { "group": "A", "mode": "none" }
          },
          "thresholds": { "mode": "absolute", "steps": [{ "color": "green", "value": null }] },
          "unit": "ops"
        },
        "overrides": []
      },
      "gridPos": { "h": 9, "w": 12, "x": 0, "y": 9 },
      "id": 3,
      "options": {
        "legend": { "displayMode": "list", "placement": "bottom", "showLegend": true, "calcs": ["mean"] },
        "tooltip": { "mode": "multi", "sort": "desc" }
      },
      "targets": [
        {
          "datasource": { "type": "prometheus", "uid": "victoriametrics" },
          "expr": "rate(deltav_pollerd_polls_completed_total[1m])",
          "legendFormat": "{{location}} ({{result}})",
          "refId": "A"
        }
      ],
      "title": "Polls/sec by location + result",
      "type": "timeseries",
      "description": "Direct from Pollerd's /actuator/prometheus (deltav_pollerd_polls_completed_total). Faceted by location and result so 'down' polls stand out from 'up' ones."
    },
    {
      "datasource": { "type": "prometheus", "uid": "victoriametrics" },
      "fieldConfig": {
        "defaults": {
          "color": { "mode": "palette-classic" },
          "custom": {
            "drawStyle": "line",
            "fillOpacity": 5,
            "lineWidth": 1,
            "showPoints": "auto",
            "stacking": { "group": "A", "mode": "none" }
          },
          "thresholds": {
            "mode": "absolute",
            "steps": [
              { "color": "green", "value": null },
              { "color": "red", "value": 1 }
            ]
          },
          "unit": "ops"
        },
        "overrides": []
      },
      "gridPos": { "h": 9, "w": 12, "x": 12, "y": 9 },
      "id": 4,
      "options": {
        "legend": { "displayMode": "list", "placement": "bottom", "showLegend": true, "calcs": ["sum"] },
        "tooltip": { "mode": "multi", "sort": "desc" }
      },
      "targets": [
        {
          "datasource": { "type": "prometheus", "uid": "victoriametrics" },
          "expr": "rate(deltav_timeseries_batches_failed_total{producer=\"pollerd\"}[5m])",
          "legendFormat": "{{location}} ({{reason}})",
          "refId": "A"
        }
      ],
      "title": "Phase 3 publish failures (pollerd)",
      "type": "timeseries",
      "description": "Rate of deltav_timeseries_batches_failed_total scoped to producer=pollerd. Should stay flat at zero; non-zero indicates Kafka or serialization issues with the Phase 3 publisher."
    },
    {
      "datasource": { "type": "prometheus", "uid": "victoriametrics" },
      "fieldConfig": {
        "defaults": {
          "color": { "mode": "palette-classic" },
          "custom": {
            "drawStyle": "line",
            "fillOpacity": 10,
            "lineWidth": 2,
            "showPoints": "auto",
            "stacking": { "group": "A", "mode": "none" }
          },
          "thresholds": { "mode": "absolute", "steps": [{ "color": "green", "value": null }] },
          "unit": "short"
        },
        "overrides": []
      },
      "gridPos": { "h": 9, "w": 24, "x": 0, "y": 18 },
      "id": 5,
      "options": {
        "legend": { "displayMode": "list", "placement": "bottom", "showLegend": true, "calcs": ["sum"] },
        "tooltip": { "mode": "multi", "sort": "desc" }
      },
      "targets": [
        {
          "datasource": { "type": "prometheus", "uid": "victoriametrics" },
          "expr": "increase(deltav_timeseries_batches_published_total{producer=\"pollerd\"}[5m])",
          "legendFormat": "{{location}}",
          "refId": "A"
        }
      ],
      "title": "Phase 3 records published (5m bucket)",
      "type": "timeseries",
      "description": "Confirms Pollerd's publisher is emitting to deltav-timeseries. Each bucket = records sent in the prior 5 minutes, by location. Zero across the board means the publisher is not running or deltav.timeseries.enabled is off."
    }
  ],
  "preload": false,
  "refresh": "10s",
  "schemaVersion": 39,
  "tags": ["delta-v", "pollerd", "polling"],
  "templating": { "list": [] },
  "time": { "from": "now-1h", "to": "now" },
  "timepicker": {},
  "timezone": "browser",
  "title": "Pollerd Monitoring",
  "uid": "pollerd-monitoring",
  "version": 1,
  "weekStart": ""
}
```

- [ ] **Step 2: Validate the JSON parses**

Run: `python3 -c "import json; json.load(open('opennms-container/delta-v/grafana/dashboards/pollerd-monitoring.json')); print('valid JSON')"`
Expected: `valid JSON`.

- [ ] **Step 3: Commit**

```bash
git add opennms-container/delta-v/grafana/dashboards/pollerd-monitoring.json
git commit -m "feat(grafana): add Pollerd service-latency dashboard"
```

---

## Task 7: Correct the stale release-plan doc

**Files:**
- Modify: `docs/plans/2026-04-23-v1.2.0-release-plan.md`

- [ ] **Step 1: Correct the PerspectivePollerd Phase 3 status**

In `docs/plans/2026-04-23-v1.2.0-release-plan.md`, the Track 4 section has a subsection headed `#### PerspectivePollerd Phase 3 — ⏳ DEFERRED`. Change the heading to `#### PerspectivePollerd Phase 3 — ✅ DONE (develop, post-rc2.2)` and replace its body paragraph with:

```markdown
PerspectivePollerd Phase 3 shipped after rc2.2 in commit `5df8e8cab4c`.
`InstrumentedPerspectivePollerd extends PerspectivePollerd` and overrides
the public `persistResponseTimeData(...)` callback — the private Quartz
lambda calls that public method, so the method is the seam (the earlier
"no public seam" assessment did not hold). `PerspectiveResponseTimePublisher`
mirrors the Pollerd publisher with `PRODUCER_PERSPECTIVE_POLLERD`. Gated by
`deltav.perspective.timeseries.enabled`. First released in v1.2.0-rc3,
which also unified the two publishers into the shared
`core/poller-timeseries-common` module (see
`docs/superpowers/specs/2026-05-15-poller-timeseries-unification-design.md`).
```

- [ ] **Step 2: Commit**

```bash
git add docs/plans/2026-04-23-v1.2.0-release-plan.md
git commit -m "docs(release-plan): correct stale PerspectivePollerd Phase 3 status"
```

---

## Task 8: Full build + local smoke validation

**Files:** none (verification only)

- [ ] **Step 1: Full reactor build of the touched modules**

Run: `./mvnw -q -pl :org.opennms.core.poller-timeseries-common,:org.opennms.core.daemon-boot-pollerd,:org.opennms.core.daemon-boot-perspectivepollerd -am clean install`
Expected: `BUILD SUCCESS`, `ResponseTimePublisherTest` 5 tests green.

- [ ] **Step 2: Rebuild the Pollerd and PerspectivePollerd daemon images**

Run the project image build for the two daemons (per the repo's `opennms-container/delta-v/build.sh` daemon build path). Confirm both images build without error.

- [ ] **Step 3: Bring up the local docker-compose stack**

Run: `cd opennms-container/delta-v && docker compose --profile metrics up -d`
Wait for all services healthy.

- [ ] **Step 4: Verify both daemons booted clean**

Run: `docker compose logs pollerd perspectivepollerd | grep -iE "started .*Application|exception" | tail -30`
Expected: each daemon logs a Spring Boot "Started ... Application" line; no startup exceptions. (Daemon-startup regressions do not surface in Maven/CI — this step is mandatory per memory `project_alarmd_alarm_lifecycle_gap`.)

- [ ] **Step 5: Verify both producers reach VictoriaMetrics**

After at least one poll cycle, query VictoriaMetrics:
Run: `curl -s 'http://localhost:8428/api/v1/query?query=opennms_response_time_response' | python3 -c "import sys,json; d=json.load(sys.stdin); print(sorted({m['metric'].get('producer') for m in d['data']['result']}))"`
Expected: the set includes both `pollerd` and `perspective_pollerd` (note: PerspectivePollerd's series label is `perspective_pollerd` — see Known Issues in the spec).

- [ ] **Step 6: Verify both Grafana dashboards render**

Open Grafana (`http://localhost:3000`), folder "Delta-V". Confirm both `Pollerd Monitoring` and `Perspective Monitoring` dashboards exist and their panels show data after a poll cycle.

- [ ] **Step 7: No commit** — this task is verification only. If any step fails, fix the cause and re-run the affected earlier task; do not proceed.

---

## Task 9: Release (rc3) — post-merge, separate chore PR

**Files:** version metadata (handled by `versions:set`)

> Do this only **after** the unification PR is reviewed and merged to `develop`. The version bump is a separate chore PR, matching how rc1→rc2 was cut (PR #249).

- [ ] **Step 1: Bump the reactor version**

On a fresh branch off updated `develop`:
Run: `./mvnw -q versions:set -DnewVersion=1.2.0-rc3 -DprocessAllModules=true -DgenerateBackupPoms=false`

- [ ] **Step 2: Verify the new module was bumped too**

Run: `grep -c "1.2.0-rc3" core/poller-timeseries-common/pom.xml`
Expected: `1` (the `<parent>` version). Confirms `versions:set` reached the newly-added module.

- [ ] **Step 3: Commit, PR, merge**

```bash
git add -A
git commit -m "chore: bump version 1.2.0-rc2.2 → 1.2.0-rc3"
```
Open the PR with `gh pr create --repo pbrane/delta-v --base develop` and merge after review.

- [ ] **Step 4: Tag rc3**

After merge, on updated `develop`: `git tag v1.2.0-rc3 && git push origin v1.2.0-rc3`. This is the first released tag carrying PerspectivePollerd Phase 3 plus the publisher unification.

---

## Self-review notes

- **Spec coverage:** new module (Task 1), `ResponseTimeSample` (Task 2), `ResponseTimePublisher` + first unit test (Task 3), Pollerd migration (Task 4), PerspectivePollerd migration (Task 5), Pollerd dashboard (Task 6), stale-doc correction (Task 7), no-behavior-change verification via E2E + smoke (Task 8), rc3 (Task 9). All spec sections mapped.
- **Producer-label inconsistency** (spec Known Issues) is deliberately *not* fixed — the new dashboard uses today's actual `producer="pollerd"` value; Task 8 Step 5 documents the `perspective_pollerd` asymmetry rather than changing it.
- **Type consistency:** the shared bean is `ResponseTimePublisher` everywhere; both daemons' `ObjectProvider<ResponseTimePublisher>`, both `@Bean` methods named `responseTimePublisher`, both adapters produce `ResponseTimeSample`. The two deleted classes (`PollResultPublisher`, `PerspectiveResponseTimePublisher`) have no remaining references after Tasks 4–5.
