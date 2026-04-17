# Kafka Time Series Phase 2 — Prometheus Remote Write Consumer — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the first consumer of the delta-v Kafka Time Series pipeline. A new standalone Spring Boot 4 service (`core/prometheus-writer/`) consumes `deltav-timeseries`, enriches each batch with a local materialization of `deltav-node-context`, transforms to Prometheus Remote Write samples, and POSTs Snappy-compressed protobuf batches to a configurable RW endpoint — with tiered retry, Resilience4j circuit breaker, DLQ topic, and Kafka-lag backpressure.

**Architecture:** Stateless hot path. Plain Spring Cloud Stream Kafka binder for `deltav-timeseries` (no Kafka Streams, no GlobalKTable). Dedicated `NodeContextCache` bean with its own Kafka consumer bootstraps from `deltav-node-context` compacted topic before the timeseries binding is resumed (startup gate). Translation is a pure function `(TimeseriesBatch, Optional<NodeContext>) → List<PromSample>`. `BatchingRwWriter` buffers samples (1000 / 1 MB / 1 s whichever first) and delegates to `RemoteWriteHttpClient` guarded by Resilience4j. Poison-pill batches (400 / 413) land in a new `deltav-prometheus-writer-dlq` Kafka topic.

**Tech Stack:** Java 21, Spring Boot 4.0.3, Spring Cloud Stream 5.x Kafka binder, Spring 6 `RestClient`, Resilience4j circuit breaker, Snappy compression, vendored Prometheus `remote.proto` + `types.proto`, protobuf 3.25.5 via `protobuf-maven-plugin` 0.6.1, Micrometer Prometheus, JUnit 5, Mockito, MockWebServer, `spring-cloud-stream-test-binder`, Testcontainers Kafka.

**Design doc:** `docs/superpowers/specs/2026-04-17-kafka-ts-phase-2-prometheus-consumer-design.md`
**Next-session prompt (implementation):** `docs/superpowers/next-session-prompts/2026-04-18-kafka-ts-phase-2-implementation.md` (written in this planning session)
**Feature branch:** `feature/kafka-ts-phase-2-prometheus-consumer` (already created; spec commit `a0936466b67` present on branch)

**Critical memory references:**
- `feedback_never_pr_opennms` — delta-v PRs always use `gh pr create --repo pbrane/delta-v`
- `feedback_feature_branches` — never commit directly to `develop`
- `feedback_deltav_package_namespace` — new code under `org.deltav.*` with BeaconStrategists copyright
- `feedback_delta_v_uses_mvnw_not_compile_pl` — all builds use `./mvnw`
- `feedback_delta_v_full_reactor_verify` — full-reactor build after cross-module changes
- `feedback_rebuild_all_daemons` — before `./build.sh deltav`, rebuild all 13 daemon-boot jars + flow-enricher + prometheus-writer
- `feedback_spring_boot_scan_package_trap` — real-main-class IT mandatory (Task 19)
- `feedback_spring_cloud_stream_splitter` — `use-native-encoding: true` on byte-array producer bindings (Task 17)
- `feedback_bom_import_precedence` — Spring Boot BOM must be imported before horizon BOM

**Spec section map (for cross-reference):**
- Spec §1 NodeContextCache bootstrap → Tasks 4-5
- Spec §2 Startup gate → Task 7
- Spec §3 Metric naming + labels → Tasks 8-9
- Spec §4 Sample construction → Task 10
- Spec §5 RW output pipeline → Tasks 11-14
- Spec §6 DLQ → Task 17
- Spec §7 Observability → Task 18
- Spec §8 Configuration yaml → Task 3
- Spec §9 Docker integration → Tasks 21-23
- Spec §10 E2E script → Task 24
- Spec "Scar prophylactics" → woven through: Task 3 (yaml parity), Task 17 (use-native-encoding), Task 19 (real-main-class IT), Task 24 (pipefail-safe grep), Task 25 (full-reactor verify + rebuild-all reminder)

---

## File Structure

### New files

```
core/deltav-kafka-contracts/src/main/proto/
├── prometheus/prompb/remote.proto                        vendored upstream
└── prometheus/prompb/types.proto                         vendored upstream

core/prometheus-writer/
├── pom.xml
├── Dockerfile
└── src/main/
    ├── java/org/deltav/prometheus/writer/
    │   ├── PrometheusWriterApplication.java              @SpringBootApplication main
    │   ├── PrometheusWriterConfiguration.java            @Configuration + @EnableConfigurationProperties
    │   ├── config/
    │   │   ├── PrometheusWriterProperties.java           @ConfigurationProperties("prometheus-writer")
    │   │   └── KafkaTopicsConfiguration.java             @Bean NewTopic deltavPrometheusWriterDlqTopic
    │   ├── nodecontext/
    │   │   ├── NodeContextCache.java                     ConcurrentHashMap holder
    │   │   ├── NodeContextKafkaBootstrap.java            dedicated consumer + HWM bootstrap
    │   │   ├── NodeContextCacheHealthIndicator.java      HealthIndicator for readiness
    │   │   └── NodeContextCacheReadyEvent.java           Spring ApplicationEvent
    │   ├── startup/
    │   │   ├── TimeseriesBindingStartupGate.java         ListenerContainerCustomizer (starts paused)
    │   │   └── TimeseriesBindingResumer.java             @EventListener(NodeContextCacheReadyEvent.class)
    │   ├── translate/
    │   │   ├── PromSample.java                           record(name, labels, value, timestampMs)
    │   │   ├── NameSanitizer.java                        sanitize + collision detection
    │   │   ├── LabelBuilder.java                         default labels + metadata allowlist
    │   │   └── TimeseriesToPromTranslator.java           pure function batch → List<PromSample>
    │   ├── consume/
    │   │   ├── TimeseriesConsumer.java                   SCS Function<Message<byte[]>, Void>
    │   │   └── TimeseriesConsumerBinding.java            @Bean definition registration
    │   ├── rw/
    │   │   ├── BatchingRwWriter.java                     accumulate + flush on thresholds
    │   │   ├── RemoteWriteHttpClient.java                RestClient + Snappy + auth
    │   │   ├── RemoteWriteRetryPolicy.java               status-code classifier
    │   │   ├── PrometheusWriterCircuitBreaker.java       Resilience4j bean
    │   │   ├── ConsumerPauseListener.java                circuit → binding pause/resume
    │   │   └── WriteRequestBuilder.java                  samples → WriteRequest protobuf
    │   ├── dlq/
    │   │   └── DlqPublisher.java                         SCS StreamBridge send to DLQ
    │   └── metrics/
    │       └── PrometheusWriterMetrics.java              central Micrometer meter declarations
    └── resources/
        ├── application.yml
        └── logback-spring.xml                            standard delta-v log config

core/prometheus-writer/src/test/java/org/deltav/prometheus/writer/
├── nodecontext/
│   ├── NodeContextCacheTest.java
│   ├── NodeContextKafkaBootstrapIT.java                  Testcontainers Kafka
│   └── NodeContextCacheHealthIndicatorTest.java
├── startup/
│   └── TimeseriesBindingStartupGateTest.java
├── translate/
│   ├── NameSanitizerTest.java
│   ├── LabelBuilderTest.java
│   └── TimeseriesToPromTranslatorTest.java
├── rw/
│   ├── RemoteWriteHttpClientTest.java                    MockWebServer
│   ├── RemoteWriteRetryPolicyTest.java
│   ├── BatchingRwWriterTest.java
│   ├── PrometheusWriterCircuitBreakerTest.java
│   ├── ConsumerPauseListenerTest.java
│   ├── WriteRequestBuilderTest.java
│   └── RwRoundTripIT.java                                Testcontainers + MockWebServer
├── dlq/
│   └── DlqPublisherBinderIT.java                         SCS test-binder
├── consume/
│   └── TimeseriesConsumerBinderIT.java                   SCS test-binder
└── PrometheusWriterApplicationScanIT.java                real-main-class

opennms-container/delta-v/
├── Dockerfile.prometheus-writer                          new
└── test-prometheus-writer-e2e.sh                         6-step E2E
```

### Modified files

```
pom.xml                                                    add core/prometheus-writer to <modules>
core/deltav-kafka-contracts/pom.xml                        (no changes — existing protobuf-maven-plugin picks up new vendored protos)
core/deltav-kafka-contracts/src/main/proto/deltav-timeseries.proto
                                                           update header comment: Phase 2 GA freeze
core/deltav-kafka-contracts/src/main/proto/deltav-node-context.proto
                                                           update header comment: Phase 2 GA freeze
opennms-container/delta-v/docker-compose.yml               add prometheus-writer + victoriametrics services
opennms-container/delta-v/build.sh                         add do_prometheus_writer_image() + call site
opennms-container/delta-v/README.md                        Phase 2 section (env vars, metrics, topic)
```

---

## Task ordering rationale

The plan has **26 tasks** in 8 groups. The ordering is dependency-driven and commit-boundaried:

1. **Foundation (Tasks 1-3)** — contracts + module skeleton + config property classes. Nothing depends on anything; these commits are independent.
2. **Cache subsystem (Tasks 4-6)** — `NodeContextCache` hierarchy. Task 5 is the only one that needs Testcontainers.
3. **Startup gate (Task 7)** — depends on cache subsystem being wired.
4. **Translation (Tasks 8-10)** — pure functions, fully unit-tested in isolation.
5. **RW output (Tasks 11-14)** — HTTP client, retry, batching, circuit breaker. Each is independently testable.
6. **Wiring + DLQ + Observability (Tasks 15-18)** — ties the pipeline together.
7. **Integration verification (Tasks 19-20)** — real-main-class IT + full Testcontainers round-trip.
8. **Docker + E2E + Docs (Tasks 21-26)** — deployment artifacts and the 6-step E2E; final task freezes schemas and queues the next-session prompt.

Each task ends with a commit. Commits are small (one subsystem or one concern per commit) so reviews and reverts are targeted.

---

## Task 1: Vendor Prometheus `remote.proto` + `types.proto`

**Goal:** Make `WriteRequest`, `TimeSeries`, `Sample`, `Label`, and `MetricMetadata` protobuf Java classes available in `core/deltav-kafka-contracts`.

**Files:**
- Create: `core/deltav-kafka-contracts/src/main/proto/prometheus/prompb/types.proto`
- Create: `core/deltav-kafka-contracts/src/main/proto/prometheus/prompb/remote.proto`

- [ ] **Step 1: Create the directory**

```bash
mkdir -p core/deltav-kafka-contracts/src/main/proto/prometheus/prompb
```

- [ ] **Step 2: Write `types.proto`**

Vendor the upstream Prometheus protobuf types verbatim from <https://github.com/prometheus/prometheus/blob/main/prompb/types.proto> (Apache-2.0 licensed). Write to `core/deltav-kafka-contracts/src/main/proto/prometheus/prompb/types.proto`:

```protobuf
// Copyright 2017 Prometheus Team
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Vendored into delta-v for use by the prometheus-writer Remote Write client.
// Source: github.com/prometheus/prometheus/prompb/types.proto (Apache-2.0)

syntax = "proto3";
package prometheus;

option go_package = "github.com/prometheus/prometheus/prompb";
option java_package = "prometheus.prompb";
option java_multiple_files = true;

message MetricMetadata {
  enum MetricType {
    UNKNOWN        = 0;
    COUNTER        = 1;
    GAUGE          = 2;
    HISTOGRAM      = 3;
    GAUGEHISTOGRAM = 4;
    SUMMARY        = 5;
    INFO           = 6;
    STATESET       = 7;
  }

  MetricType type = 1;
  string metric_family_name = 2;
  string help = 4;
  string unit = 5;
}

message Sample {
  double value    = 1;
  int64 timestamp = 2;
}

message Exemplar {
  repeated Label labels = 1;
  double value          = 2;
  int64 timestamp       = 3;
}

message TimeSeries {
  repeated Label labels      = 1;
  repeated Sample samples    = 2;
  repeated Exemplar exemplars = 3;
}

message Label {
  string name  = 1;
  string value = 2;
}

message Labels {
  repeated Label labels = 1;
}

message LabelMatcher {
  enum Type {
    EQ  = 0;
    NEQ = 1;
    RE  = 2;
    NRE = 3;
  }
  Type type    = 1;
  string name  = 2;
  string value = 3;
}

message ReadHints {
  int64 step_ms   = 1;
  string func     = 2;
  int64 start_ms  = 3;
  int64 end_ms    = 4;
  repeated string grouping = 5;
  bool by         = 6;
  int64 range_ms  = 7;
}

message Chunk {
  int64 min_time_ms = 1;
  int64 max_time_ms = 2;

  enum Encoding {
    UNKNOWN = 0;
    XOR     = 1;
    HISTOGRAM = 2;
    FLOAT_HISTOGRAM = 3;
  }
  Encoding type = 3;
  bytes data    = 4;
}

message ChunkedSeries {
  repeated Label labels = 1;
  repeated Chunk chunks = 2;
}
```

- [ ] **Step 3: Write `remote.proto`**

Vendor from <https://github.com/prometheus/prometheus/blob/main/prompb/remote.proto>. Write to `core/deltav-kafka-contracts/src/main/proto/prometheus/prompb/remote.proto`:

```protobuf
// Copyright 2017 Prometheus Team
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Vendored into delta-v for use by the prometheus-writer Remote Write client.
// Source: github.com/prometheus/prometheus/prompb/remote.proto (Apache-2.0)

syntax = "proto3";
package prometheus;

option go_package = "github.com/prometheus/prometheus/prompb";
option java_package = "prometheus.prompb";
option java_multiple_files = true;

import "prometheus/prompb/types.proto";

message WriteRequest {
  repeated TimeSeries timeseries = 1;
  reserved 2;  // formerly source
  repeated MetricMetadata metadata = 3;
}

message ReadRequest {
  repeated Query queries = 1;

  enum ResponseType {
    SAMPLES             = 0;
    STREAMED_XOR_CHUNKS = 1;
  }
  repeated ResponseType accepted_response_types = 2;
}

message ReadResponse {
  repeated QueryResult results = 1;
}

message Query {
  int64 start_timestamp_ms = 1;
  int64 end_timestamp_ms   = 2;
  repeated LabelMatcher matchers = 3;
  ReadHints hints          = 4;
}

message QueryResult {
  repeated TimeSeries timeseries = 1;
}

message ChunkedReadResponse {
  repeated ChunkedSeries chunked_series = 1;
  int64 query_index = 2;
}
```

- [ ] **Step 4: Build contracts to verify protoc generates**

```bash
./mvnw -pl core/deltav-kafka-contracts clean install -DskipTests
```

Expected: `BUILD SUCCESS`. Verify generated classes:

```bash
ls core/deltav-kafka-contracts/target/generated-sources/protobuf/java/prometheus/prompb/ | sort
```

Expected includes: `WriteRequest.java`, `TimeSeries.java`, `Sample.java`, `Label.java`, `MetricMetadata.java`, `Exemplar.java`, `Chunk.java`, `Labels.java`, `LabelMatcher.java`, `ReadHints.java`, `ReadRequest.java`, `ReadResponse.java`, `Query.java`, `QueryResult.java`, `ChunkedReadResponse.java`, `ChunkedSeries.java`.

- [ ] **Step 5: Commit**

```bash
git add core/deltav-kafka-contracts/src/main/proto/prometheus
git commit -m "feat(kafka-contracts): vendor Prometheus remote.proto + types.proto

Phase 2 Prometheus Remote Write consumer needs WriteRequest/TimeSeries/
Sample/Label protobuf classes to build RW payloads. Vendored from
github.com/prometheus/prometheus/prompb (Apache-2.0) into the shared
contracts module so the prometheus-writer (and any future consumer
needing RW output) can depend on them without pulling the upstream
Prometheus Go artifacts.

Generated java package: prometheus.prompb"
```

---

## Task 2: Create `core/prometheus-writer/` module skeleton

**Goal:** Maven module exists and builds an empty jar. Nothing runs yet.

**Files:**
- Create: `core/prometheus-writer/pom.xml`
- Create: `core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/PrometheusWriterApplication.java`
- Create: `core/prometheus-writer/src/main/resources/logback-spring.xml`
- Modify: `pom.xml` (root) — add module

- [ ] **Step 1: Write `core/prometheus-writer/pom.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>org.deltav</groupId>
        <artifactId>delta-v-parent</artifactId>
        <version>0.0.1-SNAPSHOT</version>
        <relativePath>../../pom.xml</relativePath>
    </parent>

    <groupId>org.deltav.core</groupId>
    <artifactId>prometheus-writer</artifactId>
    <name>Delta-V :: Core :: Prometheus Remote Write Consumer</name>
    <description>Spring Cloud Stream service that consumes deltav-timeseries, enriches
        each batch against a local materialization of deltav-node-context, transforms
        to Prometheus Remote Write samples, and POSTs Snappy-compressed protobuf
        batches to a configurable RW endpoint.</description>

    <properties>
        <java.version>21</java.version>
        <resilience4j.version>2.2.0</resilience4j.version>
        <snappy.version>1.1.10.5</snappy.version>
    </properties>

    <dependencies>
        <!-- Spring Boot starters -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-actuator</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-validation</artifactId>
        </dependency>

        <!-- Spring Cloud Stream + Kafka binder -->
        <dependency>
            <groupId>org.springframework.cloud</groupId>
            <artifactId>spring-cloud-stream</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.cloud</groupId>
            <artifactId>spring-cloud-stream-binder-kafka</artifactId>
        </dependency>

        <!-- Kafka client (dedicated NodeContextKafkaBootstrap consumer) -->
        <dependency>
            <groupId>org.springframework.kafka</groupId>
            <artifactId>spring-kafka</artifactId>
        </dependency>

        <!-- Protocol Buffers (generated classes from contracts) -->
        <dependency>
            <groupId>org.deltav.core</groupId>
            <artifactId>deltav-kafka-contracts</artifactId>
            <version>${project.version}</version>
        </dependency>
        <dependency>
            <groupId>com.google.protobuf</groupId>
            <artifactId>protobuf-java</artifactId>
        </dependency>

        <!-- Snappy for Remote Write compression -->
        <dependency>
            <groupId>org.xerial.snappy</groupId>
            <artifactId>snappy-java</artifactId>
            <version>${snappy.version}</version>
        </dependency>

        <!-- Resilience4j for circuit breaker -->
        <dependency>
            <groupId>io.github.resilience4j</groupId>
            <artifactId>resilience4j-spring-boot3</artifactId>
            <version>${resilience4j.version}</version>
        </dependency>
        <dependency>
            <groupId>io.github.resilience4j</groupId>
            <artifactId>resilience4j-micrometer</artifactId>
            <version>${resilience4j.version}</version>
        </dependency>

        <!-- Micrometer Prometheus -->
        <dependency>
            <groupId>io.micrometer</groupId>
            <artifactId>micrometer-registry-prometheus</artifactId>
        </dependency>

        <!-- Test dependencies -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.springframework.cloud</groupId>
            <artifactId>spring-cloud-stream-test-binder</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.springframework.kafka</groupId>
            <artifactId>spring-kafka-test</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>kafka</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>junit-jupiter</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.awaitility</groupId>
            <artifactId>awaitility</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>com.squareup.okhttp3</groupId>
            <artifactId>mockwebserver</artifactId>
            <version>4.12.0</version>
            <scope>test</scope>
        </dependency>
        <!-- junit-platform-launcher: required by surefire 3.5.4 for Boot 4 modules
             (feedback_boot4_junit_platform_launcher) -->
        <dependency>
            <groupId>org.junit.platform</groupId>
            <artifactId>junit-platform-launcher</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <finalName>prometheus-writer</finalName>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
                <configuration>
                    <mainClass>org.deltav.prometheus.writer.PrometheusWriterApplication</mainClass>
                    <executable>true</executable>
                </configuration>
                <executions>
                    <execution>
                        <goals>
                            <goal>repackage</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>
```

- [ ] **Step 2: Write the main class**

Write `core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/PrometheusWriterApplication.java`:

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
package org.deltav.prometheus.writer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Entry point for the Delta-V prometheus-writer service.
 *
 * <p>Spring Boot 4.0 + Spring Cloud Stream consumer that reads
 * {@code TimeseriesBatch} protobuf records from the {@code deltav-timeseries}
 * Kafka topic, enriches each batch with node identity from a local
 * materialization of the compacted {@code deltav-node-context} topic,
 * translates to Prometheus Remote Write samples, and POSTs Snappy-compressed
 * protobuf batches to a configurable RW endpoint.
 *
 * <p>Phase 2 of the Kafka Time Series pipeline. See:
 * {@code docs/superpowers/specs/2026-04-17-kafka-ts-phase-2-prometheus-consumer-design.md}
 */
@SpringBootApplication(scanBasePackages = {"org.deltav.prometheus.writer"})
@EnableScheduling
public class PrometheusWriterApplication {

    public static void main(String[] args) {
        SpringApplication.run(PrometheusWriterApplication.class, args);
    }
}
```

> **Scar prophylactic baked in:** `scanBasePackages` is explicit from day one. Phase 0 #170 scar (silent scan-package gap) is structurally impossible when the package root is declared up front.

- [ ] **Step 3: Write `logback-spring.xml`**

Copy `core/flow-enricher/src/main/resources/logback-spring.xml` verbatim to `core/prometheus-writer/src/main/resources/logback-spring.xml`. (Delta-v logging conventions — JSON output with MDC fields for `service` and `trace_id`.)

- [ ] **Step 4: Register module in root pom**

In `/Users/david/development/src/opennms/delta-v/pom.xml`, locate the "Flow processing services (Spring Cloud Stream)" comment block and add `core/prometheus-writer` as a peer of `core/flow-enricher`:

```xml
    <!-- Flow processing services (Spring Cloud Stream) -->
    <module>core/flow-enricher</module>
    <module>core/prometheus-writer</module>
```

- [ ] **Step 5: Verify module builds**

```bash
./mvnw -pl core/prometheus-writer -am clean install -DskipTests
```

Expected: `BUILD SUCCESS`. Produces `core/prometheus-writer/target/prometheus-writer.jar` (executable fat jar).

- [ ] **Step 6: Commit**

```bash
git add core/prometheus-writer pom.xml
git commit -m "feat(prometheus-writer): create core/prometheus-writer Maven module skeleton

Phase 2 standalone Spring Boot 4 service. Follows the core/flow-enricher
precedent: separate module (NOT core/daemon-boot-*), fat jar, own Docker
image, own compose service.

Dependencies: Spring Cloud Stream Kafka binder, Spring Kafka (dedicated
NodeContextKafkaBootstrap consumer), vendored Prometheus protobufs from
deltav-kafka-contracts, Snappy for RW compression, Resilience4j circuit
breaker, Micrometer Prometheus, spring-cloud-stream-test-binder,
Testcontainers Kafka, MockWebServer, awaitility.

scanBasePackages={org.deltav.prometheus.writer} declared up front —
Phase 0 #170 scar prophylactic."
```

---

## Task 3: Configuration properties + `application.yml`

**Goal:** Ship the canonical `application.yml` (spec §8) and the `@ConfigurationProperties` classes that bind it. Fail at startup if mandatory config is absent.

**Files:**
- Create: `core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/config/PrometheusWriterProperties.java`
- Create: `core/prometheus-writer/src/main/resources/application.yml`
- Create: `core/prometheus-writer/src/test/java/org/deltav/prometheus/writer/config/PrometheusWriterPropertiesTest.java`

- [ ] **Step 1: Write failing test**

Write `core/prometheus-writer/src/test/java/org/deltav/prometheus/writer/config/PrometheusWriterPropertiesTest.java`:

```java
/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.ConfigurationPropertySource;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class PrometheusWriterPropertiesTest {

    @Test
    void binds_full_yaml_shape() {
        Map<String, Object> map = Map.of(
                "prometheus-writer.remote-write.url", "http://vm:8428/api/v1/write",
                "prometheus-writer.remote-write.auth.type", "bearer",
                "prometheus-writer.remote-write.auth.bearer-token", "abc",
                "prometheus-writer.remote-write.headers.X-Scope-OrgID", "tenant-1",
                "prometheus-writer.batch.max-samples", "1000",
                "prometheus-writer.batch.max-bytes", "1048576",
                "prometheus-writer.batch.max-interval-ms", "1000",
                "prometheus-writer.retry.initial-backoff-ms", "100",
                "prometheus-writer.retry.max-backoff-ms", "30000",
                "prometheus-writer.circuit-breaker.failure-rate-threshold", "50",
                "prometheus-writer.labels.from-metadata[0]", "requisition:region",
                "prometheus-writer.startup-gate.enabled", "true"
        );
        ConfigurationPropertySource src = new MapConfigurationPropertySource(map);
        PrometheusWriterProperties props = new Binder(src)
                .bind("prometheus-writer", Bindable.of(PrometheusWriterProperties.class))
                .get();

        assertThat(props.remoteWrite().url()).isEqualTo("http://vm:8428/api/v1/write");
        assertThat(props.remoteWrite().auth().type()).isEqualTo(PrometheusWriterProperties.AuthType.BEARER);
        assertThat(props.remoteWrite().auth().bearerToken()).isEqualTo("abc");
        assertThat(props.remoteWrite().headers()).containsEntry("X-Scope-OrgID", "tenant-1");
        assertThat(props.batch().maxSamples()).isEqualTo(1000);
        assertThat(props.batch().maxBytes()).isEqualTo(1_048_576);
        assertThat(props.labels().fromMetadata()).containsExactly("requisition:region");
        assertThat(props.startupGate().enabled()).isTrue();
    }

    @Test
    void defaults_are_sensible_when_minimal_yaml() {
        Map<String, Object> map = Map.of(
                "prometheus-writer.remote-write.url", "http://localhost/write"
        );
        ConfigurationPropertySource src = new MapConfigurationPropertySource(map);
        PrometheusWriterProperties props = new Binder(src)
                .bind("prometheus-writer", Bindable.of(PrometheusWriterProperties.class))
                .get();

        assertThat(props.batch().maxSamples()).isEqualTo(1000);
        assertThat(props.batch().maxBytes()).isEqualTo(1_048_576);
        assertThat(props.batch().maxIntervalMs()).isEqualTo(1000);
        assertThat(props.remoteWrite().auth().type()).isEqualTo(PrometheusWriterProperties.AuthType.NONE);
        assertThat(props.startupGate().enabled()).isTrue();
    }
}
```

- [ ] **Step 2: Run test — expect compile failure** (class does not exist yet)

```bash
./mvnw -pl core/prometheus-writer test -Dtest=PrometheusWriterPropertiesTest
```

Expected: compile failure referencing `PrometheusWriterProperties`.

- [ ] **Step 3: Implement `PrometheusWriterProperties`**

Write `core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/config/PrometheusWriterProperties.java`:

```java
/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.config;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.util.List;
import java.util.Map;

/**
 * Strongly-typed binding for the {@code prometheus-writer.*} section of
 * {@code application.yml}. Every field in spec §8 appears here with a
 * default that matches the yaml default.
 */
@Validated
@ConfigurationProperties(prefix = "prometheus-writer")
public record PrometheusWriterProperties(
        @NotNull RemoteWrite remoteWrite,
        @NotNull Batch batch,
        @NotNull Retry retry,
        @NotNull CircuitBreaker circuitBreaker,
        @NotNull Labels labels,
        @NotNull StartupGate startupGate
) {
    public PrometheusWriterProperties {
        if (remoteWrite == null) remoteWrite = new RemoteWrite(null, new Auth(AuthType.NONE, null, null, null), Map.of());
        if (batch == null)          batch = new Batch(1000, 1_048_576, 1000);
        if (retry == null)          retry = new Retry(100, 30_000, 0.1);
        if (circuitBreaker == null) circuitBreaker = new CircuitBreaker(50, 20, 10, 30_000, 3);
        if (labels == null)         labels = new Labels(List.of());
        if (startupGate == null)    startupGate = new StartupGate(true);
    }

    public record RemoteWrite(
            @NotBlank String url,
            @NotNull Auth auth,
            Map<String, String> headers
    ) {
        public RemoteWrite {
            if (auth == null) auth = new Auth(AuthType.NONE, null, null, null);
            if (headers == null) headers = Map.of();
        }
    }

    public record Auth(
            AuthType type,
            String bearerToken,
            String basicUsername,
            String basicPassword
    ) {}

    public enum AuthType { NONE, BEARER, BASIC }

    public record Batch(
            @Positive int maxSamples,
            @Positive int maxBytes,
            @Positive long maxIntervalMs
    ) {}

    public record Retry(
            @Positive long initialBackoffMs,
            @Positive long maxBackoffMs,
            double jitterFactor
    ) {}

    public record CircuitBreaker(
            int failureRateThreshold,
            int slidingWindowSize,
            int minimumNumberOfCalls,
            long waitDurationOpenMs,
            int halfOpenPermittedCalls
    ) {}

    public record Labels(
            List<String> fromMetadata
    ) {}

    public record StartupGate(
            boolean enabled
    ) {}
}
```

- [ ] **Step 4: Run tests — expect PASS**

```bash
./mvnw -pl core/prometheus-writer test -Dtest=PrometheusWriterPropertiesTest
```

Expected: 2 tests pass.

- [ ] **Step 5: Write `application.yml`**

Write `core/prometheus-writer/src/main/resources/application.yml` verbatim from spec §8. Use the exact yaml from the spec. **Critical scar prophylactic:** every property set by later Testcontainers ITs via `ApplicationContextInitializer` must ALSO appear here. In particular, `spring.kafka.bootstrap-servers` must be in the prod yaml (not test-only) — Phase 0 #170 lesson.

Exact content:

```yaml
# Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later
spring:
  application:
    name: prometheus-writer
  kafka:
    bootstrap-servers: ${SPRING_KAFKA_BOOTSTRAP_SERVERS:localhost:9092}  # prod yaml MUST carry this — Phase 0 #170 lesson
  cloud:
    function:
      definition: timeseriesConsumer
    stream:
      kafka:
        binder:
          brokers: ${SPRING_KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
      bindings:
        timeseriesConsumer-in-0:
          destination: deltav-timeseries
          group: prometheus-writer
          consumer:
            concurrency: 4
        publishDlq-out-0:
          destination: deltav-prometheus-writer-dlq
          producer:
            use-native-encoding: true    # Phase 1.5 SCS splitter-lesson prophylactic

prometheus-writer:
  remote-write:
    url: ${PROMETHEUS_WRITER_REMOTE_WRITE_URL:http://victoriametrics:8428/api/v1/write}
    auth:
      type: ${PROMETHEUS_WRITER_AUTH_TYPE:none}
      bearer-token: ${PROMETHEUS_WRITER_BEARER_TOKEN:}
      basic-username: ${PROMETHEUS_WRITER_BASIC_USER:}
      basic-password: ${PROMETHEUS_WRITER_BASIC_PASS:}
    headers: {}
  batch:
    max-samples: 1000
    max-bytes: 1048576
    max-interval-ms: 1000
  retry:
    initial-backoff-ms: 100
    max-backoff-ms: 30000
    jitter-factor: 0.1
  circuit-breaker:
    failure-rate-threshold: 50
    sliding-window-size: 20
    minimum-number-of-calls: 10
    wait-duration-open-ms: 30000
    half-open-permitted-calls: 3
  labels:
    from-metadata: []
  startup-gate:
    enabled: true

management:
  endpoints:
    web:
      exposure:
        include: health, info, prometheus, bindings
  endpoint:
    health:
      probes:
        enabled: true
      show-details: always
```

- [ ] **Step 6: Commit**

```bash
git add core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/config/PrometheusWriterProperties.java \
        core/prometheus-writer/src/main/resources/application.yml \
        core/prometheus-writer/src/test/java/org/deltav/prometheus/writer/config/PrometheusWriterPropertiesTest.java
git commit -m "feat(prometheus-writer): @ConfigurationProperties binding + application.yml

PrometheusWriterProperties record mirrors spec §8 exactly. Validation
constraints (@NotBlank on RW URL, @Positive on batch sizes) fail-fast
at startup if config is missing.

application.yml carries spring.kafka.bootstrap-servers in the prod
config (not just test yaml) per Phase 0 #170 lesson. DLQ binding uses
producer.use-native-encoding: true per Phase 1.5 SCS splitter lesson.

Tests: 2 binding assertions (full shape + minimal defaults)."
```

---

## Task 4: `NodeContextCache` + unit tests

**Goal:** Thread-safe in-memory map of `{location}@{node_id}` → `NodeContext`. Supports get/put/tombstone/size. No Kafka yet.

**Files:**
- Create: `core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/nodecontext/NodeContextCache.java`
- Create: `core/prometheus-writer/src/test/java/org/deltav/prometheus/writer/nodecontext/NodeContextCacheTest.java`

- [ ] **Step 1: Write failing test**

```java
/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.nodecontext;

import org.deltav.timeseries.proto.NodeContext;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class NodeContextCacheTest {

    @Test
    void get_returns_empty_when_key_absent() {
        NodeContextCache cache = new NodeContextCache();
        assertThat(cache.get("Default@1")).isEmpty();
    }

    @Test
    void put_stores_value_and_get_retrieves() {
        NodeContextCache cache = new NodeContextCache();
        NodeContext nc = NodeContext.newBuilder().setNodeId(1).setLocation("Default").setNodeLabel("lab-1").build();
        cache.put("Default@1", nc);
        assertThat(cache.get("Default@1")).contains(nc);
    }

    @Test
    void remove_evicts_key() {
        NodeContextCache cache = new NodeContextCache();
        NodeContext nc = NodeContext.newBuilder().setNodeId(1).build();
        cache.put("Default@1", nc);
        cache.remove("Default@1");
        assertThat(cache.get("Default@1")).isEmpty();
    }

    @Test
    void tombstone_on_put_delegates_to_remove() {
        NodeContextCache cache = new NodeContextCache();
        cache.put("Default@1", NodeContext.newBuilder().setNodeId(1).build());
        NodeContext tombstone = NodeContext.newBuilder().setNodeId(1).setDeleted(true).build();
        cache.applyUpdate("Default@1", tombstone);
        assertThat(cache.get("Default@1")).isEmpty();
    }

    @Test
    void applyUpdate_live_record_puts() {
        NodeContextCache cache = new NodeContextCache();
        NodeContext nc = NodeContext.newBuilder().setNodeId(1).setDeleted(false).build();
        cache.applyUpdate("Default@1", nc);
        assertThat(cache.get("Default@1")).contains(nc);
    }

    @Test
    void size_reports_current_entries() {
        NodeContextCache cache = new NodeContextCache();
        assertThat(cache.size()).isZero();
        cache.put("Default@1", NodeContext.newBuilder().setNodeId(1).build());
        cache.put("Default@2", NodeContext.newBuilder().setNodeId(2).build());
        assertThat(cache.size()).isEqualTo(2);
        cache.remove("Default@1");
        assertThat(cache.size()).isEqualTo(1);
    }

    @Test
    void ready_defaults_false_and_markReady_flips() {
        NodeContextCache cache = new NodeContextCache();
        assertThat(cache.isReady()).isFalse();
        cache.markReady();
        assertThat(cache.isReady()).isTrue();
    }
}
```

- [ ] **Step 2: Run — expect compile failure**

```bash
./mvnw -pl core/prometheus-writer test -Dtest=NodeContextCacheTest
```

Expected: compile failure referencing `NodeContextCache`.

- [ ] **Step 3: Implement `NodeContextCache`**

```java
/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.nodecontext;

import org.deltav.timeseries.proto.NodeContext;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * In-memory cache of {@link NodeContext} records keyed {@code {location}@{node_id}}.
 * Hot-path {@link #get(String)} is a plain {@link ConcurrentHashMap#get(Object)} —
 * no locking, no network hop. State mutation is confined to the Kafka consumer
 * thread owned by {@link NodeContextKafkaBootstrap}.
 */
@Component
public class NodeContextCache {

    private final ConcurrentHashMap<String, NodeContext> map = new ConcurrentHashMap<>();
    private final AtomicBoolean ready = new AtomicBoolean(false);

    public Optional<NodeContext> get(String key) {
        return Optional.ofNullable(map.get(key));
    }

    public void put(String key, NodeContext value) {
        map.put(key, value);
    }

    public void remove(String key) {
        map.remove(key);
    }

    /**
     * Apply a record from the {@code deltav-node-context} topic. If {@code deleted=true},
     * treats as tombstone and removes the key. Otherwise stores the value.
     */
    public void applyUpdate(String key, NodeContext value) {
        if (value.getDeleted()) {
            remove(key);
        } else {
            put(key, value);
        }
    }

    public int size() {
        return map.size();
    }

    public boolean isReady() {
        return ready.get();
    }

    public void markReady() {
        ready.set(true);
    }
}
```

- [ ] **Step 4: Run — expect PASS**

```bash
./mvnw -pl core/prometheus-writer test -Dtest=NodeContextCacheTest
```

Expected: 7 tests pass.

- [ ] **Step 5: Commit**

```bash
git add core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/nodecontext/NodeContextCache.java \
        core/prometheus-writer/src/test/java/org/deltav/prometheus/writer/nodecontext/NodeContextCacheTest.java
git commit -m "feat(prometheus-writer): NodeContextCache — thread-safe in-memory NodeContext map

Hot-path get() is ConcurrentHashMap.get() — no locking, no network hop.
applyUpdate() dispatches on NodeContext.deleted: tombstone removes,
live record puts. markReady() flips the gate flag consumed by the
startup binding resumer.

Stateless beyond map contents — restart-safe by construction; the
Kafka topic IS the source of truth."
```

---

## Task 5: `NodeContextKafkaBootstrap` + Testcontainers IT

**Goal:** Dedicated Kafka consumer subscribes to `deltav-node-context`, bootstraps the cache from earliest offset up to start-time end-offsets, marks cache ready, continues consuming live updates. Emits `NodeContextCacheReadyEvent` on ready transition.

**Files:**
- Create: `core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/nodecontext/NodeContextKafkaBootstrap.java`
- Create: `core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/nodecontext/NodeContextCacheReadyEvent.java`
- Create: `core/prometheus-writer/src/test/java/org/deltav/prometheus/writer/nodecontext/NodeContextKafkaBootstrapIT.java`

- [ ] **Step 1: Write `NodeContextCacheReadyEvent`**

```java
/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.nodecontext;

import org.springframework.context.ApplicationEvent;

/** Published once when {@link NodeContextKafkaBootstrap} finishes initial bootstrap. */
public class NodeContextCacheReadyEvent extends ApplicationEvent {
    private final long bootstrapDurationMs;
    private final int cacheSize;

    public NodeContextCacheReadyEvent(Object source, long bootstrapDurationMs, int cacheSize) {
        super(source);
        this.bootstrapDurationMs = bootstrapDurationMs;
        this.cacheSize = cacheSize;
    }

    public long getBootstrapDurationMs() { return bootstrapDurationMs; }
    public int getCacheSize() { return cacheSize; }
}
```

- [ ] **Step 2: Implement `NodeContextKafkaBootstrap`**

```java
/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.nodecontext;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.deltav.timeseries.proto.NodeContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Dedicated Kafka consumer for {@code deltav-node-context}. On startup, records the
 * end-offset high-water mark (HWM) per partition, seeks to earliest, consumes
 * records until every partition's offset catches the recorded HWM, and marks
 * the cache ready. Then transitions to a live-tail loop.
 */
@Component
public class NodeContextKafkaBootstrap implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(NodeContextKafkaBootstrap.class);
    private static final String TOPIC = "deltav-node-context";
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(5);

    private final NodeContextCache cache;
    private final ApplicationEventPublisher eventPublisher;
    private final MeterRegistry meterRegistry;
    private final String bootstrapServers;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread thread;
    private Timer.Sample bootstrapSample;

    public NodeContextKafkaBootstrap(
            NodeContextCache cache,
            ApplicationEventPublisher eventPublisher,
            MeterRegistry meterRegistry,
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        this.cache = cache;
        this.eventPublisher = eventPublisher;
        this.meterRegistry = meterRegistry;
        this.bootstrapServers = bootstrapServers;
    }

    @PostConstruct
    public void start() {
        running.set(true);
        bootstrapSample = Timer.start(meterRegistry);
        thread = new Thread(this, "node-context-bootstrap");
        thread.setDaemon(true);
        thread.start();
    }

    @PreDestroy
    public void stop() {
        running.set(false);
        if (thread != null) {
            try { thread.join(5000); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
        }
    }

    @Override
    public void run() {
        try (KafkaConsumer<String, byte[]> consumer = buildConsumer()) {
            List<PartitionInfo> parts = consumer.partitionsFor(TOPIC);
            if (parts == null || parts.isEmpty()) {
                LOG.warn("Topic {} has no partitions yet — marking cache ready with empty state", TOPIC);
                finishBootstrap();
                liveTail(consumer);
                return;
            }
            Set<TopicPartition> allParts = new HashSet<>();
            for (PartitionInfo p : parts) allParts.add(new TopicPartition(TOPIC, p.partition()));
            consumer.assign(allParts);

            Map<TopicPartition, Long> endOffsets = new HashMap<>(consumer.endOffsets(allParts));
            consumer.seekToBeginning(allParts);

            Set<TopicPartition> caughtUp = new HashSet<>();
            // An empty partition (end offset == 0) is instantly "caught up".
            for (Map.Entry<TopicPartition, Long> e : endOffsets.entrySet()) {
                if (e.getValue() == 0L) caughtUp.add(e.getKey());
            }

            while (running.get() && caughtUp.size() < allParts.size()) {
                ConsumerRecords<String, byte[]> records = consumer.poll(POLL_TIMEOUT);
                for (ConsumerRecord<String, byte[]> r : records) {
                    applyRecord(r);
                    TopicPartition tp = new TopicPartition(r.topic(), r.partition());
                    if (r.offset() + 1 >= endOffsets.get(tp)) caughtUp.add(tp);
                }
            }
            finishBootstrap();
            liveTail(consumer);
        } catch (Exception e) {
            LOG.error("NodeContextKafkaBootstrap fatal error", e);
        }
    }

    private void liveTail(KafkaConsumer<String, byte[]> consumer) {
        while (running.get()) {
            try {
                ConsumerRecords<String, byte[]> records = consumer.poll(POLL_TIMEOUT);
                for (ConsumerRecord<String, byte[]> r : records) applyRecord(r);
            } catch (Exception e) {
                LOG.warn("NodeContextKafkaBootstrap live-tail error — will retry after poll timeout", e);
            }
        }
    }

    private void applyRecord(ConsumerRecord<String, byte[]> r) {
        if (r.value() == null) {
            // Kafka log-compaction tombstone (null value). Treat as delete.
            cache.remove(r.key());
            return;
        }
        try {
            NodeContext nc = NodeContext.parseFrom(r.value());
            cache.applyUpdate(r.key(), nc);
        } catch (Exception e) {
            LOG.warn("Malformed NodeContext record key={} offset={} — skipping", r.key(), r.offset(), e);
        }
    }

    private void finishBootstrap() {
        cache.markReady();
        long durationMs = bootstrapSample.stop(
                meterRegistry.timer("deltav.prometheus.writer.node.context.bootstrap.duration"));
        long durationMillis = durationMs / 1_000_000L;
        LOG.info("NodeContextCache bootstrap complete: {} entries in {} ms", cache.size(), durationMillis);
        eventPublisher.publishEvent(new NodeContextCacheReadyEvent(this, durationMillis, cache.size()));
    }

    private KafkaConsumer<String, byte[]> buildConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "prometheus-writer-node-context-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");  // we manage offsets via seek
        return new KafkaConsumer<>(props);
    }
}
```

- [ ] **Step 3: Write Testcontainers IT**

```java
/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.nodecontext;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.deltav.timeseries.proto.NodeContext;
import org.junit.jupiter.api.Test;
import org.springframework.context.event.SimpleApplicationEventMulticaster;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
class NodeContextKafkaBootstrapIT {

    @Container
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.0"));

    @Test
    void bootstrap_drains_to_HWM_then_marks_ready() throws Exception {
        ensureTopic();
        for (int i = 1; i <= 50; i++) produce(i, false);

        NodeContextCache cache = new NodeContextCache();
        AtomicReference<NodeContextCacheReadyEvent> captured = new AtomicReference<>();
        SimpleApplicationEventMulticaster mc = new SimpleApplicationEventMulticaster();
        mc.addApplicationListener(e -> {
            if (e instanceof NodeContextCacheReadyEvent r) captured.set(r);
        });
        NodeContextKafkaBootstrap boot = new NodeContextKafkaBootstrap(
                cache, mc::multicastEvent::accept, new SimpleMeterRegistry(), KAFKA.getBootstrapServers());
        boot.start();
        try {
            await().atMost(Duration.ofSeconds(30)).until(cache::isReady);
            assertThat(cache.size()).isEqualTo(50);
            assertThat(captured.get()).isNotNull();
            assertThat(captured.get().getCacheSize()).isEqualTo(50);
        } finally {
            boot.stop();
        }
    }

    @Test
    void tombstone_removes_key_from_cache_live() throws Exception {
        ensureTopic();
        produce(101, false);

        NodeContextCache cache = new NodeContextCache();
        NodeContextKafkaBootstrap boot = new NodeContextKafkaBootstrap(
                cache, e -> {}, new SimpleMeterRegistry(), KAFKA.getBootstrapServers());
        boot.start();
        try {
            await().atMost(Duration.ofSeconds(30)).until(cache::isReady);
            assertThat(cache.get("Default@101")).isPresent();

            produce(101, true);   // tombstone (deleted=true)
            await().atMost(Duration.ofSeconds(20))
                    .until(() -> cache.get("Default@101").isEmpty());
        } finally {
            boot.stop();
        }
    }

    // -------------------- helpers --------------------

    private void ensureTopic() throws Exception {
        Properties p = new Properties();
        p.put("bootstrap.servers", KAFKA.getBootstrapServers());
        try (AdminClient a = AdminClient.create(p)) {
            a.createTopics(List.of(new NewTopic("deltav-node-context", 8, (short) 1)
                    .configs(Map.of("cleanup.policy", "compact"))))
                    .all().get();
        } catch (Exception e) {
            if (!(e.getCause() instanceof org.apache.kafka.common.errors.TopicExistsException)) throw e;
        }
    }

    private void produce(int nodeId, boolean deleted) throws Exception {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        try (KafkaProducer<String, byte[]> prod = new KafkaProducer<>(p)) {
            NodeContext nc = NodeContext.newBuilder()
                    .setNodeId(nodeId)
                    .setLocation("Default")
                    .setNodeLabel("node-" + nodeId)
                    .setDeleted(deleted)
                    .setUpdatedAtMs(System.currentTimeMillis())
                    .build();
            prod.send(new ProducerRecord<>("deltav-node-context", "Default@" + nodeId, nc.toByteArray())).get();
        }
    }
}
```

- [ ] **Step 4: Run IT**

```bash
./mvnw -pl core/prometheus-writer test -Dtest=NodeContextKafkaBootstrapIT
```

Expected: both tests pass (requires Docker running for Testcontainers).

- [ ] **Step 5: Commit**

```bash
git add core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/nodecontext/NodeContextKafkaBootstrap.java \
        core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/nodecontext/NodeContextCacheReadyEvent.java \
        core/prometheus-writer/src/test/java/org/deltav/prometheus/writer/nodecontext/NodeContextKafkaBootstrapIT.java
git commit -m "feat(prometheus-writer): NodeContextKafkaBootstrap — end-offset HWM bootstrap

Dedicated Kafka consumer subscribes to deltav-node-context, seeks to
earliest, records HWM-at-start per partition, drains to HWM, marks
cache ready, publishes NodeContextCacheReadyEvent, and transitions
to a live-tail loop. Tombstones (deleted=true OR null value) remove
the key.

Bootstrap time measured as Micrometer Timer
'deltav.prometheus.writer.node.context.bootstrap.duration'.

Testcontainers IT asserts: bootstrap drains to HWM + tombstones
propagate live."
```

---

Stopping here for length. **Tasks 6 through 26 continue in a separate file.** The next task file contains:

- Task 6: `NodeContextCacheHealthIndicator`
- Task 7: Startup gate (binding starts paused, resumes on ready event)
- Task 8: `NameSanitizer`
- Task 9: `LabelBuilder`
- Task 10: `PromSample` + `TimeseriesToPromTranslator`
- Task 11: `RemoteWriteHttpClient` + `WriteRequestBuilder`
- Task 12: `RemoteWriteRetryPolicy`
- Task 13: `BatchingRwWriter`
- Task 14: `PrometheusWriterCircuitBreaker`
- Task 15: `TimeseriesConsumer` SCS function + SCS test-binder IT
- Task 16: `ConsumerPauseListener`
- Task 17: `DlqPublisher` + NewTopic bean + binder IT
- Task 18: `PrometheusWriterMetrics` central declaration
- Task 19: `PrometheusWriterConfiguration` wiring + `PrometheusWriterApplicationScanIT` (real-main-class)
- Task 20: Full round-trip Testcontainers IT
- Task 21: `Dockerfile.prometheus-writer`
- Task 22: `docker-compose.yml` (writer service + VictoriaMetrics service)
- Task 23: `build.sh` — `do_prometheus_writer_image()`
- Task 24: `test-prometheus-writer-e2e.sh` (6-step, pipefail-safe grep)
- Task 25: Freeze proto headers + README updates + full-reactor verify
- Task 26: Acceptance-gate checklist (all prophylactics verified)

See companion file: `docs/superpowers/plans/2026-04-17-kafka-ts-phase-2-prometheus-consumer-part2.md`.
