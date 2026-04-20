# prometheus-writer Label Coverage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the `instance` and `foreign_id` labels, populate the default `from-metadata` allowlist, and add a `LabelCardinalityTracker` that exposes two new Micrometer meters at `/actuator/prometheus`.

**Architecture:** Three new beans (`InstanceSource` enum, `InstanceLabelResolver`, `LabelCardinalityTracker`) collaborate with the existing `LabelBuilder`. `PrometheusWriterProperties` gains a new `Metrics` sub-record and the `Labels` sub-record gains an `instanceSource` field. Cardinality tracking uses Caffeine (already in dependencyManagement via spring-boot-dependencies BOM) backing a Micrometer `Gauge`; a `DistributionSummary` records labels-per-sample regardless of the cardinality-tracking enabled flag.

**Tech Stack:** Java 21, Spring Boot 4, Micrometer (already wired), Caffeine 3.2.3 (transitive, needs explicit declaration), JUnit 5 + AssertJ + Mockito (existing test stack), Testcontainers Kafka + okhttp MockWebServer (existing IT stack).

**Branch:** `feat/prometheus-writer-label-coverage` (already created off `develop`, with the spec already committed as `24ee74a6ea9`).

**Spec reference:** `docs/superpowers/specs/2026-04-19-prometheus-writer-label-coverage-design.md`.

**PR target:** `pbrane/delta-v` `develop`. Title prefix `feat(prometheus-writer):`. **NEVER `OpenNMS/opennms`.**

---

## Pre-flight (one-time per session)

- [ ] **Verify branch and clean working tree**

Run: `git branch --show-current && git status --short`
Expected:
```
feat/prometheus-writer-label-coverage
```
(no dirty files)

- [ ] **Verify spec is committed**

Run: `git log --oneline -3`
Expected: most recent commit is `24ee74a6ea9 docs(spec): prometheus-writer label coverage ...`

If either check fails, stop and resolve before starting Task 1.

---

### Task 1: Add Caffeine to prometheus-writer's pom.xml

**Files:**
- Modify: `core/prometheus-writer/pom.xml`

Caffeine 3.2.3 is on the classpath transitively at test scope (via `testcontainers`) but must be a runtime dependency for production code. The version is managed by the Spring Boot BOM — declare without a `<version>`.

- [ ] **Step 1: Add the Caffeine dependency block**

In `core/prometheus-writer/pom.xml`, locate the `<dependencies>` block and add this entry immediately after the `micrometer-registry-prometheus` dependency (lines 89-93 in current file). Insert before the `<!-- Test dependencies -->` comment:

```xml
        <!-- Caffeine for cardinality-tracking cache (LabelCardinalityTracker) -->
        <dependency>
            <groupId>com.github.ben-manes.caffeine</groupId>
            <artifactId>caffeine</artifactId>
        </dependency>
```

- [ ] **Step 2: Verify the dependency resolves at compile scope**

Run from repo root:
```
./mvnw -pl core/prometheus-writer dependency:tree | grep caffeine
```
Expected: a line ending in `caffeine:jar:3.2.3:compile` (NOT `:test`).

- [ ] **Step 3: Commit**

```
git add core/prometheus-writer/pom.xml
git commit -m "build(prometheus-writer): pull Caffeine to compile scope for LabelCardinalityTracker"
```

---

### Task 2: Create the `InstanceSource` enum

**Files:**
- Create: `core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/translate/InstanceSource.java`

The enum is tiny and has no behavior — no test needed for the enum itself. It is exercised through `InstanceLabelResolver` (Task 6) and `PrometheusWriterProperties` binding (Task 3).

- [ ] **Step 1: Create the enum file**

```java
/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.translate;

/**
 * Source policy for the Prometheus-ecosystem {@code instance} label.
 * Bound from {@code prometheus-writer.labels.instance-source} in
 * {@code application.yml} via Spring Boot configuration binding.
 */
public enum InstanceSource {
    /** Use {@code NodeContext.nodeLabel} (default; matches Grafana convention). */
    NODE_LABEL,
    /** Use {@code "{foreign_source}:{foreign_id}"} (stable across renames). */
    FOREIGN_ID,
    /** Use {@code "node:{node_id}"} (pure stable integer key). */
    NODE_ID
}
```

- [ ] **Step 2: Compile to confirm syntax**

Run: `./mvnw -pl core/prometheus-writer compile -q`
Expected: BUILD SUCCESS.

- [ ] **Step 3: Commit**

```
git add core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/translate/InstanceSource.java
git commit -m "feat(prometheus-writer): add InstanceSource enum for the new instance label"
```

---

### Task 3: Modify `PrometheusWriterProperties` and cascade-update all callers

**Files:**
- Modify: `core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/config/PrometheusWriterProperties.java`
- Modify: `core/prometheus-writer/src/test/java/org/deltav/prometheus/writer/config/PrometheusWriterPropertiesTest.java`
- Modify: `core/prometheus-writer/src/test/java/org/deltav/prometheus/writer/translate/TimeseriesToPromTranslatorTest.java` (line 32-33)
- Modify: `core/prometheus-writer/src/test/java/org/deltav/prometheus/writer/startup/TimeseriesBindingStartupGateTest.java` (lines 29-31)
- Modify: `core/prometheus-writer/src/test/java/org/deltav/prometheus/writer/rw/BatchingRwWriterTest.java` (lines 26-30)
- Modify: `core/prometheus-writer/src/test/java/org/deltav/prometheus/writer/rw/PrometheusWriterCircuitBreakerTest.java` (lines 17-22)
- Modify: `core/prometheus-writer/src/test/java/org/deltav/prometheus/writer/rw/RemoteWriteHttpClientTest.java` (lines 30-33)

The `PrometheusWriterProperties` record gains a 7th positional field (`metrics`). The nested `Labels` record gains a 1st positional field (`instanceSource`). Both changes are positional and break every direct constructor call until updated. Do all updates in this single task to keep the build green between tasks.

- [ ] **Step 1: Write the failing test for new properties defaults**

Append two new test methods to `PrometheusWriterPropertiesTest.java` (after the existing `defaults_are_sensible_when_minimal_yaml` method). Add the imports inside the `import` block as well:

```java
// Add to imports:
import org.deltav.prometheus.writer.translate.InstanceSource;

// Add as new test methods at end of class:
    @Test
    void defaults_include_new_instance_source_and_metadata_keys() {
        Map<String, Object> map = Map.of(
                "prometheus-writer.remote-write.url", "http://localhost/write"
        );
        ConfigurationPropertySource src = new MapConfigurationPropertySource(map);
        PrometheusWriterProperties props = new Binder(src)
                .bind("prometheus-writer", Bindable.of(PrometheusWriterProperties.class))
                .get();

        assertThat(props.labels().instanceSource()).isEqualTo(InstanceSource.NODE_LABEL);
        assertThat(props.labels().fromMetadata())
                .containsExactly("snmp:sysContact", "snmp:sysLocation");
    }

    @Test
    void defaults_include_metrics_cardinality_tracking_enabled_with_default_cap() {
        Map<String, Object> map = Map.of(
                "prometheus-writer.remote-write.url", "http://localhost/write"
        );
        ConfigurationPropertySource src = new MapConfigurationPropertySource(map);
        PrometheusWriterProperties props = new Binder(src)
                .bind("prometheus-writer", Bindable.of(PrometheusWriterProperties.class))
                .get();

        assertThat(props.metrics().cardinalityTracking().enabled()).isTrue();
        assertThat(props.metrics().cardinalityTracking().cap()).isEqualTo(100_000);
    }
```

- [ ] **Step 2: Run the new tests — expect compile failure**

Run: `./mvnw -pl core/prometheus-writer test -Dtest=PrometheusWriterPropertiesTest -q`
Expected: COMPILATION ERROR. The methods `instanceSource()` and `metrics()` do not exist on `PrometheusWriterProperties` / `Labels` yet.

- [ ] **Step 3: Modify `PrometheusWriterProperties.java`**

Replace the entire file contents with:

```java
/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.config;

import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import org.deltav.prometheus.writer.translate.InstanceSource;
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
        @NotNull Metrics metrics,
        @NotNull StartupGate startupGate
) {
    public PrometheusWriterProperties {
        if (remoteWrite == null) remoteWrite = new RemoteWrite(null, new Auth(AuthType.NONE, null, null, null), Map.of());
        if (batch == null)          batch = new Batch(1000, 1_048_576, 1000);
        if (retry == null)          retry = new Retry(100, 30_000, 0.1);
        if (circuitBreaker == null) circuitBreaker = new CircuitBreaker(50, 20, 10, 30_000, 3);
        if (labels == null)         labels = new Labels(InstanceSource.NODE_LABEL,
                                                        List.of("snmp:sysContact", "snmp:sysLocation"));
        if (metrics == null)        metrics = new Metrics(new CardinalityTracking(true, 100_000));
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
            @DecimalMin("0.0") @DecimalMax("1.0") double jitterFactor
    ) {}

    public record CircuitBreaker(
            @Min(1) @Max(100) int failureRateThreshold,
            @Positive int slidingWindowSize,
            @Positive int minimumNumberOfCalls,
            @Positive long waitDurationOpenMs,
            @Positive int halfOpenPermittedCalls
    ) {}

    public record Labels(
            @NotNull InstanceSource instanceSource,
            List<String> fromMetadata
    ) {
        public Labels {
            if (instanceSource == null) instanceSource = InstanceSource.NODE_LABEL;
            if (fromMetadata == null) fromMetadata = List.of();
        }
    }

    public record Metrics(@NotNull CardinalityTracking cardinalityTracking) {
        public Metrics {
            if (cardinalityTracking == null) cardinalityTracking = new CardinalityTracking(true, 100_000);
        }
    }

    public record CardinalityTracking(
            boolean enabled,
            @Positive int cap
    ) {}

    public record StartupGate(
            boolean enabled
    ) {}
}
```

- [ ] **Step 4: Update `TimeseriesToPromTranslatorTest.java` (lines 31-35)**

The current code:
```java
        NameSanitizer sanitizer = new NameSanitizer();
        LabelBuilder labelBuilder = new LabelBuilder(sanitizer);
        PrometheusWriterProperties props = new PrometheusWriterProperties(null, null, null, null,
                new PrometheusWriterProperties.Labels(List.of()), null);
        meterRegistry = new SimpleMeterRegistry();
        translator = new TimeseriesToPromTranslator(sanitizer, labelBuilder, props, meterRegistry);
```

Replace `new PrometheusWriterProperties.Labels(List.of())` with `new PrometheusWriterProperties.Labels(InstanceSource.NODE_LABEL, List.of())` and add a 7th `null` argument to `new PrometheusWriterProperties(...)`. The `LabelBuilder` constructor change comes in Task 7 — leave it alone for now (this test will fail at Task 7 boundary; we'll fix it then).

Final lines 31-35 become:
```java
        NameSanitizer sanitizer = new NameSanitizer();
        LabelBuilder labelBuilder = new LabelBuilder(sanitizer);
        PrometheusWriterProperties props = new PrometheusWriterProperties(null, null, null, null,
                new PrometheusWriterProperties.Labels(InstanceSource.NODE_LABEL, List.of()), null, null);
        meterRegistry = new SimpleMeterRegistry();
        translator = new TimeseriesToPromTranslator(sanitizer, labelBuilder, props, meterRegistry);
```

(the `InstanceSource` import is already in `org.deltav.prometheus.writer.translate` — same package as this test, so no new import needed.)

- [ ] **Step 5: Update `TimeseriesBindingStartupGateTest.java` (lines 29-31)**

Current:
```java
        return new PrometheusWriterProperties(
                null, null, null, null, null,
                new PrometheusWriterProperties.StartupGate(enabled));
```

Becomes (add a `null` between the existing 5 nulls and the StartupGate):
```java
        return new PrometheusWriterProperties(
                null, null, null, null, null, null,
                new PrometheusWriterProperties.StartupGate(enabled));
```

- [ ] **Step 6: Update `BatchingRwWriterTest.java` (lines 26-30)**

Current:
```java
        return new PrometheusWriterProperties(
                new PrometheusWriterProperties.RemoteWrite("http://test/write",
                    new PrometheusWriterProperties.Auth(PrometheusWriterProperties.AuthType.NONE, null, null, null), Map.of()),
                new PrometheusWriterProperties.Batch(maxSamples, maxBytes, maxIntervalMs),
                null, null, null, null);
```

Becomes (add a `null` for the new `metrics` field — the trailing-args order is `circuitBreaker, labels, metrics, startupGate`):
```java
        return new PrometheusWriterProperties(
                new PrometheusWriterProperties.RemoteWrite("http://test/write",
                    new PrometheusWriterProperties.Auth(PrometheusWriterProperties.AuthType.NONE, null, null, null), Map.of()),
                new PrometheusWriterProperties.Batch(maxSamples, maxBytes, maxIntervalMs),
                null, null, null, null, null);
```

- [ ] **Step 7: Update `PrometheusWriterCircuitBreakerTest.java` (lines 17-22)**

Current:
```java
        return new PrometheusWriterProperties(
                new PrometheusWriterProperties.RemoteWrite("http://t/w",
                    new PrometheusWriterProperties.Auth(PrometheusWriterProperties.AuthType.NONE, null, null, null), Map.of()),
                null, null,
                new PrometheusWriterProperties.CircuitBreaker(failureRate, window, minCalls, waitOpenMs, halfOpenCalls),
                null, null);
```

Becomes (add a `null` for the new `metrics` field):
```java
        return new PrometheusWriterProperties(
                new PrometheusWriterProperties.RemoteWrite("http://t/w",
                    new PrometheusWriterProperties.Auth(PrometheusWriterProperties.AuthType.NONE, null, null, null), Map.of()),
                null, null,
                new PrometheusWriterProperties.CircuitBreaker(failureRate, window, minCalls, waitOpenMs, halfOpenCalls),
                null, null, null);
```

- [ ] **Step 8: Update `RemoteWriteHttpClientTest.java` (lines 30-33)**

Current:
```java
        PrometheusWriterProperties props = new PrometheusWriterProperties(
                new PrometheusWriterProperties.RemoteWrite(server.url("/api/v1/write").toString(),
                        new PrometheusWriterProperties.Auth(auth, tok, user, pass), extra),
                null, null, null, null, null);
```

Becomes (add a 7th `null`):
```java
        PrometheusWriterProperties props = new PrometheusWriterProperties(
                new PrometheusWriterProperties.RemoteWrite(server.url("/api/v1/write").toString(),
                        new PrometheusWriterProperties.Auth(auth, tok, user, pass), extra),
                null, null, null, null, null, null);
```

- [ ] **Step 9: Run all prometheus-writer unit tests to confirm green**

Run: `./mvnw -pl core/prometheus-writer test -q`
Expected: BUILD SUCCESS. Tests run:
- `PrometheusWriterPropertiesTest` — all 4 methods pass (2 existing + 2 new).
- All other tests that were modified for fixture compilation pass.

If any test fails, check the failure carefully — it likely means a fixture wasn't updated. Do not move forward until all green.

- [ ] **Step 10: Commit**

```
git add core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/config/PrometheusWriterProperties.java \
        core/prometheus-writer/src/test/java/org/deltav/prometheus/writer/config/PrometheusWriterPropertiesTest.java \
        core/prometheus-writer/src/test/java/org/deltav/prometheus/writer/translate/TimeseriesToPromTranslatorTest.java \
        core/prometheus-writer/src/test/java/org/deltav/prometheus/writer/startup/TimeseriesBindingStartupGateTest.java \
        core/prometheus-writer/src/test/java/org/deltav/prometheus/writer/rw/BatchingRwWriterTest.java \
        core/prometheus-writer/src/test/java/org/deltav/prometheus/writer/rw/PrometheusWriterCircuitBreakerTest.java \
        core/prometheus-writer/src/test/java/org/deltav/prometheus/writer/rw/RemoteWriteHttpClientTest.java
git commit -m "feat(prometheus-writer): expand Properties with Metrics record + Labels.instanceSource

Defaults: instance-source=NODE_LABEL, from-metadata=[snmp:sysContact, snmp:sysLocation],
metrics.cardinality-tracking={enabled=true, cap=100000}. All test fixtures that
construct PrometheusWriterProperties or its Labels record updated to match the
new positional shape."
```

---

### Task 4: Add new meter constants and update the meter pinning test

**Files:**
- Modify: `core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/metrics/PrometheusWriterMetrics.java`
- Modify: `core/prometheus-writer/src/test/java/org/deltav/prometheus/writer/metrics/PrometheusWriterMetricsTest.java`

The two new meter names follow the project convention (Micrometer dot-separated names that Prometheus renders as underscores):
- `deltav.prometheus.writer.distinct.series` → `deltav_prometheus_writer_distinct_series`
- `deltav.prometheus.writer.labels.per.sample` → `deltav_prometheus_writer_labels_per_sample`

The test `PrometheusWriterMetricsTest.declared_constants_match_canonical_set` pins the canonical list and will fail until both sides are in sync.

- [ ] **Step 1: Update the test's expected set first (TDD)**

In `PrometheusWriterMetricsTest.java`, add two new entries to the `EXPECTED_METER_NAMES` set. After the existing `"deltav.prometheus.writer.consumer.paused"` entry, add:

```java
            "deltav.prometheus.writer.distinct.series",
            "deltav.prometheus.writer.labels.per.sample"
```

The full set declaration becomes (showing only the changed area):
```java
    private static final Set<String> EXPECTED_METER_NAMES = new TreeSet<>(Set.of(
            "deltav.prometheus.writer.records.consumed",
            // ... existing entries unchanged ...
            "deltav.prometheus.writer.consumer.paused",
            "deltav.prometheus.writer.distinct.series",
            "deltav.prometheus.writer.labels.per.sample"
    ));
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `./mvnw -pl core/prometheus-writer test -Dtest=PrometheusWriterMetricsTest -q`
Expected: FAIL. The expected set has 22 elements; the declared set has 20.

- [ ] **Step 3: Add the new constants to `PrometheusWriterMetrics.java`**

After the existing `// circuit + consumer state` block (the last block in the file), add a new section:

```java
    // cardinality observability
    public static final String DISTINCT_SERIES         = "deltav.prometheus.writer.distinct.series";
    public static final String LABELS_PER_SAMPLE       = "deltav.prometheus.writer.labels.per.sample";
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `./mvnw -pl core/prometheus-writer test -Dtest=PrometheusWriterMetricsTest -q`
Expected: BUILD SUCCESS.

- [ ] **Step 5: Commit**

```
git add core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/metrics/PrometheusWriterMetrics.java \
        core/prometheus-writer/src/test/java/org/deltav/prometheus/writer/metrics/PrometheusWriterMetricsTest.java
git commit -m "feat(prometheus-writer): declare DISTINCT_SERIES + LABELS_PER_SAMPLE meter constants"
```

---

### Task 5: Create `LabelCardinalityTracker` (TDD)

**Files:**
- Test: `core/prometheus-writer/src/test/java/org/deltav/prometheus/writer/metrics/LabelCardinalityTrackerTest.java`
- Create: `core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/metrics/LabelCardinalityTracker.java`

The tracker is a `@Component` that observes label maps emitted by `LabelBuilder`. It exposes one Gauge (gated by `enabled`) and one DistributionSummary (always emitted). The internal `recordToCache` method is package-private to allow test override of the throwing-Caffeine scenario without mocking Caffeine itself.

- [ ] **Step 1: Write the failing test**

Create `LabelCardinalityTrackerTest.java`:

```java
/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.metrics;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.deltav.prometheus.writer.config.PrometheusWriterProperties;
import org.deltav.prometheus.writer.config.PrometheusWriterProperties.CardinalityTracking;
import org.deltav.prometheus.writer.config.PrometheusWriterProperties.Metrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class LabelCardinalityTrackerTest {

    private MeterRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
    }

    private PrometheusWriterProperties props(boolean enabled, int cap) {
        return new PrometheusWriterProperties(null, null, null, null, null,
                new Metrics(new CardinalityTracking(enabled, cap)), null);
    }

    private LabelCardinalityTracker tracker(boolean enabled, int cap) {
        return new LabelCardinalityTracker(props(enabled, cap), registry);
    }

    @Test
    void enabled_recordsDistinctLabelsetsToGauge() {
        LabelCardinalityTracker t = tracker(true, 100);
        t.record(Map.of("a", "1"));
        t.record(Map.of("a", "2"));
        t.record(Map.of("a", "3"));

        Gauge g = registry.find("deltav.prometheus.writer.distinct.series").gauge();
        assertThat(g).isNotNull();
        assertThat(g.value()).isEqualTo(3.0);
    }

    @Test
    void enabled_duplicateLabelsetCountsOnce() {
        LabelCardinalityTracker t = tracker(true, 100);
        t.record(Map.of("a", "1", "b", "x"));
        t.record(Map.of("a", "1", "b", "x"));
        t.record(Map.of("a", "1", "b", "x"));

        Gauge g = registry.find("deltav.prometheus.writer.distinct.series").gauge();
        assertThat(g.value()).isEqualTo(1.0);
    }

    @Test
    void enabled_capExceeded_evictsAndStaysAtCap() {
        LabelCardinalityTracker t = tracker(true, 2);
        t.record(Map.of("a", "1"));
        t.record(Map.of("a", "2"));
        t.record(Map.of("a", "3"));
        t.record(Map.of("a", "4"));
        t.record(Map.of("a", "5"));

        // Caffeine eviction is async/lazy; the cache may briefly exceed the cap
        // before the maintenance thread runs. The `cleanUp()` call forces it.
        Gauge g = registry.find("deltav.prometheus.writer.distinct.series").gauge();
        // Force cleanup via the tracker's package-private accessor.
        t.cleanUpForTesting();
        assertThat(g.value()).isLessThanOrEqualTo(2.0);
    }

    @Test
    void enabled_recordsLabelCountToDistributionSummary() {
        LabelCardinalityTracker t = tracker(true, 100);
        t.record(Map.of("a", "1", "b", "2", "c", "3"));    // 3 labels
        t.record(Map.of("x", "1"));                         // 1 label

        DistributionSummary s = registry.find("deltav.prometheus.writer.labels.per.sample").summary();
        assertThat(s).isNotNull();
        assertThat(s.count()).isEqualTo(2);
        assertThat(s.totalAmount()).isEqualTo(4.0);
    }

    @Test
    void disabled_recordIsNoOpForGauge_butStillRecordsDistributionSummary() {
        LabelCardinalityTracker t = tracker(false, 100);
        t.record(Map.of("a", "1"));
        t.record(Map.of("a", "2"));

        // Gauge should not be registered when disabled.
        Gauge g = registry.find("deltav.prometheus.writer.distinct.series").gauge();
        assertThat(g).isNull();

        // DistributionSummary IS still registered and recorded.
        DistributionSummary s = registry.find("deltav.prometheus.writer.labels.per.sample").summary();
        assertThat(s).isNotNull();
        assertThat(s.count()).isEqualTo(2);
    }

    @Test
    void recordSurvivesInternalThrow() {
        // Override recordToCache to throw — verifies the catch in record() prevents propagation.
        LabelCardinalityTracker throwing = new LabelCardinalityTracker(props(true, 100), registry) {
            @Override
            void recordToCache(String canonical) {
                throw new RuntimeException("boom");
            }
        };
        throwing.record(Map.of("a", "1"));   // must NOT throw
        throwing.record(Map.of("b", "2"));   // must NOT throw

        // DistributionSummary still records — the throw happens AFTER the summary line in record().
        // Actually: per record() implementation the summary record comes after the cache put.
        // So if cache throws, summary is NOT recorded. Confirm zero count here:
        DistributionSummary s = registry.find("deltav.prometheus.writer.labels.per.sample").summary();
        assertThat(s.count()).isEqualTo(0);
    }

    @Test
    void canonicalizationOrderIndependent() {
        LabelCardinalityTracker t = tracker(true, 100);

        Map<String, String> first = new LinkedHashMap<>();
        first.put("a", "1");
        first.put("b", "2");
        first.put("c", "3");

        Map<String, String> sameInDifferentOrder = new LinkedHashMap<>();
        sameInDifferentOrder.put("c", "3");
        sameInDifferentOrder.put("a", "1");
        sameInDifferentOrder.put("b", "2");

        t.record(first);
        t.record(sameInDifferentOrder);

        Gauge g = registry.find("deltav.prometheus.writer.distinct.series").gauge();
        assertThat(g.value()).isEqualTo(1.0);
    }
}
```

- [ ] **Step 2: Run the test — expect compile failure**

Run: `./mvnw -pl core/prometheus-writer test -Dtest=LabelCardinalityTrackerTest -q`
Expected: COMPILATION ERROR. The class `LabelCardinalityTracker` does not exist.

- [ ] **Step 3: Implement `LabelCardinalityTracker.java`**

Create `core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/metrics/LabelCardinalityTracker.java`:

```java
/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.metrics;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.deltav.prometheus.writer.config.PrometheusWriterProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Observes label tuples emitted by {@link org.deltav.prometheus.writer.translate.LabelBuilder}
 * to give operators an early-warning signal on Prometheus series cardinality.
 *
 * <p>Exposes two meters at {@code /actuator/prometheus}:
 * <ul>
 *   <li>{@code deltav_prometheus_writer_distinct_series} — Gauge backed by a
 *       capped Caffeine cache. Reads {@code estimatedSize()} on scrape. Toggled
 *       by {@code prometheus-writer.metrics.cardinality-tracking.enabled}. A
 *       sustained reading at {@code cap} indicates cardinality has exceeded the
 *       budget — operator should prune {@code labels.from-metadata}.</li>
 *   <li>{@code deltav_prometheus_writer_labels_per_sample} — DistributionSummary.
 *       Always emitted; overhead is one atomic increment.</li>
 * </ul>
 *
 * <p>{@link #record(Map)} MUST NEVER throw — observability is best-effort, sample
 * shipping is not. Internal failures are logged at WARN at most once per minute.
 */
@Component
public class LabelCardinalityTracker {

    private static final Logger LOG = LoggerFactory.getLogger(LabelCardinalityTracker.class);
    private static final long WARN_INTERVAL_NANOS = 60L * 1_000_000_000L;
    private static final Object PRESENT = new Object();

    private final boolean enabled;
    private final Cache<String, Object> distinctLabelsets;     // null when disabled
    private final DistributionSummary labelsPerSample;
    private final AtomicLong lastWarnNanos = new AtomicLong(Long.MIN_VALUE);

    public LabelCardinalityTracker(PrometheusWriterProperties props, MeterRegistry registry) {
        var cfg = props.metrics().cardinalityTracking();
        this.enabled = cfg.enabled();
        if (enabled) {
            this.distinctLabelsets = Caffeine.newBuilder()
                    .maximumSize(cfg.cap())
                    .build();
            Gauge.builder(PrometheusWriterMetrics.DISTINCT_SERIES,
                          distinctLabelsets, c -> (double) c.estimatedSize())
                 .description("Distinct label tuples observed since startup, capped at "
                              + cfg.cap() + ". Sustained reading at cap means cardinality "
                              + "exceeds budget — prune labels.from-metadata.")
                 .register(registry);
        } else {
            this.distinctLabelsets = null;
        }
        this.labelsPerSample = DistributionSummary.builder(PrometheusWriterMetrics.LABELS_PER_SAMPLE)
                .description("Number of labels emitted per Prometheus sample.")
                .register(registry);
    }

    /**
     * Observe one label tuple. Never throws.
     *
     * <p>When {@code enabled=false}, only the labels-per-sample summary is recorded;
     * the cache and gauge are bypassed entirely.</p>
     */
    public void record(Map<String, String> labels) {
        if (!enabled) {
            labelsPerSample.record(labels.size());
            return;
        }
        try {
            recordToCache(canonicalize(labels));
            labelsPerSample.record(labels.size());
        } catch (Throwable t) {
            warnRateLimited(t);
        }
    }

    /** Package-private for test override. */
    void recordToCache(String canonical) {
        distinctLabelsets.put(canonical, PRESENT);
    }

    /** Package-private for test forcing of Caffeine maintenance. */
    void cleanUpForTesting() {
        if (distinctLabelsets != null) {
            distinctLabelsets.cleanUp();
        }
    }

    private static String canonicalize(Map<String, String> labels) {
        return labels.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining("\n"));
    }

    private void warnRateLimited(Throwable t) {
        long now = System.nanoTime();
        long last = lastWarnNanos.get();
        if (now - last >= WARN_INTERVAL_NANOS && lastWarnNanos.compareAndSet(last, now)) {
            LOG.warn("LabelCardinalityTracker.record() failed; tracker degraded but writer continues", t);
        }
    }
}
```

- [ ] **Step 4: Run the test — expect pass**

Run: `./mvnw -pl core/prometheus-writer test -Dtest=LabelCardinalityTrackerTest -q`
Expected: BUILD SUCCESS. All 7 test methods pass.

- [ ] **Step 5: Commit**

```
git add core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/metrics/LabelCardinalityTracker.java \
        core/prometheus-writer/src/test/java/org/deltav/prometheus/writer/metrics/LabelCardinalityTrackerTest.java
git commit -m "feat(prometheus-writer): add LabelCardinalityTracker with Caffeine-backed gauge + summary"
```

---

### Task 6: Create `InstanceLabelResolver` (TDD)

**Files:**
- Test: `core/prometheus-writer/src/test/java/org/deltav/prometheus/writer/translate/InstanceLabelResolverTest.java`
- Create: `core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/translate/InstanceLabelResolver.java`

- [ ] **Step 1: Write the failing test**

Create `InstanceLabelResolverTest.java`:

```java
/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.translate;

import org.deltav.prometheus.writer.config.PrometheusWriterProperties;
import org.deltav.prometheus.writer.config.PrometheusWriterProperties.Labels;
import org.deltav.timeseries.proto.NodeContext;
import org.deltav.timeseries.proto.TimeseriesBatch;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class InstanceLabelResolverTest {

    private static PrometheusWriterProperties propsWith(InstanceSource source) {
        return new PrometheusWriterProperties(null, null, null, null,
                new Labels(source, List.of()), null, null);
    }

    private static InstanceLabelResolver resolver(InstanceSource source) {
        return new InstanceLabelResolver(propsWith(source));
    }

    private static TimeseriesBatch batch(int nodeId) {
        return TimeseriesBatch.newBuilder().setNodeId(nodeId).setLocation("Default").build();
    }

    private static NodeContext nc(String nodeLabel, String foreignSource, String foreignId) {
        return NodeContext.newBuilder()
                .setNodeLabel(nodeLabel)
                .setForeignSource(foreignSource)
                .setForeignId(foreignId)
                .build();
    }

    @Test
    void nodeLabelSource_returnsNodeLabel() {
        InstanceLabelResolver r = resolver(InstanceSource.NODE_LABEL);
        assertThat(r.resolve(batch(42), Optional.of(nc("router-1", "fs", "fi"))))
                .isEqualTo("router-1");
    }

    @Test
    void nodeLabelSource_emptyNodeLabel_fallsBackToNodeId() {
        InstanceLabelResolver r = resolver(InstanceSource.NODE_LABEL);
        assertThat(r.resolve(batch(42), Optional.of(nc("", "fs", "fi"))))
                .isEqualTo("node:42");
    }

    @Test
    void nodeLabelSource_absentNodeContext_fallsBackToNodeId() {
        InstanceLabelResolver r = resolver(InstanceSource.NODE_LABEL);
        assertThat(r.resolve(batch(42), Optional.empty()))
                .isEqualTo("node:42");
    }

    @Test
    void foreignIdSource_bothPresent_concatenates() {
        InstanceLabelResolver r = resolver(InstanceSource.FOREIGN_ID);
        assertThat(r.resolve(batch(42), Optional.of(nc("router-1", "provision-prod", "server-01"))))
                .isEqualTo("provision-prod:server-01");
    }

    @Test
    void foreignIdSource_emptyForeignSource_fallsBackToNodeId() {
        InstanceLabelResolver r = resolver(InstanceSource.FOREIGN_ID);
        assertThat(r.resolve(batch(42), Optional.of(nc("router-1", "", "server-01"))))
                .isEqualTo("node:42");
    }

    @Test
    void foreignIdSource_emptyForeignId_fallsBackToNodeId() {
        InstanceLabelResolver r = resolver(InstanceSource.FOREIGN_ID);
        assertThat(r.resolve(batch(42), Optional.of(nc("router-1", "provision-prod", ""))))
                .isEqualTo("node:42");
    }

    @Test
    void foreignIdSource_absentNodeContext_fallsBackToNodeId() {
        InstanceLabelResolver r = resolver(InstanceSource.FOREIGN_ID);
        assertThat(r.resolve(batch(42), Optional.empty()))
                .isEqualTo("node:42");
    }

    @Test
    void nodeIdSource_alwaysReturnsNodeIdForm() {
        InstanceLabelResolver r = resolver(InstanceSource.NODE_ID);
        assertThat(r.resolve(batch(42), Optional.of(nc("router-1", "fs", "fi"))))
                .isEqualTo("node:42");
        assertThat(r.resolve(batch(7), Optional.empty())).isEqualTo("node:7");
    }
}
```

- [ ] **Step 2: Run the test — expect compile failure**

Run: `./mvnw -pl core/prometheus-writer test -Dtest=InstanceLabelResolverTest -q`
Expected: COMPILATION ERROR. `InstanceLabelResolver` does not exist.

- [ ] **Step 3: Implement `InstanceLabelResolver.java`**

Create `core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/translate/InstanceLabelResolver.java`:

```java
/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.translate;

import org.deltav.prometheus.writer.config.PrometheusWriterProperties;
import org.deltav.timeseries.proto.NodeContext;
import org.deltav.timeseries.proto.TimeseriesBatch;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * Resolves the Prometheus-ecosystem {@code instance} label per the configured
 * {@link InstanceSource} policy. Always returns a non-empty string — falls back
 * to {@code "node:{node_id}"} whenever the chosen source produces an empty
 * value, so {@code {instance=""}} never appears on the wire (Grafana templating
 * unhappiness).
 */
@Component
public class InstanceLabelResolver {

    private final InstanceSource source;

    public InstanceLabelResolver(PrometheusWriterProperties props) {
        this.source = props.labels().instanceSource();
    }

    public String resolve(TimeseriesBatch batch, Optional<NodeContext> nc) {
        return switch (source) {
            case NODE_LABEL -> {
                String label = nc.map(NodeContext::getNodeLabel).orElse("");
                yield label.isEmpty() ? fallback(batch) : label;
            }
            case FOREIGN_ID -> {
                String fs = nc.map(NodeContext::getForeignSource).orElse("");
                String fi = nc.map(NodeContext::getForeignId).orElse("");
                yield (fs.isEmpty() || fi.isEmpty()) ? fallback(batch) : fs + ":" + fi;
            }
            case NODE_ID -> fallback(batch);
        };
    }

    private static String fallback(TimeseriesBatch batch) {
        return "node:" + batch.getNodeId();
    }
}
```

- [ ] **Step 4: Run the test — expect pass**

Run: `./mvnw -pl core/prometheus-writer test -Dtest=InstanceLabelResolverTest -q`
Expected: BUILD SUCCESS. All 8 test methods pass.

- [ ] **Step 5: Commit**

```
git add core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/translate/InstanceLabelResolver.java \
        core/prometheus-writer/src/test/java/org/deltav/prometheus/writer/translate/InstanceLabelResolverTest.java
git commit -m "feat(prometheus-writer): add InstanceLabelResolver with NODE_LABEL/FOREIGN_ID/NODE_ID sources"
```

---

### Task 7: Modify `LabelBuilder` to emit `instance` + `foreign_id` and call the tracker; cascade-fix dependent tests

**Files:**
- Modify: `core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/translate/LabelBuilder.java`
- Modify: `core/prometheus-writer/src/test/java/org/deltav/prometheus/writer/translate/LabelBuilderTest.java`
- Modify: `core/prometheus-writer/src/test/java/org/deltav/prometheus/writer/translate/TimeseriesToPromTranslatorTest.java`

`LabelBuilder` gains two constructor params: `InstanceLabelResolver` and `LabelCardinalityTracker`. The label map gains `instance` (position 2) and `foreign_id` (position 6). The tracker is called after the map is built, before returning.

`LabelBuilderTest.setUp` constructs `LabelBuilder` directly and must be updated. `TimeseriesToPromTranslatorTest.setUp` also constructs `LabelBuilder` directly (line 31) and must be updated. No other production callers exist (it's `@Component`-injected via Spring).

- [ ] **Step 1: Update `LabelBuilderTest.java` with new test cases and the new constructor signature**

Replace the entire file contents with:

```java
/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.translate;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.deltav.prometheus.writer.config.PrometheusWriterProperties;
import org.deltav.prometheus.writer.config.PrometheusWriterProperties.CardinalityTracking;
import org.deltav.prometheus.writer.config.PrometheusWriterProperties.Labels;
import org.deltav.prometheus.writer.config.PrometheusWriterProperties.Metrics;
import org.deltav.prometheus.writer.metrics.LabelCardinalityTracker;
import org.deltav.timeseries.proto.NodeContext;
import org.deltav.timeseries.proto.ProducerType;
import org.deltav.timeseries.proto.Resource;
import org.deltav.timeseries.proto.TimeseriesBatch;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class LabelBuilderTest {

    private LabelBuilder labelBuilder;

    @BeforeEach
    void setUp() {
        // Use a real (no-op) LabelCardinalityTracker disabled-mode so test focus stays on LabelBuilder behavior.
        PrometheusWriterProperties props = new PrometheusWriterProperties(null, null, null, null,
                new Labels(InstanceSource.NODE_LABEL, List.of()),
                new Metrics(new CardinalityTracking(false, 100)),
                null);
        InstanceLabelResolver resolver = new InstanceLabelResolver(props);
        LabelCardinalityTracker tracker = new LabelCardinalityTracker(props, new SimpleMeterRegistry());
        labelBuilder = new LabelBuilder(new NameSanitizer(), resolver, tracker);
    }

    private TimeseriesBatch batch(int nodeId, String location, String collectionPackage, ProducerType producer) {
        return TimeseriesBatch.newBuilder()
                .setNodeId(nodeId)
                .setLocation(location)
                .setCollectionPackage(collectionPackage)
                .setProducer(producer)
                .build();
    }

    private Resource resource(String type, String instance) {
        return Resource.newBuilder()
                .setType(type)
                .setInstance(instance)
                .build();
    }

    private NodeContext nc(String nodeLabel, String foreignSource, String foreignId,
                           List<String> categories, Map<String, String> metadata) {
        NodeContext.Builder b = NodeContext.newBuilder()
                .setNodeLabel(nodeLabel)
                .setForeignSource(foreignSource)
                .setForeignId(foreignId);
        b.addAllCategories(categories);
        b.putAllMetadata(metadata);
        return b.build();
    }

    @Test
    void default_labels_always_emitted() {
        TimeseriesBatch b = batch(42, "Default", "snmp-default", ProducerType.PRODUCER_COLLECTD);
        Resource r = resource("node", "");
        NodeContext n = nc("router-1", "fs-1", "fi-1", List.of(), Map.of());

        Map<String, String> labels = labelBuilder.build(b, r, Optional.of(n), List.of());

        assertThat(labels.keySet()).containsExactlyInAnyOrder(
                "node_id", "instance", "location", "node_label", "foreign_source", "foreign_id",
                "categories", "resource_type", "resource_instance", "collection_package", "producer");
        assertThat(labels).hasSize(11);
        assertThat(labels.get("node_id")).isEqualTo("42");
        assertThat(labels.get("instance")).isEqualTo("router-1");
        assertThat(labels.get("location")).isEqualTo("Default");
        assertThat(labels.get("node_label")).isEqualTo("router-1");
        assertThat(labels.get("foreign_source")).isEqualTo("fs-1");
        assertThat(labels.get("foreign_id")).isEqualTo("fi-1");
        assertThat(labels.get("resource_type")).isEqualTo("node");
        assertThat(labels.get("collection_package")).isEqualTo("snmp-default");
        assertThat(labels.get("producer")).isEqualTo("collectd");
    }

    @Test
    void categories_sorted_comma_joined() {
        TimeseriesBatch b = batch(1, "Default", "pkg", ProducerType.PRODUCER_COLLECTD);
        Resource r = resource("node", "");
        NodeContext n = nc("n", "fs", "fi", List.of("production", "critical"), Map.of());

        Map<String, String> labels = labelBuilder.build(b, r, Optional.of(n), List.of());

        assertThat(labels.get("categories")).isEqualTo("critical,production");
    }

    @Test
    void categories_empty_when_none() {
        TimeseriesBatch b = batch(1, "Default", "pkg", ProducerType.PRODUCER_COLLECTD);
        Resource r = resource("node", "");
        NodeContext n = nc("n", "fs", "fi", Collections.emptyList(), Map.of());

        Map<String, String> labels = labelBuilder.build(b, r, Optional.of(n), List.of());

        assertThat(labels.get("categories")).isEqualTo("");
    }

    @Test
    void resource_instance_empty_for_non_tabular() {
        TimeseriesBatch b = batch(1, "Default", "pkg", ProducerType.PRODUCER_COLLECTD);
        Resource r = resource("node", "");
        NodeContext n = nc("n", "fs", "fi", List.of(), Map.of());

        Map<String, String> labels = labelBuilder.build(b, r, Optional.of(n), List.of());

        assertThat(labels.get("resource_instance")).isEqualTo("");
    }

    @Test
    void metadata_allowlist_promotes_listed_keys() {
        TimeseriesBatch b = batch(1, "Default", "pkg", ProducerType.PRODUCER_COLLECTD);
        Resource r = resource("node", "");
        NodeContext n = nc("n", "fs", "fi", List.of(), Map.of("requisition:region", "us-east-1"));

        Map<String, String> labels = labelBuilder.build(b, r, Optional.of(n), List.of("requisition:region"));

        assertThat(labels).containsEntry("requisition_region", "us-east-1");
    }

    @Test
    void metadata_allowlist_emits_empty_for_missing_key() {
        TimeseriesBatch b = batch(1, "Default", "pkg", ProducerType.PRODUCER_COLLECTD);
        Resource r = resource("node", "");
        NodeContext n = nc("n", "fs", "fi", List.of(), Map.of());

        Map<String, String> labels = labelBuilder.build(b, r, Optional.of(n), List.of("requisition:env"));

        assertThat(labels).containsEntry("requisition_env", "");
    }

    @Test
    void metadata_not_in_allowlist_not_emitted() {
        TimeseriesBatch b = batch(1, "Default", "pkg", ProducerType.PRODUCER_COLLECTD);
        Resource r = resource("node", "");
        NodeContext n = nc("n", "fs", "fi", List.of(), Map.of("foo:bar", "baz"));

        Map<String, String> labels = labelBuilder.build(b, r, Optional.of(n), List.of());

        assertThat(labels).doesNotContainKey("foo_bar");
        assertThat(labels).doesNotContainKey("foo:bar");
    }

    @Test
    void producer_enum_name_stringified() {
        TimeseriesBatch b = batch(1, "Default", "pkg", ProducerType.PRODUCER_COLLECTD);
        Resource r = resource("node", "");

        Map<String, String> labels = labelBuilder.build(b, r, Optional.empty(), List.of());

        assertThat(labels.get("producer")).isEqualTo("collectd");
    }

    @Test
    void instance_label_default_uses_node_label() {
        TimeseriesBatch b = batch(99, "Default", "pkg", ProducerType.PRODUCER_COLLECTD);
        Resource r = resource("node", "");
        NodeContext n = nc("router-99.prod.example.com", "fs", "fi", List.of(), Map.of());

        Map<String, String> labels = labelBuilder.build(b, r, Optional.of(n), List.of());

        assertThat(labels.get("instance")).isEqualTo("router-99.prod.example.com");
    }

    @Test
    void instance_label_falls_back_when_node_label_empty() {
        TimeseriesBatch b = batch(99, "Default", "pkg", ProducerType.PRODUCER_COLLECTD);
        Resource r = resource("node", "");
        NodeContext n = nc("", "fs", "fi", List.of(), Map.of());

        Map<String, String> labels = labelBuilder.build(b, r, Optional.of(n), List.of());

        assertThat(labels.get("instance")).isEqualTo("node:99");
    }

    @Test
    void foreign_id_always_emitted() {
        TimeseriesBatch b = batch(7, "Default", "pkg", ProducerType.PRODUCER_COLLECTD);
        Resource r = resource("node", "");
        NodeContext n = nc("router-7", "provision-prod", "server-07", List.of(), Map.of());

        Map<String, String> labels = labelBuilder.build(b, r, Optional.of(n), List.of());

        assertThat(labels).containsEntry("foreign_id", "server-07");
    }

    @Test
    void foreign_id_empty_when_node_context_absent() {
        TimeseriesBatch b = batch(7, "Default", "pkg", ProducerType.PRODUCER_COLLECTD);
        Resource r = resource("node", "");

        Map<String, String> labels = labelBuilder.build(b, r, Optional.empty(), List.of());

        assertThat(labels).containsEntry("foreign_id", "");
    }
}
```

- [ ] **Step 2: Run the LabelBuilderTest — expect compilation failure**

Run: `./mvnw -pl core/prometheus-writer test -Dtest=LabelBuilderTest -q`
Expected: COMPILATION ERROR. The new constructor signature `LabelBuilder(NameSanitizer, InstanceLabelResolver, LabelCardinalityTracker)` does not exist yet.

- [ ] **Step 3: Modify `LabelBuilder.java`**

Replace the entire file contents with:

```java
/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.translate;

import org.deltav.prometheus.writer.metrics.LabelCardinalityTracker;
import org.deltav.timeseries.proto.NodeContext;
import org.deltav.timeseries.proto.ProducerType;
import org.deltav.timeseries.proto.Resource;
import org.deltav.timeseries.proto.TimeseriesBatch;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

@Component
public class LabelBuilder {
    private final NameSanitizer sanitizer;
    private final InstanceLabelResolver instanceResolver;
    private final LabelCardinalityTracker tracker;

    public LabelBuilder(NameSanitizer sanitizer,
                        InstanceLabelResolver instanceResolver,
                        LabelCardinalityTracker tracker) {
        this.sanitizer = sanitizer;
        this.instanceResolver = instanceResolver;
        this.tracker = tracker;
    }

    public Map<String, String> build(TimeseriesBatch batch, Resource resource,
                                     Optional<NodeContext> nc, List<String> metadataAllowlist) {
        Map<String, String> labels = new LinkedHashMap<>();
        labels.put("node_id", Integer.toString(batch.getNodeId()));
        labels.put("instance", instanceResolver.resolve(batch, nc));
        labels.put("location", nullToEmpty(batch.getLocation()));
        labels.put("node_label", nc.map(NodeContext::getNodeLabel).orElse(""));
        labels.put("foreign_source", nc.map(NodeContext::getForeignSource).orElse(""));
        labels.put("foreign_id", nc.map(NodeContext::getForeignId).orElse(""));
        labels.put("categories", nc.map(x -> {
            List<String> sorted = new ArrayList<>(x.getCategoriesList());
            Collections.sort(sorted);
            return String.join(",", sorted);
        }).orElse(""));
        labels.put("resource_type", nullToEmpty(resource.getType()));
        labels.put("resource_instance", nullToEmpty(resource.getInstance()));
        labels.put("collection_package", nullToEmpty(batch.getCollectionPackage()));
        labels.put("producer", producerLabel(batch.getProducer()));

        for (String metaKey : metadataAllowlist) {
            String labelName = sanitizer.sanitize(metaKey);
            String value = nc.map(n -> n.getMetadataMap().getOrDefault(metaKey, "")).orElse("");
            labels.put(labelName, value);
        }

        tracker.record(labels);
        return labels;
    }

    private static String producerLabel(ProducerType p) {
        String name = p.name();
        return name.startsWith("PRODUCER_") ? name.substring("PRODUCER_".length()).toLowerCase(Locale.ROOT) : name.toLowerCase(Locale.ROOT);
    }

    private static String nullToEmpty(String s) { return s == null ? "" : s; }
}
```

- [ ] **Step 4: Update `TimeseriesToPromTranslatorTest.java` setUp (line 31)**

The current setUp constructs `LabelBuilder` with 1 arg:
```java
        LabelBuilder labelBuilder = new LabelBuilder(sanitizer);
```

Replace it with the 3-arg form. Final setUp method becomes:
```java
    @BeforeEach
    void setUp() {
        NameSanitizer sanitizer = new NameSanitizer();
        PrometheusWriterProperties props = new PrometheusWriterProperties(null, null, null, null,
                new PrometheusWriterProperties.Labels(InstanceSource.NODE_LABEL, List.of()),
                new PrometheusWriterProperties.Metrics(
                        new PrometheusWriterProperties.CardinalityTracking(false, 100)),
                null);
        InstanceLabelResolver resolver = new InstanceLabelResolver(props);
        LabelCardinalityTracker tracker = new LabelCardinalityTracker(props, new SimpleMeterRegistry());
        LabelBuilder labelBuilder = new LabelBuilder(sanitizer, resolver, tracker);
        meterRegistry = new SimpleMeterRegistry();
        translator = new TimeseriesToPromTranslator(sanitizer, labelBuilder, props, meterRegistry);
    }
```

Add the new import lines if not already present:
```java
import org.deltav.prometheus.writer.metrics.LabelCardinalityTracker;
```

(`InstanceSource`, `InstanceLabelResolver` are in the same package — no imports needed. `SimpleMeterRegistry` and `PrometheusWriterProperties` should already be imported.)

- [ ] **Step 5: Run the full prometheus-writer test suite to confirm green**

Run: `./mvnw -pl core/prometheus-writer test -q`
Expected: BUILD SUCCESS. Notable tests:
- `LabelBuilderTest` — 12 methods (8 existing + 4 new) all pass.
- `TimeseriesToPromTranslatorTest` — all existing methods still pass with the new resolver/tracker collaborators wired in disabled mode.
- `InstanceLabelResolverTest`, `LabelCardinalityTrackerTest`, `PrometheusWriterPropertiesTest`, `PrometheusWriterMetricsTest` — all pass.

- [ ] **Step 6: Commit**

```
git add core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/translate/LabelBuilder.java \
        core/prometheus-writer/src/test/java/org/deltav/prometheus/writer/translate/LabelBuilderTest.java \
        core/prometheus-writer/src/test/java/org/deltav/prometheus/writer/translate/TimeseriesToPromTranslatorTest.java
git commit -m "feat(prometheus-writer): emit instance + foreign_id labels via LabelBuilder

LabelBuilder gains constructor deps on InstanceLabelResolver + LabelCardinalityTracker;
the label map gets two new entries and the tracker observes every map before return.
TimeseriesToPromTranslatorTest fixture updated to wire a disabled-mode tracker."
```

---

### Task 8: Update `application.yml` with new defaults and documentation comments

**Files:**
- Modify: `core/prometheus-writer/src/main/resources/application.yml`

- [ ] **Step 1: Replace the `labels:` block and add the `metrics:` block**

In the existing `prometheus-writer:` block, replace:

```yaml
  labels:
    from-metadata: []
```

with:

```yaml
  labels:
    # Source policy for the Prometheus-ecosystem `instance` label.
    # NODE_LABEL  — use the human-readable node label (default; matches Grafana
    #               convention but is mutable — series rename when a node is renamed).
    # FOREIGN_ID  — use "{foreign_source}:{foreign_id}" — stable across renames
    #               but less recognizable in dashboards. Falls back to "node:{node_id}"
    #               when foreign_source or foreign_id is empty.
    # NODE_ID     — use "node:{node_id}" — pure stable integer key; ugliest but never
    #               mutates.
    # In all cases, when the chosen source produces an empty string, falls back to
    # "node:{node_id}" so the `instance` label is never empty.
    instance-source: NODE_LABEL

    # Top-level NodeContext metadata keys to promote to Prometheus labels.
    # Each entry adds one label per series. CARDINALITY WARNING: a key whose
    # values are mostly unique (e.g. per-node free-form text) multiplies series
    # cardinality. A key with <=10 distinct values across the deployment is
    # essentially free. Watch the deltav_prometheus_writer_distinct_series gauge
    # to validate. The two defaults below are universally low-cardinality on
    # SNMP-managed devices.
    from-metadata:
      - snmp:sysContact
      - snmp:sysLocation
```

Then, immediately after the `labels:` block (before `startup-gate:`), add:

```yaml
  metrics:
    cardinality-tracking:
      # Toggles the Caffeine-backed distinct-series tracker. When true, observe
      # each emitted label tuple in a capped cache and expose the gauge
      # deltav_prometheus_writer_distinct_series at /actuator/prometheus. When
      # false, the cache and gauge are skipped (extreme-throughput deployments
      # where the per-sample canonicalize+put overhead matters). The companion
      # DistributionSummary deltav_prometheus_writer_labels_per_sample is
      # always emitted regardless of this flag — its overhead is a single
      # atomic increment.
      enabled: true

      # Maximum distinct label tuples held in the cardinality cache. When the
      # cache fills, oldest entries are evicted (Caffeine LRU); the gauge
      # reports cache.estimatedSize(), so a steady reading of `cap` means
      # "cardinality has exceeded the budget — prune your `from-metadata`
      # allowlist." Sized for ~1000 nodes * 50 resources per node * ~2x slack.
      cap: 100000
```

- [ ] **Step 2: Verify yaml binds cleanly by running the application-scan IT**

Run: `./mvnw -pl core/prometheus-writer test -Dtest=PrometheusWriterApplicationScanIT -q`
Expected: BUILD SUCCESS. The application-scan IT loads the full application context, so a yaml-binding mistake (e.g. wrong indent, unknown property) shows up as Spring context creation failure here.

- [ ] **Step 3: Commit**

```
git add core/prometheus-writer/src/main/resources/application.yml
git commit -m "feat(prometheus-writer): wire instance-source + populated from-metadata + cardinality-tracking yaml

Default from-metadata changes from [] to [snmp:sysContact, snmp:sysLocation] —
behavior change at upgrade (additive labels, increased cardinality). PR
description must call this out."
```

---

### Task 9: Extend `RwRoundTripIT` to assert the new labels round-trip end-to-end

**Files:**
- Modify: `core/prometheus-writer/src/test/java/org/deltav/prometheus/writer/rw/RwRoundTripIT.java`

The IT publishes a fabricated NodeContext + TimeseriesBatch through a real Kafka container and asserts the resulting Prometheus Remote Write payload at a `MockWebServer`. Extend the publishing helper to populate `foreignId` and one metadata entry, then assert the new labels appear on the wire.

- [ ] **Step 1: Update `publishNodeContext` to take and populate `foreignId` and metadata**

Find the `publishNodeContext` helper (line 138 in current file). Replace its signature and body with:

```java
    private void publishNodeContext(int nodeId, String location, String label, String foreignId,
                                    List<String> categories, Map<String, String> metadata) throws Exception {
        NodeContext.Builder b = NodeContext.newBuilder()
                .setNodeId(nodeId).setLocation(location).setNodeLabel(label)
                .setForeignSource("provision-prod")
                .setForeignId(foreignId)
                .addAllCategories(categories)
                .setUpdatedAtMs(System.currentTimeMillis());
        b.putAllMetadata(metadata);
        try (KafkaProducer<String, byte[]> prod = newProducer()) {
            prod.send(new ProducerRecord<>("deltav-node-context", location + "@" + nodeId, b.build().toByteArray())).get();
        }
    }
```

- [ ] **Step 2: Update the test method's call site to pass the new args**

Find line 110: `publishNodeContext(5, "Default", "server-01", List.of("production", "critical"));` — replace with:

```java
        publishNodeContext(5, "Default", "server-01", "node-server-01",
                List.of("production", "critical"),
                Map.of("snmp:sysContact", "noc@example.com"));
```

- [ ] **Step 3: Add `prometheus-writer.labels.from-metadata` to the dynamic test props**

In `props(DynamicPropertyRegistry reg)` (around line 71), add a final line that explicitly enables the metadata key the test asserts on. This protects the IT from an accidental change to the application.yml default:

```java
        reg.add("prometheus-writer.labels.from-metadata[0]", () -> "snmp:sysContact");
```

- [ ] **Step 4: Add new assertions in `end_to_end_sample_lands_at_rw_target`**

After the existing `assertThat(labels).containsEntry("categories", "critical,production");` assertion, add:

```java
        assertThat(labels).containsEntry("instance", "server-01");
        assertThat(labels).containsEntry("foreign_source", "provision-prod");
        assertThat(labels).containsEntry("foreign_id", "node-server-01");
        assertThat(labels).containsEntry("snmp_sysContact", "noc@example.com");
```

- [ ] **Step 5: Run the IT to verify end-to-end round-trip**

Run: `./mvnw -pl core/prometheus-writer test -Dtest=RwRoundTripIT -q`
Expected: BUILD SUCCESS. The IT spins up a Kafka container, drives a real consumer, and asserts the new labels arrive at the mock RW endpoint.

If the test fails because the cached test fixture now requires more time for the consumer to pick up the additional metadata key, increase the `Thread.sleep(3000)` line to `Thread.sleep(5000)` and re-run.

- [ ] **Step 6: Commit**

```
git add core/prometheus-writer/src/test/java/org/deltav/prometheus/writer/rw/RwRoundTripIT.java
git commit -m "test(prometheus-writer): assert instance/foreign_id/snmp_sysContact labels round-trip in RwRoundTripIT"
```

---

### Task 10: Module-level verification gate

- [ ] **Step 1: Run the full prometheus-writer verify cycle**

Run: `./mvnw -pl core/prometheus-writer verify`
Expected: BUILD SUCCESS. Runs all unit tests + all ITs in this module (RwRoundTripIT, CircuitBreakerIT, TimeseriesConsumerBinderIT, DlqPublisherBinderIT, NodeContextKafkaBootstrapIT, PrometheusWriterApplicationScanIT).

If any test fails, treat it as a blocker. Read the failure carefully — most likely a test fixture missed the cascade update from Task 3 or Task 7.

(No commit. This is a gate.)

---

### Task 11: Full-reactor verification gate

Per `feedback_delta_v_full_reactor_verify` — never push without confirming the full reactor still builds.

- [ ] **Step 1: Run the full-reactor build (skip tests for speed; the previous task verified them in module)**

Run: `./mvnw -B -DskipTests -fae clean install`
Expected: BUILD SUCCESS at the bottom (modules built: ~70+).

If any other module fails to compile, the most likely cause is a sibling module that imports `PrometheusWriterProperties` — rare but possible. Inspect the failure. If a downstream module needs the same field-positional update, treat that as in scope.

(No commit. This is a gate.)

---

### Task 12: Push the branch and open the PR

- [ ] **Step 1: Push the branch with upstream tracking**

Run: `git push -u origin feat/prometheus-writer-label-coverage`
Expected: branch is created on `pbrane/delta-v` (this is the default `origin` remote per project convention).

- [ ] **Step 2: Open the PR — `--repo pbrane/delta-v` is non-negotiable**

Run:

```
gh pr create --repo pbrane/delta-v --base develop --head feat/prometheus-writer-label-coverage \
  --title "feat(prometheus-writer): add instance + foreign_id labels, populate from-metadata default, cardinality tracker" \
  --body "$(cat <<'EOF'
## Summary

Closes the post-M2 label-coverage gaps surfaced in `project_prometheus_label_coverage_gaps`:

- **`instance` label** (Prometheus-ecosystem convention). Configurable source via `prometheus-writer.labels.instance-source`: `NODE_LABEL` (default — matches `node_label`), `FOREIGN_ID` (stable across renames), or `NODE_ID` (`"node:{node_id}"` form). All sources fall back to `"node:{node_id}"` when empty so `{instance=""}` never appears on the wire.
- **`foreign_id` label** (always-on, sourced from `NodeContext.foreignId`).
- **Default `prometheus-writer.labels.from-metadata`** changes from `[]` to `[snmp:sysContact, snmp:sysLocation]`. **Behavior change at upgrade** — these are additive labels (no existing query breaks) but they do silently increase series cardinality. Operators wanting the previous zero-metadata behavior can set `from-metadata: []` in their override yaml.
- **`LabelCardinalityTracker`** exposes two new Micrometer meters at `/actuator/prometheus`:
  - `deltav_prometheus_writer_distinct_series` — Gauge backed by capped Caffeine cache (default cap 100k; gated by `prometheus-writer.metrics.cardinality-tracking.enabled`). Sustained reading at cap = "your label allowlist is too wide; prune it."
  - `deltav_prometheus_writer_labels_per_sample` — DistributionSummary, always emitted (zero-cost).

Spec: `docs/superpowers/specs/2026-04-19-prometheus-writer-label-coverage-design.md`. Plan: `docs/superpowers/plans/2026-04-19-prometheus-writer-label-coverage-plan.md`.

**Out of scope** (deferred to follow-up PR): interface-scoped enrichment (`ifAlias`, `ifDescr`). Doing this properly requires a `snmp_interface_metadata` map in `NodeContext` keyed by ifIndex (the existing `interface_metadata` is keyed by IP and carries `OnmsIpInterface` operator metadata, structurally the wrong scope for SnmpInterface column data). Tracked in the spec's "After This PR" section.

## Test plan

- [x] Unit tests for every new path (`InstanceLabelResolverTest`, `LabelCardinalityTrackerTest`, expanded `LabelBuilderTest` and `PrometheusWriterPropertiesTest`).
- [x] `RwRoundTripIT` extended to assert `instance`, `foreign_source`, `foreign_id`, and `snmp_sysContact` labels round-trip end-to-end through a real Kafka container into the mock RW endpoint.
- [x] `./mvnw -pl core/prometheus-writer verify` — green.
- [x] `./mvnw -B -DskipTests -fae clean install` — full-reactor green.
- [ ] Post-merge: flip `project_prometheus_label_coverage_gaps` memo OPEN → PARTIALLY RESOLVED, referencing this PR + the queued interface-metadata follow-up.
EOF
)"
```

Expected: GitHub CLI returns the PR URL. Capture it for the post-merge memo update.

- [ ] **Step 3: Print the PR URL for the user**

The `gh pr create` command prints the URL on success. Echo it back so the user can open it.

---

## Self-Review Checklist (run after writing all tasks; not a separate task)

The following list pins what I checked when writing this plan. Re-verify if the plan is changed.

**1. Spec coverage**

| Spec section | Implementing task |
|---|---|
| `instance` label, configurable source, fallback | Task 2 (enum), Task 6 (resolver), Task 7 (LabelBuilder wiring), Task 8 (yaml) |
| `foreign_id` label, always-on | Task 7 (LabelBuilder map line) |
| Default `from-metadata` populated | Task 3 (Properties default), Task 8 (yaml) |
| `LabelCardinalityTracker` with Gauge + DistributionSummary | Task 4 (constants), Task 5 (component) |
| `cardinality-tracking.{enabled,cap}` properties | Task 3 (Properties), Task 8 (yaml) |
| application.yml documentation comments | Task 8 |
| Unit tests for every new path | Tasks 5, 6, 7 |
| RwRoundTripIT extension | Task 9 |
| `./mvnw -pl core/prometheus-writer verify` green | Task 10 |
| Full-reactor build green | Task 11 |
| PR opened `--repo pbrane/delta-v` | Task 12 |

All scope items have an implementing task. The deferred items in Out of Scope (interface metadata, service metadata, NameSanitizer reserved-name protection, Grafana dashboard, performance regression test) are correctly excluded.

**2. Placeholder scan:** no `TBD`, `TODO`, `fill in`, `add appropriate handling`, or "similar to Task N" references. All code blocks contain complete, runnable code.

**3. Type consistency:** Constructor signatures match across tasks:
- `LabelBuilder(NameSanitizer, InstanceLabelResolver, LabelCardinalityTracker)` — Task 7 production + Task 7 test fixture + Task 7 TimeseriesToPromTranslatorTest fixture.
- `LabelCardinalityTracker(PrometheusWriterProperties, MeterRegistry)` — Task 5 production + Task 5 test + Task 7 test fixture + Task 7 TimeseriesToPromTranslatorTest fixture.
- `InstanceLabelResolver(PrometheusWriterProperties)` — Task 6 production + Task 6 test + Task 7 test fixture + Task 7 TimeseriesToPromTranslatorTest fixture.
- `PrometheusWriterProperties(remoteWrite, batch, retry, circuitBreaker, labels, metrics, startupGate)` — 7 positional args. Task 3 production + 6 test-fixture call sites all updated.
- `Labels(InstanceSource, List<String>)` — 2 positional args. Task 3 production + 2 test-fixture call sites updated.
- `Metrics(CardinalityTracking)` — 1 positional arg. Task 3 production + 2 test-fixture call sites use it.
- Meter constants `DISTINCT_SERIES` / `LABELS_PER_SAMPLE` — Task 4 declares; Task 5 references.

All names match across tasks.
