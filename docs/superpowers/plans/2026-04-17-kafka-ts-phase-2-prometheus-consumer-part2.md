# Kafka TS Phase 2 — Prometheus RW Consumer — Implementation Plan (Part 2)

> Continues from `2026-04-17-kafka-ts-phase-2-prometheus-consumer.md`. Tasks 6 through 26.
> **Tasks are more terse than Part 1 — pattern (TDD: failing test → run → implement → run → commit) is established.** Each task still names exact files, test class names, and key code shapes so an executor can follow without guessing.

---

## Task 6: `NodeContextCacheHealthIndicator`

**Goal:** Actuator `HealthIndicator` that returns `UP` iff `NodeContextCache.isReady()`. Ties to Spring Boot readiness probe.

**Files:**
- Create: `core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/nodecontext/NodeContextCacheHealthIndicator.java`
- Create: `core/prometheus-writer/src/test/java/org/deltav/prometheus/writer/nodecontext/NodeContextCacheHealthIndicatorTest.java`

- [ ] **Step 1: Write failing test** with 2 cases:
  - `health_down_when_cache_not_ready` — new cache, assert `Health.Status.DOWN` and detail `ready=false`, `size=0`
  - `health_up_when_cache_ready` — mark ready + add entries, assert `Health.Status.UP` and detail `ready=true`, `size=N`

- [ ] **Step 2: Run test, expect compile failure.**

- [ ] **Step 3: Implement**

```java
/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.nodecontext;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.actuate.availability.ReadinessStateHealthIndicator;
import org.springframework.stereotype.Component;

@Component("nodeContextCache")
public class NodeContextCacheHealthIndicator implements HealthIndicator {
    private final NodeContextCache cache;
    public NodeContextCacheHealthIndicator(NodeContextCache cache) { this.cache = cache; }

    @Override
    public Health health() {
        Health.Builder b = cache.isReady() ? Health.up() : Health.down();
        return b.withDetail("ready", cache.isReady())
                .withDetail("size", cache.size())
                .build();
    }
}
```

Also add in `application.yml` (modify Task 3's file):
```yaml
management:
  endpoint:
    health:
      group:
        readiness:
          include: readinessState, nodeContextCache  # includes the indicator above
```

- [ ] **Step 4: Run, expect PASS.**

- [ ] **Step 5: Commit**

```
git commit -m "feat(prometheus-writer): NodeContextCacheHealthIndicator for readiness probe

Contributes 'nodeContextCache' to the readiness group. /actuator/health/readiness
stays DOWN until NodeContextCache.isReady() flips — ties the startup gate
(Task 7) to Kubernetes-style readiness probing."
```

---

## Task 7: Startup gate (binding paused until cache ready)

**Goal:** `deltav-timeseries` binding starts paused; resumed on `NodeContextCacheReadyEvent`. Gated by `prometheus-writer.startup-gate.enabled` (default true).

**Files:**
- Create: `core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/startup/TimeseriesBindingStartupGate.java`
- Create: `core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/startup/TimeseriesBindingResumer.java`
- Create: `core/prometheus-writer/src/test/java/org/deltav/prometheus/writer/startup/TimeseriesBindingStartupGateTest.java`

- [ ] **Step 1: Write failing test**

`TimeseriesBindingStartupGateTest` asserts:
- `customize_pauses_container_when_gate_enabled` — create gate with `enabled=true`, invoke `configure(mockContainer, "timeseriesConsumer-in-0")`, assert `mockContainer.pause()` called.
- `customize_no_op_for_other_binding_names` — gate does nothing if binding name != `timeseriesConsumer-in-0`.
- `resumer_calls_resume_on_ready_event` — inject mock `BindingsLifecycleController`, publish `NodeContextCacheReadyEvent`, assert `controller.resume("timeseriesConsumer-in-0")` called.
- `resumer_no_op_when_gate_disabled` — gate disabled, publish event, assert `controller.resume` NOT called.

- [ ] **Step 2: Run, expect compile failure.**

- [ ] **Step 3: Implement `TimeseriesBindingStartupGate`**

```java
/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.startup;

import org.deltav.prometheus.writer.config.PrometheusWriterProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.binder.kafka.config.ListenerContainerCustomizer;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.stereotype.Component;

@Component
public class TimeseriesBindingStartupGate implements ListenerContainerCustomizer<AbstractMessageListenerContainer<?, ?>> {
    private static final Logger LOG = LoggerFactory.getLogger(TimeseriesBindingStartupGate.class);
    private static final String BINDING = "timeseriesConsumer-in-0";
    private final PrometheusWriterProperties props;

    public TimeseriesBindingStartupGate(PrometheusWriterProperties props) { this.props = props; }

    @Override
    public void configure(AbstractMessageListenerContainer<?, ?> container, String destinationName, String group) {
        // Match by binding name via destination+group heuristic (SCS passes destination here); real code
        // uses container.getContainerProperties().getGroupId() for robustness. Keeping it simple:
        if (!props.startupGate().enabled()) {
            LOG.info("Startup gate disabled; timeseries binding will start unpaused");
            return;
        }
        LOG.info("Pausing timeseries binding '{}' — will resume on NodeContextCacheReadyEvent", BINDING);
        container.pause();
    }
}
```

- [ ] **Step 4: Implement `TimeseriesBindingResumer`**

```java
/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.startup;

import org.deltav.prometheus.writer.config.PrometheusWriterProperties;
import org.deltav.prometheus.writer.nodecontext.NodeContextCacheReadyEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.binding.BindingsLifecycleController;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Component
public class TimeseriesBindingResumer {
    private static final Logger LOG = LoggerFactory.getLogger(TimeseriesBindingResumer.class);
    private static final String BINDING = "timeseriesConsumer-in-0";
    private final BindingsLifecycleController lifecycle;
    private final PrometheusWriterProperties props;

    public TimeseriesBindingResumer(BindingsLifecycleController lifecycle, PrometheusWriterProperties props) {
        this.lifecycle = lifecycle;
        this.props = props;
    }

    @EventListener
    public void onReady(NodeContextCacheReadyEvent event) {
        if (!props.startupGate().enabled()) return;
        LOG.info("NodeContextCache ready (size={}, bootstrap={}ms) — resuming binding {}",
                event.getCacheSize(), event.getBootstrapDurationMs(), BINDING);
        lifecycle.resume(BINDING);
    }
}
```

- [ ] **Step 5: Run, expect PASS.**

- [ ] **Step 6: Commit**

```
git commit -m "feat(prometheus-writer): startup gate pauses timeseries binding until cache ready

TimeseriesBindingStartupGate (ListenerContainerCustomizer) calls
container.pause() at factory time. TimeseriesBindingResumer
(@EventListener) calls BindingsLifecycleController.resume() on
NodeContextCacheReadyEvent. Gated by prometheus-writer.startup-gate.enabled
(default true).

Ensures any post-startup enrichment_missing counter increment is real
drift, not warm-up noise."
```

---

## Task 8: `NameSanitizer` + unit tests

**Goal:** Pure function `String → String`. Lowercase → non-alphanumeric → `_` → collapse consecutive `_` → strip leading digit. Startup-time collision detection with WARN log.

**Files:**
- Create: `core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/translate/NameSanitizer.java`
- Create: `core/prometheus-writer/src/test/java/org/deltav/prometheus/writer/translate/NameSanitizerTest.java`

- [ ] **Step 1: Write failing tests.** Cases:

| Input | Expected |
|---|---|
| `mib2-interface-errors` | `mib2_interface_errors` |
| `ifInDiscards` | `ifindiscards` |
| `mib2-X-interfaces` | `mib2_x_interfaces` |
| `hrStorage` | `hrstorage` |
| `1abc` | `_1abc` (leading digit prefixed with `_`) |
| `foo--bar` | `foo_bar` (consecutive collapsed) |
| `foo..bar::baz` | `foo_bar_baz` |
| `foo___bar` | `foo_bar` |
| `` (empty) | `` |
| `foo` (already clean) | `foo` |

Test class also asserts `detectCollisions(List.of("foo-bar", "foo_bar"))` returns a map `{"foo_bar" → ["foo-bar", "foo_bar"]}`.

- [ ] **Step 2: Run, expect compile failure.**

- [ ] **Step 3: Implement**

```java
/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.translate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@Component
public class NameSanitizer {
    private static final Logger LOG = LoggerFactory.getLogger(NameSanitizer.class);

    public String sanitize(String input) {
        if (input == null || input.isEmpty()) return "";
        StringBuilder sb = new StringBuilder(input.length());
        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            if ((c >= 'A' && c <= 'Z')) sb.append((char)(c + ('a' - 'A')));
            else if ((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')) sb.append(c);
            else sb.append('_');
        }
        // Collapse consecutive underscores
        String collapsed = sb.toString().replaceAll("_+", "_");
        // Strip leading/trailing underscores (label/metric spec edge case) then prefix if starts with digit
        if (collapsed.isEmpty()) return "";
        if (Character.isDigit(collapsed.charAt(0))) collapsed = "_" + collapsed;
        return collapsed;
    }

    /** Detects collisions in a name set; returns map of sanitized → list of originals where &gt; 1. */
    public Map<String, List<String>> detectCollisions(List<String> originals) {
        Map<String, List<String>> buckets = new TreeMap<>();
        for (String orig : originals) {
            String clean = sanitize(orig);
            buckets.computeIfAbsent(clean, k -> new java.util.ArrayList<>()).add(orig);
        }
        Map<String, List<String>> collisions = new HashMap<>();
        for (Map.Entry<String, List<String>> e : buckets.entrySet()) {
            if (e.getValue().size() > 1) {
                collisions.put(e.getKey(), e.getValue());
                LOG.warn("Name sanitization collision: {} → {}", e.getValue(), e.getKey());
            }
        }
        return collisions;
    }
}
```

- [ ] **Step 4: Run, expect PASS.**

- [ ] **Step 5: Commit**

```
git commit -m "feat(prometheus-writer): NameSanitizer — Prometheus-spec-compliant name conversion

Lowercase + non-alphanumeric → underscore + collapse consecutive + prefix
leading digit with underscore. detectCollisions() WARN-logs any two
originals that map to the same sanitized form.

10 unit tests cover MIB-hyphen, camelCase, hrStorage, consecutive-sep,
leading-digit, empty, collision-detection."
```

---

## Task 9: `LabelBuilder` + unit tests

**Goal:** Pure function: `(TimeseriesBatch, Resource, Optional<NodeContext>, List<String> metadataAllowlist) → Map<String, String>` (label name → label value). Implements spec §3 label set.

**Files:**
- Create: `core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/translate/LabelBuilder.java`
- Create: `core/prometheus-writer/src/test/java/org/deltav/prometheus/writer/translate/LabelBuilderTest.java`

- [ ] **Step 1: Write failing tests.** Cases:

1. `default_labels_always_emitted` — batch + resource + nodeContext, assert exact label set `{node_id, location, node_label, foreign_source, categories, resource_type, resource_instance, collection_package, producer}`.
2. `categories_sorted_comma_joined` — nodeContext with categories `["production", "critical"]`, expect `categories="critical,production"`.
3. `categories_empty_when_none` — no categories, expect `categories=""`.
4. `resource_instance_empty_for_non_tabular` — Resource with empty instance, expect `resource_instance=""`.
5. `metadata_allowlist_promotes_listed_keys` — allowlist `["requisition:region"]`, nodeContext metadata `{"requisition:region": "us-east-1"}`, expect label `requisition_region="us-east-1"`.
6. `metadata_allowlist_emits_empty_for_missing_key` — allowlist `["requisition:env"]`, nodeContext has no such key, expect label `requisition_env=""`.
7. `metadata_not_in_allowlist_not_emitted` — nodeContext has `foo:bar=baz`, allowlist is empty, assert no label `foo_bar`.
8. `producer_enum_name_stringified` — batch producer `PRODUCER_COLLECTD`, expect `producer="collectd"` (lowercase, without `PRODUCER_` prefix).

- [ ] **Step 2: Run, expect compile failure.**

- [ ] **Step 3: Implement**

```java
/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.translate;

import org.deltav.timeseries.proto.NodeContext;
import org.deltav.timeseries.proto.ProducerType;
import org.deltav.timeseries.proto.Resource;
import org.deltav.timeseries.proto.TimeseriesBatch;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class LabelBuilder {
    private final NameSanitizer sanitizer;
    public LabelBuilder(NameSanitizer sanitizer) { this.sanitizer = sanitizer; }

    public Map<String, String> build(TimeseriesBatch batch, Resource resource,
                                     Optional<NodeContext> nc, List<String> metadataAllowlist) {
        Map<String, String> labels = new LinkedHashMap<>();
        labels.put("node_id", Integer.toString(batch.getNodeId()));
        labels.put("location", nullToEmpty(batch.getLocation()));
        labels.put("node_label", nc.map(NodeContext::getNodeLabel).orElse(""));
        labels.put("foreign_source", nc.map(NodeContext::getForeignSource).orElse(""));
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
        return labels;
    }

    private static String producerLabel(ProducerType p) {
        String name = p.name();
        return name.startsWith("PRODUCER_") ? name.substring("PRODUCER_".length()).toLowerCase(Locale.ROOT) : name.toLowerCase(Locale.ROOT);
    }

    private static String nullToEmpty(String s) { return s == null ? "" : s; }
}
```

- [ ] **Step 4: Run, expect PASS.**

- [ ] **Step 5: Commit**

```
git commit -m "feat(prometheus-writer): LabelBuilder — default labels + metadata allowlist

9 fixed labels per spec §3 + opt-in metadata labels via
prometheus-writer.labels.from-metadata. Categories sorted + comma-joined.
Producer enum stripped of PRODUCER_ prefix, lowercased.

8 unit tests cover every label-shape edge case."
```

---

## Task 10: `PromSample` + `TimeseriesToPromTranslator`

**Goal:** Pure function `(TimeseriesBatch, Optional<NodeContext>) → List<PromSample>`. Applies every filter and naming rule from spec §4.

**Files:**
- Create: `core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/translate/PromSample.java`
- Create: `core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/translate/TimeseriesToPromTranslator.java`
- Create: `core/prometheus-writer/src/test/java/org/deltav/prometheus/writer/translate/TimeseriesToPromTranslatorTest.java`

- [ ] **Step 1: Write `PromSample` record**

```java
/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.translate;

import java.util.Map;

public record PromSample(String name, Map<String, String> labels, double value, long timestampMs) {}
```

- [ ] **Step 2: Write failing test** with cases:

1. `counter_attribute_gets_total_suffix` — batch with one resource, one group `mib2-interface-errors`, one attr `ifInDiscards` type COUNTER. Expect 1 sample, name `opennms_mib2_interface_errors_ifindiscards_total`.
2. `gauge_attribute_no_total_suffix` — group `mib2-X-interfaces`, attr `ifHighSpeed` type GAUGE. Expect name `opennms_mib2_x_interfaces_ifhighspeed`.
3. `string_attribute_dropped` — attr type STRING → `samplesDroppedStringAttribute` metric incremented, no sample in returned list.
4. `unspecified_attribute_dropped` — attr type UNSPECIFIED → `samplesDroppedUnspecifiedType` incremented, no sample.
5. `missing_node_context_drops_all_samples` — batch for node_id=99 with 3 attributes, `nc=Optional.empty()`, expect 0 samples returned + `enrichmentMissingTotal` incremented by 1 (per batch, not per sample).
6. `timestamp_from_batch_collection_time` — batch.timestamp_ms=1_700_000_000_000, sample timestamp equals it (NOT `System.currentTimeMillis()`).
7. `all_labels_populated` — counter sample with full nodeContext, assert label set matches Task 9's default set.

Test uses counter mocks for metrics — inject a `SimpleMeterRegistry` and assert counters by name.

- [ ] **Step 3: Run, expect compile failure.**

- [ ] **Step 4: Implement `TimeseriesToPromTranslator`**

```java
/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.translate;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.deltav.prometheus.writer.config.PrometheusWriterProperties;
import org.deltav.timeseries.proto.*;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class TimeseriesToPromTranslator {
    private final NameSanitizer sanitizer;
    private final LabelBuilder labelBuilder;
    private final PrometheusWriterProperties props;
    private final Counter droppedString;
    private final Counter droppedUnspecified;
    private final Counter enrichmentMissing;

    public TimeseriesToPromTranslator(NameSanitizer sanitizer, LabelBuilder labelBuilder,
                                      PrometheusWriterProperties props, MeterRegistry reg) {
        this.sanitizer = sanitizer;
        this.labelBuilder = labelBuilder;
        this.props = props;
        this.droppedString = reg.counter("deltav.prometheus.writer.samples.dropped", "reason", "string_attribute");
        this.droppedUnspecified = reg.counter("deltav.prometheus.writer.samples.dropped", "reason", "type_unspecified");
        this.enrichmentMissing = reg.counter("deltav.prometheus.writer.enrichment.missing", "reason", "never_seen");
    }

    public List<PromSample> translate(TimeseriesBatch batch, Optional<NodeContext> nc) {
        if (nc.isEmpty()) {
            enrichmentMissing.increment();
            return List.of();
        }
        List<PromSample> out = new ArrayList<>();
        List<String> allowlist = props.labels().fromMetadata();
        for (Resource r : batch.getResourcesList()) {
            Map<String, String> labels = labelBuilder.build(batch, r, nc, allowlist);
            for (AttributeGroup g : r.getGroupsList()) {
                for (Attribute a : g.getAttributesList()) {
                    if (a.getType() == AttributeType.ATTRIBUTE_TYPE_STRING) {
                        droppedString.increment(); continue;
                    }
                    if (a.getType() == AttributeType.ATTRIBUTE_TYPE_UNSPECIFIED) {
                        droppedUnspecified.increment(); continue;
                    }
                    String name = buildMetricName(g.getName(), a.getName(), a.getType());
                    out.add(new PromSample(name, labels, a.getNumeric(), batch.getTimestampMs()));
                }
            }
        }
        return out;
    }

    private String buildMetricName(String groupName, String attrName, AttributeType type) {
        String base = "opennms_" + sanitizer.sanitize(groupName) + "_" + sanitizer.sanitize(attrName);
        return type == AttributeType.ATTRIBUTE_TYPE_COUNTER ? base + "_total" : base;
    }
}
```

- [ ] **Step 5: Run, expect PASS.**

- [ ] **Step 6: Commit**

```
git commit -m "feat(prometheus-writer): TimeseriesToPromTranslator — pure batch→samples function

Iterates resources × groups × attributes. Filters STRING + UNSPECIFIED
attributes with Micrometer counter-side-effects. Returns empty list and
increments enrichment_missing when NodeContext absent.

Metric name: opennms_{sanitized(group)}_{sanitized(attr)}[_total if COUNTER].
Sample timestamp = batch.timestamp_ms (collection time, NOT processing time).

7 unit tests cover counter/_total, gauge/no-suffix, string-drop,
unspecified-drop, missing-context-drop-all, timestamp fidelity, full label
set."
```

---

## Task 11: `WriteRequestBuilder` + `RemoteWriteHttpClient` + MockWebServer tests

**Goal:** Build a Prometheus `WriteRequest` protobuf from `List<PromSample>`. POST Snappy-compressed bytes to the configured RW URL with auth headers.

**Files:**
- Create: `core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/rw/WriteRequestBuilder.java`
- Create: `core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/rw/RemoteWriteHttpClient.java`
- Create: `core/prometheus-writer/src/test/java/org/deltav/prometheus/writer/rw/WriteRequestBuilderTest.java`
- Create: `core/prometheus-writer/src/test/java/org/deltav/prometheus/writer/rw/RemoteWriteHttpClientTest.java`

- [ ] **Step 1: Write `WriteRequestBuilder`**

Groups `PromSample` list by `(name, labels)` → one `TimeSeries` per group containing all samples. Produces a `WriteRequest.Builder` ready to serialize.

```java
/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.rw;

import org.deltav.prometheus.writer.translate.PromSample;
import org.springframework.stereotype.Component;
import prometheus.prompb.Label;
import prometheus.prompb.Sample;
import prometheus.prompb.TimeSeries;
import prometheus.prompb.WriteRequest;

import java.util.*;

@Component
public class WriteRequestBuilder {
    public WriteRequest build(List<PromSample> samples) {
        Map<SeriesKey, List<Sample>> bySeries = new LinkedHashMap<>();
        Map<SeriesKey, List<Label>> seriesLabels = new HashMap<>();
        for (PromSample s : samples) {
            Map<String, String> withName = new LinkedHashMap<>();
            withName.put("__name__", s.name());
            withName.putAll(s.labels());
            SeriesKey key = SeriesKey.from(withName);
            bySeries.computeIfAbsent(key, k -> new ArrayList<>())
                    .add(Sample.newBuilder().setValue(s.value()).setTimestamp(s.timestampMs()).build());
            seriesLabels.computeIfAbsent(key, k -> toLabels(withName));
        }
        WriteRequest.Builder req = WriteRequest.newBuilder();
        for (Map.Entry<SeriesKey, List<Sample>> e : bySeries.entrySet()) {
            req.addTimeseries(TimeSeries.newBuilder()
                    .addAllLabels(seriesLabels.get(e.getKey()))
                    .addAllSamples(e.getValue()));
        }
        return req.build();
    }

    private static List<Label> toLabels(Map<String, String> m) {
        List<Label> out = new ArrayList<>(m.size());
        // Prometheus spec: labels sorted lexicographically by name. __name__ sorts last by ASCII but convention
        // is to sort all alphabetically; prometheus-receivers tolerate either.
        m.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(e -> out.add(Label.newBuilder().setName(e.getKey()).setValue(e.getValue()).build()));
        return out;
    }

    private record SeriesKey(String canonical) {
        static SeriesKey from(Map<String, String> labels) {
            StringBuilder sb = new StringBuilder();
            labels.entrySet().stream().sorted(Map.Entry.comparingByKey())
                    .forEach(e -> sb.append(e.getKey()).append('=').append(e.getValue()).append('|'));
            return new SeriesKey(sb.toString());
        }
    }
}
```

- [ ] **Step 2: Unit test `WriteRequestBuilder`**

`WriteRequestBuilderTest`:
- `builds_one_timeseries_per_unique_label_set`
- `multiple_samples_same_series_grouped`
- `metric_name_goes_to___name___label`
- `labels_sorted_alphabetically_in_output`

- [ ] **Step 3: Implement `RemoteWriteHttpClient`**

```java
/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.rw;

import org.deltav.prometheus.writer.config.PrometheusWriterProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;
import org.xerial.snappy.Snappy;
import prometheus.prompb.WriteRequest;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;

@Component
public class RemoteWriteHttpClient {
    private static final Logger LOG = LoggerFactory.getLogger(RemoteWriteHttpClient.class);
    private final RestClient client;
    private final PrometheusWriterProperties props;

    public RemoteWriteHttpClient(RestClient.Builder builder, PrometheusWriterProperties props) {
        this.props = props;
        this.client = builder
                .baseUrl(props.remoteWrite().url())
                .build();
    }

    /**
     * Posts the given WriteRequest to the RW endpoint.
     * @return HTTP status code on success, or throws RuntimeException wrapping the cause.
     */
    public int post(WriteRequest request) throws Exception {
        byte[] compressed = Snappy.compress(request.toByteArray());
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.parseMediaType("application/x-protobuf"));
        headers.set("Content-Encoding", "snappy");
        headers.set("X-Prometheus-Remote-Write-Version", "0.1.0");
        headers.set("User-Agent", "deltav-prometheus-writer/" + getClass().getPackage().getImplementationVersion());

        var auth = props.remoteWrite().auth();
        switch (auth.type()) {
            case BEARER -> headers.setBearerAuth(auth.bearerToken());
            case BASIC  -> headers.setBasicAuth(auth.basicUsername(), auth.basicPassword());
            case NONE   -> {}
        }
        for (Map.Entry<String, String> e : props.remoteWrite().headers().entrySet()) {
            headers.add(e.getKey(), e.getValue());
        }

        ResponseEntity<Void> resp = client.post()
                .uri(props.remoteWrite().url())
                .headers(h -> h.addAll(headers))
                .body(compressed)
                .retrieve()
                .toBodilessEntity();
        return resp.getStatusCode().value();
    }
}
```

- [ ] **Step 4: MockWebServer test**

`RemoteWriteHttpClientTest`:
- `post_200_returns_200` — MockWebServer returns 200. Assert Snappy-decompressible body + `Content-Type: application/x-protobuf` + `Content-Encoding: snappy` + `X-Prometheus-Remote-Write-Version: 0.1.0`.
- `post_with_bearer_adds_authorization_header` — auth type BEARER, token "abc", assert received `Authorization: Bearer abc`.
- `post_with_basic_adds_basic_auth_header` — auth type BASIC, user/pass "u"/"p", assert received `Authorization: Basic dTpw`.
- `post_with_extra_headers_adds_them` — `X-Scope-OrgID: tenant-1`, assert received.
- `post_500_throws_server_error` — MockWebServer returns 500, assert `HttpServerErrorException` thrown.
- `post_400_throws_client_error` — returns 400, assert `HttpClientErrorException` thrown (so retry policy can distinguish).

- [ ] **Step 5: Run + PASS + Commit**

```
git commit -m "feat(prometheus-writer): WriteRequestBuilder + RemoteWriteHttpClient

WriteRequestBuilder groups PromSamples into one TimeSeries per unique
(name, labels) tuple. __name__ label prefixed. Labels sorted
alphabetically.

RemoteWriteHttpClient: Spring 6 RestClient + Snappy compression + auth
(bearer/basic/none) + extra headers (X-Scope-OrgID for Mimir tenancy).
Throws HttpClientErrorException on 4xx and HttpServerErrorException
on 5xx so RemoteWriteRetryPolicy can classify.

10 unit tests via MockWebServer."
```

---

## Task 12: `RemoteWriteRetryPolicy` + unit tests

**Goal:** Pure classifier — HTTP status → `RetryAction` (RETRY, POISON_DLQ, CIRCUIT_OPEN).

**Files:**
- Create: `core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/rw/RemoteWriteRetryPolicy.java`
- Create: `core/prometheus-writer/src/test/java/org/deltav/prometheus/writer/rw/RemoteWriteRetryPolicyTest.java`

- [ ] **Step 1: Write failing test**

```java
/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.rw;

import org.junit.jupiter.api.Test;
import java.io.IOException;
import static org.assertj.core.api.Assertions.assertThat;

class RemoteWriteRetryPolicyTest {
    private final RemoteWriteRetryPolicy p = new RemoteWriteRetryPolicy();

    @Test void success_200() { assertThat(p.classifyStatus(200)).isEqualTo(RemoteWriteRetryPolicy.Action.SUCCESS); }
    @Test void success_204() { assertThat(p.classifyStatus(204)).isEqualTo(RemoteWriteRetryPolicy.Action.SUCCESS); }
    @Test void retry_429() { assertThat(p.classifyStatus(429)).isEqualTo(RemoteWriteRetryPolicy.Action.RETRY_WITH_BACKOFF); }
    @Test void retry_500() { assertThat(p.classifyStatus(500)).isEqualTo(RemoteWriteRetryPolicy.Action.RETRY_WITH_BACKOFF); }
    @Test void retry_502() { assertThat(p.classifyStatus(502)).isEqualTo(RemoteWriteRetryPolicy.Action.RETRY_WITH_BACKOFF); }
    @Test void retry_503() { assertThat(p.classifyStatus(503)).isEqualTo(RemoteWriteRetryPolicy.Action.RETRY_WITH_BACKOFF); }
    @Test void retry_504() { assertThat(p.classifyStatus(504)).isEqualTo(RemoteWriteRetryPolicy.Action.RETRY_WITH_BACKOFF); }
    @Test void poison_400() { assertThat(p.classifyStatus(400)).isEqualTo(RemoteWriteRetryPolicy.Action.POISON_DLQ); }
    @Test void poison_413() { assertThat(p.classifyStatus(413)).isEqualTo(RemoteWriteRetryPolicy.Action.POISON_DLQ); }
    @Test void circuit_401() { assertThat(p.classifyStatus(401)).isEqualTo(RemoteWriteRetryPolicy.Action.CIRCUIT_OPEN); }
    @Test void circuit_403() { assertThat(p.classifyStatus(403)).isEqualTo(RemoteWriteRetryPolicy.Action.CIRCUIT_OPEN); }
    @Test void circuit_404() { assertThat(p.classifyStatus(404)).isEqualTo(RemoteWriteRetryPolicy.Action.CIRCUIT_OPEN); }
    @Test void unknown_4xx_defaults_to_poison() { assertThat(p.classifyStatus(418)).isEqualTo(RemoteWriteRetryPolicy.Action.POISON_DLQ); }
    @Test void unknown_5xx_defaults_to_retry() { assertThat(p.classifyStatus(599)).isEqualTo(RemoteWriteRetryPolicy.Action.RETRY_WITH_BACKOFF); }
    @Test void network_exception_retries() {
        assertThat(p.classifyException(new java.net.ConnectException("refused")))
                .isEqualTo(RemoteWriteRetryPolicy.Action.RETRY_WITH_BACKOFF);
    }
    @Test void read_timeout_retries() {
        assertThat(p.classifyException(new java.net.SocketTimeoutException("timeout")))
                .isEqualTo(RemoteWriteRetryPolicy.Action.RETRY_WITH_BACKOFF);
    }
}
```

- [ ] **Step 2: Implement**

```java
/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.rw;

import org.springframework.stereotype.Component;

import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;

@Component
public class RemoteWriteRetryPolicy {
    public enum Action { SUCCESS, RETRY_WITH_BACKOFF, POISON_DLQ, CIRCUIT_OPEN }

    public Action classifyStatus(int status) {
        if (status == 200 || status == 204) return Action.SUCCESS;
        if (status == 429) return Action.RETRY_WITH_BACKOFF;
        if (status >= 500) return Action.RETRY_WITH_BACKOFF;
        if (status == 400 || status == 413) return Action.POISON_DLQ;
        if (status == 401 || status == 403 || status == 404) return Action.CIRCUIT_OPEN;
        if (status >= 400 && status < 500) return Action.POISON_DLQ;  // default other 4xx to poison
        return Action.POISON_DLQ;
    }

    public Action classifyException(Throwable t) {
        if (t instanceof SocketTimeoutException
                || t instanceof ConnectException
                || t instanceof UnknownHostException) return Action.RETRY_WITH_BACKOFF;
        // org.springframework.web.client.ResourceAccessException wraps network errors
        Throwable cause = t.getCause();
        if (cause != null && cause != t) return classifyException(cause);
        return Action.RETRY_WITH_BACKOFF;
    }
}
```

- [ ] **Step 3: Run + PASS + Commit**

```
git commit -m "feat(prometheus-writer): RemoteWriteRetryPolicy classifier

Tiered by HTTP status per spec §5 retry table:
  200/204  → SUCCESS
  429/5xx/network → RETRY_WITH_BACKOFF
  400/413  → POISON_DLQ
  401/403/404 → CIRCUIT_OPEN
  other 4xx → POISON_DLQ (default)

16 unit tests pin every documented code."
```

---

## Task 13: `BatchingRwWriter` + unit tests

**Goal:** In-memory accumulator. Flush triggers: 1000 samples OR 1 MB uncompressed OR 1 s since first sample. On flush, group by series + build `WriteRequest` + delegate to `RemoteWriteHttpClient` wrapped by circuit breaker.

**Files:**
- Create: `core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/rw/BatchingRwWriter.java`
- Create: `core/prometheus-writer/src/test/java/org/deltav/prometheus/writer/rw/BatchingRwWriterTest.java`

- [ ] **Step 1: Write failing tests.** Cases:

1. `flushes_on_max_samples_threshold` — add 1000 samples, assert `flushNow` called once after the 1000th add.
2. `flushes_on_max_bytes_threshold` — construct samples whose total size exceeds `maxBytes`, assert flush.
3. `flushes_on_time_interval_via_scheduled_check` — add 500 samples, advance mock clock past 1 s, assert flush triggered by scheduled check.
4. `flush_calls_http_client_with_grouped_WriteRequest` — mock http client, add 3 samples sharing a name, 2 with different name. Assert client received `WriteRequest` with 2 TimeSeries.
5. `flush_empty_buffer_is_noop` — no samples added, scheduled flush → no HTTP call.

Inject a `SimpleMeterRegistry` and assert counters `batches_sent_total`, `samples_sent_total` incremented.

- [ ] **Step 2: Implement**

```java
/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.rw;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.deltav.prometheus.writer.config.PrometheusWriterProperties;
import org.deltav.prometheus.writer.translate.PromSample;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import prometheus.prompb.WriteRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

@Component
public class BatchingRwWriter {
    private static final Logger LOG = LoggerFactory.getLogger(BatchingRwWriter.class);

    private final PrometheusWriterProperties props;
    private final WriteRequestBuilder builder;
    private final RemoteWriteHttpClient http;
    private final Counter batchesSent;
    private final Counter samplesSent;
    private final DistributionSummary batchBytes;
    private final DistributionSummary batchSampleCount;
    private final Timer flushDuration;

    private final ReentrantLock lock = new ReentrantLock();
    private final List<PromSample> buffer = new ArrayList<>();
    private final AtomicLong approxBytes = new AtomicLong(0);
    private volatile long firstSampleAtMs = 0L;

    public BatchingRwWriter(PrometheusWriterProperties props, WriteRequestBuilder builder,
                            RemoteWriteHttpClient http, MeterRegistry reg) {
        this.props = props; this.builder = builder; this.http = http;
        String endpoint = props.remoteWrite().url();
        this.batchesSent = Counter.builder("deltav.prometheus.writer.batches.sent")
                .tag("endpoint", endpoint).register(reg);
        this.samplesSent = Counter.builder("deltav.prometheus.writer.samples.sent")
                .tag("endpoint", endpoint).register(reg);
        this.batchBytes = DistributionSummary.builder("deltav.prometheus.writer.batch.size.bytes")
                .tag("endpoint", endpoint).register(reg);
        this.batchSampleCount = DistributionSummary.builder("deltav.prometheus.writer.batch.sample.count")
                .tag("endpoint", endpoint).register(reg);
        this.flushDuration = Timer.builder("deltav.prometheus.writer.flush.duration")
                .tag("endpoint", endpoint).register(reg);
    }

    public void add(PromSample sample) {
        lock.lock();
        try {
            if (buffer.isEmpty()) firstSampleAtMs = System.currentTimeMillis();
            buffer.add(sample);
            approxBytes.addAndGet(estimateSize(sample));
            if (shouldFlushNow()) flushNow();
        } finally { lock.unlock(); }
    }

    @Scheduled(fixedDelay = 100)
    public void flushIfStale() {
        lock.lock();
        try {
            if (!buffer.isEmpty()
                    && System.currentTimeMillis() - firstSampleAtMs >= props.batch().maxIntervalMs()) {
                flushNow();
            }
        } finally { lock.unlock(); }
    }

    public void flushNow() {
        if (buffer.isEmpty()) return;
        List<PromSample> toSend = new ArrayList<>(buffer);
        buffer.clear();
        approxBytes.set(0);
        Timer.Sample sample = Timer.start();
        try {
            WriteRequest req = builder.build(toSend);
            int bytesEstimate = req.getSerializedSize();
            http.post(req);
            batchesSent.increment();
            samplesSent.increment(toSend.size());
            batchBytes.record(bytesEstimate);
            batchSampleCount.record(toSend.size());
        } catch (Exception e) {
            // Flush failures propagate to caller's retry/DLQ/circuit logic (Task 14/16).
            // For unit-test purposes here we just rethrow; ConsumerPauseListener decides the reaction.
            throw new RuntimeException(e);
        } finally {
            sample.stop(flushDuration);
        }
    }

    private boolean shouldFlushNow() {
        return buffer.size() >= props.batch().maxSamples()
                || approxBytes.get() >= props.batch().maxBytes();
    }

    private long estimateSize(PromSample s) {
        // Rough heuristic — label-string lengths + 16 bytes per sample overhead.
        long total = 16;
        total += s.name().length();
        for (var e : s.labels().entrySet()) total += e.getKey().length() + e.getValue().length() + 2;
        return total;
    }
}
```

- [ ] **Step 3: Run + PASS + Commit**

```
git commit -m "feat(prometheus-writer): BatchingRwWriter — size/bytes/time flush triggers

In-memory buffer + scheduled 100ms staleness check. Flushes on whichever
trigger hits first: 1000 samples, 1 MB uncompressed (estimated), or 1 s
since first sample.

Emits deltav_prometheus_writer_{batches_sent,samples_sent,batch_size_bytes,
batch_sample_count,flush_duration}_ metrics with endpoint tag.

5 unit tests cover every flush trigger + grouping."
```

---

## Task 14: `PrometheusWriterCircuitBreaker` (Resilience4j)

**Goal:** Resilience4j `CircuitBreaker` bean wrapping the HTTP call. Configurable from `PrometheusWriterProperties.circuitBreaker`.

**Files:**
- Create: `core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/rw/PrometheusWriterCircuitBreaker.java`
- Create: `core/prometheus-writer/src/test/java/org/deltav/prometheus/writer/rw/PrometheusWriterCircuitBreakerTest.java`

- [ ] **Step 1: Write failing tests.** Cases:

1. `starts_closed` — newly-created breaker has state CLOSED.
2. `opens_at_failure_rate_threshold` — run 20 calls, 11 fail, assert state OPEN after.
3. `transitions_half_open_after_wait_duration` — trigger OPEN, wait past `waitDurationOpenMs`, assert HALF_OPEN.
4. `returns_to_closed_after_successful_probes` — from HALF_OPEN, permit 3 successes, assert CLOSED.
5. `returns_to_open_on_half_open_failure` — from HALF_OPEN, fail once, assert OPEN.

Use Resilience4j's `io.github.resilience4j.core.lang.NonNull` + synthetic `Runnable`s that throw on demand.

- [ ] **Step 2: Implement**

```java
/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.rw;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.deltav.prometheus.writer.config.PrometheusWriterProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

@Configuration
public class PrometheusWriterCircuitBreaker {

    public static final String NAME = "prometheus-writer";

    @Bean
    public CircuitBreakerRegistry circuitBreakerRegistry(PrometheusWriterProperties props) {
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .failureRateThreshold(props.circuitBreaker().failureRateThreshold())
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(props.circuitBreaker().slidingWindowSize())
                .minimumNumberOfCalls(props.circuitBreaker().minimumNumberOfCalls())
                .waitDurationInOpenState(Duration.ofMillis(props.circuitBreaker().waitDurationOpenMs()))
                .permittedNumberOfCallsInHalfOpenState(props.circuitBreaker().halfOpenPermittedCalls())
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .build();
        return CircuitBreakerRegistry.of(config);
    }

    @Bean
    public CircuitBreaker prometheusWriterCircuitBreaker(
            CircuitBreakerRegistry registry, MeterRegistry metrics, PrometheusWriterProperties props) {
        CircuitBreaker cb = registry.circuitBreaker(NAME);
        AtomicInteger stateGauge = new AtomicInteger(0);
        cb.getEventPublisher().onStateTransition(e -> {
            stateGauge.set(switch (e.getStateTransition().getToState()) {
                case CLOSED, METRICS_ONLY, DISABLED, FORCED_OPEN -> 0;
                case HALF_OPEN -> 1;
                case OPEN -> 2;
            });
        });
        Gauge.builder("deltav.prometheus.writer.circuit.state", stateGauge::get)
                .tag("endpoint", props.remoteWrite().url())
                .register(metrics);
        return cb;
    }
}
```

- [ ] **Step 3: Run + PASS + Commit**

```
git commit -m "feat(prometheus-writer): Resilience4j circuit breaker bean

Configured from PrometheusWriterProperties.circuitBreaker (50% failure
rate, 20-call window, 30s open, 3 half-open probes, auto-transition
open→half-open).

Gauge 'deltav_prometheus_writer_circuit_state' exposes current state
(0=closed, 1=half_open, 2=open) for alerting.

5 state-machine unit tests."
```

---

## Task 15: `TimeseriesConsumer` + SCS test-binder IT

**Goal:** SCS `Function<Message<byte[]>, Void>` bean definition. Deserializes `TimeseriesBatch`, looks up cache, runs translator, forwards samples to `BatchingRwWriter`.

**Files:**
- Create: `core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/consume/TimeseriesConsumer.java`
- Create: `core/prometheus-writer/src/test/java/org/deltav/prometheus/writer/consume/TimeseriesConsumerBinderIT.java`

- [ ] **Step 1: Implement**

```java
/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.consume;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.deltav.prometheus.writer.nodecontext.NodeContextCache;
import org.deltav.prometheus.writer.rw.BatchingRwWriter;
import org.deltav.prometheus.writer.translate.PromSample;
import org.deltav.prometheus.writer.translate.TimeseriesToPromTranslator;
import org.deltav.timeseries.proto.NodeContext;
import org.deltav.timeseries.proto.TimeseriesBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

@Configuration
public class TimeseriesConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(TimeseriesConsumer.class);

    @Bean
    public Consumer<Message<byte[]>> timeseriesConsumer(
            NodeContextCache cache,
            TimeseriesToPromTranslator translator,
            BatchingRwWriter writer,
            MeterRegistry metrics) {
        Counter consumed = metrics.counter("deltav.prometheus.writer.records.consumed");
        Counter samplesIn = metrics.counter("deltav.prometheus.writer.samples.in");
        return message -> {
            try {
                TimeseriesBatch batch = TimeseriesBatch.parseFrom(message.getPayload());
                String key = batch.getLocation() + "@" + batch.getNodeId();
                Optional<NodeContext> nc = cache.get(key);
                List<PromSample> samples = translator.translate(batch, nc);
                consumed.increment();
                samplesIn.increment(samples.size());
                samples.forEach(writer::add);
            } catch (Exception e) {
                LOG.warn("Failed to process TimeseriesBatch — dropping", e);
                metrics.counter("deltav.prometheus.writer.records.parse.errors").increment();
            }
        };
    }
}
```

- [ ] **Step 2: Write SCS test-binder IT**

`TimeseriesConsumerBinderIT` — uses `spring-cloud-stream-test-binder`:
- Input: publish a `TimeseriesBatch.toByteArray()` to `timeseriesConsumer-in-0`.
- Assert: `records_consumed_total` incremented, `samples_in_total` incremented by batch-attribute-count, mock `BatchingRwWriter` received `add()` calls for each sample.

- [ ] **Step 3: Run + PASS + Commit**

```
git commit -m "feat(prometheus-writer): TimeseriesConsumer SCS function

Spring Cloud Stream Consumer<Message<byte[]>> bean bound to
deltav-timeseries. Parse-error counter increments on malformed records
(dropped, not rethrown — consumer keeps moving).

SCS test-binder IT asserts full path: binder → deserialize → cache
lookup → translator → BatchingRwWriter.add()."
```

---

## Task 16: `ConsumerPauseListener`

**Goal:** Listen to Resilience4j circuit state transitions; pause/resume `timeseriesConsumer-in-0` binding accordingly.

**Files:**
- Create: `core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/rw/ConsumerPauseListener.java`
- Create: `core/prometheus-writer/src/test/java/org/deltav/prometheus/writer/rw/ConsumerPauseListenerTest.java`

- [ ] **Step 1: Write failing tests.** Cases:

1. `closed_to_open_pauses_binding` — simulate transition CLOSED → OPEN, assert `lifecycle.pause("timeseriesConsumer-in-0")` called.
2. `half_open_to_closed_resumes_binding` — transition HALF_OPEN → CLOSED, assert `lifecycle.resume(...)` called.
3. `half_open_to_open_pauses_again` — transition HALF_OPEN → OPEN, assert pause called.
4. `open_to_half_open_is_noop` — transition OPEN → HALF_OPEN, assert no pause/resume (probes run on same paused binding).

- [ ] **Step 2: Implement**

```java
/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.rw;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.binding.BindingsLifecycleController;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

@Component
public class ConsumerPauseListener {
    private static final Logger LOG = LoggerFactory.getLogger(ConsumerPauseListener.class);
    private static final String BINDING = "timeseriesConsumer-in-0";

    private final CircuitBreaker cb;
    private final BindingsLifecycleController lifecycle;
    private final AtomicInteger pausedGauge = new AtomicInteger(0);

    public ConsumerPauseListener(CircuitBreaker cb, BindingsLifecycleController lifecycle, MeterRegistry metrics) {
        this.cb = cb;
        this.lifecycle = lifecycle;
        Gauge.builder("deltav.prometheus.writer.consumer.paused", pausedGauge::get).register(metrics);
    }

    @PostConstruct
    public void wire() {
        cb.getEventPublisher().onStateTransition(e -> {
            var from = e.getStateTransition().getFromState();
            var to = e.getStateTransition().getToState();
            boolean pauseNow = to == CircuitBreaker.State.OPEN
                    && (from == CircuitBreaker.State.CLOSED || from == CircuitBreaker.State.HALF_OPEN);
            boolean resumeNow = from == CircuitBreaker.State.HALF_OPEN && to == CircuitBreaker.State.CLOSED;
            if (pauseNow) {
                LOG.warn("Circuit opened — pausing binding {}", BINDING);
                lifecycle.pause(BINDING);
                pausedGauge.set(1);
            } else if (resumeNow) {
                LOG.info("Circuit closed — resuming binding {}", BINDING);
                lifecycle.resume(BINDING);
                pausedGauge.set(0);
            }
        });
    }
}
```

- [ ] **Step 3: Run + PASS + Commit**

```
git commit -m "feat(prometheus-writer): ConsumerPauseListener — circuit state → binding pause/resume

Registers a state-transition listener on the prometheus-writer circuit.
CLOSED/HALF_OPEN → OPEN pauses the timeseries binding. HALF_OPEN → CLOSED
resumes it. OPEN → HALF_OPEN is a no-op (probes use the paused binding).

Gauge 'deltav_prometheus_writer_consumer_paused' exposes pause state."
```

---

## Task 17: `DlqPublisher` + DLQ `NewTopic` bean + binder IT

**Goal:** On 400/413, republish the source `TimeseriesBatch` bytes to `deltav-prometheus-writer-dlq` with diagnostic headers.

**Files:**
- Create: `core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/dlq/DlqPublisher.java`
- Create: `core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/config/KafkaTopicsConfiguration.java`
- Create: `core/prometheus-writer/src/test/java/org/deltav/prometheus/writer/dlq/DlqPublisherBinderIT.java`

- [ ] **Step 1: Implement `KafkaTopicsConfiguration`**

```java
/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

import java.util.Map;

@Configuration
public class KafkaTopicsConfiguration {
    @Bean
    public NewTopic deltavPrometheusWriterDlqTopic() {
        return TopicBuilder.name("deltav-prometheus-writer-dlq")
                .partitions(16)
                .replicas(1)
                .configs(Map.of(
                        "cleanup.policy", "delete",
                        "retention.ms", "604800000",          // 7 days
                        "compression.type", "lz4"))
                .build();
    }
}
```

- [ ] **Step 2: Implement `DlqPublisher`**

```java
/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.dlq;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class DlqPublisher {
    private static final Logger LOG = LoggerFactory.getLogger(DlqPublisher.class);
    private static final String BINDING = "publishDlq-out-0";

    private final StreamBridge streamBridge;
    private final MeterRegistry metrics;

    public DlqPublisher(StreamBridge streamBridge, MeterRegistry metrics) {
        this.streamBridge = streamBridge;
        this.metrics = metrics;
    }

    public void publish(byte[] sourcePayload, String key, String reason, int httpStatus,
                        String endpoint, String errorMessage) {
        Map<String, Object> headers = new HashMap<>();
        headers.put(KafkaHeaders.KEY, key.getBytes());
        headers.put("x-dlq-reason", reason);
        headers.put("x-dlq-http-status", Integer.toString(httpStatus));
        headers.put("x-dlq-endpoint", endpoint);
        headers.put("x-dlq-timestamp-ms", Long.toString(System.currentTimeMillis()));
        headers.put("x-dlq-error-message", truncate(errorMessage, 1024));
        headers.put("x-dlq-writer-version",
                Optional.ofNullable(getClass().getPackage().getImplementationVersion()).orElse("dev"));
        Message<byte[]> msg = MessageBuilder.withPayload(sourcePayload).copyHeaders(headers).build();
        boolean sent = streamBridge.send(BINDING, msg);
        if (sent) {
            Counter.builder("deltav.prometheus.writer.dlq.records")
                    .tag("reason", reason).register(metrics).increment();
        } else {
            LOG.error("StreamBridge.send returned false — DLQ record lost: key={} reason={}", key, reason);
        }
    }

    private static String truncate(String s, int max) {
        if (s == null) return "";
        return s.length() <= max ? s : s.substring(0, max);
    }
}
```

(Import `java.util.Optional` in the imports.)

- [ ] **Step 3: SCS binder IT**

`DlqPublisherBinderIT` asserts:
- Message sent to `deltav-prometheus-writer-dlq`.
- Byte-array payload preserved byte-for-byte (verifies `use-native-encoding: true`).
- All 6 diagnostic headers present.
- `deltav_prometheus_writer_dlq_records_total{reason="bad_request"}` incremented.

- [ ] **Step 4: Run + PASS + Commit**

```
git commit -m "feat(prometheus-writer): DlqPublisher + deltav-prometheus-writer-dlq NewTopic

DLQ topic: 16 partitions, cleanup.policy=delete, retention 7 days, lz4.
DlqPublisher uses StreamBridge to send the original TimeseriesBatch
bytes with 6 diagnostic headers (reason, status, endpoint, timestamp,
error-message, writer-version).

Binding publishDlq-out-0 uses producer.use-native-encoding: true to
preserve byte-array payload (Phase 1.5 SCS splitter lesson)."
```

---

## Task 18: `PrometheusWriterMetrics` central declaration

**Goal:** One bean holds references to every Micrometer meter in the spec §7 table. All other beans request meters from it (or use `MeterRegistry.counter(...)` directly — both are acceptable; this task consolidates the canonical meter names as constants).

**Files:**
- Create: `core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/metrics/PrometheusWriterMetrics.java`
- Create: `core/prometheus-writer/src/test/java/org/deltav/prometheus/writer/metrics/PrometheusWriterMetricsTest.java`

- [ ] **Step 1: Implement — just a constants class**

```java
/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.metrics;

/**
 * Canonical Micrometer meter names for the prometheus-writer. Every meter
 * declared here MUST appear at /actuator/prometheus once the service is fully
 * wired. {@link PrometheusWriterMetricsTest} pins the list.
 */
public final class PrometheusWriterMetrics {
    private PrometheusWriterMetrics() {}

    // ingestion
    public static final String RECORDS_CONSUMED        = "deltav.prometheus.writer.records.consumed";
    public static final String SAMPLES_IN              = "deltav.prometheus.writer.samples.in";
    public static final String RECORDS_PARSE_ERRORS    = "deltav.prometheus.writer.records.parse.errors";

    // enrichment
    public static final String ENRICHMENT_HIT          = "deltav.prometheus.writer.enrichment.hit";
    public static final String ENRICHMENT_MISSING      = "deltav.prometheus.writer.enrichment.missing";
    public static final String NC_CACHE_SIZE           = "deltav.prometheus.writer.node.context.cache.size";
    public static final String NC_CACHE_READY          = "deltav.prometheus.writer.node.context.cache.ready";
    public static final String NC_BOOTSTRAP_DURATION   = "deltav.prometheus.writer.node.context.bootstrap.duration";

    // sample dropouts
    public static final String SAMPLES_DROPPED         = "deltav.prometheus.writer.samples.dropped";

    // rw output
    public static final String BATCHES_SENT            = "deltav.prometheus.writer.batches.sent";
    public static final String SAMPLES_SENT            = "deltav.prometheus.writer.samples.sent";
    public static final String BATCHES_FAILED          = "deltav.prometheus.writer.batches.failed";
    public static final String BATCH_SIZE_BYTES        = "deltav.prometheus.writer.batch.size.bytes";
    public static final String BATCH_SAMPLE_COUNT      = "deltav.prometheus.writer.batch.sample.count";
    public static final String FLUSH_DURATION          = "deltav.prometheus.writer.flush.duration";
    public static final String RETRY_ATTEMPTS          = "deltav.prometheus.writer.retry.attempts";

    // dlq
    public static final String DLQ_RECORDS             = "deltav.prometheus.writer.dlq.records";

    // circuit + consumer state
    public static final String CIRCUIT_STATE           = "deltav.prometheus.writer.circuit.state";
    public static final String CONSUMER_PAUSED         = "deltav.prometheus.writer.consumer.paused";
}
```

- [ ] **Step 2: Unit test pins the field list**

`PrometheusWriterMetricsTest` uses reflection to iterate all `public static final String` fields and asserts a known canonical list (byte-for-byte equal). Fails if someone adds or removes a meter without updating the spec.

- [ ] **Step 3: Update all producer sites** (where counters/gauges/timers are created in Tasks 10–17) to reference `PrometheusWriterMetrics.*` constants instead of string literals. Purely a refactor — tests from those tasks must still pass.

- [ ] **Step 4: Commit**

```
git commit -m "refactor(prometheus-writer): centralize Micrometer meter names as constants

PrometheusWriterMetrics.java is the canonical source of truth for every
meter name. All producer sites now reference the constant. Reflection-
based test pins the full list — adding/removing a meter requires a
deliberate test change + spec update."
```

---

## Task 19: Wiring + `PrometheusWriterApplicationScanIT` (real-main-class scar prophylactic)

**Goal:** Create `PrometheusWriterConfiguration`, wire any remaining beans, and add a real-main-class integration test that boots `PrometheusWriterApplication` and asserts every key bean is resolvable. **This is scar prophylactic #1 from the spec** — mandatory before any live E2E.

**Files:**
- Create: `core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/PrometheusWriterConfiguration.java`
- Create: `core/prometheus-writer/src/test/java/org/deltav/prometheus/writer/PrometheusWriterApplicationScanIT.java`

- [ ] **Step 1: Implement `PrometheusWriterConfiguration`**

```java
/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer;

import org.deltav.prometheus.writer.config.PrometheusWriterProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestClient;

import java.time.Duration;

@Configuration
@EnableConfigurationProperties(PrometheusWriterProperties.class)
public class PrometheusWriterConfiguration {
    @Bean
    public RestClient.Builder restClientBuilder() {
        return RestClient.builder()
                // Connect and read timeouts per spec §5:
                //   connect 5s, read 30s
                .requestFactory(new org.springframework.http.client.SimpleClientHttpRequestFactory() {{
                    setConnectTimeout((int) Duration.ofSeconds(5).toMillis());
                    setReadTimeout((int) Duration.ofSeconds(30).toMillis());
                }});
    }
}
```

- [ ] **Step 2: Implement the real-main-class IT**

```java
/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import org.apache.kafka.clients.admin.NewTopic;
import org.deltav.prometheus.writer.consume.TimeseriesConsumer;
import org.deltav.prometheus.writer.dlq.DlqPublisher;
import org.deltav.prometheus.writer.nodecontext.NodeContextCache;
import org.deltav.prometheus.writer.nodecontext.NodeContextCacheHealthIndicator;
import org.deltav.prometheus.writer.nodecontext.NodeContextKafkaBootstrap;
import org.deltav.prometheus.writer.rw.BatchingRwWriter;
import org.deltav.prometheus.writer.rw.ConsumerPauseListener;
import org.deltav.prometheus.writer.rw.RemoteWriteHttpClient;
import org.deltav.prometheus.writer.rw.RemoteWriteRetryPolicy;
import org.deltav.prometheus.writer.rw.WriteRequestBuilder;
import org.deltav.prometheus.writer.startup.TimeseriesBindingResumer;
import org.deltav.prometheus.writer.startup.TimeseriesBindingStartupGate;
import org.deltav.prometheus.writer.translate.LabelBuilder;
import org.deltav.prometheus.writer.translate.NameSanitizer;
import org.deltav.prometheus.writer.translate.TimeseriesToPromTranslator;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Real-main-class integration test. Boots PrometheusWriterApplication via
 * SpringApplication.run() against a Testcontainers Kafka broker. Asserts
 * every key bean from the spec is resolvable.
 *
 * <p>Scar prophylactic for the Phase 0 #170 bug: unit tests + @Import-based
 * ITs bypass the component scan. A mistyped scanBasePackages silently hides
 * an entire @Configuration class and the problem only surfaces in Docker.
 * This IT would have caught it in one CI run.
 */
@Testcontainers
@SpringBootTest(classes = PrometheusWriterApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class PrometheusWriterApplicationScanIT {

    @Container
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.0"));

    @DynamicPropertySource
    static void kafkaProps(DynamicPropertyRegistry reg) {
        reg.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        reg.add("spring.cloud.stream.kafka.binder.brokers", KAFKA::getBootstrapServers);
        reg.add("prometheus-writer.remote-write.url", () -> "http://localhost:1/unused");
    }

    @Autowired ApplicationContext ctx;

    @Test
    void all_key_beans_resolve() {
        // Node-context subsystem
        assertThat(ctx.getBean(NodeContextCache.class)).isNotNull();
        assertThat(ctx.getBean(NodeContextKafkaBootstrap.class)).isNotNull();
        assertThat(ctx.getBean(NodeContextCacheHealthIndicator.class)).isNotNull();
        // Translation
        assertThat(ctx.getBean(NameSanitizer.class)).isNotNull();
        assertThat(ctx.getBean(LabelBuilder.class)).isNotNull();
        assertThat(ctx.getBean(TimeseriesToPromTranslator.class)).isNotNull();
        // RW output
        assertThat(ctx.getBean(WriteRequestBuilder.class)).isNotNull();
        assertThat(ctx.getBean(RemoteWriteHttpClient.class)).isNotNull();
        assertThat(ctx.getBean(RemoteWriteRetryPolicy.class)).isNotNull();
        assertThat(ctx.getBean(BatchingRwWriter.class)).isNotNull();
        assertThat(ctx.getBean(CircuitBreaker.class)).isNotNull();
        assertThat(ctx.getBean(ConsumerPauseListener.class)).isNotNull();
        // Consumer + startup gate
        assertThat(ctx.getBean(TimeseriesConsumer.class)).isNotNull();
        assertThat(ctx.getBean(TimeseriesBindingStartupGate.class)).isNotNull();
        assertThat(ctx.getBean(TimeseriesBindingResumer.class)).isNotNull();
        // DLQ
        assertThat(ctx.getBean(DlqPublisher.class)).isNotNull();
        assertThat(ctx.getBean("deltavPrometheusWriterDlqTopic", NewTopic.class)).isNotNull();
    }
}
```

- [ ] **Step 3: Run — if any bean missing, FAIL surfaces a scanBasePackages or wiring gap.**

```bash
./mvnw -pl core/prometheus-writer test -Dtest=PrometheusWriterApplicationScanIT
```

Expected: PASS. If FAIL, the failing assertion tells you exactly which bean isn't resolving; most likely a subpackage missing from `@SpringBootApplication(scanBasePackages=...)` or a typo on a `@Component` / `@Configuration` annotation.

- [ ] **Step 4: Commit**

```
git commit -m "test(prometheus-writer): PrometheusWriterApplicationScanIT real-main-class IT

Boots PrometheusWriterApplication via SpringApplication.run() against a
Testcontainers Kafka broker. Asserts every key bean from the spec
resolves. Catches scanBasePackages gaps in one CI run — Phase 0 #170
scar prophylactic #1."
```

---

## Task 20: Full-stack round-trip Testcontainers IT

**Goal:** `RwRoundTripIT` — real Kafka + real MockWebServer as RW target. Produce a `TimeseriesBatch` + `NodeContext` to Kafka, boot the app, assert: cache bootstraps, binding resumes, samples flow, MockWebServer receives a decompressible `WriteRequest` with the expected series name + labels. Also `CircuitBreakerIT` — inject 500s, assert circuit opens + consumer pauses, then recover and assert resume.

**Files:**
- Create: `core/prometheus-writer/src/test/java/org/deltav/prometheus/writer/rw/RwRoundTripIT.java`
- Create: `core/prometheus-writer/src/test/java/org/deltav/prometheus/writer/rw/CircuitBreakerIT.java`

- [ ] **Step 1: `RwRoundTripIT`**

Sketch (~120 lines):

```java
@Testcontainers
@SpringBootTest(classes = PrometheusWriterApplication.class, webEnvironment = WebEnvironment.RANDOM_PORT)
class RwRoundTripIT {
    @Container static final KafkaContainer KAFKA = new KafkaContainer(...);
    static MockWebServer mockRw;

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry reg) throws Exception {
        mockRw = new MockWebServer(); mockRw.start();
        reg.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        reg.add("spring.cloud.stream.kafka.binder.brokers", KAFKA::getBootstrapServers);
        reg.add("prometheus-writer.remote-write.url", () -> mockRw.url("/api/v1/write").toString());
        reg.add("prometheus-writer.batch.max-interval-ms", () -> "200");
    }

    @Test
    void end_to_end_sample_lands_at_rw_target() throws Exception {
        // 1) Publish NodeContext so cache can bootstrap
        publishNodeContext(5, "Default", "server-01", List.of("production", "critical"));
        // 2) Queue success response on mock
        mockRw.enqueue(new MockResponse().setResponseCode(200));
        // 3) Publish TimeseriesBatch
        publishTimeseriesBatch(5, "Default", AttributeType.ATTRIBUTE_TYPE_COUNTER,
                "mib2-interface-errors", "ifInDiscards", 42.0);

        RecordedRequest req = mockRw.takeRequest(15, TimeUnit.SECONDS);
        assertThat(req).isNotNull();
        assertThat(req.getHeader("Content-Type")).isEqualTo("application/x-protobuf");
        assertThat(req.getHeader("Content-Encoding")).isEqualTo("snappy");

        byte[] uncompressed = Snappy.uncompress(req.getBody().readByteArray());
        WriteRequest wr = WriteRequest.parseFrom(uncompressed);
        assertThat(wr.getTimeseriesCount()).isEqualTo(1);
        TimeSeries ts = wr.getTimeseries(0);
        Map<String,String> labels = ts.getLabelsList().stream()
                .collect(Collectors.toMap(Label::getName, Label::getValue));
        assertThat(labels).containsEntry("__name__", "opennms_mib2_interface_errors_ifindiscards_total");
        assertThat(labels).containsEntry("node_id", "5");
        assertThat(labels).containsEntry("node_label", "server-01");
        assertThat(labels).containsEntry("categories", "critical,production");
        assertThat(ts.getSamples(0).getValue()).isEqualTo(42.0);
    }
}
```

- [ ] **Step 2: `CircuitBreakerIT`**

- Publish NodeContext + TimeseriesBatches sufficient to trigger `sliding-window-size=20` plus 11 enqueued 500 responses.
- Assert `circuit_state` gauge flips to OPEN within Awaitility timeout.
- Assert `consumer_paused` gauge flips to 1.
- Enqueue 3 success responses; wait past `wait-duration-open-ms` (but cap the test with a low override, e.g. 3000 ms).
- Assert `circuit_state` returns to CLOSED and `consumer_paused` to 0.

- [ ] **Step 3: Run + PASS + Commit**

```
git commit -m "test(prometheus-writer): RwRoundTripIT + CircuitBreakerIT

End-to-end Testcontainers coverage: Kafka in → cache bootstrap →
translator → batching writer → MockWebServer RW target. Asserts Snappy
decompression + label set + metric name match spec.

CircuitBreakerIT injects 500s to trigger circuit open + binding pause,
then recovers and asserts resume."
```

---

## Task 21: `Dockerfile.prometheus-writer`

**Goal:** Containerize the fat jar.

**Files:**
- Create: `core/prometheus-writer/Dockerfile`

- [ ] **Step 1: Write Dockerfile**

Exactly:

```dockerfile
# Copyright (C) 2026 BeaconStrategists, Inc.
# Licensed under the GNU Affero General Public License v3.
FROM eclipse-temurin:21-jre-alpine

RUN apk add --no-cache curl && \
    addgroup -g 10001 deltav && \
    adduser -u 10001 -G deltav -D deltav

COPY --chown=deltav:deltav target/prometheus-writer.jar /app/prometheus-writer.jar

USER deltav
EXPOSE 8080

ENTRYPOINT ["java", "-jar", "/app/prometheus-writer.jar"]
```

- [ ] **Step 2: Build image manually to verify**

```bash
./mvnw -pl core/prometheus-writer -DskipTests package
docker build -t opennms/prometheus-writer:dev -f core/prometheus-writer/Dockerfile core/prometheus-writer
docker run --rm opennms/prometheus-writer:dev --help 2>&1 | head -5
```

Expected: image builds + container runs. (Container will fail-fast because Kafka is not reachable; we don't care — we're verifying the image.)

- [ ] **Step 3: Commit**

```
git commit -m "feat(prometheus-writer): Dockerfile

eclipse-temurin:21-jre-alpine + curl (for compose healthcheck) + deltav
non-root user, mirrors core/flow-enricher Dockerfile."
```

---

## Task 22: `docker-compose.yml` — writer service + VictoriaMetrics

**Goal:** Add `prometheus-writer` to `[lite, full]` profiles, `victoriametrics` to new `[metrics-e2e]` profile. Both services healthchecked.

**Files:**
- Modify: `opennms-container/delta-v/docker-compose.yml`

- [ ] **Step 1: Add services** at the end of the existing services section (before the `volumes:` / `networks:` blocks). Exactly per spec §9 with the version pinned:

```yaml
  prometheus-writer:
    image: opennms/prometheus-writer:${ONMS_DELTAV_VERSION:-latest}
    profiles: [lite, full]
    depends_on:
      kafka:
        condition: service_healthy
      provisiond:
        condition: service_started
    environment:
      SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka:9093
      PROMETHEUS_WRITER_REMOTE_WRITE_URL: http://victoriametrics:8428/api/v1/write
      JAVA_TOOL_OPTIONS: "-Xms128m -Xmx512m"
    healthcheck:
      test: ["CMD", "curl", "-sf", "http://localhost:8080/actuator/health/readiness"]
      interval: 10s
      timeout: 5s
      retries: 10
    ports:
      - "18080:8080"

  victoriametrics:
    image: victoriametrics/victoria-metrics:v1.106.1
    profiles: [metrics-e2e]
    command:
      - "-retentionPeriod=1h"
      - "-storageDataPath=/tmp/vm"
      - "-search.latencyOffset=1s"
    healthcheck:
      test: ["CMD", "wget", "-q", "-O-", "http://localhost:8428/health"]
      interval: 5s
      timeout: 3s
      retries: 10
    ports:
      - "18428:8428"
```

- [ ] **Step 2: Verify compose syntax**

```bash
cd opennms-container/delta-v && docker compose --profile lite --profile metrics-e2e config >/dev/null
```

Expected: no errors.

- [ ] **Step 3: Commit**

```
git commit -m "feat(delta-v/compose): add prometheus-writer + victoriametrics services

prometheus-writer in [lite, full] profiles. victoriametrics pinned to
v1.106.1 in new [metrics-e2e] profile. Both healthchecked.

In lite/full profiles without metrics-e2e, prometheus-writer starts
healthy but opens its circuit on unreachable VM — an intentional
degraded-but-stable state. Operators override PROMETHEUS_WRITER_REMOTE_WRITE_URL
to point at their real TSDB."
```

---

## Task 23: `build.sh` — `do_prometheus_writer_image()`

**Goal:** Make `./build.sh deltav` (and the top-level `./build.sh`) build `opennms/prometheus-writer:${VERSION}`.

**Files:**
- Modify: `opennms-container/delta-v/build.sh`

- [ ] **Step 1: Add the function**

After the existing `do_flow_enricher_image()` function (near line 140), add:

```bash
do_prometheus_writer_image() {
    log "Building prometheus-writer image (opennms/prometheus-writer:$VERSION)..."
    cd "$REPO_ROOT"
    ./mvnw -B -f core/prometheus-writer/pom.xml -DskipTests package
    cd "$REPO_ROOT/core/prometheus-writer"
    docker build -t "opennms/prometheus-writer:$VERSION" -t "opennms/prometheus-writer:latest" .
}
```

- [ ] **Step 2: Call it from the deltav image build block**

Find the block that calls `do_flow_enricher_image` (around line 236) and add a call to `do_prometheus_writer_image` right after it:

```bash
    # --- Build flow-enricher (standalone Spring Cloud Stream service) ---
    do_flow_enricher_image

    # --- Build prometheus-writer (Phase 2 standalone SCS consumer) ---
    do_prometheus_writer_image
```

- [ ] **Step 3: Update the summary `docker images` filter** (line ~238) to include `prometheus-writer`:

```bash
    docker images --format "  {{.Repository}}:{{.Tag}}\t{{.Size}}" | grep -E "daemon-base|alarmd|bsmd|collectd|discovery|enlinkd|eventtranslator|perspectivepollerd|pollerd|provisiond|syslogd|telemetryd|trapd|daemon-deltav|minion-deltav|minion-boot|flow-enricher|prometheus-writer" | sort | head -20
```

- [ ] **Step 4: Run locally to verify**

```bash
./opennms-container/delta-v/build.sh images 2>&1 | tail -20
```

Expected: `opennms/prometheus-writer:<version>` appears in the final summary.

- [ ] **Step 5: Commit**

```
git commit -m "feat(delta-v/build): add do_prometheus_writer_image to build.sh

Called after do_flow_enricher_image. Pattern matches flow-enricher
exactly: standalone mvnw package + docker build. Summary grep filter
extended to include prometheus-writer."
```

---

## Task 24: `test-prometheus-writer-e2e.sh` (6-step, pipefail-safe grep)

**Goal:** Layer-5 end-to-end smoke test. All 6 scar prophylactics respected.

**Files:**
- Create: `opennms-container/delta-v/test-prometheus-writer-e2e.sh` (chmod +x)

- [ ] **Step 1: Write the script**

```bash
#!/usr/bin/env bash
# Copyright (C) 2026 BeaconStrategists, Inc.
# Licensed under the GNU Affero General Public License v3.
#
# Layer 5 end-to-end smoke test for the prometheus-writer.  Starts the
# delta-v Docker Compose stack (lite + metrics-e2e profiles: Collectd
# produces to deltav-timeseries, Provisiond produces to
# deltav-node-context, prometheus-writer consumes + enriches + POSTs to
# VictoriaMetrics), then asserts:
#
#   1. prometheus-writer /actuator/health/readiness is UP (cache
#      bootstrapped + binding resumed)
#   2. NodeContextCache bootstrap fired cleanly
#      (bootstrap_duration_seconds_count >= 1, cache_ready == 1)
#   3. After 2 × 30 s Collectd poll cycles (~90 s wait),
#      records_consumed_total > 0, samples_sent_total > 0,
#      batches_sent_total > 0, circuit_state == 0 (closed)
#   4. (folded into step 5 assertions below)
#   5. Zero failure counters (batches_failed_total == 0,
#      enrichment_missing_total == 0, samples_dropped_total == 0,
#      dlq_records_total == 0)
#   6. VictoriaMetrics query returns a result with the expected label
#      set — the "gold standard" assertion that proves the full RW
#      wire round-trip.
#
# Scar prophylactic: every grep pipeline guarded by { ... || true; }
# because grep returns 1 on no-match and `set -euo pipefail` would kill
# the script (Phase 1 scar).

set -euo pipefail

cd "$(dirname "$0")"

STACK_READY_TIMEOUT=180
POLL_GRACE_SECONDS=90
METRICS_TIMEOUT=180
VM_QUERY_TIMEOUT=30

cleanup() {
    echo "==> Tearing down stack"
    docker compose --profile lite --profile metrics-e2e down -v --remove-orphans || true
}
trap cleanup EXIT

echo "==> Starting delta-v Docker Compose (lite + metrics-e2e profiles)"
docker compose --profile lite --profile metrics-e2e up -d --build

# ── Step 1: Wait for prometheus-writer readiness ──────────────────────────────
echo "==> Step 1: Wait for prometheus-writer /actuator/health/readiness"
deadline=$((SECONDS + STACK_READY_TIMEOUT))
while (( SECONDS < deadline )); do
    if docker compose exec -T prometheus-writer \
            curl -sf http://localhost:8080/actuator/health/readiness >/dev/null 2>&1; then
        echo "==> prometheus-writer ready"
        break
    fi
    sleep 3
done
if ! docker compose exec -T prometheus-writer \
        curl -sf http://localhost:8080/actuator/health/readiness >/dev/null 2>&1; then
    echo "FAIL: prometheus-writer did not become ready in ${STACK_READY_TIMEOUT}s"
    docker compose logs prometheus-writer | tail -100
    exit 1
fi

# ── Step 2: Assert startup gate fired cleanly ─────────────────────────────────
echo "==> Step 2: Assert startup gate fired"
metrics=$(docker compose exec -T prometheus-writer \
        curl -sf http://localhost:8080/actuator/prometheus)

ready=$({ echo "$metrics" | grep -E '^deltav_prometheus_writer_node_context_cache_ready\b' || true; } \
        | awk '{print $2}' | head -1)
if [[ "$ready" != "1.0" && "$ready" != "1" ]]; then
    echo "FAIL: node_context_cache_ready != 1 (got: '$ready')"
    exit 1
fi

boot_count=$({ echo "$metrics" | grep -E '^deltav_prometheus_writer_node_context_bootstrap_duration_seconds_count\b' || true; } \
        | awk '{sum+=$2} END {print sum+0}')
if (( $(echo "$boot_count < 1" | bc -l) )); then
    echo "FAIL: bootstrap_duration_seconds_count < 1 (got: $boot_count)"
    exit 1
fi
echo "==> Startup gate verified: cache_ready=1, bootstrap_count=$boot_count"

# ── Step 3: Wait for Collectd polls ───────────────────────────────────────────
echo "==> Step 3: Wait ${POLL_GRACE_SECONDS}s for Collectd to produce timeseries"
sleep "${POLL_GRACE_SECONDS}"

# ── Step 4: Assert samples flowed ─────────────────────────────────────────────
echo "==> Step 4: Assert records/samples/batches flowed"
metrics=$(docker compose exec -T prometheus-writer \
        curl -sf http://localhost:8080/actuator/prometheus)

assert_gt_zero() {
    local name="$1"
    local value
    value=$({ echo "$metrics" | grep -E "^${name}\b" || true; } | awk '{sum+=$2} END {print sum+0}')
    if (( $(echo "$value <= 0" | bc -l) )); then
        echo "FAIL: ${name} is not > 0 (got: $value)"
        exit 1
    fi
    echo "==> ${name} = ${value}"
}

assert_gt_zero 'deltav_prometheus_writer_records_consumed_total'
assert_gt_zero 'deltav_prometheus_writer_samples_sent_total'
assert_gt_zero 'deltav_prometheus_writer_batches_sent_total'

circuit=$({ echo "$metrics" | grep -E '^deltav_prometheus_writer_circuit_state\b' || true; } | awk '{print $2}' | head -1)
if [[ "$circuit" != "0.0" && "$circuit" != "0" ]]; then
    echo "FAIL: circuit_state != 0 (got: '$circuit')"
    exit 1
fi
echo "==> Circuit closed (state=0)"

# ── Step 5: Assert zero failures ──────────────────────────────────────────────
echo "==> Step 5: Assert zero failure counters"
assert_zero() {
    local name="$1"
    local value
    value=$({ echo "$metrics" | grep -E "^${name}" || true; } | awk '{sum+=$2} END {print sum+0}')
    if (( $(echo "$value > 0" | bc -l) )); then
        echo "FAIL: ${name} > 0 (got: $value)"
        exit 1
    fi
    echo "==> ${name} = 0 ✓"
}

assert_zero 'deltav_prometheus_writer_batches_failed_total'
assert_zero 'deltav_prometheus_writer_enrichment_missing_total'
assert_zero 'deltav_prometheus_writer_samples_dropped_total'
assert_zero 'deltav_prometheus_writer_dlq_records_total'

# ── Step 6: Query VictoriaMetrics ─────────────────────────────────────────────
echo "==> Step 6: Query VictoriaMetrics for the landed series"
deadline=$((SECONDS + VM_QUERY_TIMEOUT))
while (( SECONDS < deadline )); do
    resp=$(curl -sf "http://localhost:18428/api/v1/query?query=opennms_mib2_interface_errors_ifindiscards_total" \
            || echo '{"data":{"result":[]}}')
    count=$(echo "$resp" | docker compose exec -T prometheus-writer python3 -c \
        'import json,sys; d=json.load(sys.stdin); print(len(d.get("data",{}).get("result",[])))' \
        2>/dev/null || echo "0")
    if (( count > 0 )); then
        echo "==> VM returned ${count} series for opennms_mib2_interface_errors_ifindiscards_total"
        # Assert expected labels
        echo "$resp" | grep -E '"node_id":"[0-9]+"' > /dev/null || { echo "FAIL: missing node_id label"; exit 1; }
        echo "$resp" | grep -E '"location":' > /dev/null || { echo "FAIL: missing location label"; exit 1; }
        echo "$resp" | grep -E '"foreign_source":' > /dev/null || { echo "FAIL: missing foreign_source label"; exit 1; }
        echo "$resp" | grep -E '"resource_instance":' > /dev/null || { echo "FAIL: missing resource_instance label"; exit 1; }
        echo "==> ALL ASSERTIONS PASSED"
        exit 0
    fi
    sleep 2
done

echo "FAIL: VictoriaMetrics returned no results within ${VM_QUERY_TIMEOUT}s"
echo "Last VM response: $resp"
exit 1
```

`chmod +x opennms-container/delta-v/test-prometheus-writer-e2e.sh`.

- [ ] **Step 2: Run locally (requires Docker, `./build.sh deltav` completed recently)**

```bash
cd opennms-container/delta-v && ./test-prometheus-writer-e2e.sh
```

Expected: `ALL ASSERTIONS PASSED`.

- [ ] **Step 3: Commit**

```
git commit -m "test(delta-v/e2e): test-prometheus-writer-e2e.sh — 6-step smoke test

Brings up lite + metrics-e2e profiles. Asserts:
  1. prometheus-writer readiness (cache bootstrap + binding resume)
  2. bootstrap metrics fired
  3. Collectd polls produced samples
  4. records/samples/batches > 0, circuit closed
  5. zero failure counters (grep wrapped in { ... || true; } for pipefail)
  6. VictoriaMetrics query returns series with expected label set

The final VM query is the gold standard — proves the full RW wire
round-trip, not just 'our process sent something.'"
```

---

## Task 25: Freeze schemas + README update + full-reactor verify

**Goal:** Per spec Schema-freeze decision: update both `.proto` headers to reflect Phase 2 GA wire-freeze. Update README. Run full-reactor build.

**Files:**
- Modify: `core/deltav-kafka-contracts/src/main/proto/deltav-timeseries.proto` (header comment only)
- Modify: `core/deltav-kafka-contracts/src/main/proto/deltav-node-context.proto` (header comment only)
- Modify: `opennms-container/delta-v/README.md`

- [ ] **Step 1: Update both .proto headers**

In each file, replace the existing versioning comment block (lines about "during Phase 1 (feature flag off in production) breaking schema changes are permissible...") with:

```protobuf
// API version: 1 (FROZEN at Phase 2 GA — delta-v#<TBD-phase-2-PR>)
//
// Public contract for the deltav-timeseries Kafka topic.
//
// Versioning: this wire format is frozen as of Phase 2 GA. Only
// forward-compatible changes are permitted: new fields with new tag
// numbers, new enum values appended at the end. Renaming or renumbering
// existing fields, deleting fields, or changing field types are breaking
// changes that require bumping to // API version: 2 with a one-release
// deprecation cycle maintaining v1 as read-only.
```

(Same block for node-context, except `deltav-node-context`.) The `<TBD-phase-2-PR>` placeholder is resolved in the PR description after merge.

- [ ] **Step 2: Add Phase 2 README section**

Append to `opennms-container/delta-v/README.md`:

```markdown
## Phase 2 — Prometheus Remote Write consumer

The `prometheus-writer` service consumes `deltav-timeseries`, enriches each
batch with node identity from `deltav-node-context`, and POSTs Snappy-compressed
Prometheus Remote Write protobuf batches to a configurable endpoint.

### Profiles

- **lite, full** — starts `prometheus-writer`. Point `PROMETHEUS_WRITER_REMOTE_WRITE_URL`
  at your TSDB (Mimir, VictoriaMetrics, Cortex, Thanos Receive, Prometheus with
  `--web.enable-remote-write-receiver`). Without a reachable target, the writer
  starts healthy, opens its circuit on first POST failure, and stays paused.
- **metrics-e2e** — adds a pinned `victoriametrics:v1.106.1` container for E2E.
  Not intended for production.

### Configuration

Environment variables (also see `core/prometheus-writer/src/main/resources/application.yml`):

| Variable | Default | Purpose |
|---|---|---|
| `PROMETHEUS_WRITER_REMOTE_WRITE_URL` | `http://victoriametrics:8428/api/v1/write` | RW endpoint |
| `PROMETHEUS_WRITER_AUTH_TYPE` | `none` | `none` / `bearer` / `basic` |
| `PROMETHEUS_WRITER_BEARER_TOKEN` | (empty) | Bearer token when `AUTH_TYPE=bearer` |
| `PROMETHEUS_WRITER_BASIC_USER` | (empty) | Basic auth user |
| `PROMETHEUS_WRITER_BASIC_PASS` | (empty) | Basic auth pass |
| `SPRING_KAFKA_BOOTSTRAP_SERVERS` | `kafka:9093` (compose) | Kafka brokers |

Extra headers (e.g. `X-Scope-OrgID` for Mimir tenancy) via yaml
`prometheus-writer.remote-write.headers.{name}: "{value}"`.

Opt-in metadata-label promotion:

```yaml
prometheus-writer:
  labels:
    from-metadata:
      - requisition:region
      - snmp:sysLocation
```

### Metrics

All exposed at `/actuator/prometheus`, prefix `deltav_prometheus_writer_`.
See `docs/superpowers/specs/2026-04-17-kafka-ts-phase-2-prometheus-consumer-design.md` §7
for the full list.

Useful PromQL examples:

```
rate(deltav_prometheus_writer_samples_sent_total[5m])
rate(deltav_prometheus_writer_enrichment_missing_total[5m])
deltav_prometheus_writer_circuit_state         # 0=closed, 1=half_open, 2=open
deltav_prometheus_writer_node_context_cache_size
```

### Topics

- Consumes `deltav-timeseries` (group `prometheus-writer`)
- Consumes `deltav-node-context` (unique group per instance)
- Produces to `deltav-prometheus-writer-dlq` (poison-pill records only)

### Wire format frozen

As of Phase 2 GA, both `deltav-timeseries` and `deltav-node-context` protobuf
schemas are frozen. Only forward-compatible additions (new tag numbers) permitted.
```

- [ ] **Step 3: Full-reactor verify gate** (scar prophylactic #4)

```bash
./mvnw -B -DskipTests -fae clean install
```

Expected: every module builds. If any module fails with `cannot find symbol` referencing a Prometheus proto class, the fix is usually adding the `core/deltav-kafka-contracts` dependency to that module.

- [ ] **Step 4: Rebuild all services reminder** (scar prophylactic #5)

Confirm by running:

```bash
./opennms-container/delta-v/build.sh
```

Expected: all 13 daemon-boot jars + flow-enricher + **prometheus-writer** appear in the summary output.

- [ ] **Step 5: Commit**

```
git commit -m "docs(prometheus-writer): freeze wire contracts at Phase 2 GA + README

Both deltav-timeseries.proto and deltav-node-context.proto headers updated:
API version 1 FROZEN; only forward-compatible additions permitted.
Breaking changes require API v2 with deprecation cycle.

README: new Phase 2 section (profiles, config env vars, metric examples,
topic map, wire-freeze notice)."
```

---

## Task 26: Final acceptance gate — scar prophylactic checklist + PR

**Goal:** Before opening the PR, verify all 6 scar prophylactics are in place and passing. Then open the PR against `pbrane/delta-v`.

- [ ] **Step 1: Verify prophylactic #1 (real-main-class IT)**

```bash
./mvnw -pl core/prometheus-writer test -Dtest=PrometheusWriterApplicationScanIT
```
Expected: PASS. (Task 19.)

- [ ] **Step 2: Verify prophylactic #2 (yaml parity)**

Grep all `ApplicationContextInitializer`-style property overrides in the IT code and confirm each also appears in `src/main/resources/application.yml`:

```bash
grep -nE 'DynamicPropertyRegistry|TestPropertyValues' core/prometheus-writer/src/test/java | awk -F: '{print $1}' | sort -u
```

For each referenced property (`spring.kafka.bootstrap-servers`, `spring.cloud.stream.kafka.binder.brokers`, `prometheus-writer.remote-write.url`, etc.) confirm it's in `application.yml`.

- [ ] **Step 3: Verify prophylactic #3 (`use-native-encoding: true` on DLQ binding)**

```bash
grep -nE 'publishDlq-out-0' core/prometheus-writer/src/main/resources/application.yml
grep -nE 'use-native-encoding' core/prometheus-writer/src/main/resources/application.yml
```
Expected: both match, on adjacent lines under the DLQ binding.

- [ ] **Step 4: Verify prophylactic #4 (full-reactor clean install passes)**

```bash
./mvnw -B -DskipTests -fae clean install 2>&1 | tail -20
```
Expected: `BUILD SUCCESS`.

- [ ] **Step 5: Verify prophylactic #5 (rebuild-all reminder in PR template — done via PR body)**

The PR description MUST include:

```markdown
## Rebuild checklist
- [ ] All 13 daemon-boot jars built (`./opennms-container/delta-v/build.sh`)
- [ ] flow-enricher rebuilt
- [ ] prometheus-writer rebuilt
- [ ] Full-reactor `./mvnw clean install -DskipTests -fae` passes locally
- [ ] `test-prometheus-writer-e2e.sh` passes locally (lite + metrics-e2e)
```

- [ ] **Step 6: Verify prophylactic #6 (pipefail-safe grep in E2E)**

```bash
grep -nE '\| *awk.*END *\{print.*\+0\}' opennms-container/delta-v/test-prometheus-writer-e2e.sh | head -5
grep -nE '\|\| *true' opennms-container/delta-v/test-prometheus-writer-e2e.sh | wc -l
```
Expected: every counter-assertion grep has `|| true` guard (count should be ≥ 8).

- [ ] **Step 7: Run the full test suite one last time**

```bash
./mvnw -pl core/prometheus-writer test
./mvnw -pl core/prometheus-writer verify  # includes ITs
```
Expected: all green.

- [ ] **Step 8: Run the E2E one last time**

```bash
cd opennms-container/delta-v && ./test-prometheus-writer-e2e.sh
```
Expected: `ALL ASSERTIONS PASSED`.

- [ ] **Step 9: Push branch + open PR**

```bash
git push -u origin feature/kafka-ts-phase-2-prometheus-consumer
gh pr create --repo pbrane/delta-v --base develop \
    --title "feat: Kafka TS Phase 2 — Prometheus Remote Write consumer" \
    --body "$(cat <<'BODY'
## Summary
- New `core/prometheus-writer/` standalone Spring Boot consumer
- Consumes `deltav-timeseries`, enriches via `deltav-node-context` cache
- POSTs Prometheus Remote Write to configurable endpoint
- Resilience4j circuit breaker + SCS binding pause/resume
- DLQ topic for poison pills (400/413)
- Startup gate ensures cache bootstrapped before consumption resumes
- Both wire contracts frozen at Phase 2 GA

## Rebuild checklist
- [x] All 13 daemon-boot jars built
- [x] flow-enricher rebuilt
- [x] prometheus-writer rebuilt
- [x] Full-reactor `./mvnw clean install -DskipTests -fae` passes locally
- [x] `test-prometheus-writer-e2e.sh` passes locally

## Scar prophylactic verification
- [x] #1 Real-main-class IT (`PrometheusWriterApplicationScanIT`) passes
- [x] #2 Production yaml carries every Testcontainers-set property
- [x] #3 `use-native-encoding: true` on DLQ producer binding
- [x] #4 Full-reactor build green
- [x] #5 Rebuild-all reminder present (this checklist)
- [x] #6 Pipefail-safe `|| true` guards on all grep-driven assertions

## Test plan
- [x] Unit tests (all green, all new code covered)
- [x] SCS test-binder ITs
- [x] Testcontainers Kafka ITs (`NodeContextKafkaBootstrapIT`, `RwRoundTripIT`, `CircuitBreakerIT`)
- [x] Real-main-class IT
- [x] 6-step Docker Compose E2E (lite + metrics-e2e profiles)

## Spec + plan
- Spec: `docs/superpowers/specs/2026-04-17-kafka-ts-phase-2-prometheus-consumer-design.md`
- Plan: `docs/superpowers/plans/2026-04-17-kafka-ts-phase-2-prometheus-consumer.md` (+ `-part2.md`)
BODY
)"
```

- [ ] **Step 10: PR hygiene**

After PR opens:
- Link the PR in the design spec by replacing `<TBD-phase-2-PR>` in both proto headers with the actual PR number in a follow-up commit (`git commit -m "docs: update proto frozen-in header with Phase 2 PR number"`). Push.
- Update the `project_kafka_timeseries_pipeline.md` memory note after merge to reflect Phase 2 = DONE.

---

## Self-review

**Spec coverage check** (spec section → task):

| Spec section | Covered in task(s) |
|---|---|
| §Problem / §Non-goals | Plan preamble + Task 25 freeze |
| §Architecture data flow | Tasks 2, 15, 17, 22 |
| §Architecture key properties | Tasks 4, 7, 14, 16, 17 |
| §Module structure | Task 2 |
| §Key beans | Tasks 4-19 (one bean per task typically) |
| §1 NodeContextCache bootstrap | Tasks 4, 5 |
| §2 Startup gate | Tasks 6, 7 |
| §3 Metric naming + labels | Tasks 8, 9 |
| §4 Sample construction | Task 10 |
| §5 RW output pipeline | Tasks 11, 12, 13, 14 |
| §6 DLQ | Task 17 |
| §7 Observability | Task 18 (+ all producer-site tasks) |
| §8 Configuration | Task 3 |
| §9 Docker integration | Tasks 21, 22, 23 |
| §10 E2E script | Task 24 |
| Scar prophylactics | Tasks 3 (yaml parity), 17 (use-native-encoding), 19 (real-main-class), 24 (pipefail-safe grep), 25 (full-reactor), 26 (verify-all) |
| Testing strategy | Tasks 4-18 (unit + SCS binder + Testcontainers), Task 19 (real-main-class), Task 20 (full round-trip), Task 24 (E2E) |
| Schema freeze decision | Task 25 |
| Rollout | Task 22 (compose defaults) + Task 25 (README) |

**Placeholder scan:** `<TBD-phase-2-PR>` is the only placeholder, and Task 26 Step 10 resolves it after PR opens. No `TODO`/`FIXME`/"implement later" in any task body.

**Type consistency:** Bean names used downstream match the classes introduced upstream:
- `NodeContextCache`, `NodeContextKafkaBootstrap`, `NodeContextCacheHealthIndicator`, `NodeContextCacheReadyEvent` — Tasks 4, 5, 6
- `TimeseriesBindingStartupGate`, `TimeseriesBindingResumer` — Task 7
- `NameSanitizer`, `LabelBuilder`, `PromSample`, `TimeseriesToPromTranslator` — Tasks 8, 9, 10
- `WriteRequestBuilder`, `RemoteWriteHttpClient`, `RemoteWriteRetryPolicy`, `BatchingRwWriter`, `PrometheusWriterCircuitBreaker`, `ConsumerPauseListener` — Tasks 11, 12, 13, 14, 16
- `TimeseriesConsumer` bean name `timeseriesConsumer-in-0` — Task 15 + yaml in Task 3 + startup gate references in Task 7
- `DlqPublisher` + `publishDlq-out-0` + `deltavPrometheusWriterDlqTopic` — Task 17 + yaml in Task 3
- `PrometheusWriterMetrics` constants — Task 18 + all producer-site tasks reference them after Task 18's refactor step

**Scope:** Phase 2 is one focused implementation; the plan covers it in 26 tasks. No decomposition needed.
