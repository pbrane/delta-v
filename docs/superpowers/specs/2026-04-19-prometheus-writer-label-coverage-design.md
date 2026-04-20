# prometheus-writer Label Coverage — Design Spec

**Date:** 2026-04-19
**Branch:** `feat/prometheus-writer-label-coverage` (to be created off develop)
**Memo:** `project_prometheus_label_coverage_gaps`
**Predecessor PRs:** delta-v#174 (Phase 2 Prometheus RW consumer), delta-v#175 (NodeContextKafkaBootstrap race fix), delta-v#177 (Collectd ServiceParameters identity fix), delta-v#179 (Phase 2 E2E self-contained)

## Goal

Close two label-coverage gaps surfaced during M2 post-merge review:

- **Gap A:** Add the Prometheus-ecosystem `instance` label so Grafana dashboards and alert templates can use the standard `{{$labels.instance}}` idiom without each operator having to know Delta-V's non-standard convention.
- **Gap B (reduced scope):** Add the always-on `foreign_id` label, populate a sensible default top-level metadata allowlist (`snmp:sysContact`, `snmp:sysLocation`), and ship a cardinality observability metric so operators can see what the new labels are doing to their VictoriaMetrics series count.

Interface-scoped enrichment (`ifAlias`/`ifDescr`) is **deferred** to a follow-up PR — see Out of Scope.

## Scope

### In Scope

- New `instance` label, configurable source, default `node_label`, deterministic `node:{node_id}` fallback whenever the source produces an empty string.
- New `foreign_id` label, always emitted from `NodeContext.foreignId` (or `""` when NodeContext absent).
- New default for `prometheus-writer.labels.from-metadata` — populated as `[snmp:sysContact, snmp:sysLocation]` instead of `[]`.
- New `LabelCardinalityTracker` exposing two Micrometer meters at `/actuator/prometheus`:
  - `deltav_prometheus_writer_distinct_series` — Gauge backed by capped Caffeine cache. Toggled by `cardinality-tracking.enabled`.
  - `deltav_prometheus_writer_labels_per_sample` — DistributionSummary. Always emitted (its overhead is a single atomic increment).
- New configuration: `prometheus-writer.metrics.cardinality-tracking.{enabled,cap}` defaulting to `true` / `100000` (gates only the cache+gauge).
- Documentation comments in `application.yml` warning operators about the cardinality cost of expanding `from-metadata`.
- Unit tests for every new path; extension of `RwRoundTripIT` to assert the new labels round-trip end-to-end through VictoriaMetrics.

### Out of Scope

- `deltav-node-context.proto` contract changes. The contract author signposted forward-compatible additions (`InterfaceContext` field 2+) but the session-level guardrail defers any contract motion to a separate work item.
- Interface-scoped metadata wiring (`ifAlias`, `ifDescr`, `ifName`). These columns live on `OnmsSnmpInterface`, which is not currently surfaced in `NodeContext` at all. The proto's existing `interface_metadata` map is keyed by IP and carries `OnmsIpInterface.getMetaData()` (operator-set IP-scoped metadata) — it is structurally the wrong field for SNMP-interface-keyed column data. Doing this properly requires (a) a `snmp_interface_metadata` map in `NodeContext` keyed by ifIndex, (b) a `NodeToProtobufTranslator` change in provisiond to populate it from `OnmsSnmpInterface` columns, and (c) the LabelBuilder consumer wiring. That trio is a coherent follow-up PR; partial wiring this round would either misuse the existing field or hard-code a bridge that the follow-up has to delete.
- Service-scoped metadata wiring (`service_metadata`). Same reasoning: defer until the contract direction is settled.
- `__name__` / `__tmp__` / leading-underscore reserved-name protection in `NameSanitizer`. Pre-existing risk, not introduced by this PR. Document as known limitation.
- Performance regression test. Added per-call work is sub-microsecond; no measurable regression expected.
- E2E test against live Collectd → Kafka → prometheus-writer pipeline. The existing Phase 2 E2E in `tools/labbox/` continues to pass with new labels; not extended in this PR.

## Background

`LabelBuilder.build()` (file `core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/translate/LabelBuilder.java`) currently emits 9 labels per sample: `node_id`, `location`, `node_label`, `foreign_source`, `categories`, `resource_type`, `resource_instance`, `collection_package`, `producer`, plus zero-or-more allowlisted top-level metadata keys (default allowlist is `[]`).

Two ecosystem-compatibility and operator-utility gaps:

1. **No `instance` label.** Prometheus has no hard requirement, but the ecosystem convention is overwhelming: Grafana templates, alert messages, and SRE muscle memory all reach for `{{instance}}` first. Delta-V emits `node_id` and `node_label` but not `instance` — every dashboard author has to learn the local convention.
2. **NodeContext under-consumed.** The proto carries `foreign_id` (stable identifier within a foreign source) but LabelBuilder doesn't read it. The default top-level metadata allowlist is empty, so `snmp:sysContact` and `snmp:sysLocation` (universally-low-cardinality fields) silently never appear as labels unless each operator adds them to their override yaml.

Cardinality is a real concern for both gaps. New labels multiply the distinct-series count VictoriaMetrics has to index. Default expansion needs an observability metric so operators can see the impact.

The label set is multi-resource by design — each `(TimeseriesBatch, Resource)` pair gets its own label map, with `resource_type` + `resource_instance` already disambiguating sub-node resources (interfaces, storage, generic indexed). The new labels (`instance`, `foreign_id`) are scoped to the node and DO NOT need per-resource semantics.

## Design

### 1. Component Architecture

Three new files, one modified component, two modified configuration sites.

```
core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/
├── translate/
│   ├── LabelBuilder.java                    (MODIFIED)
│   ├── InstanceLabelResolver.java           (NEW)
│   ├── InstanceSource.java                  (NEW — public enum)
│   └── NameSanitizer.java                   (unchanged)
├── metrics/
│   └── LabelCardinalityTracker.java         (NEW)
└── config/
    └── PrometheusWriterProperties.java      (MODIFIED)

core/prometheus-writer/src/main/resources/
└── application.yml                          (MODIFIED)
```

Component graph:

```
TimeseriesToPromTranslator (existing)
        │
        ▼
LabelBuilder.build(batch, resource, nc, allowlist) → Map<String,String>
        │
        ├── InstanceLabelResolver.resolve(batch, nc) → String   (NEW)
        ├── NameSanitizer.sanitize(metaKey)                     (existing)
        └── LabelCardinalityTracker.record(labels)              (NEW, fire-and-forget)
                  │
                  ├── Caffeine<String, Object> (capped)
                  └── Micrometer:
                        ├── deltav_prometheus_writer_distinct_series   (Gauge)
                        └── deltav_prometheus_writer_labels_per_sample (DistributionSummary)
```

### 2. `InstanceLabelResolver`

```java
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

The `node:{node_id}` fallback guarantees `instance` is never the empty string — `{instance=""}` makes Grafana templating unhappy, and the operator-friendly default is "always have something to display."

### 3. `InstanceSource` enum

```java
package org.deltav.prometheus.writer.translate;

public enum InstanceSource { NODE_LABEL, FOREIGN_ID, NODE_ID }
```

Public for Spring Boot configuration binding.

### 4. `LabelCardinalityTracker`

```java
@Component
public class LabelCardinalityTracker {
    private static final Logger LOG = LoggerFactory.getLogger(LabelCardinalityTracker.class);

    private final boolean enabled;
    private final Cache<String, Object> distinctLabelsets;   // null when disabled
    private final DistributionSummary labelsPerSample;       // always present
    private final RateLimitedLogger errorLogger;             // 1 WARN/min

    public LabelCardinalityTracker(PrometheusWriterProperties props, MeterRegistry registry) {
        var cfg = props.metrics().cardinalityTracking();
        this.enabled = cfg.enabled();
        if (enabled) {
            this.distinctLabelsets = Caffeine.newBuilder()
                    .maximumSize(cfg.cap())
                    .build();
            Gauge.builder("deltav_prometheus_writer_distinct_series",
                          distinctLabelsets, c -> c.estimatedSize())
                 .description("Distinct label tuples observed since startup, capped at "
                              + cfg.cap() + ". Sustained reading at cap means cardinality "
                              + "exceeds budget — prune labels.from-metadata.")
                 .register(registry);
        } else {
            this.distinctLabelsets = null;
        }
        this.labelsPerSample = DistributionSummary.builder("deltav_prometheus_writer_labels_per_sample")
                .description("Number of labels emitted per Prometheus sample.")
                .register(registry);
        this.errorLogger = new RateLimitedLogger(LOG, Duration.ofMinutes(1));
    }

    public void record(Map<String, String> labels) {
        if (!enabled) {
            labelsPerSample.record(labels.size());
            return;
        }
        try {
            distinctLabelsets.put(canonicalize(labels), PRESENT);
            labelsPerSample.record(labels.size());
        } catch (Throwable t) {
            errorLogger.warn("LabelCardinalityTracker.record() failed; tracker degraded but writer continues", t);
        }
    }

    private static String canonicalize(Map<String, String> labels) {
        return labels.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining("\n"));
    }

    private static final Object PRESENT = new Object();
}
```

`RateLimitedLogger` is a small inline helper (or pulled from any existing project utility if one exists; check first).

**Critical guarantee:** `record()` MUST NOT throw. The catch is non-negotiable — observability is best-effort, sample shipping is not.

### 5. `LabelBuilder` modifications

```java
@Component
public class LabelBuilder {
    private final NameSanitizer sanitizer;
    private final InstanceLabelResolver instanceResolver;       // NEW
    private final LabelCardinalityTracker tracker;              // NEW

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
        labels.put("instance", instanceResolver.resolve(batch, nc));        // NEW
        labels.put("location", nullToEmpty(batch.getLocation()));
        labels.put("node_label", nc.map(NodeContext::getNodeLabel).orElse(""));
        labels.put("foreign_source", nc.map(NodeContext::getForeignSource).orElse(""));
        labels.put("foreign_id", nc.map(NodeContext::getForeignId).orElse(""));   // NEW
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

        tracker.record(labels);                                              // NEW
        return labels;
    }

    // producerLabel and nullToEmpty unchanged
}
```

`instance` is positioned as the second label (after `node_id`) so it appears prominently in any debug print of the label map. Order does not affect Prometheus/VictoriaMetrics behavior — it's purely a readability nicety.

### 6. `PrometheusWriterProperties` modifications

```java
public record PrometheusWriterProperties(
        @NotNull RemoteWrite remoteWrite,
        @NotNull Batch batch,
        @NotNull Retry retry,
        @NotNull CircuitBreaker circuitBreaker,
        @NotNull Labels labels,
        @NotNull Metrics metrics,                  // NEW
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

    // ... existing nested records unchanged ...

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
}
```

### 7. `application.yml` modifications

Append to existing `prometheus-writer:` block:

```yaml
prometheus-writer:
  # ... existing remote-write, batch, retry, circuit-breaker ...

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
    # cardinality. A key with ≤10 distinct values across the deployment is
    # essentially free. Watch the deltav_prometheus_writer_distinct_series gauge
    # to validate. The two defaults below are universally low-cardinality on
    # SNMP-managed devices.
    from-metadata:
      - snmp:sysContact
      - snmp:sysLocation

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
      # allowlist." Sized for ~1000 nodes × 50 resources per node × ~2x slack.
      cap: 100000

  # ... existing startup-gate ...
```

### 8. Final per-sample label set

After this PR, every sample emitted carries:

| Label | Source | Cardinality character |
|---|---|---|
| `node_id` | `batch.node_id` | one per node |
| `instance` | `InstanceLabelResolver.resolve()` | depends on source — `node_label` or `foreign_source:foreign_id` |
| `location` | `batch.location` | very low |
| `node_label` | `nc.nodeLabel` | one per node, mutable |
| `foreign_source` | `nc.foreignSource` | very low |
| `foreign_id` | `nc.foreignId` | one per node |
| `categories` | sorted, comma-joined | low |
| `resource_type` | `resource.type` | very low |
| `resource_instance` | `resource.instance` | medium per node |
| `collection_package` | `batch.collectionPackage` | very low |
| `producer` | enum, lowercased | very low |
| `snmp_sysContact` | metadata allowlist | low (typically ≤ team-count) |
| `snmp_sysLocation` | metadata allowlist | depends on operator string discipline |

**Default count: 13 labels per sample** (up from 9 today). `node_label` and `instance` are intentional duplicates when `instance-source=NODE_LABEL` so consumers can pick which they prefer.

## Edge Cases & Error Handling

| Scenario | Detection | Response |
|---|---|---|
| `nc` is `Optional.empty()` (NodeContext not yet bootstrapped from compacted topic) | Existing branch in `LabelBuilder` | All NC-derived labels = `""`; `instance` falls through to `"node:{node_id}"`. Same handling as today. |
| Configured `instance-source` enum is null (binding edge case) | `@NotNull` on `Labels.instanceSource` | Spring Boot config validation fails fast at startup. |
| `nc.nodeLabel` empty when `instance-source=NODE_LABEL` | `InstanceLabelResolver` switch | Falls back to `"node:{node_id}"`. Tested. |
| `instance-source=FOREIGN_ID` but `foreign_source` or `foreign_id` is empty | `InstanceLabelResolver` switch | Falls back to `"node:{node_id}"`. Tested. |
| `LabelCardinalityTracker.record()` throws | try/catch in tracker | Rate-limited WARN log (≤1/min); writer continues. **Tracker failure must NEVER fail the sample.** Tested by injecting throwing Caffeine. |
| Cardinality cap reached | Caffeine evicts silently | Gauge reports `estimatedSize() == cap`. The "stuck at cap" reading IS the alert signal. Tested. |
| Two metadata keys sanitize to the same label name | Not detected | Last write wins (pre-existing `LinkedHashMap.put` behavior). Documented as known limitation; not in scope to fix. |

## Performance

`LabelBuilder.build()` is called per `(batch, resource)` pair, with SCS consumer concurrency = 4 by default. Conservative estimate at scale: 1000 nodes × 50 resources × 5min collection interval = ~170 calls/sec aggregate.

Per-call added work:

| Operation | Cost |
|---|---|
| `InstanceLabelResolver.resolve()` | one switch + 0-2 string concats — negligible |
| `foreign_id` label put | one map.put — negligible |
| `LabelCardinalityTracker.record()` (enabled) | one canonicalization (sort + StringBuilder) + Caffeine put + DistributionSummary record — ~500 ns/call estimate |

At 170 calls/sec the tracker overhead is ~85 µs/sec — irrelevant. Profiling not required.

**Disabled-tracker overhead:** When `cardinality-tracking.enabled: false`, `record()` is one boolean check + DistributionSummary record. Zero additional allocation. Verified by test.

**Memory:** Default cap = 100k entries × ~200-400 bytes/entry ≈ 20-40 MB. Acceptable for the writer process (typically 1-2 GB heap).

**Concurrency:** All new components are stateless except `LabelCardinalityTracker`, whose mutable state is a thread-safe Caffeine cache + thread-safe Micrometer DistributionSummary. No new locks.

## Testing Strategy

### New unit test files

**`InstanceLabelResolverTest.java`** — one method per branch:
- `nodeLabelSource_returnsNodeLabel`
- `nodeLabelSource_emptyNodeLabel_fallsBackToNodeId`
- `nodeLabelSource_absentNodeContext_fallsBackToNodeId`
- `foreignIdSource_bothPresent_concatenates`
- `foreignIdSource_emptyForeignSource_fallsBackToNodeId`
- `foreignIdSource_emptyForeignId_fallsBackToNodeId`
- `foreignIdSource_absentNodeContext_fallsBackToNodeId`
- `nodeIdSource_alwaysReturnsNodeIdForm`

**`LabelCardinalityTrackerTest.java`**:
- `enabled_recordsDistinctLabelsetsToGauge`
- `enabled_duplicateLabelsetCountsOnce`
- `enabled_capExceeded_evictsAndStaysAtCap` (cap=2, record 5 distinct → gauge reads 2)
- `enabled_recordsLabelCountToDistributionSummary`
- `disabled_recordIsNoOpForGauge_butStillRecordsDistributionSummary`
- `recordSurvivesInternalThrow` (inject throwing Caffeine)
- `canonicalizationOrderIndependent` (same labels different insertion order → 1 entry)

### Existing unit test extensions

**`LabelBuilderTest.java`**:
- New: `instance_label_default_uses_node_label`
- New: `instance_label_falls_back_when_node_label_empty`
- New: `foreign_id_always_emitted`
- Modified: `default_labels_always_emitted` — `hasSize(11)` (instead of 9), keyset adds `instance` and `foreign_id`
- Modified: setUp builds `LabelBuilder(sanitizer, instanceResolver, noOpTracker)` — use a real `LabelCardinalityTracker` configured `enabled=false` with a `SimpleMeterRegistry`

**`PrometheusWriterPropertiesTest.java`**:
- New: `defaults_include_new_instance_source_and_metadata_keys`
- New: `defaults_include_metrics_cardinality_tracking_enabled_with_default_cap`

### Integration test extension

**`RwRoundTripIT.java`** — already round-trips a fabricated batch through the consumer to a real VictoriaMetrics testcontainer:
- Mock `NodeContext` builder gains `setForeignId("server-01")` and `putMetadata("snmp:sysContact", "noc@example.com")`.
- New assertion: query VictoriaMetrics for the test metric, parse returned labels, assert presence and value of `instance`, `foreign_id`, `snmp_sysContact`.

### Verification gates

- `./mvnw -pl core/prometheus-writer verify` — green.
- `./mvnw -B -DskipTests -fae clean install` — full-reactor green (per `feedback_delta_v_full_reactor_verify`).

## Backward Compatibility

| Change | Impact |
|---|---|
| New `instance` label | Additive. No existing query breaks. Grafana dashboards built on the old label set get a free upgrade. |
| New `foreign_id` label | Additive. No existing query breaks. |
| Default `from-metadata` changes from `[]` to `[snmp:sysContact, snmp:sysLocation]` | **Behavior change at upgrade.** Existing dashboards continue to work (no labels removed). New labels appear, which silently increases cardinality. Operators wanting zero metadata labels must explicitly set `from-metadata: []`. **PR description must call this out as the one upgrade-time behavior change.** |
| New `cardinality-tracking` meters | Additive. New endpoint output, no existing meters changed. |
| Existing `node_id`, `node_label`, etc. preserved unchanged | No breaking label change. Downstream correlators and event-based alerting that key on `node_id` continue working as-is. |

## Success Criteria (from session prompt, with scope reduction)

- [x] `instance` label emitted by default, sourced from `node_label` with a configurable override.
- [x] `foreign_id` added as a standard label (no configuration needed).
- [ ] ~~Interface-scoped metadata allowlist~~ — DEFERRED to follow-up PR (see Out of Scope).
- [ ] ~~Wire one standard interface metadata key (`snmp:ifAlias` or `snmp:ifDescr`)~~ — DEFERRED.
- [x] Bounded sensible default for `prometheus-writer.labels.from-metadata` = `[snmp:sysContact, snmp:sysLocation]`.
- [x] `deltav_prometheus_writer_distinct_series` (and `deltav_prometheus_writer_labels_per_sample`) emitted at `/actuator/prometheus` and documented.
- [x] Unit tests for every new label path.
- [x] IT extension validating end-to-end that new labels land on the wire to VictoriaMetrics.
- [x] `./mvnw -pl core/prometheus-writer verify` green.
- [x] `./mvnw -B -DskipTests -fae clean install` full-reactor green.
- [x] PR opened `--repo pbrane/delta-v --base develop`. Title `feat(prometheus-writer):` per the adjacent convention.
- [x] Post-merge: flip `project_prometheus_label_coverage_gaps` memo status OPEN → PARTIALLY RESOLVED (delta-v#<PR>), with explicit note that interface/service metadata gap remains open and references the follow-up.

## After This PR

Queued follow-ups (not in scope here):

- **`snmp_interface_metadata` proto field + producer + consumer wiring.** Three-component change: (a) add `map<string, SnmpInterfaceContext> snmp_interface_metadata = 12;` to `NodeContext` keyed by ifIndex (string), (b) extend `NodeToProtobufTranslator` in provisiond to populate from `OnmsSnmpInterface` columns (`ifAlias`, `ifDescr`, `ifName`, `ifSpeed`), (c) extend `LabelBuilder` to look up by `resource.instance` for resources whose `resource.type` indicates SNMP interface scope. Includes a separate `interface-metadata-from` allowlist property.
- **Service-scoped metadata wiring.** Symmetric to the interface follow-up; lower priority.
- **Reserved-name protection in `NameSanitizer`.** Prevent operator from accidentally configuring a metadata key whose sanitization collides with `__name__` etc.
- **Grafana dashboard bundle.** A committed JSON dashboard in `opennms-container/delta-v/grafana-dashboards/` that exercises the new label set, making "Grafana-friendly defaults" reproducible.
