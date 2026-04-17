# Kafka Time Series Pipeline — Phase 2: Prometheus Remote Write Consumer

**Date:** 2026-04-17
**Status:** Approved (pending implementation plan)
**Owner:** David Hustace
**Predecessors:**
- `2026-04-15-kafka-time-series-producer-design.md` — Phase 0 (Collectd producer, shipped delta-v#169 / #170)
- `2026-04-16-kafka-time-series-phase-1-node-context-design.md` — Phase 1 (Provisiond node-context producer, shipped delta-v#171)

**Related memories:**
- `project_kafka_timeseries_pipeline.md` — overall pipeline map (Phase 0/1/2+ status)
- `project_kafka_timeseries_producer_next_session.md` — Phase 0 outcomes + E2E evidence
- `project_node_context_phase1_done.md` — Phase 1 outcomes, topic config, E2E-discovered bugs
- `feedback_spring_boot_scan_package_trap.md` — mandatory real-main-class IT to avoid silent scan-package gaps
- `feedback_spring_cloud_stream_splitter.md` — `use-native-encoding: true` on byte-array SCS bindings
- `feedback_delta_v_full_reactor_verify.md` — full-reactor verify after cross-module changes, not just `-pl`
- `project_dropwizard_prometheus_bridge_pattern.md` — horizon-lib Dropwizard → Prom bridge (not needed here, but cross-referenced)
- `feedback_deltav_package_namespace.md` — `org.deltav.*` + BeaconStrategists copyright for new code
- `feedback_never_pr_opennms.md` — PRs go to `pbrane/delta-v`, never `OpenNMS/opennms`

## Problem

Phase 0 shipped the `deltav-timeseries` producer (Collectd emits protobuf `TimeseriesBatch` records per poll cycle). Phase 1 shipped the `deltav-node-context` producer (Provisiond emits compacted `NodeContext` records on node-lifecycle UEIs plus restart bootstrap). Both topics are live on `develop`.

**No consumer exists.** `deltav-timeseries` is retention-capped at 24 hours, so any record older than that is lost. Without a consumer, the pipeline does nothing useful: Collectd's metrics hit Kafka and decay unseen; Provisiond's node-context changes land in a compacted topic that no one reads.

Phase 2 is the first consumer: a Spring Boot service that

1. consumes `deltav-timeseries` (metric payloads, 16 partitions, 24h retention),
2. enriches each batch with node identity from a materialized view of `deltav-node-context` (compacted, 8 partitions),
3. transforms the enriched batch into Prometheus Remote Write samples,
4. POSTs Snappy-compressed protobuf batches to a configurable RW endpoint (Mimir / Cortex / VictoriaMetrics / Thanos Receive / vanilla Prometheus with `--web.enable-remote-write-receiver`),
5. handles backpressure, retries, and poison pills in a bounded, observable way.

Once Phase 2 ships, the pipeline is end-to-end useful: a real operator can point the writer at their TSDB, run `rate(opennms_mib2_interface_errors_ifindiscards_total{node_label="eth0.site-a"}[5m])`, and see live delta-v metrics in Grafana.

## Non-goals

Explicitly out of scope for this phase:

- **Staleness markers on node delete.** Prometheus RW supports `StaleNaN` but requires series-per-node state. Phase 2 relies on RW target retention to age out deleted-node series. Revisit as a dedicated `staleness-emitter` consumer if operators request immediate cleanup.
- **Schema changes to either wire contract.** Phase 2 uses the existing `TimeseriesBatch` and `NodeContext` protos as shipped in Phase 0/1. Both contracts **freeze at Phase 2 GA** — only forward-compatible additions (new tag numbers) permitted afterward. This includes not adding `hostname`, `ip-primary`, `ifIndex→IP map`, or similar fields that would enable richer enrichment.
- **COUNTER32 wrap detection.** Modern interfaces use HC/COUNTER64 via `collectd-configuration.xml`. Phase 2 emits COUNTER as Prometheus counters and trusts `rate()`'s standard reset-tolerant math. Sub-1G interfaces running COUNTER32 at high throughput can see wraps that look like resets — documented, not fixed in Phase 2.
- **Multiple RW targets / fan-out.** One writer instance → one RW endpoint. Operators who want fan-out run Mimir / VictoriaMetrics / Thanos Receive in distributor mode upstream of Phase 2.
- **mTLS for RW target.** Bearer, basic, and extra-headers auth cover mainstream deployments. mTLS deferred until a real operator need surfaces.
- **String-attribute emission as info metrics.** String attributes (`ATTRIBUTE_TYPE_STRING`) are dropped + counted. Operators who need string data as labels promote them via the metadata-label opt-in (see Architecture §5.2).
- **Interface-level enrichment.** `NodeContext.interface_metadata` is IP-keyed; SNMP samples are ifIndex/ifName-keyed. Joining them requires either a new field in the schema (frozen — see above) or an IP-lookup axis that doesn't exist. Phase 2 enriches at the node level only.
- **Binary-search split on 400 responses.** Poison-pill batches go to a DLQ topic wholesale. Operator investigates from the DLQ; we do not attempt per-sample isolation in the writer.
- **`NodeContextCache` as a reusable library.** Phase 2 implements it inline in `core/prometheus-writer/`. When consumer #2 (Thresholder) lands, extract to `core/deltav-node-context-cache` as a shared starter. YAGNI for Phase 2 of one consumer.
- **Compose profile `metrics`.** Phase 2 adds `prometheus-writer` to existing `[lite, full]` profiles and introduces a new `[metrics-e2e]` profile for the test-only VictoriaMetrics container. No production-facing profile changes.

## Architecture

### Data flow

```
┌─────────────────┐                                ┌──────────────────┐
│  Collectd       │  deltav-timeseries             │  RW target       │
│  (+ future      │  16 parts, 24h retention   ┌─▶│  (VM in E2E;     │
│   producers)    │─────────────────────┐       │  │   prod swaps URL)│
└─────────────────┘                     │       │  └──────────────────┘
                                        ▼       │           ▲
                             ┌────────────────────────┐     │ POST
                             │ core/prometheus-writer  │     │ /api/v1/write
                             │ (Spring Boot 4, SCS)    │     │
                             │                         │     │
┌─────────────────┐          │ ┌─────────────────────┐ │     │
│  Provisiond     │ deltav-  │ │ TimeseriesBatch     │ │     │
│  change-feed    │ node-    │ │ consumer (paused    │ │     │
│  (Phase 1)      │ context  │ │  until cache ready) │ │     │
└─────────────────┘ 8 parts  │ └──────────┬──────────┘ │     │
                   compacted │            │ enrich      │     │
                   ────────▶│            ▼             │     │
                             │ ┌─────────────────────┐ │     │
                             │ │ NodeContextCache    │ │     │
                             │ │ Map<key,NodeContext>│ │     │
                             │ │ tails compacted topic│ │     │
                             │ └──────────┬──────────┘ │     │
                             │            │            │     │
                             │            ▼            │     │
                             │ ┌─────────────────────┐ │     │
                             │ │ Translator          │ │     │
                             │ │ TSBatch → PromSample│ │     │
                             │ │ (sanitize, filter,  │ │     │
                             │ │  build labels)      │ │     │
                             │ └──────────┬──────────┘ │     │
                             │            ▼            │     │
                             │ ┌─────────────────────┐ │     │
                             │ │ BatchingRwWriter    │ │     │
                             │ │  1000/1MB/1s        │ │     │
                             │ │  + Resilience4j CB  │ │     │
                             │ │  + tiered retry     │ │     │
                             │ └──────┬──────┬───────┘ │     │
                             │        │ 2xx  │ 4xx-    │     │
                             │        │      │ poison  │     │
                             └────────┼──────┼─────────┘     │
                                      │      │               │
                                      │      ▼               │
                                      │      deltav-prometheus-writer-dlq
                                      │      (new, 16 parts, 7d retention)
                                      ▼
                                      /actuator/prometheus
                                      (deltav_prometheus_writer_*)
```

### Key properties

1. **Stateless hot path.** Translation from `TimeseriesBatch` to Prometheus samples is a pure function of `(batch, NodeContextCache.get(key))`. No per-series state, no wrap detection, no staleness tracking. Restart-safe by construction.
2. **Startup gate.** The `deltav-timeseries` consumer binding starts paused. The `NodeContextCache` bootstraps (tail to end-offset across all 8 partitions), then the binding resumes. Post-gate, any `enrichment_missing` miss is genuine drift, not warmup.
3. **Lazy tombstone handling.** `NodeContext { deleted=true }` → `cache.remove(key)`. Subsequent lookups for the tombstoned node take the drop-+-counter path as "missing." No staleness markers emitted to the RW target.
4. **Backpressure via Kafka lag.** On RW target outage, Resilience4j opens the circuit and pauses the Kafka consumer binding. Kafka's 24h source-topic retention becomes the outage budget. On target recovery, the circuit closes and the binding resumes; consumer lag drains naturally.
5. **Poison-pill isolation via DLQ.** 400 / 413 HTTP responses mean "the consumer produced something the target won't accept." The source `TimeseriesBatch` goes to `deltav-prometheus-writer-dlq` with diagnostic headers, the consumer continues. Operator replays from DLQ in a debug tool to find the producer bug.

### Module structure

**New module:** `core/prometheus-writer/`

Mirrors `core/flow-enricher/` conventions:

- Maven artifactId: `org.deltav.core.prometheus-writer`
- Package root: `org.deltav.prometheus.writer`
- Main class: `org.deltav.prometheus.writer.PrometheusWriterApplication`
  - `@SpringBootApplication(scanBasePackages = {"org.deltav.prometheus.writer"})`
  - (Scan-package lesson from Phase 0 #170 baked in from day one.)
- Copyright header: BeaconStrategists, Inc. (new code, no horizon FQN constraint)
- Depends on `core/deltav-kafka-contracts` for generated `TimeseriesBatch` and `NodeContext` classes
- Adds a vendored Prometheus remote-write proto (`prometheus/prompb/remote.proto` + `types.proto`) to `core/deltav-kafka-contracts/src/main/proto/` so the generator produces `WriteRequest`, `TimeSeries`, `Sample`, `Label` Java classes

### Key beans

Organized by subsystem; each bean has one clear responsibility:

**Node-context subsystem:**
- `NodeContextCache` — `ConcurrentHashMap<String, NodeContext>`, hot-path `Optional<NodeContext> get(String key)`
- `NodeContextKafkaBootstrap` — owns the Kafka consumer for `deltav-node-context`, seeks to earliest on first run, records end-offset HWM per partition, drives the cache to "ready" when HWM reached on all 8 partitions
- `NodeContextCacheHealthIndicator` — `HealthIndicator` returning `UP` iff cache is ready; ties to Spring Boot `/actuator/health/readiness`

**Enrichment + translation subsystem:**
- `TimeseriesConsumer` — SCS `@Bean Function<Message<byte[]>, Void>` bound to `deltav-timeseries`, deserializes `TimeseriesBatch`, looks up cache, delegates to `TimeseriesToPromTranslator`, forwards samples to `BatchingRwWriter`
- `TimeseriesToPromTranslator` — pure function `(TimeseriesBatch, Optional<NodeContext>) → List<PromSample>`; applies filters (string attributes dropped, unspecified types dropped, enrichment misses rejected)
- `NameSanitizer` — lowercase + non-alphanumeric → `_` + collapse consecutive + strip leading digit; startup-time collision detection with WARN log
- `LabelBuilder` — assembles the label set per Q4b (core identity labels always, metadata labels from allowlist config)

**RW output subsystem:**
- `BatchingRwWriter` — accumulates samples in memory, flushes on 1000 / 1 MB / 1000 ms (configurable), delegates to `RemoteWriteHttpClient`
- `RemoteWriteHttpClient` — Spring `RestClient`-based, Snappy-compresses the `WriteRequest` protobuf, POSTs to configured URL with auth headers
- `RemoteWriteRetryPolicy` — tiered retry classifier: 5xx/429/network → retry with exp backoff + jitter; 400/413 → DLQ + drop; 401/403/404 → open circuit
- `PrometheusWriterCircuitBreaker` — Resilience4j `CircuitBreaker` bean wired to the HTTP client; opens on 50% failure rate over 20 requests, stays open 30s, 3 probe calls in half-open
- `ConsumerPauseListener` — listens to circuit state transitions; pauses/resumes the `deltav-timeseries` binding via `BindingsLifecycleController`

**DLQ subsystem:**
- `DlqPublisher` — SCS producer binding `publishDlq-out-0` → `deltav-prometheus-writer-dlq`, `producer.use-native-encoding: true` for byte-array payloads (Phase 1.5 SCS splitter lesson)

**Observability subsystem:**
- `PrometheusWriterMetrics` — Micrometer `MeterRegistry` holder; all counters/gauges/timers created here, injected into the beans that increment them. No Dropwizard bridge needed (no horizon-lib code paths in the hot path).

## Detailed design

### 1. NodeContextCache bootstrap protocol

On `@PostConstruct`:

```
endOffsetsAtStart = kafkaConsumer.endOffsets(allPartitions)  # 8 partitions
kafkaConsumer.seekToBeginning(allPartitions)
while anyPartitionBelowHWM:
    records = kafkaConsumer.poll(timeout=5s)
    for r in records:
        if r.value.deleted:
            cache.remove(r.key)
        else:
            cache.put(r.key, r.value)
        if r.offset >= endOffsetsAtStart[r.partition]:
            mark partition as caught up
cacheReady.set(true)
record bootstrap_duration_seconds
# continue consuming live updates in a background thread
```

- Deterministic: every writer instance reads the whole compacted topic on startup
- Bounded time: at 10k nodes (delta-v scale ceiling), topic is <10 MB compressed; bootstrap completes in seconds
- No RocksDB, no state directory, no Docker volume
- Restart-safe: cache is always rebuilt from the topic; never persisted

### 2. Startup gate mechanism

- `@ConditionalOnProperty(value = "prometheus-writer.startup-gate.enabled", havingValue = "true", matchIfMissing = true)` — defaults ON, off only for pathological tests
- `ListenerContainerCustomizer<AbstractMessageListenerContainer>` bean that calls `container.pause()` immediately after factory creates it
- `@EventListener(NodeContextCacheReadyEvent.class)` bean calls `BindingsLifecycleController.resume("timeseriesConsumer-in-0")`
- Spring Boot readiness: `NodeContextCacheHealthIndicator` contributes to `/actuator/health/readiness`; probe goes green in concert with binding resume
- Liveness (`/actuator/health/liveness`) is always green once the process is up — cache bootstrap is expected to succeed on any running Kafka

### 3. Metric naming + labels

**Metric name construction:**
```
opennms_{sanitize(group.name)}_{sanitize(attribute.name)}[_total if COUNTER]
```

Examples:
- `mib2-interface-errors.ifInDiscards` (COUNTER) → `opennms_mib2_interface_errors_ifindiscards_total`
- `mib2-X-interfaces.ifHighSpeed` (GAUGE) → `opennms_mib2_x_interfaces_ifhighspeed`
- `hrStorage.hrStorageUsed` (GAUGE) → `opennms_hrstorage_hrstorageused`

**Default label set (always emitted):**
- `node_id` — from `TimeseriesBatch.node_id`, stringified
- `location` — from `TimeseriesBatch.location`
- `node_label` — from `NodeContext.node_label`
- `foreign_source` — from `NodeContext.foreign_source`
- `categories` — from `NodeContext.categories`, sorted + comma-joined (e.g. `"critical,production"`); empty string if none
- `resource_type` — from `Resource.type`
- `resource_instance` — from `Resource.instance` (empty for non-tabular resources)
- `collection_package` — from `TimeseriesBatch.collection_package`
- `producer` — from `TimeseriesBatch.producer` enum (e.g. `"collectd"`)

**Opt-in metadata labels (via config allowlist):**
```yaml
prometheus-writer:
  labels:
    from-metadata:
      - requisition:region
      - requisition:environment
      - snmp:sysLocation
```
Each listed key becomes a label named by its sanitized form (`requisition:region` → `requisition_region`). Missing keys emit empty-string label values. Operators explicitly scope cardinality.

**Labels NOT emitted by default:**
- `foreign_id` — redundant with `node_id` within a `foreign_source`, rarely queried
- `resource_id` — derivable from `resource_type` + `resource_instance` + `node_id`, bulky label value
- Arbitrary metadata — cardinality hazard without allowlist

### 4. Sample construction

For each `Attribute` in each `AttributeGroup` in each `Resource` in the batch:

```
if attribute.type == STRING:
    samples_dropped_total{reason="string_attribute"}++; continue
if attribute.type == UNSPECIFIED:
    samples_dropped_total{reason="type_unspecified"}++; continue

name = buildMetricName(group.name, attribute.name, attribute.type)
labels = buildLabels(batch, resource, nodeContext)
sample = PromSample{
    name,
    labels,
    value = attribute.value.numeric,
    timestamp_ms = batch.timestamp_ms,    # collection time, not processing time
}
emit(sample)
```

### 5. RW output pipeline

**Batching:**
- In-memory `List<PromSample>` with size + byte-count tracked
- Flush triggers: `samples.size() >= 1000` OR `bytesUncompressed >= 1_048_576` OR time-since-first-sample >= `1000 ms`
- Flush task scheduled via Spring `@Scheduled(fixedDelay = 100 ms)` checker for time-based trigger
- On flush: group samples by `(name, labels)` into `TimeSeries`, build `WriteRequest`, Snappy-compress, POST

**HTTP client:**
- Spring 6 `RestClient` (Boot 4 default)
- Timeouts: connect 5s, read 30s
- Headers:
  - `Content-Type: application/x-protobuf`
  - `Content-Encoding: snappy`
  - `X-Prometheus-Remote-Write-Version: 0.1.0`
  - `User-Agent: deltav-prometheus-writer/<version>`
  - Auth per config: `Authorization: Bearer <token>` or `Authorization: Basic <base64>` or none
  - Extra headers from config (e.g. `X-Scope-OrgID: <tenant>` for Mimir multi-tenant)

**Retry classifier:**

| HTTP status / error | Classification | Action |
|---|---|---|
| 200, 204 | success | ack |
| 429 | rate-limited | retry with `Retry-After` header respect (fallback to exp backoff) |
| 500, 502, 503, 504 | transient | retry with 100ms → 30s exp backoff + jitter |
| Network error (timeout, ECONNREFUSED, ECONNRESET) | transient | same as 5xx |
| 400 | poison | DLQ + drop, increment `dlq_records_total{reason="bad_request"}`, do NOT retry |
| 413 | poison (too large) | DLQ + drop, increment `dlq_records_total{reason="payload_too_large"}` |
| 401, 403 | auth failure | open circuit, log ERROR once per circuit cycle, increment `batches_failed_total{reason="auth"}` |
| 404 | bad URL | open circuit, log ERROR, increment `batches_failed_total{reason="bad_url"}` |

Retry bounded by circuit-closed state; circuit opens on persistent failure regardless of classifier.

**Circuit breaker (Resilience4j):**
- `failureRateThreshold = 50%`
- `slidingWindowType = COUNT_BASED`
- `slidingWindowSize = 20`
- `minimumNumberOfCalls = 10`
- `waitDurationInOpenState = 30s`
- `permittedNumberOfCallsInHalfOpenState = 3`
- `automaticTransitionFromOpenToHalfOpenEnabled = true`

**State machine:**
```
CLOSED ── 50% failures in last 20 calls ──▶ OPEN
OPEN ── 30s elapsed ──▶ HALF_OPEN
HALF_OPEN ── 3 calls succeed ──▶ CLOSED
HALF_OPEN ── any call fails ──▶ OPEN
```

On every transition:
- emit `deltav_prometheus_writer_circuit_state{endpoint} = 0|1|2`
- log INFO with state + endpoint
- `ConsumerPauseListener` calls `BindingsLifecycleController.pause()` on CLOSED → OPEN or HALF_OPEN → OPEN transitions
- `ConsumerPauseListener` calls `BindingsLifecycleController.resume()` on HALF_OPEN → CLOSED transition
- (no pause/resume on OPEN → HALF_OPEN transition — probes run without the consumer producing load)

### 6. DLQ

**Topic config (new NewTopic bean):**
- Name: `deltav-prometheus-writer-dlq`
- Partitions: 16 (match `deltav-timeseries` for key-preserving writes)
- `cleanup.policy=delete`
- `retention.ms=604800000` (7 days)
- `compression.type=lz4`

**Record shape:**
- Key: same `{location}@{node_id}` as the source
- Value: the original `TimeseriesBatch` protobuf bytes, unmodified
- Headers (all strings):
  - `x-dlq-reason`: `bad_request` | `payload_too_large`
  - `x-dlq-http-status`: `400` | `413`
  - `x-dlq-endpoint`: the RW URL that rejected it
  - `x-dlq-timestamp-ms`: epoch ms of the DLQ write
  - `x-dlq-error-message`: RW target's response body, truncated to 1 KB
  - `x-dlq-writer-version`: producer version string

**SCS binding:**
- `spring.cloud.stream.bindings.publishDlq-out-0.destination: deltav-prometheus-writer-dlq`
- `spring.cloud.stream.bindings.publishDlq-out-0.producer.use-native-encoding: true` (byte-array payload, splitter-lesson prophylactic)

### 7. Observability

All metrics exposed at `/actuator/prometheus` via Micrometer. Meter name prefix: `deltav_prometheus_writer_`.

**Ingestion:**
- `deltav_prometheus_writer_records_consumed_total{location, producer, collection_package}` — Counter
- `deltav_prometheus_writer_samples_in_total{location}` — Counter (individual Prom samples after fan-out, before filters)

**Enrichment:**
- `deltav_prometheus_writer_enrichment_hit_total{location}` — Counter
- `deltav_prometheus_writer_enrichment_missing_total{location, reason}` — Counter (`reason ∈ {never_seen, tombstoned}`)
- `deltav_prometheus_writer_node_context_cache_size` — Gauge (entry count)
- `deltav_prometheus_writer_node_context_cache_ready` — Gauge (0/1)
- `deltav_prometheus_writer_node_context_bootstrap_duration_seconds` — Timer (one-shot)

**Sample dropouts:**
- `deltav_prometheus_writer_samples_dropped_total{reason}` — Counter (`reason ∈ {string_attribute, type_unspecified, sanitize_collision, enrichment_missing}`)

**RW output:**
- `deltav_prometheus_writer_batches_sent_total{endpoint}` — Counter
- `deltav_prometheus_writer_samples_sent_total{endpoint}` — Counter (successful samples, post-flush)
- `deltav_prometheus_writer_batches_failed_total{endpoint, reason}` — Counter (`reason ∈ {remote_write_5xx, remote_write_429, remote_write_4xx, network_error, serialization_error, auth, bad_url}`)
- `deltav_prometheus_writer_batch_size_bytes{endpoint}` — DistributionSummary (post-Snappy)
- `deltav_prometheus_writer_batch_sample_count{endpoint}` — DistributionSummary
- `deltav_prometheus_writer_flush_duration_seconds{endpoint}` — Timer
- `deltav_prometheus_writer_retry_attempts_total{endpoint, reason}` — Counter

**DLQ:**
- `deltav_prometheus_writer_dlq_records_total{reason}` — Counter

**Circuit + consumer state:**
- `deltav_prometheus_writer_circuit_state{endpoint}` — Gauge (0=closed, 1=half_open, 2=open)
- `deltav_prometheus_writer_consumer_paused` — Gauge (0/1)

### 8. Configuration (`application.yml`)

```yaml
spring:
  application:
    name: prometheus-writer
  kafka:
    bootstrap-servers: ${SPRING_KAFKA_BOOTSTRAP_SERVERS:localhost:9092}  # prod yaml carries it — Phase 0 #170 lesson
  cloud:
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
            use-native-encoding: true    # splitter-lesson prophylactic

prometheus-writer:
  remote-write:
    url: ${PROMETHEUS_WRITER_REMOTE_WRITE_URL:http://victoriametrics:8428/api/v1/write}
    auth:
      type: ${PROMETHEUS_WRITER_AUTH_TYPE:none}   # none | bearer | basic
      bearer-token: ${PROMETHEUS_WRITER_BEARER_TOKEN:}
      basic-username: ${PROMETHEUS_WRITER_BASIC_USER:}
      basic-password: ${PROMETHEUS_WRITER_BASIC_PASS:}
    headers:
      # Example: X-Scope-OrgID: "delta-v"   # for Mimir multi-tenant
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
    from-metadata: []                     # optional allowlist, empty by default
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

### 9. Docker integration

**New Dockerfile:** `opennms-container/delta-v/Dockerfile.prometheus-writer`
- Base: same JRE layer as existing daemon-boot-* services
- COPY the fat jar from `core/prometheus-writer/target/prometheus-writer-*.jar`
- ENTRYPOINT runs the jar with JVM flags matching other delta-v services (heap tuned conservatively for 10k-node scale: `-Xmx512m`)

**New compose service** (`opennms-container/delta-v/docker-compose.yml`):

```yaml
prometheus-writer:
  image: opennms/deltav-prometheus-writer:${ONMS_DELTAV_VERSION}
  profiles: [lite, full]
  depends_on:
    kafka: {condition: service_healthy}
    provisiond: {condition: service_started}
  environment:
    SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka:9093
    PROMETHEUS_WRITER_REMOTE_WRITE_URL: http://victoriametrics:8428/api/v1/write
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

(The VM version `v1.106.1` is pinned explicitly per Q10d.)

**`build.sh` integration:**
- `opennms-container/delta-v/build.sh` iterates `core/daemon-boot-*/` (13 modules today) and explicitly builds `core/flow-enricher/` as a standalone service. Phase 2 adds a third top-level build step for `core/prometheus-writer/` (same pattern as flow-enricher — not a daemon-boot-* module).
- PR template reminder: "Rebuild all 13 daemon-boot jars + flow-enricher + prometheus-writer before `./build.sh deltav`" (per `feedback_rebuild_all_daemons.md`; memory note to be updated on merge)

### 10. E2E test script

**New file:** `opennms-container/delta-v/test-prometheus-writer-e2e.sh`

6 steps, modeled after `test-node-context-e2e.sh`:

**Step 1: Stack up**
```
docker compose --profile lite --profile metrics-e2e up -d --build
```
Wait for `prometheus-writer /actuator/health/readiness` green (timeout 120s). Ready = `NodeContextCache` bootstrapped + binding resumed.

**Step 2: Assert startup gate fired**
```
metrics=$(docker compose exec -T prometheus-writer curl -sf http://localhost:8080/actuator/prometheus)
assert: deltav_prometheus_writer_node_context_cache_ready == 1
assert: deltav_prometheus_writer_node_context_bootstrap_duration_seconds_count >= 1
```

**Step 3: Wait for Collectd polls**
```
sleep 90    # 2× default 30s poll cycle + grace
```

**Step 4: Assert samples flowed**
```
metrics=$(...)
assert: deltav_prometheus_writer_records_consumed_total > 0
assert: deltav_prometheus_writer_samples_sent_total > 0
assert: deltav_prometheus_writer_batches_sent_total > 0
assert: deltav_prometheus_writer_circuit_state == 0   # closed
```

**Step 5: Assert zero failures**
Each assertion guards against grep-no-match-under-pipefail with `{ ... || true; }`:
```
failure_count=$({ echo "$metrics" | grep '^deltav_prometheus_writer_batches_failed_total' || true; } | awk '{sum+=$2} END {print sum+0}')
assert: failure_count == 0
# Repeat for enrichment_missing_total, samples_dropped_total, dlq_records_total
```

**Step 6: Query VictoriaMetrics**
```
curl -sf "http://localhost:18428/api/v1/query?query=opennms_mib2_interface_errors_ifindiscards_total"
assert: result[] is non-empty
assert: at least one result has labels {node_id, location, foreign_source, resource_instance}
```

**Teardown:**
```
docker compose --profile lite --profile metrics-e2e down -v --remove-orphans
```

## Scar prophylactics

Per Phase 0/1 lessons, Phase 2 bakes in these guards from day one:

1. **Real-main-class IT.** `PrometheusWriterApplicationScanIT` does `@SpringBootTest(classes = PrometheusWriterApplication.class)` and asserts the key beans resolve (`NodeContextCache`, `TimeseriesConsumer`, `BatchingRwWriter`, `PrometheusWriterCircuitBreaker`, `DlqPublisher`). Exercises the real `SpringApplication.run()` scan path. Would have caught Phase 0's `scanBasePackages` gap in one CI run.
2. **Production yaml parity.** Every property any Testcontainers IT sets via `ApplicationContextInitializer` also exists in `src/main/resources/application.yml`. Checked by CI lint at PR time.
3. **`use-native-encoding: true`** on the DLQ producer binding. Splitter-lesson prophylactic (Phase 1.5 scar).
4. **Full-reactor verify gate.** CI runs `./mvnw clean install -DskipTests -fae` from the reactor root before the PR can merge. Catches cross-module cascade failures that `-pl core/prometheus-writer` would miss.
5. **Rebuild-all-services reminder** in PR description template. `build.sh deltav` expects all 13 daemon-boot jars + flow-enricher + prometheus-writer to be current; stale jars cause silent runtime surprises.
6. **Pipefail-safe grep** in the E2E script. All `grep`-driven counter assertions wrapped in `{ ... || true; }` per Phase 1 scar.

## Testing strategy

### Unit tests (JUnit 5)
- `NodeContextCacheTest` — put/remove/tombstone semantics
- `NameSanitizerTest` — every edge case (leading digit, consecutive separators, collisions)
- `TimeseriesToPromTranslatorTest` — filter + label-build logic, string drop, unspecified drop, counter `_total` suffix
- `LabelBuilderTest` — metadata allowlist, categories comma-join, empty-value handling
- `RemoteWriteRetryPolicyTest` — status code → action classification
- `BatchingRwWriterTest` — flush triggers (size / bytes / time), grouping by series

### SCS test-binder ITs (Spring Cloud Stream)
- `TimeseriesConsumerBinderIT` — assert the consumer processes a `TimeseriesBatch` → samples land in a mock RW client
- `DlqPublisherBinderIT` — assert DLQ binding produces with `use-native-encoding` and headers set

### Testcontainers broker ITs
- `NodeContextBootstrapTC` — real Kafka broker, publish 100 `NodeContext` records, assert cache reaches "ready" with all 100 entries
- `RwRoundTripTC` — real Kafka + stub HTTP sink, assert end-to-end: Kafka in → sink receives decompressible `WriteRequest`
- `CircuitBreakerTC` — inject a failing sink, assert circuit opens + binding pauses + circuit recovers + binding resumes

### Real-main-class IT
- `PrometheusWriterApplicationScanIT` (see Scar prophylactic #1)

### Docker Compose E2E
- `test-prometheus-writer-e2e.sh` (see §10)

### Load / soak (manual, not CI)
- Deploy at labbox or dev cluster, point at real VM instance, run for 24 hours
- Assert no memory leak (sample count steady, cache size steady)
- Assert no samples lost over a 10-minute VM restart (circuit opens, pauses, VM recovers, consumer resumes, lag drains)

## Schema freeze decision

Upon Phase 2 merge:

1. `core/deltav-kafka-contracts/src/main/proto/deltav-timeseries.proto` — header comment updated to reflect **Phase 2 GA, wire format frozen**
2. `core/deltav-kafka-contracts/src/main/proto/deltav-node-context.proto` — header comment updated similarly
3. Going forward: additions permitted only via new tag numbers. Breaking changes require bump to `// API version: 2` with a one-release deprecation cycle maintaining v1 as read-only.

This commitment unblocks future consumers (Thresholder, additional Prom RW instances built by operators, third-party processors) to target stable contracts.

## Rollout

- Feature flag: none (Phase 2 is pure addition; no existing functionality changes)
- Docker Compose default: `prometheus-writer` service starts in `[lite, full]` profiles, pointed at `http://victoriametrics:8428/...` which is only up in `[metrics-e2e]` profile → in normal `lite`/`full` runs, the writer starts healthy but opens its circuit due to network unreachable, logs WARN, stays paused. This is intentional: the writer is ready for operator to override the RW URL; the default is a placeholder.
- Production deployment: operator sets `PROMETHEUS_WRITER_REMOTE_WRITE_URL` env var (and auth config) to their real TSDB before bringing up the service.

## Open questions → answered

All 10 design questions from the next-session prompt plus Q11 (schema freeze) have been decided:

| # | Topic | Decision |
|---|---|---|
| 1 | Deployment shape | New module `core/prometheus-writer/`, standalone Spring Boot |
| 2 | Enrichment mechanism | Plain SCS + `NodeContextCache` bean (not Kafka Streams GlobalKTable) |
| 3a | RW target pluggability | Single configurable endpoint |
| 3b | E2E backend | VictoriaMetrics single-node (pinned v1.106.1) |
| 3c | Auth | Bearer / basic / none + extra headers; no mTLS |
| 4a | Metric name scheme | `opennms_{group}_{attr}[_total]`, snake_case |
| 4b | Label set | Strict allowlist default; metadata labels opt-in via config |
| 4c | String attributes | Drop + counter |
| 4d | Sanitization | Lowercase + non-alphanumeric → `_`, collapse, strip leading digit |
| 5a | COUNTER | Emit as Prom counter with `_total`, trust `rate()` for resets |
| 5b | GAUGE | Emit as-is; no clamping |
| 5c | UNSPECIFIED | Drop + counter |
| 5d | Timestamp | Collection time from `TimeseriesBatch.timestamp_ms` |
| 6a | Startup drift | Block consumer until cache bootstrapped |
| 6b | Post-startup miss | Drop + counter |
| 6c | Tombstone | `cache.remove(key)`; no staleness marker |
| 7 | Staleness on delete | Defer; rely on RW target retention |
| 8a | Batch window | 1000 samples / 1 MB / 1000 ms (whichever first) |
| 8b | Retry policy | Tiered by HTTP status |
| 8c | Poison pill | DLQ to `deltav-prometheus-writer-dlq` |
| 8d | Consumer pause | Resilience4j circuit + SCS binding pause/resume |
| 9 | Observability | Full `deltav_prometheus_writer_*` metric set at `/actuator/prometheus` |
| 10a | Compose profiles | Writer in `[lite, full]`; VM in new `[metrics-e2e]` |
| 10b | E2E structure | 6 steps ending in VM query |
| 10c | Scar prophylactics | All 6 guards baked in from day one |
| 10d | VM version | Pinned (v1.106.1) |
| 11a | Schema changes | None — both contracts freeze at Phase 2 GA |
| 11b | Versioning rule | Keep `// API version: 1` header convention; new fields at new tags, breaking changes bump version |

## Deliverables

1. This design document (committed in this PR).
2. Implementation plan: `docs/superpowers/plans/2026-04-17-kafka-ts-phase-2-prometheus-consumer.md` (next session — see next-session prompt queued alongside).
3. Next-session prompt: `docs/superpowers/next-session-prompts/2026-04-18-kafka-ts-phase-2-implementation.md` (written in this PR).
