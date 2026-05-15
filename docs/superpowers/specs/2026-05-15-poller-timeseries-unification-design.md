# Pollerd / PerspectivePollerd Timeseries Publisher Unification

> **Status:** Design approved 2026-05-15. Implementation plan to follow. This is a pure unification refactor — no behavior change — plus the Grafana dashboard gap-fill and an rc3 cut.

## Goal

Both Pollerd and PerspectivePollerd publish per-poll response-time samples to the `deltav-timeseries` Kafka topic ("Phase 3" of the Kafka Time Series pipeline). Each daemon currently carries its own near-verbatim copy of the publisher. This effort:

1. Collapses the duplicated publisher code into one shared implementation in a new scoped module.
2. Fills the Grafana dashboard gap — PerspectivePollerd has a latency dashboard, Pollerd does not.
3. Cuts **v1.2.0-rc3**, which is the first released tag to carry PerspectivePollerd Phase 3.

**No behavior change.** The wire output (`TimeseriesBatch` protobuf bytes), Kafka key, topic, Micrometer counters, and gate flags are all preserved exactly.

## Current state (verified 2026-05-15)

### Phase 3 is already implemented for both daemons

| Daemon | Phase 3 status | Where |
|---|---|---|
| Pollerd | Shipped | v1.2.0-beta2 (PRs #220–#222) |
| PerspectivePollerd | **Implemented, on `develop`, not yet in any rc tag** | commit `5df8e8cab4c` ("feat(v1.2.0): PerspectivePollerd Phase 3 publisher to deltav-timeseries"), landed after rc2.2 |

The v1.2.0 release plan doc (`docs/plans/2026-04-23-v1.2.0-release-plan.md`, Track 4) and the memory `project_v1_2_pollerd_timeseries` both still say PerspectivePollerd Phase 3 is "DEFERRED." That is stale — corrected as part of this effort.

The release plan's deferral rationale (PerspectivePollerd's poll callback is "a private lambda with no public seam") turned out not to hold: `InstrumentedPerspectivePollerd extends PerspectivePollerd` and overrides `persistResponseTimeData(...)`, a *public* method on horizon's `PerspectivePollerd`. The private Quartz lambda calls that public method — so the method is the seam.

### The duplication

Two publisher classes, ~95% byte-identical:

- `core/daemon-boot-pollerd/.../boot/PollResultPublisher.java` (167 lines)
- `core/daemon-boot-perspectivepollerd/.../boot/PerspectiveResponseTimePublisher.java` (164 lines)

The entire `publish()` control flow (null/NaN guards, `Timer.Sample`, two try/catch blocks, three Micrometer counters, the `finally`) and the entire `buildBatch()` body are identical. They differ only in:

| Varying element | Pollerd | PerspectivePollerd |
|---|---|---|
| Input type | `PollableService` | `PerspectivePolledService` |
| Node-id getter | `getNodeId()` | `getNodeId()` |
| Service-name getter | `getSvcName()` | `getServiceName()` |
| Location getter | `getNodeLocation()` | `getPerspectiveLocation()` |
| Producer type | `PRODUCER_POLLERD` (constructor-injected) | `PRODUCER_PERSPECTIVE_POLLERD` (hardcoded) |
| Producer label | constructor-injected | `"perspectivepollerd"` (hardcoded) |

### The hooks (stay daemon-specific)

| Daemon | Hook class | Horizon base | Override |
|---|---|---|---|
| Pollerd | `InstrumentedPollContext` | `StandalonePollContext` | `trackPoll`, `openOutage`, `resolveOutage` |
| PerspectivePollerd | `InstrumentedPerspectivePollerd` | `PerspectivePollerd` | `persistResponseTimeData` |

Both hooks already share the same shape per call: delegate to `super`, record Phase-2 `deltav_*` counters, invoke the publisher. They cannot merge — horizon gives the two daemons different base classes — but they are already as parallel as possible. **The hooks are not the duplication worth removing; the publisher is.**

### Tests

There are **no** existing unit tests for either publisher or either hook (the `src/test` trees are empty). No-behavior-change currently rests entirely on the E2E smoke scripts.

### Protobuf contract

`core/deltav-kafka-contracts/src/main/proto/deltav-timeseries.proto` defines `TimeseriesBatch`, `Resource`, `AttributeGroup`, `Attribute`, and the `ProducerType` enum (`PRODUCER_COLLECTD`, `PRODUCER_POLLERD`, `PRODUCER_PERSPECTIVE_POLLERD`). There is no dedicated `ResponseTimeSample` protobuf — response times reuse `TimeseriesBatch` with a single `Resource` → single `AttributeGroup` ("response-time") → single `Attribute` ("response", GAUGE). This stays unchanged.

### Grafana dashboards

`opennms-container/delta-v/grafana/dashboards/` is baked into the `deltav/grafana` image; the provisioning provider auto-loads every JSON under `/etc/grafana/dashboards`. The smoke VM runs that image.

- `perspective-monitoring.json` — **exists**; response-time + Phase 3 publish-health panels for PerspectivePollerd.
- No Pollerd equivalent — `opennms_response_time_response{producer="pollerd"}` lands in VictoriaMetrics from beta2 but is unsurfaced.

## Design

### 1. New module: `core/poller-timeseries-common`

- groupId `org.deltav.core`, artifactId `org.opennms.core.poller-timeseries-common` (sibling convention — rides the future `project_artifactid_namespace_cleanup` rename), Java package `org.deltav.poller.timeseries`
- Registered in the root `pom.xml` `<modules>` list
- Dependencies: `org.opennms.core.deltav-kafka-contracts` (protobuf types), `spring-cloud-stream` (`StreamBridge`), `micrometer-core`. **No horizon poller types.**
- Scoped deliberately: only Pollerd and PerspectivePollerd depend on it, so the `deltav-kafka-contracts` protobuf dependency does not spread to the other 11 daemons (which it would if this lived in `core/daemon-common`).

### 2. Two new types in that module

```java
public record ResponseTimeSample(
        int nodeId,
        String serviceName,
        String location,
        double responseTimeMs,
        long timestampMs,
        ProducerType producerType,
        String producerLabel) {}

public class ResponseTimePublisher {
    public ResponseTimePublisher(StreamBridge streamBridge, MeterRegistry meterRegistry) { ... }
    public void publish(ResponseTimeSample sample) { ... }
}
```

`ResponseTimePublisher.publish()` is the existing `publish()` + `buildBatch()` body moved **verbatim**, generalized only to read fields off the record instead of calling horizon getters. Preserved exactly:

- null / NaN guards (returns silently on no measurable response time)
- `Timer.Sample` around the publish, stopped in a `finally`
- Kafka key `{location}@{nodeId}`, binding `publishTimeseries-out-0`
- `TimeseriesBatch` shape: single `Resource` (`resource_id = node[N].monitoredService[svc]`, type `monitoredService`), single `AttributeGroup` (`response-time`), single `Attribute` (`response`, GAUGE, ms)
- counters `deltav_timeseries_batches_published_total`, `deltav_timeseries_batches_failed_total` (with `reason` tag), `deltav_timeseries_publish_duration_seconds` — all tagged `location` + `producer`

### 3. Per-daemon changes

**Pollerd:**
- Delete `PollResultPublisher`.
- `InstrumentedPollContext` gains a small `toSample(PollableService, PollStatus)` adapter and calls the shared `ResponseTimePublisher`.
- `PollerdTimeseriesConfiguration` wires the shared `ResponseTimePublisher` bean with `PRODUCER_POLLERD` / `"pollerd"`.

**PerspectivePollerd:**
- Delete `PerspectiveResponseTimePublisher`.
- `InstrumentedPerspectivePollerd` gains a small `toSample(PerspectivePolledService, PollStatus)` adapter and calls the shared `ResponseTimePublisher`.
- `PerspectivePollerdTimeseriesConfiguration` wires the shared bean with `PRODUCER_PERSPECTIVE_POLLERD` / `"perspectivepollerd"`.

The gate flags `deltav.timeseries.enabled` (Pollerd) and `deltav.perspective.timeseries.enabled` (PerspectivePollerd) are **unchanged** — daemon-scoped per `feedback_no_shared_config_across_daemons`.

Net code delta: ~330 duplicated lines → one shared publisher (~110 lines) + two ~6-line adapters.

### 4. No-behavior-change guarantee + testing

- **Guarantee:** the shared `publish()` is the existing code verbatim. `ResponseTimeSample` captures 100% of what differed between the two daemons. Equivalent input → byte-identical `TimeseriesBatch`.
- **New unit test** (the first to exist for this code): `ResponseTimePublisherTest` in `core/poller-timeseries-common` — mock `StreamBridge`, assert emitted batch fields / key / producer for both producer types, assert failure paths increment the correct counters with the correct `reason` tag.
- **E2E unchanged:** `test-timeseries-e2e.sh` and `test-perspective-e2e.sh` already assert samples reach `deltav-timeseries`; they validate no-behavior-change at integration level.
- **Mandatory local smoke before merge:** both daemons booted via docker-compose on the Mac dev env; both `producer="pollerd"` and `producer="perspectivepollerd"` (or `perspective_pollerd` — see Known Issues) series confirmed in VictoriaMetrics. Non-negotiable per the `project_alarmd_alarm_lifecycle_gap` lesson — daemon-startup regressions do not surface in Maven build or CI. Also: adding a dependency to the daemon modules requires `./mvnw clean install` downstream per `feedback_spring_boot_repackage_needs_clean`.

### 5. Grafana dashboards

- **PerspectivePollerd:** `perspective-monitoring.json` exists — no change.
- **Pollerd:** add `opennms-container/delta-v/grafana/dashboards/pollerd-monitoring.json`, structurally mirroring `perspective-monitoring.json`, retargeted to `producer="pollerd"`:
  - Per-node/service response time (ms) — `opennms_response_time_response{producer="pollerd"}`
  - Latest avg response time by location
  - Polls/sec by location + result — `rate(deltav_pollerd_polls_completed_total[1m])`
  - Phase 3 publish failures — `rate(deltav_timeseries_batches_failed_total{producer="pollerd"}[5m])`
  - Phase 3 records published (5m bucket) — `increase(deltav_timeseries_batches_published_total{producer="pollerd"}[5m])`
- Two sibling dashboards with parallel layouts (rather than one merged dashboard) mirror the code design: shared where the daemons are identical, parallel-but-separate where they genuinely differ.
- The new JSON bakes into `deltav/grafana` automatically; it ships in rc3 and appears on the smoke VM with no extra plumbing.
- **Smoke validation step:** confirm both dashboards render populated panels after a poll cycle on the VM.

### 6. Release packaging

- After the refactor merges to `develop`: cut **v1.2.0-rc3** — the first released tag carrying PerspectivePollerd Phase 3 (`5df8e8cab4c`) plus this unification.
- Stale-doc correction folded in: `docs/plans/2026-04-23-v1.2.0-release-plan.md` Track 4 ("PerspectivePollerd Phase 3 — DEFERRED") corrected to DONE; memory `project_v1_2_pollerd_timeseries` updated.

## Out of scope

- **Collectd's publisher** (`TimeseriesKafkaPublisher`) — different shape (multi-resource `CollectionSet`); not unified here.
- **Phase-2 counter recording** in the hooks (`deltav_pollerd_*` vs `deltav_perspective_*`) — different metric names and tag sets per daemon; left as-is.
- **The deferred design-doc questions** (gauge-vs-histogram, label-cardinality knobs, persistent disk buffering) — beta2 settled them by default; revisiting them is a separate effort.
- **Fixing the `producer` label inconsistency** — see Known Issues; it is a behavior change and therefore excluded from this pure refactor.

## Known issues / follow-ups

**`producer` label rendering is inconsistent.** The `opennms_response_time_response` time-series in VictoriaMetrics carries a `producer` label derived by `prometheus-writer` from the protobuf `ProducerType` enum. Observed values: `producer="pollerd"` (no underscore) for Pollerd, but `producer="perspective_pollerd"` (underscore) in the existing `perspective-monitoring.json` queries. Separately, the in-daemon `deltav_timeseries_batches_*` Micrometer counters use `producer="perspectivepollerd"` (no underscore, from the publisher's hardcoded label). Three different conventions across two metric families.

This is a real inconsistency but **fixing it changes metric label values in VictoriaMetrics** — a behavior change that breaks existing dashboard queries — so it is excluded from this pure-refactor effort. The new `pollerd-monitoring.json` will use whatever value Pollerd actually emits today (`producer="pollerd"`). Recommendation: file a separate follow-up to make `prometheus-writer`'s `ProducerType`-enum-to-label rendering consistent, and align the publisher's Micrometer `producerLabel` with it, in one coordinated change with dashboard updates.

## References

- Original design doc: `docs/plans/2026-04-23-v1.2.0-pollerd-timeseries-design.md`
- Release plan (stale on Track 4): `docs/plans/2026-04-23-v1.2.0-release-plan.md`
- PerspectivePollerd Phase 3 commit: `5df8e8cab4c`
- Reference publisher (collectd, out of scope): `core/daemon-boot-collectd/src/main/java/org/deltav/collectd/timeseries/TimeseriesKafkaPublisher.java`
- Memory: `project_kafka_timeseries_pipeline`, `project_v1_2_pollerd_timeseries`, `feedback_no_shared_config_across_daemons`, `feedback_spring_boot_repackage_needs_clean`, `feedback_meter_naming_horizon_vs_deltav`, `project_alarmd_alarm_lifecycle_gap` (smoke-before-merge lesson)
