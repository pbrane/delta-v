# TracerRegistry × N → OpenTelemetry SDK + OT Shim — Design Spec

**Date:** 2026-05-12
**Context:** v1.2.0 requirement G (Daemon null-op audit, full sweep) → priority 2.
**Scope (PR1 of 3):** Replace the 3 `NoOpTracerRegistry` instances in delta-v daemon-boot code with an `OpenTelemetryTracerRegistry` backed by the OpenTelemetry SDK via the OpenTracing → OpenTelemetry shim. The horizon-side `TracerRegistry` interface and all its consumers remain unchanged.
**Out of scope (deferred to follow-on PRs):**
- **PR2:** Tempo container in docker-compose + Grafana data source.
- **PR3:** Explicit `@WithSpan` / `tracer.buildSpan(...)` instrumentation on horizon paths that don't already produce spans.

## Background

The 2026-05-09 daemon null-op audit (memory: `project_daemon_nullop_audit_findings`) listed `TracerRegistry NoOp × 2` (Minion + Telemetryd) as priority 2. Investigation on 2026-05-12 corrected the count:

| Location | Affected daemons |
|---|---|
| `core/daemon-boot-minion/.../MinionInfraConfiguration.java:47-48` | Minion |
| `core/daemon-boot-telemetryd/.../TelemetrydDaemonConfiguration.java:153-154` | Telemetryd |
| `core/daemon-common/.../KafkaRpcClientConfiguration.java:57-59` (audit missed this) | Pollerd, Collectd, Enlinkd, PerspectivePollerd, Provisiond, Discovery (every daemon with `RPC_KAFKA_ENABLED=true`) |

The shared bean in daemon-common is the high-leverage fix: replacing one bean propagates the change to 6 RPC-enabled daemons via Spring's `@ConditionalOnProperty` activation. Combined with the two daemon-specific @Beans, this PR touches 3 @Bean definitions and reaches 8 daemons.

Horizon's `TracerRegistry`/`TracerWrapper` API speaks `io.opentracing.Tracer` (OpenTracing, deprecated since 2022). The audit's "OpenTelemetry exporter" recommendation isn't a direct fit. Two viable paths:

1. **Keep OpenTracing:** wire horizon's `JaegerTracerWrapper` (legacy Jaeger client, OpenTracing spans).
2. **Modernize to OpenTelemetry:** add OTel SDK and bridge via the official `opentelemetry-opentracing-shim` so existing OpenTracing-instrumented horizon code transparently produces OTel spans.

Choice (per 2026-05-12 brainstorm): path 2. The shim is the canonical OT→OTel migration path; it preserves horizon's API contract while landing the modern SDK + OTLP exporter on the daemon side.

## Goal

Close the registry-layer gap so spans are *created and propagated* in all 8 daemons. Wire an OTLP exporter behind a configurable endpoint. Do not deploy a tracing backend in this PR — that's PR2's scope.

## Verification before scoping (lessons from `feedback_audit_verify_ctor_signatures`)

- All 3 NoOp call-sites verified in current source (commits `2698e3bd447`, `635d570acf6` on develop as of 2026-05-12).
- Horizon's `TracerRegistry` interface confirmed at `delta-v-horizon/core/tracing/api/src/main/java/org/opennms/core/tracing/api/TracerRegistry.java`. Two methods: `Tracer getTracer()`, `void init(String serviceName)`.
- Horizon's `TracerRegistryImpl` (OSGi-based, uses `@Autowired(required=false) TracerWrapper`) cannot be reused as-is in Spring Boot daemons — no OSGi `TracerWrapper` registry exists in delta-v.
- Existing daemon-common deps include `io.opentracing:opentracing-api` and `io.opentracing:opentracing-util` (no OpenTelemetry deps yet).
- No tracing backend deployed in delta-v stack today (no Jaeger, Tempo, or OTel collector in `docker-compose.yml`).

## Approach

Replace `NoOpTracerRegistry` with `OpenTelemetryTracerRegistry` (new class in `daemon-common`) that:
- Implements horizon's existing `TracerRegistry` interface (zero call-site changes outside the 3 @Bean rewires).
- Lazily initializes a process-global `OpenTelemetry` SDK via `AutoConfiguredOpenTelemetrySdk.initialize()`.
- Returns `io.opentracing.Tracer` via `OpenTracingShim.createTracerShim(GlobalOpenTelemetry.get())` from `getTracer()`.
- Implements `init(String serviceName)` by setting the system property `otel.service.name` when the env var `OTEL_SERVICE_NAME` is not already set.

The OT→OTel shim is the load-bearing piece. It satisfies horizon's `io.opentracing.Tracer` contract while producing OTel spans backed by the modern SDK, OTLP exporter, and standard envvar-driven configuration (`OTEL_SERVICE_NAME`, `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_TRACES_EXPORTER`).

## Architecture

```
              ┌────────────────────────────────────────────────┐
              │   Existing horizon callers, unchanged          │
              │   (KafkaRpcClient, KafkaTwinPublisher, etc.)   │
              │   call: TracerRegistry.getTracer()             │
              │   expect: io.opentracing.Tracer                │
              └──────────────┬─────────────────────────────────┘
                             ▼
              ┌────────────────────────────────────────────────┐
              │  OpenTelemetryTracerRegistry  (NEW, daemon-    │
              │  common). Implements horizon's TracerRegistry. │
              │  Wraps GlobalOpenTelemetry via OT→OTel shim.   │
              └──────────────┬─────────────────────────────────┘
                             ▼
              ┌────────────────────────────────────────────────┐
              │  io.opentelemetry.opentracingshim              │
              │  .OpenTracingShim.createTracerShim(otel)       │
              │  returns: io.opentracing.Tracer  (existing     │
              │  callers see no API change)                    │
              └──────────────┬─────────────────────────────────┘
                             │ spans flow as OTel SpanData
                             ▼
              ┌────────────────────────────────────────────────┐
              │  AutoConfiguredOpenTelemetrySdk (lazy global   │
              │  init at first getTracer() call):              │
              │   - reads OTEL_SERVICE_NAME, OTEL_EXPORTER_*   │
              │   - default exporter: configurable             │
              │   - registered globally, idempotent            │
              └──────────────┬─────────────────────────────────┘
                             ▼
              PR1: configurable but undeployed (OTEL_TRACES_EXPORTER=none default)
              PR2: defaults flipped to OTLP → Tempo container
              PR3: explicit @WithSpan instrumentation on RPC + Telemetryd paths
```

All bean wiring uses constructor-injection factory methods per project CLAUDE.md.

## File-level changes

```
core/daemon-common/pom.xml
    + io.opentelemetry:opentelemetry-sdk
    + io.opentelemetry:opentelemetry-exporter-otlp
    + io.opentelemetry:opentelemetry-opentracing-shim
    + io.opentelemetry:opentelemetry-sdk-extension-autoconfigure
      (versions managed via OTel BOM imported in delta-v parent pom)

core/daemon-common/src/main/java/org/deltav/core/daemon/common/
    OpenTelemetryTracerRegistry.java
        + NEW. Single class, ~50 LOC. See "Reference implementation" below.

    NoOpTracerRegistry.java
        - DELETED. Per feedback_karaf_is_dead — no @Deprecated shim.

core/daemon-common/src/main/java/org/deltav/core/daemon/common/
    KafkaRpcClientConfiguration.java
        ~ Replace `return new NoOpTracerRegistry();` with
          `return new OpenTelemetryTracerRegistry();`
        ~ Imports updated.

core/daemon-boot-minion/src/main/java/org/deltav/minion/boot/
    MinionInfraConfiguration.java
        ~ Replace `return new NoOpTracerRegistry();` with
          `return new OpenTelemetryTracerRegistry();`
        ~ Imports updated.

core/daemon-boot-telemetryd/src/main/java/org/deltav/netmgt/telemetry/boot/
    TelemetrydDaemonConfiguration.java
        ~ Replace `return new NoOpTracerRegistry();` with
          `return new OpenTelemetryTracerRegistry();`
        ~ Imports updated.

core/daemon-common/src/test/java/org/deltav/core/daemon/common/
    OpenTelemetryTracerRegistryWiringTest.java
        + NEW. Four JUnit cases. No Spring context, no live network.
          See "Testing" below.

opennms-container/delta-v/docker-compose.yml
    + Per affected daemon (Minion, Telemetryd, Pollerd, Collectd, Enlinkd,
      PerspectivePollerd, Provisiond, Discovery), add to environment:
        OTEL_SERVICE_NAME: "<daemon-name>"
        OTEL_EXPORTER_OTLP_ENDPOINT: "${OTEL_EXPORTER_OTLP_ENDPOINT:-}"
        OTEL_TRACES_EXPORTER: "${OTEL_TRACES_EXPORTER:-none}"

    Default OTEL_TRACES_EXPORTER=none → SDK uses no-op exporter (spans
    created in-process, dropped before export). PR2 flips this default to
    "otlp" once Tempo is in the stack.
```

## Reference implementation

```java
package org.deltav.core.daemon.common;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.opentracingshim.OpenTracingShim;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;
import org.opennms.core.tracing.api.TracerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OpenTelemetry-backed implementation of horizon's {@link TracerRegistry}.
 *
 * <p>Returns an {@link io.opentracing.Tracer} produced by the OpenTracing
 * shim over a globally-registered OpenTelemetry SDK. Existing
 * OpenTracing-instrumented horizon code (Kafka RPC, Twin publish, etc.)
 * gets OpenTelemetry-backed spans transparently.</p>
 *
 * <p>Configuration is via standard OTel environment variables
 * ({@code OTEL_SERVICE_NAME}, {@code OTEL_EXPORTER_OTLP_ENDPOINT},
 * {@code OTEL_TRACES_EXPORTER}). When no exporter endpoint is configured,
 * the SDK uses the no-op exporter — spans are created in-process and
 * dropped before export. This is the PR1 default; PR2 will deploy a
 * Tempo backend and flip the exporter default to OTLP.</p>
 *
 * <p>SDK initialization is lazy and idempotent. The first call to
 * {@link #getTracer()} from any of the daemon's {@code tracerRegistry}
 * @Bean methods triggers initialization; subsequent callers see the
 * same global SDK.</p>
 */
public final class OpenTelemetryTracerRegistry implements TracerRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(OpenTelemetryTracerRegistry.class);

    @Override
    public Tracer getTracer() {
        try {
            return OpenTracingShim.createTracerShim(getOrInitSdk());
        } catch (RuntimeException e) {
            LOG.error("OpenTelemetry SDK initialization failed; falling back to NoopTracer. "
                    + "Check OTEL_* environment variables.", e);
            return GlobalTracer.get();
        }
    }

    @Override
    public void init(String serviceName) {
        // Only set if not already configured externally (OTEL_SERVICE_NAME env
        // or otel.service.name system property). Lets operator-supplied service
        // names take precedence over daemon-default behavior.
        if (System.getProperty("otel.service.name") == null
                && System.getenv("OTEL_SERVICE_NAME") == null) {
            System.setProperty("otel.service.name", serviceName);
        }
    }

    private OpenTelemetry getOrInitSdk() {
        // AutoConfiguredOpenTelemetrySdk.initialize() is idempotent — first
        // invocation builds and registers the SDK globally; subsequent
        // invocations return the existing global. Safe to call from multiple
        // beans across @Configuration classes.
        return AutoConfiguredOpenTelemetrySdk.initialize().getOpenTelemetrySdk();
    }
}
```

## Data flow

### Boot-time

1. Daemon JVM starts; Spring @Configuration scanning begins.
2. Exactly one of the 3 `tracerRegistry()` @Beans runs (depending on which daemon and the `opennms.rpc.kafka.enabled` property): `KafkaRpcClientConfiguration`, `MinionInfraConfiguration`, or `TelemetrydDaemonConfiguration`. Bean returns a new `OpenTelemetryTracerRegistry`. SDK is NOT initialized yet.
3. Other beans (`KafkaRpcClientFactory`, `LocalTwinSubscriber`, etc.) inject the `TracerRegistry`. Most don't call `getTracer()` until first use.
4. First caller invokes `tracerRegistry.getTracer()`. `AutoConfiguredOpenTelemetrySdk.initialize()` reads env, builds SDK, registers globally.
5. `OpenTracingShim.createTracerShim(sdk)` returns an `io.opentracing.Tracer`. Spans flow from here forward.

### Runtime (per-span)

Horizon's existing OpenTracing-instrumented code is unchanged:

```
tracer.buildSpan("rpc.send.snmp.detect").start()
    │
    ▼  [OpenTracing API; backed by OTel via shim]
    ▼
... work proceeds; context propagates via shim → OTel injectors ...
    │
    ▼
span.finish() → OTel Span.end()
    │
    ▼
BatchSpanProcessor + configured exporter
    │
    ├─ PR1 default (OTEL_TRACES_EXPORTER=none): NoopSpanExporter,
    │  spans dropped before export. API gap closed; no operational impact.
    │
    ├─ PR1 with operator-supplied endpoint: OtlpGrpcSpanExporter,
    │  sent to whatever OTEL_EXPORTER_OTLP_ENDPOINT points at.
    │
    └─ PR2 default: OTLP → Tempo → Grafana trace UI.
```

### Service-name precedence

In order:
1. `OTEL_SERVICE_NAME` env var in docker-compose per daemon — authoritative.
2. `init(serviceName)` from horizon code (e.g., `KafkaRpcClientFactory.init()`) — sets system property if env is unset.
3. OTel SDK fallback: `unknown_service:java`.

The chain `env → init() → fallback` matches OTel autoconfigure precedence; no custom logic beyond the unset-guard in `init()`.

### Lazy vs eager SDK init — why lazy

Multi-bean construction order. The 3 `@Bean tracerRegistry()` definitions live in different `@Configuration` classes (and only one fires per daemon due to `@ConditionalOnProperty`). Eager init in the constructor would force every daemon's classpath scan to also build the OTel SDK at @Bean construction, even if no caller eventually needs it. Lazy init via `AutoConfiguredOpenTelemetrySdk.initialize()` (which is idempotent and thread-safe through `GlobalOpenTelemetry`) defers cost until first use and stays safe under concurrent first-use.

## Error handling

### Boot-time

| Failure | Behavior |
|---|---|
| `AutoConfiguredOpenTelemetrySdk.initialize()` throws (malformed `OTEL_EXPORTER_OTLP_ENDPOINT`, etc.) | `getTracer()` catches, logs ERROR with env values, returns `GlobalTracer.get()` (NoopTracer). Daemon continues. |
| OTel SDK deps missing from classpath | `ClassNotFoundException` at first call → caught → Noop fallback. Visible at startup as ERROR. |
| Multiple daemons each constructing their own `tracerRegistry()` @Bean | First-to-init wins; subsequent calls share the same global SDK (idempotent). |
| Service name unresolved (no env, no `init()` call) | `unknown_service:java` attribute on spans. Not an error; spans still flow. |

### Runtime

| Failure | Behavior | Compliance |
|---|---|---|
| OTLP endpoint unreachable | `BatchSpanProcessor` retries with backoff; WARN per failed batch. Application threads unaffected. | Honors `feedback_rpc_timeout_no_outages` by construction — tracing has no event-emission path. |
| Span buffer full (sustained exporter outage) | Spans drop at queue capacity (default 2048). `bsp.dropped_spans` counter increments. | Bounded memory; backpressure preferred over OOM. |
| `span.finish()` after SDK shutdown | No-op; discarded silently. | Daemon-shutdown path won't crash. |
| Caller throws between `start()` and `finish()` | Horizon's RPC code uses try/finally; span is closed in finally. | Existing horizon hygiene. |
| Trace context propagation across Kafka boundary | OT shim's text-map injector serializes context into Kafka headers; `KafkaRpcClient` already uses `TextMapCodec`. | Unchanged by this PR. |

### Cross-cutting hygiene

- No silent failures — every error mode logs ERROR or WARN. Noop fallback is logged ERROR so operators can spot misconfig.
- No backwards-compat shim — `NoOpTracerRegistry.java` is deleted.
- No event emission — tracing infrastructure cannot create OpenNMS events or outages (structural).
- Test isolation — `GlobalOpenTelemetry.resetForTest()` is called in `@BeforeEach` / `@AfterEach` to avoid SDK state leaking across tests.

### Observability of the tracing layer itself

OTel SDK exposes Micrometer-compatible self-metrics that surface at `/actuator/prometheus` via the existing daemon-common Prometheus bridge:
- `otel.span.processor.queue.size`
- `otel.span.processor.processed_spans`
- `otel.span.processor.dropped_spans`
- `otel.exporter.exported_spans_count` / `failed_count`

No additional wiring required to monitor whether tracing is healthy.

## Testing

Wiring-only depth (no Spring context, no Tempo, no live network). Single test class with four cases:

```java
@Test
void getTracerReturnsNonNullAndNotTheGlobalNoop() { /* ... */ }

@Test
void initSetsServiceNameSystemPropertyWhenUnset() { /* ... */ }

@Test
void initDoesNotOverrideExternalServiceNameSetting() { /* ... */ }

@Test
void getTracerSucceedsEvenWithUnknownExporter() { /* ... */ }
```

Each uses `GlobalOpenTelemetry.resetForTest()` in `@BeforeEach`/`@AfterEach` to isolate SDK state. The misconfigured-exporter test forces a garbage `OTEL_TRACES_EXPORTER` value and asserts no exception escapes — proves the Section 4 contract.

### Coverage matrix

| Concern | Caught? |
|---|---|
| Maven deps present (SDK + shim + OTLP + autoconfigure) | Yes — import compile-time |
| `OpenTelemetryTracerRegistry` constructs without throwing | Yes |
| Real OTel SDK is wired (not the legacy NoopTracer fallback) | Yes — `isNotInstanceOf(NoopTracer.class)` |
| `init()` precedence rules | Yes — two tests |
| Misconfig doesn't crash daemons | Yes |
| `GlobalOpenTelemetry` state isolation | Yes — `resetForTest()` in setup/teardown |

### Existing test compatibility (Watchpoint #1)

Tests that boot the full `KafkaRpcClientConfiguration` or one of the two daemon-boot `@Configuration` classes will exercise the new bean. Some may attempt SDK init and try to export to nowhere. Mitigation strategy:

1. Add `src/test/resources/application.yml` (or extend existing) with:
   ```yaml
   otel:
     traces.exporter: none
     metrics.exporter: none
     logs.exporter: none
   ```
2. Discover any specific test that doesn't pick up the application config and add a `@BeforeAll` system-property setter or `@MockBean(TracerRegistry.class)`.

### Pre-merge verification (beyond JUnit)

1. `./mvnw -pl :org.opennms.core.daemon-common -am clean install` succeeds. **daemon-common must build first** because 6 daemons depend on it; if it breaks, the whole reactor breaks.
2. `./build.sh deltav` rebuilds all 12 daemon boot JARs (per `feedback_rebuild_all_daemons` — though build.sh self-heals).
3. `docker compose --profile full up -d` shows all 8 affected daemons (Minion, Telemetryd, Pollerd, Collectd, Enlinkd, PerspectivePollerd, Provisiond, Discovery) healthy.
4. Each daemon's boot log contains a single "OpenTelemetry SDK initialized" line (verifies lazy init fired once when first caller requested a tracer).
5. No `getTracer()` errors in any daemon log.
6. E2E suite remains green (per `feedback_e2e_continuous_validation_grpc_migration`). This fill shouldn't touch any E2E path; running them is verification, not new coverage.

## Implementation watchpoints

### 1. Test context pollution

Tests that boot `KafkaRpcClientConfiguration`, `MinionInfraConfiguration`, or `TelemetrydDaemonConfiguration` Spring contexts will exercise the new bean. Some may attempt SDK init at startup. Mitigation: `OTEL_TRACES_EXPORTER=none` in test application config or `@MockBean(TracerRegistry.class)` per-test.

**Verification gate:** all existing tests pass on the feature branch before merging.

### 2. Transitive dependency hygiene

The OpenTelemetry deps may pull in transitive ServiceMix/OSGi bundles (unlikely but possible from `opentelemetry-sdk-extension-autoconfigure`'s SPI scan). Per `feedback_karaf_is_dead`.

**Verification gate:** `./mvnw -pl :org.opennms.core.daemon-common dependency:tree | grep -iE 'servicemix|karaf|org.osgi|aries|felix'` shows no NEW entries vs. develop baseline. Add `<exclusions>` for any new offenders.

### 3. Spring Boot 4 + OTel autoconfigure interplay

`opentelemetry-sdk-extension-autoconfigure` does its own classpath scan and SPI discovery at `initialize()` time. Spring Boot 4's class loader and module isolation can interact poorly with SDK SPI lookup.

**Verification gate:** During Task 8 (container smoke), watch for `ServiceLoader` warnings or `NoSuchProviderException` in the OTel init log lines. If they appear, may need to add an explicit `OpenTelemetrySdkBuilder` configuration instead of the autoconfigure module.

### 4. OTel BOM version selection

Choose an OTel BOM version that's compatible with both OpenTracing shim and the OTLP exporter. The shim has historically lagged the main SDK by a release or two. Use the BOM line `io.opentelemetry:opentelemetry-bom-alpha:<version>` (the shim is in the alpha BOM) or pin the shim version explicitly if BOM versions diverge.

**Verification gate:** Compilation succeeds with the chosen BOM version. Run wiring tests; if `OpenTracingShim.createTracerShim(...)` throws `NoSuchMethodError`, version mismatch — pin explicitly.

### 5. Service-name uniqueness in docker-compose

Each daemon container needs its own `OTEL_SERVICE_NAME` for spans to be attributable. Make sure docker-compose changes set the name **per daemon**, not as a shared `x-otel-defaults` block that resolves to the same value everywhere.

**Verification gate:** Inspect `docker compose config` output to confirm each daemon service has a distinct `OTEL_SERVICE_NAME` env var.

## PR shape

- Branch: `feat/v1.2.0-tracerregistry-otel-fill` off `develop`.
- Target: `--repo pbrane/delta-v --base develop` (per project CLAUDE.md "CRITICAL: Git Remote Rules").
- Commit style: Conventional Commits, e.g. `feat(v1.2.0): wire OpenTelemetry SDK in daemon TracerRegistry beans`.
- Single PR. ~150 LOC: new class (~50), 3 @Bean rewires (~15), test class (~80), pom + compose edits.

## What this fill does NOT include

- **Tempo container or Grafana data source.** PR2's scope. PR1 lands the SDK and exporter; PR2 stands up the backend.
- **Explicit `@WithSpan` / `tracer.buildSpan(...)` instrumentation** on horizon paths that don't currently produce spans. PR3's scope. PR1 wires the registry; horizon's existing OpenTracing-instrumented code (Kafka RPC, Twin publish, etc.) gets real spans for free via the shim.
- **Changes to horizon's `TracerRegistry` interface.** Out of scope — the OT shim is the explicit migration path that lets us modernize the SDK without touching the interface.
- **DRY cleanup of the 3 @Bean factory methods.** Could in principle move all 3 into a shared parent or auto-configure class, but that's a separate code-organization PR.

## Related memory

- `project_daemon_nullop_audit_findings` — parent audit (corrected this session)
- `feedback_audit_verify_ctor_signatures` — verify before scoping; surfaced the 3rd NoOp site
- `feedback_karaf_is_dead` — applies: `NoOpTracerRegistry.java` deleted, no shim
- `feedback_rpc_timeout_no_outages` — honored by construction (tracing has no event-emission path)
- `feedback_grpc_is_for_remote_minion_only` — orthogonal: gRPC migration is daemon-gateway-to-Minion; tracing flows over OTLP-gRPC to a tracing backend, distinct from the IPC channel
- `feedback_horizon_parallel_vs_additive` — the OT shim is an *additive* abstraction (preserves horizon's API, adds OTel under the hood); not parallel
- `project_v1_2_app_observability` — strategic owner of the observability track this PR contributes to
