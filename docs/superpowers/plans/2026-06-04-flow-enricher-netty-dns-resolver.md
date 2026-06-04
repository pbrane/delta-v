# Flow-Enricher Configurable Netty DNS Resolver Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the flow-enricher's always-off `NoOpDnsResolver` with Horizon's `NettyDnsResolver` behind a config gate (`deltav.flows.dns.*`, on by default), wrapped in a delta-v CIDR-scoping decorator.

**Architecture:** A `@ConfigurationProperties` record binds the curated knobs. When `enabled` (default true), a lifecycle-managed `NettyDnsResolver` bean is built from those props and wrapped in a `LocalityFilteringDnsResolver` (CIDR gate, default scope `all`); when `enabled=false`, the existing `NoOpDnsResolver` is used. The chosen `DnsResolver` is injected into the four horizon flow parsers exactly as today; the sFlow parser's `setDnsLookupsEnabled` tracks `enabled`.

**Tech Stack:** Java 21, Spring Boot 4, horizon `org.opennms.netmgt.dnsresolver.{api,netty}` 1.0.x, Micrometer, JUnit 5 + Mockito + AssertJ.

**Spec:** `docs/superpowers/specs/2026-06-04-flow-enricher-netty-dns-resolver-design.md`

**Namespace note:** The spec illustrated `deltav.flow-enricher.dns.*`; this plan uses **`deltav.flows.dns.*`** to match the module's existing properties (`deltav.flows.parser.system-id`, `deltav.flows.parser.location`).

---

## File Structure

- **Create** `core/flow-enricher/src/main/java/org/deltav/flows/enricher/parser/FlowEnricherDnsProperties.java` — `@ConfigurationProperties("deltav.flows.dns")` record (+ nested `Cache`, `CircuitBreaker`, `Scope` enum).
- **Create** `core/flow-enricher/src/main/java/org/deltav/flows/enricher/parser/LocalityFilteringDnsResolver.java` — CIDR-scoping `DnsResolver` decorator with IPv4-mapped-IPv6 normalization + `deltav_dns_*` metrics.
- **Modify** `core/flow-enricher/pom.xml` — add the `org.opennms.features.dnsresolver.netty` dependency.
- **Modify** `core/flow-enricher/src/main/java/org/deltav/flows/enricher/FlowEnricherConfiguration.java` — `@EnableConfigurationProperties`, conditional `NettyDnsResolver` bean, conditional `flowParserDnsResolver` (decorator vs NoOp), sFlow flag follows `enabled`.
- **Modify** `core/flow-enricher/src/main/resources/application.yml` — `deltav.flows.dns.*` defaults.
- **Modify** `opennms-container/delta-v/README.md` — knob table + on-by-default behaviour-change note.
- **Test** `core/flow-enricher/src/test/java/org/deltav/flows/enricher/parser/LocalityFilteringDnsResolverTest.java`
- **Test** `core/flow-enricher/src/test/java/org/deltav/flows/enricher/parser/FlowEnricherDnsPropertiesTest.java`
- **Test** `core/flow-enricher/src/test/java/org/deltav/flows/enricher/parser/FlowEnricherDnsWiringTest.java`

Build/test commands (run from repo root `/Users/david/development/src/opennms/delta-v`):
- Single test class: `./mvnw -q --projects :org.deltav.flows.flow-enricher test -Dtest=ClassName`
- Full module: `./mvnw -q --projects :org.deltav.flows.flow-enricher test`

---

## Task 1: Add the Netty DNS resolver dependency

**Files:**
- Modify: `core/flow-enricher/pom.xml` (dependencies section)

- [ ] **Step 1: Add the dependency**

In `core/flow-enricher/pom.xml`, inside `<dependencies>`, add (the `api` artifact is already on the classpath transitively via the netflow parser; this adds the **impl**):

```xml
        <dependency>
            <groupId>org.opennms.features.dnsresolver</groupId>
            <artifactId>org.opennms.features.dnsresolver.netty</artifactId>
        </dependency>
```

Do NOT hardcode `<version>`. The horizon dependencyManagement / BOM imported by the parent manages it (same as the sibling `org.opennms.features.telemetry.protocols.netflow.parser` dep already in this pom). If the build fails with "dependencies.dependency.version is missing", add `<version>1.0.11</version>` (the version currently resolvable in `~/.m2`), matching how other horizon deps in this pom pin versions.

- [ ] **Step 2: Verify it resolves and the impl class is reachable**

Run: `./mvnw -q --projects :org.deltav.flows.flow-enricher -am dependency:resolve 2>&1 | tail -5 && ./mvnw -q --projects :org.deltav.flows.flow-enricher compile`
Expected: BUILD SUCCESS, no missing-artifact error. (Compilation still passes — nothing references the new jar yet.)

- [ ] **Step 3: Commit**

```bash
git add core/flow-enricher/pom.xml
git commit -m "build(flow-enricher): add horizon netty DNS resolver dependency"
```

---

## Task 2: `FlowEnricherDnsProperties` configuration binding

**Files:**
- Create: `core/flow-enricher/src/main/java/org/deltav/flows/enricher/parser/FlowEnricherDnsProperties.java`
- Test: `core/flow-enricher/src/test/java/org/deltav/flows/enricher/parser/FlowEnricherDnsPropertiesTest.java`

- [ ] **Step 1: Write the failing test**

Create `FlowEnricherDnsPropertiesTest.java`:

```java
/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.flows.enricher.parser;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.context.ConfigurationPropertiesAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

class FlowEnricherDnsPropertiesTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(org.springframework.boot.autoconfigure.AutoConfigurations.of(
                    ConfigurationPropertiesAutoConfiguration.class))
            .withUserConfiguration(Config.class);

    @EnableConfigurationProperties(FlowEnricherDnsProperties.class)
    static class Config {}

    @Test
    void bindsHorizonDefaultsWhenNothingSet() {
        runner.run(ctx -> {
            FlowEnricherDnsProperties p = ctx.getBean(FlowEnricherDnsProperties.class);
            assertThat(p.enabled()).isTrue();
            assertThat(p.scope()).isEqualTo(FlowEnricherDnsProperties.Scope.ALL);
            assertThat(p.nameservers()).isEmpty();
            assertThat(p.queryTimeoutMs()).isEqualTo(5000L);
            assertThat(p.maxConcurrent()).isEqualTo(1000);
            assertThat(p.cache().minTtlS()).isEqualTo(-1);
            assertThat(p.cache().maxTtlS()).isEqualTo(-1);
            assertThat(p.cache().negativeTtlS()).isEqualTo(-1);
            assertThat(p.circuitBreaker().enabled()).isTrue();
            assertThat(p.circuitBreaker().failureRateThreshold()).isEqualTo(80);
        });
    }

    @Test
    void bindsOverridesIncludingRelaxedScopeEnum() {
        runner.withPropertyValues(
                "deltav.flows.dns.enabled=false",
                "deltav.flows.dns.scope=private",
                "deltav.flows.dns.nameservers=8.8.8.8",
                "deltav.flows.dns.query-timeout-ms=250",
                "deltav.flows.dns.max-concurrent=500",
                "deltav.flows.dns.cache.negative-ttl-s=120",
                "deltav.flows.dns.circuit-breaker.failure-rate-threshold=50"
        ).run(ctx -> {
            FlowEnricherDnsProperties p = ctx.getBean(FlowEnricherDnsProperties.class);
            assertThat(p.enabled()).isFalse();
            assertThat(p.scope()).isEqualTo(FlowEnricherDnsProperties.Scope.PRIVATE);
            assertThat(p.nameservers()).isEqualTo("8.8.8.8");
            assertThat(p.queryTimeoutMs()).isEqualTo(250L);
            assertThat(p.maxConcurrent()).isEqualTo(500);
            assertThat(p.cache().negativeTtlS()).isEqualTo(120);
            assertThat(p.circuitBreaker().failureRateThreshold()).isEqualTo(50);
        });
    }
}
```

- [ ] **Step 2: Run, verify it fails to compile** (`FlowEnricherDnsProperties` does not exist)

Run: `./mvnw -q --projects :org.deltav.flows.flow-enricher test -Dtest=FlowEnricherDnsPropertiesTest`
Expected: COMPILE FAILURE — cannot find symbol `FlowEnricherDnsProperties`.

- [ ] **Step 3: Write the properties record**

Create `FlowEnricherDnsProperties.java`:

```java
/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.flows.enricher.parser;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Curated, daemon-scoped tuning for the flow-enricher's reverse-DNS enrichment.
 * Defaults mirror horizon's {@code NettyDnsResolver} field defaults. The obscure
 * NettyDnsResolver knobs (numContexts, ring buffers, breaker wait, bulkhead
 * max-wait) are intentionally NOT exposed and stay at their horizon defaults;
 * {@code bulkheadMaxWaitDurationMillis} is derived as {@code queryTimeoutMs + 100}
 * at wiring time to preserve horizon's relationship.
 */
@ConfigurationProperties("deltav.flows.dns")
public record FlowEnricherDnsProperties(
        @DefaultValue("true") boolean enabled,
        @DefaultValue("all") Scope scope,
        @DefaultValue("") String nameservers,
        @DefaultValue("5000") long queryTimeoutMs,
        @DefaultValue("1000") int maxConcurrent,
        @DefaultValue Cache cache,
        @DefaultValue CircuitBreaker circuitBreaker) {

    /** Which addresses the locality gate allows through to the resolver. */
    public enum Scope { ALL, PRIVATE }

    public record Cache(
            @DefaultValue("-1") int minTtlS,
            @DefaultValue("-1") int maxTtlS,
            @DefaultValue("-1") int negativeTtlS) {}

    public record CircuitBreaker(
            @DefaultValue("true") boolean enabled,
            @DefaultValue("80") int failureRateThreshold) {}
}
```

- [ ] **Step 4: Run, verify it passes**

Run: `./mvnw -q --projects :org.deltav.flows.flow-enricher test -Dtest=FlowEnricherDnsPropertiesTest`
Expected: PASS (both tests).

- [ ] **Step 5: Commit**

```bash
git add core/flow-enricher/src/main/java/org/deltav/flows/enricher/parser/FlowEnricherDnsProperties.java \
        core/flow-enricher/src/test/java/org/deltav/flows/enricher/parser/FlowEnricherDnsPropertiesTest.java
git commit -m "feat(flow-enricher): DNS config properties (deltav.flows.dns.*) with horizon defaults"
```

---

## Task 3: `LocalityFilteringDnsResolver` decorator (CIDR gate + IPv4-mapped normalization + metrics)

**Files:**
- Create: `core/flow-enricher/src/main/java/org/deltav/flows/enricher/parser/LocalityFilteringDnsResolver.java`
- Test: `core/flow-enricher/src/test/java/org/deltav/flows/enricher/parser/LocalityFilteringDnsResolverTest.java`

- [ ] **Step 1: Write the failing test** (covers scope, the `filtered` metric, and the IPv4-mapped-IPv6 correctness edge case built via `getByAddress(byte[16])`)

Create `LocalityFilteringDnsResolverTest.java`:

```java
/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.flows.enricher.parser;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.InetAddress;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.opennms.netmgt.dnsresolver.api.DnsResolver;

class LocalityFilteringDnsResolverTest {

    private final DnsResolver delegate = mock(DnsResolver.class);
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();

    private LocalityFilteringDnsResolver privateOnly() {
        when(delegate.reverseLookup(any())).thenReturn(
                CompletableFuture.completedFuture(Optional.of("host.example")));
        return new LocalityFilteringDnsResolver(delegate, true, registry);
    }

    private static InetAddress v4(String s) throws Exception { return InetAddress.getByName(s); }

    /** Build a genuine 16-byte IPv4-mapped Inet6Address (getByName would collapse to Inet4Address). */
    private static InetAddress v4mapped(int a, int b, int c, int d) throws Exception {
        byte[] bytes = new byte[16];
        bytes[10] = (byte) 0xff;
        bytes[11] = (byte) 0xff;
        bytes[12] = (byte) a; bytes[13] = (byte) b; bytes[14] = (byte) c; bytes[15] = (byte) d;
        return InetAddress.getByAddress(bytes);
    }

    @Test
    void scopePrivate_delegatesForPrivateIpv4() throws Exception {
        var r = privateOnly();
        for (String ip : new String[]{"10.0.0.1", "172.16.5.5", "192.168.1.1", "127.0.0.1", "169.254.1.1"}) {
            r.reverseLookup(v4(ip));
        }
        verify(delegate, times(5)).reverseLookup(any());
        assertThat(registry.counter("deltav_dns_reverse_lookups_total", "result", "filtered").count()).isZero();
    }

    @Test
    void scopePrivate_skipsPublicIpv4AndCountsFiltered() throws Exception {
        var r = privateOnly();
        CompletableFuture<Optional<String>> f = r.reverseLookup(v4("8.8.8.8"));
        assertThat(f.join()).isEmpty();
        verify(delegate, never()).reverseLookup(any());
        assertThat(registry.counter("deltav_dns_reverse_lookups_total", "result", "filtered").count()).isEqualTo(1.0);
    }

    @Test
    void scopePrivate_ipv4MappedIpv6PrivateIsTreatedAsPrivate() throws Exception {
        var r = privateOnly();
        // ::ffff:10.0.0.1 etc. as real Inet6Address — must NOT be skipped
        r.reverseLookup(v4mapped(10, 0, 0, 1));
        r.reverseLookup(v4mapped(192, 168, 1, 1));
        r.reverseLookup(v4mapped(127, 0, 0, 1));
        verify(delegate, times(3)).reverseLookup(any());
        assertThat(registry.counter("deltav_dns_reverse_lookups_total", "result", "filtered").count()).isZero();
    }

    @Test
    void scopePrivate_ipv4MappedIpv6PublicIsSkipped() throws Exception {
        var r = privateOnly();
        assertThat(r.reverseLookup(v4mapped(8, 8, 8, 8)).join()).isEmpty();
        verify(delegate, never()).reverseLookup(any());
    }

    @Test
    void scopePrivate_ipv6UlaIsPrivate() throws Exception {
        var r = privateOnly();
        r.reverseLookup(InetAddress.getByName("fd00::1")); // fc00::/7 ULA
        verify(delegate, times(1)).reverseLookup(any());
    }

    @Test
    void scopeAll_alwaysDelegates() throws Exception {
        when(delegate.reverseLookup(any())).thenReturn(
                CompletableFuture.completedFuture(Optional.of("h")));
        var r = new LocalityFilteringDnsResolver(delegate, false, registry);
        r.reverseLookup(v4("8.8.8.8"));
        verify(delegate, times(1)).reverseLookup(any());
    }

    @Test
    void forwardLookupAlwaysDelegates() {
        when(delegate.lookup(any())).thenReturn(
                CompletableFuture.completedFuture(Optional.empty()));
        new LocalityFilteringDnsResolver(delegate, true, registry).lookup("example.com");
        verify(delegate, times(1)).lookup("example.com");
    }
}
```

- [ ] **Step 2: Run, verify it fails to compile** (class does not exist)

Run: `./mvnw -q --projects :org.deltav.flows.flow-enricher test -Dtest=LocalityFilteringDnsResolverTest`
Expected: COMPILE FAILURE — cannot find symbol `LocalityFilteringDnsResolver`.

- [ ] **Step 3: Write the decorator**

Create `LocalityFilteringDnsResolver.java`:

```java
/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.flows.enricher.parser;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import io.micrometer.core.instrument.MeterRegistry;
import org.opennms.netmgt.dnsresolver.api.DnsResolver;

/**
 * A {@link DnsResolver} decorator that, when {@code privateOnly} is set,
 * only reverse-resolves IPs in private/local ranges (RFC1918, IPv6 ULA,
 * loopback, link-local) and short-circuits everything else to an empty
 * result — bounding reverse-DNS cardinality to the operator's own address
 * space. When {@code privateOnly} is false, every address is delegated
 * (Horizon-equivalent behaviour).
 *
 * <p>Flow addresses arrive as IPv6 (the ClickHouse {@code flows_raw.*_address}
 * columns are IPv6), so a private IPv4 can reach us as an IPv4-mapped
 * {@code Inet6Address} (e.g. {@code ::ffff:10.0.0.1}). {@link InetAddress}'s
 * {@code isSiteLocalAddress()} checks the IPv6 prefix and would mis-classify
 * such an address as public; we therefore normalize IPv4-mapped IPv6 to the
 * embedded IPv4 before testing.
 */
public class LocalityFilteringDnsResolver implements DnsResolver {

    private static final String METRIC = "deltav_dns_reverse_lookups_total";

    private final DnsResolver delegate;
    private final boolean privateOnly;
    private final MeterRegistry registry;

    public LocalityFilteringDnsResolver(DnsResolver delegate, boolean privateOnly, MeterRegistry registry) {
        this.delegate = Objects.requireNonNull(delegate);
        this.privateOnly = privateOnly;
        this.registry = Objects.requireNonNull(registry);
    }

    @Override
    public CompletableFuture<Optional<InetAddress>> lookup(String hostname) {
        return delegate.lookup(hostname);
    }

    @Override
    public CompletableFuture<Optional<String>> reverseLookup(InetAddress address) {
        if (privateOnly && !isPrivate(address)) {
            registry.counter(METRIC, "result", "filtered").increment();
            return CompletableFuture.completedFuture(Optional.empty());
        }
        return delegate.reverseLookup(address).whenComplete((result, ex) -> {
            final String tag = ex != null ? "error" : (result != null && result.isPresent() ? "hit" : "miss");
            registry.counter(METRIC, "result", tag).increment();
        });
    }

    static boolean isPrivate(InetAddress address) {
        final InetAddress a = normalizeMapped(address);
        if (a.isSiteLocalAddress() || a.isLoopbackAddress() || a.isLinkLocalAddress()) {
            return true;
        }
        final byte[] b = a.getAddress();
        // IPv6 Unique Local Address fc00::/7 (Java has no isUniqueLocalAddress()).
        return b.length == 16 && (b[0] & 0xFE) == 0xFC;
    }

    /** Unwrap an IPv4-mapped IPv6 address (::ffff:a.b.c.d) to its embedded Inet4Address. */
    static InetAddress normalizeMapped(InetAddress address) {
        final byte[] b = address.getAddress();
        if (b.length == 16 && isV4Mapped(b)) {
            try {
                return InetAddress.getByAddress(Arrays.copyOfRange(b, 12, 16));
            } catch (UnknownHostException e) {
                return address; // 4-byte array is always valid; unreachable in practice
            }
        }
        return address;
    }

    private static boolean isV4Mapped(byte[] b) {
        for (int i = 0; i < 10; i++) {
            if (b[i] != 0) {
                return false;
            }
        }
        return b[10] == (byte) 0xff && b[11] == (byte) 0xff;
    }
}
```

- [ ] **Step 4: Run, verify it passes**

Run: `./mvnw -q --projects :org.deltav.flows.flow-enricher test -Dtest=LocalityFilteringDnsResolverTest`
Expected: PASS (all 7 tests). If `scopePrivate_ipv4MappedIpv6PrivateIsTreatedAsPrivate` fails, the `normalizeMapped` unwrap is the bug to fix — that test exists specifically to catch a missing unwrap.

- [ ] **Step 5: Commit**

```bash
git add core/flow-enricher/src/main/java/org/deltav/flows/enricher/parser/LocalityFilteringDnsResolver.java \
        core/flow-enricher/src/test/java/org/deltav/flows/enricher/parser/LocalityFilteringDnsResolverTest.java
git commit -m "feat(flow-enricher): LocalityFilteringDnsResolver CIDR gate + IPv4-mapped normalization"
```

---

## Task 4: Conditional resolver wiring in `FlowEnricherConfiguration`

**Files:**
- Modify: `core/flow-enricher/src/main/java/org/deltav/flows/enricher/FlowEnricherConfiguration.java`
- Test: `core/flow-enricher/src/test/java/org/deltav/flows/enricher/parser/FlowEnricherDnsWiringTest.java`

Context (current state): a single unconditional bean `flowParserDnsResolver()` returns `new NoOpDnsResolver()` (around line 246); the four parser beans inject `DnsResolver flowParserDnsResolver` by name; `sflowUdpParser(...)` calls `parser.setDnsLookupsEnabled(false)` unconditionally.

- [ ] **Step 1: Write the failing wiring test**

Create `FlowEnricherDnsWiringTest.java`. It verifies the two mutually-exclusive `flowParserDnsResolver` beans by invoking the `@Bean` factory methods directly with mocks (deterministic; no live Netty/DNS):

```java
/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.flows.enricher.parser;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.deltav.flows.enricher.FlowEnricherConfiguration;
import org.junit.jupiter.api.Test;
import org.opennms.netmgt.dnsresolver.api.DnsResolver;
import org.opennms.netmgt.dnsresolver.netty.NettyDnsResolver;

class FlowEnricherDnsWiringTest {

    private final FlowEnricherConfiguration cfg = new FlowEnricherConfiguration();

    private static FlowEnricherDnsProperties props(boolean enabled, FlowEnricherDnsProperties.Scope scope) {
        return new FlowEnricherDnsProperties(enabled, scope, "", 5000L, 1000,
                new FlowEnricherDnsProperties.Cache(-1, -1, -1),
                new FlowEnricherDnsProperties.CircuitBreaker(true, 80));
    }

    @Test
    void enabledYieldsLocalityFilteringDecorator() {
        DnsResolver r = cfg.flowParserDnsResolver(
                mock(NettyDnsResolver.class),
                props(true, FlowEnricherDnsProperties.Scope.ALL),
                new SimpleMeterRegistry());
        assertThat(r).isInstanceOf(LocalityFilteringDnsResolver.class);
    }

    @Test
    void disabledYieldsNoOpResolver() {
        DnsResolver r = cfg.noOpFlowParserDnsResolver();
        assertThat(r).isInstanceOf(NoOpDnsResolver.class);
    }
}
```

- [ ] **Step 2: Run, verify it fails to compile** (`cfg.flowParserDnsResolver(NettyDnsResolver, …)` / `noOpFlowParserDnsResolver()` signatures don't exist yet)

Run: `./mvnw -q --projects :org.deltav.flows.flow-enricher test -Dtest=FlowEnricherDnsWiringTest`
Expected: COMPILE FAILURE.

- [ ] **Step 3: Add imports + `@EnableConfigurationProperties` to `FlowEnricherConfiguration`**

In the imports block of `FlowEnricherConfiguration.java`, add:

```java
import io.micrometer.core.instrument.MeterRegistry;
import org.deltav.flows.enricher.parser.FlowEnricherDnsProperties;
import org.deltav.flows.enricher.parser.LocalityFilteringDnsResolver;
import org.opennms.netmgt.dnsresolver.netty.NettyDnsResolver;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
```

On the class declaration (the line `public class FlowEnricherConfiguration {` or its existing `@Configuration`), add the annotation directly above it:

```java
@EnableConfigurationProperties(FlowEnricherDnsProperties.class)
```

(Keep `import org.deltav.flows.enricher.parser.NoOpDnsResolver;` — it already exists.)

- [ ] **Step 4: Replace the single `flowParserDnsResolver()` bean with the conditional pair**

Delete the existing method (the javadoc block + `@Bean DnsResolver flowParserDnsResolver() { return new NoOpDnsResolver(); }`) and replace with:

```java
    /**
     * Lifecycle-managed horizon {@link NettyDnsResolver}, built only when
     * reverse-DNS is enabled. Configured from {@link FlowEnricherDnsProperties};
     * obscure knobs stay at horizon defaults. {@code init()}/{@code destroy()}
     * build and tear down the Netty event loops + Caffeine cache + breaker.
     */
    @Bean(initMethod = "init", destroyMethod = "destroy")
    @ConditionalOnProperty(name = "deltav.flows.dns.enabled", havingValue = "true", matchIfMissing = true)
    NettyDnsResolver nettyDnsResolver(
            EventForwarder flowParserEventForwarder,
            MetricRegistry flowEnricherMetricRegistry,
            FlowEnricherDnsProperties dnsProperties) {
        final NettyDnsResolver resolver = new NettyDnsResolver(flowParserEventForwarder, flowEnricherMetricRegistry);
        if (!dnsProperties.nameservers().isBlank()) {
            resolver.setNameservers(dnsProperties.nameservers());
        }
        resolver.setQueryTimeoutMillis(dnsProperties.queryTimeoutMs());
        resolver.setBulkheadMaxConcurrentCalls(dnsProperties.maxConcurrent());
        resolver.setBulkheadMaxWaitDurationMillis(dnsProperties.queryTimeoutMs() + 100);
        resolver.setMinTtlSeconds(dnsProperties.cache().minTtlS());
        resolver.setMaxTtlSeconds(dnsProperties.cache().maxTtlS());
        resolver.setNegativeTtlSeconds(dnsProperties.cache().negativeTtlS());
        resolver.setBreakerEnabled(dnsProperties.circuitBreaker().enabled());
        resolver.setBreakerFailureRateThreshold(dnsProperties.circuitBreaker().failureRateThreshold());
        return resolver;
    }

    /**
     * The {@link DnsResolver} injected into all four flow parsers. When DNS is
     * enabled (default), it is the {@link NettyDnsResolver} wrapped in a
     * {@link LocalityFilteringDnsResolver} (scope gate). Both conditional beans
     * below are named {@code flowParserDnsResolver}; the conditions are mutually
     * exclusive, so exactly one exists and the parsers' by-name injection works
     * unchanged.
     */
    @Bean("flowParserDnsResolver")
    @ConditionalOnProperty(name = "deltav.flows.dns.enabled", havingValue = "true", matchIfMissing = true)
    DnsResolver flowParserDnsResolver(
            NettyDnsResolver nettyDnsResolver,
            FlowEnricherDnsProperties dnsProperties,
            MeterRegistry meterRegistry) {
        final boolean privateOnly = dnsProperties.scope() == FlowEnricherDnsProperties.Scope.PRIVATE;
        return new LocalityFilteringDnsResolver(nettyDnsResolver, privateOnly, meterRegistry);
    }

    /** No-op resolver used only when reverse-DNS is explicitly disabled. */
    @Bean("flowParserDnsResolver")
    @ConditionalOnProperty(name = "deltav.flows.dns.enabled", havingValue = "false")
    DnsResolver noOpFlowParserDnsResolver() {
        return new NoOpDnsResolver();
    }
```

- [ ] **Step 5: Make the sFlow flag follow `enabled`**

In `sflowUdpParser(...)`, add a `FlowEnricherDnsProperties` parameter and replace the hardcoded `false`:

```java
    @Bean
    SFlowUdpParser sflowUdpParser(
            ThreadLocalDispatcher threadLocalDispatcher,
            DnsResolver flowParserDnsResolver,
            FlowEnricherDnsProperties dnsProperties) {
        final SFlowUdpParser parser = new SFlowUdpParser(
                "SFlow",
                threadLocalDispatcher,
                flowParserDnsResolver);
        // Reverse-DNS on the sFlow parser tracks the global toggle. The PR #156
        // NPE on unresolvable Docker-bridge IPs is avoided when disabled; when
        // enabled, the NettyDnsResolver path handles failures gracefully.
        parser.setDnsLookupsEnabled(dnsProperties.enabled());
        return parser;
    }
```

- [ ] **Step 6: Run the wiring test + compile**

Run: `./mvnw -q --projects :org.deltav.flows.flow-enricher test -Dtest=FlowEnricherDnsWiringTest`
Expected: PASS (both tests).

- [ ] **Step 7: Commit**

```bash
git add core/flow-enricher/src/main/java/org/deltav/flows/enricher/FlowEnricherConfiguration.java \
        core/flow-enricher/src/test/java/org/deltav/flows/enricher/parser/FlowEnricherDnsWiringTest.java
git commit -m "feat(flow-enricher): conditional Netty DNS resolver wiring (default on) + sFlow flag follows toggle"
```

---

## Task 5: Defaults in `application.yml` + README

**Files:**
- Modify: `core/flow-enricher/src/main/resources/application.yml`
- Modify: `opennms-container/delta-v/README.md`

- [ ] **Step 1: Add the `deltav.flows.dns` block to `application.yml`**

At the end of `application.yml` add a top-level `deltav:` block (if one already exists, merge under it). Values mirror the record defaults but are written explicitly so operators see them, with env-var overrides:

```yaml
# Reverse-DNS enrichment of flow IPs (src/dst/etc -> *_hostname). ON by default
# (Horizon parity). Backed by horizon's NettyDnsResolver (circuit breaker +
# bulkhead + TTL cache + timeout). For high-cardinality internet-facing flows,
# set scope=private or lower query-timeout-ms, or disable entirely.
deltav:
  flows:
    dns:
      enabled: ${DELTAV_FLOWS_DNS_ENABLED:true}
      scope: ${DELTAV_FLOWS_DNS_SCOPE:all}            # all | private
      nameservers: ${DELTAV_FLOWS_DNS_NAMESERVERS:}   # blank = system resolv.conf
      query-timeout-ms: ${DELTAV_FLOWS_DNS_QUERY_TIMEOUT_MS:5000}
      max-concurrent: ${DELTAV_FLOWS_DNS_MAX_CONCURRENT:1000}
      cache:
        min-ttl-s: ${DELTAV_FLOWS_DNS_CACHE_MIN_TTL_S:-1}
        max-ttl-s: ${DELTAV_FLOWS_DNS_CACHE_MAX_TTL_S:-1}
        negative-ttl-s: ${DELTAV_FLOWS_DNS_CACHE_NEGATIVE_TTL_S:-1}
      circuit-breaker:
        enabled: ${DELTAV_FLOWS_DNS_BREAKER_ENABLED:true}
        failure-rate-threshold: ${DELTAV_FLOWS_DNS_BREAKER_FAILURE_RATE:80}
```

- [ ] **Step 2: Add the README knob table + behaviour-change note**

In `opennms-container/delta-v/README.md`, in the flow-enricher section, add:

```markdown
### Flow reverse-DNS enrichment (`deltav.flows.dns.*`)

**Behaviour change (v1.3.0-rc9+):** reverse-DNS enrichment is **ON by default**
(Horizon parity). The flow-enricher now reverse-resolves flow IPs to hostnames
(`src_hostname`/`dst_hostname`/…) via horizon's `NettyDnsResolver`. Previously
delta-v shipped a no-op resolver (no lookups). Disable with
`DELTAV_FLOWS_DNS_ENABLED=false`.

| Env var | Default | Meaning |
|---|---|---|
| `DELTAV_FLOWS_DNS_ENABLED` | `true` | master on/off |
| `DELTAV_FLOWS_DNS_SCOPE` | `all` | `all` (resolve every IP) or `private` (only RFC1918/ULA/loopback/link-local — bounds cardinality to your address space) |
| `DELTAV_FLOWS_DNS_NAMESERVERS` | _(blank)_ | DNS server(s); blank = system resolv.conf |
| `DELTAV_FLOWS_DNS_QUERY_TIMEOUT_MS` | `5000` | per-lookup timeout |
| `DELTAV_FLOWS_DNS_MAX_CONCURRENT` | `1000` | bulkhead: max in-flight lookups |
| `DELTAV_FLOWS_DNS_CACHE_*_TTL_S` | `-1` | cache min/max/negative TTL (`-1` = NettyDnsResolver default) |
| `DELTAV_FLOWS_DNS_BREAKER_ENABLED` / `_FAILURE_RATE` | `true` / `80` | circuit breaker on unhealthy DNS |

For high-cardinality internet-facing flows, prefer `DELTAV_FLOWS_DNS_SCOPE=private`
and/or a lower `DELTAV_FLOWS_DNS_QUERY_TIMEOUT_MS` to protect flow throughput.
Metrics: `deltav_dns_reverse_lookups_total{result=hit|miss|error|filtered}`.
```

- [ ] **Step 3: Validate YAML + commit**

Run: `./mvnw -q --projects :org.deltav.flows.flow-enricher test -Dtest=FlowEnricherDnsPropertiesTest` (confirms the yml-shaped keys still bind via the record).
Expected: PASS.

```bash
git add core/flow-enricher/src/main/resources/application.yml opennms-container/delta-v/README.md
git commit -m "docs(flow-enricher): default deltav.flows.dns.* config + on-by-default README note"
```

---

## Task 6: Full-module build + verification

**Files:** none (verification)

- [ ] **Step 1: Build + run the whole module**

Run: `./mvnw -q --projects :org.deltav.flows.flow-enricher -am clean test`
Expected: BUILD SUCCESS; all tests pass (existing enrichment tests + the three new DNS test classes).

- [ ] **Step 2: Confirm no leftover unconditional NoOp reference + exactly one DnsResolver path**

Run:
```bash
grep -n 'new NoOpDnsResolver' core/flow-enricher/src/main/java/org/deltav/flows/enricher/FlowEnricherConfiguration.java
grep -n 'setDnsLookupsEnabled' core/flow-enricher/src/main/java/org/deltav/flows/enricher/FlowEnricherConfiguration.java
```
Expected: the `new NoOpDnsResolver()` appears only inside the `@ConditionalOnProperty(... havingValue="false")` bean; `setDnsLookupsEnabled` uses `dnsProperties.enabled()` (not a literal `false`).

- [ ] **Step 3: Final commit (if any fixups)**

```bash
git add -A && git commit -m "test(flow-enricher): verify DNS resolver module build green" --allow-empty
```

> **Deferred E2E (next deploy/rc):** with defaults (`enabled=true`, `scope=all`), confirm flows show populated `src_hostname`/`dst_hostname` for resolvable IPs and the `deltav_dns_reverse_lookups_total` meter increments; with `DELTAV_FLOWS_DNS_SCOPE=private`, public-IP flows have empty hostnames (and `result=filtered` increments) while private ones resolve; flow throughput stays healthy with no circuit-breaker-open storm under nl6 load.

---

## Self-Review

**Spec coverage:**
- On-by-default toggle + behaviour-change callout → Task 4 (conditional, `matchIfMissing=true`), Task 5 (README note). ✓
- Curated knobs + nameservers, Horizon defaults → Task 2 (record defaults), Task 4 (setters), Task 5 (yml). ✓
- Hardcoded obscure knobs; `bulkheadMaxWait = queryTimeout+100` → Task 4 (only curated setters called; derived max-wait). ✓
- `LocalityFilteringDnsResolver` CIDR gate, scope all|private default all → Task 3 + Task 4 (scope→privateOnly). ✓
- IPv4-mapped IPv6 normalization → Task 3 (`normalizeMapped`) + explicit tests. ✓
- `deltav_dns_*` metrics (hit/miss/error/filtered) → Task 3. ✓
- sFlow flag follows enabled → Task 4 Step 5. ✓
- NettyDnsResolver lifecycle init/destroy → Task 4 Step 4. ✓
- Tests: decorator (incl mapped), properties binding, conditional wiring → Tasks 2/3/4. ✓

**Placeholder scan:** No TBD/TODO; every code step shows complete code. ✓

**Type consistency:** `FlowEnricherDnsProperties` accessor names (`enabled()`, `scope()`, `nameservers()`, `queryTimeoutMs()`, `maxConcurrent()`, `cache().minTtlS()/maxTtlS()/negativeTtlS()`, `circuitBreaker().enabled()/failureRateThreshold()`) are identical in Tasks 2, 4, and the wiring test. `Scope.ALL`/`Scope.PRIVATE` consistent. `LocalityFilteringDnsResolver(DnsResolver, boolean, MeterRegistry)` ctor identical in Tasks 3 and 4. Bean name `flowParserDnsResolver` matches the parsers' injection point. ✓

**Open item flagged (not a placeholder):** Task 1 Step 1 — the netty dep version is managed by horizon's dependencyManagement; if unmanaged, pin `1.0.11`. The engineer confirms at build time.
