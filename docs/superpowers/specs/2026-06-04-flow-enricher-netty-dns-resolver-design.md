# Configurable Netty DNS reverse-resolution for the flow-enricher

**Date:** 2026-06-04
**Status:** Approved design
**Branch:** `feat/flow-enricher-netty-dns-resolver`

## Goal

Make the flow-enricher's reverse-DNS enrichment (src/dst — and, like Horizon, every IP in the
record — resolved to hostnames, populating `src_hostname` / `dst_hostname` / `next_hop_hostname`)
a **configurable** capability backed by Horizon's hardened `NettyDnsResolver`, replacing the
current always-off `NoOpDnsResolver`. Behavior is **Horizon-faithful by default**, with a curated
set of safety/tuning knobs plus an optional delta-v **CIDR scope gate** to structurally bound
lookup cardinality.

## Current state

`FlowEnricherConfiguration.flowParserDnsResolver()` returns a `NoOpDnsResolver` (both `lookup` and
`reverseLookup` return `Optional.empty()`), injected into all four horizon flow parsers
(Netflow v5/v9, IPFIX, sFlow); the sFlow parser additionally has `setDnsLookupsEnabled(false)`.
No `*_hostname` field is ever populated. The enrichment hot path performs node/interface lookups
via the in-memory NodeContext cache (post-migration), not DNS.

## How Horizon does it (the reference behavior)

`org.opennms.netmgt.telemetry.protocols.netflow.parser.RecordEnricher.enrich(record)`:
- Single `dnsLookupsEnabled` boolean gate (code default **`true`**). Off → empty enrichment, no lookups.
- An `IpAddressCapturingVisitor` walks **every** value in the record and collects **all** IP-typed
  fields into a `Set<InetAddress>` — exporter, src, dst, next-hop, etc. No src/dst or locality distinction.
- Fires `dnsResolver.reverseLookup(addr)` for each (async); `CompletableFuture.allOf(...)` gates the
  record's enrichment future until **all** lookups complete or time out. Failures resolve to `null`.
- The only bound is the `DnsResolver` implementation. In production Horizon that is `NettyDnsResolver`,
  whose circuit breaker + bulkhead + TTL cache + query timeout absorb the "resolve every IP in every
  flow" load. There is **no locality/CIDR filtering** anywhere.

## Decisions (confirmed)

- **On by default.** Master toggle `deltav.flow-enricher.dns.enabled` defaults **`true`**.
  This is a **behavior change** from current delta-v (which ships NoOp/off) — see the callout below.
- **Curated safety knobs + `nameservers`** exposed as config; obscure NettyDnsResolver internals
  hardcoded to Horizon's defaults.
- **CIDR scope gate** via a delta-v `LocalityFilteringDnsResolver` decorator; `scope` defaults
  **`all`** (= Horizon, no filtering). `private` is the opt-in structural cardinality cap.
- **All other defaults mirror Horizon's `NettyDnsResolver`** field defaults (enumerated below).

## ⚠️ Behavior change — reverse-DNS is ON by default

Shipping `enabled: true` means **every delta-v deployment begins reverse-resolving flow IPs** on
upgrade, where today it does none. With the default `scope: all`, that includes high-cardinality
internet-facing src/dst addresses — partially reintroducing the per-flow external-lookup cost the
NodeContext migration removed from the hot path. This is intentional (Horizon parity) but MUST be:
- called out in release notes / README as a default-behavior change, and
- mitigated by the load-bearing guards that are now in the default path: the circuit breaker,
  bulkhead concurrency cap, TTL+negative cache, and query timeout (all from `NettyDnsResolver`),
  plus the optional `scope: private` gate.

Operators running heavy internet-facing flows should consider `scope: private`, a shorter
`query-timeout-ms`, or `enabled: false`.

## Components

### 1. Configuration properties (`deltav.flow-enricher.dns.*`)

Daemon-scoped namespace (per the no-shared-config rule). Defaults = Horizon `NettyDnsResolver` defaults.

| Property | Type | Default | Maps to / meaning |
|---|---|---|---|
| `enabled` | boolean | **`true`** | master gate; off → `NoOpDnsResolver` |
| `scope` | enum `all`\|`private` | **`all`** | delta-v CIDR gate (see §3) |
| `nameservers` | string | `""` | `setNameservers` — `""` = platform default (system resolv.conf, port 53) |
| `query-timeout-ms` | long | `5000` | `setQueryTimeoutMillis` |
| `max-concurrent` | int | `1000` | `setBulkheadMaxConcurrentCalls` |
| `cache.min-ttl-s` | int | `-1` | `setMinTtlSeconds` (`-1` = CaffeineDnsCache default) |
| `cache.max-ttl-s` | int | `-1` | `setMaxTtlSeconds` |
| `cache.negative-ttl-s` | int | `-1` | `setNegativeTtlSeconds` |
| `circuit-breaker.enabled` | boolean | `true` | `setBreakerEnabled` |
| `circuit-breaker.failure-rate-threshold` | int | `80` | `setBreakerFailureRateThreshold` (percent) |

**Hardcoded to Horizon defaults** (not exposed): `numContexts=0` (→ derived from CPU at `init`),
`bulkheadMaxWaitDurationMillis = query-timeout-ms + 100`, `breakerWaitDurationInOpenState=15`,
`breakerRingBufferSizeInClosedState=100`, `breakerRingBufferSizeInHalfOpenState=10`,
`maxCacheSize=-1` (→ CaffeineDnsCache `DEFAULT_MAX_SIZE=10000`). `bulkheadMaxWaitDurationMillis`
is derived from `query-timeout-ms` to preserve Horizon's `queryTimeout+100` relationship.

A `@ConfigurationProperties("deltav.flow-enricher.dns")` record/bean (`FlowEnricherDnsProperties`)
binds these, with `@Validated` bounds (positive timeouts, threshold 1–100).

### 2. Resolver bean (conditional, lifecycle-managed) — delta-v-native

> **PIVOT (2026-06-04, after deploy-E2E):** the original plan reused horizon's
> `org.opennms.netmgt.dnsresolver.netty.NettyDnsResolver:1.0.11`. That artifact was compiled against
> **resilience4j 1.x** and its `init()` unconditionally calls `CircuitBreakerConfig.Builder
> .ringBufferSizeInHalfOpenState()/ringBufferSizeInClosedState()` — **removed in resilience4j 2.x**.
> The flow-enricher's Spring Boot 4 classpath mandates resilience4j **2.3.0**, so the horizon impl
> throws `NoSuchMethodError` and the application **fails to start** with DNS enabled. All unit tests
> passed because they mock the resolver and never call `init()`; the deploy E2E caught it.
> **Resolution:** drop the horizon `dnsresolver.netty` dependency and write a delta-v-native resolver
> against the current classpath (Boot-native adapter; the horizon impl is an OSGi-era shape).

- New `DeltavNettyDnsResolver implements org.opennms.netmgt.dnsresolver.api.DnsResolver` in
  `org.deltav.flows.enricher.parser` — built on **Netty `DnsNameResolver`** (`io.netty:netty-resolver-dns`,
  already on the classpath) for async forward (`resolve`) + reverse (PTR query against the
  `in-addr.arpa`/`ip6.arpa` name) lookups, bounded by **resilience4j 2.x** `CircuitBreaker`
  (`slidingWindowSize` / `failureRateThreshold` / `waitDurationInOpenState` /
  `permittedNumberOfCallsInHalfOpenState`) + `Bulkhead` (`maxConcurrentCalls` / `maxWaitDuration`),
  with a **Caffeine** cache (positive + negative, bounded size + TTL). Custom `nameservers` supported
  via the `DnsNameResolverBuilder` name-server provider (blank → platform default / system resolv.conf).
- `@Bean(initMethod = "init", destroyMethod = "close")` `@ConditionalOnProperty(name =
  "deltav.flows.dns.enabled", havingValue = "true", matchIfMissing = true)` — `init()` builds the
  Netty event loop + resolver + breaker + bulkhead + cache; `close()` shuts down the resolver +
  event loop. Configured entirely from `FlowEnricherDnsProperties`.
- The `bulkheadMaxWait = queryTimeoutMs + 100` relationship and all curated-knob defaults are
  preserved; the obscure knobs become sensible constants in the native impl.

### 3. `LocalityFilteringDnsResolver` decorator (delta-v, new)

`org.deltav.flows.enricher.parser.LocalityFilteringDnsResolver implements DnsResolver` — wraps the
delegate (`NettyDnsResolver`):
- `reverseLookup(InetAddress addr)`: if `scope == all` **or** `addr` is in the private/local set,
  delegate; else return `CompletableFuture.completedFuture(Optional.empty())` (and bump a
  `filtered` counter).
- `lookup(String)`: pass through to the delegate unchanged (Horizon's `RecordEnricher` only calls
  `reverseLookup`; forward lookups are out of the hot path).
- **IPv4-mapped IPv6 normalization (load-bearing).** Flow addresses arrive as IPv6 (the
  `flows_raw.src_address`/`dst_address` columns are `IPv6`), so a private IPv4 such as `10.0.0.1`
  can reach the decorator as an `Inet6Address` holding `::ffff:10.0.0.1`. `Inet6Address`'s
  `isSiteLocalAddress()` checks the **IPv6** site-local prefix and returns `false` for an
  IPv4-mapped private address — which would wrongly classify it as public and skip it under
  `scope: private`. So **before** classifying, normalize: if the address is a 16-byte IPv4-mapped
  form (`bytes[0..9]==0 && bytes[10]==bytes[11]==(byte)0xff`), reconstruct the embedded IPv4 via
  `InetAddress.getByAddress(Arrays.copyOfRange(bytes,12,16))` (yields an `Inet4Address`) and run the
  predicate on that. (Same IPv4-mapped-IPv6 gotcha the node-context migration's `IpNormalizer`
  handles.)
- **Private/local set** (`scope: private`, applied to the normalized address): IPv4 `10.0.0.0/8`,
  `172.16.0.0/12`, `192.168.0.0/16`, `127.0.0.0/8` (loopback), `169.254.0.0/16` (link-local); IPv6
  `fc00::/7` (ULA), `::1` (loopback), `fe80::/10` (link-local). Implemented with
  `isSiteLocalAddress() || isLoopbackAddress() || isLinkLocalAddress()` plus an explicit `fc00::/7`
  check (Java's `isSiteLocalAddress()` does not cover IPv6 ULA). Pure in-memory check; no allocation
  per call beyond the normalization copy for the mapped-IPv6 case.

This is an **additive** delta-v capability (Horizon has no equivalent) → delta-v adapter is the
correct layer (per the horizon-parallel-vs-additive rule), not a horizon source change.

### 4. Wiring into the parsers

`FlowEnricherConfiguration`:
- Replace the unconditional `flowParserDnsResolver()` → `NoOpDnsResolver` bean with a
  `@ConditionalOnProperty` pair:
  - `enabled` true or absent (`matchIfMissing = true`) → `LocalityFilteringDnsResolver(
    nettyDnsResolver, scope)` as the `DnsResolver`.
  - `enabled=false` explicitly (`havingValue = "false"`, no `matchIfMissing`) → `NoOpDnsResolver`
    (today's behavior). The two conditions are mutually exclusive, so exactly one `DnsResolver`
    bean exists.
- The Netflow5/9 + IPFIX parser beans already take the injected `DnsResolver` — no change beyond
  receiving the new bean.
- **sFlow:** make `parser.setDnsLookupsEnabled(...)` follow `enabled` (true when DNS enabled). Keep
  the PR #156 NPE note; the defensive-disable becomes conditional rather than hardcoded `false`.

### 5. Observability

`deltav_dns_*` Micrometer meters (daemon-agnostic naming, `opennms_*`/`deltav_*` convention):
- `deltav_dns_reverse_lookups_total{result=hit|miss|timeout|error|filtered}` — counter.
- `deltav_dns_circuit_breaker_state` — gauge (0 closed / 1 half-open / 2 open), if surfaceable from
  the Resilience4j breaker; otherwise omit.
- `deltav_dns_lookup_seconds` — timer on `reverseLookup` latency (so the hot-path cost is visible).
The decorator owns the `filtered` increment; the rest wrap the delegate's `reverseLookup`. If
`NettyDnsResolver` already exposes its own metrics, prefer bridging those over duplicating.

## Hot-path / performance considerations

- With `enabled: true` + `scope: all`, a record's enrichment future is gated (`allOf`) on **all**
  its IP lookups; an unresolvable IP can cost up to `query-timeout-ms` (default **5 s**) before that
  record completes, until the breaker trips (80% failure over a 100-sample ring → opens 15 s →
  fast-fail). The bulkhead caps concurrent in-flight lookups at 1000. The cache (incl. negative,
  default size 10 000) absorbs repeats.
- **Recommendation surfaced in docs:** for high-cardinality internet-facing flows, set
  `scope: private` (structural cap to your own ranges) and/or lower `query-timeout-ms`. These are
  the knobs that keep DNS-on-by-default from degrading flow throughput.
- The NodeContext-based node/interface enrichment is unchanged and remains DB-free; DNS is a
  separate, parser-level concern layered before document mapping.

## Testing

- **`LocalityFilteringDnsResolver`** unit tests: private IPv4 (10/8, 172.16/12, 192.168/16) and IPv6
  ULA (`fc00::/7`) + loopback + link-local → delegate called; public (`8.8.8.8`, `2001:4860::`) →
  empty, delegate NOT called, `filtered` counter bumped; `scope: all` → delegate always called;
  `lookup()` always delegates.
- **IPv4-mapped IPv6 edge cases (bulletproofing the normalization)** — construct the inputs as
  16-byte `Inet6Address` instances (via `InetAddress.getByAddress(byte[16])`, NOT
  `getByName("::ffff:…")` which Java may collapse to `Inet4Address`, hiding the bug):
  `::ffff:10.0.0.1`, `::ffff:172.16.0.1`, `::ffff:192.168.1.1`, `::ffff:127.0.0.1` → classified
  **private** (delegate called) under `scope: private`; `::ffff:8.8.8.8` → classified **public**
  (skipped). Assert that without the normalization step these private-mapped addresses would
  otherwise be mis-skipped — i.e. the test fails if the unwrap is removed.
- **Conditional wiring** (Spring slice / `ApplicationContextRunner`): `enabled=true` →
  `DnsResolver` bean is `LocalityFilteringDnsResolver` wrapping `NettyDnsResolver`; `enabled=false`
  → `NoOpDnsResolver`; sFlow `dnsLookupsEnabled` tracks `enabled`.
- **Config binding** test: properties map to the right `NettyDnsResolver` setters with the
  documented defaults; validation rejects non-positive timeouts / out-of-range threshold.
- **No live DNS in tests** — mock the delegate `DnsResolver`; assert delegation/short-circuit only.
- **E2E (next deploy):** with defaults, flows show populated `src_hostname`/`dst_hostname` for
  resolvable IPs; with `scope: private`, public-IP flows have empty hostnames while private ones
  resolve; flow throughput remains acceptable and no breaker-open storm under nl6 load.

## Out of scope

- Forward DNS (`lookup`) enrichment — `reverseLookup` only (matches Horizon's `RecordEnricher`).
- Changing which IPs the horizon parser collects (it resolves all IPs in the record; we gate by
  CIDR, we do not change the capture set).
- A per-field (src-only / dst-only) toggle — the parser resolves the whole record; field-level
  control would require a horizon change.
- Exposing the obscure NettyDnsResolver internals (ring buffers, numContexts, breaker wait) as config.

## Build order

1. `FlowEnricherDnsProperties` (`@ConfigurationProperties`) + defaults + validation, with tests.
2. `LocalityFilteringDnsResolver` decorator + CIDR logic + `filtered` metric, with unit tests.
3. `NettyDnsResolver` `@Bean` (conditional, init/destroy) wired from properties.
4. `FlowEnricherConfiguration`: conditional `DnsResolver` bean (Netty+decorator vs NoOp); sFlow flag
   follows `enabled`; context-runner wiring tests.
5. `deltav_dns_*` metrics.
6. `application.yml` defaults + README knob table + the on-by-default behavior-change note.
