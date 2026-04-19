# M2 — Collectd ServiceParameters identity-populate gap (design)

- **Date:** 2026-04-19
- **Branch:** `fix/collectd-serviceparameters-identity`
- **PR target:** `pbrane/delta-v` `develop` (never `OpenNMS/opennms`)
- **Status:** design draft (awaiting user review before implementation plan)
- **Primary memo:** `project_collectd_serviceparameters_identity_gap`
- **Follow-on from:** delta-v#175 (consumer-side fixes for `NodeContextKafkaBootstrap` race, SCS-binder topic-partition race, location-empty cache-key fallback)

## 1. Context and problem

After delta-v#175, the Phase 2 Prometheus-remote-write consumer (`core/prometheus-writer`) correctly reads every `TimeseriesBatch` record published by Collectd. `records_consumed_total` is non-zero but `samples_sent_total` stays at zero. The root cause is producer-side: Delta-V's standalone Collectd (`core/daemon-boot-collectd`) publishes every record with `nodeId = 0` and `location = ""`. Consumer enrichment cache keys never match, so every record hits `enrichment_missing` and is dropped.

The existing producer (`TimeseriesKafkaPersister`) reads `node-id` and `location` from a `ServiceParameters` map:

```java
this.nodeId = parseIntOrZero(asString(params.get("node-id"), ""));
this.location = asString(params.get("location"), "");
```

`ServiceParameters` in this context is horizon's `CollectionSpecification.getServiceParameters()` — a static wrapper around the collectd-configuration.xml package parameters, wrapped `Collections.unmodifiableMap(...)`. The horizon API **never populates `node-id` or `location`** into that map (the enum `ServiceParameters.ParameterName` has no such slot) and the per-collection `CollectionAgent` — which does know both — is not threaded through the `Persister` API. Consequently, `parseIntOrZero(null)` silently returns 0 and the empty-string location is a silent default.

**M2 scope:** producer-side fix. Populate the persister with real `nodeId` + `location` from the `CollectionAgent` on every collection cycle, and fail loudly at publish time if identity is missing so the bug cannot silently regress.

**Out of scope:** consumer-side changes (delta-v#175 shipped those); the three deferred horizon-inner-persister bugs (#1/#2/#4 per `project_phase0_inner_persister_bugs_followup`) — they affect the inner `TimeseriesPersister` path, not the Kafka producer; any `ServiceParameters` contract change in horizon.

## 2. Acceptance criteria

- `ServiceParameters.nodeId` / `.location` reads are removed from `TimeseriesKafkaPersister`; identity is sourced from a Delta-V-owned capture path.
- Silent defaults (`parseIntOrZero` helper, empty-location fallback) are deleted; a single `IllegalStateException` guards the publish site.
- New unit tests cover the populated-identity contract, the missing-identity failure, and the `nodeId <= 0` failure path.
- New integration test asserts the Spring wiring so the decorator bean is the one injected into downstream consumers.
- `./mvnw -pl core/daemon-boot-collectd verify` green.
- `./mvnw -B -DskipTests -fae clean install` full-reactor green (per `feedback_delta_v_full_reactor_verify`).
- `./opennms-container/delta-v/test-prometheus-writer-e2e.sh` exits 0 with `ALL ASSERTIONS PASSED` — first successful end-to-end Phase 2 E2E.
- `enrichment_lookup_fallback_total` counter (from delta-v#175) stays at 0 throughout the E2E run.
- Post-merge memory updates: `project_collectd_serviceparameters_identity_gap` status flip OPEN → RESOLVED (delta-v#<PR>); `project_phase2_prometheus_writer_done` addendum noting full E2E green as of delta-v#<PR>.

## 3. Architecture

The design rests on two observations:

1. **Horizon's `Persister` never receives a `CollectionAgent`.** Upstream persisters extract `nodeId` from `CollectionResource.getPath()` (format-dependent) and never have a direct handle on location. Adding identity to `ServiceParameters` fights the horizon API — that map is static package config, wrapped unmodifiable by `CollectionSpecification.getServiceParameters()`.
2. **`LocationAwareCollectorClient` is the single Delta-V-reachable bean that sees the `CollectionAgent` on every cycle.** Inside `CollectorRequestBuilderImpl.execute()` (horizon, `features/collection/client-rpc`), both `agent.getNodeId()` and `agent.getLocationName()` are live on the Collectd scheduler thread, immediately before RPC dispatch. `CollectableService.doCollection()` blocks on the resulting `CompletableFuture.get()`, so control returns to the same thread before `createPersister` runs.

The fix introduces two thin Delta-V beans that collaborate through a request-scoped holder:

1. **`AgentIdentityHolder`** — a `ThreadLocal<AgentIdentity>` with `set`, `getOrThrow`, `clear`. Singleton bean.
2. **`AgentIdentityCapturingCollectorClient`** — `@Primary` Spring decorator over the horizon `LocationAwareCollectorClient` bean. Captures `agent` via `withAgent`, and at `execute()` time calls `holder.set(agent.getNodeId(), agent.getLocationName())` before delegating. Dumb conduit: captures whatever the agent provided, no validation, never throws on identity shape (per Q2 decision: persist-time-only fail-fast).
3. **`TimeseriesKafkaPersister`** — new constructor `(TimeseriesKafkaPublisher, String collectionPackage, AgentIdentityHolder)` (per Q4 decision: option A2, explicit dependencies). At `completeCollectionSet`, reads identity, validates `nodeId > 0`, publishes, and always clears the holder in `finally`.
4. **`FanoutPersisterFactory`** — extracts the `collection` string from `ServiceParameters` once per `createPersister` call, passes it and the injected holder to the new persister constructor.

**Left untouched:** `CollectionSetToProtobufTranslator` (already takes `nodeId`/`location` as explicit args), `TimeseriesKafkaPublisher`, horizon's `TimeseriesPersisterFactory` and inner `TimeseriesPersister` path, `CollectdRpcConfiguration`.

### 3.1 Spring bean wiring

Verified in the delta-v codebase: the horizon bean is defined in `core/daemon-boot-collectd/src/main/java/org/deltav/netmgt/collectd/boot/CollectdRpcConfiguration.java` as a plain `@Bean`. Spring Boot 4 disables bean definition overriding by default, so the decorator uses a different bean name + `@Primary`:

```java
@Bean
@Primary
public LocationAwareCollectorClient agentIdentityCapturingCollectorClient(
        @Qualifier("locationAwareCollectorClient") LocationAwareCollectorClient inner,
        AgentIdentityHolder holder) {
    return new AgentIdentityCapturingCollectorClient(inner, holder);
}
```

Both new beans (`AgentIdentityHolder`, `AgentIdentityCapturingCollectorClient`) are gated by the existing `@ConditionalOnProperty("deltav.timeseries.enabled=true")` on `TimeseriesKafkaPublisherConfiguration`. When off, horizon's raw bean is injected as today and the Kafka publish path is not wired — zero behavioral change for non-Kafka deployments.

### 3.2 AgentIdentityHolder contract invariants

- **`set(nodeId, location)`** — unconditional overwrite. Never throws. Normalizes null location to `""`. This is the sole protection against thread reuse with stale identity between cycles on the same scheduler thread.
- **`getOrThrow()`** — returns current identity, or throws `IllegalStateException("AgentIdentity not populated — LocationAwareCollectorClient decorator not wired?")` if the slot is empty. Called exactly once per cycle at `completeCollectionSet` time.
- **`clear()`** — idempotent `ThreadLocal.remove()`. Called from `completeCollectionSet`'s outer `finally` block so it always runs regardless of whether `capturedSet` was null or whether publish threw.

### 3.3 AgentIdentity record contract

A dumb immutable data carrier. No validation on `nodeId`. Location normalized to `""` when null. Validation lives exclusively at the persist site.

## 4. Data flow (one collection cycle)

```
┌─────────────────────────────────────────────────────────────────────────┐
│ CollectableService.doCollection()   [Collectd scheduler thread]
└─────────────────────────────────────────────────────────────────────────┘
    │
    │ 1. m_spec.getServiceParameters()  ─▶ ServiceParameters (static package config)
    │
    │ 2. m_spec.collect(m_agent)
    │    └─▶ locationAwareCollectorClient.collect()        ← @Primary decorator
    │           .withAgent(agent)                          ← captures agent
    │           .withCollectorClassName(...)
    │           .execute()                                 ← DECORATOR HOOK
    │                │
    │                ├─▶ holder.set(agent.getNodeId(), agent.getLocationName())
    │                └─▶ delegate.execute()     [RPC to Minion; blocks on .get()]
    │                         │
    │                         ▼
    │                    CollectionSet
    │
    │ 3. persisterFactory.createPersister(serviceParameters, …)
    │    └─▶ FanoutPersisterFactory
    │           ├── collectionPackage = params.get("collection") — "default" fallback
    │           └── new TimeseriesKafkaPersister(publisher, collectionPackage, holder)
    │
    │ 4. result.visit(persister)
    │    └─▶ FanoutPersister walks visit steps for both inner + kafka persisters
    │           └── completeCollectionSet on kafka persister:
    │                   try {
    │                     if (capturedSet != null) {
    │                       identity = holder.getOrThrow()      ← still set from step 2
    │                       if (identity.nodeId() <= 0) throw …
    │                       publisher.publish(capturedSet, collectionPackage,
    │                                         identity.nodeId(), identity.location())
    │                     }
    │                   } finally {
    │                     holder.clear()
    │                     capturedSet = null
    │                   }
    │
    │ 5. thresholding + status update (unchanged)
    ▼
```

### 4.1 Invariants the flow relies on

1. **Same-thread synchronous window.** Steps 2→3→4 run on one scheduler thread. The RPC future is `.get()`-blocked inside horizon's `CollectionSpecification.collect()`, so control returns on the same thread before `createPersister` runs. No cross-thread hand-off bridges a thread boundary the holder can't see through.
2. **Single capture point.** Any bypass (e.g., a test wiring that injects the raw `LocationAwareCollectorClientImpl` directly) fails loudly at step 4's `getOrThrow()`. No secondary population site to keep in sync.
3. **Holder lifetime bounded by the cycle.** Set in step 2 (unconditional overwrite), read + cleared in step 4 (try/finally). Unconditional-overwrite semantics make cleanup best-effort rather than load-bearing: an exception anywhere between steps 2 and 4 never causes cross-cycle contamination, because the next cycle's step 2 overwrites.

### 4.2 Edge cases

- **Inner persister throws mid-visit** (horizon bugs #1/#2/#4): `FanoutPersister.runInner` catches the throwable, continues to the Kafka persister; `completeCollectionSet` still runs on the Kafka side; publish + clear succeed.
- **Kafka persister's own `getOrThrow()` or `publish` throws:** the outer `finally` still clears. `FanoutPersister.runKafka` catches the `IllegalStateException`, increments `deltav.collectd.persister.kafka.failures{step=completeCollectionSet}`, logs WARN.
- **`CollectionSet` is null (failed collection):** `visitCollectionSet` was never called with a non-null set → `capturedSet` stays null → the `if (capturedSet != null)` short-circuits → the outer `finally` still clears the holder (idempotent no-op if already empty).
- **RPC timeout:** `execute()` already set the holder before dispatching. `.get()` throws. `doCollection` propagates. Holder remains set until the next cycle overwrites — no record is published with bogus identity because `completeCollectionSet` never runs.

## 5. Error handling and fail-fast semantics (persist-time only)

Per Q2 decision: **fail-fast at persist time, not capture time.** The capture path never aborts a collection; only the Kafka publish is refused when identity is invalid.

**Single validation site** — `TimeseriesKafkaPersister.completeCollectionSet`:

```java
@Override
public void completeCollectionSet(CollectionSet set) {
    try {
        if (capturedSet != null) {
            AgentIdentity identity = holder.getOrThrow();
            if (identity.nodeId() <= 0) {
                throw new IllegalStateException(
                    "Invalid agent identity for Kafka publish: nodeId must be > 0, got "
                    + identity.nodeId());
            }
            publisher.publish(capturedSet, collectionPackage,
                              identity.nodeId(), identity.location());
        }
    } finally {
        holder.clear();
        capturedSet = null;
    }
}
```

Both failure modes — "decorator not wired" (from `getOrThrow`) and "malformed nodeId" (explicit check) — surface as `IllegalStateException` caught by `FanoutPersister.runKafka`, which increments `deltav.collectd.persister.kafka.failures{step=completeCollectionSet}` and logs WARN. The inner persister (RRD/Newts) path is unaffected — it completes normally. Ops alert rule: `rate(deltav_collectd_persister_kafka_failures_total{step="completeCollectionSet"}[5m]) > 0`.

Legitimate `nodeId == 0` edge cases (if any ever surface — not evidenced today) aborting only the Kafka publish for that cycle is the correct behavior: inner persistence still happens, RPC still fires, only the Kafka record is refused.

## 6. Observability

- **Metric counter** (pre-existing, reused): `deltav.collectd.persister.kafka.failures{step=completeCollectionSet}`. `FanoutPersister.preRegisterCounters` already registers this at construction so ops can alert on `rate > 0`.
- **Downstream smoke test** (pre-existing, from delta-v#175): `enrichment_lookup_fallback_total` on the consumer side. Should stay at 0 post-M2, indicating Collectd is populating `location` correctly and consumer fallback is not being triggered. Spike above 0 = location populated but doesn't match what provisiond emits.
- **Log line** (INFO, once at startup): `AgentIdentityCapturingCollectorClient` logs its construction — `"Wrapping LocationAwareCollectorClient for identity capture; delegate = {}"` — for operational confirmation that the decorator is in play.

## 7. Components — file-level changes

**New files** (all under `core/daemon-boot-collectd/src/main/java/org/deltav/collectd/identity/`):

1. `AgentIdentity.java` — `record AgentIdentity(int nodeId, String location)` with compact constructor normalizing null location to `""`. No nodeId validation.
2. `AgentIdentityHolder.java` — ThreadLocal holder with `set` / `getOrThrow` / `clear` per §3.2.
3. `AgentIdentityCapturingCollectorClient.java` — implements `LocationAwareCollectorClient`; delegates everything to the wrapped client except `collect()`, which returns a wrapped builder.
4. `AgentIdentityCapturingCollectorRequestBuilder.java` (package-private static nested inside #3 — acceptable because the outer class is its sole consumer) — implements `CollectorRequestBuilder`. `withAgent` captures reference; all `withXxx` return `this`; `execute()` sets holder (if agent captured) and delegates.

**Modified files:**

5. `TimeseriesKafkaPersister.java` — new constructor `(TimeseriesKafkaPublisher, String collectionPackage, AgentIdentityHolder)`; `parseIntOrZero`, `asString`, and all `ServiceParameters` identity reads deleted; `completeCollectionSet` rewritten per §5; Javadoc updated to document the holder-based identity source.
6. `TimeseriesKafkaPublisherConfiguration.java`:
   - New `@Bean AgentIdentityHolder agentIdentityHolder()`.
   - New `@Bean @Primary AgentIdentityCapturingCollectorClient` (snippet in §3.1).
   - `FanoutPersisterFactory` constructor gains `AgentIdentityHolder holder`; `createPersister` now extracts `collection` from `ServiceParameters` once (via `(String) params.getParameters().getOrDefault("collection", "default")`) and passes both the string and the holder to the new persister constructor.

**Explicitly untouched:** `CollectionSetToProtobufTranslator`, `TimeseriesKafkaPublisher`, `CollectdRpcConfiguration`, `CollectdDaemonConfiguration`, `opennms-container/delta-v/` resources, horizon's inner `TimeseriesPersister` path.

## 8. Testing strategy

**Unit tests** (new files under `core/daemon-boot-collectd/src/test/java/org/deltav/collectd/identity/`):

- **`AgentIdentityTest`** — null location normalizes to `""`; non-null passes through; record equality on both fields.
- **`AgentIdentityHolderTest`** — (1) set then getOrThrow returns; (2) getOrThrow on empty throws IllegalStateException with diagnostic message; (3) set → clear → getOrThrow throws; (4) set with null location → getOrThrow.location() is ""; (5) double set overwrites silently; (6) thread isolation: set on A, assert B's getOrThrow throws.
- **`AgentIdentityCapturingCollectorRequestBuilderTest`** — (1) withAgent + execute sets holder with agent's nodeId + location; (2) withAgent omitted + execute → holder not set (delegate handles its own validation); (3) all `withXxx` return `this` and call delegate; (4) agent with `nodeId == 0` captured as-is — no throw at this layer; (5) agent with null location captured as `""`.
- **`AgentIdentityCapturingCollectorClientTest`** — every `LocationAwareCollectorClient` method other than `collect()` forwards straight to the wrapped client; `collect()` returns a decorated builder.
- **`TimeseriesKafkaPersisterTest` (rewrite, existing file)** — (1) populated holder → publisher.publish with right identity, holder cleared; (2) empty holder → IllegalStateException, holder cleared idempotently; (3) holder with nodeId == 0 → IllegalStateException with "nodeId must be > 0" in message, holder cleared; (4) holder with nodeId == -1 → same; (5) capturedSet == null path → no publish call, holder still cleared; (6) `visitResource/Group/Attribute/persistNumeric/persistString` still no-ops.

**Integration test** (new file under `core/daemon-boot-collectd/src/test/java/org/deltav/collectd/timeseries/`):

- **`AgentIdentityWiringIT`** — extends `CollectdApplicationScanIT` Spring-context pattern. Boots with `deltav.timeseries.enabled=true`, mocked broker + DB. Asserts: (1) `LocationAwareCollectorClient` bean resolution returns the decorator via `@Primary`; (2) the instance injected into `Collectd` is `instanceof AgentIdentityCapturingCollectorClient`; (3) a manual `client.collect().withAgent(fakeAgent).withCollector(noopCollector).execute()` sets the holder (read immediately after via `agentIdentityHolder.getOrThrow()`); (4) `FanoutPersisterFactory.createPersister(serviceParameters, repository)` produces a `FanoutPersister` whose kafka-side delegate has the holder wired (verify via a package-private accessor added on the persister for test purposes, OR by driving a full collection cycle and asserting the publisher was called with the captured identity).

- **`TimeseriesPublisherFeatureFlagOffIT` (existing, extended)** — one new assertion: when `deltav.timeseries.enabled=false`, the `LocationAwareCollectorClient` bean is NOT the decorator; the horizon bean wins. Keeps the off-path honest.

**Not in automated suite:**

- **Phase 2 E2E** (`./opennms-container/delta-v/test-prometheus-writer-e2e.sh`) — mandatory local verification per the prompt's success criteria, not wired into `./mvnw verify`. PR description will cite it as the acceptance gate with captured log excerpt showing `ALL ASSERTIONS PASSED` + `enrichment_lookup_fallback_total == 0`.
- Tests exercising the three deferred horizon-inner bugs — out of scope per `project_phase0_inner_persister_bugs_followup`.

**Scope estimate:** ~15 new test methods across 5 new test classes + 1 rewrite + 1 extension. ~300 lines of test code. All fast (no broker, no DB, no Docker).

## 9. Rollout

- **Branch:** `fix/collectd-serviceparameters-identity` (new, not reusing `fix/collectd-publisher-inert` which was merged and deleted).
- **PR target:** `gh pr create --repo pbrane/delta-v --base develop ...` — per `feedback_never_pr_opennms`.
- **Commit convention:** `fix(collectd):` per the conventional-commit pattern used by adjacent PRs (#174, #175).
- **Code conventions:** all new files under `org.deltav.collectd.identity.*` with BeaconStrategists copyright header per `feedback_deltav_package_namespace`.
- **Build verification before PR:**
  - `./mvnw -pl core/daemon-boot-collectd verify` (module-level, fast).
  - `./mvnw -B -DskipTests -fae clean install` (full reactor, per `feedback_delta_v_full_reactor_verify`).
  - Local `./opennms-container/delta-v/test-prometheus-writer-e2e.sh` end-to-end run, asserting exit 0 + `enrichment_lookup_fallback_total == 0`.
- **Known environment quirk** (from PR #175 session): strict-mode Bash wrapping may kill `build.sh` with exit 255 + empty log. Workaround: `bash -c '<cmd>' &; disown`. Documented here so the implementor doesn't re-discover it.

## 10. Post-merge

- Flip `project_collectd_serviceparameters_identity_gap` memory status `OPEN` → `RESOLVED (delta-v#<PR>)`.
- Addendum to `project_phase2_prometheus_writer_done`: "E2E now passes end-to-end as of delta-v#<PR>."
- Next phase queued: **Phase 3 Thresholder** — consumer of `deltav-timeseries`, evaluates thresholds (including stddev) and emits fault events. Spec spawns from `project_thresholder_brainstorm`. Likely extracts `NodeContextCache` as `core/deltav-node-context-cache` (YAGNI'd in Phase 2).

## 11. Risks and mitigations

| Risk | Likelihood | Mitigation |
| --- | --- | --- |
| `@Primary` bean resolution conflicts with some other Delta-V Spring config that overrides `LocationAwareCollectorClient` | Low | `AgentIdentityWiringIT` explicitly asserts bean resolution. A qualifier-based fallback is trivial if `@Primary` turns out not to win. |
| ThreadLocal leak across cycles on shared scheduler thread | Low | Triple-guarded: outer `finally` clear + unconditional overwrite on next cycle + `getOrThrow` surfaces stale state as `IllegalStateException` (if a cycle ever observed someone else's identity). |
| Collectd uses async/pooled thread for `createPersister` not visible to holder set in `execute()` | Low | Verified against `CollectableService.doCollection()`: synchronous `.get()` on the CompletableFuture, same thread. IT asserts the observed behavior. If horizon changes this contract in a future release, `getOrThrow()` fires on every cycle and the failure is immediately visible. |
| Inner persister's own ServiceParameters use breaks | None | No inner-persister code path is touched. Inner still reads `collection`/meta-tag params from the same unchanged `ServiceParameters` instance. |
| `collection` string extraction in `FanoutPersisterFactory` differs from horizon's own default lookup | Low | Uses the same `.getOrDefault("collection", "default")` pattern the current persister uses. No behavior change for that field. |

## 12. Decisions log (from brainstorm session)

- **Q1 (fix-layer):** Option A — `LocationAwareCollectorClient` decorator + ThreadLocal holder. Rejected: walking `CollectionResource.getPath()` (fragile, no location info).
- **Q2 (fail-fast scope):** Option A — persist-time only. Rejected: capture-time throws (would abort collection cycles for the inner persister path too).
- **Q3 (test strategy):** Option B — unit + integration test; E2E as mandatory local verification but not in `./mvnw verify`. Rejected: wiring E2E into CI (container startup cost; not module-local).
- **Q4a (persister signature):** Option A2 — explicit constructor `(publisher, collectionPackage, holder)`. Rejected: keeping `ServiceParameters` in signature (residual coupling).
- **Q4b (horizon-inner-persister bugs):** Confirmed deferred. Do not fold into M2 unless one directly blocks verification — none do, per §4.2's FanoutPersister isolation.
- **Refinement during Section 3:** outer `finally` for `holder.clear()` + `capturedSet = null`, even when `capturedSet == null`, so cleanup is guaranteed regardless of path through `completeCollectionSet`.
- **Refinement during Section 4:** validation moved out of `AgentIdentity` constructor and into `TimeseriesKafkaPersister.completeCollectionSet`, realigning with Q2.

## 13. References

- `project_collectd_serviceparameters_identity_gap` — primary memo with live evidence from labbox 2026-04-18.
- `project_collectd_publisher_inert_investigation` — RESOLVED (delta-v#175); why the consumer-side hypothesis was wrong.
- `project_phase2_prometheus_writer_done` — the consumer side this M2 completes.
- `project_kafka_timeseries_producer_next_session` — Phase 0 publisher history.
- `project_collectd_scheduler_publisher_split` — Phase 0 scope note that originally flagged this gap.
- `project_phase0_inner_persister_bugs_followup` — the three deferred horizon-inner bugs (#1/#2/#4).
- `feedback_never_pr_opennms`, `feedback_feature_branches`, `feedback_delta_v_uses_mvnw_not_compile_pl`, `feedback_delta_v_full_reactor_verify`, `feedback_deltav_package_namespace`, `feedback_kafka_topic_partition_race` — standing project guardrails applied here.
