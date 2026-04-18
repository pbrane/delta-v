# Phase 2 NodeContextKafkaBootstrap race fix + Collectd silent-swallow hygiene

> **This design SUPERSEDES the originally-committed design.** The original assumed the Phase 0 Collectd publisher was inert on develop. Live-stack investigation (Task 2 of the original plan, 2026-04-18) proved the publisher is working correctly — 6 batches published per 90 s. The real bug is a race in Phase 2's `NodeContextKafkaBootstrap` that leaves the `NodeContextCache` empty whenever the `deltav-node-context` topic doesn't exist yet at prometheus-writer startup. Every incoming timeseries record then hits `enrichment_missing` and drops. Root-cause evidence captured in `project_collectd_publisher_inert_investigation` memory (now flipped to document the real issue).
>
> The original spec + plan sit in branch history at commits `8c2729588e5` (spec v1) and `262309050ed` (plan v1). They are retained as-is for audit; this v2 design lands as a new commit over them.

**Status:** approved v2 (2026-04-18).
**Branch:** `fix/collectd-publisher-inert` (name retained for link stability; actual scope is mostly Phase 2).
**Target PR base:** `pbrane/delta-v develop`. Never `OpenNMS/opennms`.
**Related:** `project_collectd_publisher_inert_investigation` (memory — evidence of the real root cause), `project_phase2_prometheus_writer_done` (memory — the PR this unblocks), PR #174 (merged as `5ef18ed5384`).

## 1. Context and goal

Phase 2 PR #174's Docker E2E (`opennms-container/delta-v/test-prometheus-writer-e2e.sh`) fails at Step 4: `deltav_prometheus_writer_records_consumed_total = 0` after 90 s. Prior investigation assumed the Phase 0 Collectd publisher was inert. Live-stack diagnostics on 2026-04-18 (commit `d3c6d049cab` added the diagnostic exposure that made this visible) proved otherwise:

- Collectd's `TimeseriesKafkaPublisherConfiguration` loads; `@ConditionalOnProperty` matches; `compositePersisterFactory` is created and wraps the correct horizon inner factory; `FanoutPersister` IS invoked; `deltav_timeseries_batches_published_total = 6.0` after 90 s; topic `deltav-timeseries` has real SNMP records.
- prometheus-writer IS consuming — partition 9 shows `CURRENT-OFFSET=6, LAG=0`, `deltav_prometheus_writer_records_consumed_total = 6.0`.
- But `deltav_prometheus_writer_enrichment_missing_total{reason="never_seen"} = 6.0` — **every** consumed record dropped for missing `NodeContext` enrichment.
- `deltav_prometheus_writer_node_context_cache_size = 0.0` despite `node_context_cache_ready = 1.0`.

The real bug is in `core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/nodecontext/NodeContextKafkaBootstrap.java`. Its startup sequence has a race: when `consumer.partitionsFor(topic)` returns `null` because the `deltav-node-context` topic doesn't exist yet (provisiond hasn't created it), the code takes a "no partitions yet" branch that marks the cache ready with zero entries and enters `liveTail(consumer)` **without ever calling `consumer.assign(...)` or `consumer.subscribe(...)`**. `liveTail`'s `consumer.poll()` then throws `IllegalStateException: Consumer is not subscribed to any topics or assigned any partitions` on every 5 s cycle forever. Records written to the topic after provisiond creates it are never consumed by this consumer. Live log evidence:

```
15:22:54.842 WARN  NodeContextKafkaBootstrap : Topic deltav-node-context has no partitions yet — marking cache ready with empty state
15:22:54.845 INFO  NodeContextKafkaBootstrap : NodeContextCache bootstrap complete: 0 entries in 115 ms
15:22:54.846 INFO  TimeseriesBindingResumer  : NodeContextCache ready (size=0, bootstrap=115ms) — resuming binding
15:22:54.851 WARN  NodeContextKafkaBootstrap : NodeContextKafkaBootstrap live-tail error — will retry after poll timeout
(last WARN repeats every 5 s forever)
```

**Goal:** fix the race so the cache is populated from the `deltav-node-context` topic under both orderings (topic-exists-at-startup and topic-created-later). Ship the original Collectd-side silent-swallow hygiene (observability counters, dev fail-fast toggle, scan IT, compose default flip, proto-header pin) as protective tooling — no longer the critical fix but still valuable now that we know Phase 0 inner-persister bugs #1, #2, and a new #4 were hiding behind `FanoutPersister`'s isolation without operator signal. **Acceptance:** `./test-prometheus-writer-e2e.sh` exits 0 with `ALL ASSERTIONS PASSED` — first end-to-end pass of the full pipeline.

## 2. Scope

### In scope

- `NodeContextKafkaBootstrap` race fix (Opt1 — `consumer.subscribe(...)` + `ConsumerRebalanceListener`). Replaces the `partitionsFor`-based branching with a unified single-path flow.
- Testcontainers IT case that reproduces the topic-created-after-bootstrap-starts race (the regression gate).
- Collectd `FanoutPersister` observability: pre-registered Micrometer counters (`deltav.collectd.persister.inner.failures{step=…}`, `deltav.collectd.persister.kafka.failures{step=…}`) so silent-swallow becomes alertable.
- Collectd `FanoutPersister` dev fail-fast toggle: `deltav.collectd.persister.{inner,kafka}.fail-fast` Spring properties, default `false` in production. CI / ITs override to `true` to turn silent swallows into loud test failures.
- `FanoutPersisterTest` unit tests covering the new observability + fail-fast matrix.
- `CollectdApplicationScanIT` real-main-class integration test (Phase 2 scar-prophylactic lineage — would have pointed at the Phase 2 consumer side earlier if it had existed).
- `DELTAV_TIMESERIES_ENABLED` default flip `false` → `true` in `opennms-container/delta-v/docker-compose.yml`.
- `<TBD-phase-2-PR>` → `#174` substitution in both `core/deltav-kafka-contracts/src/main/proto/deltav-{timeseries,node-context}.proto` headers.
- Partial revert of the diagnostic commit (`d3c6d049cab`): remove the one-shot startup log + the env/beans actuator exposure. **Keep `/actuator/conditions` permanently** and keep the `compositePersisterFactory wrapping inner=...` startup log (gains operator value once fail-fast surfaces).

### Out of scope (follow-up PRs)

- **Phase 0 inner-persister bugs #1, #2, and #4.** All three are confirmed present in the live stack; all three are currently caught by `FanoutPersister.runInner` and WARN-logged. Phase 2 E2E no longer blocks on them because the Kafka path publishes regardless. They deserve separate focused investigation PRs and are memory-noted. See §5.
- `build.sh check_daemon_boot_freshness` transitive-dependency gap (forced `touch core/daemon-boot-collectd/pom.xml` during PR #174 investigation). Separate tooling PR.
- Phase 3 Thresholder (next numbered phase; separate brainstorm → spec → plan cycle).
- Extract a Collectd-wide canonical meter-name constants class (Phase 2 did this for `core/prometheus-writer`; separable Collectd refactor).
- `@Configuration`-class-name-vs-`@Bean`-method-name collision survey (Phase 2 found three; `feedback_configuration_class_bean_name_collision` captured the rule; a sweep PR is its own work).

## 3. Race-fix design

### Current racy code (shape)

```java
// inside NodeContextKafkaBootstrap.run()
List<PartitionInfo> parts = consumer.partitionsFor(TOPIC);
if (parts == null || parts.isEmpty()) {
    LOG.warn("Topic {} has no partitions yet — marking cache ready with empty state", TOPIC);
    finishBootstrap();        // cache marked ready, size=0
    liveTail(consumer);       // BUG: consumer has NO assignment/subscription
    return;
}
// happy path: assign partitions, seekToBeginning, drain to HWM, finishBootstrap, liveTail
```

### Fixed code (Opt1 — subscribe + ConsumerRebalanceListener)

Single-path flow. Works whether topic exists at startup or gets created later. No branching on `partitionsFor`.

```java
// inside NodeContextKafkaBootstrap.run()
Map<TopicPartition, Long> endOffsets = new ConcurrentHashMap<>();
Set<TopicPartition> caughtUp = ConcurrentHashMap.newKeySet();
AtomicBoolean bootstrapMarked = new AtomicBoolean(false);

consumer.subscribe(List.of(TOPIC), new ConsumerRebalanceListener() {
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> assigned) {
        // Record HWM at assignment time + seek to beginning so we replay the compacted topic.
        Map<TopicPartition, Long> hwm = consumer.endOffsets(assigned);
        endOffsets.putAll(hwm);
        consumer.seekToBeginning(assigned);
        // An empty partition (end offset == 0) is instantly caught up.
        for (Map.Entry<TopicPartition, Long> e : hwm.entrySet()) {
            if (e.getValue() == 0L) caughtUp.add(e.getKey());
        }
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> revoked) {
        // No-op. Kafka may reassign us; the new onPartitionsAssigned will re-establish HWM.
    }
});

while (running.get()) {
    ConsumerRecords<String, byte[]> records = consumer.poll(POLL_TIMEOUT);
    for (ConsumerRecord<String, byte[]> r : records) {
        applyRecord(r);
        TopicPartition tp = new TopicPartition(r.topic(), r.partition());
        Long hwm = endOffsets.get(tp);
        if (hwm != null && r.offset() + 1 >= hwm) caughtUp.add(tp);
    }
    // Mark the cache ready ONCE, after the first full drain-to-HWM across all currently-assigned partitions.
    if (!bootstrapMarked.get()
            && !endOffsets.isEmpty()
            && caughtUp.containsAll(endOffsets.keySet())) {
        if (bootstrapMarked.compareAndSet(false, true)) {
            finishBootstrap();  // marks cache ready, publishes NodeContextCacheReadyEvent
        }
    }
}
```

### Key properties

- **Single code path.** No "no partitions" branch to forget about. Kafka's rebalance protocol handles topic-doesn't-exist-yet uniformly.
- **Cache ready fires only after true drain.** `finishBootstrap()` runs exactly once, after `caughtUp` covers every partition the broker has assigned. The `TimeseriesBindingResumer` gate therefore holds the `deltav-timeseries` binding paused until the cache is actually populated, preventing the "6 records hit enrichment_missing at startup" symptom.
- **Survives mid-flight rebalance.** If Kafka reassigns partitions later (broker restart, topic partition expansion), `onPartitionsAssigned` fires again; `endOffsets` and `caughtUp` update; new records are drained. `bootstrapMarked` prevents duplicate `NodeContextCacheReadyEvent` publication.
- **Tombstones still handled.** `applyRecord` already treats `value == null` and `deleted=true` as delete; unchanged.

### What about startup latency?

The subscribe + first rebalance typically completes in a few hundred milliseconds when the topic exists. When the topic does NOT exist yet, Kafka does not assign until someone creates it. During that window, the cache is not marked ready, the binding stays paused, and `deltav-timeseries` records accumulate on the broker (our consumer group continues to hold its assignment). Once provisiond creates the topic, the rebalance fires, we drain, cache-ready fires, binding resumes — records from the buffer flow through with enrichment intact.

Worst-case startup delay: however long provisiond takes to come up and emit its first `NodeContext`. In practice <30 s per the live-stack observation.

## 4. Revised commit sequence

Seven commits total. Commit 1 is already on the branch (the Task 1 diagnostic exposure from the original plan).

1. **`diag(daemon-boot-collectd): expose env/beans/conditions + startup logs`** (already landed as `d3c6d049cab`). Retained; partially reverted in Commit 7.
2. **`fix(prometheus-writer): NodeContextKafkaBootstrap subscribe + ConsumerRebalanceListener`**. Replace `partitionsFor`-based branching with unified subscribe flow per §3. Delete the `finishBootstrap()` short-circuit in the "no partitions" case — that branch no longer exists.
3. **`test(prometheus-writer): NodeContextKafkaBootstrapIT topic-created-after-start case`**. New test method in the existing IT file. Exercises the exact path that was broken: bootstrap starts, topic doesn't exist, cache NOT ready; then create topic + produce records; cache becomes ready with the records.
4. **`feat(daemon-boot-collectd): FanoutPersister observability + dev fail-fast`**. `(X)` counters pre-registered for all 10 visitor steps on both inner and kafka sides. `(Y)` `deltav.collectd.persister.{inner,kafka}.fail-fast` Spring properties, default `false`.
5. **`test(daemon-boot-collectd): FanoutPersisterTest + CollectdApplicationScanIT`**. T0 unit tests (7 methods — counters, fail-fast matrix, `Throwable`-not-just-`RuntimeException`) + T1 real-main-class IT (3 methods — bean resolution, NewTopic bean, TimeseriesKafkaPublisher bean).
6. **`feat(compose+proto): enable Kafka time-series publisher default + pin Phase 2 proto header`**. `DELTAV_TIMESERIES_ENABLED: ${...:-false}` → `${...:-true}`; `<TBD-phase-2-PR>` → `#174` in both proto headers.
7. **`chore(daemon-boot-collectd): revert temporary env/beans actuator + one-shot startup log`**. Keep `/actuator/conditions` and the `compositePersisterFactory wrapping inner=...` log permanently. Revert the rest.

Each commit is surgical: one concern, one commit. Commits 2-3 fix the actual Phase 2 blocker; Commits 4-7 ship the protective tooling + operational tidy.

## 5. Phase 0 inner bugs out-of-scope (memory-noted)

Three distinct inner-persister bugs were observed during Task 2's live investigation. All three are already caught by `FanoutPersister.runInner` and WARN-logged. After this PR's `(X)` counters land, they become operator-visible at `/actuator/prometheus`. They do not block Phase 2 E2E because Kafka publishes regardless of inner failures.

| # | Exception | Location | Count/90 s |
|---|---|---|---|
| 1 | `UnexpectedRollbackException` (tx marked rollback-only) | `TimeseriesPersister.visitResource` → `MetaTagDataLoader.load` → our `withReadOnlyTransaction` → Spring `TransactionTemplate.execute` | 8 |
| 2 | `NullPointerException: "this.currentBuilder is null"` | `TimeseriesPersister.persistNumericAttribute` → `TimeseriesPersistOperationBuilder.setAttributeValue` | 76 |
| 4 | `ClassCastException: ResourcePath cannot be cast to CollectionResource` (new — not in Phase 0's known-issues list) | `TimeseriesPersister.getUserDefinedMetaTags` → Guava `LocalCache.get` (type-mismatched loader) | 10 |

A new memory note `project_phase0_inner_persister_bugs_followup` captures these with stack-trace excerpts for a separate PR. Phase 0 known-issue #3 (`NoSuchMethodError: ResourceTypeUtils.getResourcePathWithRepository`) did not surface during this investigation.

## 6. Test strategy summary

- **T0 unit**: `FanoutPersisterTest` (7 tests) — Mockito + `SimpleMeterRegistry`. Covers counter increment on inner/kafka failure, pre-registration with zero values, fail-fast matrix (4 cells), `Throwable`-not-just-`RuntimeException` catch.
- **T1 real-main-class IT**: `CollectdApplicationScanIT` — `@SpringBootTest(classes = CollectdApplication.class)` + Testcontainers Kafka + `@TestPropertySource` setting `deltav.timeseries.enabled=true` + both fail-fast toggles true. Three tests: `PersisterFactory` resolves to `FanoutPersisterFactory`; `deltavTimeseriesTopic` `NewTopic` bean present; `TimeseriesKafkaPublisher` bean present. Mandatory `commons-io:2.18.0` test dep per memory `feedback_boot4_testcontainers_commons_io`.
- **Race IT**: new case added to `NodeContextKafkaBootstrapIT`. Does NOT pre-create topic; starts bootstrap; asserts cache is NOT ready; creates topic + produces records mid-flight; asserts cache becomes ready within 30 s AND contains the produced records. Ideally also an assertion that a mid-run partition-expansion scenario triggers rebalance-driven catch-up (nice-to-have; drop if test infrastructure makes it fragile).
- **Live acceptance**: Phase 2 E2E (`test-prometheus-writer-e2e.sh`) exits 0 with `ALL ASSERTIONS PASSED` after full-reactor build + all-image rebuild.

## 7. Acceptance criteria

1. Memory note `project_collectd_publisher_inert_investigation` final status flipped to `RESOLVED (real bug was Phase 2 NodeContextKafkaBootstrap race)` with a pointer to this PR.
2. Memory note `project_phase0_inner_persister_bugs_followup` created, capturing bugs #1/#2/#4 with stack traces + rate-per-90s observations.
3. Memory note `project_nodecontext_startup_race_pattern` (feedback memory) created, documenting: *any Spring-Kafka consumer using `consumer.partitionsFor(topic)` must handle the topic-does-not-exist-yet case via `subscribe` + `ConsumerRebalanceListener`, never via skip-and-enter-liveTail.*
4. `./mvnw -pl core/prometheus-writer verify` green — all existing Phase 2 tests pass AND the new `NodeContextKafkaBootstrapIT` race-test passes.
5. `./mvnw -pl core/daemon-boot-collectd verify` green — 7 `FanoutPersisterTest` + 3 `CollectdApplicationScanIT` tests pass.
6. `./mvnw -B -DskipTests -fae clean install` green (full-reactor scar prophylactic).
7. Live producer + consumer: after `docker compose up -d` (compose default now `true`), within 90 s: `kafka-console-consumer --topic deltav-timeseries` reads ≥ 1 record; prometheus-writer `/actuator/prometheus` shows `records_consumed_total > 0` AND `enrichment_missing_total == 0` AND `samples_sent_total > 0`.
8. `./opennms-container/delta-v/test-prometheus-writer-e2e.sh` exits 0 with `ALL ASSERTIONS PASSED`. This is the first end-to-end pass of the full Phase 2 pipeline.
9. CI green on PR.
10. PR opened `--repo pbrane/delta-v --base develop`. Title: `fix(prometheus-writer): NodeContextKafkaBootstrap race + Collectd silent-swallow hygiene`.
11. Post-merge memory updates: `project_phase2_prometheus_writer_done` gets an addendum noting E2E now passes end-to-end.

## 8. Risks and trade-offs

- **ConsumerRebalanceListener edge cases.** Kafka rebalances can be triggered by broker failover, partition expansion, or group-member changes. The new code handles reassignment correctly (new `onPartitionsAssigned` re-seeks) but duplicates the drain work each time. For our use case (single consumer instance, compacted topic, small state) this is benign. In a scaled-out deployment this would need revisiting — out of scope now, memory-noted as a follow-up if operators later scale prometheus-writer horizontally.
- **Startup wait-for-topic latency.** If provisiond is slow to come up, prometheus-writer waits (cache not ready → binding paused → timeseries records buffer in Kafka). Provisiond startup typically < 30 s; long delays would show up as a metric-visibility gap rather than lost data (Kafka retains the records; they flow once provisiond catches up).
- **Phase 0 inner bugs remain swallowed in production.** This PR's `(X)` counters make them visible but do not fix them. Operators running the dashboard will see the counter-rate and know something's wrong; without the dashboard they see the same invisible WARN-log swallow. Acceptable because each bug deserves its own investigation; the counters are the prerequisite for knowing they're worth investigating.
- **Compose default flip side-effects.** Any existing operator-local E2E that explicitly expected `DELTAV_TIMESERIES_ENABLED=false` as the default breaks. Unlikely — publisher is passive when Kafka reachable; only observable effect is batches published.

## 9. References

- Memory: `project_collectd_publisher_inert_investigation` (the investigation narrative — original hypothesis REFUTED, real root cause confirmed).
- Memory: `project_phase2_prometheus_writer_done` (PR #174 merged; E2E deferred to this follow-up PR).
- Memory: `project_kafka_timeseries_pipeline` (Phase 0 DONE, Phase 1 DONE, Phase 2 awaiting this PR's E2E green).
- Memory: `feedback_configuration_class_bean_name_collision` (Phase 2 scar, still applicable to the Collectd-side hygiene tooling this PR ships).
- Memory: `feedback_boot4_testcontainers_commons_io` (Phase 2 scar; the `commons-io:2.18.0` test dep T1 adds traces back to this).
- Memory: `feedback_spring_boot_scan_package_trap` (Phase 0 #170 scar; the `CollectdApplicationScanIT` pattern descends from here).
- Memory: `feedback_delta_v_full_reactor_verify` (build-verify hygiene).
- Prior E2E logs: `/tmp/phase2-e2e.log`, `/tmp/phase2-e2e-2.log`, `/tmp/phase2-e2e-3.log`, `/tmp/phase2-e2e-4.log`.
- Original spec (v1, superseded): this file's previous content at commit `8c2729588e5`.
- Original plan (v1, superseded): `docs/superpowers/plans/2026-04-18-collectd-publisher-inert-fix.md` at commit `262309050ed` (to be rewritten).
