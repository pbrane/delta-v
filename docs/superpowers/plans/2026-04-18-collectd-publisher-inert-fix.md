# Phase 2 NodeContextKafkaBootstrap Race Fix + Collectd Hygiene — Implementation Plan (v2)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **This plan SUPERSEDES the v1 plan at commit `262309050ed`.** The original plan assumed the Phase 0 Collectd publisher was inert. Live-stack investigation (Task 2 of v1) refuted that — the publisher works fine. The actual bug is a race in Phase 2's `NodeContextKafkaBootstrap`. v1's diagnostic-exposure commit (`d3c6d049cab`) is retained on the branch; everything else gets rewritten.

**Goal:** Fix the `NodeContextKafkaBootstrap` race so the cache is populated under both topic-exists-at-startup and topic-created-later orderings. Ship complementary Collectd silent-swallow hygiene (Micrometer counters, dev fail-fast toggle, real-main-class scan IT). Acceptance: Phase 2 E2E (`./test-prometheus-writer-e2e.sh`) exits 0 with `ALL ASSERTIONS PASSED` end-to-end — first successful run of the full pipeline.

**Architecture (v2):** Replace `NodeContextKafkaBootstrap.run()`'s `partitionsFor`-based branching with a single-path `consumer.subscribe(...)` flow whose `ConsumerRebalanceListener` records HWM, seeks to beginning, drains, and marks cache-ready exactly once after first full drain. Add complementary observability + fail-fast to Collectd's existing `FanoutPersister` so the Phase 0 inner-persister bugs (#1, #2, #4) become operator-visible (they remain swallowed today but are alertable after this PR). Compose default flip + proto-header pin close out Phase 2's deferred loose ends.

**Tech Stack:** Spring Boot 4.0.3, horizon 1.0.10, Spring Kafka, Apache Kafka client 3.x, Micrometer Prometheus, JUnit 5, Mockito, Testcontainers Kafka (`apache/kafka:3.8.0`), `commons-io:2.18.0` (test-scope, per `feedback_boot4_testcontainers_commons_io`).

**Spec:** `docs/superpowers/specs/2026-04-18-collectd-publisher-inert-fix-design.md` (v2, commit `f0a4d5b0f94`).

**Branch:** `fix/collectd-publisher-inert` (name retained from v1; actual scope is mostly Phase 2 prometheus-writer + Collectd hygiene).

**PR target:** `pbrane/delta-v` base `develop`. **NEVER `OpenNMS/opennms`** — memory `feedback_never_pr_opennms`.

**Critical memory references:**
- `feedback_never_pr_opennms` — PRs always `--repo pbrane/delta-v`.
- `feedback_feature_branches` — never commit directly to `develop`.
- `feedback_delta_v_uses_mvnw_not_compile_pl` — use `./mvnw`, never `compile.pl`.
- `feedback_delta_v_full_reactor_verify` — full-reactor build before push.
- `feedback_boot4_testcontainers_commons_io` — Boot 4 modules using Testcontainers need explicit `commons-io:2.18.0` test dep.
- `feedback_configuration_class_bean_name_collision` — Collectd-side hygiene work obeys this rule.
- `feedback_deltav_package_namespace` — new code uses `org.deltav.*` with BeaconStrategists copyright.
- `project_collectd_publisher_inert_investigation` — root-cause memo (real bug: NodeContextKafkaBootstrap race).
- `project_phase2_prometheus_writer_done` — PR #174 merged (`5ef18ed5384`); E2E deferred to this PR.

---

## Task ordering rationale

Nine tasks grouped by the six new commits from spec §4 + three acceptance gates. The race fix (Task 1) is the critical blocker; everything else is protective tooling or operational tidy. Each task either ends with a commit or is a verification-only step.

1. **Race fix** — Tasks 1-2 (Commits 2-3 in spec §4)
2. **Collectd hygiene** — Tasks 3-4 (Commits 4-5)
3. **Operational** — Tasks 5-6 (Commits 6-7)
4. **Acceptance gates** — Tasks 7-9 (no commits; verification + push + PR)

---

## Task 1: NodeContextKafkaBootstrap race fix (subscribe + ConsumerRebalanceListener)

**Goal:** Replace the racy `partitionsFor`-based two-branch logic in `NodeContextKafkaBootstrap.run()` with a single-path `consumer.subscribe(List.of(TOPIC))` flow whose `ConsumerRebalanceListener` records HWM, seeks to beginning, drains, and marks the cache ready exactly once.

**Files:**
- Modify: `core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/nodecontext/NodeContextKafkaBootstrap.java`
- Modify (TDD test first, then verified by impl): `core/prometheus-writer/src/test/java/org/deltav/prometheus/writer/nodecontext/NodeContextKafkaBootstrapIT.java`

> **TDD note:** the existing IT (`bootstrap_drains_to_HWM_then_marks_ready`, `tombstone_removes_key_from_cache_live`) tests the happy path with topic pre-created. Both should still pass after this fix without modification — the new code handles the topic-exists-at-startup case as a degenerate of the topic-created-later case. Task 2 adds the new race-test method explicitly.

- [ ] **Step 1: Run existing NodeContextKafkaBootstrapIT to establish baseline**

```bash
./mvnw -pl core/prometheus-writer test -Dtest=NodeContextKafkaBootstrapIT
```

Expected: `2/2 PASS` in ~15-20s (Testcontainers Kafka spin-up dominates). Establishes that the existing two tests pass against the current racy code (they do — they pre-create the topic, so the racy branch is never exercised).

- [ ] **Step 2: Replace `run()` method with the subscribe-based flow**

Edit `core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/nodecontext/NodeContextKafkaBootstrap.java`. Three changes:

1. Update imports — add `ConsumerRebalanceListener`, `ConcurrentHashMap`, drop `HashMap`/`HashSet` if no longer used, drop `PartitionInfo` (no longer referenced).
2. Replace the entire `run()` method body.
3. Remove the now-unused `partitionsFor`-branch logic.

**Updated imports** (replace the existing `java.util.*` and `org.apache.kafka.*` block):

```java
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
```

```java
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
```

(Drop `import org.apache.kafka.common.PartitionInfo;`, `import java.util.HashMap;`, `import java.util.HashSet;` — no longer used.)

**Replace the entire `run()` method** (currently lines 98-140) with:

```java
    @Override
    public void run() {
        // Tracks per-partition end-offset captured at the moment the broker assigned the
        // partition to us. The bootstrap is "complete" when every assigned partition has
        // been drained up to its captured HWM. Both maps mutate from the consumer thread
        // (poll loop) and the rebalance listener (called inline during poll), so a
        // ConcurrentHashMap is defensive-but-cheap.
        Map<TopicPartition, Long> endOffsets = new ConcurrentHashMap<>();
        Set<TopicPartition> caughtUp = ConcurrentHashMap.newKeySet();
        AtomicBoolean bootstrapMarked = new AtomicBoolean(false);

        try (KafkaConsumer<String, byte[]> consumer = buildConsumer()) {
            consumer.subscribe(List.of(TOPIC), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> assigned) {
                    // Record HWM at assignment time so we know when we have replayed the
                    // compacted topic up to "now". Then seek to the beginning so we
                    // actually replay everything (compacted topic semantics: we need
                    // the latest value per key, not just the live tail).
                    Map<TopicPartition, Long> hwm = consumer.endOffsets(assigned);
                    endOffsets.putAll(hwm);
                    consumer.seekToBeginning(assigned);
                    // Empty partitions (end offset == 0) are instantly caught up.
                    for (Map.Entry<TopicPartition, Long> e : hwm.entrySet()) {
                        if (e.getValue() == 0L) {
                            caughtUp.add(e.getKey());
                        }
                    }
                }

                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> revoked) {
                    // No-op. Kafka may reassign us; the next onPartitionsAssigned will
                    // re-establish HWM and re-seek. AtomicBoolean bootstrapMarked
                    // prevents duplicate NodeContextCacheReadyEvent publication.
                }
            });

            while (running.get()) {
                ConsumerRecords<String, byte[]> records = consumer.poll(POLL_TIMEOUT);
                for (ConsumerRecord<String, byte[]> r : records) {
                    applyRecord(r);
                    TopicPartition tp = new TopicPartition(r.topic(), r.partition());
                    Long hwm = endOffsets.get(tp);
                    if (hwm != null && r.offset() + 1 >= hwm) {
                        caughtUp.add(tp);
                    }
                }
                // Mark cache ready exactly once, after the first full drain across all
                // currently-assigned partitions. The compareAndSet guards against double-
                // firing if poll() somehow re-enters this branch before bootstrapMarked
                // becomes visible (defensive — single consumer thread, but cheap).
                if (!bootstrapMarked.get()
                        && !endOffsets.isEmpty()
                        && caughtUp.containsAll(endOffsets.keySet())
                        && bootstrapMarked.compareAndSet(false, true)) {
                    finishBootstrap();
                }
            }
        } catch (Exception e) {
            LOG.error("NodeContextKafkaBootstrap fatal error", e);
        }
    }
```

**Delete the now-unused `liveTail(KafkaConsumer)` method** (currently lines 142-153). It is superseded by the merged poll loop in the new `run()`. The method is private and has no callers outside `run()`.

`applyRecord`, `finishBootstrap`, `buildConsumer`, `start`, `stop`, `onAppReady`, the constructor, and the gauge registration all stay unchanged.

- [ ] **Step 3: Re-run the existing IT to confirm no regression**

```bash
./mvnw -pl core/prometheus-writer test -Dtest=NodeContextKafkaBootstrapIT
```

Expected: still `2/2 PASS`. The two existing tests (`bootstrap_drains_to_HWM_then_marks_ready`, `tombstone_removes_key_from_cache_live`) now exercise the new subscribe-based path. Cache should be ready and populated; tombstones should still propagate.

If either fails, STOP and report. The likely culprits: (a) the new `bootstrapMarked` gate fires before all partitions are caught up (fix: check the `endOffsets.isEmpty()` short-circuit), (b) the `ConsumerRebalanceListener` interferes with the existing test's `produce(...)` followed by `await().until(cache::isReady)` timing (fix: extend the await timeout to 30s).

- [ ] **Step 4: Verify the full module test suite still passes**

```bash
./mvnw -pl core/prometheus-writer verify
```

Expected: all unit + IT tests pass (~88+ total per the Phase 2 baseline). No new failures introduced by the rewrite of `run()`.

- [ ] **Step 5: Commit**

```bash
git add core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/nodecontext/NodeContextKafkaBootstrap.java
git commit -m "fix(prometheus-writer): NodeContextKafkaBootstrap subscribe + ConsumerRebalanceListener

Race fix. Old code path: consumer.partitionsFor(TOPIC) returns null when
deltav-node-context topic doesn't exist yet (provisiond hasn't created
it), the 'no partitions yet' branch calls finishBootstrap() + liveTail()
WITHOUT ever calling consumer.assign() or consumer.subscribe(). liveTail's
consumer.poll() then throws IllegalStateException ('Consumer is not
subscribed to any topics or assigned any partitions') on every 5s cycle
forever. Records later written to the topic by provisiond are never
consumed; cache stays empty; every prometheus-writer record hits
enrichment_missing.

New code path: single-path consumer.subscribe(List.of(TOPIC)) with a
ConsumerRebalanceListener.onPartitionsAssigned that records HWM, seeks
to beginning, and tracks 'caught up' state. The merged poll loop drains
records and marks the cache ready exactly once (guarded by AtomicBoolean
compareAndSet) after the first full drain to HWM across all assigned
partitions. Same flow handles topic-exists-at-startup AND
topic-created-later — no branching on partitionsFor.

Existing IT tests (bootstrap_drains_to_HWM_then_marks_ready,
tombstone_removes_key_from_cache_live) still pass — both pre-create the
topic so they exercise the new path's degenerate case. Task 2 adds the
new IT case that exercises the topic-created-after-start race that was
broken.

Surfaced by PR #174 E2E investigation
(project_collectd_publisher_inert_investigation memory)."
```

---

## Task 2: NodeContextKafkaBootstrapIT — topic-created-after-start case

**Goal:** Add a regression-gate IT method that exercises the exact path that was broken in v1: bootstrap starts, topic does not exist, cache must NOT be marked ready, then create topic + produce records mid-flight, cache must become ready with the records.

**Files:**
- Modify: `core/prometheus-writer/src/test/java/org/deltav/prometheus/writer/nodecontext/NodeContextKafkaBootstrapIT.java`

- [ ] **Step 1: Add the new test method to the existing IT class**

Open `core/prometheus-writer/src/test/java/org/deltav/prometheus/writer/nodecontext/NodeContextKafkaBootstrapIT.java` and add this test method alongside the existing two. Place it after `tombstone_removes_key_from_cache_live` and before the `@AfterEach` (or at the end of the class — the order doesn't matter to JUnit):

```java
    @Test
    void bootstrap_survives_topic_created_after_start() throws Exception {
        // CRITICAL: do NOT call ensureTopic() at the start of this test. The whole
        // point is that the topic does not exist when NodeContextKafkaBootstrap
        // begins. The v1 racy code marked the cache ready immediately with size=0
        // and entered an unassigned-poll loop that threw IllegalStateException
        // forever, never consuming any records that were later written to the topic.
        NodeContextCache cache = new NodeContextCache();
        AtomicReference<NodeContextCacheReadyEvent> capturedEvent = new AtomicReference<>();
        ApplicationEventPublisher publisher = e -> {
            if (e instanceof NodeContextCacheReadyEvent r) capturedEvent.set(r);
        };
        NodeContextKafkaBootstrap boot = new NodeContextKafkaBootstrap(
                cache, publisher, new SimpleMeterRegistry(), KAFKA.getBootstrapServers());
        boot.start();
        try {
            // Sanity: cache must NOT be ready yet because the topic doesn't exist.
            // Wait a moment to make sure the bootstrap thread has had time to attempt
            // a subscribe and not short-circuit to ready.
            Thread.sleep(2000);
            assertThat(cache.isReady())
                    .as("cache must not be ready before topic exists")
                    .isFalse();
            assertThat(capturedEvent.get())
                    .as("ready event must not have fired before topic exists")
                    .isNull();

            // Now create the topic mid-flight. Kafka should rebalance our consumer
            // (which subscribed to the not-yet-existent topic) and assign partitions.
            ensureTopic();
            // Produce a record so there's something for the bootstrap drain to land.
            produce(200, false);

            // Cache should become ready within 30s (allows for rebalance + drain).
            await().atMost(Duration.ofSeconds(30)).until(cache::isReady);
            assertThat(capturedEvent.get())
                    .as("ready event must have fired exactly once after first drain")
                    .isNotNull();
            assertThat(cache.get("Default@200")).isPresent();
        } finally {
            boot.stop();
        }
    }
```

If `Thread.sleep(2000)` triggers a checkstyle/spotbugs warning in this codebase, replace with the existing `Awaitility.with().pollDelay(...)` idiom — the surrounding tests' style sets the precedent.

- [ ] **Step 2: Run only the new test**

```bash
./mvnw -pl core/prometheus-writer test -Dtest=NodeContextKafkaBootstrapIT#bootstrap_survives_topic_created_after_start
```

Expected: `1/1 PASS` in ~15-25s (Testcontainers Kafka + 2s sanity wait + rebalance + drain).

If it fails on the `assertThat(cache.isReady()).isFalse()` line: the new code is incorrectly marking the cache ready before partitions are assigned. Re-check `endOffsets.isEmpty()` short-circuit in `run()` — that gate prevents premature ready-firing.

If it fails on `await().atMost(Duration.ofSeconds(30)).until(cache::isReady)`: the rebalance + drain didn't complete in time. Either (a) bump the timeout to 60s, or (b) the new code isn't catching up to HWM (re-check the `caughtUp.containsAll(endOffsets.keySet())` condition).

If it fails on `assertThat(cache.get("Default@200")).isPresent()`: the record was consumed but not stored. Check `applyRecord` — should be unchanged from v1.

- [ ] **Step 3: Run the full IT class to ensure no test interference**

```bash
./mvnw -pl core/prometheus-writer test -Dtest=NodeContextKafkaBootstrapIT
```

Expected: `3/3 PASS`. The new test plus the two existing tests (`bootstrap_drains_to_HWM_then_marks_ready`, `tombstone_removes_key_from_cache_live`) all pass against the new subscribe-based code.

- [ ] **Step 4: Commit**

```bash
git add core/prometheus-writer/src/test/java/org/deltav/prometheus/writer/nodecontext/NodeContextKafkaBootstrapIT.java
git commit -m "test(prometheus-writer): NodeContextKafkaBootstrapIT topic-created-after-start case

Regression gate for the race fixed in the prior commit. Exercises the
exact scenario that was broken: NodeContextKafkaBootstrap starts before
deltav-node-context topic exists; cache must NOT be marked ready; then
topic is created mid-flight and a record produced; cache must become
ready and contain the record.

Reproduces the race that the v1 code took the 'no partitions yet' branch
on and silently entered an unassigned-poll loop. With the new
subscribe + ConsumerRebalanceListener flow, Kafka rebalances and assigns
the partitions when the topic appears, the listener records HWM and
seeks to beginning, the merged poll loop drains, and the cache-ready
gate fires.

Existing two tests (bootstrap_drains_to_HWM_then_marks_ready,
tombstone_removes_key_from_cache_live) unchanged — they exercise the
topic-exists-at-startup degenerate case."
```

---

## Task 3: FanoutPersister observability + dev fail-fast (Commit 4 — feat-only per spec)

**Goal:** Add the (X) Micrometer counters + (Y) Spring fail-fast properties to `FanoutPersister` per spec §3-4. Make the Phase 0 inner-persister bugs (#1, #2, #4) operator-visible without fixing them. Default fail-fast off in production; ITs override via `@TestPropertySource`.

> **Spec §4 mandates this commit is feat-only** (FanoutPersisterTest lives in Commit 5 / Task 4). Pure implementation here; tests in the next task. Run-to-fail-then-pass TDD discipline is preserved by writing tests in Task 4 against this task's already-landed implementation; ITs and the existing test suite verify no regression.

**Files:**
- Modify: `core/daemon-boot-collectd/src/main/java/org/deltav/collectd/timeseries/TimeseriesKafkaPublisherConfiguration.java`

- [ ] **Step 1: Add Counter + MeterRegistry imports**

The class already imports `MeterRegistry` (line 19). Add `Counter`:

```java
import io.micrometer.core.instrument.Counter;
```

- [ ] **Step 2: Add meter-name constants + STEPS array inside FanoutPersister**

Inside the `FanoutPersister` static nested class, near the top (after the `LOG` field at line 150), add:

```java
        private static final String INNER_FAILURES_METER =
                "deltav.collectd.persister.inner.failures";
        private static final String KAFKA_FAILURES_METER =
                "deltav.collectd.persister.kafka.failures";
        private static final String[] STEPS = new String[]{
                "visitCollectionSet", "visitResource", "visitGroup", "visitAttribute",
                "completeAttribute", "completeGroup", "completeResource", "completeCollectionSet",
                "persistNumericAttribute", "persistStringAttribute"
        };
```

- [ ] **Step 3: Replace the FanoutPersister constructor + fields to take MeterRegistry + fail-fast booleans**

Current shape (lines 152-158):

```java
        private final Persister innerPersister;
        private final TimeseriesKafkaPersister kafkaPersister;

        FanoutPersister(Persister innerPersister, TimeseriesKafkaPersister kafkaPersister) {
            this.innerPersister = innerPersister;
            this.kafkaPersister = kafkaPersister;
        }
```

Replace with:

```java
        private final Persister innerPersister;
        private final TimeseriesKafkaPersister kafkaPersister;
        private final MeterRegistry meterRegistry;
        private final boolean failFastInner;
        private final boolean failFastKafka;

        FanoutPersister(Persister innerPersister, TimeseriesKafkaPersister kafkaPersister,
                        MeterRegistry meterRegistry, boolean failFastInner, boolean failFastKafka) {
            this.innerPersister = innerPersister;
            this.kafkaPersister = kafkaPersister;
            this.meterRegistry = meterRegistry;
            this.failFastInner = failFastInner;
            this.failFastKafka = failFastKafka;
            preRegisterCounters();
        }

        private void preRegisterCounters() {
            // Pre-register all 20 (2 sides × 10 steps) counter tag combinations at
            // construction time so ops can alert on rate>0 rather than missing-metric
            // (which would otherwise be ambiguous with "no failures occurred").
            for (String step : STEPS) {
                Counter.builder(INNER_FAILURES_METER).tag("step", step).register(meterRegistry);
                Counter.builder(KAFKA_FAILURES_METER).tag("step", step).register(meterRegistry);
            }
        }
```

- [ ] **Step 4: Replace runInner + runKafka to increment counters + honor fail-fast**

Current shape (lines 160-180):

```java
        private void runInner(String step, Runnable task) {
            try {
                task.run();
            } catch (Throwable e) {
                LOG.warn("Inner persister threw during {}; continuing with Kafka path", step, e);
            }
        }

        private void runKafka(String step, Runnable task) {
            try {
                task.run();
            } catch (Throwable e) {
                LOG.warn("Kafka persister threw during {}; continuing", step, e);
            }
        }
```

Replace with:

```java
        private void runInner(String step, Runnable task) {
            try {
                task.run();
            } catch (Throwable e) {
                meterRegistry.counter(INNER_FAILURES_METER, "step", step).increment();
                LOG.warn("Inner persister threw during {}; continuing with Kafka path", step, e);
                if (failFastInner) {
                    throw new RuntimeException(
                            "Inner persister failed during " + step + " (fail-fast enabled)", e);
                }
            }
        }

        private void runKafka(String step, Runnable task) {
            try {
                task.run();
            } catch (Throwable e) {
                meterRegistry.counter(KAFKA_FAILURES_METER, "step", step).increment();
                LOG.warn("Kafka persister threw during {}; continuing", step, e);
                if (failFastKafka) {
                    throw new RuntimeException(
                            "Kafka persister failed during " + step + " (fail-fast enabled)", e);
                }
            }
        }
```

The `catch (Throwable e)` is preserved (per the existing comment about LinkageError from horizon's classpath quirks).

- [ ] **Step 5: Extend FanoutPersisterFactory to carry MeterRegistry + fail-fast flags**

Current shape (lines 102-127):

```java
    static final class FanoutPersisterFactory implements PersisterFactory {
        private final PersisterFactory innerFactory;
        private final TimeseriesKafkaPublisher publisher;

        FanoutPersisterFactory(PersisterFactory innerFactory, TimeseriesKafkaPublisher publisher) {
            this.innerFactory = innerFactory;
            this.publisher = publisher;
        }

        @Override
        public Persister createPersister(ServiceParameters params, RrdRepository repository) {
            return new FanoutPersister(
                    innerFactory.createPersister(params, repository),
                    new TimeseriesKafkaPersister(publisher, params));
        }

        @Override
        public Persister createPersister(ServiceParameters params, RrdRepository repository,
                                         boolean dontPersistCounters, boolean forceStoreByGroup,
                                         boolean dontReorderAttributes) {
            return new FanoutPersister(
                    innerFactory.createPersister(params, repository, dontPersistCounters,
                            forceStoreByGroup, dontReorderAttributes),
                    new TimeseriesKafkaPersister(publisher, params));
        }
    }
```

Replace with:

```java
    static final class FanoutPersisterFactory implements PersisterFactory {
        private final PersisterFactory innerFactory;
        private final TimeseriesKafkaPublisher publisher;
        private final MeterRegistry meterRegistry;
        private final boolean failFastInner;
        private final boolean failFastKafka;

        FanoutPersisterFactory(PersisterFactory innerFactory, TimeseriesKafkaPublisher publisher,
                               MeterRegistry meterRegistry,
                               boolean failFastInner, boolean failFastKafka) {
            this.innerFactory = innerFactory;
            this.publisher = publisher;
            this.meterRegistry = meterRegistry;
            this.failFastInner = failFastInner;
            this.failFastKafka = failFastKafka;
        }

        @Override
        public Persister createPersister(ServiceParameters params, RrdRepository repository) {
            return new FanoutPersister(
                    innerFactory.createPersister(params, repository),
                    new TimeseriesKafkaPersister(publisher, params),
                    meterRegistry, failFastInner, failFastKafka);
        }

        @Override
        public Persister createPersister(ServiceParameters params, RrdRepository repository,
                                         boolean dontPersistCounters, boolean forceStoreByGroup,
                                         boolean dontReorderAttributes) {
            return new FanoutPersister(
                    innerFactory.createPersister(params, repository, dontPersistCounters,
                            forceStoreByGroup, dontReorderAttributes),
                    new TimeseriesKafkaPersister(publisher, params),
                    meterRegistry, failFastInner, failFastKafka);
        }
    }
```

- [ ] **Step 6: Update the @Bean method to inject MeterRegistry + read the fail-fast properties**

Current shape (lines 90-105):

```java
    @Bean
    @Primary
    public PersisterFactory compositePersisterFactory(
            @Qualifier("timeseriesPersisterFactory") PersisterFactory innerFactory,
            TimeseriesKafkaPublisher publisher) {
        LOG.info("Creating compositePersisterFactory wrapping inner={}@{}",
                innerFactory.getClass().getName(),
                System.identityHashCode(innerFactory));
        return new FanoutPersisterFactory(innerFactory, publisher);
    }
```

Replace with:

```java
    @Bean
    @Primary
    public PersisterFactory compositePersisterFactory(
            @Qualifier("timeseriesPersisterFactory") PersisterFactory innerFactory,
            TimeseriesKafkaPublisher publisher,
            MeterRegistry meterRegistry,
            @Value("${deltav.collectd.persister.inner.fail-fast:false}") boolean failFastInner,
            @Value("${deltav.collectd.persister.kafka.fail-fast:false}") boolean failFastKafka) {
        LOG.info("Creating compositePersisterFactory wrapping inner={}@{} (failFastInner={}, failFastKafka={})",
                innerFactory.getClass().getName(),
                System.identityHashCode(innerFactory),
                failFastInner, failFastKafka);
        return new FanoutPersisterFactory(innerFactory, publisher, meterRegistry,
                failFastInner, failFastKafka);
    }
```

The `@Value` import (`org.springframework.beans.factory.annotation.Value`) is already at line 36. No new import needed.

- [ ] **Step 7: Update the class-level Javadoc to document the new meters + properties**

Replace the `FanoutPersister` Javadoc block (currently lines 130-148) with:

```java
    /**
     * Forwards each visitor callback to both delegate persisters, inner first
     * then Kafka. Each delegate is invoked inside its own try/catch so that
     * an exception in one does not prevent the other from running.
     *
     * <p>Observability (added 2026-04-18 — see project_collectd_publisher_inert_investigation):
     * <ul>
     *   <li>{@code deltav.collectd.persister.inner.failures{step=<visitor-step>}}
     *       — Micrometer counter, pre-registered for all 10 visitor steps at
     *       construction so ops can alert on rate&gt;0 rather than missing-metric.</li>
     *   <li>{@code deltav.collectd.persister.kafka.failures{step=<visitor-step>}}
     *       — symmetric for the Kafka side.</li>
     * </ul>
     *
     * <p>Fail-fast toggles (default false in production; CI/IT profiles enable):
     * <ul>
     *   <li>{@code deltav.collectd.persister.inner.fail-fast} — when true, inner
     *       Throwable propagates as RuntimeException instead of being swallowed.</li>
     *   <li>{@code deltav.collectd.persister.kafka.fail-fast} — symmetric.</li>
     * </ul>
     *
     * <p>Phase 0 horizon-side inner-persister bugs (UnexpectedRollbackException
     * in MetaTagDataLoader; NPE in TimeseriesPersistOperationBuilder.setAttributeValue;
     * ClassCastException in TimeseriesPersister.getUserDefinedMetaTags) are still
     * caught and WARN-logged here. They do not block the Kafka path. See memory
     * project_phase0_inner_persister_bugs_followup for the dedicated fix queue.
     */
```

- [ ] **Step 8: Verify the module compiles + existing tests pass**

```bash
./mvnw -pl core/daemon-boot-collectd -DskipTests compile
```

Expected: `BUILD SUCCESS`. The new constructor signature is backwards-compatible at the @Bean injection site only because we updated the @Bean method too.

Then run the existing test suite to confirm no regression:

```bash
./mvnw -pl core/daemon-boot-collectd test
```

Expected: all existing tests pass. `FanoutPersister`'s 5-arg constructor is package-private and only called from `FanoutPersisterFactory.createPersister` (also updated). If any existing test instantiated `FanoutPersister` with the old 2-arg constructor, that test will fail to compile — STOP and report; we'll need to update those tests too.

- [ ] **Step 9: Commit**

```bash
git add core/daemon-boot-collectd/src/main/java/org/deltav/collectd/timeseries/TimeseriesKafkaPublisherConfiguration.java
git commit -m "feat(daemon-boot-collectd): FanoutPersister observability + dev fail-fast

(X) Two Micrometer counters — deltav.collectd.persister.{inner,kafka}.failures
tagged by visitor step — pre-registered at construction for all 10 steps
(× 2 sides = 20 tag combinations) so ops can alert on rate>0 rather than
missing-metric. Phase 0 horizon-side inner-persister bugs (#1
UnexpectedRollbackException, #2 NPE in TimeseriesPersistOperationBuilder,
#4 ClassCastException in TimeseriesPersister.getUserDefinedMetaTags)
become operator-visible without being fixed here — they remain swallowed
by the existing isolation, deserve their own focused investigation, and
are tracked under project_phase0_inner_persister_bugs_followup.

(Y) Two Spring properties — deltav.collectd.persister.{inner,kafka}.fail-fast,
default false in production. When true, Throwables propagate as
RuntimeException instead of being swallowed + logged. CI / integration
tests enable these via @TestPropertySource so silent regressions surface
as test failures.

Tests landing in the next commit (Task 4): FanoutPersisterTest covers
counter increments, pre-registration, fail-fast matrix, and the
Throwable-not-just-RuntimeException catch (Phase 0 #3 LinkageError scar
guard). CollectdApplicationScanIT exercises real-main-class wiring."
```

---

## Task 4: FanoutPersisterTest + CollectdApplicationScanIT (Commit 5 — test-only per spec)

**Goal:** Pin Task 3's behavior with 7 unit tests + 3 real-main-class IT tests. Mandatory `commons-io:2.18.0` test dep per `feedback_boot4_testcontainers_commons_io`.

**Files:**
- Modify (if needed): `core/daemon-boot-collectd/pom.xml`
- Create: `core/daemon-boot-collectd/src/test/java/org/deltav/collectd/timeseries/FanoutPersisterTest.java`
- Create: `core/daemon-boot-collectd/src/test/java/org/deltav/netmgt/collectd/boot/CollectdApplicationScanIT.java`

- [ ] **Step 1: Verify commons-io:2.18.0 test dep is present**

```bash
grep -A2 "commons-io" core/daemon-boot-collectd/pom.xml | head -10
```

If `commons-io:commons-io:2.18.0` with `<scope>test</scope>` is absent, add to `core/daemon-boot-collectd/pom.xml` inside `<dependencies>` (near other test-scoped deps):

```xml
        <!-- Required by Testcontainers 2.x / commons-compress 1.28+ — see
             feedback_boot4_testcontainers_commons_io memory. Without this,
             container start hangs at the wait-strategy timeout. -->
        <dependency>
            <groupId>commons-io</groupId>
            <artifactId>commons-io</artifactId>
            <version>2.18.0</version>
            <scope>test</scope>
        </dependency>
```

- [ ] **Step 2: Create FanoutPersisterTest — 7 unit tests**

Write `core/daemon-boot-collectd/src/test/java/org/deltav/collectd/timeseries/FanoutPersisterTest.java`:

```java
/*
 * Copyright (C) 2026 BeaconStrategists, Inc.
 *
 * Licensed under the GNU Affero General Public License v3.
 */
package org.deltav.collectd.timeseries;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.deltav.collectd.timeseries.TimeseriesKafkaPublisherConfiguration.FanoutPersister;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opennms.netmgt.collection.api.CollectionResource;
import org.opennms.netmgt.collection.api.Persister;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class FanoutPersisterTest {

    private static final String INNER_FAILURES = "deltav.collectd.persister.inner.failures";
    private static final String KAFKA_FAILURES = "deltav.collectd.persister.kafka.failures";
    private static final String[] STEPS = new String[]{
            "visitCollectionSet", "visitResource", "visitGroup", "visitAttribute",
            "completeAttribute", "completeGroup", "completeResource", "completeCollectionSet",
            "persistNumericAttribute", "persistStringAttribute"
    };

    private MeterRegistry registry;
    private Persister inner;
    private TimeseriesKafkaPersister kafka;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        inner = mock(Persister.class);
        kafka = mock(TimeseriesKafkaPersister.class);
    }

    private FanoutPersister make(boolean failFastInner, boolean failFastKafka) {
        return new FanoutPersister(inner, kafka, registry, failFastInner, failFastKafka);
    }

    @Test
    void counter_increments_on_inner_failure_in_visitResource() {
        CollectionResource r = mock(CollectionResource.class);
        doThrow(new RuntimeException("inner boom")).when(inner).visitResource(r);
        doNothing().when(kafka).visitResource(r);

        make(false, false).visitResource(r);

        Counter c = registry.find(INNER_FAILURES).tag("step", "visitResource").counter();
        assertThat(c).isNotNull();
        assertThat(c.count()).isEqualTo(1.0);
        verify(kafka).visitResource(r);
    }

    @Test
    void counter_increments_on_kafka_failure_in_visitResource() {
        CollectionResource r = mock(CollectionResource.class);
        doNothing().when(inner).visitResource(r);
        doThrow(new RuntimeException("kafka boom")).when(kafka).visitResource(r);

        make(false, false).visitResource(r);

        Counter c = registry.find(KAFKA_FAILURES).tag("step", "visitResource").counter();
        assertThat(c).isNotNull();
        assertThat(c.count()).isEqualTo(1.0);
        verify(inner).visitResource(r);
    }

    @Test
    void counters_preregistered_at_startup_with_zero_value() {
        make(false, false);
        for (String step : STEPS) {
            Counter ic = registry.find(INNER_FAILURES).tag("step", step).counter();
            Counter kc = registry.find(KAFKA_FAILURES).tag("step", step).counter();
            assertThat(ic).as("inner counter step=%s", step).isNotNull();
            assertThat(ic.count()).isZero();
            assertThat(kc).as("kafka counter step=%s", step).isNotNull();
            assertThat(kc.count()).isZero();
        }
    }

    @Test
    void fail_fast_inner_true_rethrows() {
        CollectionResource r = mock(CollectionResource.class);
        doThrow(new RuntimeException("inner boom")).when(inner).visitResource(r);

        FanoutPersister p = make(true, false);
        assertThatThrownBy(() -> p.visitResource(r))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("fail-fast enabled");
    }

    @Test
    void fail_fast_inner_false_swallows_and_continues() {
        CollectionResource r = mock(CollectionResource.class);
        doThrow(new RuntimeException("inner boom")).when(inner).visitResource(r);
        doNothing().when(kafka).visitResource(r);

        make(false, false).visitResource(r);  // must NOT throw

        verify(kafka).visitResource(r);
    }

    @Test
    void fail_fast_kafka_true_rethrows() {
        CollectionResource r = mock(CollectionResource.class);
        doNothing().when(inner).visitResource(r);
        doThrow(new RuntimeException("kafka boom")).when(kafka).visitResource(r);

        FanoutPersister p = make(false, true);
        assertThatThrownBy(() -> p.visitResource(r))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("fail-fast enabled");
    }

    @Test
    void throwable_not_just_runtime_exception() {
        CollectionResource r = mock(CollectionResource.class);
        doThrow(new LinkageError("NoSuchMethodError from horizon"))
                .when(inner).visitResource(r);
        doNothing().when(kafka).visitResource(r);

        make(false, false).visitResource(r);  // must NOT throw

        Counter c = registry.find(INNER_FAILURES).tag("step", "visitResource").counter();
        assertThat(c.count()).isEqualTo(1.0);
        verify(kafka).visitResource(r);
    }
}
```

- [ ] **Step 3: Run FanoutPersisterTest**

```bash
./mvnw -pl core/daemon-boot-collectd test -Dtest=FanoutPersisterTest
```

Expected: `7/7 PASS` in <1s. If any fail, the Task 3 implementation has a defect — re-read the test failure and adjust the impl in `TimeseriesKafkaPublisherConfiguration.java` (tests stay fixed; impl bends).

- [ ] **Step 4: Create CollectdApplicationScanIT — 3 real-main-class IT tests**

Write `core/daemon-boot-collectd/src/test/java/org/deltav/netmgt/collectd/boot/CollectdApplicationScanIT.java`:

```java
/*
 * Copyright (C) 2026 BeaconStrategists, Inc.
 *
 * Licensed under the GNU Affero General Public License v3.
 */
package org.deltav.netmgt.collectd.boot;

import org.apache.kafka.clients.admin.NewTopic;
import org.deltav.collectd.timeseries.TimeseriesKafkaPublisher;
import org.deltav.collectd.timeseries.TimeseriesKafkaPublisherConfiguration.FanoutPersisterFactory;
import org.junit.jupiter.api.Test;
import org.opennms.netmgt.collection.api.PersisterFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Real-main-class integration test. Boots CollectdApplication via
 * SpringApplication.run() against a Testcontainers Kafka broker with
 * deltav.timeseries.enabled=true and fail-fast toggles on. Asserts the
 * composite PersisterFactory wires correctly so horizon's CollectableService
 * resolves to our FanoutPersisterFactory, not the bare inner factory.
 *
 * <p>Phase 2 PR #174 scar-prophylactic lineage: real-main-class startup
 * catches @Configuration and bean-wiring bugs that unit tests + @Import-based
 * ITs all bypass. Mandatory commons-io:2.18.0 test dep added to pom per
 * feedback_boot4_testcontainers_commons_io memory.
 */
@Testcontainers
@SpringBootTest(classes = CollectdApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestPropertySource(properties = {
        "deltav.timeseries.enabled=true",
        "deltav.collectd.persister.inner.fail-fast=true",
        "deltav.collectd.persister.kafka.fail-fast=true",
        // H2 in-memory DB to avoid needing a real Postgres in this IT
        "spring.datasource.url=jdbc:h2:mem:scan-it;MODE=PostgreSQL",
        "spring.jpa.hibernate.ddl-auto=create-drop"
})
class CollectdApplicationScanIT {

    @Container
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.0"))
                    .withStartupTimeout(Duration.ofSeconds(120));

    @DynamicPropertySource
    static void kafkaProps(DynamicPropertyRegistry reg) {
        reg.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        reg.add("spring.cloud.stream.kafka.binder.brokers", KAFKA::getBootstrapServers);
    }

    @Autowired ApplicationContext ctx;

    @Test
    void persister_factory_autowires_to_FanoutPersisterFactory_when_flag_true() {
        PersisterFactory factory = ctx.getBean(
                "compositePersisterFactory", PersisterFactory.class);
        assertThat(factory)
                .as("with deltav.timeseries.enabled=true, the @Primary "
                        + "compositePersisterFactory must be FanoutPersisterFactory")
                .isInstanceOf(FanoutPersisterFactory.class);
    }

    @Test
    void deltav_timeseries_topic_bean_exists_when_flag_true() {
        NewTopic topic = ctx.getBean("deltavTimeseriesTopic", NewTopic.class);
        assertThat(topic).isNotNull();
        assertThat(topic.name()).isEqualTo("deltav-timeseries");
    }

    @Test
    void timeseries_kafka_publisher_bean_exists_when_flag_true() {
        TimeseriesKafkaPublisher publisher = ctx.getBean(TimeseriesKafkaPublisher.class);
        assertThat(publisher).isNotNull();
    }
}
```

- [ ] **Step 5: Run the IT**

```bash
./mvnw -pl core/daemon-boot-collectd verify -Dit.test=CollectdApplicationScanIT
```

Expected: `3/3 PASS` in ~30-60s (Testcontainers Kafka spin-up dominates).

If the test fails with `Timed out waiting for log output matching '.*Transitioning from RECOVERY to RUNNING.*'`: the `commons-io:2.18.0` dep isn't on the classpath. Re-check Step 1.

If `persister_factory_autowires_to_FanoutPersisterFactory_when_flag_true` fails (bean is wrong type): the Task 3 changes broke the @Bean method. Most likely a stale build — `./mvnw -pl core/daemon-boot-collectd clean install -DskipTests` then re-run.

- [ ] **Step 6: Run the full module test suite to ensure no regression**

```bash
./mvnw -pl core/daemon-boot-collectd verify
```

Expected: all unit + IT tests pass. The Task 3 + Task 4 changes are now fully covered.

- [ ] **Step 7: Commit**

```bash
git add core/daemon-boot-collectd/pom.xml \
        core/daemon-boot-collectd/src/test/java/org/deltav/collectd/timeseries/FanoutPersisterTest.java \
        core/daemon-boot-collectd/src/test/java/org/deltav/netmgt/collectd/boot/CollectdApplicationScanIT.java
git commit -m "test(daemon-boot-collectd): FanoutPersisterTest + CollectdApplicationScanIT

T0 FanoutPersisterTest (7 unit tests):
  - counter increments on inner / kafka failure (visitResource)
  - counters pre-registered at startup with zero value (all 10 steps)
  - fail-fast inner / kafka true / false matrix
  - Throwable catch covers LinkageError (Phase 0 #3 NoSuchMethodError scar guard)

T1 CollectdApplicationScanIT (3 real-main-class ITs, Testcontainers Kafka):
  - PersisterFactory autowires to FanoutPersisterFactory when flag=true
  - deltavTimeseriesTopic NewTopic bean exists
  - TimeseriesKafkaPublisher bean exists

Phase 2 PR #174 scar-prophylactic lineage: real-main-class boot via
SpringApplication.run() against Testcontainers Kafka catches
@Configuration class-name vs @Bean-method-name collisions and bean-wiring
bugs that unit tests + @Import-based ITs all bypass. Mandatory
commons-io:2.18.0 test dep added per feedback_boot4_testcontainers_commons_io
memory."
```

---

## Task 5: Compose default flip + proto-header substitution (Commit 6)

**Goal:** Flip `DELTAV_TIMESERIES_ENABLED` default `false` → `true` so the Phase 0 producer is on by default in the delta-v stack. Resolve Phase 2's deferred `<TBD-phase-2-PR>` proto-header placeholder to `#174`.

**Files:**
- Modify: `opennms-container/delta-v/docker-compose.yml`
- Modify: `core/deltav-kafka-contracts/src/main/proto/deltav-timeseries.proto`
- Modify: `core/deltav-kafka-contracts/src/main/proto/deltav-node-context.proto`

- [ ] **Step 1: Flip the compose default**

Find line 176 of `opennms-container/delta-v/docker-compose.yml`:

```yaml
      DELTAV_TIMESERIES_ENABLED: ${DELTAV_TIMESERIES_ENABLED:-false}
```

Change to:

```yaml
      DELTAV_TIMESERIES_ENABLED: ${DELTAV_TIMESERIES_ENABLED:-true}
```

- [ ] **Step 2: Substitute the placeholder in both proto headers**

Verify the placeholder exists:

```bash
grep -n "TBD-phase-2-PR" core/deltav-kafka-contracts/src/main/proto/deltav-timeseries.proto core/deltav-kafka-contracts/src/main/proto/deltav-node-context.proto
```

Expected: each file has one match in its header comment block. Edit each (do NOT use `sed -i` — manual edit confirms the line context):

In `core/deltav-kafka-contracts/src/main/proto/deltav-timeseries.proto`, find:

```protobuf
// API version: 1 (FROZEN at Phase 2 GA — delta-v#<TBD-phase-2-PR>)
```

Change to:

```protobuf
// API version: 1 (FROZEN at Phase 2 GA — delta-v#174)
```

Same change in `core/deltav-kafka-contracts/src/main/proto/deltav-node-context.proto`.

- [ ] **Step 3: Verify yaml + proto compile**

```bash
cd opennms-container/delta-v && docker compose --profile lite --profile metrics-e2e config >/dev/null
cd /Users/david/development/src/opennms/delta-v
./mvnw -pl core/deltav-kafka-contracts -DskipTests clean install
```

Expected: both exit 0. Compose config parses; protobuf-maven-plugin BUILD SUCCESS.

- [ ] **Step 4: Commit**

```bash
git add opennms-container/delta-v/docker-compose.yml \
        core/deltav-kafka-contracts/src/main/proto/deltav-timeseries.proto \
        core/deltav-kafka-contracts/src/main/proto/deltav-node-context.proto
git commit -m "feat(compose+proto): enable Kafka time-series publisher by default; pin Phase 2 proto header

Compose default flipped from DELTAV_TIMESERIES_ENABLED=false to =true so
the Phase 0 producer is on by default in the delta-v stack (matches its
'DONE + E2E verified' status as of this PR). Operators who need it off
can still export the env var explicitly.

Phase 2 proto-header placeholder resolved: <TBD-phase-2-PR> -> #174 in
both deltav-timeseries.proto and deltav-node-context.proto."
```

---

## Task 6: Diagnostic cleanup (Commit 7 — partial revert of `d3c6d049cab`)

**Goal:** Remove the temporary `env` + `beans` actuator exposure and the one-shot startup log added in Commit 1. **Keep `/actuator/conditions`** permanently (no secret exposure, useful for future wiring-regression triage). Keep the `compositePersisterFactory wrapping inner=...` log because Task 3 extended it to also report fail-fast state — it's now permanently informative at startup.

**Files:**
- Modify: `core/daemon-boot-collectd/src/main/resources/application.yml`
- Modify: `core/daemon-boot-collectd/src/main/java/org/deltav/collectd/timeseries/TimeseriesKafkaPublisherConfiguration.java`

- [ ] **Step 1: Revert env + beans from actuator exposure**

Edit `core/daemon-boot-collectd/src/main/resources/application.yml`. Find the include line set in Commit 1:

```yaml
        include: health, info, prometheus, env, beans, conditions
```

Change to:

```yaml
        include: health, info, prometheus, conditions
```

(Keep `conditions` permanently.)

- [ ] **Step 2: Remove the no-arg constructor diagnostic LOG.info**

Edit `core/daemon-boot-collectd/src/main/java/org/deltav/collectd/timeseries/TimeseriesKafkaPublisherConfiguration.java`. Delete the no-arg constructor block added in Commit 1:

```java
    public TimeseriesKafkaPublisherConfiguration() {
        LOG.info("TimeseriesKafkaPublisherConfiguration loaded — @ConditionalOnProperty(deltav.timeseries.enabled=true) matched");
    }
```

Leave the `private static final Logger LOG = ...` field — `runInner` and `runKafka` still use it for WARN logs, and the `compositePersisterFactory` @Bean method (Task 3) also uses it.

The `@Bean` method's `LOG.info("Creating compositePersisterFactory wrapping inner={}@{} (failFastInner={}, failFastKafka={})", ...)` from Task 3 stays — it's now permanently useful at startup.

- [ ] **Step 3: Verify all tests still pass**

```bash
./mvnw -pl core/daemon-boot-collectd verify
```

Expected: all tests still green.

- [ ] **Step 4: Commit**

```bash
git add core/daemon-boot-collectd/src/main/resources/application.yml \
        core/daemon-boot-collectd/src/main/java/org/deltav/collectd/timeseries/TimeseriesKafkaPublisherConfiguration.java
git commit -m "chore(daemon-boot-collectd): revert temporary env/beans actuator exposure + one-shot startup log

Commit d3c6d049cab exposed /actuator/env + /actuator/beans + /actuator/conditions
and added two diagnostic startup logs so we could identify the wiring
hypothesis. Root cause is captured in
project_collectd_publisher_inert_investigation memory; the real bug
turned out to be the Phase 2 NodeContextKafkaBootstrap race (fixed
earlier in this PR), not Collectd-side wiring.

Revert env + beans exposure (noisy + can expose secrets) and the
'TimeseriesKafkaPublisherConfiguration loaded' log (one-shot startup
line not worth keeping). Keep /actuator/conditions on permanently —
costs nothing at runtime, exposes no secrets, makes future wiring
diagnosis one curl away. Keep the compositePersisterFactory wrap log
because it now also reports fail-fast state (Task 3 extended it)."
```

---

## Task 7: Full-reactor verify

**Goal:** Confirm the Task 1-6 changes don't cascade into other reactor modules.

- [ ] **Step 1: Run the full-reactor build**

```bash
./mvnw -B -DskipTests -fae clean install 2>&1 | tail -10
```

Expected: `BUILD SUCCESS`. Both `core/prometheus-writer` and `core/daemon-boot-collectd` compile against their changes; no other reactor module breaks. The `feedback_delta_v_full_reactor_verify` memory cites this as a mandatory pre-push gate.

If any module fails with `cannot find symbol` referencing `FanoutPersister.<init>(...)` or `FanoutPersisterFactory.<init>(...)`: an unexpected caller exists. Both classes are package-private inside `core/daemon-boot-collectd` so this is unlikely, but if it happens — STOP and report.

- [ ] **Step 2: No commit** — verification gate only.

---

## Task 8: Full E2E acceptance gate

**Goal:** Run the Phase 2 E2E end-to-end. Acceptance bar from spec §7.

- [ ] **Step 1: Rebuild all delta-v Docker images**

```bash
./opennms-container/delta-v/build.sh deltav 2>&1 | tail -5
```

Expected: `opennms/collectd:0.0.1-SNAPSHOT`, `opennms/prometheus-writer:0.0.1-SNAPSHOT`, plus the other 12 daemon images. Self-healing freshness check should rebuild collectd + prometheus-writer (both have source changes). If either is unexpectedly skipped, force a rebuild via `touch core/daemon-boot-collectd/pom.xml core/prometheus-writer/pom.xml` and retry.

- [ ] **Step 2: Run the Phase 2 E2E with proper exit-code propagation**

```bash
set -o pipefail
./opennms-container/delta-v/test-prometheus-writer-e2e.sh > /tmp/v2-e2e.log 2>&1
echo "E2E exit code: $?"
tail -20 /tmp/v2-e2e.log
```

Expected: exit `0`, log ends with `==> ALL ASSERTIONS PASSED`.

The compose default (Task 5 Step 1) is now `true`, so `DELTAV_TIMESERIES_ENABLED` does not need to be exported. The race fix (Tasks 1-2) means prometheus-writer's NodeContextCache populates correctly. The compose script's existing assertions (`records_consumed_total > 0`, `samples_sent_total > 0`, `batches_sent_total > 0`, `circuit_state == 0`, all failure counters == 0, VictoriaMetrics returns the expected series with full label set) should all pass.

- [ ] **Step 3: If the E2E fails — STOP and report**

`grep -E "^==>|^FAIL" /tmp/v2-e2e.log` to identify which step broke. Possible failures:
- `enrichment_missing_total > 0` (acceptance criterion 7 in spec): the race fix didn't fully resolve the issue. Re-check Task 1 Step 2 — likely the `bootstrapMarked` gate or the rebalance listener has a defect.
- `batches_failed_total > 0` (Phase 0 inner bug surfaced as Kafka-side failure): unlikely but possible.
- VictoriaMetrics query returns no results: VM container didn't come up, or the labels differ from what Phase 2's E2E asserts. Check `docker compose logs victoriametrics`.

Per spec §5, Phase 0 inner bugs #1, #2, #4 are punted to a follow-up PR. They should NOT block this E2E because Kafka publishes regardless. If one of them surfaces in a way that DOES block, that's the (II) decision being violated — STOP and report; don't try to inline-fix.

- [ ] **Step 4: No commit** — acceptance gate only.

---

## Task 9: Push + open PR

**Goal:** Push branch + open PR against `pbrane/delta-v develop`.

- [ ] **Step 1: Confirm branch is clean**

```bash
git status --short
git log --oneline develop..HEAD | wc -l
```

Expected: clean working tree (modulo `provisiond-overlay/etc/imports/*.xml` requisition drift per `feedback_provisiond_requisition_drift` — leave unstaged). Commit count: 9 (1 spec-v1 + 1 plan-v1 + 1 diag-exposure + 1 spec-v2 + 6 new from this plan = 9 ahead of `develop`). 1 spec-v2 supersedes spec-v1 in content; both stay in history.

- [ ] **Step 2: Push**

```bash
git push -u origin fix/collectd-publisher-inert 2>&1 | tail -3
```

- [ ] **Step 3: Open the PR — NEVER use OpenNMS/opennms**

```bash
gh pr create --repo pbrane/delta-v --base develop \
    --title "fix(prometheus-writer): NodeContextKafkaBootstrap race + Collectd silent-swallow hygiene" \
    --body "$(cat <<'BODY'
## Summary

Unblocks the Phase 2 prometheus-writer E2E (test-prometheus-writer-e2e.sh).
First end-to-end pass of the full delta-v Kafka time-series pipeline.

- **Race fix (the critical change)**: `core/prometheus-writer/.../NodeContextKafkaBootstrap.java` no longer takes a 'no partitions yet' branch that skipped `consumer.assign()/.subscribe()` and silently looped on `IllegalStateException` forever. Replaced with single-path `consumer.subscribe(...)` + `ConsumerRebalanceListener` that handles topic-exists-at-startup AND topic-created-later uniformly.
- **Race regression IT**: new `NodeContextKafkaBootstrapIT.bootstrap_survives_topic_created_after_start` exercises the exact scenario that was broken.
- **Collectd silent-swallow hygiene**: `FanoutPersister` gains `(X)` two Micrometer counters (`deltav.collectd.persister.{inner,kafka}.failures{step=…}`, pre-registered for all 10 visitor steps) + `(Y)` two Spring fail-fast properties (`deltav.collectd.persister.{inner,kafka}.fail-fast`, default false). Surface inner-persister bugs that are currently swallowed without operator signal.
- **Real-main-class scan IT**: new `CollectdApplicationScanIT` (3 tests) — Phase 2 PR #174 scar-prophylactic lineage — would have pointed at the Phase 2 consumer side earlier if it had existed.
- **Compose default flip**: `DELTAV_TIMESERIES_ENABLED` default `false` → `true` in `docker-compose.yml`.
- **Phase 2 proto-header pin**: `<TBD-phase-2-PR>` → `#174` in both `deltav-timeseries.proto` and `deltav-node-context.proto`.

## Investigation history

The original v1 spec/plan assumed the Phase 0 Collectd publisher was inert on develop. Live-stack diagnosis (commit `d3c6d049cab` exposed the actuator + startup logs that made the truth visible) refuted the hypothesis: the publisher works fine — 6 batches per 90 s, real records on the topic, prometheus-writer consumes them. The actual bug is in Phase 2's consumer-side `NodeContextKafkaBootstrap`. Memory `project_collectd_publisher_inert_investigation` has the full root-cause narrative + evidence.

The v1 spec (`8c2729588e5`) and v1 plan (`262309050ed`) are retained on the branch for audit. The v2 spec (`f0a4d5b0f94`) and v2 plan (this commit) supersede them.

## Out of scope (memory-noted, separate PRs)

- Phase 0 inner-persister bugs #1 (UnexpectedRollbackException), #2 (NPE in TimeseriesPersistOperationBuilder), and new #4 (ClassCastException in TimeseriesPersister.getUserDefinedMetaTags). All confirmed present and counted by this PR's new (X) counters; all currently swallowed by FanoutPersister isolation; none block Phase 2 E2E. See `project_phase0_inner_persister_bugs_followup` memory.
- `build.sh check_daemon_boot_freshness` transitive-dep gap (forced `touch core/daemon-boot-collectd/pom.xml` during PR #174 investigation).
- Phase 3 Thresholder.

## Acceptance verification

- [x] `./mvnw -pl core/prometheus-writer verify` — all existing Phase 2 tests + new race-IT pass
- [x] `./mvnw -pl core/daemon-boot-collectd verify` — 7 FanoutPersisterTest + 3 CollectdApplicationScanIT + existing tests pass
- [x] `./mvnw -B -DskipTests -fae clean install` — full-reactor BUILD SUCCESS
- [x] `./opennms-container/delta-v/test-prometheus-writer-e2e.sh` — exit 0, ALL ASSERTIONS PASSED (first end-to-end pass)
- [x] Memory `project_collectd_publisher_inert_investigation` updated to RESOLVED with confirmed real root cause
- [x] Memory `project_phase0_inner_persister_bugs_followup` created with stack traces + per-step rates from this investigation
- [x] Memory `project_nodecontext_startup_race_pattern` (feedback) created with the rule

## Spec + plan

- Spec (v2): `docs/superpowers/specs/2026-04-18-collectd-publisher-inert-fix-design.md`
- Plan (v2): `docs/superpowers/plans/2026-04-18-collectd-publisher-inert-fix.md`
BODY
)"
```

- [ ] **Step 4: Record the PR URL** (output of `gh pr create`).

- [ ] **Step 5: Post-merge memory updates** (do these manually after merge, not part of this PR):

- Flip `project_collectd_publisher_inert_investigation` status: `DIAGNOSED` → `RESOLVED (delta-v#TBD)`.
- Add addendum to `project_phase2_prometheus_writer_done` noting "E2E now passes end-to-end as of delta-v#TBD".
- Create `project_phase0_inner_persister_bugs_followup` capturing bugs #1, #2, #4 with the per-step counts from this investigation.
- Create feedback memory `project_nodecontext_startup_race_pattern` documenting the rule: *any Spring-Kafka consumer using `consumer.partitionsFor(topic)` to make wiring decisions must handle the topic-does-not-exist-yet case via `subscribe` + `ConsumerRebalanceListener`, never via skip-and-enter-liveTail.*

---

## Self-review (post-plan, pre-execution)

**Spec coverage check** (each spec section → plan task):

| Spec section | Plan task(s) |
|---|---|
| §1 Context and goal | Plan preamble |
| §2 Scope (in + out) | Task ordering rationale + per-task scope notes |
| §3 Race-fix design | Task 1 |
| §4 Commit 1 (already landed) | (no task — pre-existing) |
| §4 Commit 2 (race fix) | Task 1 |
| §4 Commit 3 (race IT) | Task 2 |
| §4 Commit 4 (FanoutPersister obs + fail-fast) | Task 3 |
| §4 Commit 5 (FanoutPersisterTest + scan IT) | Task 4 |
| §4 Commit 6 (compose + proto) | Task 5 |
| §4 Commit 7 (diagnostic cleanup) | Task 6 |
| §5 Phase 0 inner bugs out-of-scope | Task 8 Step 3 (escape hatch + memory follow-up note) + Task 9 Step 5 (memory creation) |
| §6 Test strategy | Tasks 1-4 (T0 + T1 + race-IT) + Task 8 (live E2E) |
| §7 Acceptance criteria | Tasks 7-9 |
| §8 Risks and trade-offs | Inline in Task 1 (rebalance edge cases note) + Task 8 Step 3 (Phase 0 bug escape hatch) |
| §9 References | Plan preamble |

**Placeholder scan**: only legitimate runtime-fill placeholders — `TBD` in the post-merge memory updates (waiting on actual PR number) and the `<TBD-phase-2-PR>` literal in Task 5's substitution target. No plan-level TBDs.

**Type consistency** (post-Task-3): `FanoutPersister(Persister, TimeseriesKafkaPersister, MeterRegistry, boolean, boolean)` constructor signature is consistent in Task 3 Step 3 (impl), Task 4 Step 2 (`make()` helper in `FanoutPersisterTest`), and Task 4 Step 4 (no direct construction in `CollectdApplicationScanIT` — type-based bean lookup). `FanoutPersisterFactory(PersisterFactory, TimeseriesKafkaPublisher, MeterRegistry, boolean, boolean)` consistent in Task 3 Step 5 (impl) and Task 3 Step 6 (`@Bean` injection signature). Bean names `compositePersisterFactory` (this PR keeps the name from v1's diagnostic commit; the v2 design rejected the rename hypothesis since wiring was correct) and `timeseriesPersisterFactory` (horizon's inner) are stable across all references. Counter meter names `deltav.collectd.persister.inner.failures` / `deltav.collectd.persister.kafka.failures` consistent in Task 3 (constants), Task 4 Step 2 (test constants), Task 4 Step 7 (commit message).

---

## Execution handoff

Plan complete and saved to `docs/superpowers/plans/2026-04-18-collectd-publisher-inert-fix.md` (overwriting v1 in place; v1 remains in branch history at commit `262309050ed`).

The execution path is the same `superpowers:subagent-driven-development` already in use this session. Resume with Task 1.
