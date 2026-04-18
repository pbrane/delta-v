# Collectd Kafka Publisher Inert Fix — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Unblock the delta-v Kafka Time Series pipeline by identifying and fixing the wiring regression that makes `TimeseriesKafkaPublisher` silently inert, ship observability + regression tests so this class of silent swallow cannot re-occur, flip the compose default, and close out Phase 2's deferred proto-header placeholder. Acceptance is the Phase 2 E2E (`opennms-container/delta-v/test-prometheus-writer-e2e.sh`) exiting 0 with `ALL ASSERTIONS PASSED`.

**Architecture:** Diagnose-first, fix-second, then harden. Commit 1 exposes actuator endpoints + adds two diagnostic `LOG.info` lines to surface the runtime wiring decisions. Commit 2 ships the minimum-diff surgical fix once evidence identifies the root cause. Commit 3 adds Micrometer counters + Spring fail-fast properties inside `FanoutPersister`. Commit 4 adds `FanoutPersisterTest` unit tests + `CollectdApplicationScanIT` real-main-class IT. Commit 5 flips `DELTAV_TIMESERIES_ENABLED` default and substitutes the `<TBD-phase-2-PR>` proto placeholder. Commit 6 partially reverts Commit 1 (keeps `/actuator/conditions`).

**Tech Stack:** Spring Boot 4.0.3, horizon 1.0.10, Spring Cloud Stream Kafka binder, Micrometer Prometheus, Resilience4j unchanged (Collectd doesn't use it), JUnit 5, Mockito, Testcontainers Kafka (`apache/kafka:3.8.0`).

**Spec:** `docs/superpowers/specs/2026-04-18-collectd-publisher-inert-fix-design.md`

**Branch:** `fix/collectd-publisher-inert` (already created; spec commit `8c2729588e5` already on branch).

**PR target:** `pbrane/delta-v` base `develop`. NEVER `OpenNMS/opennms`.

**Critical memory references:**
- `feedback_never_pr_opennms` — PRs always `--repo pbrane/delta-v`.
- `feedback_feature_branches` — never commit directly to `develop`.
- `feedback_delta_v_uses_mvnw_not_compile_pl` — use `./mvnw`, never `compile.pl`.
- `feedback_delta_v_full_reactor_verify` — run full-reactor build before push.
- `feedback_boot4_testcontainers_commons_io` — Testcontainers needs explicit `commons-io:2.18.0` test dep on Boot 4.
- `feedback_configuration_class_bean_name_collision` — `@Configuration` classes must not share camelCase name with their `@Bean` methods.
- `feedback_deltav_package_namespace` — new code under `org.deltav.*` with BeaconStrategists copyright.
- `project_collectd_publisher_inert_investigation` — root-cause hypothesis space + investigation plan.

---

## Task ordering rationale

14 tasks grouped by the six commits from the spec. Task ordering is dependency-driven. Commit 2's fix is contingent on Commit 1's evidence; the plan describes the most-likely shape (hypothesis C — bean-name vs `@Primary` injection) and flags adjustments for A / B / D.

1. **Commit 1 — diagnostic exposure** (Tasks 1-2)
2. **Commit 2 — wiring fix** (Task 3)
3. **Commit 3 — observability + fail-fast** (Tasks 4-8)
4. **Commit 4 — scan IT** (Task 9)
5. **Commit 5 — compose default + proto headers** (Task 10)
6. **Commit 6 — diagnostic cleanup** (Task 11)
7. **Acceptance gates** (Tasks 12-14)

---

## Task 1: Expose diagnostic actuator endpoints + add startup logs

**Files:**
- Modify: `core/daemon-boot-collectd/src/main/resources/application.yml`
- Modify: `core/daemon-boot-collectd/src/main/java/org/deltav/collectd/timeseries/TimeseriesKafkaPublisherConfiguration.java`

- [ ] **Step 1: Extend actuator exposure in application.yml**

Locate the `management.endpoints.web.exposure.include` setting in `core/daemon-boot-collectd/src/main/resources/application.yml`. Read the file first to find exact current state:

```bash
grep -nE "management|endpoint|exposure" core/daemon-boot-collectd/src/main/resources/application.yml
```

If the include line already exists, edit to add `env,beans,conditions`:

```yaml
management:
  endpoints:
    web:
      exposure:
        include: health, info, prometheus, env, beans, conditions
```

If the block doesn't exist yet (possible — Collectd may rely on Spring Boot defaults), add the full block at the end of the file, before any trailing `---` document-end marker.

- [ ] **Step 2: Add diagnostic startup logs to TimeseriesKafkaPublisherConfiguration**

Edit `core/daemon-boot-collectd/src/main/java/org/deltav/collectd/timeseries/TimeseriesKafkaPublisherConfiguration.java`.

First, confirm the existing logger import (`org.slf4j.Logger`, `org.slf4j.LoggerFactory`) is present. If not, add them at the top.

Add a class-level `LOG` field near the top of the class (before the `@Bean` methods), immediately after the `@ConditionalOnProperty` annotation and class declaration:

```java
    private static final Logger LOG = LoggerFactory.getLogger(TimeseriesKafkaPublisherConfiguration.class);
```

Add a no-arg constructor that logs load-time evidence. Insert after the `LOG` field, before the first `@Bean`:

```java
    public TimeseriesKafkaPublisherConfiguration() {
        LOG.info("TimeseriesKafkaPublisherConfiguration loaded — @ConditionalOnProperty(deltav.timeseries.enabled=true) matched");
    }
```

Modify the `compositePersisterFactory` `@Bean` method to log the inner-factory identity just before returning:

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

- [ ] **Step 3: Verify the module compiles**

```bash
./mvnw -pl core/daemon-boot-collectd -DskipTests compile
```

Expected: `BUILD SUCCESS`. The startup logs do not yet run; they fire only when the app boots.

- [ ] **Step 4: Commit**

```bash
git add core/daemon-boot-collectd/src/main/resources/application.yml \
        core/daemon-boot-collectd/src/main/java/org/deltav/collectd/timeseries/TimeseriesKafkaPublisherConfiguration.java
git commit -m "diag(daemon-boot-collectd): expose env/beans/conditions + startup logs

Temporary diagnostic exposure on Collectd's actuator plus two INFO log
lines in TimeseriesKafkaPublisherConfiguration (class load + composite
persister factory wrap). Lets us confirm which of the three wiring
hypotheses (property resolution, condition evaluation, or @Primary vs
qualifier injection) explains the silent inert publisher on develop.

Will be partially reverted in Commit 6 — /actuator/conditions stays
on permanently; env + beans + startup logs revert."
```

---

## Task 2: Capture evidence + identify root cause

**Files:**
- No code changes — this is a run-experiment + memory-update task.
- Modify: `/Users/david/.claude/projects/-Users-david-development-src-opennms-delta-v/memory/project_collectd_publisher_inert_investigation.md`

- [ ] **Step 1: Rebuild Collectd image**

The `build.sh check_daemon_boot_freshness` check walks `daemon-boot-*/src/main/` + `pom.xml` but misses transitive deps. Since we just modified `daemon-boot-collectd`'s own sources, the check will catch it. If not, touch the pom as a workaround:

```bash
touch core/daemon-boot-collectd/pom.xml
./opennms-container/delta-v/build.sh deltav 2>&1 | tail -5
```

Expected: `opennms/collectd:0.0.1-SNAPSHOT` rebuilt. Other 13 daemon-boot images stay cached.

- [ ] **Step 2: Start the stack**

```bash
cd opennms-container/delta-v
DELTAV_TIMESERIES_ENABLED=true docker compose --profile lite --profile metrics-e2e up -d
echo "--- sleeping 90s for first full poll cycle ---"
sleep 90
```

- [ ] **Step 3: Capture log evidence**

```bash
docker compose logs --no-color collectd 2>&1 | \
  grep -E "TimeseriesKafkaPublisherConfiguration|compositePersisterFactory|persister.factory|PersisterFactory"
```

Three possible outcomes:
- **Neither log line present**: class did not load → hypothesis A (property) or B (condition).
- **Only the class-load line present**: class loaded but `@Bean` did not run → unexpected (hypothesis D) — possibly the bean ran but the log did not flush, possibly a deeper Spring issue.
- **Both lines present**: class loaded and composite was wrapped → hypothesis C (bean-name resolution bypass).

- [ ] **Step 4: Capture actuator evidence**

```bash
docker compose exec -T collectd curl -sf http://localhost:8080/actuator/env/deltav.timeseries.enabled 2>&1 | head -20
echo "---"
docker compose exec -T collectd curl -sf http://localhost:8080/actuator/conditions 2>&1 | \
  python3 -c 'import json,sys; c=json.load(sys.stdin); m=c["contexts"]["application"]; \
    print("positive:", "TimeseriesKafkaPublisherConfiguration" in m.get("positiveMatches",{})); \
    print("negative:", [k for k in m.get("negativeMatches",{}) if "Timeseries" in k])'
echo "---"
docker compose exec -T collectd curl -sf http://localhost:8080/actuator/beans 2>&1 | \
  python3 -c 'import json,sys; b=json.load(sys.stdin)["contexts"]["application"]["beans"]; \
    print({name: {"type": v["type"].split(".")[-1], "primary": v.get("primary", False)} \
           for name, v in b.items() if "PersisterFactory" in v.get("type","")})'
```

- [ ] **Step 5: Confirm topic state**

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
  --describe --topic deltav-timeseries 2>&1 | head -10
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 \
  --describe --group prometheus-writer 2>&1 | head -10
```

- [ ] **Step 6: Tear down**

```bash
docker compose --profile lite --profile metrics-e2e down -v --remove-orphans
```

- [ ] **Step 7: Update the investigation memory with captured evidence**

Edit `/Users/david/.claude/projects/-Users-david-development-src-opennms-delta-v/memory/project_collectd_publisher_inert_investigation.md`. Under `## Hypothesis space`, add a new section:

```markdown
## Root cause identified (2026-04-18)

**Confirmed hypothesis:** [A / B / C / D — fill from evidence]

### Evidence

**Startup log lines** (from `docker compose logs collectd`):
```
[paste the matching log lines here]
```

**Actuator /conditions for TimeseriesKafkaPublisherConfiguration**:
```
[paste positive-matches or negative-matches JSON excerpt]
```

**Actuator /beans for PersisterFactory type**:
```
[paste the bean map]
```

**Kafka topic state**: [records present or zero across N partitions]

### Root cause narrative

[One paragraph explaining why hypothesis X fits the evidence. Tie the actuator + log evidence together. Cite specific bean names and their @Primary status.]
```

- [ ] **Step 8: No commit — evidence is captured in the memory note, not the repo**

Memory files are under `~/.claude/...` and are not part of the repo. The evidence-capture step is a diagnostic step whose artifact is the memory update + the informed decision about Commit 2's fix shape.

---

## Task 3: Wiring fix (contingent on Task 2's evidence)

**Files (expected, for hypothesis C — adjust for A or B):**
- Modify: `core/daemon-boot-collectd/src/main/java/org/deltav/collectd/timeseries/TimeseriesKafkaPublisherConfiguration.java`

- [ ] **Step 1: Apply the fix per confirmed hypothesis**

### Hypothesis C — bean-name vs `@Primary` injection (strongest prior)

Horizon's `CollectableService` resolves `PersisterFactory` by `@Qualifier("timeseriesPersisterFactory")` rather than by type. `@Primary` is bypassed. Rename the composite bean to match the name horizon expects.

**Edit 1**: Find the `compositePersisterFactory` `@Bean` method. Change its `@Bean` annotation + method name:

```java
    // Before:
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

```java
    // After:
    @Bean(name = "timeseriesPersisterFactory")
    @Primary
    public PersisterFactory timeseriesPersisterFactory(
            @Qualifier("innerTimeseriesPersisterFactory") PersisterFactory innerFactory,
            TimeseriesKafkaPublisher publisher) {
        LOG.info("Creating composite timeseriesPersisterFactory wrapping inner={}@{}",
                innerFactory.getClass().getName(),
                System.identityHashCode(innerFactory));
        return new FanoutPersisterFactory(innerFactory, publisher);
    }
```

**Edit 2**: Find the inner `PersisterFactory` bean — it's declared in a companion `@Configuration` in `core/daemon-boot-collectd` (search for `@Bean.*PersisterFactory` to locate it). If the commit `7760eef1422` ("rename persisterFactory bean to timeseriesPersisterFactory") is relevant, look at where that rename landed. Expected location:

```bash
grep -rn "timeseriesPersisterFactory" core/daemon-boot-collectd/src/main/java/
```

Rename the inner bean from `timeseriesPersisterFactory` to `innerTimeseriesPersisterFactory` via `@Bean(name = "innerTimeseriesPersisterFactory")`:

```java
    // Before:
    @Bean
    public PersisterFactory timeseriesPersisterFactory(...) { ... }
```

```java
    // After:
    @Bean(name = "innerTimeseriesPersisterFactory")
    public PersisterFactory innerTimeseriesPersisterFactory(...) { ... }
```

Both inner and outer beans now use explicit `name=` so horizon's `@Qualifier("timeseriesPersisterFactory")` resolves to OUR composite, which internally wraps `innerTimeseriesPersisterFactory`.

### Hypothesis A — property not resolving

Edit `core/daemon-boot-collectd/src/main/resources/application.yml`. Add an explicit binding of `deltav.timeseries.enabled` with the env-var fallback (belt-and-suspenders — the yaml binding happens first; env-var wins if present because `@Value`/env precedence). Locate the existing `deltav:` block:

```yaml
deltav:
  timeseries:
    enabled: ${DELTAV_TIMESERIES_ENABLED:false}
```

This already exists per prior inspection. If hypothesis A is confirmed, the issue is that the yaml binding is not propagating to `@ConditionalOnProperty`. The fix is to add an explicit `@PropertySource` or replace `@ConditionalOnProperty` with `@ConditionalOnExpression`. Apply the hypothesis-B fix (below) instead — it subsumes A.

### Hypothesis B — condition evaluating false

Edit the class-level annotation in `TimeseriesKafkaPublisherConfiguration.java`:

```java
    // Before:
    @ConditionalOnProperty(name = "deltav.timeseries.enabled", havingValue = "true")
```

```java
    // After:
    @ConditionalOnExpression("#{'${deltav.timeseries.enabled:false}' == 'true'}")
```

### Hypothesis D — unknown

STOP and report BLOCKED. Do not commit a speculative fix. Re-extend diagnosis (e.g. attach a remote debugger to the running collectd container, inspect horizon's `CollectableService.setPersisterFactory` source at the exact 1.0.10 version). Return to brainstorming.

- [ ] **Step 2: Rebuild Collectd image**

```bash
touch core/daemon-boot-collectd/pom.xml
./opennms-container/delta-v/build.sh deltav 2>&1 | tail -5
```

- [ ] **Step 3: Verify the fix live**

```bash
cd opennms-container/delta-v
DELTAV_TIMESERIES_ENABLED=true docker compose --profile lite --profile metrics-e2e up -d
sleep 90
echo "=== deltav-timeseries record count ==="
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic deltav-timeseries \
  --from-beginning --max-messages 1 --timeout-ms 10000 2>&1 | tail -3
echo "=== prometheus-writer counter ==="
docker compose exec -T prometheus-writer curl -sf http://localhost:8080/actuator/prometheus 2>/dev/null | \
  grep -E '^deltav_prometheus_writer_records_consumed_total '
```

Expected: console-consumer reads at least 1 message (byte-array output, not "Processed a total of 0 messages"). `records_consumed_total > 0.0`.

If zero records, STOP — the fix did not work; re-investigate.

- [ ] **Step 4: Tear down**

```bash
docker compose --profile lite --profile metrics-e2e down -v --remove-orphans
cd /Users/david/development/src/opennms/delta-v
```

- [ ] **Step 5: Commit the fix**

Commit message for hypothesis C:

```bash
git add core/daemon-boot-collectd/src/main/java/org/deltav/collectd/timeseries/TimeseriesKafkaPublisherConfiguration.java
# also add whatever file declared the inner PersisterFactory bean
git commit -m "fix(daemon-boot-collectd): rename compositePersisterFactory bean to timeseriesPersisterFactory

Root cause: horizon CollectableService (horizon 1.0.10) resolves
PersisterFactory by qualifier name ('timeseriesPersisterFactory')
rather than by type, so @Primary on our composite bean was bypassed
and the inner factory won at the injection point. Collectd silently
ran the legacy persist path with zero Kafka publishes.

Fix: give our composite the name 'timeseriesPersisterFactory' that
horizon expects; rename the inner bean to
'innerTimeseriesPersisterFactory'. Horizon now resolves to our
composite; internally we wrap the renamed inner.

Evidence captured in project_collectd_publisher_inert_investigation
memo via the diagnostic commit."
```

Adjust the message body for hypothesis A or B if they were confirmed instead.

---

## Task 4: Write the FanoutPersisterTest skeleton

**Files:**
- Create: `core/daemon-boot-collectd/src/test/java/org/deltav/collectd/timeseries/FanoutPersisterTest.java`

- [ ] **Step 1: Write the test class with all 7 test methods (most will fail until Tasks 5-7)**

```java
/*
 * Copyright (C) 2026 BeaconStrategists, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
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
                .hasMessageContaining("inner boom");
    }

    @Test
    void fail_fast_inner_false_swallows_and_continues() {
        CollectionResource r = mock(CollectionResource.class);
        doThrow(new RuntimeException("inner boom")).when(inner).visitResource(r);
        doNothing().when(kafka).visitResource(r);

        make(false, false).visitResource(r);  // should NOT throw

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
                .hasMessageContaining("kafka boom");
    }

    @Test
    void throwable_not_just_runtime_exception() {
        CollectionResource r = mock(CollectionResource.class);
        doThrow(new LinkageError("NoSuchMethodError from horizon"))
                .when(inner).visitResource(r);
        doNothing().when(kafka).visitResource(r);

        make(false, false).visitResource(r);  // should NOT throw

        Counter c = registry.find(INNER_FAILURES).tag("step", "visitResource").counter();
        assertThat(c.count()).isEqualTo(1.0);
        verify(kafka).visitResource(r);
    }
}
```

- [ ] **Step 2: Run — expect compile failure**

```bash
./mvnw -pl core/daemon-boot-collectd test -Dtest=FanoutPersisterTest
```

Expected: compile failure — `FanoutPersister` constructor doesn't take `MeterRegistry` + two booleans yet; the `TimeseriesKafkaPersister` reference may or may not compile.

- [ ] **Step 3: Do NOT commit yet** — Task 5 adds the implementation that makes these tests pass.

---

## Task 5: Implement counter pre-registration

**Files:**
- Modify: `core/daemon-boot-collectd/src/main/java/org/deltav/collectd/timeseries/TimeseriesKafkaPublisherConfiguration.java`

- [ ] **Step 1: Add imports + pre-registration helper**

At the top of `TimeseriesKafkaPublisherConfiguration.java`, add imports:

```java
import io.micrometer.core.instrument.Counter;
```

(`MeterRegistry` is likely already imported.)

Add a private static helper just inside the `FanoutPersister` nested class, near the other fields:

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

- [ ] **Step 2: Extend FanoutPersister constructor to take MeterRegistry + fail-fast flags**

Replace the existing `FanoutPersister` constructor + fields:

```java
        // Before (current shape):
        private final Persister innerPersister;
        private final TimeseriesKafkaPersister kafkaPersister;

        FanoutPersister(Persister innerPersister, TimeseriesKafkaPersister kafkaPersister) {
            this.innerPersister = innerPersister;
            this.kafkaPersister = kafkaPersister;
        }
```

```java
        // After:
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
            for (String step : STEPS) {
                Counter.builder(INNER_FAILURES_METER).tag("step", step).register(meterRegistry);
                Counter.builder(KAFKA_FAILURES_METER).tag("step", step).register(meterRegistry);
            }
        }
```

- [ ] **Step 3: Run only the pre-registration test**

```bash
./mvnw -pl core/daemon-boot-collectd test -Dtest=FanoutPersisterTest#counters_preregistered_at_startup_with_zero_value
```

Expected: PASS. Other tests may still fail; that's Task 6's job.

- [ ] **Step 4: Do NOT commit yet** — bundle commit after Task 7.

---

## Task 6: Implement counter increments + fail-fast branches

**Files:**
- Modify: `core/daemon-boot-collectd/src/main/java/org/deltav/collectd/timeseries/TimeseriesKafkaPublisherConfiguration.java`

- [ ] **Step 1: Rewrite runInner and runKafka helpers with increment + fail-fast**

Replace the existing `runInner` and `runKafka` methods inside `FanoutPersister`:

```java
        // Before (current shape):
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

```java
        // After:
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

- [ ] **Step 2: Run the counter-increment + fail-fast tests**

```bash
./mvnw -pl core/daemon-boot-collectd test -Dtest=FanoutPersisterTest
```

Expected: all 7 tests PASS except possibly `throwable_not_just_runtime_exception` if the earlier Throwable catch was narrower. If any fail, re-read the implementation to confirm the catch is `Throwable` (not `Exception`) and the branch conditions match.

- [ ] **Step 3: Do NOT commit yet** — Task 7 updates `FanoutPersisterFactory` to pass the new args.

---

## Task 7: Wire FanoutPersisterFactory to pass MeterRegistry + fail-fast properties

**Files:**
- Modify: `core/daemon-boot-collectd/src/main/java/org/deltav/collectd/timeseries/TimeseriesKafkaPublisherConfiguration.java`

- [ ] **Step 1: Extend FanoutPersisterFactory to carry MeterRegistry + flags**

Replace the existing `FanoutPersisterFactory` nested class header + fields + constructor:

```java
        // Before:
        static final class FanoutPersisterFactory implements PersisterFactory {
            private final PersisterFactory innerFactory;
            private final TimeseriesKafkaPublisher publisher;

            FanoutPersisterFactory(PersisterFactory innerFactory, TimeseriesKafkaPublisher publisher) {
                this.innerFactory = innerFactory;
                this.publisher = publisher;
            }
```

```java
        // After:
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
```

- [ ] **Step 2: Update both createPersister overloads to pass the new args**

```java
        // Before:
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
```

```java
        // After:
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
```

- [ ] **Step 3: Update the @Bean method to inject MeterRegistry + read the fail-fast properties**

The `@Bean` method (named `timeseriesPersisterFactory` after Task 3's rename, or `compositePersisterFactory` before) gains two additional parameters:

```java
    @Bean(name = "timeseriesPersisterFactory")
    @Primary
    public PersisterFactory timeseriesPersisterFactory(
            @Qualifier("innerTimeseriesPersisterFactory") PersisterFactory innerFactory,
            TimeseriesKafkaPublisher publisher,
            MeterRegistry meterRegistry,
            @Value("${deltav.collectd.persister.inner.fail-fast:false}") boolean failFastInner,
            @Value("${deltav.collectd.persister.kafka.fail-fast:false}") boolean failFastKafka) {
        LOG.info("Creating composite timeseriesPersisterFactory wrapping inner={}@{} (failFastInner={}, failFastKafka={})",
                innerFactory.getClass().getName(),
                System.identityHashCode(innerFactory),
                failFastInner, failFastKafka);
        return new FanoutPersisterFactory(innerFactory, publisher, meterRegistry,
                failFastInner, failFastKafka);
    }
```

- [ ] **Step 4: Update the class-level javadoc to describe the new meters + properties**

Replace the existing class-level javadoc block:

```java
/**
 * Feature-flagged configuration for the Kafka Time Series producer. When
 * {@code deltav.timeseries.enabled=true}, publishes one TimeseriesBatch
 * protobuf record per CollectionSet poll to the deltav-timeseries topic.
 * When the flag is false (default), none of the beans are created and the
 * persister chain stays on the existing InMemoryStorage-backed
 * TimeseriesPersisterFactory path.
 *
 * <p>Observability (added post-Phase-2 to prevent silent-swallow regressions):
 * <ul>
 *   <li>{@code deltav.collectd.persister.inner.failures{step=<visitor-step>}}
 *       — Micrometer counter, pre-registered for all 10 visitor steps at
 *       startup so alerts on rate&gt;0 are unambiguous vs missing-metric.</li>
 *   <li>{@code deltav.collectd.persister.kafka.failures{step=<visitor-step>}}
 *       — symmetric for the Kafka side.</li>
 * </ul>
 *
 * <p>Fail-fast toggles (for CI / integration tests — default off in production):
 * <ul>
 *   <li>{@code deltav.collectd.persister.inner.fail-fast} — when true, inner
 *       Throwable propagates instead of being swallowed + logged.</li>
 *   <li>{@code deltav.collectd.persister.kafka.fail-fast} — symmetric for Kafka.</li>
 * </ul>
 *
 * <p>See the {@code project_collectd_publisher_inert_investigation} memory note
 * for the root-cause narrative that motivated these gates.
 */
```

- [ ] **Step 5: Run the full FanoutPersisterTest suite — expect 7/7 PASS**

```bash
./mvnw -pl core/daemon-boot-collectd test -Dtest=FanoutPersisterTest
```

Expected: 7/7 PASS.

- [ ] **Step 6: Commit all three Tasks 4-7 together as one Commit 3**

```bash
git add core/daemon-boot-collectd/src/main/java/org/deltav/collectd/timeseries/TimeseriesKafkaPublisherConfiguration.java \
        core/daemon-boot-collectd/src/test/java/org/deltav/collectd/timeseries/FanoutPersisterTest.java
git commit -m "feat(daemon-boot-collectd): FanoutPersister observability + dev fail-fast

(X) Two Micrometer counters — deltav.collectd.persister.{inner,kafka}.failures
tagged by visitor step — pre-registered at startup for all 10 steps so
ops can alert on rate>0 rather than missing-metric. Silent-swallow bugs
like the one that hid the Kafka publisher being inert for weeks until
Phase 2 PR #174 asserted downstream consumption cannot hide again.

(Y) Two Spring properties — deltav.collectd.persister.{inner,kafka}.fail-fast,
default false in production. When true, Throwables propagate instead of
being swallowed + logged. CI and test profiles enable these via
@TestPropertySource so integration tests fail on inner-persister
regressions instead of silently absorbing them.

Symmetric for inner and Kafka sides because the same class of wiring
bug could hit either. Seven FanoutPersisterTest unit tests cover
counter increments, pre-registration, fail-fast matrix, and the
LinkageError-catches-Throwable scar guard (Phase 0 #3)."
```

---

## Task 8: (placeholder — no-op, Task 7 Step 6 handled the Commit 3 commit)

Leaving this slot empty in the numbering so the Commit ↔ Task mapping stays clean in the commit log. Skip to Task 9.

---

## Task 9: CollectdApplicationScanIT real-main-class IT

**Files:**
- Create: `core/daemon-boot-collectd/src/test/java/org/deltav/netmgt/collectd/boot/CollectdApplicationScanIT.java`
- Possibly modify: `core/daemon-boot-collectd/pom.xml` (add `commons-io:2.18.0` test dep if not present — per memory `feedback_boot4_testcontainers_commons_io`)

- [ ] **Step 1: Check if commons-io test dep is already in collectd's pom**

```bash
grep -A2 "commons-io" core/daemon-boot-collectd/pom.xml | head -10
```

If `commons-io:2.18.0` with `<scope>test</scope>` is absent, add to `core/daemon-boot-collectd/pom.xml` inside the `<dependencies>` section (near other test-scoped deps):

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

- [ ] **Step 2: Write the IT**

Create `core/daemon-boot-collectd/src/test/java/org/deltav/netmgt/collectd/boot/CollectdApplicationScanIT.java`:

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
import org.springframework.beans.factory.annotation.Qualifier;
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
 * <p>Scar prophylactic: would have caught the silent-inert wiring bug
 * (project_collectd_publisher_inert_investigation) in one CI run.
 */
@Testcontainers
@SpringBootTest(classes = CollectdApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestPropertySource(properties = {
        "deltav.timeseries.enabled=true",
        "deltav.collectd.persister.inner.fail-fast=true",
        "deltav.collectd.persister.kafka.fail-fast=true",
        // Avoid horizon config loaders hitting real SNMP agents during boot
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
                "timeseriesPersisterFactory", PersisterFactory.class);
        assertThat(factory)
                .as("with deltav.timeseries.enabled=true, the bean named "
                        + "'timeseriesPersisterFactory' must be our FanoutPersisterFactory "
                        + "wrapping the inner (horizon CollectableService resolves by qualifier name)")
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

- [ ] **Step 3: Run the IT**

```bash
./mvnw -pl core/daemon-boot-collectd verify -Dit.test=CollectdApplicationScanIT
```

Expected: 3/3 PASS in ~30-60s (Testcontainers Kafka startup dominates).

**If test fails** with `Timed out waiting for log output matching '.*Transitioning from RECOVERY to RUNNING.*'`: the commons-io 2.18.0 dep is missing. Add it per Step 1.

**If `persister_factory_autowires_to_FanoutPersisterFactory_when_flag_true` fails** with the bean being a different class: Task 3's wiring fix didn't take effect. Re-check the `@Bean(name="timeseriesPersisterFactory")` rename + inner-bean `@Bean(name="innerTimeseriesPersisterFactory")` rename. STOP and report — the IT is doing its job.

- [ ] **Step 4: Commit**

```bash
git add core/daemon-boot-collectd/src/test/java/org/deltav/netmgt/collectd/boot/CollectdApplicationScanIT.java \
        core/daemon-boot-collectd/pom.xml
git commit -m "test(daemon-boot-collectd): CollectdApplicationScanIT real-main-class IT

Boots CollectdApplication via SpringApplication.run() against a
Testcontainers Kafka broker with deltav.timeseries.enabled=true +
fail-fast toggles on. Asserts:
  - timeseriesPersisterFactory bean resolves to FanoutPersisterFactory
    (the wiring regression this PR fixes would have been caught here)
  - deltavTimeseriesTopic NewTopic bean exists
  - TimeseriesKafkaPublisher bean exists

Phase 2 PR #174 scar-prophylactic lineage: real-main-class startup
catches @Configuration and bean-wiring bugs that unit tests +
@Import-based ITs all bypass. Mandatory commons-io:2.18.0 test dep
added per feedback_boot4_testcontainers_commons_io memory."
```

---

## Task 10: Compose default flip + proto-header substitution

**Files:**
- Modify: `opennms-container/delta-v/docker-compose.yml`
- Modify: `core/deltav-kafka-contracts/src/main/proto/deltav-timeseries.proto`
- Modify: `core/deltav-kafka-contracts/src/main/proto/deltav-node-context.proto`

- [ ] **Step 1: Flip the compose default**

Edit `opennms-container/delta-v/docker-compose.yml`. Find line 176:

```yaml
      DELTAV_TIMESERIES_ENABLED: ${DELTAV_TIMESERIES_ENABLED:-false}
```

Change to:

```yaml
      DELTAV_TIMESERIES_ENABLED: ${DELTAV_TIMESERIES_ENABLED:-true}
```

- [ ] **Step 2: Substitute the `<TBD-phase-2-PR>` placeholder in both proto headers**

```bash
grep -n "TBD-phase-2-PR" core/deltav-kafka-contracts/src/main/proto/deltav-timeseries.proto core/deltav-kafka-contracts/src/main/proto/deltav-node-context.proto
```

Expected: each file has one match in its header comment block. Edit each manually (do NOT use `sed -i` — the surrounding text differs slightly per file and a manual edit confirms context):

Locate in `core/deltav-kafka-contracts/src/main/proto/deltav-timeseries.proto`:

```protobuf
// API version: 1 (FROZEN at Phase 2 GA — delta-v#<TBD-phase-2-PR>)
```

Change to:

```protobuf
// API version: 1 (FROZEN at Phase 2 GA — delta-v#174)
```

Same substitution in `core/deltav-kafka-contracts/src/main/proto/deltav-node-context.proto`.

- [ ] **Step 3: Verify docker-compose yaml syntax + protos compile**

```bash
cd opennms-container/delta-v && docker compose --profile lite --profile metrics-e2e config >/dev/null
cd /Users/david/development/src/opennms/delta-v
./mvnw -pl core/deltav-kafka-contracts -DskipTests clean install
```

Expected: both exit 0; protobuf-maven-plugin BUILD SUCCESS.

- [ ] **Step 4: Commit**

```bash
git add opennms-container/delta-v/docker-compose.yml \
        core/deltav-kafka-contracts/src/main/proto/deltav-timeseries.proto \
        core/deltav-kafka-contracts/src/main/proto/deltav-node-context.proto
git commit -m "feat(compose+proto): enable Kafka time-series publisher by default; pin Phase 2 proto header

Compose default flipped from DELTAV_TIMESERIES_ENABLED=false to =true so
the Phase 0 producer is on by default in the delta-v stack (matches its
'DONE + E2E verified' status). Operators who need it off can still
export the env var explicitly.

Proto-header placeholder from Phase 2 resolved: <TBD-phase-2-PR> -> #174
in both deltav-timeseries.proto and deltav-node-context.proto."
```

---

## Task 11: Diagnostic cleanup (partial revert of Task 1)

**Files:**
- Modify: `core/daemon-boot-collectd/src/main/resources/application.yml`
- Modify: `core/daemon-boot-collectd/src/main/java/org/deltav/collectd/timeseries/TimeseriesKafkaPublisherConfiguration.java`

- [ ] **Step 1: Revert env + beans from actuator exposure; keep conditions**

Edit `core/daemon-boot-collectd/src/main/resources/application.yml`. Find the management block from Task 1:

```yaml
management:
  endpoints:
    web:
      exposure:
        include: health, info, prometheus, env, beans, conditions
```

Change to:

```yaml
management:
  endpoints:
    web:
      exposure:
        include: health, info, prometheus, conditions
```

(Keep `conditions` permanently — it costs nothing at runtime, exposes no secrets, and makes future wiring-regression triage one curl away.)

- [ ] **Step 2: Revert the two diagnostic LOG.info lines in TimeseriesKafkaPublisherConfiguration**

Remove the no-arg constructor that logs "TimeseriesKafkaPublisherConfiguration loaded":

```java
    // DELETE this block:
    public TimeseriesKafkaPublisherConfiguration() {
        LOG.info("TimeseriesKafkaPublisherConfiguration loaded — @ConditionalOnProperty(deltav.timeseries.enabled=true) matched");
    }
```

Leave the `private static final Logger LOG` field — other code in the class uses it (`FanoutPersister.runInner` / `runKafka` WARN logs).

The `compositePersisterFactory`/`timeseriesPersisterFactory` `@Bean` method already logs via the `LOG.info(...)` Task 7 kept in place (describing the wrapped inner factory + fail-fast settings). That line stays — it's the one piece of startup-time evidence worth keeping forever.

If after Task 7 the log line in the `@Bean` method reads exactly as Task 1 wrote it (without the `failFastInner` / `failFastKafka` args), update it to match Task 7 Step 3's version. If it already has the fail-fast info, leave as-is.

- [ ] **Step 3: Verify unit tests + IT still pass**

```bash
./mvnw -pl core/daemon-boot-collectd verify
```

Expected: all tests PASS.

- [ ] **Step 4: Commit**

```bash
git add core/daemon-boot-collectd/src/main/resources/application.yml \
        core/daemon-boot-collectd/src/main/java/org/deltav/collectd/timeseries/TimeseriesKafkaPublisherConfiguration.java
git commit -m "chore(daemon-boot-collectd): revert temporary env/beans actuator + startup log

Task 1 exposed /actuator/env + /actuator/beans + /actuator/conditions
and added two diagnostic startup logs so we could identify which
wiring hypothesis explained the silent inert publisher. Root cause is
captured in project_collectd_publisher_inert_investigation memory.

Revert env + beans exposure (noisy + expose secrets) and the
'TimeseriesKafkaPublisherConfiguration loaded' log (one-shot startup
line not worth keeping). Keep /actuator/conditions on permanently —
it incurs no cost, exposes no secrets, and makes future wiring-regression
triage one curl away. Keep the compositePersisterFactory wrap log
because it now also reports fail-fast state."
```

---

## Task 12: Full-reactor verify

- [ ] **Step 1: Run the full-reactor build**

```bash
./mvnw -B -DskipTests -fae clean install 2>&1 | tail -10
```

Expected: `BUILD SUCCESS`. Every reactor module compiles against the new `TimeseriesKafkaPublisherConfiguration` signature.

If any module fails with `cannot find symbol` referencing `FanoutPersister.FanoutPersister(Persister, TimeseriesKafkaPersister)`: the 2-arg constructor was used by test code somewhere else in the reactor. Update call sites to pass `MeterRegistry` + two booleans, or add a compatibility 2-arg constructor that defaults both booleans to `false` + uses a `NoopMeterRegistry`. (Unlikely — `FanoutPersister` is a nested class with package-private visibility scoped to `org.deltav.collectd.timeseries` — no external reactor deps.)

- [ ] **Step 2: No commit** — the full-reactor build is a verification gate, not a code change.

---

## Task 13: Full E2E acceptance gate

- [ ] **Step 1: Rebuild all delta-v images**

```bash
./opennms-container/delta-v/build.sh deltav 2>&1 | tail -5
```

Expected: opennms/collectd, opennms/prometheus-writer, and 12 other daemon images rebuilt.

- [ ] **Step 2: Run the Phase 2 E2E**

```bash
set -o pipefail
./opennms-container/delta-v/test-prometheus-writer-e2e.sh > /tmp/collectd-fix-e2e.log 2>&1
echo "E2E exit code: $?"
tail -10 /tmp/collectd-fix-e2e.log
```

Expected: exit 0, final log line `==> ALL ASSERTIONS PASSED`.

Note the `set -o pipefail` + `> log 2>&1` — `tee` was found to swallow the exit code during Phase 2 investigation.

**If any step fails**: run `grep -E "^==>|^FAIL" /tmp/collectd-fix-e2e.log` to see which step broke. If Step 5 (zero failure counters) fires with `enrichment_missing_total > 0` — Phase 1 node-context producer isn't feeding the cache. Check provisiond logs. If Step 6 VictoriaMetrics query fails — VM container didn't come up. Check `docker compose logs victoriametrics`.

- [ ] **Step 3: If a Phase 0 #2 or #3 bug surfaces (MetaTagDataLoader NPE, ResourceTypeUtils NoSuchMethodError)**

Per (P) best-effort inline:

- If `TimeseriesPersistOperationBuilder.setAttributeValue` NPE appears in collectd logs: locate the class in horizon 1.0.10's source, identify the init-order dependency, add a fix as a new commit on this branch. Add a regression test to `FanoutPersisterTest` or a targeted unit test.
- If `ResourceTypeUtils.getResourcePathWithRepository` NoSuchMethodError appears: the shim classpath has a Linkage issue. Either port the missing method to the inlined helper from Phase 0 #170's `3a89122f5b0` fix, or add an explicit dep for the horizon class that supplies it.
- If NEITHER surfaces: close them out in `project_collectd_publisher_inert_investigation` memo as "resolved or no longer reproducible during 2026-04-18 live E2E verification" without further action.

- [ ] **Step 4: No commit** unless a Phase 0 #2 or #3 fix was made in Step 3. If so, commit with a message like:

```
fix(daemon-boot-collectd): [resolve Phase 0 known-issue #N]

[Describe the trigger condition + fix]

Caught during the collectd-publisher-inert-fix E2E (PR [TBD]).
Memory project_collectd_publisher_inert_investigation updated.
```

---

## Task 14: Push + open PR

- [ ] **Step 1: Confirm the branch is clean**

```bash
git status --short
git log --oneline develop..HEAD | wc -l
```

Expected: clean (only the `provisiond-overlay/etc/imports/delta-v.xml` requisition drift, per memory `feedback_provisiond_requisition_drift` — leave unstaged). Commit count: 7 (1 spec + 6 fix commits) or 8 if a Phase 0 #2/#3 fix was added.

- [ ] **Step 2: Push**

```bash
git push -u origin fix/collectd-publisher-inert 2>&1 | tail -3
```

- [ ] **Step 3: Open the PR — NEVER use OpenNMS/opennms (memory feedback_never_pr_opennms)**

```bash
gh pr create --repo pbrane/delta-v --base develop \
    --title "fix(collectd): wire Kafka publisher + add observability/fail-fast + Phase 2 E2E gate" \
    --body "$(cat <<'BODY'
## Summary

- Fixes the Phase 0 \`TimeseriesKafkaPublisher\` / \`FanoutPersisterFactory\` wiring that was silently inert on current develop
- Ships two Micrometer counters + two Spring fail-fast properties so this class of silent swallow cannot re-occur
- Adds \`FanoutPersisterTest\` (7 unit tests) + \`CollectdApplicationScanIT\` (3 real-main-class ITs) as regression gates
- Flips the compose default \`DELTAV_TIMESERIES_ENABLED\` \`false\` → \`true\`
- Resolves Phase 2's deferred \`<TBD-phase-2-PR>\` proto-header placeholder → \`#174\`

## Root-cause narrative

See memory \`project_collectd_publisher_inert_investigation\` for the confirmed hypothesis + evidence. [Summarize the root cause in 2-3 sentences here, referencing the specific actuator + log evidence that identified it.]

## Six-commit sequence

1. \`diag(daemon-boot-collectd)\`: diagnostic actuator exposure + startup logs
2. \`fix(daemon-boot-collectd)\`: the wiring fix (see hypothesis-specific commit body)
3. \`feat(daemon-boot-collectd)\`: FanoutPersister observability + dev fail-fast
4. \`test(daemon-boot-collectd)\`: CollectdApplicationScanIT real-main-class IT
5. \`feat(compose+proto)\`: enable default + pin Phase 2 proto header
6. \`chore(daemon-boot-collectd)\`: revert temporary env/beans actuator exposure

## Acceptance verification

- [x] \`./mvnw -pl core/daemon-boot-collectd verify\` — 7 unit + 3 IT tests pass
- [x] \`./mvnw -B -DskipTests -fae clean install\` — full-reactor BUILD SUCCESS
- [x] Live producer verification: \`docker compose up -d\` + wait 90s → \`deltav-timeseries\` has records, \`prometheus-writer records_consumed_total > 0\`
- [x] \`./opennms-container/delta-v/test-prometheus-writer-e2e.sh\` — exit 0, \`ALL ASSERTIONS PASSED\` (first end-to-end pass of the full Phase 2 pipeline)
- [x] Memory \`project_collectd_publisher_inert_investigation\` updated: OPEN → DONE with confirmed hypothesis + evidence

## Spec + plan

- Spec: \`docs/superpowers/specs/2026-04-18-collectd-publisher-inert-fix-design.md\`
- Plan: \`docs/superpowers/plans/2026-04-18-collectd-publisher-inert-fix.md\`
BODY
)"
```

- [ ] **Step 4: Record the PR URL**

Output from `gh pr create` includes the PR URL. Record it.

- [ ] **Step 5: Post-merge memory updates**

(Manually, after merge — not part of this PR.)

- Mark `project_collectd_publisher_inert_investigation`: OPEN → DONE; include the confirmed hypothesis.
- Update `project_phase2_prometheus_writer_done`: add addendum "E2E now passes end-to-end post-PR [TBD]".
- Update `project_kafka_timeseries_pipeline`: confirm producer genuinely live.
- If Phase 0 #2 or #3 were fixed in Task 13 Step 3, create separate memory entries documenting resolution.

---

## Self-review (post-plan, pre-execution)

**Spec coverage check** (each spec section → plan task):

| Spec section | Plan task(s) |
|---|---|
| §1 Context and goal | Plan preamble |
| §2 Scope | Task ordering rationale + Out-of-Scope comments |
| §3 Hypothesis space | Task 3 Step 1 per-hypothesis shape |
| §4 Commit 1 (diagnostic) | Tasks 1-2 |
| §4 Commit 2 (wiring fix) | Task 3 |
| §4 Commit 3 (observability + fail-fast) | Tasks 4-7 |
| §4 Commit 4 (tests) | Tasks 4-7 (T0) + Task 9 (T1) |
| §4 Commit 5 (compose + protos) | Task 10 |
| §4 Commit 6 (cleanup) | Task 11 |
| §5 Phase 0 #2/#3 best-effort | Task 13 Step 3 |
| §6 Acceptance criteria | Tasks 12-14 |
| §7 Risks and trade-offs | Task 3 Step 1 per-hypothesis fallbacks + the "STOP" escape hatches |
| §8 References | Plan preamble "Critical memory references" |

**Placeholder scan:** one `[TBD]` in the PR body template for the PR URL (gets filled in at PR-open time) and one `[Describe the trigger condition + fix]` in the Task 13 Step 4 template commit body for a hypothetical Phase 0 #2/#3 fix. Both are legitimate fill-in-at-runtime placeholders, not plan-level TBDs.

**Type consistency:** `FanoutPersister` constructor signature — `(Persister, TimeseriesKafkaPersister, MeterRegistry, boolean, boolean)` — is consistent in Tasks 4 (test), 5 (impl add registry + pre-reg), 6 (runInner/runKafka use fields), 7 (factory passes args). `FanoutPersisterFactory` constructor — `(PersisterFactory, TimeseriesKafkaPublisher, MeterRegistry, boolean, boolean)` — consistent in Task 7 impl + `@Bean` method signature. Counter metric names `deltav.collectd.persister.inner.failures` / `deltav.collectd.persister.kafka.failures` consistent between Task 4 test constants, Task 5 impl constants, Task 9 (not referenced directly but implied by meter pre-registration assertion). Bean names `timeseriesPersisterFactory` (composite, @Primary) + `innerTimeseriesPersisterFactory` consistent in Task 3 (fix), Task 7 (impl `@Bean`), Task 9 (IT bean lookup).

---

## Execution handoff (after plan saved)

Plan complete and saved to `docs/superpowers/plans/2026-04-18-collectd-publisher-inert-fix.md`. Two execution options:

1. **Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration.
2. **Inline Execution** — Execute tasks in this session using `executing-plans`, batch execution with checkpoints.

Which approach?
