# Collectd Kafka publisher inert on develop — investigation + fix design

**Status:** approved (2026-04-18).
**Branch:** `fix/collectd-publisher-inert`.
**Target PR base:** `pbrane/delta-v develop`.
**Related:** `project_collectd_publisher_inert_investigation` (memory), `project_phase2_prometheus_writer_done` (memory), PR #174 (merged as `5ef18ed5384`).

## 1. Context and goal

The Phase 0 Kafka time-series producer (`core/daemon-boot-collectd/src/main/java/org/deltav/collectd/timeseries/TimeseriesKafkaPublisherConfiguration.java`) is silently inert on current `develop`. It was verified live at PR #170 merge time (2026-04-16) and no commits have since touched the module's Java or yaml sources, yet live Docker-compose inspection on 2026-04-18 showed:

- `records_consumed_total = 0` on the Phase 2 `prometheus-writer` after 90+ seconds of Collectd collection cycles.
- Zero records on Kafka topic `deltav-timeseries` across all partitions.
- Zero log lines mentioning `TimeseriesKafkaPublisher`, `FanoutPersister`, or `org.deltav.collectd.timeseries` in the Collectd container.
- Horizon's default `RingBufferTimeseriesWriter` is the only persister path initialised; `FanoutPersisterFactory` (which should wrap it when `deltav.timeseries.enabled=true`) never fires.

The symptom was masked for the weeks between Phase 0 and Phase 2's downstream-consumption assertion because:

- The compose default `${DELTAV_TIMESERIES_ENABLED:-false}` means any E2E that did not explicitly export the env var ran Collectd with the publisher `@ConditionalOnProperty` evaluating false.
- `FanoutPersister` catches `Throwable` and logs at WARN, so even operational failures when the flag was true would have been absorbed without operator signal.

**Goal:** unblock the delta-v Kafka time-series pipeline by identifying and fixing the wiring regression, ship observability + regression tests so this class of silent swallow cannot re-occur, flip the compose default, and close out Phase 2's deferred proto-header placeholder. The acceptance bar is the Phase 2 E2E (`opennms-container/delta-v/test-prometheus-writer-e2e.sh`) exiting 0 with `ALL ASSERTIONS PASSED` — the first successful end-to-end run of the entire pipeline.

## 2. Scope

### In scope

- Live-diagnostic exposure to identify the wiring root cause.
- Minimal surgical fix for whichever of the three hypotheses (property resolution, condition evaluation, bean-name vs `@Primary` injection) is confirmed.
- Observability for silent inner/Kafka persister failures (Micrometer counters).
- Dev-time fail-fast toggle (Spring property, default off in production).
- Regression tests: one real-main-class IT plus unit-test coverage for the new observability/fail-fast behavior.
- Phase 0 known-issues #2 (`TimeseriesPersistOperationBuilder.setAttributeValue` NPE) and #3 (`ResourceTypeUtils.getResourcePathWithRepository` `NoSuchMethodError`): best-effort inline handling if they surface during investigation.
- Compose default flip: `DELTAV_TIMESERIES_ENABLED` default `false` → `true`.
- Proto-header cleanup: replace `<TBD-phase-2-PR>` placeholder with `#174` in both `deltav-timeseries.proto` and `deltav-node-context.proto`.

### Out of scope

- Collectd-wide canonical meter-name constants class (Phase 2 introduced the pattern for the writer; extracting one for Collectd is a separable refactor).
- `build.sh check_daemon_boot_freshness` transitive-dependency gap surfaced during Phase 2 (separate tooling PR).
- Phase 3 Thresholder (next numbered phase; separate brainstorm → spec → plan cycle).
- Codebase-wide survey for other `@Configuration`-class-name vs `@Bean`-method-name collisions (Phase 2 found three; `feedback_configuration_class_bean_name_collision` captured the rule; a sweep PR is its own work).
- Horizon-side changes in `pbrane/delta-v-horizon`. If the investigation reveals the bug sits there, scope requires explicit re-approval; do not silently widen.
- Pollerd / PerspectivePollerd latency producers (Phase 3+ candidates).

## 3. Hypothesis space

Static code inspection cannot distinguish the three candidate root causes. All three are consistent with the observed symptoms; live diagnostics decide which holds.

**(A) Property not resolving from env var.** Spring's relaxed binding should translate `DELTAV_TIMESERIES_ENABLED=true` to `deltav.timeseries.enabled=true`, and `@Value("${deltav.timeseries.partitions:16}")` did resolve successfully during inspection (the 16-partition topic got created). But a property-source ordering edge case or a Spring Boot 4 relaxed-binding behavior change could still leave `@ConditionalOnProperty` reading a different effective value.

**(B) Condition evaluating false despite property resolving true.** `@ConditionalOnProperty(name="deltav.timeseries.enabled", havingValue="true")` in Boot 4 may behave differently than in Boot 3 in subtle cases (property defined as empty string, metadata tracking, etc.).

**(C) `@Primary` bypassed by qualifier/name injection in horizon.** The `compositePersisterFactory` bean has `@Primary`, which wins for type-based `@Autowired PersisterFactory`. But if horizon's `CollectableService` uses `@Qualifier("timeseriesPersisterFactory")` or similar name-based injection, `@Primary` is irrelevant and the inner factory wins. This is the strongest prior after static analysis.

**(D) Unknown.** Handled as BLOCKED; diagnosis extends rather than a speculative fix lands.

## 4. Commit sequence

Six commits, each one concern, surgical diff, reverse-chronological revert-safe.

### Commit 1: Diagnostic exposure

**Goal:** surface the runtime decisions that gate `TimeseriesKafkaPublisher` instantiation so log + actuator snapshots can confirm which hypothesis holds.

**Files:**

- `core/daemon-boot-collectd/src/main/resources/application.yml` — extend `management.endpoints.web.exposure.include` to include `env, beans, conditions`. Unconditional for this commit; Commit 6 reverts `env` + `beans` and keeps `conditions`.
- `core/daemon-boot-collectd/src/main/java/org/deltav/collectd/timeseries/TimeseriesKafkaPublisherConfiguration.java` — add two `LOG.info(...)` calls:
  1. At first-bean creation inside the `@Configuration` class (or via a dedicated `@PostConstruct`-annotated helper) — logs `"TimeseriesKafkaPublisherConfiguration loaded — @ConditionalOnProperty matched"`. Absence of this line in logs means the condition failed to match.
  2. Inside the `compositePersisterFactory` `@Bean` method — logs `"Creating compositePersisterFactory wrapping inner={fully-qualified-class}@{identity-hash}"`.

**Evidence-capture procedure:**

1. Rebuild Collectd image via `build.sh deltav` (with `touch core/daemon-boot-collectd/pom.xml` workaround to force the self-healing freshness check to rebuild — per memory `project_collectd_publisher_inert_investigation`).
2. `DELTAV_TIMESERIES_ENABLED=true docker compose --profile lite --profile metrics-e2e up -d`.
3. `docker compose logs collectd | grep -E "TimeseriesKafkaPublisherConfiguration|compositePersisterFactory"` — capture startup log evidence.
4. `docker compose exec collectd curl -sf http://localhost:8080/actuator/conditions | jq '.contexts.application.positiveMatches.TimeseriesKafkaPublisherConfiguration, .contexts.application.negativeMatches.TimeseriesKafkaPublisherConfiguration'` — capture condition decision.
5. `docker compose exec collectd curl -sf http://localhost:8080/actuator/beans | jq '.contexts.application.beans | with_entries(select(.value.type | contains("PersisterFactory")))'` — enumerate `PersisterFactory` beans and their primary status.
6. `docker compose exec collectd curl -sf http://localhost:8080/actuator/env/deltav.timeseries.enabled` — confirm property resolution.
7. Update `project_collectd_publisher_inert_investigation` memory with the specific log lines + actuator snapshots that identified the root cause.

**Exit criterion:** one hypothesis definitively confirmed (A, B, C) or a new hypothesis D identified. No fix in this commit.

### Commit 2: Wiring fix

**Goal:** minimum-diff surgical fix for the confirmed hypothesis.

**Contingent shape per hypothesis:**

- **(C) confirmed — strongest prior.** Rename the composite bean to `timeseriesPersisterFactory` (the name horizon expects). Rename the inner bean out of the way (to something like `innerTimeseriesPersisterFactory`). The exact mechanism depends on whether the inner bean is declared locally (rename in place) or imported from horizon (requires a `BeanPostProcessor` or `BeanDefinitionRegistryPostProcessor` rename). Diff ~15 LOC.
- **(A) confirmed.** Explicit property in `application.yml`: `deltav.timeseries.enabled: ${DELTAV_TIMESERIES_ENABLED:false}` as a belt-and-suspenders binding. Diff: 1 line.
- **(B) confirmed.** Replace `@ConditionalOnProperty` with `@ConditionalOnExpression("#{'${deltav.timeseries.enabled}' == 'true'}")` or a custom `@Conditional`. Diff: 1 annotation.
- **(D) unknown.** Do not commit. Report BLOCKED, extend diagnosis.

**Local verification before commit:**

1. Rebuild Collectd.
2. `DELTAV_TIMESERIES_ENABLED=true docker compose up -d` + wait 90 s.
3. Assert `kafka-console-consumer --topic deltav-timeseries` receives ≥ 1 record.
4. Assert `prometheus-writer /actuator/prometheus` shows `records_consumed_total > 0`.
5. Commit only if both pass.

### Commit 3: Observability + fail-fast toggle

**Goal:** eliminate silent-swallow invisibility with two counters, and eliminate silent-swallow in dev with a fail-fast toggle.

**File:** `core/daemon-boot-collectd/src/main/java/org/deltav/collectd/timeseries/TimeseriesKafkaPublisherConfiguration.java` (extend the `FanoutPersister` nested class).

**Observability (X):**

Two counters, tagged by visitor step (10 steps: `visitCollectionSet`, `visitResource`, `visitGroup`, `visitAttribute`, `completeAttribute`, `completeGroup`, `completeResource`, `completeCollectionSet`, `persistNumericAttribute`, `persistStringAttribute`):

- `deltav.collectd.persister.inner.failures{step=<step>}` — incremented when `runInner` catches a `Throwable`.
- `deltav.collectd.persister.kafka.failures{step=<step>}` — symmetric for `runKafka`.

Counters are **pre-registered at startup** for all 20 (2 sides × 10 steps) tag combinations so ops can alert on `rate > 0` rather than missing-metric (which would otherwise be ambiguous with "no failures occurred").

The existing per-failure WARN logs stay — they are useful during triage. The counters are the primary silent-swallow signal.

`FanoutPersister` gains a constructor-injected `MeterRegistry`. `compositePersisterFactory` `@Bean` signature gains `MeterRegistry` (auto-wired by Spring).

**Fail-fast toggle (Y):**

Two Spring properties:

- `deltav.collectd.persister.inner.fail-fast` (boolean, default `false`).
- `deltav.collectd.persister.kafka.fail-fast` (boolean, default `false`).

Read in `FanoutPersisterFactory` via `@Value("${deltav.collectd.persister.inner.fail-fast:false}")` and passed to each `FanoutPersister` instance. Behavior:

- `false` (default): current WARN-and-continue. Production unchanged.
- `true`: rethrow the caught `Throwable` wrapped in `RuntimeException` (so the horizon API contract does not need to change).

Kept symmetric and independently toggleable because CI may want to flip one without the other.

**Meter-name pinning:** Documented in `FanoutPersister` class Javadoc with a `@see` link to `project_collectd_publisher_inert_investigation` memory. A Collectd canonical-meters constants class is out of scope (see §2).

**Diff size:** ~40 LOC in `TimeseriesKafkaPublisherConfiguration.java`.

### Commit 4: Tests (T0 + T1)

**T0 — `FanoutPersisterTest.java` unit tests** (`core/daemon-boot-collectd/src/test/java/org/deltav/collectd/timeseries/FanoutPersisterTest.java`).

Uses Mockito + `SimpleMeterRegistry`. Seven test methods:

1. `counter_increments_on_inner_failure` — mock inner throws in `visitResource`, assert counter `deltav.collectd.persister.inner.failures{step=visitResource}` = 1, Kafka side still called.
2. `counter_increments_on_kafka_failure` — symmetric; inner still called (isolation preserved in default mode).
3. `counters_preregistered_at_startup_with_zero_value` — construct `FanoutPersister`, assert all 20 step-tagged counters exist with `count() == 0`.
4. `fail_fast_inner_true_rethrows` — with `failFastInner = true`, inner throws, `visitResource` rethrows.
5. `fail_fast_inner_false_swallows_and_continues` — default, inner throws, `visitResource` returns normally, Kafka still invoked. Pins PR #170's isolation baseline.
6. `fail_fast_kafka_true_rethrows` — symmetric for Kafka side.
7. `throwable_not_just_runtime_exception` — inner throws `LinkageError` (Phase 0 #3 scar), assert counter increments, Kafka still called. Guards against a future refactor that narrows to `RuntimeException` and silently re-opens Phase 0 #3.

**T1 — `CollectdApplicationScanIT.java` real-main-class IT** (`core/daemon-boot-collectd/src/test/java/org/deltav/netmgt/collectd/boot/CollectdApplicationScanIT.java`).

`@Testcontainers` + `@SpringBootTest(classes=CollectdApplication.class)` + Testcontainers Kafka + `@TestPropertySource(properties={"deltav.timeseries.enabled=true", "deltav.collectd.persister.inner.fail-fast=true", "deltav.collectd.persister.kafka.fail-fast=true"})`.

Kafka image `apache/kafka:3.8.0` (Phase 2 lineage). Adds `commons-io:2.18.0` test dep to the collectd pom if not already present (per memory `feedback_boot4_testcontainers_commons_io`).

Three test methods:

1. `persister_factory_autowires_to_FanoutPersisterFactory_when_flag_true` — `@Autowired PersisterFactory factory`; `assertThat(factory).isInstanceOf(FanoutPersisterFactory.class)`. The regression gate — would have caught Commit 2's bug in one CI run.
2. `deltav_timeseries_topic_bean_exists_when_flag_true` — `ctx.getBean("deltavTimeseriesTopic", NewTopic.class)` non-null with `topic.name().equals("deltav-timeseries")`.
3. `timeseries_kafka_publisher_bean_exists_when_flag_true` — `ctx.getBean(TimeseriesKafkaPublisher.class)` non-null.

A flag-off companion IT is explicitly skipped — Spring's `@ConditionalOnProperty` is well-tested upstream and we only care about the flag-on path.

**Test-run budget:** T0 ~0.5 s. T1 ~30–60 s (Testcontainers Kafka spin-up dominates).

### Commit 5: Compose default flip + proto headers

Three small edits, bundled because the compose flip is a prerequisite for the E2E acceptance gate and the proto-header substitution is a Phase 2 loose end.

- `opennms-container/delta-v/docker-compose.yml` line 176: `${DELTAV_TIMESERIES_ENABLED:-false}` → `${DELTAV_TIMESERIES_ENABLED:-true}`.
- `core/deltav-kafka-contracts/src/main/proto/deltav-timeseries.proto` header: `<TBD-phase-2-PR>` → `#174`.
- `core/deltav-kafka-contracts/src/main/proto/deltav-node-context.proto` header: same substitution.

### Commit 6: Diagnostic cleanup

Partial revert of Commit 1 — the diagnostic served its purpose; keep the one piece that is useful permanently.

- `core/daemon-boot-collectd/src/main/resources/application.yml`: revert `env` and `beans` from `management.endpoints.web.exposure.include`. **Keep `conditions`** permanently — invaluable for future wiring-regression triage, no secret exposure, cheap to serve.
- `TimeseriesKafkaPublisherConfiguration.java`: revert the two diagnostic `LOG.info(...)` lines from Commit 1.

## 5. Phase 0 known-issues #2 and #3: best-effort inline

Per scope decision (P) in brainstorming:

- If `TimeseriesPersistOperationBuilder.setAttributeValue` NPE surfaces during live verification (Commits 2 or 4), fix inline in this PR and add a regression test.
- If `ResourceTypeUtils.getResourcePathWithRepository` `NoSuchMethodError` surfaces, same.
- If neither surfaces, close them out in the `project_collectd_publisher_inert_investigation` memory as `resolved or no longer reproducible` without further action.
- Do not contrive trigger conditions to force them to reproduce.

## 6. Acceptance criteria

1. `project_collectd_publisher_inert_investigation` memory updated with the confirmed hypothesis + evidence. Memory status flips `OPEN` → `DONE`.
2. `./mvnw -pl core/daemon-boot-collectd verify` green — 7 new `FanoutPersisterTest` tests + 3 new `CollectdApplicationScanIT` tests all pass alongside the pre-existing suite.
3. `./mvnw -B -DskipTests -fae clean install` green (full-reactor build).
4. Live producer verification: after `docker compose up -d` (with the flipped default), `kafka-console-consumer --topic deltav-timeseries` receives ≥ 1 record within 90 s, and `prometheus-writer /actuator/prometheus` shows `records_consumed_total > 0`.
5. `./opennms-container/delta-v/test-prometheus-writer-e2e.sh` exits 0 with `ALL ASSERTIONS PASSED`. This is the first end-to-end pass of the full Phase 2 pipeline.
6. CI checks green on the PR.
7. PR opened against `pbrane/delta-v`, base `develop`. Title: `fix(collectd): wire Kafka publisher + add observability/fail-fast + Phase 2 E2E gate`.
8. Post-merge memory updates: `project_phase2_prometheus_writer_done` gets an addendum noting E2E now passes; `project_kafka_timeseries_pipeline` confirms producer genuinely live.

## 7. Risks and trade-offs

- **Flipping the compose default might break an existing operator-local E2E that assumes the publisher is off.** Low risk because the publisher is passive when Kafka is reachable (just buffers + sends) and is now properly tested. Operators who want it off can still set the env var explicitly.
- **Fail-fast mode off in production means a regression of the same class could re-mask itself.** This is mitigated by (X) counters being pre-registered and alertable. Ops who adopt the dashboard catch it regardless.
- **The `@Configuration` class-name collision pattern (Phase 2 scar) may lurk elsewhere in delta-v.** Out of scope here; follow-up sweep PR is recommended (see §2).
- **If hypothesis D (unknown) materialises, scope blows up.** Mitigated by the explicit BLOCKED escape hatch — do not commit a speculative fix; return to brainstorming.

## 8. References

- Memory: `project_collectd_publisher_inert_investigation` (OPEN at spec-authoring time; DONE at merge).
- Memory: `project_phase2_prometheus_writer_done` (DONE, merged as PR #174 / `5ef18ed5384`).
- Memory: `project_kafka_timeseries_pipeline` (Phase 0/1 done, Phase 2 awaiting this PR's E2E green).
- Memory: `feedback_configuration_class_bean_name_collision` (Phase 2 scar).
- Memory: `feedback_boot4_testcontainers_commons_io` (Phase 2 scar).
- Memory: `feedback_spring_boot_scan_package_trap` (Phase 0 #170 scar; the real-main-class IT pattern descends from here).
- Memory: `feedback_delta_v_full_reactor_verify` (build-verify hygiene).
- Prior E2E logs: `/tmp/phase2-e2e.log`, `/tmp/phase2-e2e-2.log`, `/tmp/phase2-e2e-3.log`, `/tmp/phase2-e2e-4.log`.
