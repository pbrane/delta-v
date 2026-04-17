# Next-Session Prompt — Kafka TS Phase 2: Prometheus RW Consumer Implementation

**Kick off implementation of the Phase 2 Prometheus Remote Write consumer** using the approved design + task-by-task plan committed on `feature/kafka-ts-phase-2-prometheus-consumer`.

Phase 0 (Collectd producer, delta-v#169/#170) and Phase 1 (Provisiond node-context producer, delta-v#171) are both live on `develop`. Phase 2's design was brainstormed and the spec shipped in **delta-v#172** (prompt file) and the current feature branch (spec + plan commits). Nothing is built yet — this is the execution session.

---

## Open this prompt by invoking subagent-driven-development

```
/skill superpowers:subagent-driven-development

Execute the Phase 2 Prometheus Remote Write consumer implementation plan
task-by-task on the current feature branch.

Plan (26 tasks across two files):
- docs/superpowers/plans/2026-04-17-kafka-ts-phase-2-prometheus-consumer.md      (Tasks 1-5)
- docs/superpowers/plans/2026-04-17-kafka-ts-phase-2-prometheus-consumer-part2.md (Tasks 6-26)

Design spec (the source of truth; every decision is pinned here):
- docs/superpowers/specs/2026-04-17-kafka-ts-phase-2-prometheus-consumer-design.md

Feature branch: feature/kafka-ts-phase-2-prometheus-consumer
Base for PR: pbrane/delta-v develop (NEVER OpenNMS/opennms — memory feedback_never_pr_opennms)
```

---

## Preflight checks before invoking the skill

Run these first — they are quick and they catch stale-state bugs that otherwise fail late:

```bash
git branch --show-current       # expect: feature/kafka-ts-phase-2-prometheus-consumer
git status --short              # expect: clean (the delta-v.xml requisition drift is expected;
                                #         discard if present via git checkout --)
git log --oneline -3            # expect: latest commit is the Phase 2 spec (a0936466b67)
docker ps | grep -i kafka       # expect: no leftover Kafka from a previous session
```

Then:

```bash
./mvnw -version                  # expect: Maven 3.9.x, Java 21
docker --version                 # expect: Docker running (Testcontainers + E2E need it)
```

---

## Memory to load first

These memories carry load-bearing context not in the codebase. Read them before starting Task 1:

- `project_kafka_timeseries_pipeline` — overall pipeline map (Phase 0/1 done, Phase 2 in progress)
- `project_node_context_phase1_done` — Phase 1 shipping details + the 3 runtime bugs E2E caught that unit tests missed
- `project_kafka_timeseries_producer_next_session` — Phase 0 shipping details + #169→#170 cascade
- `feedback_spring_boot_scan_package_trap` — why Task 19 (real-main-class IT) is non-negotiable
- `feedback_spring_cloud_stream_splitter` — why Task 17 sets `use-native-encoding: true` on the DLQ binding
- `feedback_delta_v_full_reactor_verify` — why Task 25 runs the full-reactor build before push
- `feedback_deltav_package_namespace` — `org.deltav.*` + BeaconStrategists copyright
- `feedback_delta_v_uses_mvnw_not_compile_pl` — all builds use `./mvnw`
- `feedback_never_pr_opennms` — PR goes to `pbrane/delta-v`, never `OpenNMS/opennms`
- `feedback_rebuild_all_daemons` — `./build.sh deltav` expects all 13 daemon-boot jars + flow-enricher + prometheus-writer current

---

## Guardrails for this implementation session

1. **Task ordering is dependency-driven; do not reorder.** Task 5 (`NodeContextKafkaBootstrap`) depends on Task 4 (`NodeContextCache`). Task 15 (`TimeseriesConsumer`) depends on Tasks 4, 10, 13. Task 19 (real-main-class IT) must come after everything it asserts is wired. Task 26 (PR) is last.
2. **Every task ends with a commit.** Don't bundle commits across tasks — the plan's commit messages are pre-written; use them verbatim (with minor adjustments if a task step deviates).
3. **TDD order within each task:** failing test → run-fail → implement → run-pass → commit. The plan's Step numbering reinforces this. Don't short-circuit.
4. **Scar prophylactics** are not "nice to have" — they are tasks or explicit acceptance criteria. Task 3 (yaml parity), Task 17 (`use-native-encoding: true`), Task 19 (real-main-class IT), Task 24 (pipefail-safe grep), Task 25 (full-reactor verify), Task 26 (final verify-all checklist). Phase 0 #170 cost 4 post-merge commits because one of these (scan-package) was skipped at planning time — don't repeat that.
5. **Before each subagent dispatch:** re-read the task's "Files" list and the TDD steps. Subagents need explicit file paths and test class names.
6. **Between tasks, human/lead review:** run any new tests + `./mvnw -pl core/prometheus-writer test` to confirm nothing regressed. Full-reactor build at Task 25 — not sooner — because a full reactor build is ~3-5 minutes and we don't need to pay it every task.
7. **Do NOT touch `develop`.** Stay on `feature/kafka-ts-phase-2-prometheus-consumer`. All commits + push + PR open against this branch.
8. **If an unexpected issue surfaces** (e.g. a horizon classpath gap, Spring Boot 4 incompatibility), STOP and document in a memory note before patching. Phase 0/1 established this pattern — silent runtime surprises are the #1 cost driver.

---

## Task-level plan summary (for context — the plan is canonical)

1. Vendor Prometheus `remote.proto` + `types.proto` into `core/deltav-kafka-contracts`
2. Create `core/prometheus-writer/` module skeleton + root-pom inclusion + main class
3. `PrometheusWriterProperties` + `application.yml` (yaml-parity prophylactic)
4. `NodeContextCache` POJO + unit tests
5. `NodeContextKafkaBootstrap` + Testcontainers IT
6. `NodeContextCacheHealthIndicator` + unit test
7. Startup gate (pause binding + resume on cache-ready event)
8. `NameSanitizer` + unit tests
9. `LabelBuilder` + unit tests
10. `PromSample` + `TimeseriesToPromTranslator` + unit tests
11. `WriteRequestBuilder` + `RemoteWriteHttpClient` + MockWebServer tests
12. `RemoteWriteRetryPolicy` + unit tests
13. `BatchingRwWriter` + unit tests
14. `PrometheusWriterCircuitBreaker` (Resilience4j) + unit tests
15. `TimeseriesConsumer` SCS function + SCS test-binder IT
16. `ConsumerPauseListener` + unit tests
17. `DlqPublisher` + DLQ `NewTopic` bean + SCS test-binder IT (`use-native-encoding` prophylactic)
18. `PrometheusWriterMetrics` central declaration + meter-name pin test
19. `PrometheusWriterConfiguration` wiring + `PrometheusWriterApplicationScanIT` real-main-class (scan-package prophylactic)
20. Full-stack round-trip Testcontainers IT (`RwRoundTripIT` + `CircuitBreakerIT`)
21. `Dockerfile.prometheus-writer`
22. `docker-compose.yml` additions (writer service + VictoriaMetrics v1.106.1 pinned)
23. `build.sh` — `do_prometheus_writer_image()`
24. `test-prometheus-writer-e2e.sh` 6-step (pipefail-safe prophylactic)
25. Freeze both proto headers + README update + full-reactor verify gate
26. Final acceptance checklist + PR open against `pbrane/delta-v` `develop`

---

## Success criteria

The session is DONE when:

- [ ] All 26 tasks committed on `feature/kafka-ts-phase-2-prometheus-consumer`
- [ ] `./mvnw -pl core/prometheus-writer verify` all green (unit + ITs + real-main-class IT)
- [ ] `./mvnw clean install -DskipTests -fae` (full reactor) all green
- [ ] `./opennms-container/delta-v/build.sh` produces `opennms/prometheus-writer:<version>` alongside the 13 daemon-boot images + flow-enricher
- [ ] `./opennms-container/delta-v/test-prometheus-writer-e2e.sh` exits 0 with `ALL ASSERTIONS PASSED`
- [ ] PR opened against `pbrane/delta-v` base `develop`, title starts `feat: Kafka TS Phase 2 —`
- [ ] PR description includes the rebuild-checklist + scar-prophylactic verification sections
- [ ] Memory updated: `project_kafka_timeseries_pipeline` (Phase 2 IN PROGRESS → awaiting merge); once merged, add a `project_phase2_prometheus_writer_done` note capturing any runtime bugs that only surfaced in E2E (the Phase 0/1 tradition)

---

## After Phase 2 ships

Out of scope for this session but good to queue:

- Extract `NodeContextCache` as `core/deltav-node-context-cache` starter when consumer #2 (Thresholder) lands (YAGNI'd in Phase 2)
- `staleness-emitter` dedicated consumer if operators ask for immediate series cleanup on node delete
- Phase 3: Thresholder (spec spawns from `project_thresholder_brainstorm`)
- Phase 3+: Pollerd + PerspectivePollerd latency producers to `deltav-timeseries`
