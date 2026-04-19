# Next-Session Prompt — M2: Collectd `ServiceParameters` identity-populate gap

**Kick off investigation + fix for the Phase 2 E2E remaining blocker.** PR #175 (fix/collectd-publisher-inert) shipped the consumer-side fixes (NodeContextKafkaBootstrap race, SCS-binder topic-partition race, location-empty cache-key fallback). After merge, `records_consumed_total > 0` but `samples_sent_total` stays 0 because Collectd publishes `TimeseriesBatch` records with `nodeId=0 + location=""`. That's a producer-side gap: Collectd's collection-scheduling path doesn't populate `ServiceParameters.{nodeId,location}`. M2 fixes the producer so Phase 2 E2E fully passes green for the first time.

**The full investigation map, fix direction, and smoke test are in memory `project_collectd_serviceparameters_identity_gap`. Load it first — everything below is ramp-up around that memo.**

---

## Open this prompt by invoking brainstorming

This is a fresh phase of work (spec + plan do not yet exist for M2). Start with brainstorming:

```
/skill superpowers:brainstorming

Kick off the M2 investigation + fix for the Phase 2 E2E blocker. Primary
memo with the full plan is project_collectd_serviceparameters_identity_gap.

Branch target for M2 work: new `fix/collectd-serviceparameters-identity`
(do NOT reuse fix/collectd-publisher-inert — that's merged + deleted).
PR target: pbrane/delta-v develop. NEVER OpenNMS/opennms.
```

The brainstorming session will read the memo, ask clarifying questions (scope: just-the-fix vs. fix + fail-fast regression guard; investigation depth; test strategy), and produce a spec. From the spec, `superpowers:writing-plans` produces the implementation plan; from the plan, `superpowers:subagent-driven-development` executes.

---

## Preflight checks before invoking the skill

```bash
git branch --show-current       # expect: develop (or a freshly created feature branch)
git status --short              # expect: clean (discard provisiond requisition drift if present — memory feedback_provisiond_requisition_drift)
git log --oneline -3            # expect: 8a674fadd3a feat: fix(prometheus-writer)... (#175) at the top
docker ps | grep -iE "delta-v" # expect: no leftover delta-v stack from PR #175 E2E attempts
./mvnw -version                 # expect: Maven 3.9.x, Java 21
docker --version                # expect: Docker Desktop running
```

---

## Memory to load first

Read these before starting the brainstorm — they carry load-bearing context the codebase alone doesn't:

- **`project_collectd_serviceparameters_identity_gap`** — PRIMARY. Root cause narrative, fix-direction options, testing strategy, smoke test (`enrichment_lookup_fallback_total` should stay at 0 after the fix).
- **`project_collectd_publisher_inert_investigation`** — RESOLVED (delta-v#175). Full evidence trail of why the consumer-side hypothesis was wrong. Context for why this M2 scope is producer-side.
- **`project_phase2_prometheus_writer_done`** — the consumer side this M2 completes. What's already wired; what's the acceptance bar.
- **`project_kafka_timeseries_producer_next_session`** — Phase 0 publisher history. Has the original note about the location/nodeId gap being a known followup.
- **`project_collectd_scheduler_publisher_split`** — Phase 0 scope note that originally flagged this gap as future work.
- **`project_phase0_inner_persister_bugs_followup`** — 3 related horizon-side bugs that might surface during M2 investigation. Out of scope but worth memory-noting any new reproductions.
- **`feedback_never_pr_opennms`** — delta-v PRs always `--repo pbrane/delta-v`.
- **`feedback_feature_branches`** — never commit directly to develop.
- **`feedback_delta_v_uses_mvnw_not_compile_pl`** — all builds use `./mvnw`.
- **`feedback_delta_v_full_reactor_verify`** — full-reactor build before push.
- **`feedback_deltav_package_namespace`** — new code under `org.deltav.*` with BeaconStrategists copyright.
- **`feedback_kafka_topic_partition_race`** — just-shipped rule; apply if M2 touches any Kafka topic declarations.

---

## Guardrails for this work

1. **Producer-side scope only.** M2 fixes Collectd's `ServiceParameters` identity population. Do NOT touch `core/prometheus-writer/` — that side is done in PR #175. If the investigation suggests a consumer-side fix is better than the producer-side one, STOP and re-brainstorm — don't silently widen scope.
2. **Fix at the right layer.** The `parseIntOrZero(null) → 0` silent-default in `TimeseriesKafkaPersister` is a code smell. The brainstorm should cover whether to (a) fix `ServiceParameters` population upstream AND (b) also make the publisher fail-fast on missing identity. Arguably both ship — the fail-fast guard prevents silent regression if `ServiceParameters` population breaks again.
3. **Phase 0 inner bugs (#1, #2, #4) stay out of scope.** They're memory-noted in `project_phase0_inner_persister_bugs_followup`; surface counts via delta-v#175's counters. Each deserves its own focused investigation. Do not fold any into M2 unless one directly blocks M2's verification.
4. **Acceptance is the Phase 2 E2E**. `./opennms-container/delta-v/test-prometheus-writer-e2e.sh` must exit 0 with `ALL ASSERTIONS PASSED`. The `enrichment_lookup_fallback_total` counter from delta-v#175 should stay at 0 throughout a successful E2E — that's the smoke test for "Collectd is now populating `location` correctly."
5. **Env quirk from PR #175**: the Bash tool's strict-mode wrapping kills `build.sh` with 255 and empty log. Workaround: `bash -c '<cmd>' &` + `disown`. See session 7d5b9c9e-… memory trail if you hit it again.

---

## Investigation starting points (copy from `project_collectd_serviceparameters_identity_gap`)

Where is `ServiceParameters` constructed in Delta-V Collectd's collection path?

1. `core/daemon-boot-collectd/src/main/java/org/deltav/netmgt/collectd/boot/CollectdDaemonConfiguration.java` — the `Collectd` + `serviceCollectorRegistry` bean wiring.
2. Horizon's `CollectableService.run()` → `doCollection()` constructs `ServiceParameters` per cycle. Source under `.claude/worktrees/provisiond-spring-boot/` if you need to read horizon 1.0.10 source directly.
3. Provisiond's published `OnmsMonitoredService` records — Collectd looks these up by service+IP for the collection poll; the result likely has `nodeId`+`location` but isn't being threaded into the published `ServiceParameters`.

The lookup happens (Collectd knows which agent to poll); the result isn't being propagated to the `ServiceParameters` map the persister reads. Most likely fix: decorate or re-wire so `OnmsMonitoredService.getNodeId()` + `.getLocation()` make it into `ServiceParameters` before `TimeseriesKafkaPersister.visitCollectionSet(...)` reads them.

---

## Success criteria for M2

The session is DONE when:

- [ ] `ServiceParameters.nodeId` and `ServiceParameters.location` are populated correctly at the point where `TimeseriesKafkaPersister` reads them.
- [ ] `parseIntOrZero` silent-default removed or fail-fast-guarded (optional, scope-dependent).
- [ ] New unit test(s) covering the populated-ServiceParameters contract in `core/daemon-boot-collectd`.
- [ ] `./mvnw -pl core/daemon-boot-collectd verify` green.
- [ ] `./mvnw -B -DskipTests -fae clean install` full-reactor green.
- [ ] Live verification: Collectd publishes `TimeseriesBatch` with real nodeId + location; `kafka-console-consumer` shows keys like `Default@5` instead of `@0`.
- [ ] **`./opennms-container/delta-v/test-prometheus-writer-e2e.sh` exits 0 with `ALL ASSERTIONS PASSED`** — first successful full Phase 2 E2E.
- [ ] `enrichment_lookup_fallback_total` stays at 0 throughout the E2E run (smoke test).
- [ ] PR opened `--repo pbrane/delta-v --base develop`. Title starts `fix(collectd):` or `feat(collectd):` per the conventional-commit style used by prior PRs.
- [ ] Post-merge: flip `project_collectd_serviceparameters_identity_gap` memory status `OPEN` → `RESOLVED (delta-v#<PR>)`. Add addendum to `project_phase2_prometheus_writer_done` noting "E2E now passes end-to-end as of delta-v#<PR>."

---

## After M2 ships

Out of scope for this session but queued next:

- **Phase 3 Thresholder** — the next numbered phase. Spec spawns from `project_thresholder_brainstorm`. Consumer of `deltav-timeseries`; evaluates thresholds (including stddev) and emits fault events. Replaces inline horizon `ThresholdingVisitor`. Likely extracts `NodeContextCache` as a `core/deltav-node-context-cache` starter (YAGNI'd in Phase 2).
- Phase 0 horizon-inner-persister bugs (#1, #2, #4) — dedicated investigation PRs per `project_phase0_inner_persister_bugs_followup`.
- `build.sh check_daemon_boot_freshness` transitive-deps gap — small tooling PR.
- NodeContext proto schema freeze (post-first-consumer-shipped, per Phase 1 note).
- Collectd-wide canonical meter-names constants class (apply Phase 2's `PrometheusWriterMetrics` pattern to Collectd's new (X) counters from delta-v#175).
