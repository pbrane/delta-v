# Next-Session Prompt — prometheus-writer label coverage (Gaps A + B)

**Close the label-coverage gaps surfaced during M2 post-merge review.** Delta-V's Prometheus Remote Write consumer ships samples with correct identity (after M2/delta-v#177) but (A) omits the Prometheus-ecosystem-convention `instance` label and (B) under-consumes the rich `NodeContext` state-table (`foreign_id`, `interface_metadata`, `service_metadata`, and a default-empty top-level metadata allowlist that silently hides sysContact/sysLocation). This session adds those labels, establishes cardinality discipline, and ships one small focused PR per gap (or a single combined PR if analysis shows the test surface can share).

**The full investigation + proposed fix directions are in memory `project_prometheus_label_coverage_gaps`. Load it first — everything below is ramp-up.**

---

## Open this prompt by invoking brainstorming

This is scoped but not yet spec'd. Start with brainstorming to settle the design choices:

```
/skill superpowers:brainstorming

Kick off the prometheus-writer label coverage work. Primary memo is
project_prometheus_label_coverage_gaps. Scope: two gaps surfaced during
M2 post-merge review.

Gap A: add `instance` label (Prometheus-ecosystem convention).
Gap B: expand NodeContext usage — foreign_id + interface_metadata +
       service_metadata + less-conservative default metadata allowlist +
       cardinality-observability metric.

Branch target: new `feat/prometheus-writer-label-coverage` (or split
into two branches if the design phase decides to ship as two PRs).
PR target: pbrane/delta-v develop. NEVER OpenNMS/opennms.
```

Brainstorming should settle these design questions:

1. **`instance` source policy** — default to `node_label`, derive from `{foreign_source}:{foreign_id}`, or make it configurable from day one? What's the fallback when `node_label` is empty?
2. **One PR or two** — Gap A is 20 LOC, Gap B is 80 LOC with richer testing. Split for easier review or combine since they share the `LabelBuilder` surface?
3. **Default metadata allowlist** — agree on a safe default set (`snmp:sysContact`, `snmp:sysLocation`) or keep empty for the lowest cardinality floor?
4. **Interface-scoped metadata match key** — `NodeContext.interface_metadata` is keyed by IP address (string). Collectd's `Resource.instance` for interface-typed resources carries `ifIndex` (integer string) — NOT the IP. We need a path from `ifIndex → interface IP` to do the lookup. Options: (a) carry IP alongside ifIndex in the `TimeseriesBatch.Resource` proto (requires a contracts bump), (b) put an `ifIndex → ipAddr` map inside `NodeContext` (contracts bump), (c) lookup via the NodeContext's `interface_metadata` scanning for a matching ifIndex in the metadata map itself (hacky but no contracts change). Pick one, with a clear reason.
5. **Cardinality metric shape** — a `histogram` of labels-per-sample, a `counter` tracking total unique series, or a `gauge` snapshot refreshed on a schedule?

From the spec, `superpowers:writing-plans` produces the plan; from the plan, `superpowers:subagent-driven-development` executes.

---

## Preflight checks before invoking the skill

```bash
git branch --show-current       # expect: develop (fresh feature branch will be created by brainstorming)
git status --short              # expect: clean
git log --oneline -5            # expect: 78eca97f389 fix(collectd): ... (#177) within the last few commits
./mvnw -version                 # Maven 3.9.14 + Java 21
```

---

## Memory to load first

Before brainstorming:

- **`project_prometheus_label_coverage_gaps`** — PRIMARY. Both gaps, proposed fix shapes, cardinality-discipline requirements.
- **`project_phase2_prometheus_writer_done`** — Phase 2 shipping context. Explains why the current label set is what it is. (delta-v#174)
- **`project_collectd_serviceparameters_identity_gap`** — M2 context. The identity fix that made the existing labels correct in the first place. (delta-v#177)
- **`feedback_never_pr_opennms`** — `--repo pbrane/delta-v`.
- **`feedback_feature_branches`** — never commit to develop.
- **`feedback_deltav_package_namespace`** — `org.deltav.*` + BeaconStrategists copyright.
- **`feedback_delta_v_full_reactor_verify`** — full-reactor build before push.

---

## Guardrails for this work

1. **Contract change discipline.** `deltav-node-context.proto` is API v1 FROZEN at Phase 2 GA. If Gap B design-phase analysis (question 4 in the brainstorm list) concludes we need a contract bump to carry `ifIndex → ipAddr` mapping cleanly, that's a v2 bump requiring a one-release deprecation cycle. Don't do an unversioned field addition to v1 even if it's "technically forward-compatible" — the freeze is deliberate. Prefer a non-contract-change solution for this PR; defer the v2 bump to its own work item.
2. **Cardinality is a feature, not a bug.** Every new label is a potential cardinality multiplier. The cardinality-observability metric (brainstorm question 5) is MANDATORY before any default allowlist expansion. Don't ship Gap B without it.
3. **Existing `node_id` stays.** Even after adding `instance`, keep `node_id` as the stable integer joining key for downstream correlators and event-based alerting. No breaking label change.
4. **Grafana-friendly defaults.** Any default label we add should be immediately usable in `{{$labels.xxx}}` Grafana templates without additional config. Test: "a brand-new Delta-V operator opens Grafana with prometheus-writer's output and can build a device-picker dashboard in 10 minutes with zero label renaming."

---

## Investigation starting points (copy from `project_prometheus_label_coverage_gaps`)

The hot files:

1. `core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/translate/LabelBuilder.java` — the 46-line class at the heart of both gaps.
2. `core/prometheus-writer/src/main/java/org/deltav/prometheus/writer/config/PrometheusWriterProperties.java` — contains `Labels(List<String> fromMetadata)`; needs expansion.
3. `core/prometheus-writer/src/main/resources/application.yml` — documents the `labels.from-metadata: []` default; needs new properties + docs.
4. `core/deltav-kafka-contracts/src/main/proto/deltav-node-context.proto` — the contract for what's in the state table. Do NOT modify without a v2 bump decision.
5. Tests to parallel: `LabelBuilderTest.java`, `TimeseriesToPromTranslatorTest.java`, `WriteRequestBuilderTest.java`, `RwRoundTripIT.java` (end-to-end with a real VictoriaMetrics testcontainer).

---

## Success criteria

- [ ] `instance` label emitted by default, sourced from `node_label` with a configurable override.
- [ ] `foreign_id` added as a standard label (no configuration needed).
- [ ] Interface-scoped metadata allowlist configurable separately from top-level metadata; at least one standard interface metadata key (`snmp:ifAlias` or `snmp:ifDescr`) wired to demonstrate the path works end-to-end.
- [ ] A bounded, sensible default for `prometheus-writer.labels.from-metadata` — not `[]`, but specifically the low-cardinality defaults agreed in brainstorming (likely `[snmp:sysContact, snmp:sysLocation]`).
- [ ] `deltav_prometheus_writer_labels_per_sample` (or whatever shape is picked in brainstorm) emitted at `/actuator/prometheus` and documented.
- [ ] Unit tests for every new label path.
- [ ] IT (or extension of `RwRoundTripIT`) validating end-to-end that `ifAlias` from a mocked NodeContext lands as an `ifAlias` label on the wire to VictoriaMetrics.
- [ ] `./mvnw -pl core/prometheus-writer verify` green.
- [ ] `./mvnw -B -DskipTests -fae clean install` full-reactor green.
- [ ] PR opened `--repo pbrane/delta-v --base develop`. Title starts `feat(prometheus-writer):` per the adjacent convention.
- [ ] Post-merge: flip `project_prometheus_label_coverage_gaps` memo status OPEN → RESOLVED (delta-v#<PR>) with list of new labels shipped.

---

## After this PR ships

Out of scope for this session but queued next:

- **Contract v2** if Gap B design surfaces the need for an `ifIndex → ipAddr` or richer resource-to-interface mapping in `TimeseriesBatch` or `NodeContext`. Separate spec + horizon-like contract-freeze discipline.
- **Grafana dashboard bundle** — a committed JSON dashboard in `opennms-container/delta-v/grafana-dashboards/` that exercises the new label set, making the "Grafana-friendly defaults" criterion reproducible.
- **VictoriaMetrics tenant separation** — if the added labels push cardinality near VictoriaMetrics's default limits, consider per-location tenant sharding. Tracked separately.
