# Alarmd: Kafka Source of Truth + Consumer Ecosystem (Northbounder Retirement)

> **Status:** Design / roadmap. No implementation started. Original scope captured 2026-05-14 during the Spring 7 `Assert.notNull` sweep (horizon PR #17). Refined 2026-05-14 with anti-pattern framing and the Prometheus-alerts-first-class direction. Refined again 2026-05-14 with the explicit Kafka-as-source-of-truth commitment and the ALEC/OIA consumer ecosystem framing.

## Goal

Three intertwined commitments:

1. **Retire the alarmd `Northbounder` API entirely.** The in-process callback model is an anti-pattern under delta-v's microservice / data-pipeline architecture: it puts six destination integrations inside one daemon, couples their lifecycle to alarmd's, and makes every destination a code-loaded plugin instead of an independently deployable consumer. We're not porting it — we're replacing it.

2. **Make Kafka the source of truth for alarm state, not PostgreSQL.** Alarmd today writes alarm lifecycle to a PG `alarms` table. That table is a write target with concurrent ack/clear contention, can't be replayed, and forces every consumer (UI, REST, correlators, third-party integrations) into the same query path. Going Kafka-as-source-of-truth — with PG kept as a materialized current-state view managed by a consumer — gets us event sourcing, replay, multi-consumer fan-out, and an honest decoupling of "what happened" from "what's the current snapshot." See "Phased commitment" below.

3. **Open the alarm consumer ecosystem.** Once alarms are on Kafka with a published proto schema, every existing OpenNMS Integration API implementation (ALEC, situation-feedback, NCS path correlator, third-party plugins) becomes a candidate for clean reimplementation as a standalone Kafka consumer. The old OIA + OSGi service-registry abstraction goes away in delta-v. Prometheus alerts is the first new consumer; ALEC-on-Kafka is the headliner.

After this work:

- `alarmd` publishes alarm lifecycle events to a Kafka topic. Zero in-process northbounders.
- A materializer consumer maintains the PG view (Phase 2) — alarmd no longer writes PG directly.
- Each destination (Prometheus alerts, Email, HTTP, etc.) is a **new** standalone Spring Boot Kafka consumer — built fresh against the published topic, not by extracting code from horizon's `opennms-alarms/*-northbounder/` modules. Those modules are *reference material* for behavior (severity filters, alarm-shape mapping) but not sources to port.
- ALEC and other OIA consumers are reimplementable as standalone Kafka services. We don't have to do that work to ship this plan — but we don't block it either; the topic is the integration point.
- The horizon `Northbounder` interface, `AbstractNorthbounder`, `NorthboundAlarm`, and the entire `opennms-alarms/api` module are removed from delta-v's reactor. (Horizon upstream retains them — that's not our concern.)

This is the broader delta-v thesis applied: every daemon does one thing, talks to others only over Kafka, never directly imports another daemon's classes. The northbounder API violated all three; the PG-as-source-of-truth model violated the second.

## Why now

Five reasons, in priority order:

1. **The northbounder API is fundamentally an anti-pattern in delta-v.** Delta-v's design is microservices over Kafka. The horizon `Northbounder` interface is the opposite: an in-process callback registered with alarmd's dispatcher, called synchronously per alarm, with the destination's I/O happening on alarmd's threads. A failure in any destination (slow SMTP, blocked SNMP, Drools rule storm) can wedge alarmd. The fix isn't a better in-process API — it's removing the API.
2. **Prometheus alerts must be first-class.** Users have started asking for it (OpenNMS issue tracker, Mike at OpenNMS-User-Group 2026-04). Adding it as a *seventh in-process northbounder* would entrench the anti-pattern. Building it as the *first standalone consumer* sets the template for everything else — and pulls forward the platform-level decision about which alert backend we standardize on (Prometheus Alertmanager vs. VictoriaMetrics vmalert; see Open Questions).
3. **The horizon assets are already substantial.** Horizon ships `OpennmsKafkaProducer` (≥650 LOC, `AlarmLifecycleListener`-driven Kafka publishing), `KafkaAlarmDataSync` (Kafka-Streams materialization with `GlobalKTable`, `AlarmEqualityChecker`-based idempotent updates, queryable state stores), and `OpennmsModelProtos` (a defined alarm protobuf schema). This is real production code, not a POC. The publisher work isn't greenfield — it's a *targeted port* of working horizon code into a delta-v Spring Boot module. See "Reuse from horizon" below.
4. **Alarmd state management redesign is open** (memory `project_alarmd_state_management_strategy_open`). The Drools-in-alarmd approach was abandoned in PR #259 because Boot 4 wiring was a nightmare. Whatever replaces it will be much cleaner if alarmd doesn't *also* own six destination integrations *and* doesn't directly own the alarm storage.
5. **The Spring 7 `Assert.notNull` audit** (horizon PR #17, 2026-05-14) found 12 single-arg call sites in the northbounder managers. They're a footgun, but fixing them inside alarmd's process is the wrong layer — they vanish when the managers disappear.

## Reuse from horizon

The publisher work item *is not greenfield.* Horizon has solved most of the producer problem already, in code we can lift selectively. Survey:

| Horizon asset | Path | What it gives us |
|---|---|---|
| `OpennmsModelProtos` | `features/kafka/producer/src/main/java/org/opennms/features/kafka/producer/model/` | Defined protobuf schema for `Alarm` (and `Event`, `Topology`). Starting point for `deltav-alarms-state-change` wire format. Trim or extend per delta-v needs — do not reinvent. |
| `OpennmsKafkaProducer` | `features/kafka/producer/.../OpennmsKafkaProducer.java` | Reference for the production publisher: batching, error handling, callback-state-tracking, rate-limited logging, `AlarmLifecycleListener` integration. Don't lift wholesale (conflates alarms+events+feedback+topology), but the alarm-publishing slice is a working blueprint. |
| `AlarmLifecycleListener` | `opennms-alarms/api/.../AlarmLifecycleListener.java` (interface) | Alarmd's existing extension point for "tell me when alarm state changes." The delta-v publisher implements this. |
| `AlarmCallbackStateTracker` | horizon alarmd-api | Tracks which lifecycle callbacks have completed. Reusable for the publisher to know when alarms are safely emitted. |
| `KafkaAlarmDataSync` | `features/kafka/producer/.../datasync/KafkaAlarmDataSync.java` | Real Kafka-Streams materializer: `GlobalKTable` of current state, `AlarmEqualityChecker`-based idempotent updates, queryable state stores. Direct reference for Phase 2 PG-view-from-Kafka materializer. |
| `AlarmEqualityChecker` | same package | Field-comparison idempotency — exactly what the materializer needs to dedupe re-deliveries. Likely liftable nearly verbatim. |
| OIA documentation | `docs/modules/development/pages/oia/` | Reference for what ALEC and other integrations expect at the consumption point. Helps shape the proto so consumer reimplementations are mechanical. |

**What's NOT reusable:**
- The OSGi / Karaf wiring around `OpennmsKafkaProducer` — Karaf is dead in delta-v.
- The OIA dependency module (`dependencies/oia`) itself — it's a Karaf-era abstraction (OSGi service registry). Delta-v consumers go straight to Kafka.
- Horizon's listener registration via Spring DM / OSGi `ServiceTracker` — delta-v alarmd uses Spring Boot bean wiring.
- The conflation of alarms + events + topology + feedback in one producer class — delta-v should split these into separate consumers/topics by concern.

## Inventory (current state in horizon)

Located at `/Users/david/development/src/opennms/delta-v-horizon/opennms-alarms/`:

| Module | What it does | Active in delta-v today? |
| --- | --- | --- |
| `api/` | `Northbounder` interface, `NorthboundAlarm`, `AbstractNorthbounder` | Yes (referenced by `daemon/`) |
| `daemon/` | Alarmd core — schedules / dispatches alarms to registered northbounders | Yes |
| `syslog-northbounder/` | Forward alarms as syslog messages | Compiled in, but not configured at runtime in any delta-v env we run |
| `bsf-northbounder/` | Beanshell / scripting handler | Probably never used outside Meridian |
| `snmptrap-northbounder/` | Forward alarms as SNMP traps to an upstream NMS | Used by Meridian customers; unclear in delta-v |
| `drools-northbounder/` | Drools rules engine destination | Was the basis for the abandoned alarmd-Drools approach |
| `email-northbounder/` | Forward alarms as email | Compiled in |
| `jms-northbounder/` | Forward alarms to a JMS broker | Compiled in |
| `http-northbounder/` | Forward alarms over HTTP | Compiled in (not in the Spring 7 audit count above) |

**`Northbounder` interface** (`api/.../Northbounder.java`):

```java
public interface Northbounder {
    void start() throws NorthbounderException;
    boolean isReady();
    void onAlarm(NorthboundAlarm alarm) throws NorthbounderException;
    void stop() throws NorthbounderException;
    String getName();
    void reloadConfig() throws NorthbounderException;
}
```

`onAlarm` is the hot path — in-process method call per alarm. The other methods are lifecycle.

## Target architecture

### Producer side (alarmd)

Alarmd publishes to a Kafka topic — name proposal `deltav-alarms-state-change` (matches the `deltav-*` naming we've used since the Sink topic rework). Schema starts from horizon's `OpennmsModelProtos.Alarm` (see "Reuse from horizon" above), trimmed/extended for delta-v. Compacted topic, keyed by `reduction_key` (or `{location}@{reduction_key}` if location should partition; decide at implementation time). Tombstones on delete.

The publisher itself hooks into alarmd's existing `AlarmLifecycleListener` extension point. Horizon's `OpennmsKafkaProducer` is the working reference for batching, error handling, and callback-state tracking; lift the alarm-publishing slice into a clean delta-v module (`core/alarms-kafka-publisher/` or similar).

What alarmd loses:
- `opennms-alarms/api` dependency (after Phase 4)
- `org.opennms.netmgt.alarmd.northbounder.*` registration and dispatch code
- Six in-process `Manager` beans (Syslog, BSF, SNMPTrap, Drools, Email, JMS) that today register with `Alarmd.registerNorthbounder()`
- Direct PG `alarms` table writes (after Phase 2 of the source-of-truth migration; see below)

What alarmd keeps:
- The alarm lifecycle logic itself — create, update, ack, clear semantics, reduction-key dedup, etc. This is the *state management* work tracked separately in `project_alarmd_state_management_strategy_open`. It runs in alarmd's process; only the *storage* changes.

## Phased commitment: Kafka as source of truth

Today PG is the source of truth for alarm state, with alarmd writing the `alarms` table directly. **The commitment of this plan is to invert that.** PG remains as a materialized current-state view for UI/REST query needs, but Kafka becomes the durable event source.

This happens in phases — each is a safe stopping point:

### Phase 1 — alarmd dual-writes

Alarmd writes PG (current behavior, source of truth) **and** publishes to `deltav-alarms-state-change` (new audit stream). Consumers (Prometheus alerts, Email, HTTP, future ALEC) consume the Kafka stream from this phase onward.

- Validates the proto schema in production
- No PG behavior change — REST/UI/SQL queries unchanged
- Failure mode if Kafka publish lags: PG is still authoritative; consumers see eventual consistency

This is the version of the publisher described in earlier drafts of this plan. **It is not the end state.**

### Phase 2 — invert the writes

The Kafka topic becomes the *write target*. A new "alarm state materializer" service consumes `deltav-alarms-state-change` and idempotently maintains the PG `alarms` view. Alarmd writes only to Kafka; PG writes happen via the materializer.

- Architecturally mirrors `KafkaAlarmDataSync` from horizon (which materializes alarms into a Kafka-Streams state store) — same pattern, different sink
- `AlarmEqualityChecker` (lift from horizon) provides idempotency on re-delivery
- REST/UI/SQL queries still hit PG — the table content is the same, the *writer* changes
- Failure mode if materializer lags: PG view is stale by however much the consumer is behind. Consumers reading the stream directly (Prometheus alerts, ALEC) see fresh state.

After Phase 2, Kafka is genuinely the source of truth. Alarm history is replayable; new consumers join with a topic rewind; the PG table is a denormalized projection rather than a write contention point.

### Phase 3 — evaluate dropping PG (optional, deferred)

Once Kafka is source of truth and the materializer is stable, the question becomes: is the PG view still earning its keep, or can a queryable Kafka state store (Kafka Streams interactive queries, or a Redis projection) replace it?

This is a *real* question for high-volume deployments where the PG `alarms` table is a bottleneck. For typical deployments, the PG view is probably fine indefinitely — SQL query expressiveness is hard to give up. **Defer the Phase 3 decision until we've operated Phase 2 long enough to know whether PG is hurting.**

### Why phased

- Each phase is reversible. Phase 1 → fall back to PG-only by disabling the publisher. Phase 2 → fall back to alarmd dual-writing PG directly if the materializer has bugs.
- Each phase ships independent value. Phase 1 alone unblocks the Prometheus alerts forwarder and the northbounder retirement.
- Risk is bounded per phase. We don't bet the alarm pipeline on the full source-of-truth invert in one PR.

### Consumer side (each replacement)

Each consumer is a standalone Spring Boot 4 service. The reference pattern:

- Subscribes to `deltav-alarms-state-change`
- **Joins with `deltav-node-context` as a GlobalKTable** for high-cardinality node enrichment (node_label, location, categories, asset record fields, parent_node_id). Same pattern Phase 2 prometheus-writer uses for time-series enrichment (memory `project_kafka_timeseries_pipeline`). Alarmd publishes thin alarm records; the consumer enriches them locally without going back to the database or to alarmd.
- Filters by destination criteria (configurable: alarm severity, uei pattern, node category, location)
- Performs its destination-specific I/O **off** the Kafka consumer thread (no blocking the partition)
- Exposes `/actuator/prometheus` so the K8s operator can autoscale per-destination

Standard delta-v conventions apply:
- `org.deltav.*` packages, AGPL header (per `feedback_deltav_package_namespace`)
- Config under `deltav.<consumer-name>.*` (per `feedback_no_shared_config_across_daemons`)
- Boot 4.0.x with YAML config natively (these are new consumers — no historical XML to support, no Jackson XmlMapper indirection; see `project_yaml_config_migration`)
- Domain Micrometer metrics: alarms-received, alarms-forwarded, forward-latency, downstream-errors (per `project_v1_2_app_observability`)
- Kafka consumer group per consumer instance type so each destination scales independently of the others

### First-class: Prometheus alerts publisher + consumer

Two pieces, one design:

**Publisher (alarmd side):** Alarmd's existing alarm-lifecycle work writes to PostgreSQL today; the publisher addition serializes each lifecycle event (create, update, ack, clear) into protobuf and produces to `deltav-alarms-state-change` keyed by `{location}@{reduction_key}`. Thin payload: alarm id, severity, UEI, reduction key, node id, location, timestamps, ack state. Anything node-specific beyond `node_id` + `location` stays in the GlobalKTable, not in this record.

**Consumer (Prometheus alerts forwarder):** Subscribes to `deltav-alarms-state-change`, joins with `deltav-node-context` GlobalKTable for enrichment, builds an Alertmanager-compatible payload, POSTs to the configured endpoint. Cardinality discipline:

| Alertmanager field | Use for | Cardinality |
|---|---|---|
| `labels` | Routing, grouping, dedup | **Low.** `severity`, `uei`, `location`, `node_id` only. These drive the Alertmanager routing tree, so each unique combination creates a separate group. |
| `annotations` | Display, human context | **High OK.** `node_label`, `node_categories`, asset record fields (department, building, contact), the description and operator instruction from the alarm, any custom alarm parameters. None of this affects routing — it's payload for the on-call human. |

The GlobalKTable join populates `annotations` without bloating Alertmanager's routing tree. This is the design the user asked for: "send high-cardinality metadata via k-table directly to AlertManager."

**Backend evaluation (open):** Two candidates for the alert backend delta-v standardizes on:

- **Prometheus Alertmanager** — the industry default. Mature routing tree, integration ecosystem (PagerDuty, Slack, OpsGenie, etc.), well-known operator burden.
- **VictoriaMetrics vmalert + Alertmanager-compatible UI** — VictoriaMetrics ships `vmalert` for evaluating alerting rules, and the broader VictoriaMetrics suite includes `vmalertmanager` or supports forwarding to upstream Alertmanager. Lighter operationally (single binary; less memory than Prometheus + Alertmanager stack); same wire format on the forwarder side because vmalert ingests Alertmanager-compatible payloads.

The decision is shaped by what we standardize on for *time-series* — if the prometheus-writer (Phase 2 / memory `project_phase2_prometheus_writer_done`) is already writing to VictoriaMetrics, vmalert is a natural fit and the operator-burden argument wins. If it's writing to Prometheus, Alertmanager is the natural fit. Worth confirming current direction before this consumer ships.

Either way, the forwarder code is identical — both backends accept the same POST-to-`/api/v2/alerts` Alertmanager wire format. The decision is operational (what we deploy) not architectural (what the consumer produces).

## Disposition of the Spring 7 `Assert.notNull` debt

The wider audit (horizon PR #17 body) flagged 12 single-arg `Assert.notNull` sites across the six existing managers:

```
opennms-alarms/syslog-northbounder/.../SyslogNorthbounderManager.java:69,70
opennms-alarms/bsf-northbounder/.../BSFNorthbounderManager.java:74,75
opennms-alarms/snmptrap-northbounder/.../SnmpTrapNorthbounderManager.java:68,69
opennms-alarms/drools-northbounder/.../DroolsNorthbounderManager.java:79,80
opennms-alarms/email-northbounder/.../EmailNorthbounderManager.java:72,73
opennms-alarms/jms-northbounder/.../JmsNorthbounderManager.java:72,73
```

These are dormant in delta-v today (the northbounders aren't activated in any production runtime config we ship). They become **moot** once the migration ships, because the manager classes go away. Don't waste a horizon PR on them — let them die with the extraction.

The remaining 33 single-arg sites (dao-mock, webapp, springframework-security, etc.) are unrelated and tracked separately in `project_horizon_spring7_assert_notnull_audit`.

## Consumer ecosystem: ALEC and the Integration API

A side effect of putting alarms on a published Kafka topic is that the existing horizon **OpenNMS Integration API (OIA)** consumers — which today register OSGi services with alarmd's process and receive in-JVM callbacks — become reimplementable as standalone Kafka consumers.

**Specifically named:**

- **ALEC (Architecture for Learning Enabled Correlation).** ALEC consumes alarms (and inventory and events) via OIA's `AlarmHandler` interface. Its correlation algorithms produce "situation" events that are themselves alarms with parent relationships. ALEC's interface to alarms is *already abstracted* — the OIA `Alarm` model isn't far from `OpennmsModelProtos.Alarm`. The translation work largely exists in horizon's `OpennmsKafkaProducer`. **Path forward:** ALEC becomes a standalone Spring Boot service consuming `deltav-alarms-state-change`, runs its correlation logic, publishes situations either back to alarmd (via REST or a separate `deltav-situations` topic that alarmd consumes) or directly to a sibling topic for further consumers.

- **situation-feedback** (`features/situationfeedback/`). Currently a horizon OIA listener for ML feedback events. Same migration pattern: consume the alarm + situation streams from Kafka; emit feedback on a feedback topic.

- **NCS (Network Compatible Service) Path Correlator.** Same OIA consumption shape; same migration pattern.

- **Third-party plugins.** Anyone today writing an OSGi bundle that implements OIA's `AlarmHandler` is doing so because that was the only integration point. Once the Kafka topic is public, third-party plugins are just consumers — no OSGi, no Karaf bundle, no horizon process embedding required. This is a huge improvement in supportability.

**Important framing:** *delta-v doesn't have to do this work as part of this plan.* What this plan delivers is the *integration point* (the topic + the proto + the documented contract). Whether ALEC actually gets rewritten as a Kafka consumer in the v1.2 / v1.3 timeframe is a separate decision (depends on whether anyone runs ALEC in delta-v deployments yet, capacity, etc.). But this plan **unblocks** that work — it doesn't gate it on any further architectural choices.

The OIA abstraction itself is incidentally retired in delta-v: it was a Karaf-era construct (OSGi service registry + in-process callbacks) and Karaf is dead. The Kafka topic *is* delta-v's OIA replacement.

## Build order

All consumers are **new builds**, not ports. Horizon's `*-northbounder` modules are reference material for behavior (filter semantics, alarm-shape mapping, retry policy) — not source to lift.

1. **Phase 1 — Alarmd Kafka publisher (dual-write)** (must come first so the topic exists)
   - Add `{location}@{reduction_key}`-keyed protobuf publish to `deltav-alarms-state-change` in alarmd's lifecycle path. Lift from horizon's `OpennmsKafkaProducer` rather than greenfield.
   - Starting proto: trim/extend `OpennmsModelProtos.Alarm` from horizon.
   - PG writes remain in alarmd (status quo). Kafka is an additional output, not yet the source of truth.
   - Run *in parallel* with the existing in-process Northbounder dispatcher (which has zero downstream consumers configured in delta-v anyway). Validates the schema before any consumer ships.
   - Includes producing tombstones / clear events for downstream replay semantics.

2. **Prometheus alerts forwarder** (first-class consumer, design template)
   - GlobalKTable join with `deltav-node-context`, labels-vs-annotations cardinality split, POST to Alertmanager-compatible endpoint.
   - Defines the consumer template: directory layout, config shape, observability surface, test harness.
   - Smoke target: configure pointed at a local Alertmanager OR vmalert; fire test alarms; watch alerts appear with correctly enriched annotations.
   - Drives the Alertmanager-vs-vmalert decision (see Open Questions).

3. **Email forwarder** (new build)
   - SMTP destination; lowest-complexity I/O. Validates the per-destination template against a non-Prometheus output.

4. **HTTP forwarder** (new build)
   - Generic POST destination with templated body. Common ask for integrating with home-grown ITSM / ticketing.

5. **Syslog, SNMP trap forwarders** (new builds, gated on Minion gRPC)
   - Both touch the network with protocols delta-v daemons must not speak directly. Build them as consumers that *enqueue* outbound traffic via Minion's RPC channel (memory `project_minion_mandatory`). Block until `project_v1_2_minion_grpc_migration` completes.

6. **JMS forwarder** (decision: build or drop)
   - JMS-as-target is enterprise-legacy. Survey users before building. If retained, it's a clean Kafka-to-JMS bridge.

7. **BSF, Drools forwarders** (decision: build, defer, or punt)
   - Both run user-supplied scripts/rules. Defer until the alarmd state-management decision (`project_alarmd_state_management_strategy_open`) lands — that decision may absorb the use case. The BSF case may be best served by users writing their own Kafka consumer rather than us shipping a scripted runtime.

8. **Phase 2 — Alarm state materializer (Kafka becomes source of truth)**
   - New service: `core/alarms-materializer/` (or similar). Spring Boot 4, Kafka Streams. Consumes `deltav-alarms-state-change`, idempotently upserts into PG `alarms` view via `AlarmEqualityChecker` (lift from horizon).
   - Alarmd's direct PG writes are removed; alarmd publishes only to Kafka.
   - REST / Vue UI / SQL query paths continue to read the PG view — they don't notice the inversion.
   - Ship behind a feature flag so we can roll back to Phase 1 dual-write if the materializer misbehaves in production.

9. **API removal**
   - Once at least the Prometheus + Email + HTTP consumers are in production *and* Phase 2 is stable: delete `org.opennms.netmgt.alarmd.northbounder.*` from delta-v's alarmd. Drop `opennms-alarms/api` and all `opennms-alarms/*-northbounder/` dependencies from the reactor.
   - Even if Syslog/SNMP-trap/JMS/BSF/Drools aren't built yet — once the *interface* is gone, the unbuilt destinations are just future feature requests, not migration debt.
   - Update memory `project_future_features.md` to mark northbounder retirement DONE.

10. **Phase 3 (deferred decision) — evaluate PG view retirement**
    - Run Phase 2 in production long enough to know whether the PG `alarms` view is still earning its keep, or whether a Kafka Streams interactive query / Redis projection can replace it.
    - Likely outcome: PG view stays indefinitely for SQL query expressiveness. The point is we'll know, not guess.

## Open questions

- **Alert backend: Prometheus Alertmanager vs. VictoriaMetrics vmalert.** Standardization choice for delta-v infra. Decision criteria:
  - What does prometheus-writer (Phase 2, memory `project_phase2_prometheus_writer_done`) write to today — Prometheus or VictoriaMetrics? If VictoriaMetrics, vmalert is a natural pairing; the operational stack stays unified.
  - Operator burden: Alertmanager is the industry default with the largest integration ecosystem (PagerDuty, Slack, OpsGenie, etc.). vmalert is lighter operationally but the ecosystem is narrower.
  - K8s deployment shape: both work fine in K8s. VictoriaMetrics operator is mature; Prometheus Operator is more entrenched.
  - **The consumer code is identical** for either choice — both ingest the Alertmanager `/api/v2/alerts` wire format. So this decision can be made operationally (what we deploy) without blocking the consumer build.

- **GlobalKTable wire stability for alarm enrichment.** Phase 1's `deltav-node-context` (memory `project_kafka_timeseries_pipeline`) was designed for time-series consumers. The alerts consumer needs the same enrichment fields *plus* possibly asset record / parent_node_id / IP interface details. Audit what's in the protobuf today; expand if the alerts consumer needs fields that aren't there (provisiond is the producer — same producer adds the missing fields). Coordinate with the prometheus-writer team since they own the schema.

- **Alarm record schema evolution.** Once `deltav-alarms-state-change` exists, do we treat it as a permanent public contract (versioning rules, deprecation policy) or an internal channel that can break without notice? Recommend: **public contract** — third-party consumers (custom user-built forwarders, K8s operator integrations, exotic destinations) will appear quickly once the topic exists.

- **Filtering at the broker.** Today each northbounder filters in-process. With Kafka, two options: (a) per-destination topics (`deltav-alarms-to-syslog`, etc., produced by alarmd after filtering); (b) one topic, consumers filter on read. Option (b) wins for operational simplicity and matches the prometheus-writer / time-series pipeline pattern. Defer firm decision to Prometheus forwarder PR review.

- **Replay semantics.** Does a downstream consumer that's been offline for 10 minutes need to forward the 200 alarms that fired in that window? For Alertmanager, no — Alertmanager dedupes and handles replay via its own state. For email/SNMP-trap, "delivered once" matters — Kafka consumer-group offset semantics give us at-least-once, but each consumer needs to be idempotent on its own side. For the Prometheus forwarder specifically: alarm `endsAt` set to "now" on clear means Alertmanager handles fan-in correctly even if we re-deliver.

- **Alarmd parallel-publish duration.** How long do we run alarmd publishing to Kafka *and* dispatching in-process, before deleting the Northbounder API? Recommend: until the Prometheus + Email + HTTP consumers are in production. After that, the in-process path has zero configured destinations in any delta-v deployment we ship, and the API can go.

- **Where alarmd's state-management decision sits.** If `project_alarmd_state_management_strategy_open` chooses an approach that itself involves a Kafka consumer (e.g., a separate `alarm-state-engine` service), the producer change in step 1 above stays the same — but the *consumer* of `deltav-alarms-state-change` may end up being a new state engine, not alarmd itself. Worth bundling the two designs at implementation time.

- **Proto schema scope.** Start with horizon's `OpennmsModelProtos.Alarm` as the baseline. Two specific decisions to make at implementation time:
  - **Trim or keep?** Horizon's proto includes fields that may not be relevant in delta-v's slim alarm model. Decide per-field rather than carrying dead bytes forever in a public contract.
  - **Inline node metadata or rely on GlobalKTable?** The horizon proto has alarm.node inline (node label, location, etc.). With `deltav-node-context` as the enrichment source, those fields are *redundant*. Recommend keeping the alarm proto thin (`node_id` + `location` only) and letting consumers join — matches the design we want for consumer behavior.

- **PG materializer ownership.** Phase 2's materializer service: lives in alarmd's repo or a separate `core/alarms-materializer/`? Separate service is more aligned with delta-v's one-daemon-per-concern thesis, but means another deployable artifact. Probably worth the artifact — argues for the separation early.

- **Failure modes during the Phase 1 → Phase 2 transition.** What happens if alarmd is publishing to Kafka but the materializer has a bug and PG falls behind? UI shows stale data; Prometheus/ALEC consumers see fresh data. Recovery is "replay the topic"; but if the bug ate data on the PG side, do we trust the rebuilt view? Tabletop this before Phase 2 cutover.

- **ALEC ownership and timeline.** Is ALEC in delta-v's scope at all, or is it an OpenNMS-the-company concern? If delta-v eventually ships ALEC-on-Kafka, who owns it (DJ? Mike? external contributor)? This plan creates the integration point but doesn't commit to building the consumer. Worth confirming with stakeholders before claiming "ALEC is unblocked."

## Test plan (per consumer)

- Boot the consumer pointed at a local Kafka.
- `kafkacat -P -t deltav-alarms-state-change` an alarm proto payload.
- Confirm destination-side artifact (email arrives, Prometheus alert appears, SNMP trap caught by `snmptrapd -P`).
- Confirm `/actuator/prometheus` exposes `deltav_<consumer>_alarms_forwarded_total{severity="critical"}` and friends.
- Confirm consumer survives broker outage (no data loss after broker recovers).

## Related memory

- `project_kafka_timeseries_pipeline` — the GlobalKTable enrichment pattern (`deltav-node-context`) that alert consumers reuse for high-cardinality node metadata; also the precedent for Kafka-source-of-truth + materialized projection
- `project_phase2_prometheus_writer_done` — Phase 2 prometheus-writer is the reference implementation for "Kafka consumer + GlobalKTable join + Prometheus-shape output." The alerts forwarder follows the same architecture for alerts instead of time-series.
- `project_alarmd_alarm_lifecycle_gap` — alarmd lifecycle work that this retirement depends on but doesn't drive
- `project_alarmd_state_management_strategy_open` — state-management redesign that this should align with at implementation time
- `project_horizon_spring7_assert_notnull_audit` — the wider audit that surfaced the 12 northbounder sites
- `feedback_deltav_package_namespace` — `org.deltav.*` package + own copyright for new code
- `feedback_no_shared_config_across_daemons` — `deltav.<consumer>.*` config namespacing
- `project_v1_2_minion_grpc_migration` — Minion transport that syslog/SNMP-trap forwarders must use
- `project_v1_2_app_observability` — observability surface every consumer must expose
- `project_yaml_config_migration` — natural opportunity to ship these as YAML-configured from day one
- `project_future_features` — northbounders are listed as "candidates for future redesign"; this plan retires that placeholder

## Horizon source pointers (for implementation reference)

When the publisher work item is picked up, these are the horizon paths to read first:

- `features/kafka/producer/src/main/java/org/opennms/features/kafka/producer/OpennmsKafkaProducer.java` — main producer reference (~650 LOC)
- `features/kafka/producer/src/main/java/org/opennms/features/kafka/producer/model/OpennmsModelProtos.java` (proto-generated) + the `.proto` source in the same module — wire format starting point
- `features/kafka/producer/src/main/java/org/opennms/features/kafka/producer/datasync/KafkaAlarmDataSync.java` — Phase 2 materializer reference
- `features/kafka/producer/src/main/java/org/opennms/features/kafka/producer/AlarmEqualityChecker.java` (location to verify) — idempotency helper
- `opennms-alarms/api/.../AlarmLifecycleListener.java` — alarmd's listener interface (the publisher implements this)
- `opennms-alarms/api/.../AlarmCallbackStateTracker.java` (location to verify) — callback completion tracker
- `dependencies/oia/` — OIA module structure and contract; reference for what ALEC and similar consumers expect
- `docs/modules/development/pages/oia/` — OIA developer docs
