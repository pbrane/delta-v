# Alarmd Northbounder Retirement → New First-Class Kafka Consumers

> **Status:** Design / roadmap. No implementation started. Scope captured 2026-05-14 during the Spring 7 `Assert.notNull` sweep (horizon PR #17), where the wider audit found 12 single-arg `Assert.notNull` sites buried in the in-process northbounders. Updated 2026-05-14 with explicit anti-pattern framing and the Prometheus-alerts-first-class direction.

## Goal

**Retire the alarmd northbounder API entirely.** The in-process `Northbounder` interface is an anti-pattern under delta-v's microservice / data-pipeline architecture: it puts six destination integrations inside one daemon, couples their lifecycle to alarmd's, and makes every destination a code-loaded plugin instead of an independently deployable consumer. We're not porting it — we're replacing it.

After this work:

- `alarmd` publishes alarm-state-change events to a Kafka topic and stops there. Zero in-process northbounders.
- Each destination is a **new** standalone Spring Boot Kafka consumer — built fresh against the published topic, not by extracting code from horizon's `opennms-alarms/*-northbounder/` modules. Those modules are *reference material* for behavior (severity filters, alarm-shape mapping) but not sources to port.
- The horizon `Northbounder` interface, `AbstractNorthbounder`, `NorthboundAlarm`, and the entire `opennms-alarms/api` module are removed from delta-v's reactor. (Horizon upstream retains them — that's not our concern.)
- **Prometheus alerts** is the first new consumer, treated as a first-class citizen of delta-v infra (not an optional plugin or experimental side-project). It defines the consumer template for everything else.

This is the broader delta-v thesis applied: every daemon does one thing, talks to others only over Kafka, never directly imports another daemon's classes. The northbounder API violated all three.

## Why now

Four reasons, in priority order:

1. **The northbounder API is fundamentally an anti-pattern in delta-v.** Delta-v's design is microservices over Kafka. The horizon `Northbounder` interface is the opposite: an in-process callback registered with alarmd's dispatcher, called synchronously per alarm, with the destination's I/O happening on alarmd's threads. A failure in any destination (slow SMTP, blocked SNMP, Drools rule storm) can wedge alarmd. The fix isn't a better in-process API — it's removing the API.
2. **Prometheus alerts must be first-class.** Users have started asking for it (OpenNMS issue tracker, Mike at OpenNMS-User-Group 2026-04). Adding it as a *seventh in-process northbounder* would entrench the anti-pattern. Building it as the *first standalone consumer* sets the template for everything else — and pulls forward the platform-level decision about which alert backend we standardize on (Prometheus Alertmanager vs. VictoriaMetrics vmalert; see Open Questions).
3. **Alarmd state management redesign is open** (memory `project_alarmd_state_management_strategy_open`). The Drools-in-alarmd approach was abandoned in PR #259 because Boot 4 wiring was a nightmare. Whatever replaces it will be much cleaner if alarmd doesn't *also* own six destination integrations.
4. **The Spring 7 `Assert.notNull` audit** (horizon PR #17, 2026-05-14) found 12 single-arg call sites in the northbounder managers. They're a footgun, but fixing them inside alarmd's process is the wrong layer — they vanish when the managers disappear.

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

Alarmd publishes to a Kafka topic — name proposal `deltav-alarms-state-change` (matches the `deltav-*` naming we've used since the Sink topic rework). The schema is the **already-existing** `NorthboundAlarm` projection, just protobuf-serialized rather than passed as a Java object reference. The fields are stable enough that the proto IDL can be derived mechanically from the Java class.

What alarmd loses:
- `opennms-alarms/api` dependency
- `org.opennms.netmgt.alarmd.northbounder.*` registration and dispatch code
- Six in-process `Manager` beans (Syslog, BSF, SNMPTrap, Drools, Email, JMS) that today register with `Alarmd.registerNorthbounder()`

What alarmd keeps:
- `NorthboundAlarm` as the shape of the payload (rename `org.opennms.netmgt.alarmd.api.NorthboundAlarm` → `org.deltav.alarms.event.AlarmStateChange` or similar; protobuf becomes the wire format)
- Alarm lifecycle (create, ack, clear) — this is the *state management* work tracked separately in `project_alarmd_state_management_strategy_open`

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

## Build order

All consumers are **new builds**, not ports. Horizon's `*-northbounder` modules are reference material for behavior (filter semantics, alarm-shape mapping, retry policy) — not source to lift.

1. **Producer side: alarmd Kafka publisher** (must come first so the topic exists)
   - Add `{location}@{reduction_key}`-keyed protobuf publish to `deltav-alarms-state-change` in alarmd's lifecycle path.
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

8. **API removal**
   - Once at least the Prometheus + Email + HTTP consumers are in production: delete `org.opennms.netmgt.alarmd.northbounder.*` from delta-v's alarmd. Drop `opennms-alarms/api` and all `opennms-alarms/*-northbounder/` dependencies from the reactor.
   - Even if Syslog/SNMP-trap/JMS/BSF/Drools aren't built yet — once the *interface* is gone, the unbuilt destinations are just future feature requests, not migration debt.
   - Update memory `project_future_features.md` to mark northbounder retirement DONE.

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

## Test plan (per consumer)

- Boot the consumer pointed at a local Kafka.
- `kafkacat -P -t deltav-alarms-state-change` an alarm proto payload.
- Confirm destination-side artifact (email arrives, Prometheus alert appears, SNMP trap caught by `snmptrapd -P`).
- Confirm `/actuator/prometheus` exposes `deltav_<consumer>_alarms_forwarded_total{severity="critical"}` and friends.
- Confirm consumer survives broker outage (no data loss after broker recovers).

## Related memory

- `project_kafka_timeseries_pipeline` — the GlobalKTable enrichment pattern (`deltav-node-context`) that alert consumers reuse for high-cardinality node metadata
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
