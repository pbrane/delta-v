# Alarmd Northbounder Extraction → Standalone Kafka Consumers

> **Status:** Design / roadmap. No implementation started. Scope captured 2026-05-14 during the Spring 7 `Assert.notNull` sweep (horizon PR #17), where the wider audit found 12 single-arg `Assert.notNull` sites buried in the in-process northbounders.

## Goal

Move every alarm "northbounder" out of alarmd's JVM and into a standalone Spring Boot Kafka consumer. After this work:

- `alarmd` produces alarm-state-change events to a Kafka topic and stops there.
- Each downstream destination (Syslog, BSF, SNMP trap, Drools, email, JMS, **new: Prometheus**) is a separate service, deployable independently, with its own image and `/actuator` surface.
- The horizon `Northbounder` interface and `opennms-alarms/api` module are deleted from delta-v's reactor (horizon retains them — that's an upstream concern).

This aligns with the broader delta-v thesis: every daemon does one thing, talks to others only over Kafka, never directly imports another daemon's classes.

## Why now

Three independent forces converged:

1. **Alarmd state management redesign is open** (memory `project_alarmd_state_management_strategy_open`). The Drools-in-alarmd approach was abandoned in PR #259 because Boot 4 wiring was a nightmare. Whatever replaces it will be cleaner if alarmd doesn't *also* own six destination integrations.
2. **The Spring 7 `Assert.notNull` audit** (horizon PR #17, 2026-05-14) found 12 single-arg call sites in the northbounder managers. They're a footgun, but fixing them inside alarmd's process is the wrong layer — moving the code into standalone consumers gives us a natural rewrite boundary where those checks become bean-validation or constructor preconditions in idiomatic Spring Boot 4 code.
3. **A Prometheus alert forwarder doesn't exist yet.** Users have started asking for it (OpenNMS issue tracker, Mike at OpenNMS-User-Group 2026-04). Adding it as a *seventh in-process northbounder* would entrench the pattern we want to retire; building it as the *first standalone consumer* sets the template for migrating the other six.

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

### Consumer side (each northbounder)

Each consumer is a standalone Spring Boot 4 service:

- Subscribes to `deltav-alarms-state-change`
- Filters by destination criteria (configurable: alarm severity, uei pattern, node category)
- Performs its destination-specific I/O
- Exposes `/actuator/prometheus` so the K8s operator can autoscale per-destination

Standard delta-v conventions apply:
- `org.deltav.*` packages, AGPL header (per `feedback_deltav_package_namespace`)
- Config under `deltav.<consumer-name>.*` (per `feedback_no_shared_config_across_daemons`)
- Boot 4.0.x + Jackson XmlMapper if config is XML (we're moving toward YAML; this is a clean point to do YAML configs natively — see `project_yaml_config_migration`)
- Domain Micrometer metrics: alarms-received, alarms-forwarded, forward-latency, downstream-errors (per `project_v1_2_app_observability`)

### New: Prometheus alert forwarder

Translate `deltav-alarms-state-change` events into Prometheus Alertmanager API payloads (`POST /api/v2/alerts`). Maps:

- Alarm severity → Alertmanager `severity` label (critical/warning/info)
- Alarm UEI → Alertmanager alert name
- Node fields → Alertmanager labels (`instance`, `node_id`, `node_label`, `location`)
- Alarm clear → Alertmanager `endsAt` set to "now"

This is greenfield in delta-v: no horizon code to port, no `Northbounder` interface to live up to. It's the cleanest reference implementation and should ship first.

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

## Migration order

Recommended sequence — first one is the design reference, the rest follow:

1. **Prometheus alert forwarder** (greenfield)
   - Defines the consumer template, Kafka schema, config shape, observability surface.
   - Lands without disturbing any in-process northbounder.
   - Smoke target: configure with a local Alertmanager, fire test alarms, watch alerts appear.

2. **Producer side: alarmd Kafka publisher**
   - Add a `NorthboundAlarm` → `deltav-alarms-state-change` publisher to alarmd.
   - Run *in parallel* with the existing in-process dispatch — both producers active.
   - Validates the schema before any consumer migration.

3. **Email forwarder** (extracted)
   - Lowest-risk in-process northbounder to port — pure SMTP, no exotic config schemas.
   - Sets the per-destination template.

4. **HTTP forwarder** — straightforward; protobuf-payload-to-HTTP is mechanical.

5. **Syslog, SNMP trap forwarders** — both need destination-network access; need to land *after* the Minion gRPC migration (`project_v1_2_minion_grpc_migration`) so they can use Minion as the outbound transport for network-touching protocols (Minion-Mandatory rule, memory `project_minion_mandatory`).

6. **JMS forwarder** — defer or drop. JMS as a target is enterprise-legacy and may not be worth porting; survey users first.

7. **BSF, Drools forwarders** — both run user-supplied scripts/rules. Defer until we understand the Drools strategy (memory `project_alarmd_state_management_strategy_open`); the BSF case may be served well enough by users writing their own Kafka consumer rather than us shipping a scripted one.

8. **Cleanup**
   - Delete `org.opennms.netmgt.alarmd.northbounder.*` from alarmd.
   - Drop `opennms-alarms/api` from delta-v's horizon dependency set.
   - Update memory `project_future_features.md` to mark northbounder extraction DONE.

## Open questions

- **Schema evolution.** Once `deltav-alarms-state-change` exists, do we treat it as a permanent public contract (versioning rules, deprecation policy) or an internal channel that can break without notice? Recommend: public contract — third-party consumers (custom user-built forwarders, K8s operator integrations) will appear.

- **Filtering at the broker.** Today each northbounder filters in-process. With Kafka, two options: (a) per-destination topics (`deltav-alarms-to-syslog`, etc., produced by alarmd after filtering); (b) one topic, consumers filter on read. Option (b) wins for operational simplicity but spends CPU on every consumer for every alarm. Defer the decision to PR review of the Prometheus forwarder.

- **Replay semantics.** Does a downstream consumer that's been offline for 10 minutes need to forward the 200 alarms that fired in that window? For Prometheus, no — Alertmanager handles replay via its own state. For email/SNMP-trap, "delivered once" matters — Kafka consumer-group offset semantics give us at-least-once, but each consumer needs to be idempotent on its own side.

- **Alarmd parallel-publish duration.** How long do we run alarmd publishing to Kafka *and* dispatching in-process, before flipping the switch? Recommend: until at least the email and HTTP forwarders are in production, so any wire-format bugs surface before SNMP/syslog go.

- **Where alarmd's state-management decision sits.** If `project_alarmd_state_management_strategy_open` chooses an approach that itself involves a Kafka consumer (e.g., a separate `alarm-state-engine` service consuming `deltav-events`), the producer change in step 2 above stays the same — but the *consumer* of `deltav-alarms-state-change` may end up being a new state engine, not alarmd itself. Worth bundling the two designs at implementation time.

## Test plan (per consumer)

- Boot the consumer pointed at a local Kafka.
- `kafkacat -P -t deltav-alarms-state-change` an alarm proto payload.
- Confirm destination-side artifact (email arrives, Prometheus alert appears, SNMP trap caught by `snmptrapd -P`).
- Confirm `/actuator/prometheus` exposes `deltav_<consumer>_alarms_forwarded_total{severity="critical"}` and friends.
- Confirm consumer survives broker outage (no data loss after broker recovers).

## Related memory

- `project_alarmd_alarm_lifecycle_gap` — alarmd lifecycle work that this extraction depends on but doesn't drive
- `project_alarmd_state_management_strategy_open` — state-management redesign that this should align with at implementation time
- `project_horizon_spring7_assert_notnull_audit` — the wider audit that surfaced the 12 northbounder sites
- `feedback_deltav_package_namespace` — `org.deltav.*` package + own copyright for new code
- `feedback_no_shared_config_across_daemons` — `deltav.<consumer>.*` config namespacing
- `project_v1_2_minion_grpc_migration` — Minion transport that syslog/SNMP-trap forwarders must use
- `project_v1_2_app_observability` — observability surface every consumer must expose
- `project_yaml_config_migration` — natural opportunity to ship these as YAML-configured from day one
- `project_future_features` — northbounders are listed as "candidates for future redesign"; this plan retires that placeholder
