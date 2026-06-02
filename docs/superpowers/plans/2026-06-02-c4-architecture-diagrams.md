# Delta-V C4 Architecture Diagrams Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce beautiful, on-brand C4 architecture diagrams for Delta-V (L1 System Context, L2 Containers, L3 Components ×4) from a single Structurizr DSL model, themed to an Amalfi-coast palette, exported as SVG (deltav.kiwi) and PNG (PPTX).

**Architecture:** One `workspace.dsl` is the single source of truth. The model declares people, the Delta-V system, its containers, and components; views select subsets; element/relationship *styles keyed by tag* implement the Amalfi theme. Rendering uses Structurizr Lite (Docker) for the native look; export is automated via the official Structurizr Puppeteer exporter driven by `make` targets.

**Tech Stack:** Structurizr DSL, `structurizr/cli` (validation, Docker), `structurizr/lite` (rendering, Docker), Node + Puppeteer (headless SVG/PNG export), GNU Make.

**Reference spec:** `docs/superpowers/specs/2026-06-02-c4-architecture-diagrams-design.md`

**Verified architecture facts** (from `opennms-container/delta-v/docker-compose.yml` and `core/*`, 2026-06-02):
- 12 Spring Boot daemons: `pollerd, collectd, discovery, trapd, syslogd, eventtranslator, enlinkd, provisiond, bsmd, perspectivepollerd, alarmd, telemetryd`.
- Daemons reach Minion **only** via Kafka RPC → `minion-gateway` → Envoy → Minion (gRPC). Minion mediates all device I/O.
- Data stores: PostgreSQL (`jdbc:postgresql://postgres:5432/opennms`), ClickHouse (`deltav` db), VictoriaMetrics (`:8428`).
- Flow path: `telemetryd` bridges sink→Kafka; `flow-enricher` consumes, enriches via JDBC node-context, writes ClickHouse.
- Alarm path: `alarmd`→`alarms-kafka-publisher` (Kafka alarm-state)→`alarms-materializer`→Postgres; `alerts-forwarder`→Alertmanager (`/api/v2/alerts`)→on-call.
- Metrics path: daemons→Kafka time-series→`prometheus-writer`→remote-write→VictoriaMetrics (`:8428/api/v1/write`)→Grafana; `vmagent` scrapes.
- New code namespace is `org.deltav.*`.

**Conventions for every task:** The "test" for DSL work is `structurizr/cli validate` (well-formedness) plus a content grep for the identifiers just added. Commit after each task. All paths are relative to repo root unless noted.

---

## Task 0: Scaffold directory, README, and theme doc

**Files:**
- Create: `docs/architecture/c4/README.md`
- Create: `docs/architecture/c4/brand/amalfi-theme.md`
- Create: `docs/architecture/c4/exports/.gitkeep`

- [ ] **Step 1: Create the directory tree**

```bash
mkdir -p docs/architecture/c4/brand docs/architecture/c4/exports/svg docs/architecture/c4/exports/png
touch docs/architecture/c4/exports/.gitkeep
```

- [ ] **Step 2: Write the brand/theme spec** — create `docs/architecture/c4/brand/amalfi-theme.md`:

```markdown
# Amalfi Theme — Delta-V C4 Diagrams

Palette inspired by the Amalfi coastline: deep Mediterranean blue vs. sun-lit
turquoise shallows, with Positano-lemon highlights. Color encodes C4 semantics.

| Tag (Structurizr) | Hex | Role |
|---|---|---|
| `person` | `#E07A5F` | Actors (terracotta / Positano rooftops) |
| `daemon` | `#06425C` | Delta-V core Spring Boot daemons (deep sea navy) |
| `messaging` | `#19C3B2` | Kafka event + RPC spine (turquoise shallows) |
| `edge` | `#7FE3D8` | Minion / minion-gateway / Envoy (aquamarine) |
| `database` | `#0E7C9D` | PostgreSQL / ClickHouse / VictoriaMetrics (cobalt) |
| `observability` | `#F6D04D` | Grafana / Alertmanager (Positano lemon) |
| `external` | `#5C8AA0` | Systems beyond the boundary (muted slate) |
| `critical` (relationship) | `#F6D04D` | The Kafka event spine + Minion I/O path |

- Canvas/background: foam off-white `#FDFCF7`.
- Element labels / ink: navy `#08313F`.
- Typeface: Inter (primary), Open Sans (fallback).
- Element text color is white on the navy/cobalt/turquoise fills; navy ink on lemon/aquamarine.
```

- [ ] **Step 3: Write the README** — create `docs/architecture/c4/README.md`:

```markdown
# Delta-V C4 Architecture Diagrams

C4 model (Context / Container / Component) for Delta-V, authored in Structurizr
DSL. `workspace.dsl` is the single source of truth; `brand/amalfi-theme.md`
documents the palette. Exports for the website (SVG) and slides (PNG) live in
`exports/`.

## Edit / preview

    make c4-edit        # serves Structurizr Lite at http://localhost:8080

## Export (SVG + PNG)

    make c4-export      # renders all views into exports/svg and exports/png

## Views

- L1 System Context — `SystemContext`
- L2 Containers — `Containers`
- L3 Components — `DaemonArchetype`, `MinionIpc`, `FlowPipeline`, `MetricsAlarms`
```

- [ ] **Step 4: Commit**

```bash
git add docs/architecture/c4
git commit -m "docs(c4): scaffold c4 directory, README, and Amalfi theme spec"
```

---

## Task 1: workspace.dsl skeleton + L1 System Context + theme styles

**Files:**
- Create: `docs/architecture/c4/workspace.dsl`

- [ ] **Step 1: Write the skeleton with people, the system, external systems, the L1 view, and the full theme styles.** Create `docs/architecture/c4/workspace.dsl`:

```
workspace "Delta-V" "Cloud-native network monitoring platform (OpenNMS Horizon fork)" {

    model {
        operator = person "Network Operator" "Configures monitoring, requisitions, and thresholds." "person"
        noc = person "NOC / SRE" "Watches health, responds to alerts and outages." "person"

        network = softwareSystem "Monitored Network" "Routers, switches, servers and services reached via SNMP, ICMP, flows, syslog and SNMP traps." "external"
        oncall = softwareSystem "On-Call / Paging" "PagerDuty, email, and chat receivers fed by Alertmanager." "external"

        deltav = softwareSystem "Delta-V" "Network monitoring platform: discovery, polling, collection, flows, events, alarms and metrics." {
            # containers added in Task 2
        }

        # L1 relationships
        operator -> deltav "Configures and operates"
        noc -> deltav "Monitors health and outages"
        deltav -> network "Discovers, polls, collects and receives telemetry from (via Minion)"
        deltav -> oncall "Raises notifications to"
    }

    views {
        systemContext deltav "SystemContext" "Delta-V in its operating environment." {
            include *
            autolayout lr
        }

        styles {
            element "Element" {
                fontSize 22
                color "#08313F"
            }
            element "person" {
                shape person
                background "#E07A5F"
                color "#FDFCF7"
            }
            element "Software System" {
                background "#06425C"
                color "#FDFCF7"
            }
            element "external" {
                background "#5C8AA0"
                color "#FDFCF7"
            }
            element "daemon" {
                background "#06425C"
                color "#FDFCF7"
            }
            element "messaging" {
                shape pipe
                background "#19C3B2"
                color "#08313F"
            }
            element "edge" {
                background "#7FE3D8"
                color "#08313F"
            }
            element "database" {
                shape cylinder
                background "#0E7C9D"
                color "#FDFCF7"
            }
            element "observability" {
                background "#F6D04D"
                color "#08313F"
            }
            element "component" {
                background "#0E7C9D"
                color "#FDFCF7"
            }
            relationship "Relationship" {
                color "#08313F"
                thickness 2
            }
            relationship "critical" {
                color "#F6D04D"
                thickness 4
            }
        }
    }
}
```

- [ ] **Step 2: Validate the DSL**

Run:
```bash
docker run --rm -v "$PWD/docs/architecture/c4":/work structurizr/cli validate -w /work/workspace.dsl
```
Expected: exit 0, output ends with a success line (no "Error" lines). If Docker is unavailable, install the CLI zip from https://github.com/structurizr/cli/releases and run `structurizr.sh validate -w docs/architecture/c4/workspace.dsl`.

- [ ] **Step 3: Content check**

Run:
```bash
grep -Ec 'systemContext deltav "SystemContext"|operator =|noc =|network =|oncall =' docs/architecture/c4/workspace.dsl
```
Expected: `5`.

- [ ] **Step 4: Commit**

```bash
git add docs/architecture/c4/workspace.dsl
git commit -m "docs(c4): workspace skeleton, L1 system context, Amalfi styles"
```

---

## Task 2: L2 Container view

**Files:**
- Modify: `docs/architecture/c4/workspace.dsl` (the `deltav` block and `views`)

- [ ] **Step 1: Replace the `# containers added in Task 2` line inside the `deltav { ... }` block** with the container declarations:

```
            # --- Active monitoring daemons ---
            group "Monitoring Daemons" {
                pollerd = container "pollerd" "Service availability polling." "Spring Boot / Java 21" "daemon"
                collectd = container "collectd" "Performance data collection." "Spring Boot / Java 21" "daemon"
                discovery = container "discovery" "Network discovery." "Spring Boot / Java 21" "daemon"
                provisiond = container "provisiond" "Node provisioning and requisitions." "Spring Boot / Java 21" "daemon"
                enlinkd = container "enlinkd" "Link/topology discovery." "Spring Boot / Java 21" "daemon"
                perspectivepollerd = container "perspectivepollerd" "Remote-perspective polling." "Spring Boot / Java 21" "daemon"
                bsmd = container "bsmd" "Business service monitoring." "Spring Boot / Java 21" "daemon"
            }
            group "Event & Notification Daemons" {
                trapd = container "trapd" "SNMP trap reception and eventing." "Spring Boot / Java 21" "daemon"
                syslogd = container "syslogd" "Syslog reception and eventing." "Spring Boot / Java 21" "daemon"
                eventtranslator = container "eventtranslator" "Event translation rules." "Spring Boot / Java 21" "daemon"
                alarmd = container "alarmd" "Alarm reduction from events." "Spring Boot / Java 21" "daemon"
            }
            group "Streaming & Enrichment" {
                telemetryd = container "telemetryd" "Flow/telemetry ingestion bridge." "Spring Boot / Java 21" "daemon"
                flowEnricher = container "flow-enricher" "Enriches flows with node context, writes ClickHouse." "Spring Boot / Java 21" "daemon"
                alarmsPublisher = container "alarms-kafka-publisher" "Publishes alarm state to Kafka." "Spring Boot / Java 21" "daemon"
                alarmsMaterializer = container "alarms-materializer" "Projects alarm state into PostgreSQL." "Spring Boot / Java 21" "daemon"
                prometheusWriter = container "prometheus-writer" "Consumes time-series, remote-writes to VictoriaMetrics." "Spring Boot / Java 21" "daemon"
                alertsForwarder = container "alerts-forwarder" "Forwards alarm/alert state to Alertmanager." "Spring Boot / Java 21" "daemon"
            }
            group "Edge / IPC" {
                kafka = container "Kafka" "Event + RPC spine and sink transport." "Apache Kafka" "messaging"
                minionGateway = container "minion-gateway" "Bridges Kafka (cloud) to gRPC (Minion)." "Spring Boot / Java 21" "edge"
                envoy = container "Envoy" "TLS-terminating gRPC proxy in front of the gateway." "Envoy Proxy" "edge"
                minion = container "Minion" "Edge agent; performs all device I/O." "Karaf / Java" "edge"
            }
            group "Data Stores" {
                postgres = container "PostgreSQL" "Nodes, events, alarms, outages." "PostgreSQL 15" "database"
                clickhouse = container "ClickHouse" "Enriched flow documents." "ClickHouse" "database"
                victoriametrics = container "VictoriaMetrics" "Time-series metrics store." "VictoriaMetrics" "database"
            }
            group "Observability" {
                grafana = container "Grafana" "Dashboards over metrics and flows." "Grafana" "observability"
                alertmanager = container "Alertmanager" "Alert routing, grouping and notification." "Prometheus Alertmanager" "observability"
            }
```

- [ ] **Step 2: Add the L2 relationships at the end of the `model { ... }` block** (after the L1 relationships, before the closing `}` of `model`):

```
        # --- L2 container relationships ---
        operator -> grafana "Views dashboards"
        operator -> provisiond "Manages requisitions"
        noc -> grafana "Watches dashboards"

        # event + RPC spine (critical path)
        pollerd -> kafka "Consumes/produces events; sends device RPC" "Kafka" "critical"
        collectd -> kafka "Consumes/produces events; sends device RPC" "Kafka" "critical"
        discovery -> kafka "Consumes/produces events; sends device RPC" "Kafka" "critical"
        provisiond -> kafka "Consumes/produces events; sends device RPC" "Kafka" "critical"
        enlinkd -> kafka "Consumes/produces events; sends device RPC" "Kafka" "critical"
        perspectivepollerd -> kafka "Consumes/produces events; sends device RPC" "Kafka" "critical"
        bsmd -> kafka "Consumes/produces events" "Kafka"
        trapd -> kafka "Produces events from traps" "Kafka"
        syslogd -> kafka "Produces events from syslog" "Kafka"
        eventtranslator -> kafka "Translates events" "Kafka"
        alarmd -> kafka "Consumes events, produces alarm state" "Kafka"
        telemetryd -> kafka "Bridges flow sink to Kafka" "Kafka"

        # Minion I/O path (critical path)
        kafka -> minionGateway "RPC requests + sink" "Kafka" "critical"
        minionGateway -> envoy "gRPC" "gRPC" "critical"
        envoy -> minion "gRPC (mTLS)" "gRPC" "critical"
        minion -> network "Polls, collects, receives traps/syslog/flows" "SNMP/ICMP/UDP" "critical"

        # persistence
        pollerd -> postgres "Reads/writes" "JDBC"
        collectd -> postgres "Reads/writes" "JDBC"
        provisiond -> postgres "Reads/writes" "JDBC"
        enlinkd -> postgres "Reads/writes" "JDBC"
        alarmd -> postgres "Reads/writes" "JDBC"
        bsmd -> postgres "Reads/writes" "JDBC"

        # flow pipeline
        flowEnricher -> kafka "Consumes flow sink messages" "Kafka"
        flowEnricher -> postgres "Node-context lookup" "JDBC"
        flowEnricher -> clickhouse "Writes enriched flow documents" "HTTP"

        # alarm/alert pipeline
        alarmsPublisher -> kafka "Publishes alarm state" "Kafka"
        alarmsMaterializer -> kafka "Consumes alarm state" "Kafka"
        alarmsMaterializer -> postgres "Upserts alarms" "JDBC"
        alertsForwarder -> kafka "Consumes alarm/alert state" "Kafka"
        alertsForwarder -> alertmanager "POST /api/v2/alerts" "HTTP"
        alertmanager -> oncall "Notifies" "HTTP/SMTP"

        # metrics pipeline
        prometheusWriter -> kafka "Consumes time-series" "Kafka"
        prometheusWriter -> victoriametrics "Remote-write" "HTTP"
        grafana -> victoriametrics "Queries" "PromQL/HTTP"
        grafana -> clickhouse "Queries flows" "HTTP/SQL"
```

- [ ] **Step 3: Add the Container view** inside `views { ... }`, immediately after the `systemContext` block:

```
        container deltav "Containers" "The deployable units of Delta-V and how they communicate." {
            include *
            autolayout lr
        }
```

- [ ] **Step 4: Validate**

Run:
```bash
docker run --rm -v "$PWD/docs/architecture/c4":/work structurizr/cli validate -w /work/workspace.dsl
```
Expected: exit 0, no "Error" lines.

- [ ] **Step 5: Content check**

Run:
```bash
grep -Ec 'container deltav "Containers"|minionGateway =|flowEnricher =|alarmsMaterializer =|prometheusWriter =' docs/architecture/c4/workspace.dsl
```
Expected: `5`.

- [ ] **Step 6: Commit**

```bash
git add docs/architecture/c4/workspace.dsl
git commit -m "docs(c4): L2 container view — daemons, Kafka spine, Minion IPC, data stores"
```

---

## Task 3: L3 — Daemon archetype components

Models the shared Spring Boot daemon pattern (from `core/daemon-common`, package `org.deltav.core.daemon.common`) using `pollerd` as the representative daemon.

**Files:**
- Modify: `docs/architecture/c4/workspace.dsl`

- [ ] **Step 1: Add components inside the `pollerd = container "pollerd" ... "daemon"` declaration.** Change that line from a self-closing container to one with a body:

```
                pollerd = container "pollerd" "Service availability polling." "Spring Boot / Java 21" "daemon" {
                    eventConsumer = component "Kafka Event Transport" "Consumes and produces OpenNMS events over Kafka." "KafkaEventTransportConfiguration" "component"
                    eventExpander = component "Event Expander / Enrichment" "Expands and enriches events from event-conf (logmsg, descr, severity)." "EventConfEnrichmentService, EventIpcManagerEnrichingWrapper, DaemonEventConfDao" "component"
                    pollerLogic = component "Pollerd Service Logic" "Schedules pollable services and detects outages." "Pollerd" "component"
                    rpcClient = component "Kafka RPC Client" "Dispatches device monitor requests to Minion." "KafkaRpcClientConfiguration" "component"
                    daoLayer = component "JDBC DAO Layer" "Reads/writes nodes, services, outages." "JdbcEventUtil, JdbcDistPollerDao, DaemonDataSourceConfiguration" "component"
                }
```

- [ ] **Step 2: Add component-level relationships at the end of the `model { ... }` block:**

```
        # --- L3: daemon archetype (pollerd) ---
        kafka -> eventConsumer "Delivers events" "Kafka"
        eventConsumer -> eventExpander "Raw events"
        eventExpander -> pollerLogic "Enriched events"
        pollerLogic -> rpcClient "Requests device poll"
        rpcClient -> kafka "RPC request/response" "Kafka" "critical"
        pollerLogic -> daoLayer "Persists outages/state"
        daoLayer -> postgres "SQL" "JDBC"
```

- [ ] **Step 3: Add the Component view** inside `views { ... }`, after the `container` view:

```
        component pollerd "DaemonArchetype" "The shared Spring Boot daemon pattern (pollerd shown): event consume -> expand -> logic -> DAO + Minion RPC." {
            include *
            autolayout lr
        }
```

- [ ] **Step 4: Validate**

Run:
```bash
docker run --rm -v "$PWD/docs/architecture/c4":/work structurizr/cli validate -w /work/workspace.dsl
```
Expected: exit 0, no "Error" lines.

- [ ] **Step 5: Content check**

Run:
```bash
grep -Ec 'component pollerd "DaemonArchetype"|eventConsumer =|eventExpander =|rpcClient =|daoLayer =' docs/architecture/c4/workspace.dsl
```
Expected: `5`.

- [ ] **Step 6: Commit**

```bash
git add docs/architecture/c4/workspace.dsl
git commit -m "docs(c4): L3 daemon archetype component view (pollerd)"
```

---

## Task 4: L3 — Minion + minion-gateway IPC components

Components are named by responsibility (C4 components are units of functionality). This reflects the verified Kafka↔gRPC bridge; no fabricated class names are used where the source was not inspected.

**Files:**
- Modify: `docs/architecture/c4/workspace.dsl`

- [ ] **Step 1: Give `minionGateway` and `minion` component bodies.** Replace their single-line declarations with:

```
                minionGateway = container "minion-gateway" "Bridges Kafka (cloud) to gRPC (Minion)." "Spring Boot / Java 21" "edge" {
                    gwKafka = component "Kafka Bridge" "Consumes RPC requests + produces responses; relays sink/telemetry." "Kafka client" "component"
                    gwGrpc = component "gRPC Ingress" "Bidirectional streaming endpoint for Minions." "gRPC server" "component"
                    gwRouter = component "RPC Router" "Correlates RPC requests/responses by location and module." "Java" "component"
                    gwSink = component "Sink Relay" "Forwards Minion sink/telemetry to Kafka topics." "Java" "component"
                }
                minion = container "Minion" "Edge agent; performs all device I/O." "Karaf / Java" "edge" {
                    mnGrpc = component "gRPC Client" "Maintains streaming connection to the gateway via Envoy." "gRPC client" "component"
                    mnRpc = component "RPC Module Dispatcher" "Executes monitor/detector/collector requests." "Java" "component"
                    mnListeners = component "Telemetry Listeners" "SNMP traps, syslog, NetFlow/IPFIX/sFlow receivers." "Java" "component"
                    mnSink = component "Sink Producer" "Streams collected telemetry back to the gateway." "Java" "component"
                }
```

- [ ] **Step 2: Add IPC component relationships at the end of `model { ... }`:**

```
        # --- L3: Minion IPC ---
        kafka -> gwKafka "RPC requests + sink" "Kafka" "critical"
        gwKafka -> gwRouter "Hands off requests"
        gwRouter -> gwGrpc "Streams to Minion"
        gwGrpc -> envoy "gRPC" "gRPC" "critical"
        envoy -> mnGrpc "gRPC (mTLS)" "gRPC" "critical"
        mnGrpc -> mnRpc "Dispatches RPC"
        mnRpc -> network "SNMP/ICMP/etc." "SNMP/ICMP" "critical"
        mnListeners -> network "Receives traps/syslog/flows" "UDP"
        mnListeners -> mnSink "Telemetry"
        mnSink -> mnGrpc "Streams back"
        mnGrpc -> gwGrpc "Responses + sink"
        gwGrpc -> gwSink "Sink frames"
        gwSink -> kafka "Sink topics" "Kafka"
```

- [ ] **Step 3: Add the Component view** inside `views { ... }`:

```
        component minionGateway "MinionIpc" "The Kafka <-> gRPC IPC bridge: gateway, Envoy and the Minion edge agent." {
            include *
            include mnGrpc mnRpc mnListeners mnSink envoy
            autolayout lr
        }
```

- [ ] **Step 4: Validate**

Run:
```bash
docker run --rm -v "$PWD/docs/architecture/c4":/work structurizr/cli validate -w /work/workspace.dsl
```
Expected: exit 0, no "Error" lines.

- [ ] **Step 5: Content check**

Run:
```bash
grep -Ec 'component minionGateway "MinionIpc"|gwKafka =|gwGrpc =|mnGrpc =|mnListeners =' docs/architecture/c4/workspace.dsl
```
Expected: `5`.

- [ ] **Step 6: Commit**

```bash
git add docs/architecture/c4/workspace.dsl
git commit -m "docs(c4): L3 Minion + gateway IPC component view"
```

---

## Task 5: L3 — Flow & telemetry pipeline components

Component class names verified from `core/flow-enricher` (package `org.deltav.flows.enricher`).

**Files:**
- Modify: `docs/architecture/c4/workspace.dsl`

- [ ] **Step 1: Give `flowEnricher` a component body.** Replace its single-line declaration with:

```
                flowEnricher = container "flow-enricher" "Enriches flows with node context, writes ClickHouse." "Spring Boot / Java 21" "daemon" {
                    feDeser = component "Sink Message Deserializer" "Decodes Kafka sink messages into flow records." "SinkMessageDeserializer" "component"
                    feProtocol = component "Protocol Decoders" "Decodes NetFlow v5/v9, IPFIX and sFlow." "Netflow5/9, Ipfix, SFlow MessageProcessor" "component"
                    feEnrich = component "Flow Enrichment" "Adds node, interface and application context." "FlowEnrichmentFunction, JdbcSnmpInterfaceLookup" "component"
                    feClassify = component "Application Classifier" "Classifies flows by application (port-based)." "PortBasedApplicationClassifier" "component"
                    feMapper = component "Flow Document Mapper" "Maps enriched flows to ClickHouse documents." "FlowToDocumentMapper" "component"
                }
```

- [ ] **Step 2: Add flow component relationships at the end of `model { ... }`:**

```
        # --- L3: flow pipeline ---
        telemetryd -> kafka "Flow sink messages" "Kafka"
        kafka -> feDeser "Flow sink messages" "Kafka"
        feDeser -> feProtocol "Raw protocol payloads"
        feProtocol -> feEnrich "Decoded flows"
        feEnrich -> feClassify "Adds application"
        feEnrich -> postgres "Node/interface lookup" "JDBC"
        feClassify -> feMapper "Enriched flows"
        feMapper -> clickhouse "Writes documents" "HTTP"
```

- [ ] **Step 3: Add the Component view** inside `views { ... }`:

```
        component flowEnricher "FlowPipeline" "Flow/telemetry path: telemetryd -> Kafka -> flow-enricher (decode, enrich, classify, map) -> ClickHouse." {
            include *
            include telemetryd
            autolayout lr
        }
```

- [ ] **Step 4: Validate**

Run:
```bash
docker run --rm -v "$PWD/docs/architecture/c4":/work structurizr/cli validate -w /work/workspace.dsl
```
Expected: exit 0, no "Error" lines.

- [ ] **Step 5: Content check**

Run:
```bash
grep -Ec 'component flowEnricher "FlowPipeline"|feDeser =|feProtocol =|feEnrich =|feMapper =' docs/architecture/c4/workspace.dsl
```
Expected: `5`.

- [ ] **Step 6: Commit**

```bash
git add docs/architecture/c4/workspace.dsl
git commit -m "docs(c4): L3 flow & telemetry pipeline component view"
```

---

## Task 6: L3 — Metrics & alarm pipeline components

Class names verified from `core/alarms-kafka-publisher` and `core/alarms-materializer` (packages `org.deltav.alarms.publisher`, `org.deltav.alarms.materializer`).

**Files:**
- Modify: `docs/architecture/c4/workspace.dsl`

- [ ] **Step 1: Give `alarmsPublisher`, `alarmsMaterializer`, and `prometheusWriter` component bodies.** Replace their single-line declarations with:

```
                alarmsPublisher = container "alarms-kafka-publisher" "Publishes alarm state to Kafka." "Spring Boot / Java 21" "daemon" {
                    apMapper = component "Alarm State Mapper" "Maps alarm rows to wire records." "AlarmStateMapper" "component"
                    apPublisher = component "Kafka Alarm Publisher" "Publishes compacted alarm-state records." "KafkaAlarmPublisher" "component"
                    apTopic = component "Topic Initializer" "Ensures the alarm-state topic exists." "AlarmsTopicInitializer" "component"
                }
                alarmsMaterializer = container "alarms-materializer" "Projects alarm state into PostgreSQL." "Spring Boot / Java 21" "daemon" {
                    amConsumer = component "Alarm State Consumer" "Consumes alarm-state records." "AlarmStateKafkaConsumer" "component"
                    amProjector = component "Alarm State Projector" "Projects records into alarm entities." "AlarmStateProjector" "component"
                    amWriter = component "Alarm Upsert Writer" "Upserts alarms into PostgreSQL." "AlarmUpsertWriter" "component"
                    amRetention = component "Retention Engine" "Evaluates retention rules and deletes." "RetentionEngine, AlarmDeleter" "component"
                }
                prometheusWriter = container "prometheus-writer" "Consumes time-series, remote-writes to VictoriaMetrics." "Spring Boot / Java 21" "daemon" {
                    pwConsumer = component "Time-Series Consumer" "Consumes time-series samples from Kafka." "Kafka client" "component"
                    pwRemoteWrite = component "Remote-Write Client" "Sends Prometheus remote-write to VictoriaMetrics." "HTTP" "component"
                }
```

- [ ] **Step 2: Add metrics/alarm component relationships at the end of `model { ... }`:**

```
        # --- L3: metrics & alarm pipeline ---
        alarmd -> apMapper "Alarm rows"
        apMapper -> apPublisher "Wire records"
        apPublisher -> kafka "Alarm-state topic" "Kafka"
        kafka -> amConsumer "Alarm-state records" "Kafka"
        amConsumer -> amProjector "Records"
        amProjector -> amWriter "Alarm entities"
        amWriter -> postgres "Upserts" "JDBC"
        amRetention -> postgres "Deletes expired" "JDBC"
        kafka -> pwConsumer "Time-series samples" "Kafka"
        pwConsumer -> pwRemoteWrite "Samples"
        pwRemoteWrite -> victoriametrics "Remote-write" "HTTP"
        victoriametrics -> grafana "Queried by" "PromQL"
```

- [ ] **Step 3: Add the Component view** inside `views { ... }`:

```
        component alarmsMaterializer "MetricsAlarms" "Alarm path (alarmd -> publisher -> Kafka -> materializer -> Postgres) and metrics path (Kafka -> prometheus-writer -> VictoriaMetrics -> Grafana)." {
            include *
            include apMapper apPublisher apTopic pwConsumer pwRemoteWrite alarmd victoriametrics grafana
            autolayout lr
        }
```

- [ ] **Step 4: Validate**

Run:
```bash
docker run --rm -v "$PWD/docs/architecture/c4":/work structurizr/cli validate -w /work/workspace.dsl
```
Expected: exit 0, no "Error" lines.

- [ ] **Step 5: Content check**

Run:
```bash
grep -Ec 'component alarmsMaterializer "MetricsAlarms"|apPublisher =|amConsumer =|amWriter =|pwRemoteWrite =' docs/architecture/c4/workspace.dsl
```
Expected: `5`.

- [ ] **Step 6: Commit**

```bash
git add docs/architecture/c4/workspace.dsl
git commit -m "docs(c4): L3 metrics & alarm pipeline component view"
```

---

## Task 7: Render-and-export pipeline (Puppeteer + make targets)

Structurizr's native renderer has no headless export in the CLI, so automated SVG/PNG export drives Structurizr Lite with the official Structurizr Puppeteer exporter. This task wires it into `make`.

**Files:**
- Create: `docs/architecture/c4/scripts/export-diagrams.js`
- Create: `docs/architecture/c4/scripts/package.json`
- Modify: `Makefile` (repo root)

- [ ] **Step 1: Create the exporter package manifest** — `docs/architecture/c4/scripts/package.json`:

```json
{
  "name": "deltav-c4-export",
  "version": "1.0.0",
  "private": true,
  "description": "Headless SVG/PNG export of Structurizr Lite diagrams.",
  "dependencies": {
    "puppeteer": "^23.0.0"
  }
}
```

- [ ] **Step 2: Create the exporter** — `docs/architecture/c4/scripts/export-diagrams.js`. This is adapted from the official `structurizr/puppeteer` exporter; it logs in to a local Structurizr Lite instance, iterates every view, and writes both SVG and PNG.

```javascript
// Exports all Structurizr Lite diagrams as SVG + PNG.
// Usage: node export-diagrams.js <structurizrUrl> <format: svg|png|both> <outDir>
const puppeteer = require('puppeteer');
const fs = require('fs');
const path = require('path');

const url = process.argv[2] || 'http://localhost:8080/workspace/diagrams';
const format = process.argv[3] || 'both';
const outDir = process.argv[4] || '../exports';
const svgDir = path.join(outDir, 'svg');
const pngDir = path.join(outDir, 'png');
fs.mkdirSync(svgDir, { recursive: true });
fs.mkdirSync(pngDir, { recursive: true });

const wantSvg = format === 'svg' || format === 'both';
const wantPng = format === 'png' || format === 'both';

(async () => {
  const browser = await puppeteer.launch({
    headless: 'new',
    args: ['--no-sandbox', '--disable-setuid-sandbox'],
  });
  const page = await browser.newPage();
  await page.setViewport({ width: 2400, height: 1600, deviceScaleFactor: 2 });

  await page.goto(url, { waitUntil: 'networkidle0' });
  await page.waitForFunction('typeof structurizr !== "undefined" && structurizr.scripting && structurizr.scripting.isDiagramRendered() === true', { timeout: 60000 });

  const views = await page.evaluate(() => structurizr.scripting.getViews().map(v => v.key));
  console.log('Views: ' + views.join(', '));

  for (const key of views) {
    await page.evaluate((k) => structurizr.scripting.changeView(k), key);
    await page.waitForFunction('structurizr.scripting.isDiagramRendered() === true', { timeout: 60000 });

    if (wantSvg) {
      const svg = await page.evaluate(() => structurizr.scripting.exportCurrentDiagramToSVG({ includeMetadata: true }));
      fs.writeFileSync(path.join(svgDir, key + '.svg'), svg);
      console.log('SVG  ' + key);
    }
    if (wantPng) {
      const png = await page.evaluate(() => structurizr.scripting.exportCurrentDiagramToPNG({ includeMetadata: true, crop: false }));
      const base64 = png.replace(/^data:image\/png;base64,/, '');
      fs.writeFileSync(path.join(pngDir, key + '.png'), Buffer.from(base64, 'base64'));
      console.log('PNG  ' + key);
    }
  }

  await browser.close();
})().catch(e => { console.error(e); process.exit(1); });
```

- [ ] **Step 3: Add `make` targets.** Append to the root `Makefile`:

```makefile
# --- C4 architecture diagrams ---
C4_DIR := docs/architecture/c4
C4_PORT ?= 8080

.PHONY: c4-edit c4-export
c4-edit: ## Serve Structurizr Lite for live editing at http://localhost:$(C4_PORT)
	docker run -it --rm -p $(C4_PORT):8080 -v "$(PWD)/$(C4_DIR)":/usr/local/structurizr structurizr/lite

c4-export: ## Render all C4 views to SVG + PNG in $(C4_DIR)/exports
	@echo "Starting Structurizr Lite..."
	@docker run -d --rm --name deltav-c4-lite -p $(C4_PORT):8080 \
		-v "$(PWD)/$(C4_DIR)":/usr/local/structurizr structurizr/lite
	@echo "Waiting for Lite to come up..."
	@until curl -sf http://localhost:$(C4_PORT)/workspace/diagrams >/dev/null 2>&1; do sleep 2; done
	@cd $(C4_DIR)/scripts && npm install --silent && \
		node export-diagrams.js http://localhost:$(C4_PORT)/workspace/diagrams both ../exports
	@docker stop deltav-c4-lite >/dev/null
	@echo "Exports written to $(C4_DIR)/exports/{svg,png}"
```

Note: if the root `Makefile` already has a `.PHONY:` aggregate or a help-parsing convention, match it. The `## ` comment style above follows the common self-documenting-help pattern; adjust to the project's actual convention if different.

- [ ] **Step 4: Ignore exporter `node_modules`.** Append to `.gitignore` (repo root):

```
docs/architecture/c4/scripts/node_modules/
```

- [ ] **Step 5: Verify the targets parse**

Run:
```bash
make -n c4-export
```
Expected: prints the docker/npm/node command lines without executing them, no "No rule to make target" error.

- [ ] **Step 6: Commit**

```bash
git add docs/architecture/c4/scripts Makefile .gitignore
git commit -m "docs(c4): make c4-edit / c4-export via Structurizr Lite + Puppeteer"
```

---

## Task 8: Generate and commit the exported diagrams

**Files:**
- Create: `docs/architecture/c4/exports/svg/*.svg`, `docs/architecture/c4/exports/png/*.png`

- [ ] **Step 1: Run the export**

Run:
```bash
make c4-export
```
Expected: console lists 6 views (`SystemContext, Containers, DaemonArchetype, MinionIpc, FlowPipeline, MetricsAlarms`) and prints `SVG <key>` / `PNG <key>` for each.

- [ ] **Step 2: Verify all six diagrams exported in both formats**

Run:
```bash
ls docs/architecture/c4/exports/svg | sort && echo "---" && ls docs/architecture/c4/exports/png | sort
echo "svg count: $(ls docs/architecture/c4/exports/svg/*.svg | wc -l), png count: $(ls docs/architecture/c4/exports/png/*.png | wc -l)"
```
Expected: 6 `.svg` and 6 `.png` files, names `Containers, DaemonArchetype, FlowPipeline, MetricsAlarms, MinionIpc, SystemContext`.

- [ ] **Step 3: Spot-check one SVG is non-empty and themed**

Run:
```bash
grep -l '06425C\|19C3B2\|F6D04D' docs/architecture/c4/exports/svg/*.svg | head
```
Expected: at least one file matches (Amalfi palette hex present in the SVG).

**If Step 1 fails** (Puppeteer DOM/scripting API mismatch with the installed Structurizr Lite version): debug against the live instance — open `make c4-edit`, browse to `http://localhost:8080/workspace/diagrams`, and confirm in the browser console that `structurizr.scripting.getViews()` and `exportCurrentDiagramToSVG` exist. Adjust `export-diagrams.js` selectors/API calls to match, then re-run. Use superpowers:systematic-debugging if it does not resolve quickly.

- [ ] **Step 4: Commit**

```bash
git add docs/architecture/c4/exports
git commit -m "docs(c4): export L1/L2/L3 diagrams (SVG + PNG) with Amalfi theme"
```

---

## Task 9: Final review and branch finish

- [ ] **Step 1: Visually review each PNG** for layout, theme correctness, and accuracy against the spec. Open `docs/architecture/c4/exports/png/*.png`. Confirm: navy daemons, turquoise Kafka pipe, lemon critical-path lines + observability, cylinder data stores, person-shaped actors.

- [ ] **Step 2: Confirm the working tree is clean and all commits are present**

Run:
```bash
git status --porcelain && git log --oneline origin/develop..HEAD
```
Expected: empty status; commit list shows the c4 commits (Tasks 0–8) plus the spec commit.

- [ ] **Step 3: Finish the branch** using superpowers:finishing-a-development-branch (open a PR with `--repo pbrane/delta-v --base develop`, per project rules — NEVER target `OpenNMS/*`).

---

## Notes for the implementer

- **Docker is required** for `structurizr/cli`, `structurizr/lite`, and the export. If Docker is unavailable, fall back to the CLI/Lite zip distributions from the Structurizr GitHub releases.
- **`autolayout`** gives a deterministic first pass. For publication-grade layout you may open `make c4-edit`, drag elements, and save — Lite persists manual layout to `workspace.json` beside `workspace.dsl`; commit that file too if you hand-tune.
- **The DSL is the source of truth.** When the architecture changes, edit `workspace.dsl` and re-run `make c4-export`; never hand-edit exported SVG/PNG.
- **Typeface (spec):** per-element styles set `fontSize` but not font family. To apply Inter across all diagrams, add a workspace-level branding block inside `views { ... }`:

  ```
  branding {
      font "Inter" "https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap"
  }
  ```

  Add this in Task 1 alongside `styles`, or as a small follow-up edit; re-export afterward.
- **PNG background variants (spec):** Task 8 produces one PNG per view on Structurizr's default light canvas, which is the slide-ready asset. The SVG is the scalable, transparent-friendly web asset. If the slide template specifically needs a foam-background or transparent PNG batch, that is a trivial ImageMagick post-process over `exports/png/` (e.g. `convert in.png -background '#FDFCF7' -flatten out.png`) — do it as a follow-up only if the template requires it (YAGNI until then).
- **PR target rule:** all PRs go to `pbrane/delta-v`, base `develop`. Never `OpenNMS/*`.
