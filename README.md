# OpenNMS Delta-V

**Delta-V** is a microservice decomposition of [OpenNMS Horizon][], transforming the monolithic Java application into independently deployable Kafka-connected containers.

> For the original OpenNMS Horizon project description, see [OPENNMS.md](OPENNMS.md).

## Architectural Direction

Delta-V has removed Apache Karaf/OSGi from the runtime architecture. All 12 service daemons and Minion run as standalone **Spring Boot 4** applications on a **jlink custom JRE** built from `alpine:3.21`, deployed as individually-sized Docker images via layered JAR deduplication.

Delta-V code lives in the `org.deltav` package namespace with `org.deltav.core` Maven groupIds. Horizon-derived entity classes (`opennms-model-jakarta`) remain at `org.opennms` since horizon JARs reference them by FQN. Horizon dependencies are pre-built in the [delta-v-horizon](https://github.com/pbrane/delta-v-horizon) repository and consumed as Maven artifacts from GitHub Packages.

**Where we are:** All 12 daemons + Minion migrated to Spring Boot 4. The legacy `opennms-config` and `opennms-model` modules are fully decoupled from the daemon stack — all XML config loading uses Jackson XmlMapper with `defaultUseWrapper(false)`. Layered JAR deduplication extracts shared dependencies (~80% overlap) into a common `daemon-base` Docker image (~415MB), with per-daemon overlay images adding only unique libraries. Each daemon starts in 2-4 seconds.

**Flow & metrics pipeline:** Full 4-protocol flow coverage (NetFlow v5, NetFlow v9, IPFIX, sFlow) lands in ClickHouse `deltav.flows_raw` via Minion → Kafka Sink → flow-enricher → `deltav-flows` topic; four dimension materialized views drive the Grafana **Flows Overview** + **Flows Forensic** dashboards. Collectd and Provisiond publish metrics and node context to a `deltav-timeseries` Kafka topic; a Spring Cloud Stream `prometheus-writer` consumer remote-writes to VictoriaMetrics for Prometheus-compatible time-series storage and Grafana visualisation. The flow-enricher exposes 41 Dropwizard-sourced meters at `/actuator/prometheus` via a dedicated bridge. **12 E2E test suites pass** including flows, time-series, prometheus-writer, and node-context pipelines.

---

## Service Daemon Status

### Deleted (12 daemons)

| Daemon | Reason |
|--------|--------|
| **Scriptd** | Event-driven BSF scripting — unused, eliminated |
| **Notifd** | Notification system eliminated — alerts handled externally |
| **Ackd** | Acknowledgment daemon — unused |
| **Actiond** | Legacy shell-command execution on events |
| **Vacuumd** | Database maintenance automations — replaced by PostgreSQL native |
| **Statsd** | Unused statistics reporting |
| **Tl1d** | Legacy TL1 telecom protocol support |
| **Queued** | RRD write optimization — no longer needed |
| **RTCd** | Real-Time Console — dead without webapp |
| **Ticketer** | Trouble ticketing integration — not in microservice architecture |
| **DHCPd** | DHCP monitor/detector service |

### Migrated to Spring Boot 4 (12 daemons + Minion — complete)

| Daemon | Spring Boot Module | Startup | Key Feature |
|--------|--------------------|---------|------------|
| **Alarmd** | `daemon-boot-alarmd` | 2.7s | Full JPA (Hibernate 7) |
| **EventTranslator** | `daemon-boot-eventtranslator` | 2.4s | JDBC-only, event-conf enrichment |
| **Trapd** | `daemon-boot-trapd` | 2.0s | Kafka Sink bridge pattern (`daemon-sink-kafka`) |
| **Syslogd** | `daemon-boot-syslogd` | 2.2s | Reuses Sink bridge, local DNS resolver |
| **Discovery** | `daemon-boot-discovery` | ~2s | Kafka RPC client pattern (`KafkaRpcClientConfiguration`) |
| **Provisiond** | `daemon-boot-provisiond` | 4.2s | JPA + 3x Kafka RPC + Quartz + SNMP adapters |
| **BSMd** | `daemon-boot-bsmd` | 3.3s | JPA + AlarmLifecycleListener + REST API |
| **Pollerd** | `daemon-boot-pollerd` | 4.1s | JPA + Kafka RPC + Twin API + PassiveStatusKeeper |
| **PerspectivePollerd** | `daemon-boot-perspectivepollerd` | 3.6s | JPA + Kafka RPC + perspective outages + event self-consumption |
| **Telemetryd** | `daemon-boot-telemetryd` | 3s | Pure ingestion bridge for non-flow telemetry (OpenConfig via Twin API). Flow protocols (Netflow5/9, IPFIX, sFlow) bypass Telemetryd — they route Minion → Kafka Sink → flow-enricher directly. |
| **Enlinkd** | `daemon-boot-enlinkd` | 3.5s | JPA + Kafka RPC + LLDP/CDP/OSPF/ISIS/Bridge topology |
| **Collectd** | `daemon-boot-collectd` | ~3s | JPA + Kafka RPC + SNMP collection + thresholding |
| **flow-enricher** | `core/flow-enricher` | ~4s | Spring Cloud Stream + horizon UDP parsers (Netflow5/9, IPFIX, sFlow) on raw wire bytes via capturing dispatcher; per-flow enrichment (`JdbcNodeInfoLookup`, application classification, locality); publishes to ClickHouse via `deltav-flows` Kafka topic. 41 `flow_enricher_*` Prometheus meters exposed via `DropwizardToPrometheusBridge`. |
| **Minion** | `daemon-boot-minion` | ~4s | Kafka RPC server + Kafka Sink + Twin API subscriber + UDP flow listener (port 4729) |

### Docker Images

| Image | Base | Contents |
|-------|------|----------|
| `deltav/jre-deltav:21` | `alpine:3.21` | jlink custom JRE (22 modules) + diagnostic tools |
| `deltav/daemon-base` | `jre-deltav:21` | Shared libraries (~321 JARs deduped across 12 daemons) |
| `opennms/<daemon>` | `daemon-base` | Per-daemon unique libs + thin app JAR (12 images) |
| `deltav/minion-boot` | `jre-deltav:21` | Spring Boot 4 Minion — Kafka RPC server + Sink listeners + Twin API |
| `deltav/db-init` | `jre-deltav:21` | One-shot Liquibase schema migration |

### Shared Infrastructure

| Module | Package | Purpose |
|--------|---------|---------|
| `daemon-common` | `org.deltav.core.daemon.common` | DataSource, Kafka event transport, Kafka RPC client, AbstractDaoJpa, EventIpcManagerEnrichingWrapper |
| `daemon-boot-minion-common` | `org.deltav.minion.common` | Minion shared infra: KafkaTwinSubscriberConfiguration, PassiveStatusTwinSubscriber, SnmpV3 config sync |
| `daemon-sink-kafka` | `org.deltav.core.daemon.sink.kafka` | KafkaSinkBridge — consumes from Minion Sink topics (`OpenNMS.Sink.*`) |
| `dao-jpa-support` | `org.deltav.core.daemon.common` | AbstractDaoJpa base class + UpsertTemplate for JPA DAOs |
| `event-forwarder-kafka` | `org.deltav.core.event.forwarder.kafka` | KafkaEventForwarder + EventConfEnrichmentService |
| `opennms-model-jakarta` | `org.opennms.netmgt.model` | Jakarta Persistence entities + JPA DAOs + Enlinkd DAOs + utility classes (stays `org.opennms`) |

### Other Components Removed

| Component | Reason |
|-----------|--------|
| **opennms-webapp** | Legacy JSP webapp — removed from Maven reactor |
| **opennms-webapp-rest** | REST API for dead webapp |
| **opennms-full-assembly** | Monolithic assembly that packaged the webapp |
| **Notification system** | Tables, entities, DAOs, REST, Vaadin UI, config managers |
| **OnmsEvent entity** | Denormalized into OnmsAlarm/OnmsOutage; EventDao deleted |
| **Database Reports** | Jasper Reports, Availability Reports |
| **Device Config Backup** | Entire feature |
| **Charts** | Legacy JFreeChart bar charts |
| **Self-monitoring** | Deprecated monitors for monitoring OpenNMS itself |
| **Eventd TCP/UDP listeners** | Events arrive via Kafka only |
| **MessageBus JMS** | JMS implementation removed; Kafka-only |
| **Zenith Connect** | Cloud registration feature |
| **RPM/Debian packaging** | Native OS packages — Docker-only deployment |

---

## What Is Delta-V?

OpenNMS Horizon is an enterprise-grade open-source network monitoring platform. Delta-V restructures it from a single 35.6 GB monolith into lean, focused microservices:

- **Each daemon runs in its own container** — independent scaling, isolation, and restartability
- **Kafka-only event transport** — no ActiveMQ, no shared event bus
- **Events never touch PostgreSQL** — only alarms are persisted to the database
- **Layered Docker images** — shared `daemon-base` (~415MB) + 12 per-daemon overlay images on a 143MB jlink Alpine JRE
- **Spring Boot 4 migration complete** — all 12 daemons + Minion run as JARs (2-4s startup); Karaf fully retired
- **One-shot database initialization** — `deltav/db-init` replaces the Core container for schema setup
- **`org.deltav` package namespace** — original delta-v code uses `org.deltav.*` packages; horizon-derived model-jakarta stays `org.opennms`

## Architecture

```
Minion → Kafka Sink → Trapd/Syslogd
                          ↓
            KafkaEventForwarder → opennms-fault-events (Kafka)
                                        ↓
                        ┌───────────────┼───────────────┐
                        ↓               ↓               ↓
                    Alarmd      EventTranslator    All Daemons
                   (→ PostgreSQL)  (→ translate     (subscribe to
                                    → re-publish)   relevant events)
                                        ↓
                              opennms-ipc-events (Kafka)
                                        ↓
                              Provisiond, Discovery, etc.
```

### Services

| Service | Runtime | Purpose |
|---------|---------|---------|
| alarmd | Spring Boot 4 | Kafka → alarm creation/reduction → PostgreSQL |
| pollerd | Spring Boot 4 | Service availability polling |
| collectd | Spring Boot 4 | Performance data collection |
| perspectivepollerd | Spring Boot 4 | Perspective (remote location) polling |
| discovery | Spring Boot 4 | Network discovery (via Minion Kafka RPC) |
| trapd | Spring Boot 4 | SNMP trap reception (via Minion Kafka Sink) |
| syslogd | Spring Boot 4 | Syslog reception (via Minion Kafka Sink) |
| eventtranslator | Spring Boot 4 | Event translation rules + enrichment |
| enlinkd | Spring Boot 4 | Enhanced link discovery (LLDP/CDP/OSPF/ISIS/Bridge) |
| provisiond | Spring Boot 4 | Node provisioning and scanning (via Minion Kafka RPC) |
| bsmd | Spring Boot 4 | Business service monitoring |
| telemetryd | Spring Boot 4 | Non-flow telemetry ingestion bridge (OpenConfig via Twin API) |
| flow-enricher | Spring Boot 4 + Spring Cloud Stream | Flow decode via horizon UDP parsers, per-flow enrichment, publish to ClickHouse |
| minion | Spring Boot 4 | Distributed data collection agent (Kafka RPC + Sink + Twin API + UDP flow listener) |
| db-init | Spring Boot 4 | One-shot Liquibase schema migration |
| postgres | postgres:15 | PostgreSQL database (alarms only) |
| clickhouse | clickhouse/clickhouse-server | Flow storage: `deltav.flows_raw` + 4 dimension materialized views (application, source_ip, conversation, dscp) |
| clickhouse-init | one-shot | ClickHouse DDL bootstrap for `deltav.flows_raw` and dimension MVs |
| kafka | Apache Kafka | Event transport backbone |
| prometheus-writer | Spring Boot 4 + Spring Cloud Stream | Consumes `deltav-timeseries` Kafka topic; enriches samples with node context and remote-writes to VictoriaMetrics |
| victoriametrics | victoriametrics/victoria-metrics | Prometheus-compatible time-series store (remote-write sink for Collectd metrics) |
| grafana | grafana/grafana | Dashboards: Flows Overview, Flows Forensic, plus provisioned VictoriaMetrics + ClickHouse datasources |
| l8opensim / minion-lab | Mock lab | 20-device simulated monitoring location with its own Minion; exercises IPFIX + metric pipelines end-to-end |

### Kafka Topics

| Topic | Purpose |
|-------|---------|
| `opennms-fault-events` | Alarm-bearing events (traps, syslog, translated events with alarm-data) |
| `opennms-ipc-events` | Daemon-to-daemon internal events (newSuspect, nodeScanCompleted, reloadDaemonConfig) |
| `OpenNMS.Sink.Trap` | Minion → Trapd raw trap forwarding |
| `OpenNMS.Sink.Syslog` | Minion → Syslogd raw syslog forwarding |
| `OpenNMS.Sink.Telemetry-*` | Minion → flow-enricher per-protocol flow forwarding (Netflow5/9, IPFIX, sFlow) |
| `deltav-flows` | flow-enricher → ClickHouse enriched flow records (ClickHouse Kafka engine table consumes this topic) |
| `deltav-timeseries` | Collectd + Provisiond → prometheus-writer (metric samples + node-context stream for label enrichment) |
| `deltav-prometheus-writer-dlq` | prometheus-writer dead-letter queue for samples that failed VictoriaMetrics remote-write |
| `OpenNMS.twin.response` | Pollerd → Minion Twin API state sync (passive status, SNMPv3 users) |
| `OpenNMS.twin.request` | Minion → Pollerd Twin API subscription requests |

## Quick Start

```bash
cd opennms-container/delta-v

# Start everything (all daemons + webapp + full observability stack)
./deploy.sh up full

# Or for a lighter-weight demo (core daemons + flows + metrics stack):
docker compose --profile lite --profile metrics up -d

# Check service health
./deploy.sh status

# Run E2E tests
./test-e2e.sh                    # Core: trap → provision → alarm lifecycle
./test-minion-e2e.sh             # Minion: trap → Kafka Sink → alarm lifecycle
./test-minion-rpc-e2e.sh         # Minion RPC: provision → detect → poll
./test-syslog-e2e.sh             # Syslog: Cisco syslog → alarm lifecycle
./test-passive-e2e.sh            # Passive: syslog → EventTranslator → Twin API → outage
./test-collectd-e2e.sh           # Collectd: SNMP collection health
./test-perspective-e2e.sh        # Perspective: remote-location polling + outage lifecycle
./test-enlinkd-e2e.sh            # Enlinkd: LLDP topology via Containerlab cEOS
./test-flows-e2e.sh              # Flows: softflowd + hsflowd → Minion → flow-enricher → ClickHouse
./test-timeseries-e2e.sh         # Time-series: Collectd → Kafka → prometheus-writer → VictoriaMetrics
./test-node-context-e2e.sh       # Node context: Provisiond → Kafka → prometheus-writer label enrichment
./test-prometheus-writer-e2e.sh  # End-to-end metrics + 4-protocol flow coverage + Grafana dashboards
```

### Prerequisites

- **JDK 21** (daemon/minion build and runtime)
- Docker Desktop with **16 GB memory** (full profile runs 20+ containers; `lite + metrics` profile is lighter)
- `snmptrap` (net-snmp) for E2E tests

## Building

See [BUILD.md](BUILD.md) for detailed build instructions.

```bash
cd opennms-container/delta-v

# Full build: compile → JRE image → layered daemon images
./build.sh

# Or individual steps:
./build.sh compile    # Maven compile (22 modules, ~16s incremental)
./build.sh jre        # Build jlink custom JRE base image (rarely needed)
./build.sh deltav     # Build Delta-V layered images (daemon-base + 12 per-daemon + minion-boot)
```

## Key Design Decisions

1. **No events table** — Events flow exclusively via Kafka. Only alarms are persisted to PostgreSQL by Alarmd.

2. **No ActiveMQ** — All cross-container communication uses Kafka topics.

3. **Each daemon is self-contained** — Every daemon container has its own `EventWriter`, `EventListener`, `EventExpander`, and `KafkaEventForwarder`.

4. **Producer-side event enrichment** — Each daemon's `KafkaEventForwarder` loads event definitions from the database via `EventConfEnrichmentService` and applies severity + alarm-data before publishing to Kafka.

5. **Minion communicates via Kafka only** — No REST dependency. SNMPv3 user config distributed via Twin API.

6. **Minion is sole network ingress** — No daemon container binds external UDP/TCP monitoring ports. All protocol data enters via Minion → Kafka Sink → KafkaSinkBridge. All polling/collection executes on Minion via Kafka RPC (`force-remote=true`).

7. **Twin API state sync** — Bidirectional config/state sync between daemons and Minion via Kafka Twin topics.

8. **Jackson XmlMapper for config** — All daemon-boot modules use Jackson XmlMapper with `defaultUseWrapper(false)` and `JaxbAnnotationModule`. Legacy JaxbUtils/JAXB eliminated.

9. **`org.deltav` package namespace** — All original delta-v code uses `org.deltav.*` packages with BeaconStrategists copyright. Horizon-derived code (model-jakarta, BSM entities) stays `org.opennms` with dual copyright attribution.

10. **Horizon as pre-built dependency** — Horizon modules are built in `pbrane/delta-v-horizon` and consumed as Maven artifacts, keeping delta-v's 22-module reactor fast (~16s compile).

## Roadmap

**Current release: v1.1.1** — clean Linux consumer flow. Pull the 18 images from GHCR, `docker compose up`, stack comes up hands-off with 28 seed nodes imported. Full 12-test E2E suite passes in isolation.

**Next release: v1.2.0** — "Delta-V runs well on Kubernetes." Four parallel tracks:

- **Container hygiene** — self-contained images (eliminate the remaining bind mounts in `docker-compose.yml` so consumers don't need a git clone). [Design](docs/plans/2026-04-22-v1.2.0-self-contained-images-design.md).
- **Transport modernization** — Minion Kafka IPC → gRPC via Spring Cloud Gateway (lower latency, lighter dependency, same Gateway will front any future static UI). [Design](docs/plans/2026-04-22-v1.2.0-minion-grpc-migration-design.md).
- **Operational observability** — Micrometer domain metrics across all 12 daemons so a future K8s operator can autoscale on application-aware signals (backlog depth, throughput, operation latency) instead of CPU/memory. [Design](docs/plans/2026-04-23-v1.2.0-app-observability-design.md).
- **Historical observability** — Pollerd + PerspectivePollerd publish per-poll response-time samples to the `deltav-timeseries` Kafka topic so service latency lands in VictoriaMetrics alongside SNMP metrics (Kafka Time Series Phase 3). [Design](docs/plans/2026-04-23-v1.2.0-pollerd-timeseries-design.md).

See the [v1.2.0 release plan](docs/plans/2026-04-23-v1.2.0-release-plan.md) for the full scope and forward look at **v1.3.0 candidates** (K8s operator, Nephron analytics replacement, REST API gateway, YAML config migration, SpringServiceDaemon standardization, cloud-native requisitions, static UI).

## Documentation

| Document | Description |
|----------|-------------|
| [OPENNMS.md](OPENNMS.md) | Original OpenNMS Horizon project description |
| [BUILD.md](BUILD.md) | Build instructions |
| [CLAUDE.md](CLAUDE.md) | AI assistant project context |
| [SECURITY.md](SECURITY.md) | Security policy |
| [NOTICE](NOTICE) | OpenNMS derivation attribution |

Design documents and release plans are in `docs/plans/`. Session notes and superpowers agents are in `docs/superpowers/`.

## License

This project is licensed under the [GNU Affero General Public License v3](LICENSE.md).

[OpenNMS Horizon]: http://www.opennms.com/
