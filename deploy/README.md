# OpenNMS Delta-V

**Composable, containerized deployment of OpenNMS Horizon.**

Delta-V decomposes the monolithic OpenNMS into independently scalable services connected by Kafka, PostgreSQL, and (for the flow pipeline) ClickHouse. Each daemon runs in its own container as a Spring Boot fat JAR, communicating via Kafka event topics. There is no core container and no legacy webapp — schema migration is handled by one-shot `db-init` and `clickhouse-init` containers, and operator observability lives on each daemon's Spring Boot Actuator endpoints (`/actuator/health`, `/actuator/prometheus`).

```
                    ┌──────────────────────────────────────────────────┐
                    │                   Kafka (KRaft)                  │
                    │    deltav-fault-events / deltav-ipc-events     │
                    └──┬──┬──┬──┬──┬──┬──┬──┬──┬──┬──┬──┬──┬──┬─────┘
                       │  │  │  │  │  │  │  │  │  │  │  │  │  │
  ┌─────────┐     ┌────┴──┴──┴──┴──┴──┴──┴──┴──┴──┴──┴──┴──┴──┴────┐
  │Postgres │◄────┤ alarmd │ pollerd │ collectd │ provisiond │ ...   │
  │         │     │ Spring Boot fat JARs — one per daemon           │
  └─────────┘     └─────────────────────────────────────────────────┘
```

## Spring Boot Migration Progress

All 12 daemons have been migrated from Karaf to Spring Boot 4. Each runs as an independent fat JAR with its own JPA/Hibernate context, Kafka event transport, and health endpoint.

| Daemon             | Status | PR   | E2E Verified | Notes |
|--------------------|--------|------|--------------|-------|
| Alarmd             | Done   | #32  | Yes          | Kafka event consumer, alarm processing |
| Pollerd            | Done   | #47  | Yes          | Service polling via Minion RPC |
| PerspectivePollerd | Done   | #48  | Yes          | Perspective polling from remote locations |
| Provisiond         | Done   | #38  | Yes          | Node provisioning, SNMP/ICMP detection via Minion |
| Discovery          | Done   | #42  | Yes          | Network discovery via Minion |
| Trapd              | Done   | #35  | Yes          | SNMP trap reception via Kafka Sink |
| Syslogd            | Done   | #36  | Yes          | Syslog reception via Kafka Sink |
| EventTranslator    | Done   | #37  | Yes          | Event transformation rules |
| BSM Daemon         | Done   | #44  | Yes          | Business Service Monitor |
| Enlinkd            | Done   | #50  | Yes          | Link discovery (LLDP/CDP/OSPF/IS-IS/Bridge) via Minion |
| Telemetryd         | Done   | #52  | Yes          | Telemetry ingestion bridge |
| **Collectd**       | **Done** | **#56** | **Yes** | **SNMP data collection via Minion SNMP proxy** |

### Shared Infrastructure (daemon-common)

All 12 daemons share infrastructure from `core/daemon-common`:

- **BeanUtils bridge**: `BeanUtils` is registered as a Spring bean so legacy static lookups (`BeanUtils.getBean(...)`) route to the daemon's own ApplicationContext instead of falling through to the legacy `ContextRegistry` XML context chain.
- **MATE EntityScopeProvider**: `DaemonEntityScopeProvider` resolves MATE metadata expressions (`${node:label}`, `${scv:alias:password}`, `${asset:region}`) in daemons with database access (Provisiond, Pollerd, Collectd, Enlinkd, PerspectivePollerd). Daemons without DAOs fall back to `NoOpEntityScopeProvider`.
- **Secure Credentials Vault**: JCEKS-backed `SecureCredentialsVault` reads from `${opennms.home}/etc/scv.jce` for `${scv:...}` credential interpolation.
- **Thresholding**: Stubbed with a no-op `ThresholdingService`. Full thresholding support is a follow-up task.

### Karaf Image Retirement

With all 12 daemons on Spring Boot, the Karaf-based Sentinel image (`deltav/daemon-deltav`) can be retired. The only Karaf component remaining is the Minion, which runs the standard OpenNMS Minion distribution.

## Services

| Service            | Image                         | Purpose                                                                  | Host Port |
|--------------------|-------------------------------|--------------------------------------------------------------------------|-----------|
| postgres           | postgres:15                   | Shared database (alarms only)                                            | 5432      |
| kafka              | apache/kafka                  | Event bus (KRaft mode)                                                   | 19092     |
| db-init            | deltav/db-init               | One-shot PostgreSQL schema migration (exits after init)                  | —         |
| clickhouse         | clickhouse/clickhouse-server  | Flow storage: `deltav.flows_raw` + 4 dimension MVs                       | 8123      |
| clickhouse-init    | one-shot                      | One-shot ClickHouse DDL bootstrap                                         | —         |
| minion             | deltav/minion-boot           | Spring Boot 4 distributed data collection agent + UDP flow listener      | 4729/udp  |
| snmp-agent         | tandrup/netsnmp               | Local SNMP test target for Collectd / detectors                          | —         |
| alarmd             | deltav/alarmd                | Alarm processing (Kafka consumer)                                        | —         |
| pollerd            | deltav/pollerd               | Service polling via Minion RPC                                           | —         |
| collectd           | deltav/collectd              | SNMP data collection via Minion SNMP proxy                               | —         |
| discovery          | deltav/discovery             | Network discovery via Minion RPC                                         | —         |
| provisiond         | deltav/provisiond            | Node provisioning and detection                                          | —         |
| trapd              | deltav/trapd                 | SNMP trap reception (Kafka Sink)                                         | —         |
| syslogd            | deltav/syslogd               | Syslog reception (Kafka Sink)                                            | —         |
| eventtranslator    | deltav/eventtranslator       | Event transformation rules                                               | —         |
| enlinkd            | deltav/enlinkd               | Link discovery (CDP, LLDP, OSPF, IS-IS, Bridge)                          | —         |
| bsmd               | deltav/bsmd                  | Business Service Monitor                                                 | 8180      |
| perspectivepollerd | deltav/perspectivepollerd    | Perspective polling from remote locations                                | —         |
| telemetryd         | deltav/telemetryd            | Non-flow telemetry ingestion (OpenConfig via Twin API)                   | —         |
| flow-enricher      | deltav/flow-enricher         | Flow decode (horizon UDP parsers) + enrich + publish to ClickHouse       | 8080      |

All daemon containers extend a shared `deltav/daemon-base` image built on top of `deltav/jre-deltav:21` (a jlink custom JRE on `alpine:3.21`). Each per-daemon image adds only the libraries unique to that daemon via layered Docker image deduplication.

## Quick Start

### Prerequisites

- Docker Engine 24+ with Compose v2
- Java 21 (for building from source)
- 8 GB RAM allocated to Docker (16 GB recommended for full deployment)

### Build from Source

```bash
# Clone the repository
git clone https://github.com/pbrane/delta-v.git
cd delta-v

# Copy the example environment file (sets VERSION + KAFKA_EXTERNAL_HOST).
# `.env` is gitignored so local overrides (e.g., a bumped VERSION for smoke
# testing) don't pollute git status.
cp deploy/.env.example deploy/.env

# Full build: compile all modules, then build the Docker images
make build && make images

# Or build just the images (if Maven artifacts exist)
make images
```

### Deploy

```bash
# Start with a profile
make up PROFILE=active     # Core daemons + flow stack
make up PROFILE=passive    # Active + trapd/syslogd/eventtranslator
make up PROFILE=full       # All daemons
make up PROFILE=demo       # Everything + metrics/dashboards/alerting

# Check status
make status

# Verify deployment
make verify
```

#### Dev/test deployments — lean JVM overrides

On resource-constrained lab VMs, layer `docker-compose.dev.yml` on top of the base file to shrink JVM heap / metaspace / thread-stack allocations per daemon. Saves ~2–3 GB of stack-wide RSS at lab scale (28 nodes); numbers calibrated from empirical `jcmd GC.heap_info` on the v1.2.0-alpha2 smoke test.

The `demo` profile **applies this override automatically** — `make up PROFILE=demo` is the full stack + observability with the lean JVM sizing, i.e. the same orchestration the smoke VM runs. To layer it onto another profile manually:

```bash
docker compose -f docker-compose.yml -f docker-compose.dev.yml --profile <profile> up -d
```

Only use the override for dev/test/demo — the production-shaped defaults in `docker-compose.yml` are sized for real target counts. See the comment block at the top of `docker-compose.dev.yml` for per-daemon sizing rationale.

**No Web UI.** The legacy OpenNMS JSP webapp has been removed from the Maven reactor (`opennms-webapp` + `opennms-webapp-rest`). Operator observability lives on each daemon's Spring Boot Actuator:

```bash
# Per-daemon health (reachable when the daemon exposes a management port)
curl http://localhost:8180/actuator/health       # bsmd
docker compose exec flow-enricher wget -qO- http://localhost:8080/actuator/health

# Prometheus metrics from the flow-enricher (41 flow_enricher_* meters)
docker compose exec flow-enricher wget -qO- http://localhost:8080/actuator/prometheus | grep '^flow_enricher_'
```

### Manage

```bash
# View logs
make logs                     # All services
make logs SVC=alarmd          # Single service

# Stop (preserve data)
make down

# Reset (destroy all data)
make reset
```

## E2E Tests

Nine end-to-end test suites validate the full pipeline:

```bash
cd deploy

bash test-e2e.sh                 # Full alarm create/clear via SNMP traps
bash test-minion-e2e.sh          # Trap → Minion → Kafka → Alarmd lifecycle
bash test-minion-rpc-e2e.sh      # Detector + Monitor via Minion RPC
bash test-syslog-e2e.sh          # Syslog → Minion → Kafka → Alarmd lifecycle
bash test-passive-e2e.sh         # Passive status via EventTranslator + Pollerd
bash test-collectd-e2e.sh        # SNMP data collection via Minion
bash test-perspective-e2e.sh     # Perspective polling from remote locations + outage lifecycle
bash test-enlinkd-e2e.sh         # LLDP/CDP link discovery on Containerlab cEOS
bash test-flows-e2e.sh           # softflowd + hsflowd → Minion → flow-enricher → ClickHouse (18 assertions across 4 phases)
```

All scripts support:
- `--verbose` — show diagnostic output on failure
- `--pre-clean` — delete all nodes and alarms from DB before running
- `--post-cleanup` — delete test data after run

## Flow Enricher

### Flow reverse-DNS enrichment (`deltav.flows.dns.*`)

**Behaviour change (v1.3.0-rc9+):** reverse-DNS enrichment is **ON by default**
(Horizon parity). The flow-enricher now reverse-resolves flow IPs to hostnames
(`src_hostname`/`dst_hostname`/…) via horizon's `NettyDnsResolver`. Previously
delta-v shipped a no-op resolver (no lookups). Disable with
`DELTAV_FLOWS_DNS_ENABLED=false`.

| Env var | Default | Meaning |
|---|---|---|
| `DELTAV_FLOWS_DNS_ENABLED` | `true` | master on/off |
| `DELTAV_FLOWS_DNS_SCOPE` | `all` | `all` (resolve every IP) or `private` (only RFC1918/ULA/loopback/link-local — bounds cardinality to your address space) |
| `DELTAV_FLOWS_DNS_NAMESERVERS` | _(blank)_ | DNS server(s); blank = system resolv.conf |
| `DELTAV_FLOWS_DNS_QUERY_TIMEOUT_MS` | `5000` | per-lookup timeout |
| `DELTAV_FLOWS_DNS_MAX_CONCURRENT` | `1000` | bulkhead: max in-flight lookups |
| `DELTAV_FLOWS_DNS_CACHE_*_TTL_S` | `-1` | cache min/max/negative TTL (`-1` = NettyDnsResolver default) |
| `DELTAV_FLOWS_DNS_BREAKER_ENABLED` / `_FAILURE_RATE` | `true` / `80` | circuit breaker on unhealthy DNS |

For high-cardinality internet-facing flows, prefer `DELTAV_FLOWS_DNS_SCOPE=private`
and/or a lower `DELTAV_FLOWS_DNS_QUERY_TIMEOUT_MS` to protect flow throughput.
Metrics: `deltav_dns_reverse_lookups_total{result=hit|miss|error|filtered}`.

## Build Script Reference

`make` is the front door (`make build`, `make images`); the underlying engine is
`tools/build.sh`, run from the repo root for granular control:

```bash
tools/build.sh              # Full build (compile + images)
tools/build.sh compile      # Maven compile only
tools/build.sh images       # Build base Docker images
tools/build.sh deltav       # Build Delta-V layered images
tools/build.sh push         # Build and push to registry
tools/build.sh clean        # Remove Docker volumes

# Push to custom registry
DOCKER_ORG=pbranestrategy tools/build.sh push
```

## Architecture

Delta-V replaces the monolithic OpenNMS runtime with a composable service mesh:

- **db-init** runs schema migration (Liquibase) and exits — no persistent core container
- **Daemon containers** each run a single daemon as a Spring Boot fat JAR with embedded Tomcat for health endpoints (`/actuator/health`)
- **Minion** handles distributed data collection (SNMP, ICMP) via Kafka IPC
- All daemons use **Hibernate 7 / Jakarta Persistence** with the `opennms-model-jakarta` entity model

All services communicate via two Kafka topics: `deltav-fault-events` (alarm-bearing events) and `deltav-ipc-events` (daemon-to-daemon coordination). Each service generates globally unique event IDs using TSID (Time-Sorted IDs) with a unique node-id per JVM.

### Event Flow

```
Daemon → KafkaEventForwarder → Kafka → KafkaEventSubscriptionService → Alarmd
                                  ↓
                    Other daemons subscribe to relevant events
```

Events bypass the traditional `events` database table entirely. They flow through Kafka in real-time, and Alarmd processes them directly into alarms.

### Daemon Boot Pattern

Each Spring Boot daemon follows a consistent pattern:

```
core/daemon-boot-<name>/
├── src/main/java/.../boot/
│   ├── <Name>Application.java          # @SpringBootApplication entry point
│   ├── <Name>JpaConfiguration.java     # JPA entities, FilterDao, TransactionTemplate
│   ├── <Name>DaemonConfiguration.java  # Daemon bean, lifecycle, config factories
│   └── <Name>RpcConfiguration.java     # Kafka RPC client (if needed)
└── src/main/resources/
    └── application.yml                 # Datasource, Kafka, RPC settings
```

Infrastructure beans (Kafka event transport, RPC client factory, TSID) are shared via `core/daemon-common`.

## Memory Requirements

Running all services requires significant memory. If Docker Desktop runs out of memory (exit code 137), use deployment profiles:

| Profile | Services | Approx. Memory |
|---------|----------|-----------------|
| active  | ~10      | ~8 GB           |
| passive | ~13      | ~10 GB          |
| full    | all daemons | ~12 GB       |
| demo    | full + observability | ~14 GB |

## Kafka Time-Series Producer (Collectd)

### Enabling

The Kafka Time-Series producer in the Collectd daemon is off by default. To enable:

```bash
export DELTAV_TIMESERIES_ENABLED=true
docker compose up -d
```

When enabled, Collectd publishes one `TimeseriesBatch` protobuf record per CollectionSet poll to the `deltav-timeseries` Kafka topic, keyed `{location}@{node_id}`. The existing `InMemoryStorage` TSS backend continues to run alongside — the Kafka publisher is additive, not a replacement.

### Topic provisioning

Both topics are declared as Spring Boot `NewTopic` beans in the Collectd application and are created with the following default configuration the first time Collectd starts with the flag on:

| Topic | Partitions | Retention | Cleanup | Compression |
|-------|-----------|-----------|---------|-------------|
| `deltav-timeseries` | 16 | 7 days | delete | lz4 |
| `deltav-node-context` | 8 | infinite | compact | default |

Do **not** rely on Kafka broker auto-create: defaults are 1 partition / 1 replica, which silently defeats the 16-partition design.

### Tunables (environment variables)

| Variable | Default | Purpose |
|---|---|---|
| `DELTAV_TIMESERIES_ENABLED` | `false` | Kill switch — opt-in feature flag |
| `DELTAV_TIMESERIES_PARTITIONS` | `16` | `deltav-timeseries` partition count |
| `DELTAV_TIMESERIES_REPLICATION_FACTOR` | `1` dev / `3` prod | `deltav-timeseries` replication |
| `DELTAV_TIMESERIES_RETENTION_DAYS` | `7` | Time-series record retention |
| `DELTAV_NODE_CONTEXT_PARTITIONS` | `8` | Context topic partitions |
| `DELTAV_NODE_CONTEXT_REPLICATION_FACTOR` | `1` dev / `3` prod | Context topic replication |

### Observability

Collectd's `/actuator/prometheus` endpoint exposes:

| Metric | Type | Purpose |
|---|---|---|
| `deltav_timeseries_batches_published_total` | counter | Successful publishes |
| `deltav_timeseries_batches_failed_total{reason}` | counter | Failures by reason (`serialization_error`, `kafka_send_error`, `empty_batch`, `translator_error`) |
| `deltav_timeseries_batch_size_bytes` | distribution summary | Wire size per record |
| `deltav_timeseries_batch_size_warning_total` | counter | Records over 800 KB |
| `deltav_timeseries_resources_per_batch` | distribution summary | Resource count per record |
| `deltav_timeseries_publish_duration_seconds` | timer | End-to-end publish latency |

### Expected disk footprint

A 1,000-node deployment polling every 5 minutes with default settings produces roughly 140 GB of `deltav-timeseries` log on disk before the 7-day retention window rolls. Adjust `DELTAV_TIMESERIES_RETENTION_DAYS` to shape this, or reduce per-poll scope in `collectd-configuration.xml` for dense nodes.

### Known limitations (schema v1)

- Collectd is a singleton service today: there is no horizontal scale and no leader election. If Collectd restarts mid-poll, the current CollectionSet may not reach Kafka. The scheduler/publisher split that addresses this is tracked separately.
- The wire format is subject to breaking changes during Phase 1 (dev-only). Once Phase 2 ships (production-enabled), only forward-compatible schema changes are allowed.
- The `deltav-node-context` producer shipped in Phase 1 (provisiond change feed). See the Node-Context Change Feed section below for the full env-var and metric catalog.
- Node identity on the wire (`node_id`, `location`) is sourced from Collectd's `ServiceParameters` keys `node-id` and `location`. If Collectd does not populate these keys in a given deployment, records land with `node_id=0` / `location=""`; consumers must still be able to join via the `deltav-node-context` GlobalKTable to resolve identity.

## Node-Context Change Feed (Phase 1)

Provisiond publishes a compacted `deltav-node-context` Kafka topic keyed
`{location}@{node_id}`. Every node-lifecycle event (add, update, delete,
label/location/category/info/asset change) produces a `NodeContext`
protobuf record; `nodeDeleted` produces an explicit `deleted=true`
tombstone. On every provisiond restart, the full node set is re-published
(compaction absorbs duplicates).

**Feature flag:**

| Env var | Default | Purpose |
|---|---|---|
| `DELTAV_NODE_CONTEXT_ENABLED` | `true` | Kill switch. Flip to `false` + restart provisiond to remove all producer beans. |
| `DELTAV_NODE_CONTEXT_PARTITIONS` | `8` | Topic partition count. Used at first topic creation only (KafkaAdmin is idempotent; existing topic keeps its original partition count). |
| `DELTAV_NODE_CONTEXT_REPLICATION_FACTOR` | `1` (dev) / `3` (prod) | Topic replication factor. |
| `DELTAV_NODE_CONTEXT_DEBOUNCE_MS` | `250` | Per-nodeId debounce window. Collapses bursts of UEIs for the same node into one publish. |
| `DELTAV_NODE_CONTEXT_DEBOUNCE_THREADS` | `2` | Debouncer executor pool size. |

**Metrics (on `/actuator/prometheus`):**

- `deltav_node_context_records_published_total{location, producer="provisiond", reason}` — `reason` ∈ `change`, `bootstrap`, `tombstone`, `relocation_old_key`, `relocation_new_key`
- `deltav_node_context_records_failed_total{location, reason}` — `db_read_error`, `translator_error`, `serialization_error`, `kafka_send_error`, `node_not_found`, `malformed_event`, `missing_location`, `bootstrap_error`, `debouncer_rejected`
- `deltav_node_context_record_size_bytes{location}` — protobuf size distribution (pre-compression)
- `deltav_node_context_record_size_warning_total{location}` — count of records >800 KB (still published)
- `deltav_node_context_publish_duration_seconds{location, reason}` — end-to-end publish latency
- `deltav_node_context_debounce_coalesced_total` — count of events that hit an existing pending future
- `deltav_node_context_debounce_pending_gauge` — current pending-future count
- `deltav_node_context_bootstrap_duration_seconds` — total wall-clock for the startup enumeration pass

**Topic properties:**

- `cleanup.policy=compact` (log-compacted — latest-per-key forever)
- `min.compaction.lag.ms=60000` (1 min; gives bootstrapping consumers a chance to see recent updates)
- `delete.retention.ms=86400000` (24 h tombstone retention)
- `retention.ms=-1` (compaction, not time-based)

**Known limitations:**

- No horizon `metadataChanged` UEI. Metadata writes that don't also fire `nodeUpdated`/`nodeInfoChanged`/`nodeLabelChanged` are invisible until the next UEI or restart-bootstrap. `IMPORT_SUCCESSFUL_UEI` catch-all + restart-bootstrap cover most windows.
- Schema is **not frozen** during Phase 1. Breaking changes to `NodeContext` are permitted until the first consumer ships.
- Every provisiond restart republishes all nodes; log compaction absorbs the duplicates within 60 s. At 10k nodes this is ~20 MB of transient Kafka writes per restart.

### Related — `deltav-timeseries` retention default changed

Default changed from **7 days → 1 day** in Phase 1. Override via `DELTAV_TIMESERIES_RETENTION_DAYS`. Rationale: at production scale, 7-day retention for high-volume metric records consumes significantly more disk than necessary (≈40 GB steady state at 10k nodes vs ≈5.76 GB with 1-day).

## Phase 2 — Prometheus Remote Write consumer

The `prometheus-writer` service consumes `deltav-timeseries`, enriches each
batch with node identity from `deltav-node-context`, and POSTs Snappy-compressed
Prometheus Remote Write protobuf batches to a configurable endpoint.

### Profiles

- **active / passive / full** — daemon-only profiles (no TSDB). `prometheus-writer`
  is NOT started here — it has no remote-write target in these profiles. Run it
  under `metrics` or `demo`, which include VictoriaMetrics.
- **metrics** (alias: **metrics-e2e**) — adds a pinned `victoriametrics:v1.106.1`
  container, `vmagent`, `prometheus-writer`, and a Grafana service with a starter
  SNMP dashboard.
- **demo** — the canonical full-stack demo: `full` plus the entire observability
  pipeline (victoriametrics + vmagent + prometheus-writer + grafana + alertmanager
  + alerts-forwarder, with alarm-context metrics enabled). One command:
  `make up PROFILE=demo` (or `docker compose --profile demo up`). See the
  "Grafana access" section below.

### Configuration

Environment variables (also see `core/prometheus-writer/src/main/resources/application.yml`):

| Variable | Default | Purpose |
|---|---|---|
| `PROMETHEUS_WRITER_REMOTE_WRITE_URL` | `http://victoriametrics:8428/api/v1/write` | RW endpoint |
| `PROMETHEUS_WRITER_AUTH_TYPE` | `none` | `none` / `bearer` / `basic` |
| `PROMETHEUS_WRITER_BEARER_TOKEN` | (empty) | Bearer token when `AUTH_TYPE=bearer` |
| `PROMETHEUS_WRITER_BASIC_USER` | (empty) | Basic auth user |
| `PROMETHEUS_WRITER_BASIC_PASS` | (empty) | Basic auth pass |
| `SPRING_KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` (compose) | Kafka brokers |

Extra headers (e.g. `X-Scope-OrgID` for Mimir tenancy) via yaml
`prometheus-writer.remote-write.headers.{name}: "{value}"`.

Opt-in metadata-label promotion:

```yaml
prometheus-writer:
  labels:
    from-metadata:
      - requisition:region
      - snmp:sysLocation
```

### Metrics

All exposed at `/actuator/prometheus`, prefix `deltav_prometheus_writer_`.
See `docs/superpowers/specs/2026-04-17-kafka-ts-phase-2-prometheus-consumer-design.md` §7
for the full list.

Useful PromQL examples:

```
rate(deltav_prometheus_writer_samples_sent_total[5m])
rate(deltav_prometheus_writer_enrichment_missing_total[5m])
deltav_prometheus_writer_circuit_state         # 0=closed, 1=half_open, 2=open
deltav_node_context_cache_size
```

### Topics

- Consumes `deltav-timeseries` (group `prometheus-writer`)
- Consumes `deltav-node-context` (unique group per instance)
- Produces to `deltav-prometheus-writer-dlq` (poison-pill records only)

### Wire format frozen

As of Phase 2 GA, both `deltav-timeseries` and `deltav-node-context` protobuf
schemas are frozen. Only forward-compatible additions (new tag numbers) permitted.

## Grafana access (demo mode)

After `make up PROFILE=demo` (or `docker compose --profile demo up -d`), open
`http://localhost:13000/d/snmp-overview`. Anonymous Viewer access is on by
default (no login required) and the SNMP Overview dashboard is pre-provisioned
with VictoriaMetrics as the datasource.

The first poll cycle takes ~30 seconds; allow ~90 seconds after `up` for
panels to populate with data from the bundled mock SNMP agent.

To log in as admin (e.g. to create custom dashboards):

- Username: `admin`
- Password: `admin` (override via `GF_ADMIN_PASSWORD` environment variable).

## Troubleshooting

**Images not found:** Run `make images` to build all images. Verify with `docker images | grep opennms`.

**OOM kills (exit 137):** Increase Docker Desktop memory or use `make up PROFILE=active`.

**Service won't start:** Check logs: `make logs SVC=<service>`. Spring Boot daemons log to stdout. Check `/actuator/health` for health status.

**Database connection errors:** Ensure postgres is healthy before other services start. The compose healthchecks handle this, but initial schema creation takes time.

**Stale data after rebuild:** Run `make reset` to remove all volumes, then `make up PROFILE=full`.

## License

AGPL v3 — see [LICENSE.md](../LICENSE.md)
