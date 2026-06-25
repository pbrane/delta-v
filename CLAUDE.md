# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## CRITICAL: Git Remote Rules

**NEVER create pull requests against any `OpenNMS/*` repository.** This is a fork (`pbrane/delta-v`). Always use `--repo pbrane/delta-v` with `gh pr create`. The `gh` CLI defaults to the fork parent (`OpenNMS/opennms`) which is wrong.

```bash
# CORRECT
gh pr create --repo pbrane/delta-v --base develop ...

# WRONG — DO NOT DO THIS
gh pr create ...  # defaults to OpenNMS/opennms
```

## Delta-V Architecture & Conventions (CRITICAL — read before contributing)

Delta-V is a **microservice-mode** re-architecture of OpenNMS Horizon. The monolith and
Apache Karaf are **gone** — the platform runs as independent Spring Boot daemon services
orchestrated by docker-compose. Significant parts of the "legacy" guidance further down
(Karaf, JAXB config, ActiveMQ/JMS, the `bin/opennms` monolith, CircleCI, JIRA) describe
**upstream Horizon** and do **not** apply to delta-v code.

**Architecture invariants:**
- **No Karaf / OSGi.** Daemons are Spring Boot 4 microservices (`core/daemon-boot-*`), one
  image each. No bundles, feature files, or blueprint.
- **No events table, no Eventd, no in-JVM EventBus.** Events flow through Kafka; each daemon
  owns its own EventExpander.
- **Minion-mandatory I/O.** Daemons NEVER touch the network or poll locally — all device I/O
  goes through a Minion via Kafka RPC. gRPC is used **only** for the minion-gateway↔Minion
  channel (remote-Minion security); daemons stay on Kafka.
- **RPC timeouts must NEVER create outages or fault events** — an RPC failure is not a service
  being down.
- **No ActiveMQ, no Newts/Cassandra, no legacy webapp.** Messaging is Kafka; time-series is
  Prometheus/VictoriaMetrics (+ ClickHouse for flows); the JSP webapp is removed.
- **Database identity is `deltav`** (db name + role + password), not `opennms` — connect with
  `psql -U deltav -d deltav`. The `org.opennms.*` Java packages and `OPENNMS_DBINIT_*` /
  `SPRING_DATASOURCE_*` env-var / property KEYS keep their names; only the DB identity values
  are `deltav`.

**Code conventions:**
- **New delta-v code uses `org.deltav.*` packages** and its own copyright header — not `org.opennms.*`.
- **Config: Jackson `XmlMapper`, never JAXB / `JaxbUtils`** in Spring Boot daemons
  (with `defaultUseWrapper(false)` + `JaxbAnnotationModule` for `@XmlElementWrapper` models).
- **Conventional Commits** (`feat:`/`fix:`/`chore:`/`docs:`/`test:`) referencing GitHub issues —
  **not** JIRA `NMS-XXXXX`.
- **Branch + PR, never commit to `develop`.** `git pull develop` before branching; PRs target
  `--repo pbrane/delta-v --base develop`.
- Horizon code is consumed as **pre-built JARs** from `pbrane/delta-v-horizon`
  (`deltav.horizon.version`); fix shared bugs at the horizon source, not via delta-v exclusions.

## Project Overview

Delta-V (`pbrane/delta-v`) is a microservice-mode fork of OpenNMS Horizon, an enterprise-grade
open-source network monitoring platform, licensed under AGPL v3. The Maven reactor inherits
Horizon's `36.0.0-SNAPSHOT` version; the deployable Docker images are versioned on delta-v's own
`1.x` line. **Java 21 required** (`<java.version>21</java.version>` in root `pom.xml` and every
`core/*` module pom).

## Build Commands

Delta-V uses the bundled Maven wrapper (`./mvnw`) with a `make` front door. `make` is the single entry point for building and running.

```bash
# Compile + install all reactor modules (tests skipped)
make build            # = ./mvnw -DskipTests -B install

# Run all tests
make test

# Run a single test class
make test-class MODULE=:org.opennms.core.daemon-boot-pollerd TEST=SomePollerTest

# Build ALL Docker images (daemons + auxiliaries)
make images

# Rebuild a single daemon's boot JAR / image
make daemon DAEMON=provisiond            # boot JAR
make daemon-image DAEMON=provisiond      # Docker image

# Run the stack
make up PROFILE=full      # start (profiles: active|passive|full|demo)
make status               # service status
make logs SVC=trapd       # tail one service
make down                 # stop (preserve data)

# Preflight: verify the environment can build & run
make doctor               # JDK 21, Docker, GitHub Packages auth, image completeness

make help                 # list all targets
```

`build.sh`, `deploy.sh`, and `doctor.sh` (under `tools/`) are internal engines invoked by `make`; you normally don't call them directly. The legacy `compile.pl`/`assemble.pl` Perl wrappers from upstream Horizon do NOT exist in delta-v.

## Running Locally

Delta-V runs as containers via docker-compose, not the legacy `bin/opennms` monolith. Build
images and bring the stack up:

```bash
make images               # build all Docker images (or `make build` for JARs only)
make up PROFILE=full      # start the stack (profiles: active|passive|full|demo)
make status / make logs SVC=<svc> / make down
```

Postgres (the `deltav` database) and `db-init` (Liquibase schema migration) are part of the
compose stack — no manual `bin/install`/`opennms-datasources.xml` step. To recreate the DB from
scratch, `make down` with volume removal (`docker compose --profile <p> down -v`) then `make up`.

> The legacy single-binary run (`bin/runjava`/`bin/install`/`bin/opennms`, `opennms.conf`,
> `opennms-datasources.xml`) belongs to upstream Horizon and is not used in delta-v.

## Architecture

### Module Organization

Delta-V is a **slim reactor (~34 modules)** — the bulk of OpenNMS Horizon (the `opennms-*`,
`features/`, `container/`, `dependencies/` trees) lives in the separate `pbrane/delta-v-horizon`
repo and is consumed here as **pre-built JARs** (`deltav.horizon.version`). The delta-v-owned code:

- `core/` — every delta-v module:
  - `daemon-boot-*` (14) — the Spring Boot daemon apps: alarmd, bsmd, collectd, discovery,
    enlinkd, eventtranslator, perspectivepollerd, pollerd, provisiond, syslogd, telemetryd, trapd,
    plus `daemon-boot-minion` / `-minion-common`.
  - `daemon-common`, `daemon-registry`, `daemon-sink-kafka`, `dao-jpa-support` — shared daemon infra.
  - `minion-gateway`, `minion-grpc-contracts`, `deltav-kafka-contracts` — Minion gRPC ingress + contracts.
  - `opennms-model-jakarta` — `jakarta.persistence` entity model.
  - `db-init` — Liquibase schema migrator (runs as a container).
  - `flow-enricher`, `alarms-materializer`, `alarms-kafka-publisher`, `alerts-forwarder`,
    `node-context-consumer`, `event-forwarder-kafka`, `horizon-metric-bridge` — standalone
    Spring Cloud Stream / bridge services.
- `deploy/` — `compose.yml`, per-daemon `overlays/`, Dockerfiles, and the `test-*-e2e.sh` E2E scripts.
- `tools/` — `build.sh` / `deploy.sh` / `doctor.sh` (the engines invoked by `make`).
- `components/` — auxiliary image sources; `docs/` — architecture docs and plans.

### Runtime Architecture (Delta-V)

Delta-V runs as **independent Spring Boot daemon microservices**, not the Karaf-embedded
monolith. Each daemon (`core/daemon-boot-*`: alarmd, bsmd, collectd, discovery, enlinkd,
eventtranslator, perspectivepollerd, pollerd, provisiond, syslogd, telemetryd, trapd) is its
own Docker image with its own Spring context, datasource, and Kafka consumers. They are wired
together only by **Kafka** (events, RPC, sink, time-series) and **PostgreSQL** — there is no
shared in-process container.

Deployable images: the per-daemon services above + `minion-gateway` (gRPC ingress translator
for Minions) + Minion + `db-init` (Liquibase migration) + auxiliaries (clickhouse, grafana,
prometheus-writer, flow-enricher, mock-snmp-agent, nl6 simulator, etc.). Orchestrated by
`deploy/compose.yml` with profiles `active|passive|full|demo`.

> Karaf, OSGi bundles, `container/features/*.xml`, and the embedded webapp are upstream-Horizon
> constructs and are not part of delta-v's runtime. New code is a Spring `@Configuration`/`@Bean`
> in the relevant daemon, not an OSGi blueprint/feature.

### Key Technology Stack

Delta-V stack (where it diverges from upstream Horizon, the divergence is called out):

| Layer | Technology |
|-------|-----------|
| Language | Java 21 |
| Build | Maven wrapper (`./mvnw`), `make` front door |
| Daemon runtime | **Spring Boot 4** microservices (no Karaf/OSGi) |
| ORM | **Hibernate 7 / `jakarta.persistence`** (entities in `opennms-model-jakarta`) |
| Messaging | **Apache Kafka** (no ActiveMQ) — events, RPC, sink, time-series |
| Minion transport | Kafka, plus **gRPC** for minion-gateway↔Minion |
| Time-Series | **Prometheus / VictoriaMetrics** + **ClickHouse** (flows) — no Newts/RRD |
| Database | PostgreSQL 16 (db `deltav`), Liquibase schema via `db-init` |
| REST | Apache CXF / JAX-RS |
| Config serialization | **Jackson `XmlMapper`** for daemon config (not JAXB) |
| Frontend | Vue 3 + TypeScript + Vite + Pinia, Feather Design System |
| Serialization | Jackson, Protobuf, gRPC |
| Horizon dependency | pre-built JARs from `pbrane/delta-v-horizon` (`deltav.horizon.version`) |

### Frontend

This reactor is the backend microservices; there is **no `ui/` module here**. The Vue 3 SPA
(pnpm + Vite + Pinia + Feather Design System) lives in the upstream Horizon source
(`pbrane/delta-v-horizon`); observability in delta-v is primarily Grafana over
Prometheus/VictoriaMetrics + ClickHouse.

## Testing

- **delta-v modules:** JUnit 5 + Mockito + AssertJ; integration tests via **Testcontainers**
  (`@SpringBootTest @Testcontainers` against a real `postgres` container).
- **E2E:** `deploy/test-*-e2e.sh` drive the running compose stack (psql to the `deltav` DB).
  These are NOT a back-to-back suite — run them individually on a lean stack (some self-`down -v`).
- (Upstream horizon JARs still carry JUnit 4 / PowerMock; delta-v code does not.)

```bash
make test                                              # all reactor tests
make test-class MODULE=:org.opennms.core.daemon-boot-pollerd TEST=SomeTest
./mvnw -o -pl core/db-init test                        # one module's tests (incl. Testcontainers IT)
```

## Branching Model

- `develop` — default branch; all work merges here via PR.
- **Feature branches + PRs only — never commit directly to `develop`.** `git pull develop`
  before branching. PRs target `--repo pbrane/delta-v --base develop` (see the Git Remote rule).
- **Conventional Commits** referencing GitHub issues (e.g. `fix(db-init): … (#243)`).
- Release tags are on delta-v's own `v1.x` line (e.g. `v1.3.0`), not upstream's `opennms-XX.X.X`.

> Upstream's `release-XX.x` / `foundation-YYYY` branches and CI forward-merge do not apply here.

## Key Conventions

- Spring beans use **constructor injection** (not `@Autowired` field injection).
- Daemon config: **Jackson `XmlMapper`**, not JAXB (see invariants above).
- REST endpoints use **CXF / JAX-RS** annotations.
- New code registers via Spring `@Configuration`/`@Bean` (no Karaf blueprint / OSGi SCR).
- Config property keys are **daemon-scoped** (`deltav.<daemon>.<feature>.<knob>`); shared keys
  create coupling. Kafka topics and schemas are shared by design; feature flags are not.
- The Maven Enforcer Plugin bans some deps (e.g. `commons-logging` — use `slf4j-api`). Fix
  violations with `<exclusions>` + the approved alternative.

## CI/CD

**GitHub Actions** (`.github/workflows/`): `ci.yml` (build/test), `delta-v-build-images.yml`
(calls `make images` to build/publish the Docker images), `codeql-analysis.yml`, `labeler.yml`.

Daemons are Spring Boot apps — debug a failed boot from the container logs
(`make logs SVC=<daemon>` or CI artifacts), not Karaf's `karaf.log`/OSGi resolution errors.
