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

## Project Overview

OpenNMS Horizon is an enterprise-grade open-source network monitoring platform. Version 36.0.0-SNAPSHOT, licensed under AGPL v3. Java 21 required (`<java.version>21</java.version>` set in root `pom.xml` and every `core/*` module pom).

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

## Running Locally After Build

```bash
export ONMS_RELEASE=$(grep -m1 '<version>' pom.xml | sed 's/.*<version>\(.*\)<\/version>.*/\1/')
echo "RUNAS=$(id -u -n)" > "target/opennms-${ONMS_RELEASE}/etc/opennms.conf"
# Configure PostgreSQL in target/opennms-${ONMS_RELEASE}/etc/opennms-datasources.xml
./target/opennms-"${ONMS_RELEASE}"/bin/runjava -s
./target/opennms-"${ONMS_RELEASE}"/bin/install -dis
./target/opennms-"${ONMS_RELEASE}"/bin/opennms -vt start
```

A quick PostgreSQL for dev: `docker run -d -e POSTGRES_HOST_AUTH_METHOD=trust -p 5432:5432 postgres:16`

## Architecture

### Module Organization

The codebase has two structural patterns:

**Modern structure:**
- `core/` — Core platform (38 modules: api, cache, config, daemon, db, grpc, ipc, jmx, snmp, web, etc.)
- `features/` — 87+ feature modules (alarms, collection, discovery, events, flows, kafka, poller, provisioning, rest, telemetry, topology-map, vaadin UI components, etc.)
- `dependencies/` — Centralized dependency management (66 sub-modules)
- `container/` — Karaf OSGi container assembly and features
- `protocols/` — Protocol implementations (CIFS, NSClient, RADIUS, Selenium, XML)
- `integrations/` — External system integrations
- `tests/` — Shared test infrastructure (DAO tests, mock elements, mock SNMP agent)
- `ui/` — Modern Vue 3 SPA frontend

**Legacy structure (top-level `opennms-*` directories):**
- `opennms-model/` — Domain model
- `opennms-dao/`, `opennms-dao-api/` — Data access
- `opennms-config/`, `opennms-config-api/`, `opennms-config-model/`, `opennms-config-jaxb/` — Configuration
- `opennms-services/` — Core services
- `opennms-provision/` — Provisioning
- `opennms-webapp-rest/` — REST API
- `opennms-web-api/` — Web API layer
- `opennms-webapp/` — Legacy JSP webapp
- `opennms-full-assembly/` — Final Horizon assembly

### Runtime Architecture

OpenNMS embeds Apache Karaf (4.3.10) as an OSGi container. Karaf is embedded *above* the legacy webapp in the Spring context hierarchy, so the core is pre-initialized before Karaf extends it. New features should be written as OSGi bundles loaded via Karaf feature files.

**Karaf feature files** are in `container/features/src/main/resources/`:
- `features.xml` — Main features
- `features-core.xml` — Core/third-party base features
- `features-minion.xml` — Minion features
- `features-sentinel.xml` — Sentinel features

**Three deployable artifacts:** Horizon (core), Minion (distributed data collection), Sentinel (high-availability event processing).

### Key Technology Stack

| Layer | Technology |
|-------|-----------|
| Language | Java 21 |
| Build | Maven wrapper (`./mvnw`), `make` front door |
| OSGi Container | Apache Karaf 4.3.10 |
| Web Framework | Spring 4.2.x (OpenNMS-patched fork), Spring Security 4.2.x (patched) |
| ORM | Hibernate 3.6.11 (OpenNMS build) |
| REST | Apache CXF 3.6.8 |
| Messaging | Apache ActiveMQ 5.16.8, Apache Kafka 3.6.2 |
| Integration | Apache Camel 2.21.5 |
| Time-Series | Newts 3.0.0 (Cassandra-backed), RRDtool via JRRD2 |
| Servlet Container | Jetty 9.4.x (embedded) |
| Database | PostgreSQL (Liquibase 3.6.3 for schema) |
| Frontend | Vue 3 + TypeScript + Vite + Pinia, Feather Design System |
| Serialization | Jackson 2.16.2, Protobuf 3.25.5, JAXB 2.3.3, gRPC 1.75.0 |

### Frontend (ui/)

The modern UI is a Vue 3 SPA in `ui/` built with:
- **Package manager:** pnpm (enforced, version 10.24.0)
- **Build tool:** Vite
- **Component library:** Feather Design System
- **State:** Pinia
- **Visualization:** D3, Chart.js, Leaflet
- **Tests:** Vitest + Vue Test Utils + Happy-DOM

The UI also has a `menu/` sub-build that provides embeddable Vue components for legacy JSP pages. Build output goes to `src/main/dist/` and `src/menu/dist-menu/`.

## Testing

- **Unit tests:** JUnit 4 (primary) + JUnit 5 (with Vintage engine for compatibility)
- **Mocking:** Mockito 3.12.4, PowerMock 2.0.9
- **BDD:** Spock 2.3 (Groovy)
- **Integration tests:** Testcontainers 1.19.7, Maven Failsafe plugin
- **Coverage:** JaCoCo 0.8.9
- **UI tests:** Vitest

Run all tests for a module:
```bash
./mvnw --projects :opennms-dao -am verify
```

Run integration tests:
```bash
# Integration tests (Failsafe) for a module
./mvnw --projects :opennms-dao -am failsafe:integration-test failsafe:verify
```

## Branching Model

- `develop` — next major release (default branch)
- `release-XX.x` — Horizon release branches
- `foundation-YYYY` — foundation branches for Meridian
- CI auto-merges forward: `foundation-YYYY` → `release-XX.x` → `develop`
- Tags: `opennms-XX.X.X-1` for Horizon releases

## Key Conventions

- Spring beans use **constructor injection** (not `@Autowired` field injection)
- Configuration uses **JAXB** for XML serialization of config model objects
- REST endpoints use **CXF/JAX-RS** annotations
- OSGi services registered via **Karaf blueprint** or **SCR annotations**
- The Maven Enforcer Plugin bans certain dependencies (e.g., `commons-logging` — use `slf4j-api` instead). Fix violations by adding `<exclusions>` and using the approved alternative
- Commit messages should reference JIRA issues: `NMS-XXXXX: description`

## CI/CD

CircleCI with dynamic configuration. Path-based filtering determines which jobs run:
- `ui/.*` triggers UI build
- `docs/.*` triggers docs build
- Source changes trigger full build

Smoke tests run in containers. Debug Karaf failures by checking `karaf.log` in CI artifacts and searching for "exception" — read OSGi resolution errors backwards from the end of "Unable to resolve root" lines.
