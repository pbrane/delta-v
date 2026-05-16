# Gemini CLI Context: OpenNMS Delta-V

This project is **Delta-V**, a microservice decomposition of [OpenNMS Horizon](https://www.opennms.com/). It transforms the monolithic Java application into independently deployable, Kafka-connected containers, systematically removing Apache Karaf/OSGi in favor of Spring Boot 4.

## Project Overview

*   **Architecture**: Transitioning from a Karaf/OSGi monolith to standalone **Spring Boot 4** microservices.
*   **Main Technologies**:
    *   **Microservices**: Java 21, Spring Boot 4.0.3, Hibernate 7, Jakarta Persistence.
    *   **Messaging**: Apache Kafka (Event transport and RPC).
    *   **Database**: PostgreSQL (Alarms only; Events are Kafka-only).
    *   **Runtime**: jlink custom JRE on Alpine Linux 3.21.
    *   **Legacy Core**: Java 17, Karaf 4.4.9, Spring 4.2.x, Hibernate 3.6.11 (phasing out).
*   **Key Design Goal**: Eliminate the events table from PostgreSQL (Events → Kafka, Alarms → DB) and remove ActiveMQ in favor of Kafka.

## Directory Structure & Key Modules

*   `core/daemon-boot-*`: Spring Boot 4 microservice implementations (e.g., `daemon-boot-alarmd`, `daemon-boot-pollerd`).
*   `core/db-init`: One-shot Liquibase schema migration service (~312 MB).
*   `core/daemon-common`: Shared infrastructure for Spring Boot daemons (Kafka transport, RPC, enrichment).
*   `opennms-model-jakarta`: Modern Jakarta Persistence entities and DAOs for Hibernate 7.
*   `opennms-container/delta-v`: Main directory for Docker-based deployment and orchestration.
*   `bin/`: Perl wrapper scripts for the build system.
*   `docs/plans/`: Detailed architectural design and migration documents.

## Building and Running

### Prerequisites
*   **JDK 21** (Required for Spring Boot daemons and `db-init`).
*   **JDK 17** (Required for legacy Karaf-based modules).
*   **Docker Desktop**: 16 GB+ memory allocated.
*   **Perl**: Required for wrapper scripts.

### Build Commands
Builds are managed via Perl wrappers and a main shell script:

```bash
# Full build (compile, assemble, images)
cd opennms-container/delta-v
./build.sh

# Compile individual module (example)
./compile.pl -DskipTests --projects :org.opennms.core.daemon-boot-alarmd -am install

# Build Docker images only
cd opennms-container/delta-v
./build.sh images
```

### Running Locally
Use Docker Compose with profiles in `opennms-container/delta-v`:

```bash
cd opennms-container/delta-v
COMPOSE_PROFILES=lite docker compose up -d    # Minimal monitoring
COMPOSE_PROFILES=full docker compose up -d    # All 15+ services
```

### Testing
```bash
cd opennms-container/delta-v
./test-e2e.sh           # Trap -> Provision -> Alarm lifecycle
./test-minion-e2e.sh    # Minion-to-Core pipeline
./test-syslog-e2e.sh    # Syslog pipeline
```

## Development Conventions

*   **Spring Patterns**: Strictly use **constructor injection**; NEVER use `@Autowired` on fields.
*   **Persistence**: Use `opennms-model-jakarta` and Hibernate 7 for new services. Legacy `opennms-model` (javax.persistence) is incompatible with the new stack.
*   **IPC**: All inter-service communication must use Kafka topics (`deltav-fault-events`, `deltav-ipc-events`).
*   **Git Rules**: Never create PRs against `OpenNMS/*`. Use `--repo pbrane/delta-v` for all `gh pr create` commands.
*   **Coding Style**: Adhere to existing patterns in `daemon-boot-*` modules. Exclude ServiceMix Spring 4.2.x bundles in POMs for Boot 4 modules.

## Key Documentation
*   `README.md`: High-level architecture and service status.
*   `BUILD.md`: Detailed build instructions and troubleshooting.
*   `CLAUDE.md`: Tooling and Git remote rules.
*   `DELTA-V_Status.md`: Migration progress tracking.
*   `docs/plans/`: Historical context for all architectural decisions.
