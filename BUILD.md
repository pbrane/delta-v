# Building Delta-V

This guide covers building Delta-V from source and deploying locally.

## Prerequisites

| Requirement | Version | Notes |
|-------------|---------|-------|
| Java | JDK 21 | Temurin recommended. Auto-detected on macOS. |
| Docker Desktop | 4.x+ | **16 GB memory** required for full profile. |
| net-snmp | any | Optional — `snmptrap` needed for E2E tests. |

### Docker Desktop Memory

Open Docker Desktop → Settings → Resources and set Memory to **16 GB** (or higher).
The full profile runs 16 containers.

### Java Home

If `JAVA_HOME` is not set, `build.sh` auto-detects Temurin 21 on macOS:

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-21.jdk/Contents/Home
```

Or use `jenv` to manage Java versions.

## Quick Start

The fastest path from a clean checkout to a running system:

```bash
# Full build: compile all modules, then the JRE + layered daemon images
make build
make images

# Deploy all 16 services
make up PROFILE=full

# Check health (wait ~45s for startup)
make status
```

## Build Steps

### 1. Compile

Maven builds all 22 modules (parent + 21 under `core/`):

```bash
make build
```

Or directly with Maven:

```bash
mvn clean install -DskipTests    # ~16s incremental, ~45s clean
```

GroupIds are `org.deltav.core` for delta-v modules and `org.opennms.core` for the
horizon-derived `opennms-model-jakarta` module. Horizon dependencies are pulled from
the `pbrane/delta-v-horizon` GitHub Packages repository (version managed by
`deltav.horizon.version` in the root POM).

### 2. Build Docker Images

```bash
# Build every Delta-V image: JRE base (if missing) + daemon-base
# + 12 per-daemon + minion-boot + db-init + auxiliaries
make images
```

The layered image build:
1. `deltav/jre-deltav:21` — jlink custom JRE on Alpine 3.21 (22 modules, ~143MB)
2. `deltav/daemon-base` — shared libraries (~321 JARs deduped across 12 daemons, ~415MB)
3. `opennms/<daemon>` — per-daemon overlay (unique libs + thin app JAR)
4. `deltav/minion-boot` — Spring Boot 4 Minion fat JAR
5. `deltav/db-init` — one-shot Liquibase schema migration

### 3. Full Build (All Steps)

```bash
make build && make images   # compile all modules, then build every image
```

## Deployment

### Deploy Scripts

```bash
make up PROFILE=full     # Start all 16 services
make up PROFILE=active   # Core monitoring only
make status              # Check container health
make down                # Stop (preserve data)
make reset               # Stop and wipe all data
make logs SVC=trapd      # Tail logs for a specific service
```

### Compose Profiles

| Profile | Services |
|---------|----------|
| `lite` | postgres, kafka, db-init, minion, alarmd, pollerd, provisiond, discovery, bsmd |
| `passive` | + trapd, syslogd, eventtranslator |
| `full` | All 16: + collectd, enlinkd, perspectivepollerd, telemetryd, trapd, syslogd, eventtranslator |

### Verifying Health

```bash
make status
```

Expected: all containers show `(healthy)` except `db-init` (exits after schema migration).

## Rebuilding Individual Components

After the initial build, you rarely need to rebuild everything.

### Changed a daemon-boot module

Rebuild the module, rebuild the image, and redeploy:

```bash
make daemon-image DAEMON=alarmd          # rebuild just that daemon's JAR + image
make down && make up PROFILE=full
```

### Changed daemon-common (shared infrastructure)

All daemons depend on this — rebuild everything:

```bash
make build && make images
make down && make up PROFILE=full
```

### Changed opennms-model-jakarta

```bash
mvn -DskipTests -pl :org.opennms.core.model-jakarta install
make images
make down && make up PROFILE=full
```

### Changed Liquibase schema or db-init

```bash
mvn -DskipTests -pl :org.opennms.core.db-init package
make images
make reset    # Must wipe data for schema changes
make up PROFILE=full
```

## End-to-End Testing

```bash
make up PROFILE=full    # Must be running

# Individual suites (e2e scripts live in and run from deploy/)
cd deploy
./test-e2e.sh              # Core: trap → provision → alarm lifecycle
./test-minion-e2e.sh       # Minion: trap → Kafka Sink → alarm lifecycle
./test-minion-rpc-e2e.sh   # Minion RPC: provision → detect → poll
./test-syslog-e2e.sh       # Syslog: Cisco syslog → alarm lifecycle
./test-passive-e2e.sh      # Passive: syslog → EventTranslator → Twin API → outage
./test-collectd-e2e.sh     # Collectd: SNMP collection health
./test-perspective-e2e.sh  # Perspective: remote-location polling + outage lifecycle
./test-enlinkd-e2e.sh      # Enlinkd: LLDP topology via Containerlab cEOS
```

## Troubleshooting

### Container exits with code 137

Out-of-memory kill. Increase Docker Desktop memory to 16 GB.

### db-init fails with Liquibase error

```bash
docker logs delta-v-db-init-1
```

If a table/sequence doesn't exist, the changeset may need a
`<preConditions onFail="MARK_RAN">` guard.

### ClassNotFoundException at runtime

Check if the class was ported to `model-jakarta`. Some horizon utility classes
may still need porting. Check:

```bash
docker logs delta-v-<daemon> 2>&1 | grep -E "ClassNotFoundException|NoClassDefFoundError"
```

### Daemon fails to start

Spring Boot daemons log to stdout. Check container logs:

```bash
docker logs delta-v-alarmd
```

Common causes:
- Missing Kafka connectivity (check `delta-v-kafka-1` is healthy)
- PostgreSQL not ready (check `delta-v-postgres-1` is healthy)
- Stale `.m2` SNAPSHOT artifacts (run `mvn clean install -DskipTests`)

## Architecture Reference

Design documents are in `docs/plans/` and `docs/superpowers/`.
See [README.md](README.md) for the full architecture overview.
