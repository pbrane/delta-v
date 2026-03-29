# Next Session: Complete Minion Boot4 E2E Validation

> Copy everything below the line into the next Claude Code conversation.

---

## Context

The Minion-to-Spring-Boot-4 migration code is complete on branch `feature/minion-boot4-clean` (1 squashed commit from develop). The Minion itself starts in 2.4 seconds, is healthy, and passed 12/12 on `test-e2e.sh` with the old daemon images.

**BLOCKER:** E2E tests can't pass with a full clean build because of a **model split-package problem** in the existing 12 daemon-boot modules. This is NOT caused by the Minion migration — it's a latent issue from the model consolidation (PR #73) that surfaces when all daemon images are rebuilt from scratch.

## The Split-Package Problem

`opennms-model` (javax.persistence) and `model-jakarta` (jakarta.persistence) share the same package: `org.opennms.netmgt.model`. This creates two impossible constraints:

1. **JPA daemons need `opennms-model` at runtime** because `model-jakarta`'s `OnmsNode` imports `EventBuilder` from `opennms-model`. Without it: `NoClassDefFoundError: EventBuilder`.

2. **JPA daemons can't have `opennms-model` at runtime** because Hibernate 7 loads the `javax.persistence`-annotated `OnmsDistPoller` from `opennms-model` instead of the `jakarta.persistence`-annotated one from `model-jakarta`. Result: `UnknownEntityException: OnmsDistPoller`.

3. **Non-JPA daemons need `opennms-model` at runtime** for `ResourceTypeUtils` (referenced by `CollectionGroup$Rrd` during eventconf XML loading via `EventConfEnrichmentService`).

The old daemon images worked because they were built BEFORE the model consolidation PR's `exclude opennms-model` changes disrupted the transitive dependency chains.

## What Needs to Happen (Before E2E)

### Option A: Port remaining opennms-model references to model-api or model-jakarta (Recommended)

Move these classes out of `opennms-model` so JPA daemons don't need it:
- `EventBuilder` + `NodeLabelChangedEventBuilder` → `model-api` (builders with no persistence annotations)
- `ResourceTypeUtils` → `model-api` (depends on `RrdRepository` from `opennms-rrd-api`)

After this, JPA daemons only need `model-jakarta` + `model-api`, and non-JPA daemons can use `model-api` + `opennms-model` without conflict.

### Option B: Use Spring Boot's classpath ordering

Keep `opennms-model` at runtime but ensure `model-jakarta` JARs are loaded FIRST. Fragile.

### Option C: Exclude specific entity classes from opennms-model via shade plugin

Surgically remove duplicate entity classes from `opennms-model`. Complex.

## Minion-Specific Status

The Minion Boot4 code on `feature/minion-boot4-clean` is complete:

| Component | Status |
|-----------|--------|
| `daemon-boot-minion-common` (IPC wiring) | Done, 13/13 tests |
| `daemon-boot-minion` (application) | Done, compiles, boots in 2.4s |
| Protocol subsystems (10 configs) | Done, all `@ConditionalOnProperty` |
| Actuator endpoints | Done |
| Dockerfile + entrypoint | Done, 672MB image |
| docker-compose.yml | Updated |
| Minion health | Healthy |

## Key Lessons Learned

1. **Never add `opennms-model` (javax.persistence) to JPA daemons** — Hibernate 7 loads wrong entity annotations
2. **`model-jakarta` entities still reference `opennms-model` utility classes** (EventBuilder) — must port to model-api
3. **`ResourceTypeUtils`** needed at runtime by EventConfEnrichmentService → CollectionGroup$Rrd clinit — must move to model-api
4. **Alpine base image** (`opennms/jre-deltav:21`): `apk` not `microdnf`, `adduser` not `useradd`, `#!/bin/sh` not `#!/bin/bash`, `setcap` breaks `libjli.so` (use Docker `cap_add: NET_RAW`)
5. **Camel 2.21.5** incompatible with Spring 7 — use `SyslogReceiverJavaNetImpl`
6. **`HeartbeatProducer`** uses `FrameworkUtil.getBundle()` — NPEs outside OSGi; use inline timer
7. **ServiceMix Spring exclusions** must be on EVERY OpenNMS dependency including `opennms-model`
8. **All Jackson 2.x** pinned to 2.19.4 (11 artifacts: core, annotations, databind, dataformat-yaml/xml/csv, datatype-jsr310/jdk8/json-org, module-parameter-names/scala)

## E2E Baseline (2026-03-29)

| Test | Passed |
|------|--------|
| test-collectd-e2e.sh | 4/4 |
| test-minion-e2e.sh | 13/13 |
| test-syslog-e2e.sh | 15/15 |
| test-passive-e2e.sh | 16/16 |
| test-enlinkd-e2e.sh | 19/20 (1 flaky cEOS SNMP timeout) |
| test-e2e.sh | 13/13 |

## Key Constraints

- PR against `pbrane/delta-v` (NEVER `OpenNMS/opennms`)
- Rebuild all 12 daemon-boot JARs before `build.sh deltav`
- Run `--pre-clean` on E2E tests
