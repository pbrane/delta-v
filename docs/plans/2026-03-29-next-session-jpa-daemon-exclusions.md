# Next Session: Exclude opennms-model from JPA Daemon-Boot Modules

> Copy everything below the line into the next Claude Code conversation.

---

## Context

Branch `feature/minion-boot4-clean` now has the `core/opennms-model-api` module (commit `72ff449edbd`). This persistence-free module provides `EventBuilder`, `NodeLabelChangedEventBuilder`, `ResourceTypeUtils`, and 17 enums/interfaces/value types previously only available from `opennms-model`.

**BLOCKER:** JPA daemon-boot modules (alarmd, provisiond, pollerd, etc.) still have `opennms-model` on their runtime classpath. Hibernate 7 finds `javax.persistence`-annotated entities from `opennms-model` alongside `jakarta.persistence`-annotated entities from `model-jakarta`, causing `UnknownEntityException` at runtime. Confirmed in E2E: `Could not resolve root entity 'RequisitionedCategoryAssociation'`.

## The Problem

`opennms-model` enters every daemon-boot module through two universal paths:
1. **`daemon-common`** → `opennms-model` (all 12 modules)
2. **`model-jakarta`** → `opennms-model` (9 of 12 modules)

Plus module-specific transitive paths through `opennms-dao-api`, `opennms-config`, feature impls, etc.

Three modules also have **direct** `opennms-model` dependencies: `discovery`, `syslogd`, `trapd`.

## What Needs to Happen

### Phase 1: Port Missing Entities to model-jakarta

12 entities exist in `opennms-model` but NOT in `model-jakarta`. Any entity referenced at runtime by a JPA daemon must be ported:

| Entity | Used by provisiond? | Used by alarmd? | Priority |
|--------|-------------------|-----------------|----------|
| `RequisitionedCategoryAssociation` | YES (caused E2E failure) | no | P0 |
| `OnmsAssetRecord` | YES (node scan) | no | P0 |
| `OnmsPathOutage` | YES (path outage) | no | P1 |
| `OnmsAcknowledgment` | no | YES | P1 |
| `EventConfEvent` | maybe (eventconf) | maybe | P1 |
| `EventConfSource` | maybe (eventconf) | maybe | P1 |
| `OnmsFilterFavorite` | no | no | P2 |
| `ResourceReference` | no | no | P2 |
| `OnmsHwEntity` | provisiond only | no | P2 |
| `OnmsHwEntityAlias` | provisiond only | no | P2 |
| `OnmsHwEntityAttribute` | provisiond only | no | P2 |
| `HwEntityAttributeType` | provisiond only | no | P2 |

### Phase 2: Add opennms-model Exclusions to JPA Daemon-Boot Modules

Follow the pattern established in `daemon-boot-minion-common/pom.xml` which has 8 dependency blocks with `<exclusion><groupId>org.opennms</groupId><artifactId>opennms-model</artifactId></exclusion>`.

For each JPA daemon-boot module:
1. Add `<exclusion>` for `opennms-model` on every dependency that transitively pulls it in
2. Remove any direct `<dependency>` on `opennms-model` (discovery, syslogd, trapd)
3. Ensure `model-api` is available (comes transitively through `opennms-model` dep chain — once excluded, add explicit `model-api` dependency)
4. Add `javax.persistence-api:2.2` at `runtime` scope (safety net for any remaining javax-annotated classes on classpath)

### Phase 3: Validate

1. Build all daemon-boot modules
2. Verify `opennms-model` JAR is NOT in any daemon-boot fat JAR: `unzip -l target/*.jar | grep opennms-model`
3. Rebuild Docker images: `cd opennms-container/delta-v && ./build.sh deltav`
4. Deploy: `./deploy.sh up passive`
5. Run E2E: `./test-e2e.sh --pre-clean`

## Reference: Exclusion Paths per Module

| Module | Deps needing exclusion |
|--------|----------------------|
| alarmd | daemon-common, model-jakarta, opennms-alarmd, opennms-alarm-api |
| bsmd | daemon-common, model-jakarta, bsm.daemon, opennms-alarmd |
| collectd | daemon-common, model-jakarta, collection.impl, opennms-config, opennms-dao-api, snmp-collector |
| discovery | DIRECT dep, daemon-common, features.discovery, opennms-provision-api, opennms-config-api, opennms-dao-api |
| enlinkd | daemon-common, model-jakarta, enlinkd.service.impl, enlinkd.persistence.api, opennms-config, opennms-dao-api |
| eventtranslator | daemon-common, event-translator |
| perspectivepollerd | daemon-common, model-jakarta, perspectivepoller, opennms-config |
| pollerd | daemon-common, model-jakarta, poller.impl, opennms-config |
| provisiond | daemon-common, model-jakarta, opennms-provisiond, provision-persistence, opennms-provision-api |
| syslogd | DIRECT dep, daemon-common, events.syslog, opennms-provision-api, opennms-config, opennms-dao-api |
| telemetryd | daemon-common, model-jakarta, opennms-dao-api |
| trapd | DIRECT dep, daemon-common, opennms-config-api, opennms-config, opennms-dao-api |

## Approach

Start with **alarmd** (simplest — fewest exclusion paths, only 4 deps to update). Get it passing E2E, then replicate the pattern to the other 11 modules.

## Key Constraints

- PR against `pbrane/delta-v` (NEVER `OpenNMS/opennms`)
- Rebuild all 12 daemon-boot JARs before `build.sh deltav`
- Run `--pre-clean` on E2E tests
- `model-jakarta` entities reference `AddEventVisitor`/`DeleteEventVisitor` from `opennms-model` — these must stay accessible at compile time but the classes are only invoked in the provisiond code path

## E2E Baseline (2026-03-29, post model-api extraction)

| Test | Result | Notes |
|------|--------|-------|
| Docker image build | PASS | All 15+ images built |
| All services healthy | PASS | 10/10 containers healthy in <5s |
| test-e2e.sh | 4/5 | nodeScanCompleted timeout (Hibernate UnknownEntityException) |
| test-minion-e2e.sh | 2/3 | Trap forwarding issue (separate from model split) |
