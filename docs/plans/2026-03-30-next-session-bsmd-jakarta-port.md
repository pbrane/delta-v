# Next Session: Port BSM Entities to Jakarta Persistence

> Copy everything below the line into the next Claude Code conversation.

---

## Context

Branch `feature/minion-boot4-clean`. All 12 Spring Boot daemons now start except BSMd. The previous session fixed 6 crashing daemons:

1. **Group 1 (5 daemons)**: pollerd, collectd, enlinkd, perspectivepollerd, telemetryd — crashed with `ClassNotFoundException: EventBuilder`. Root cause: stale Docker images with old model-api JAR missing `events/` package. Fixed by rebuilding.
2. **Group 2 (1 daemon)**: telemetryd — crashed with `UnknownEntityException: OnmsDistPoller`. Root cause: stale Docker images without priority classpath layer — legacy opennms-model won split-package race. Fixed by rebuilding.
3. **Compilation fixes**: Promoted `opennms-model` from `runtime` to compile scope in all 13 daemon boot POMs. Transitive types (Acknowledgeable, OnmsEntity, EventConfEvent) were needed at compile time.

## What's Working (11 of 12)

- **Alarmd** — healthy
- **Pollerd** — healthy (FIXED this session)
- **Collectd** — healthy (FIXED this session)
- **Enlinkd** — healthy (FIXED this session)
- **PerspectivePollerd** — healthy (FIXED this session)
- **Telemetryd** — healthy (FIXED this session)
- **Provisiond** — healthy
- **Trapd** — healthy
- **Syslogd** — healthy
- **EventTranslator** — healthy
- **Discovery** — healthy
- **Minion** — healthy

## BSMd Failure

**Error:**
```
Not an entity: org.opennms.netmgt.bsm.persistence.api.BusinessServiceEntity
```

**Root cause:** BSM entity classes in `features/bsm/persistence/api/` use `javax.persistence` annotations. Hibernate 7 (Spring Boot 4) only recognizes `jakarta.persistence` annotations. The BSM entities weren't ported during the model-jakarta migration because they're in a separate module.

**Affected entity classes** (all in `features/bsm/persistence/api/src/main/java/org/opennms/netmgt/bsm/persistence/api/`):
- BusinessServiceEntity
- BusinessServiceEdgeEntity
- BusinessServiceChildEdgeEntity
- IPServiceEdgeEntity
- ApplicationEdgeEntity
- SingleReductionKeyEdgeEntity
- Various reduction/map function entities

## Fix Plan

1. **Port BSM entities to jakarta.persistence** — Change all `import javax.persistence.*` to `import jakarta.persistence.*` in `features/bsm/persistence/api/`
2. **Verify the entity list** — The `BsmdConfiguration.java` already lists these entities in `PersistenceManagedTypes`
3. **Rebuild & test** — Rebuild BSMd boot JAR, rebuild Docker images, verify BSMd starts

## Key Files

- `features/bsm/persistence/api/src/main/java/org/opennms/netmgt/bsm/persistence/api/` — BSM entity classes (need jakarta port)
- `core/daemon-boot-bsmd/src/main/java/org/opennms/netmgt/bsm/boot/BsmdConfiguration.java` — BSMd JPA config (already correct)

## E2E Test Status

| Test | Result | Notes |
|------|--------|-------|
| test-e2e.sh | **PASS** | Direct trap pipeline |
| test-minion-e2e.sh | **PASS** | Minion trap forwarding |
| test-syslog-e2e.sh | **PASS** | Syslog via Minion |
| test-passive-e2e.sh | READY | Needs pollerd (NOW FIXED) |
| test-collectd-e2e.sh | READY | Needs collectd (NOW FIXED) |
| test-enlinkd-e2e.sh | READY | Needs enlinkd + labbox |
