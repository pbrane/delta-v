# Next Session: Fix EventBuilder ClassNotFoundException in JPA Daemons

> Copy everything below the line into the next Claude Code conversation.

---

## Context

Branch `feature/minion-boot4-clean`. Minion trap listener fixed (PR #75 — constructor injection, port opens synchronously). E2E tests pass for trap pipeline (40/40 across test-e2e, test-minion-e2e, test-syslog-e2e). But 6 daemons crash at startup due to model-jakarta migration fallout.

## What's Working

- **Trapd** — healthy, processes traps from Minion via Kafka Sink
- **Alarmd** — healthy, creates/clears alarms
- **Provisiond** — healthy, provisions nodes from newSuspect events
- **EventTranslator** — healthy, translates trap events
- **Syslogd** — healthy, processes syslog via Minion
- **Discovery** — healthy
- **Minion** — healthy, `Listening on [all interfaces]:1162`, forwards traps/syslog via Kafka

## Crashed Daemons (6 of 12)

### Group 1: `ClassNotFoundException: EventBuilder` (5 daemons)

**Affected:** pollerd, collectd, enlinkd, bsmd, perspectivepollerd

**Error:**
```
Caused by: java.lang.ClassNotFoundException: org.opennms.netmgt.model.events.EventBuilder
    at org.hibernate.boot.registry.classloading.internal.ClassLoaderServiceImpl.classForName
```

**Root cause:** Hibernate entity scanning scans `org.opennms.netmgt.model` packages to discover `@Entity` classes. But `EventBuilder` lives in that package and was moved from `opennms-model` to `opennms-model-api` during the model split (commit `72ff449edbd`). The daemon fat JARs include `opennms-model-jakarta` (which has the JPA entities) and `opennms-model-api` (which has `EventBuilder`), but Hibernate's `ClassLoaderServiceImpl.classForName()` can't find `EventBuilder` because it's scanning the wrong JAR's class index.

**Hypothesis:** The Spring Boot fat JAR repackaging changes the classloader behavior. Hibernate's entity scanning may use a classloader that sees classes listed in one JAR's package index but can't actually load them from a different JAR. This is the classic **split-package problem** with Spring Boot's nested JAR classloader.

### Group 2: `UnknownEntityException: OnmsDistPoller` (1 daemon)

**Affected:** telemetryd

**Error:**
```
Caused by: org.hibernate.query.sqm.UnknownEntityException: Could not resolve root entity 'OnmsDistPoller'
```

**Root cause:** Telemetryd's Hibernate session doesn't have `OnmsDistPoller` mapped as an entity. Either the entity scan isn't covering the package where `OnmsDistPoller` lives, or `OnmsDistPoller` wasn't included in the model-jakarta entity set.

## Investigation Plan

### For Group 1 (EventBuilder ClassNotFoundException):

1. **Check entity scan config** — What `@EntityScan` or `packagesToScan` config do these daemon boot modules use? The entity scan should only cover packages that contain `@Entity` classes, NOT utility classes like `EventBuilder`.

2. **Check if EventBuilder is in the right JAR** — Run `jar tf` on the fat JAR to see where `EventBuilder.class` actually is:
   ```bash
   jar tf core/daemon-boot-pollerd/target/*.jar | grep EventBuilder
   ```

3. **Narrow the entity scan** — If the scan covers `org.opennms.netmgt.model` (broad), narrow it to `org.opennms.netmgt.model` subpackages that ONLY contain entities. Or exclude non-entity classes.

4. **Alternative: move EventBuilder out of model packages** — If `EventBuilder` is in `org.opennms.netmgt.model.events`, and entities are also in that package, move `EventBuilder` to a non-entity package (e.g., `org.opennms.netmgt.events`). But this has wider impact.

### For Group 2 (OnmsDistPoller not mapped):

1. **Check if OnmsDistPoller was ported to model-jakarta** — Was it in the 12-entity port?
   ```bash
   grep -r "OnmsDistPoller" core/opennms-model-jakarta/
   ```

2. **Check Telemetryd's entity scan** — Does it include the package where `OnmsDistPoller` lives?

### Key Files

- `core/daemon-common/src/main/java/org/opennms/core/daemon/common/DaemonJpaConfiguration.java` (or similar — the shared JPA config for daemon boot modules)
- `core/opennms-model-api/` — where EventBuilder was moved
- `core/opennms-model-jakarta/` — the ported JPA entities
- `opennms-model/src/main/java/org/opennms/netmgt/model/events/EventBuilder.java` — original location

## E2E Test Status

| Test | Result | Notes |
|------|--------|-------|
| test-e2e.sh | **12/12 PASS** | Direct trap pipeline |
| test-minion-e2e.sh | **13/13 PASS** | Minion trap forwarding (fixed this session) |
| test-syslog-e2e.sh | **15/15 PASS** | Syslog via Minion |
| test-passive-e2e.sh | BLOCKED | Needs pollerd |
| test-collectd-e2e.sh | BLOCKED | Needs collectd |
| test-enlinkd-e2e.sh | BLOCKED | Needs enlinkd + labbox |
