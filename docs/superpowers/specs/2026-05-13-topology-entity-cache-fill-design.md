# TopologyEntityCache Fill — Design

**Status:** Draft for review
**Audit item:** #4 from `project_daemon_nullop_audit_findings` (TopologyEntityCache no-op in Enlinkd)
**Decision:** Wire a real cache backed by a JPA DAO (Option B). Goal: be ready when a UI/API consumer of `OnmsTopologyDao` lands in v1.3.
**Author:** dhustace
**Date:** 2026-05-13
**Target release:** v1.2.0 (rc2.x or later)

---

## 1. Goal

Replace the anonymous no-op `TopologyEntityCache` bean in `EnlinkdDaemonConfiguration` with a real implementation that:

1. Queries the seven topology-related tables already populated by enlinkd discovery (nodes + lldp/cdp/ospf/isis/bridge/snmp link tables) via JPA.
2. Wraps those reads in a Guava `LoadingCache` with a 300-second TTL (configurable).
3. Honors `feedback_deltav_package_namespace` (new code lives under `org.deltav.*`).
4. Honors `feedback_no_shared_config_across_daemons` (config knob is `deltav.enlinkd.topology-cache.duration-seconds`, not the horizon `org.opennms.ui.*` system property).

The topology services and updaters already wired in `EnlinkdDaemonConfiguration` will then begin populating `OnmsTopologyDao` with real graph data. No UI consumer of that graph exists in delta-v today; the work is groundwork for v1.3 (Vue topology view or K8s operator topology API).

## 2. Why now / Why not just stay no-op

The audit identified this as "Unclear — needs user judgment" because the original framing ("in-memory vs persist topology across restarts") was wrong: the link data IS already persisted. The actual choice is between:

- **Stay no-op** (Option A): Topology services build an empty in-memory graph; safe because no consumer reads it, but the wiring is misleading and the next person to need a topology graph will have to do this work cold.
- **Fill the cache** (Option B, this spec): When the v1.3 topology surface lands, the wiring is already verified and there's a regression-tested path from DB → cache → services → `OnmsTopologyDao`.

Picking B trades a small PR now against a larger debugging session later. The DAO reimplementation is mechanical (HQL → JPQL) and the cache class is a straight port from horizon. Risk surface is contained.

## 3. Architectural snag (why we can't just use horizon's impl)

Published horizon `enlinkd.persistence.impl-1.0.11.jar` ships:

```
org.opennms.netmgt.enlinkd.persistence.impl.TopologyEntityDaoHibernate
    extends org.springframework.orm.hibernate3.support.HibernateDaoSupport
```

The `spring.orm.hibernate3` package was removed in Spring 5. Delta-v runs Spring 7 (via Spring Boot 4.0.3), so this class cannot classload. Pulling `enlinkd.persistence.impl` as a dependency would require excluding its transitive Spring-3-era jars, and the bundled `META-INF/opennms/component-dao.xml` blueprint references the broken class (irrelevant at runtime since delta-v doesn't process Karaf blueprints, but adds noise).

**Decision:** Do not depend on `enlinkd.persistence.impl`. Implement both DAO and cache fresh in delta-v.

## 4. Components

### 4.1 New: `TopologyEntityDaoJpa` (delta-v)

**Location:** `core/opennms-model-jakarta/src/main/java/org/opennms/netmgt/enlinkd/persistence/impl/TopologyEntityDaoJpa.java`

(Stays in the `org.opennms.netmgt.enlinkd.persistence.impl` package to match the existing `LldpLinkDaoJpa`, `CdpLinkDaoJpa`, etc. — these classes implement horizon interfaces and live in the horizon namespace by convention in this module. See note in §4.4 on namespace policy.)

**Shape:**

```java
@Repository
@Transactional(readOnly = true)
public class TopologyEntityDaoJpa implements TopologyEntityDao {

    @PersistenceContext
    private EntityManager em;

    @Override
    public List<NodeTopologyEntity> getNodeTopologyEntities() {
        return em.createQuery(
            "select new org.opennms.netmgt.enlinkd.model.NodeTopologyEntity("
                + "n.id, n.type, n.sysObjectId, n.label, n.location) "
                + "from org.opennms.netmgt.model.OnmsNode n",
            NodeTopologyEntity.class).getResultList();
    }

    // ... 10 more methods, one per topology projection ...
}
```

**Source for the 11 queries:** straight port of HQL from `delta-v-horizon/features/enlinkd/persistence/impl/.../TopologyEntityDaoHibernate.java`. The HQL uses standard JPQL constructor projection (`select new FQCN(...) from Entity`), which is JPA-spec-supported. No Hibernate-specific syntax in the existing queries.

**Why not extend `AbstractDaoJpa<E, ID>`:** that base class is for CRUD on a single managed entity. These queries return projection POJOs (non-entity classes), so the base class doesn't fit.

**Note on `OnmsMonitoringLocation`:** The query for `NodeTopologyEntity` references `n.location`, which is an `OnmsMonitoringLocation`. The jakarta-ported `OnmsMonitoringLocation.class` must win in classpath order — already enforced by daemon-boot-enlinkd's pom listing `model-jakarta` first (see existing comment at `core/daemon-boot-enlinkd/pom.xml:24-26`).

### 4.2 New: `TopologyEntityCacheImpl` (delta-v port)

**Location:** `core/daemon-boot-enlinkd/src/main/java/org/deltav/netmgt/enlinkd/persistence/cache/TopologyEntityCacheImpl.java`

**Shape:** straight port of `delta-v-horizon/.../TopologyEntityCacheImpl.java` (~180 lines), with two changes:

1. **Package:** `org.deltav.netmgt.enlinkd.persistence.cache` (per `feedback_deltav_package_namespace`).
2. **Config knob:** read TTL from `deltav.enlinkd.topology-cache.duration-seconds` (Spring `@Value` injection with default `300`), not from the horizon `SystemProperties.getInteger("org.opennms.ui.topology-entity-cache-duration")` global lookup.

**Interface:** implements horizon's published `org.opennms.netmgt.enlinkd.persistence.api.TopologyEntityCache` (so the 7 topology service beans don't need re-wiring beyond the bean swap).

**Dependencies:** `com.google.guava:guava` (already on enlinkd's classpath), the topology entity POJOs (in `enlinkd.persistence.api-1.0.11.jar`), and `TopologyEntityDao` interface (same jar). No Spring-Hibernate coupling.

**Copyright:** add the dual-copyright header per `feedback_deltav_package_namespace`:
```
/*
 * Copyright (C) 1999-2024 The OpenNMS Group, Inc.
 * Copyright (C) 2026 BeaconStrategists, Inc. (Modifications)
 * ...
 */
```

### 4.3 Wiring change in `EnlinkdDaemonConfiguration`

**File:** `core/daemon-boot-enlinkd/src/main/java/org/deltav/netmgt/enlinkd/boot/EnlinkdDaemonConfiguration.java`

**Delete:** the anonymous no-op bean at lines 175–199 (the entire `@Bean topologyEntityCache()` method body and the surrounding JavaDoc block).

**Add (replacing the deletion):**

```java
@Bean
public TopologyEntityDao topologyEntityDao() {
    return new TopologyEntityDaoJpa();
}

@Bean
public TopologyEntityCache topologyEntityCache(TopologyEntityDao topologyEntityDao,
                                                @Value("${deltav.enlinkd.topology-cache.duration-seconds:300}")
                                                int cacheDurationSeconds) {
    return new TopologyEntityCacheImpl(topologyEntityDao, cacheDurationSeconds);
}
```

(The constructor form replaces horizon's setter-based wiring — simpler in Spring Boot 4 and lets us pass the TTL in directly.)

**Imports:** swap the no-op imports (Collections, the 11 model classes used inline) for the new `TopologyEntityCacheImpl` and `TopologyEntityDaoJpa` imports.

### 4.4 Namespace note

Two of the three new classes follow `feedback_deltav_package_namespace` (`org.deltav.*`); the JPA DAO stays under `org.opennms.netmgt.enlinkd.persistence.impl` to match the dozen sibling `*DaoJpa` classes already in `core/opennms-model-jakarta`. Splitting that one DAO into a different package would create an inconsistency for the next person reading the module.

## 5. Configuration

**New property:** `deltav.enlinkd.topology-cache.duration-seconds` (default: `300`).

**Where set:** `application.properties` or `application.yml` for the enlinkd Spring Boot context. Default of 300s matches horizon behavior; operators can shorten for testing or lengthen for steady-state large topologies. Honors `feedback_no_shared_config_across_daemons`: daemon-scoped name, not a global horizon-era system property.

**Not added to a shared config module** — lives only in enlinkd's application properties.

## 6. Testing

### 6.1 Wiring test (no Spring context)

**File:** `core/daemon-boot-enlinkd/src/test/java/org/deltav/netmgt/enlinkd/boot/TopologyEntityCacheWiringTest.java`

**Pattern:** matches `SnmpProfileMapperWiringTest` from PR #262 — JUnit 5 + Mockito + AssertJ, no Spring context, direct calls on the `@Configuration` instance.

**What it verifies:**

- `new EnlinkdDaemonConfiguration().topologyEntityDao()` returns a `TopologyEntityDaoJpa` instance.
- `new EnlinkdDaemonConfiguration().topologyEntityCache(mockDao, 300)` returns a `TopologyEntityCacheImpl` instance (delta-v's port — full FQCN check), not an anonymous class.
- The cache's `getNodeTopologyEntities()` delegates to the mocked DAO (one Mockito verify confirms the wiring is real, not a stub).

This proves the no-op is gone and the cache → DAO wiring is correct. Service-bean wiring (the 7 topology services that depend on the cache) is unchanged from current behavior, so no new test needed there.

### 6.2 JPA integration test

**File:** `core/daemon-boot-enlinkd/src/test/java/org/deltav/netmgt/enlinkd/persistence/TopologyEntityDaoJpaIT.java`

**Pattern:** Testcontainers Postgres + Hibernate `EntityManagerFactory` built directly (no Spring context). The boot module has Failsafe + Testcontainers wired already; existing `BsmdApplicationIT` proves the path. We don't need a full `@SpringBootTest` — just JPA persistence-unit bootstrap + the DAO under test.

**Setup:**
1. `@Container` Postgres 16 with `schema.sql` init script (same schema artifact `BsmdApplicationIT` loads).
2. Bootstrap a `Persistence.createEntityManagerFactory("enlinkd-it", props)` with Hibernate dialect pointing at the container.
3. Insert one row per source table via raw SQL (`@Sql`-style script or programmatic `INSERT`): one node, one lldp_link, one cdp_link, one ospf_link, one isis_link, one ospf_area, one cdp_element, one lldp_element, one isis_element, one snmp_interface, one ip_interface.
4. Instantiate `TopologyEntityDaoJpa` with the EM and invoke each of the 11 query methods.
5. Assert the returned projection POJOs are populated (`assertThat(list).hasSize(1)` plus a single-field spot-check per projection — full equality matrix is overkill).

**Why this matters:** the JPQL constructor projection (`select new FQCN(...)`) is resolved at query-execution time, not compile time. A projection field-order swap or a wrong field name compiles cleanly and fails at runtime with `QuerySyntaxException`. One IT per projection catches the entire failure class for ~250 lines of test code total.

### 6.3 No E2E test addition

The existing enlinkd E2E (20/20 passing) verifies link tables populate. It doesn't read the topology graph because no consumer exists. Adding an E2E now would test something with no user-visible behavior. Defer E2E coverage to the v1.3 PR that wires the consumer.

## 7. Risks and rollback

| Risk | Mitigation |
|---|---|
| JPQL constructor projection field order mismatch | One IT per projection (§6.2) catches at build time |
| Hibernate session not available in enlinkd boot context | Existing `*DaoJpa` classes prove the path works; same wiring |
| Cache TTL too aggressive in dev (data churn) | Knob is configurable; default 300s matches horizon and is safe |
| Bean cycle (cache → DAO → EntityManagerFactory) | Constructor injection; EntityManagerFactory has no dependency on enlinkd beans, so no cycle |
| Memory pressure from cached projections | Topology entity count = nodes × ~5 (links + elements per protocol). For 10k nodes that's ~50k objects of ~200 bytes each = ~10MB. Negligible. |

**Rollback:** revert the single PR. The audit memory entry can re-mark #4 as no-op if needed; nothing else depends on the cache being populated.

## 8. Out of scope

- **Consumer for `OnmsTopologyDao`** — that's a v1.3 deliverable (Vue topology view or K8s operator API). This spec only wires the producer side.
- **`OnmsTopologyDao` persistence across restarts** — separate question; the in-memory graph is fine for the current scope (services rebuild it from the cache on next refresh).
- **Cache invalidation on writes** — horizon's pattern is purely TTL-based (no event hooks). Match horizon for now. If v1.3's UI needs faster turnaround, revisit then.
- **OpenTelemetry tracing of cache hits/misses** — track in DJ + Mike's OTel PR3 instrumentation work, not here.

## 9. Followup memory

After this lands, drop:

- `project_v1_3_topology_ui_resurrection` — note that producer side (this PR) is ready, consumer side (Vue topology view or K8s operator topology endpoint) is pending. Reference `OnmsTopologyDao` as the read surface.

## 10. Definition of done

- [ ] `TopologyEntityDaoJpa` lands in `core/opennms-model-jakarta` with 11 query methods.
- [ ] `TopologyEntityCacheImpl` (delta-v port) lands in `core/daemon-boot-enlinkd`.
- [ ] `EnlinkdDaemonConfiguration` no-op bean replaced with the two new beans.
- [ ] Wiring test passes locally and in CI.
- [ ] JPA IT passes against Testcontainers Postgres.
- [ ] Full E2E suite (per `feedback_e2e_continuous_validation_grpc_migration`) still passes — no regressions in enlinkd or other daemons.
- [ ] `project_daemon_nullop_audit_findings.md` updated: #4 marked DONE; cite PR number and squash commit.
- [ ] `project_v1_3_topology_ui_resurrection.md` written.
- [ ] PR opened against `pbrane/delta-v` `develop` (NEVER OpenNMS/opennms — see `feedback_never_pr_opennms`).
