# Topology Entity Cache Fill Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the anonymous no-op `TopologyEntityCache` bean in delta-v's enlinkd Spring Boot context with a real JPA-backed implementation, so the cache producer side is ready when a v1.3 topology consumer (Vue UI or K8s operator API) lands.

**Architecture:** Two new classes — `TopologyEntityDaoJpa` (delta-v JPQL reimplementation of horizon's Hibernate3-based DAO) and `TopologyEntityCacheImpl` (delta-v port of horizon's Guava `LoadingCache` wrapper). Wired in `EnlinkdDaemonConfiguration` to replace the existing inline anonymous no-op. New config knob `deltav.enlinkd.topology-cache.duration-seconds` (default 300s) controls cache TTL.

**Tech Stack:** Java 21, Spring Boot 4.0.3, Hibernate 7 (JPA), Guava `LoadingCache`, JUnit 5, Mockito, AssertJ, Testcontainers Postgres 16.

**Branch:** `feat/v1.2.0-topology-entity-cache-fill` (already created from `develop`).

**Spec:** `docs/superpowers/specs/2026-05-13-topology-entity-cache-fill-design.md`

---

## File Structure

**Create:**
- `core/opennms-model-jakarta/src/main/java/org/opennms/netmgt/enlinkd/persistence/impl/TopologyEntityDaoJpa.java` — JPA DAO, 11 read-only JPQL constructor-projection queries
- `core/daemon-boot-enlinkd/src/main/java/org/deltav/netmgt/enlinkd/persistence/cache/TopologyEntityCacheImpl.java` — Guava cache wrapper (delta-v port)
- `core/daemon-boot-enlinkd/src/test/java/org/deltav/netmgt/enlinkd/persistence/cache/TopologyEntityCacheImplTest.java` — unit tests for cache delegation + hit behavior
- `core/daemon-boot-enlinkd/src/test/java/org/deltav/netmgt/enlinkd/persistence/TopologyEntityDaoJpaIT.java` — Testcontainers Postgres IT, all 11 queries
- `core/daemon-boot-enlinkd/src/test/java/org/deltav/netmgt/enlinkd/boot/TopologyEntityCacheWiringTest.java` — wiring assertions
- `/Users/david/.claude/projects/-Users-david-development-src-opennms-delta-v/memory/project_v1_3_topology_ui_resurrection.md` — followup memory

**Modify:**
- `core/daemon-boot-enlinkd/src/main/java/org/deltav/netmgt/enlinkd/boot/EnlinkdDaemonConfiguration.java` — delete anonymous no-op bean (lines 175–199), add two new `@Bean` methods, swap imports
- `/Users/david/.claude/projects/-Users-david-development-src-opennms-delta-v/memory/project_daemon_nullop_audit_findings.md` — mark item #4 DONE with PR number

---

## Pre-flight (verified during planning, no action needed)

- `EnlinkdBootApplication.java` exists at `core/daemon-boot-enlinkd/src/main/java/org/deltav/netmgt/enlinkd/boot/EnlinkdBootApplication.java`.
- Testcontainers deps (`testcontainers`, `testcontainers-postgresql`, `testcontainers-junit-jupiter`) are already test-scoped on enlinkd's classpath.
- Anonymous no-op cache is at `EnlinkdDaemonConfiguration.java:175-199`.
- Horizon's published `enlinkd.persistence.api-1.0.11.jar` is already on enlinkd's compile classpath and ships the `TopologyEntityCache` interface, `TopologyEntityDao` interface, and the 11 projection POJOs.
- `org.opennms.features.enlinkd.persistence.impl` is NOT on the classpath (verified via `core/daemon-boot-enlinkd/pom.xml`) — no risk of horizon's broken `TopologyEntityDaoHibernate` being picked up.

---

## Task 1: Port `TopologyEntityCacheImpl` into delta-v with unit tests

**Files:**
- Create: `core/daemon-boot-enlinkd/src/main/java/org/deltav/netmgt/enlinkd/persistence/cache/TopologyEntityCacheImpl.java`
- Create: `core/daemon-boot-enlinkd/src/test/java/org/deltav/netmgt/enlinkd/persistence/cache/TopologyEntityCacheImplTest.java`

- [ ] **Step 1: Write the failing test**

Create `core/daemon-boot-enlinkd/src/test/java/org/deltav/netmgt/enlinkd/persistence/cache/TopologyEntityCacheImplTest.java`:

```java
/*
 * Copyright (C) 1999-2024 The OpenNMS Group, Inc.
 * Copyright (C) 2026 BeaconStrategists, Inc. (Modifications)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.deltav.netmgt.enlinkd.persistence.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.opennms.netmgt.enlinkd.model.NodeTopologyEntity;
import org.opennms.netmgt.enlinkd.persistence.api.TopologyEntityDao;

class TopologyEntityCacheImplTest {

    @Test
    void getNodeTopologyEntitiesReturnsResultFromDao() {
        TopologyEntityDao dao = mock(TopologyEntityDao.class);
        List<NodeTopologyEntity> fixture = Collections.emptyList();
        when(dao.getNodeTopologyEntities()).thenReturn(fixture);

        TopologyEntityCacheImpl cache = new TopologyEntityCacheImpl(dao, 300);

        assertThat(cache.getNodeTopologyEntities()).isSameAs(fixture);
    }

    @Test
    void cacheHitsAvoidRepeatedDaoCalls() {
        TopologyEntityDao dao = mock(TopologyEntityDao.class);
        when(dao.getNodeTopologyEntities()).thenReturn(Collections.emptyList());

        TopologyEntityCacheImpl cache = new TopologyEntityCacheImpl(dao, 300);
        cache.getNodeTopologyEntities();
        cache.getNodeTopologyEntities();
        cache.getNodeTopologyEntities();

        verify(dao, times(1)).getNodeTopologyEntities();
    }

    @Test
    void allElevenGettersDelegateToDao() {
        TopologyEntityDao dao = mock(TopologyEntityDao.class);
        when(dao.getNodeTopologyEntities()).thenReturn(Collections.emptyList());
        when(dao.getCdpLinkTopologyEntities()).thenReturn(Collections.emptyList());
        when(dao.getIsIsLinkTopologyEntities()).thenReturn(Collections.emptyList());
        when(dao.getLldpLinkTopologyEntities()).thenReturn(Collections.emptyList());
        when(dao.getOspfLinkTopologyEntities()).thenReturn(Collections.emptyList());
        when(dao.getOspfAreaTopologyEntities()).thenReturn(Collections.emptyList());
        when(dao.getCdpElementTopologyEntities()).thenReturn(Collections.emptyList());
        when(dao.getIsIsElementTopologyEntities()).thenReturn(Collections.emptyList());
        when(dao.getLldpElementTopologyEntities()).thenReturn(Collections.emptyList());
        when(dao.getSnmpTopologyEntities()).thenReturn(Collections.emptyList());
        when(dao.getIpTopologyEntities()).thenReturn(Collections.emptyList());

        TopologyEntityCacheImpl cache = new TopologyEntityCacheImpl(dao, 300);

        cache.getNodeTopologyEntities();
        cache.getCdpLinkTopologyEntities();
        cache.getIsIsLinkTopologyEntities();
        cache.getLldpLinkTopologyEntities();
        cache.getOspfLinkTopologyEntities();
        cache.getOspfAreaTopologyEntities();
        cache.getCdpElementTopologyEntities();
        cache.getIsIsElementTopologyEntities();
        cache.getLldpElementTopologyEntities();
        cache.getSnmpInterfaceTopologyEntities();
        cache.getIpInterfaceTopologyEntities();

        verify(dao).getNodeTopologyEntities();
        verify(dao).getCdpLinkTopologyEntities();
        verify(dao).getIsIsLinkTopologyEntities();
        verify(dao).getLldpLinkTopologyEntities();
        verify(dao).getOspfLinkTopologyEntities();
        verify(dao).getOspfAreaTopologyEntities();
        verify(dao).getCdpElementTopologyEntities();
        verify(dao).getIsIsElementTopologyEntities();
        verify(dao).getLldpElementTopologyEntities();
        verify(dao).getSnmpTopologyEntities();
        verify(dao).getIpTopologyEntities();
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./mvnw -q -pl :org.opennms.core.daemon-boot-enlinkd test -Dtest=TopologyEntityCacheImplTest`
Expected: COMPILATION FAILURE — `TopologyEntityCacheImpl` does not exist.

- [ ] **Step 3: Implement `TopologyEntityCacheImpl`**

Create `core/daemon-boot-enlinkd/src/main/java/org/deltav/netmgt/enlinkd/persistence/cache/TopologyEntityCacheImpl.java`:

```java
/*
 * Copyright (C) 1999-2024 The OpenNMS Group, Inc.
 * Copyright (C) 2026 BeaconStrategists, Inc. (Modifications)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.deltav.netmgt.enlinkd.persistence.cache;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.opennms.netmgt.enlinkd.model.CdpElementTopologyEntity;
import org.opennms.netmgt.enlinkd.model.CdpLinkTopologyEntity;
import org.opennms.netmgt.enlinkd.model.IpInterfaceTopologyEntity;
import org.opennms.netmgt.enlinkd.model.IsIsElementTopologyEntity;
import org.opennms.netmgt.enlinkd.model.IsIsLinkTopologyEntity;
import org.opennms.netmgt.enlinkd.model.LldpElementTopologyEntity;
import org.opennms.netmgt.enlinkd.model.LldpLinkTopologyEntity;
import org.opennms.netmgt.enlinkd.model.NodeTopologyEntity;
import org.opennms.netmgt.enlinkd.model.OspfAreaTopologyEntity;
import org.opennms.netmgt.enlinkd.model.OspfLinkTopologyEntity;
import org.opennms.netmgt.enlinkd.model.SnmpInterfaceTopologyEntity;
import org.opennms.netmgt.enlinkd.persistence.api.TopologyEntityCache;
import org.opennms.netmgt.enlinkd.persistence.api.TopologyEntityDao;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * Delta-V port of horizon's {@code TopologyEntityCacheImpl}. Wraps the
 * Hibernate-free {@link TopologyEntityDao} with per-entity-type Guava
 * LoadingCaches sharing a single TTL configured at construction time.
 *
 * <p>Cache TTL is supplied via constructor (typically from the
 * {@code deltav.enlinkd.topology-cache.duration-seconds} Spring property),
 * replacing horizon's global system-property lookup.</p>
 */
public class TopologyEntityCacheImpl implements TopologyEntityCache {

    private static final String CACHE_KEY = "CACHE_KEY";

    private final TopologyEntityDao topologyEntityDao;

    private final LoadingCache<String, List<NodeTopologyEntity>> nodeTopologyEntities;
    private final LoadingCache<String, List<CdpLinkTopologyEntity>> cdpLinkTopologyEntities;
    private final LoadingCache<String, List<IsIsLinkTopologyEntity>> isIsLinkTopologyEntities;
    private final LoadingCache<String, List<OspfLinkTopologyEntity>> ospfLinkTopologyEntities;
    private final LoadingCache<String, List<OspfAreaTopologyEntity>> ospfAreaTopologyEntities;
    private final LoadingCache<String, List<LldpLinkTopologyEntity>> lldpLinkTopologyEntities;
    private final LoadingCache<String, List<CdpElementTopologyEntity>> cdpElementTopologyEntities;
    private final LoadingCache<String, List<IsIsElementTopologyEntity>> isIsElementTopologyEntities;
    private final LoadingCache<String, List<LldpElementTopologyEntity>> lldpElementTopologyEntities;
    private final LoadingCache<String, List<SnmpInterfaceTopologyEntity>> snmpInterfaceTopologyEntities;
    private final LoadingCache<String, List<IpInterfaceTopologyEntity>> ipInterfaceTopologyEntities;

    public TopologyEntityCacheImpl(TopologyEntityDao topologyEntityDao, int cacheDurationSeconds) {
        this.topologyEntityDao = topologyEntityDao;
        this.nodeTopologyEntities = createCache(cacheDurationSeconds,
                () -> topologyEntityDao.getNodeTopologyEntities());
        this.cdpLinkTopologyEntities = createCache(cacheDurationSeconds,
                () -> topologyEntityDao.getCdpLinkTopologyEntities());
        this.isIsLinkTopologyEntities = createCache(cacheDurationSeconds,
                () -> topologyEntityDao.getIsIsLinkTopologyEntities());
        this.ospfLinkTopologyEntities = createCache(cacheDurationSeconds,
                () -> topologyEntityDao.getOspfLinkTopologyEntities());
        this.ospfAreaTopologyEntities = createCache(cacheDurationSeconds,
                () -> topologyEntityDao.getOspfAreaTopologyEntities());
        this.lldpLinkTopologyEntities = createCache(cacheDurationSeconds,
                () -> topologyEntityDao.getLldpLinkTopologyEntities());
        this.cdpElementTopologyEntities = createCache(cacheDurationSeconds,
                () -> topologyEntityDao.getCdpElementTopologyEntities());
        this.isIsElementTopologyEntities = createCache(cacheDurationSeconds,
                () -> topologyEntityDao.getIsIsElementTopologyEntities());
        this.lldpElementTopologyEntities = createCache(cacheDurationSeconds,
                () -> topologyEntityDao.getLldpElementTopologyEntities());
        this.snmpInterfaceTopologyEntities = createCache(cacheDurationSeconds,
                () -> topologyEntityDao.getSnmpTopologyEntities());
        this.ipInterfaceTopologyEntities = createCache(cacheDurationSeconds,
                () -> topologyEntityDao.getIpTopologyEntities());
    }

    private static <KEY, VALUE> LoadingCache<KEY, VALUE> createCache(int ttlSeconds,
                                                                     Supplier<VALUE> entitySupplier) {
        CacheLoader<KEY, VALUE> loader = new CacheLoader<KEY, VALUE>() {
            @Override
            public VALUE load(KEY key) {
                return entitySupplier.get();
            }
        };
        return CacheBuilder
                .newBuilder()
                .expireAfterWrite(ttlSeconds, TimeUnit.SECONDS)
                .build(loader);
    }

    @Override
    public List<NodeTopologyEntity> getNodeTopologyEntities() {
        return nodeTopologyEntities.getUnchecked(CACHE_KEY);
    }

    @Override
    public List<CdpLinkTopologyEntity> getCdpLinkTopologyEntities() {
        return cdpLinkTopologyEntities.getUnchecked(CACHE_KEY);
    }

    @Override
    public List<OspfLinkTopologyEntity> getOspfLinkTopologyEntities() {
        return ospfLinkTopologyEntities.getUnchecked(CACHE_KEY);
    }

    @Override
    public List<OspfAreaTopologyEntity> getOspfAreaTopologyEntities() {
        return ospfAreaTopologyEntities.getUnchecked(CACHE_KEY);
    }

    @Override
    public List<IsIsLinkTopologyEntity> getIsIsLinkTopologyEntities() {
        return isIsLinkTopologyEntities.getUnchecked(CACHE_KEY);
    }

    @Override
    public List<LldpLinkTopologyEntity> getLldpLinkTopologyEntities() {
        return lldpLinkTopologyEntities.getUnchecked(CACHE_KEY);
    }

    @Override
    public List<CdpElementTopologyEntity> getCdpElementTopologyEntities() {
        return cdpElementTopologyEntities.getUnchecked(CACHE_KEY);
    }

    @Override
    public List<IsIsElementTopologyEntity> getIsIsElementTopologyEntities() {
        return isIsElementTopologyEntities.getUnchecked(CACHE_KEY);
    }

    @Override
    public List<LldpElementTopologyEntity> getLldpElementTopologyEntities() {
        return lldpElementTopologyEntities.getUnchecked(CACHE_KEY);
    }

    @Override
    public List<SnmpInterfaceTopologyEntity> getSnmpInterfaceTopologyEntities() {
        return snmpInterfaceTopologyEntities.getUnchecked(CACHE_KEY);
    }

    @Override
    public List<IpInterfaceTopologyEntity> getIpInterfaceTopologyEntities() {
        return ipInterfaceTopologyEntities.getUnchecked(CACHE_KEY);
    }

    @Override
    public void refresh() {
        nodeTopologyEntities.refresh(CACHE_KEY);
        cdpLinkTopologyEntities.refresh(CACHE_KEY);
        isIsLinkTopologyEntities.refresh(CACHE_KEY);
        lldpLinkTopologyEntities.refresh(CACHE_KEY);
        ospfLinkTopologyEntities.refresh(CACHE_KEY);
        ospfAreaTopologyEntities.refresh(CACHE_KEY);
        cdpElementTopologyEntities.refresh(CACHE_KEY);
        isIsElementTopologyEntities.refresh(CACHE_KEY);
        lldpElementTopologyEntities.refresh(CACHE_KEY);
        snmpInterfaceTopologyEntities.refresh(CACHE_KEY);
        ipInterfaceTopologyEntities.refresh(CACHE_KEY);
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./mvnw -q -pl :org.opennms.core.daemon-boot-enlinkd test -Dtest=TopologyEntityCacheImplTest`
Expected: 3 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add core/daemon-boot-enlinkd/src/main/java/org/deltav/netmgt/enlinkd/persistence/cache/TopologyEntityCacheImpl.java \
        core/daemon-boot-enlinkd/src/test/java/org/deltav/netmgt/enlinkd/persistence/cache/TopologyEntityCacheImplTest.java
git commit -m "feat(enlinkd): port TopologyEntityCacheImpl into delta-v namespace

Mirrors horizon's Guava-backed cache wrapper but:
- lives under org.deltav.* per feedback_deltav_package_namespace
- reads TTL from a constructor arg (not the SystemProperties global)
  so it can be wired with the deltav.enlinkd.topology-cache.duration-seconds
  knob from EnlinkdDaemonConfiguration

DAO wiring follows in the next commit; this commit alone is dead code."
```

---

## Task 2: Implement `TopologyEntityDaoJpa` skeleton (compiles, returns empty)

**Files:**
- Create: `core/opennms-model-jakarta/src/main/java/org/opennms/netmgt/enlinkd/persistence/impl/TopologyEntityDaoJpa.java`

This task lands the class shape and method signatures so Task 3's IT can `@Autowired` it. The bodies return empty lists; Task 4 fills them with real queries.

- [ ] **Step 1: Create the skeleton class**

Create `core/opennms-model-jakarta/src/main/java/org/opennms/netmgt/enlinkd/persistence/impl/TopologyEntityDaoJpa.java`:

```java
/*
 * Copyright (C) 1999-2024 The OpenNMS Group, Inc.
 * Copyright (C) 2026 BeaconStrategists, Inc. (Modifications)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.opennms.netmgt.enlinkd.persistence.impl;

import java.util.Collections;
import java.util.List;

import org.opennms.netmgt.enlinkd.model.CdpElementTopologyEntity;
import org.opennms.netmgt.enlinkd.model.CdpLinkTopologyEntity;
import org.opennms.netmgt.enlinkd.model.IpInterfaceTopologyEntity;
import org.opennms.netmgt.enlinkd.model.IsIsElementTopologyEntity;
import org.opennms.netmgt.enlinkd.model.IsIsLinkTopologyEntity;
import org.opennms.netmgt.enlinkd.model.LldpElementTopologyEntity;
import org.opennms.netmgt.enlinkd.model.LldpLinkTopologyEntity;
import org.opennms.netmgt.enlinkd.model.NodeTopologyEntity;
import org.opennms.netmgt.enlinkd.model.OspfAreaTopologyEntity;
import org.opennms.netmgt.enlinkd.model.OspfLinkTopologyEntity;
import org.opennms.netmgt.enlinkd.model.SnmpInterfaceTopologyEntity;
import org.opennms.netmgt.enlinkd.persistence.api.TopologyEntityDao;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;

/**
 * JPA implementation of {@link TopologyEntityDao}. Replaces horizon's
 * {@code TopologyEntityDaoHibernate} which depends on the Spring 3-era
 * {@code spring.orm.hibernate3} package — removed in Spring 5+ and not
 * available in delta-v's Spring 7 / Spring Boot 4 runtime.
 *
 * <p>All queries are read-only JPQL constructor projections. The 11 method
 * bodies are filled in a follow-up commit; this skeleton exists so the
 * Testcontainers IT can wire and fail on empty results.</p>
 */
@Repository
@Transactional(readOnly = true)
public class TopologyEntityDaoJpa implements TopologyEntityDao {

    @PersistenceContext
    private EntityManager em;

    @Override
    public List<NodeTopologyEntity> getNodeTopologyEntities() {
        return Collections.emptyList();
    }

    @Override
    public List<CdpLinkTopologyEntity> getCdpLinkTopologyEntities() {
        return Collections.emptyList();
    }

    @Override
    public List<IsIsLinkTopologyEntity> getIsIsLinkTopologyEntities() {
        return Collections.emptyList();
    }

    @Override
    public List<LldpLinkTopologyEntity> getLldpLinkTopologyEntities() {
        return Collections.emptyList();
    }

    @Override
    public List<OspfLinkTopologyEntity> getOspfLinkTopologyEntities() {
        return Collections.emptyList();
    }

    @Override
    public List<OspfAreaTopologyEntity> getOspfAreaTopologyEntities() {
        return Collections.emptyList();
    }

    @Override
    public List<SnmpInterfaceTopologyEntity> getSnmpTopologyEntities() {
        return Collections.emptyList();
    }

    @Override
    public List<IpInterfaceTopologyEntity> getIpTopologyEntities() {
        return Collections.emptyList();
    }

    @Override
    public List<CdpElementTopologyEntity> getCdpElementTopologyEntities() {
        return Collections.emptyList();
    }

    @Override
    public List<LldpElementTopologyEntity> getLldpElementTopologyEntities() {
        return Collections.emptyList();
    }

    @Override
    public List<IsIsElementTopologyEntity> getIsIsElementTopologyEntities() {
        return Collections.emptyList();
    }
}
```

- [ ] **Step 2: Verify it compiles**

Run: `./mvnw -q -pl :org.opennms.core.model-jakarta compile`
Expected: BUILD SUCCESS, no compiler errors.

If the compiler complains about a missing `TopologyEntityDao` interface method, cross-check against the published API class:
```bash
javap -p ~/.m2/repository/org/opennms/features/enlinkd/org.opennms.features.enlinkd.persistence.api/1.0.11/org.opennms.features.enlinkd.persistence.api-1.0.11.jar | grep "abstract.*TopologyEntity"
```
(That command should list 11 abstract methods + `refresh` — `refresh` is on `TopologyEntityCache`, not `TopologyEntityDao`.)

- [ ] **Step 3: Commit**

```bash
git add core/opennms-model-jakarta/src/main/java/org/opennms/netmgt/enlinkd/persistence/impl/TopologyEntityDaoJpa.java
git commit -m "feat(enlinkd): scaffold TopologyEntityDaoJpa skeleton

Empty implementations of all 11 read-only projection queries. Lets the
follow-up Testcontainers IT wire @Autowired and fail with concrete
'expected size 1, was 0' messages, driving the query implementation in
the next commit."
```

---

## Task 3: Add Testcontainers IT (fails — DAO returns empty)

**Files:**
- Create: `core/daemon-boot-enlinkd/src/test/java/org/deltav/netmgt/enlinkd/persistence/TopologyEntityDaoJpaIT.java`
- Create: `core/daemon-boot-enlinkd/src/test/resources/application-test.properties` (if not already present — check first)

- [ ] **Step 1: Check for existing test config**

Run: `ls core/daemon-boot-enlinkd/src/test/resources/ 2>/dev/null`

If empty or no `application*.properties`/`application*.yml`, proceed with Step 2. Otherwise inspect the existing file and merge the properties below into it instead of creating a new one.

- [ ] **Step 2: Create test properties**

Create `core/daemon-boot-enlinkd/src/test/resources/application-test.properties`:

```properties
# Hibernate auto-generates the schema for the IT — avoids maintaining a
# separate enlinkd-it.sql alongside the production schema. Test fixtures are
# inserted via EntityManager, so we don't need DDL parity with production.
spring.jpa.hibernate.ddl-auto=create-drop
spring.jpa.show-sql=false
spring.jpa.properties.hibernate.format_sql=false

# Topology cache TTL is irrelevant for DAO-only IT; pick something fast for
# safety in case the test ever exercises cache behavior end-to-end.
deltav.enlinkd.topology-cache.duration-seconds=1
```

- [ ] **Step 3: Write the failing IT**

Create `core/daemon-boot-enlinkd/src/test/java/org/deltav/netmgt/enlinkd/persistence/TopologyEntityDaoJpaIT.java`:

```java
/*
 * Copyright (C) 2026 BeaconStrategists, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.deltav.netmgt.enlinkd.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.InetAddress;
import java.util.Date;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opennms.netmgt.enlinkd.model.CdpElement;
import org.opennms.netmgt.enlinkd.model.CdpElementTopologyEntity;
import org.opennms.netmgt.enlinkd.model.CdpLink;
import org.opennms.netmgt.enlinkd.model.CdpLinkTopologyEntity;
import org.opennms.netmgt.enlinkd.model.IpInterfaceTopologyEntity;
import org.opennms.netmgt.enlinkd.model.IsIsElement;
import org.opennms.netmgt.enlinkd.model.IsIsElementTopologyEntity;
import org.opennms.netmgt.enlinkd.model.IsIsLink;
import org.opennms.netmgt.enlinkd.model.IsIsLinkTopologyEntity;
import org.opennms.netmgt.enlinkd.model.LldpElement;
import org.opennms.netmgt.enlinkd.model.LldpElementTopologyEntity;
import org.opennms.netmgt.enlinkd.model.LldpLink;
import org.opennms.netmgt.enlinkd.model.LldpLinkTopologyEntity;
import org.opennms.netmgt.enlinkd.model.NodeTopologyEntity;
import org.opennms.netmgt.enlinkd.model.OspfArea;
import org.opennms.netmgt.enlinkd.model.OspfAreaTopologyEntity;
import org.opennms.netmgt.enlinkd.model.OspfLink;
import org.opennms.netmgt.enlinkd.model.OspfLinkTopologyEntity;
import org.opennms.netmgt.enlinkd.model.SnmpInterfaceTopologyEntity;
import org.opennms.netmgt.enlinkd.persistence.api.TopologyEntityDao;
import org.opennms.netmgt.model.OnmsIpInterface;
import org.opennms.netmgt.model.OnmsNode;
import org.opennms.netmgt.model.OnmsSnmpInterface;
import org.opennms.netmgt.model.PrimaryType;
import org.opennms.netmgt.model.monitoringLocations.OnmsMonitoringLocation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.annotation.Transactional;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;

/**
 * Integration test for {@code TopologyEntityDaoJpa}. Boots a Postgres
 * Testcontainer with Hibernate-generated schema, inserts one fixture row
 * per source table, and verifies all 11 JPQL constructor projections.
 *
 * <p>JPQL constructor projection (<code>select new FQCN(...)</code>) is
 * resolved reflectively at query-execution time, so a field-order swap
 * compiles cleanly and fails at runtime. This IT catches that class of bug.</p>
 */
@SpringBootTest(classes = org.deltav.netmgt.enlinkd.boot.EnlinkdBootApplication.class,
                webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles("test")
@Testcontainers
@Transactional
class TopologyEntityDaoJpaIT {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("opennms")
            .withUsername("opennms")
            .withPassword("opennms");

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("opennms.kafka.bootstrap-servers", () -> "localhost:9092");
    }

    @Autowired
    private TopologyEntityDao dao;

    @PersistenceContext
    private EntityManager em;

    private OnmsNode node;

    @BeforeEach
    void insertFixtures() throws Exception {
        OnmsMonitoringLocation location = new OnmsMonitoringLocation("Default", "Default");
        em.persist(location);

        node = new OnmsNode(location, "fixture-node");
        node.setType(OnmsNode.NodeType.ACTIVE);
        node.setSysObjectId(".1.3.6.1.4.1.9");
        em.persist(node);

        OnmsSnmpInterface snmp = new OnmsSnmpInterface(node, 1);
        snmp.setIfName("eth0");
        snmp.setIfAlias("uplink");
        snmp.setIfSpeed(1_000_000_000L);
        em.persist(snmp);

        OnmsIpInterface ip = new OnmsIpInterface(InetAddress.getByName("192.0.2.1"), node);
        ip.setNetMask(InetAddress.getByName("255.255.255.0"));
        ip.setIsManaged("M");
        ip.setSnmpPrimary(PrimaryType.PRIMARY);
        ip.setSnmpInterface(snmp);
        em.persist(ip);

        LldpLink lldpLink = new LldpLink();
        lldpLink.setNode(node);
        lldpLink.setLldpRemChassisId("aa:bb:cc:dd:ee:ff");
        lldpLink.setLldpRemSysname("peer-1");
        lldpLink.setLldpRemPortId("Gi0/1");
        lldpLink.setLldpPortId("Gi0/2");
        lldpLink.setLldpPortDescr("local-iface");
        lldpLink.setLldpPortIfindex(1);
        lldpLink.setLldpLinkCreateTime(new Date());
        lldpLink.setLldpLinkLastPollTime(new Date());
        em.persist(lldpLink);

        LldpElement lldpElement = new LldpElement();
        lldpElement.setNode(node);
        lldpElement.setLldpChassisId("11:22:33:44:55:66");
        lldpElement.setLldpSysname("local-host");
        lldpElement.setLldpNodeCreateTime(new Date());
        lldpElement.setLldpNodeLastPollTime(new Date());
        em.persist(lldpElement);

        CdpLink cdpLink = new CdpLink();
        cdpLink.setNode(node);
        cdpLink.setCdpCacheIfIndex(1);
        cdpLink.setCdpInterfaceName("Gi0/1");
        cdpLink.setCdpCacheAddress("192.0.2.2");
        cdpLink.setCdpCacheDeviceId("cdp-peer");
        cdpLink.setCdpCacheDevicePort("Gi0/0");
        cdpLink.setCdpLinkCreateTime(new Date());
        cdpLink.setCdpLinkLastPollTime(new Date());
        em.persist(cdpLink);

        CdpElement cdpElement = new CdpElement();
        cdpElement.setNode(node);
        cdpElement.setCdpGlobalDeviceId("global-dev-id");
        cdpElement.setCdpNodeCreateTime(new Date());
        cdpElement.setCdpNodeLastPollTime(new Date());
        em.persist(cdpElement);

        OspfLink ospfLink = new OspfLink();
        ospfLink.setNode(node);
        ospfLink.setOspfIpAddr(InetAddress.getByName("10.0.0.1"));
        ospfLink.setOspfIpMask(InetAddress.getByName("255.255.255.0"));
        ospfLink.setOspfRemIpAddr(InetAddress.getByName("10.0.0.2"));
        ospfLink.setOspfIfIndex(1);
        ospfLink.setOspfIfAreaId(0);
        ospfLink.setOspfLinkCreateTime(new Date());
        ospfLink.setOspfLinkLastPollTime(new Date());
        em.persist(ospfLink);

        OspfArea ospfArea = new OspfArea();
        ospfArea.setNode(node);
        ospfArea.setOspfAreaId(0);
        ospfArea.setOspfAuthType(0);
        ospfArea.setOspfImportAsExtern(1);
        ospfArea.setOspfAreaBdrRtrCount(1);
        ospfArea.setOspfAsBdrRtrCount(0);
        ospfArea.setOspfAreaLsaCount(10);
        em.persist(ospfArea);

        IsIsLink isisLink = new IsIsLink();
        isisLink.setNode(node);
        isisLink.setIsisISAdjIndex(1);
        isisLink.setIsisCircIfIndex(1);
        isisLink.setIsisISAdjNeighSysID("isis-peer");
        isisLink.setIsisISAdjNeighSNPAAddress("aa:bb:cc:dd:ee:ff");
        isisLink.setIsisLinkCreateTime(new Date());
        isisLink.setIsisLinkLastPollTime(new Date());
        em.persist(isisLink);

        IsIsElement isisElement = new IsIsElement();
        isisElement.setNode(node);
        isisElement.setIsisSysID("local-sys");
        isisElement.setIsisNodeCreateTime(new Date());
        isisElement.setIsisNodeLastPollTime(new Date());
        em.persist(isisElement);

        em.flush();
    }

    @Test
    void nodeProjectionPopulates() {
        List<NodeTopologyEntity> result = dao.getNodeTopologyEntities();
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getLabel()).isEqualTo("fixture-node");
    }

    @Test
    void cdpLinkProjectionPopulates() {
        List<CdpLinkTopologyEntity> result = dao.getCdpLinkTopologyEntities();
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getCdpCacheDeviceId()).isEqualTo("cdp-peer");
    }

    @Test
    void cdpElementProjectionPopulates() {
        List<CdpElementTopologyEntity> result = dao.getCdpElementTopologyEntities();
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getCdpGlobalDeviceId()).isEqualTo("global-dev-id");
    }

    @Test
    void lldpLinkProjectionPopulates() {
        List<LldpLinkTopologyEntity> result = dao.getLldpLinkTopologyEntities();
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getLldpRemSysname()).isEqualTo("peer-1");
    }

    @Test
    void lldpElementProjectionPopulates() {
        List<LldpElementTopologyEntity> result = dao.getLldpElementTopologyEntities();
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getLldpSysname()).isEqualTo("local-host");
    }

    @Test
    void ospfLinkProjectionPopulates() {
        List<OspfLinkTopologyEntity> result = dao.getOspfLinkTopologyEntities();
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getOspfIfIndex()).isEqualTo(1);
    }

    @Test
    void ospfAreaProjectionPopulates() {
        List<OspfAreaTopologyEntity> result = dao.getOspfAreaTopologyEntities();
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getOspfAreaId()).isEqualTo(0);
    }

    @Test
    void isisLinkProjectionPopulates() {
        List<IsIsLinkTopologyEntity> result = dao.getIsIsLinkTopologyEntities();
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getIsisISAdjNeighSysID()).isEqualTo("isis-peer");
    }

    @Test
    void isisElementProjectionPopulates() {
        List<IsIsElementTopologyEntity> result = dao.getIsIsElementTopologyEntities();
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getIsisSysID()).isEqualTo("local-sys");
    }

    @Test
    void snmpInterfaceProjectionPopulates() {
        List<SnmpInterfaceTopologyEntity> result = dao.getSnmpTopologyEntities();
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getIfName()).isEqualTo("eth0");
    }

    @Test
    void ipInterfaceProjectionPopulates() {
        List<IpInterfaceTopologyEntity> result = dao.getIpTopologyEntities();
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getIpAddress().getHostAddress()).isEqualTo("192.0.2.1");
    }
}
```

- [ ] **Step 4: Run the IT — confirm it fails for the right reason**

Run: `./mvnw -q -pl :org.opennms.core.daemon-boot-enlinkd verify -Dit.test=TopologyEntityDaoJpaIT -DfailIfNoTests=false`
Expected: 11 tests FAIL, each with `Expected size: 1 but was: 0`.

If the failure is something else (e.g., context fails to load, missing entity class, schema generation error), inspect Surefire reports under `core/daemon-boot-enlinkd/target/failsafe-reports/` and fix before proceeding.

Common fixups:
- **Field-name mismatch on a setter** (e.g., `setOspfAreaBdrRtrCount` vs entity's actual setter name): fix the fixture to call the real setter.
- **Missing entity mapping**: confirm the entity class is in `org.opennms.netmgt.enlinkd.model` (already on classpath via published api jar) AND has `@Entity` mapping in the jakarta-port — if a class is missing jakarta annotations, the schema generation will skip its table.
- **Constructor mismatch in OnmsNode**: inspect `OnmsNode`'s actual constructors in `core/opennms-model-jakarta` and adapt the fixture.

- [ ] **Step 5: Commit**

```bash
git add core/daemon-boot-enlinkd/src/test/java/org/deltav/netmgt/enlinkd/persistence/TopologyEntityDaoJpaIT.java \
        core/daemon-boot-enlinkd/src/test/resources/application-test.properties
git commit -m "test(enlinkd): Testcontainers IT for TopologyEntityDaoJpa projections

11 tests asserting one fixture row per source table flows through each
JPQL constructor projection. All currently fail with 'expected size 1
but was 0' against the empty skeleton implementation — driving Task 4's
query implementation."
```

---

## Task 4: Fill `TopologyEntityDaoJpa` with the 11 JPQL queries

**Files:**
- Modify: `core/opennms-model-jakarta/src/main/java/org/opennms/netmgt/enlinkd/persistence/impl/TopologyEntityDaoJpa.java`

- [ ] **Step 1: Replace skeleton method bodies with real queries**

Open `core/opennms-model-jakarta/src/main/java/org/opennms/netmgt/enlinkd/persistence/impl/TopologyEntityDaoJpa.java` and replace each `return Collections.emptyList();` with the corresponding query. Final state of the class body (everything between class opening brace and closing brace):

```java
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

    @Override
    public List<CdpLinkTopologyEntity> getCdpLinkTopologyEntities() {
        return em.createQuery(
                "select new org.opennms.netmgt.enlinkd.model.CdpLinkTopologyEntity("
                        + "l.id, l.node.id, l.cdpCacheIfIndex, l.cdpInterfaceName, "
                        + "l.cdpCacheAddress, l.cdpCacheDeviceId, l.cdpCacheDevicePort) "
                        + "from org.opennms.netmgt.enlinkd.model.CdpLink l",
                CdpLinkTopologyEntity.class).getResultList();
    }

    @Override
    public List<IsIsLinkTopologyEntity> getIsIsLinkTopologyEntities() {
        return em.createQuery(
                "select new org.opennms.netmgt.enlinkd.model.IsIsLinkTopologyEntity("
                        + "l.id, l.node.id, l.isisISAdjIndex, l.isisCircIfIndex, "
                        + "l.isisISAdjNeighSysID, l.isisISAdjNeighSNPAAddress) "
                        + "from org.opennms.netmgt.enlinkd.model.IsIsLink l",
                IsIsLinkTopologyEntity.class).getResultList();
    }

    @Override
    public List<LldpLinkTopologyEntity> getLldpLinkTopologyEntities() {
        return em.createQuery(
                "select new org.opennms.netmgt.enlinkd.model.LldpLinkTopologyEntity("
                        + "l.id, l.node.id, l.lldpRemChassisId, l.lldpRemSysname, "
                        + "l.lldpRemPortId, l.lldpRemPortIdSubType, l.lldpRemPortDescr, "
                        + "l.lldpPortId, l.lldpPortIdSubType, l.lldpPortDescr, l.lldpPortIfindex) "
                        + "from org.opennms.netmgt.enlinkd.model.LldpLink l",
                LldpLinkTopologyEntity.class).getResultList();
    }

    @Override
    public List<OspfLinkTopologyEntity> getOspfLinkTopologyEntities() {
        return em.createQuery(
                "select new org.opennms.netmgt.enlinkd.model.OspfLinkTopologyEntity("
                        + "l.id, l.node.id, l.ospfIpAddr, l.ospfIpMask, l.ospfRemIpAddr, "
                        + "l.ospfIfIndex, l.ospfIfAreaId) "
                        + "from org.opennms.netmgt.enlinkd.model.OspfLink l",
                OspfLinkTopologyEntity.class).getResultList();
    }

    @Override
    public List<OspfAreaTopologyEntity> getOspfAreaTopologyEntities() {
        return em.createQuery(
                "select new org.opennms.netmgt.enlinkd.model.OspfAreaTopologyEntity("
                        + "a.id, a.node.id, a.ospfAreaId, a.ospfAuthType, a.ospfImportAsExtern, "
                        + "a.ospfAreaBdrRtrCount, a.ospfAsBdrRtrCount, a.ospfAreaLsaCount) "
                        + "from org.opennms.netmgt.enlinkd.model.OspfArea a",
                OspfAreaTopologyEntity.class).getResultList();
    }

    @Override
    public List<SnmpInterfaceTopologyEntity> getSnmpTopologyEntities() {
        return em.createQuery(
                "select new org.opennms.netmgt.enlinkd.model.SnmpInterfaceTopologyEntity("
                        + "i.id, i.ifIndex, i.ifName, i.ifAlias, i.ifSpeed, i.node.id) "
                        + "from org.opennms.netmgt.model.OnmsSnmpInterface i",
                SnmpInterfaceTopologyEntity.class).getResultList();
    }

    @Override
    public List<IpInterfaceTopologyEntity> getIpTopologyEntities() {
        return em.createQuery(
                "select new org.opennms.netmgt.enlinkd.model.IpInterfaceTopologyEntity("
                        + "i.id, i.ipAddress, i.netMask, i.isManaged, i.snmpPrimary, "
                        + "i.node.id, i.snmpInterface.id) "
                        + "from org.opennms.netmgt.model.OnmsIpInterface i",
                IpInterfaceTopologyEntity.class).getResultList();
    }

    @Override
    public List<CdpElementTopologyEntity> getCdpElementTopologyEntities() {
        return em.createQuery(
                "select new org.opennms.netmgt.enlinkd.model.CdpElementTopologyEntity("
                        + "e.id, e.cdpGlobalDeviceId, e.node.id) "
                        + "from org.opennms.netmgt.enlinkd.model.CdpElement e",
                CdpElementTopologyEntity.class).getResultList();
    }

    @Override
    public List<LldpElementTopologyEntity> getLldpElementTopologyEntities() {
        return em.createQuery(
                "select new org.opennms.netmgt.enlinkd.model.LldpElementTopologyEntity("
                        + "e.id, e.lldpChassisId, e.lldpSysname, e.node.id) "
                        + "from org.opennms.netmgt.enlinkd.model.LldpElement e",
                LldpElementTopologyEntity.class).getResultList();
    }

    @Override
    public List<IsIsElementTopologyEntity> getIsIsElementTopologyEntities() {
        return em.createQuery(
                "select new org.opennms.netmgt.enlinkd.model.IsIsElementTopologyEntity("
                        + "e.id, e.isisSysID, e.node.id) "
                        + "from org.opennms.netmgt.enlinkd.model.IsIsElement e",
                IsIsElementTopologyEntity.class).getResultList();
    }
```

Also remove the now-unused `import java.util.Collections;` line at the top.

- [ ] **Step 2: Run the IT — confirm all 11 tests pass**

Run: `./mvnw -q -pl :org.opennms.core.daemon-boot-enlinkd verify -Dit.test=TopologyEntityDaoJpaIT -DfailIfNoTests=false`
Expected: 11 tests PASS.

If any test fails with `QuerySyntaxException` or `IllegalArgumentException: could not locate appropriate constructor`, inspect the projection POJO's constructor signature:
```bash
javap -p ~/.m2/repository/org/opennms/features/enlinkd/org.opennms.features.enlinkd.persistence.api/1.0.11/org.opennms.features.enlinkd.persistence.api-1.0.11.jar | grep "TopologyEntity.*("
```
…and adjust the constructor projection argument order to match.

- [ ] **Step 3: Commit**

```bash
git add core/opennms-model-jakarta/src/main/java/org/opennms/netmgt/enlinkd/persistence/impl/TopologyEntityDaoJpa.java
git commit -m "feat(enlinkd): implement 11 JPQL projection queries in TopologyEntityDaoJpa

Port of horizon's TopologyEntityDaoHibernate queries (HQL → JPQL) using
EntityManager createQuery() with typed constructor projections. Replaces
the skeleton's empty lists; TopologyEntityDaoJpaIT 11/11 now passes."
```

---

## Task 5: Wire `EnlinkdDaemonConfiguration` + wiring test

**Files:**
- Modify: `core/daemon-boot-enlinkd/src/main/java/org/deltav/netmgt/enlinkd/boot/EnlinkdDaemonConfiguration.java`
- Create: `core/daemon-boot-enlinkd/src/test/java/org/deltav/netmgt/enlinkd/boot/TopologyEntityCacheWiringTest.java`

- [ ] **Step 1: Write the failing wiring test**

Create `core/daemon-boot-enlinkd/src/test/java/org/deltav/netmgt/enlinkd/boot/TopologyEntityCacheWiringTest.java`:

```java
/*
 * Copyright (C) 2026 BeaconStrategists, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.deltav.netmgt.enlinkd.boot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Collections;

import org.deltav.netmgt.enlinkd.persistence.cache.TopologyEntityCacheImpl;
import org.junit.jupiter.api.Test;
import org.opennms.netmgt.enlinkd.persistence.api.TopologyEntityCache;
import org.opennms.netmgt.enlinkd.persistence.api.TopologyEntityDao;
import org.opennms.netmgt.enlinkd.persistence.impl.TopologyEntityDaoJpa;

/**
 * Wiring-only test for EnlinkdDaemonConfiguration's TopologyEntityCache /
 * TopologyEntityDao beans. Asserts the beans are the real delta-v impls
 * (not the deleted anonymous no-op) and that the cache delegates to the
 * injected DAO. No Spring context, no DataSource, no Postgres.
 */
class TopologyEntityCacheWiringTest {

    @Test
    void topologyEntityDaoBeanIsRealJpaImpl() {
        TopologyEntityDao dao = new EnlinkdDaemonConfiguration().topologyEntityDao();
        assertThat(dao).isInstanceOf(TopologyEntityDaoJpa.class);
    }

    @Test
    void topologyEntityCacheBeanIsDeltavImpl() {
        TopologyEntityDao mockDao = mock(TopologyEntityDao.class);
        TopologyEntityCache cache = new EnlinkdDaemonConfiguration().topologyEntityCache(mockDao, 300);
        assertThat(cache).isInstanceOf(TopologyEntityCacheImpl.class);
    }

    @Test
    void cacheDelegatesToInjectedDao() {
        TopologyEntityDao mockDao = mock(TopologyEntityDao.class);
        org.mockito.Mockito.when(mockDao.getNodeTopologyEntities()).thenReturn(Collections.emptyList());

        TopologyEntityCache cache = new EnlinkdDaemonConfiguration().topologyEntityCache(mockDao, 300);
        cache.getNodeTopologyEntities();

        verify(mockDao).getNodeTopologyEntities();
    }
}
```

- [ ] **Step 2: Run the wiring test — confirm it fails**

Run: `./mvnw -q -pl :org.opennms.core.daemon-boot-enlinkd test -Dtest=TopologyEntityCacheWiringTest`
Expected: COMPILATION FAILURE — `topologyEntityDao()` method does not exist on `EnlinkdDaemonConfiguration`.

- [ ] **Step 3: Apply the wiring change in `EnlinkdDaemonConfiguration`**

In `core/daemon-boot-enlinkd/src/main/java/org/deltav/netmgt/enlinkd/boot/EnlinkdDaemonConfiguration.java`:

**a)** Delete lines 175–199 (the anonymous no-op `topologyEntityCache()` bean and its surrounding JavaDoc, starting at the `// ── 5. TopologyEntityCache (no-op) ───────────────────────────────` comment and ending at the closing `}` of the bean method).

**b)** In place of the deleted block, insert:

```java
    // ── 5. TopologyEntityCache (real JPA-backed) ─────────────────────

    /**
     * Real DAO over the topology projection tables. Replaces the no-op
     * that was here pre-PR — produces empty lists no more.
     */
    @Bean
    public TopologyEntityDao topologyEntityDao() {
        return new TopologyEntityDaoJpa();
    }

    /**
     * Guava-backed cache wrapping {@link TopologyEntityDao}. TTL is set
     * via {@code deltav.enlinkd.topology-cache.duration-seconds} (default
     * 300s, matching horizon's behavior).
     */
    @Bean
    public TopologyEntityCache topologyEntityCache(
            TopologyEntityDao topologyEntityDao,
            @Value("${deltav.enlinkd.topology-cache.duration-seconds:300}") int cacheDurationSeconds) {
        return new TopologyEntityCacheImpl(topologyEntityDao, cacheDurationSeconds);
    }
```

**c)** Adjust imports:

Add these imports (alphabetized within their existing groups):
```java
import org.deltav.netmgt.enlinkd.persistence.cache.TopologyEntityCacheImpl;
import org.opennms.netmgt.enlinkd.persistence.impl.TopologyEntityDaoJpa;
import org.springframework.beans.factory.annotation.Value;
```

Remove these imports (only the anonymous no-op used them):
```java
import java.util.Collections;
import org.opennms.netmgt.enlinkd.model.CdpElementTopologyEntity;
import org.opennms.netmgt.enlinkd.model.CdpLinkTopologyEntity;
import org.opennms.netmgt.enlinkd.model.IpInterfaceTopologyEntity;
import org.opennms.netmgt.enlinkd.model.IsIsElementTopologyEntity;
import org.opennms.netmgt.enlinkd.model.IsIsLinkTopologyEntity;
import org.opennms.netmgt.enlinkd.model.LldpElementTopologyEntity;
import org.opennms.netmgt.enlinkd.model.LldpLinkTopologyEntity;
import org.opennms.netmgt.enlinkd.model.NodeTopologyEntity;
import org.opennms.netmgt.enlinkd.model.OspfAreaTopologyEntity;
import org.opennms.netmgt.enlinkd.model.OspfLinkTopologyEntity;
import org.opennms.netmgt.enlinkd.model.SnmpInterfaceTopologyEntity;
```

Note: if any of those model imports are referenced elsewhere in the file (search for the class name before removing), keep the import. If `List` import was only used by the no-op return types, leave it — it's used by other `@Bean` method signatures in the file.

- [ ] **Step 4: Run the wiring test — confirm it passes**

Run: `./mvnw -q -pl :org.opennms.core.daemon-boot-enlinkd test -Dtest=TopologyEntityCacheWiringTest`
Expected: 3 tests PASS.

- [ ] **Step 5: Run the full module test suite — confirm no regressions**

Run: `./mvnw -q -pl :org.opennms.core.daemon-boot-enlinkd verify`
Expected: BUILD SUCCESS, all tests pass (the new IT + wiring test + any pre-existing).

If any pre-existing test fails, investigate whether the wiring change broke an unrelated bean. Most likely the topology service beans (lines 206–301 in `EnlinkdDaemonConfiguration`) silently start getting non-empty cache results, which is the intent — but if a test was asserting empty cache behavior, that test needs updating to reflect the new reality.

- [ ] **Step 6: Commit**

```bash
git add core/daemon-boot-enlinkd/src/main/java/org/deltav/netmgt/enlinkd/boot/EnlinkdDaemonConfiguration.java \
        core/daemon-boot-enlinkd/src/test/java/org/deltav/netmgt/enlinkd/boot/TopologyEntityCacheWiringTest.java
git commit -m "feat(enlinkd): wire real TopologyEntityCache + DAO beans

Replaces the anonymous no-op TopologyEntityCache that previously fed empty
lists to all 7 topology services (and through them to OnmsTopologyDao).
Now backed by TopologyEntityDaoJpa + delta-v's TopologyEntityCacheImpl.

Closes audit item #4 from project_daemon_nullop_audit_findings:
TopologyEntityCache no-op → real cache, ready for v1.3 UI/API consumer.

TTL is configurable via deltav.enlinkd.topology-cache.duration-seconds
(default 300s, matches horizon). Wiring test asserts bean types and
delegation; integration test (added in prior commit) exercises all 11
JPQL projections against a Testcontainers Postgres."
```

---

## Task 6: Rebuild all daemon-boot JARs + run E2E suite

Per `feedback_rebuild_all_daemons` and `feedback_e2e_continuous_validation_grpc_migration`, every commit on this branch must keep the E2E suite green.

- [ ] **Step 1: Clean stale .m2 SNAPSHOT artifacts**

Per `feedback_clean_m2_between_branches` — since we touched modules shared across the daemon boot graph, scrub stale snapshots:

```bash
find ~/.m2/repository/org/opennms/core -name '*.m2-SNAPSHOT' -type d -exec rm -rf {} + 2>/dev/null
find ~/.m2/repository/org/deltav -name '*.m2-SNAPSHOT' -type d -exec rm -rf {} + 2>/dev/null
```
Expected: no errors (the find may emit "No such file or directory" on a cold repo — harmless).

- [ ] **Step 2: Rebuild the touched modules and all 12 daemon-boot JARs**

```bash
cd /Users/david/development/src/opennms/delta-v
./mvnw -q -DskipTests install -pl :org.opennms.core.model-jakarta -am
./mvnw -q -DskipTests install -pl :org.opennms.core.daemon-boot-alarmd,\
:org.opennms.core.daemon-boot-bsmd,\
:org.opennms.core.daemon-boot-collectd,\
:org.opennms.core.daemon-boot-discovery,\
:org.opennms.core.daemon-boot-enlinkd,\
:org.opennms.core.daemon-boot-eventtranslator,\
:org.opennms.core.daemon-boot-perspectivepollerd,\
:org.opennms.core.daemon-boot-pollerd,\
:org.opennms.core.daemon-boot-provisiond,\
:org.opennms.core.daemon-boot-syslogd,\
:org.opennms.core.daemon-boot-telemetryd,\
:org.opennms.core.daemon-boot-trapd
```
Expected: BUILD SUCCESS for both invocations.

- [ ] **Step 3: Rebuild delta-v container images**

```bash
cd opennms-container/delta-v
./build.sh deltav
```
Expected: BUILD SUCCESS; all 12 daemon images rebuilt with the new enlinkd JAR.

- [ ] **Step 4: Run the full E2E suite**

```bash
cd opennms-container/delta-v
./test-e2e.sh
```

This is the umbrella E2E entry point that invokes the individual `test-*-e2e.sh` scripts. Expected: full suite passes at the current baseline (93/93 last confirmed per `project_passive_outages_fixed`).

If `test-e2e.sh` has changed shape since the last documented run, inspect it to confirm it covers at minimum: `test-enlinkd-e2e.sh`, `test-minion-rpc-e2e.sh`, `test-passive-e2e.sh`, `test-grpc-*-e2e.sh`. The enlinkd E2E is the most directly relevant regression detector for this PR.

- [ ] **Step 5: If any regression, investigate before continuing**

Triage rule of thumb:
- Enlinkd-related regression (LLDP/CDP/OSPF/IS-IS/Bridge tests fail) → most likely caused by this PR; bisect to a specific commit and investigate.
- Non-enlinkd regression (alarmd, provisiond, etc.) → likely pre-existing or environmental; capture details and ask the user.

Do NOT proceed to Task 7 with regressions. Either fix or escalate.

- [ ] **Step 6: Commit (only if any fixup was needed)**

If a fixup was needed, commit it with a clear message:
```bash
git add <fixup files>
git commit -m "fix(enlinkd): <describe the regression and the fix>"
```

If no fixup needed, skip this step.

---

## Task 7: Update audit memory + write v1.3 followup memory

These are memory-file edits in `/Users/david/.claude/projects/-Users-david-development-src-opennms-delta-v/memory/` — outside the git repo, so no `git add` / `git commit`.

- [ ] **Step 1: Update `project_daemon_nullop_audit_findings.md`**

Open `/Users/david/.claude/projects/-Users-david-development-src-opennms-delta-v/memory/project_daemon_nullop_audit_findings.md` and find the "Top priorities" section. Replace item 4:

OLD:
```
4. **TopologyEntityCache design decision** — small, but needs user input before we commit either direction.
```

NEW (substitute `<PR>` and `<SHA>` with actual values from Task 8):
```
4. **TopologyEntityCache fill** — ✅ DONE (PR #<PR>, merged <DATE>, squash commit `<SHA>`). Real JPA DAO + Guava cache replacing the anonymous no-op; producer side ready for v1.3 UI/API consumer. See `project_v1_3_topology_ui_resurrection`.
```

Also update the "Unclear — needs user judgment" section: delete the TopologyEntityCache subsection entirely (it's no longer unclear).

- [ ] **Step 2: Write `project_v1_3_topology_ui_resurrection.md`**

Create `/Users/david/.claude/projects/-Users-david-development-src-opennms-delta-v/memory/project_v1_3_topology_ui_resurrection.md`:

```markdown
---
name: v1.3 topology UI resurrection — producer side ready
description: Producer side of the topology graph (TopologyEntityDaoJpa + TopologyEntityCacheImpl wired in enlinkd) lands in v1.2.0. Consumer (Vue UI topology view or K8s operator topology endpoint) is the v1.3 deliverable.
type: project
---

# v1.3 topology UI resurrection — producer side ready, consumer pending

**State of the world after PR #<PR>:**
- `TopologyEntityCache` in enlinkd's Spring Boot context returns real data (was anonymous no-op).
- 7 topology services (`NodeTopologyService`, `CdpTopologyService`, `LldpTopologyService`, `OspfTopologyService`, `IsisTopologyService`, `BridgeTopologyService`, `UserDefinedLinkTopologyService`) get real cache hits.
- 9 topology updaters populate `OnmsTopologyDao` (in-memory graph) with non-empty entity collections.
- **`OnmsTopologyDao` has no consumer in delta-v.** The graph is built and held in memory but nobody reads it.

**What's still missing (v1.3 work):**
- Topology REST endpoint or gRPC service exposing `OnmsTopologyDao.getTopology(...)` to an external client.
- Vue UI component for topology view, OR K8s operator topology endpoint for the operator's UI.

**Why this matters:**
- The producer-side wiring is verified. The next person (a v1.3 contributor) can wire a consumer and get a working topology graph on day one, no debugging the cache or DAO.
- The cache TTL is configurable via `deltav.enlinkd.topology-cache.duration-seconds` (default 300s) — if the v1.3 UI needs faster turnaround, dial it down without code change.

**Where to look:**
- `core/daemon-boot-enlinkd/src/main/java/org/deltav/netmgt/enlinkd/boot/EnlinkdDaemonConfiguration.java` — bean wiring for the entire topology stack.
- `core/opennms-model-jakarta/src/main/java/org/opennms/netmgt/enlinkd/persistence/impl/TopologyEntityDaoJpa.java` — 11 JPQL projections (read-only).
- `core/daemon-boot-enlinkd/src/main/java/org/deltav/netmgt/enlinkd/persistence/cache/TopologyEntityCacheImpl.java` — Guava-backed cache.
- `OnmsTopologyDao` interface (in horizon `enlinkd.api` jar) and its `OnmsTopologyDaoInMemoryImpl` — the in-memory graph that a future consumer will read.

**Open question for v1.3:** is the in-memory graph the right surface, or should we publish topology updates to Kafka and let consumers subscribe? Depends on whether topology change events need to fan out to multiple consumers.

**Related:**
- `project_daemon_nullop_audit_findings` — parent audit (item #4)
- `project_v1_3_kubernetes_operator_inbound` — likely first consumer
- `project_module_restructure` — long-term shape of where topology code lives
```

- [ ] **Step 3: Update `MEMORY.md` index**

Open `/Users/david/.claude/projects/-Users-david-development-src-opennms-delta-v/memory/MEMORY.md` and add this line under "Future Architecture":

```
- [project_v1_3_topology_ui_resurrection.md](project_v1_3_topology_ui_resurrection.md) — FUTURE (v1.3): Producer side ready (enlinkd cache wired in PR #<PR>); consumer (Vue topology view or K8s operator topology API) is the v1.3 deliverable.
```

(Replace `<PR>` with the actual PR number once Task 8 opens it.)

---

## Task 8: Push branch + open PR

- [ ] **Step 1: Push the branch**

```bash
git push -u origin feat/v1.2.0-topology-entity-cache-fill
```
Expected: branch published to `pbrane/delta-v`.

- [ ] **Step 2: Open PR**

```bash
gh pr create --repo pbrane/delta-v --base develop --title "feat(v1.2.0): fill TopologyEntityCache (audit item #4)" --body "$(cat <<'EOF'
## Summary

- Replaces the anonymous no-op `TopologyEntityCache` bean in enlinkd's Spring Boot context with a real Guava-backed cache wrapping a new JPA-backed `TopologyEntityDao` implementation.
- Closes audit item #4 from \`project_daemon_nullop_audit_findings\` (the only one classified "Unclear — needs user judgment").
- Cache TTL is configurable via \`deltav.enlinkd.topology-cache.duration-seconds\` (default 300s).

## Why now

Producer side of the topology graph (cache → services → \`OnmsTopologyDao\`) is wired and verified. When a v1.3 consumer lands (Vue UI topology view or K8s operator topology endpoint), it can read from \`OnmsTopologyDao\` immediately — no cold-start debugging of the cache or DAO.

## What changed

- **New:** \`TopologyEntityDaoJpa\` — delta-v JPQL reimplementation of horizon's Spring 3-based \`TopologyEntityDaoHibernate\` (which is unusable on Spring 7).
- **New:** \`TopologyEntityCacheImpl\` (delta-v port) — same Guava LoadingCache pattern as horizon's, but with constructor-injected TTL and the delta-v package namespace.
- **Modified:** \`EnlinkdDaemonConfiguration\` — replaces the inline anonymous no-op with two real \`@Bean\` methods.

## Test plan

- [x] \`TopologyEntityCacheImplTest\` — 3 unit tests covering delegation, cache hits, all 11 getter wirings
- [x] \`TopologyEntityDaoJpaIT\` — Testcontainers Postgres IT, 11 tests asserting one fixture row per source table flows through each JPQL projection
- [x] \`TopologyEntityCacheWiringTest\` — 3 wiring assertions on \`EnlinkdDaemonConfiguration\`
- [x] All 12 daemon-boot JARs rebuilt (per \`feedback_rebuild_all_daemons\`)
- [x] Full E2E suite passes (per \`feedback_e2e_continuous_validation_grpc_migration\`)

## Spec

\`docs/superpowers/specs/2026-05-13-topology-entity-cache-fill-design.md\`
EOF
)"
```
Expected: PR URL printed; capture it.

- [ ] **Step 3: Record PR number in audit memory**

Edit the placeholders \`<PR>\` / \`<SHA>\` / \`<DATE>\` left in Task 7 Steps 1, 2, 3 with the actual PR number from Step 2 (SHA is set on merge; come back and fill it after the squash merge lands).

---

## Definition of Done

- [ ] All 6 implementation tasks committed on branch `feat/v1.2.0-topology-entity-cache-fill`.
- [ ] Branch pushed to `pbrane/delta-v` (NEVER `OpenNMS/opennms` per `feedback_never_pr_opennms`).
- [ ] PR opened against `pbrane/delta-v` `develop`.
- [ ] `project_daemon_nullop_audit_findings.md` updated: item #4 marked DONE.
- [ ] `project_v1_3_topology_ui_resurrection.md` written; `MEMORY.md` index updated.
- [ ] Full E2E suite still passes — no regressions in enlinkd or other daemons.
