# Provisiond `SnmpProfileMapper` Fill — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `NoOpSnmpProfileMapper` in provisiond with a real `SnmpProfileMapperImpl`, enabling `<snmp-profile>` filter-expression resolution during requisition import.

**Architecture:** Wire `SnmpProfileMapperImpl(filterDao, snmpAgentConfigFactory, locationAwareSnmpClient)` via constructor-injection factory method in `ProvisiondBootConfiguration`. Adds a new `filterDaoInitializer` bean replicating the collectd pattern. Deletes the NoOp stub. Single PR.

**Tech Stack:** Java 21, Spring Boot 4.0.3, Maven, JUnit 5, AssertJ, Mockito. Horizon JARs at version 1.0.13.

**Spec:** `docs/superpowers/specs/2026-05-11-provisiond-snmp-profile-mapper-fill-design.md`

**Branch:** `feat/v1.2.0-provisiond-snmpprofilemapper-fill` (already created at `9246e05f3be`; spec committed).

---

## Task 1: Add Maven dependency on `profile-mapper` module

**Files:**
- Modify: `core/daemon-boot-provisiond/pom.xml` (insert near the existing `org.opennms.core.snmp.proxy.rpc-impl` block around line 258)

- [ ] **Step 1: Read existing snmp dependency block for exclusion pattern**

Run:
```bash
sed -n '258,290p' core/daemon-boot-provisiond/pom.xml
```
Expected output: shows the `org.opennms.core.snmp.proxy.rpc-impl` dependency with multiple `<exclusion>` entries for ServiceMix bundles, hibernate-core, slf4j-api, pax-logging-api.

- [ ] **Step 2: Add `profile-mapper` dependency with conservative exclusions**

Edit `core/daemon-boot-provisiond/pom.xml`. Find the closing `</dependency>` immediately after the `org.opennms.core.snmp.proxy.rpc-impl` block (around line 275-280) and insert before it the closing tag, the new dependency:

```xml
        <!-- SNMP profile mapper — backs the SnmpProfileMapperImpl bean in provisiond -->
        <dependency>
            <groupId>org.opennms.core.snmp</groupId>
            <artifactId>org.opennms.core.snmp.profile-mapper</artifactId>
            <exclusions>
                <exclusion><groupId>org.apache.servicemix.bundles</groupId><artifactId>org.apache.servicemix.bundles.spring-beans</artifactId></exclusion>
                <exclusion><groupId>org.apache.servicemix.bundles</groupId><artifactId>org.apache.servicemix.bundles.spring-context</artifactId></exclusion>
                <exclusion><groupId>org.apache.servicemix.bundles</groupId><artifactId>org.apache.servicemix.bundles.spring-core</artifactId></exclusion>
                <exclusion><groupId>org.apache.servicemix.bundles</groupId><artifactId>org.apache.servicemix.bundles.spring-tx</artifactId></exclusion>
                <exclusion><groupId>org.slf4j</groupId><artifactId>slf4j-api</artifactId></exclusion>
                <exclusion><groupId>org.ops4j.pax.logging</groupId><artifactId>pax-logging-api</artifactId></exclusion>
            </exclusions>
        </dependency>
```

(Version inherits from the horizon BOM imported at the parent level; do not specify.)

- [ ] **Step 3: Run transitive-dependency audit (watchpoint #2)**

Run:
```bash
./mvnw -pl :org.opennms.core.daemon-boot-provisiond dependency:tree 2>&1 | \
    grep -iE 'servicemix|karaf|org\.osgi|aries|felix' | sort -u
```
Expected output: empty, OR any remaining entries are pre-existing transitive deps already present before this change (compare against develop's baseline if uncertain).

If new ServiceMix/OSGi/Karaf/Felix/Aries entries appear: add additional `<exclusion>` entries to the dependency block in step 2, matching the offending groupId+artifactId, then re-run this step.

- [ ] **Step 4: Compile**

Run:
```bash
./mvnw -pl :org.opennms.core.daemon-boot-provisiond -DskipTests clean compile 2>&1 | tail -5
```
Expected output: `BUILD SUCCESS`.

- [ ] **Step 5: Commit**

```bash
git add core/daemon-boot-provisiond/pom.xml
git commit -m "$(cat <<'EOF'
feat(v1.2.0): add org.opennms.core.snmp.profile-mapper dep to provisiond

Brings SnmpProfileMapperImpl onto the classpath in preparation for
replacing NoOpSnmpProfileMapper. Excludes ServiceMix and pax-logging
transitives to keep the Spring Boot 4 classpath clean.
EOF
)"
```

---

## Task 2: Add `filterDaoInitializer` bean + schema-loading helper

**Files:**
- Modify: `core/daemon-boot-provisiond/src/main/java/org/deltav/netmgt/provision/boot/ProvisiondBootConfiguration.java` (imports + new bean method + helper)

- [ ] **Step 1: Add imports for FilterDao + schema config**

Edit `ProvisiondBootConfiguration.java`. Find the existing imports (line ~30) and add the following alongside the existing `org.opennms.netmgt.config.api.SnmpAgentConfigFactory` import (keep alphabetical order):

```java
import org.opennms.netmgt.config.api.DefaultDatabaseSchemaConfig;
import org.opennms.netmgt.config.filter.DatabaseSchema;
import org.opennms.netmgt.filter.FilterDaoFactory;
import org.opennms.netmgt.filter.JdbcFilterDao;
import org.opennms.netmgt.filter.api.FilterDao;
```

- [ ] **Step 2: Add the `filterDaoInitializer` bean and `loadDatabaseSchemaConfig` helper**

Find the existing `@Bean public SnmpAgentConfigFactory snmpPeerFactory(...)` method (around line 241). Insert immediately AFTER that method's closing brace and BEFORE the existing `@Bean public SnmpProfileMapper snmpProfileMapper()` method:

```java
    /**
     * Initializes FilterDaoFactory with a JDBC-backed FilterDao.
     * Required by SnmpProfileMapperImpl (filter-expression evaluation in <snmp-profile>
     * elements) and by horizon code paths that still consult FilterDaoFactory.getInstance().
     *
     * <p>Pattern matches CollectdJpaConfiguration.filterDaoInitializer.
     * The bean name "filterDaoInitializer" is intentional so consumers can use
     * @DependsOn("filterDaoInitializer") to guarantee the static-singleton side
     * effect ran before they resolve.</p>
     */
    @Bean
    public JdbcFilterDao filterDaoInitializer(DataSource dataSource) {
        LOG.info("Initializing FilterDaoFactory with JdbcFilterDao");
        var jdbcFilterDao = new JdbcFilterDao();
        jdbcFilterDao.setDataSource(dataSource);
        var schemaConfig = loadDatabaseSchemaConfig();
        jdbcFilterDao.setDatabaseSchemaConfigFactory(schemaConfig);
        jdbcFilterDao.afterPropertiesSet();
        FilterDaoFactory.setInstance(jdbcFilterDao);
        return jdbcFilterDao;
    }

    private DefaultDatabaseSchemaConfig loadDatabaseSchemaConfig() {
        try (var is = getClass().getResourceAsStream("/database-schema.xml")) {
            if (is == null) {
                throw new IllegalStateException(
                        "database-schema.xml not found on classpath — expected from opennms-config jar");
            }
            var schema = XML_MAPPER.readValue(is, DatabaseSchema.class);
            return new DefaultDatabaseSchemaConfig(schema);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to load database-schema.xml from classpath", e);
        }
    }
```

(Verify `DataSource` import is already present — it is, at line 26.)

- [ ] **Step 3: Compile**

Run:
```bash
./mvnw -pl :org.opennms.core.daemon-boot-provisiond -DskipTests clean compile 2>&1 | tail -5
```
Expected output: `BUILD SUCCESS`.

- [ ] **Step 4: Commit**

```bash
git add core/daemon-boot-provisiond/src/main/java/org/deltav/netmgt/provision/boot/ProvisiondBootConfiguration.java
git commit -m "$(cat <<'EOF'
feat(v1.2.0): add filterDaoInitializer bean to provisiond

JdbcFilterDao + DatabaseSchemaConfigFactory wiring, replicating the
collectd pattern. Required by the upcoming real SnmpProfileMapperImpl
bean and by horizon code paths that still call FilterDaoFactory.getInstance().
The database-schema.xml resource is provided by the transitive opennms-config
JAR dependency — no overlay file required.
EOF
)"
```

---

## Task 3: Rewire `snmpProfileMapper` bean to real impl

**Files:**
- Modify: `core/daemon-boot-provisiond/src/main/java/org/deltav/netmgt/provision/boot/ProvisiondBootConfiguration.java` (replace bean body, add `@DependsOn`, update imports)

- [ ] **Step 1: Add imports**

Edit `ProvisiondBootConfiguration.java`. Add (keep alphabetical):

```java
import org.opennms.core.snmp.profile.mapper.impl.SnmpProfileMapperImpl;
import org.springframework.context.annotation.DependsOn;
```

**Keep** `import org.opennms.netmgt.snmp.SnmpProfileMapper;` — the interface is still the bean return type.

`NoOpSnmpProfileMapper` is in the same package (`org.deltav.netmgt.provision.boot`), so there is no import line for it to remove (same-package classes aren't imported in Java).

- [ ] **Step 2: Replace the `snmpProfileMapper` bean method**

Find:
```java
    @Bean
    public SnmpProfileMapper snmpProfileMapper() {
        return new NoOpSnmpProfileMapper();
    }
```
(at `ProvisiondBootConfiguration.java:250-253`)

Replace with:
```java
    @Bean
    @DependsOn("filterDaoInitializer")
    public SnmpProfileMapper snmpProfileMapper(
            FilterDao filterDao,
            SnmpAgentConfigFactory snmpAgentConfigFactory,
            LocationAwareSnmpClient locationAwareSnmpClient) {
        return new SnmpProfileMapperImpl(filterDao, snmpAgentConfigFactory, locationAwareSnmpClient);
    }
```

- [ ] **Step 3: Compile**

Run:
```bash
./mvnw -pl :org.opennms.core.daemon-boot-provisiond -DskipTests clean compile 2>&1 | tail -5
```
Expected output: `BUILD SUCCESS`.

If `cannot find symbol: class NoOpSnmpProfileMapper`: that's expected only if the next task already deleted it — for this task it should still compile because we haven't deleted the file yet.

- [ ] **Step 4: Commit**

```bash
git add core/daemon-boot-provisiond/src/main/java/org/deltav/netmgt/provision/boot/ProvisiondBootConfiguration.java
git commit -m "$(cat <<'EOF'
feat(v1.2.0): replace NoOpSnmpProfileMapper with real SnmpProfileMapperImpl

Constructor-injection factory method wires the horizon impl with all three
runtime deps. @DependsOn("filterDaoInitializer") guarantees the
FilterDaoFactory static singleton is initialized before any consumer
resolves the SnmpProfileMapper bean.
EOF
)"
```

---

## Task 4: Delete `NoOpSnmpProfileMapper.java`

**Files:**
- Delete: `core/daemon-boot-provisiond/src/main/java/org/deltav/netmgt/provision/boot/NoOpSnmpProfileMapper.java`

- [ ] **Step 1: Verify no remaining references**

Run:
```bash
grep -rn "NoOpSnmpProfileMapper" core/ 2>/dev/null | grep -v target | grep -v ".m2"
```
Expected output: only the file itself (`core/daemon-boot-provisiond/src/main/java/org/deltav/netmgt/provision/boot/NoOpSnmpProfileMapper.java`) — no other source file references it.

If anything else shows up: investigate and remove the reference before continuing.

- [ ] **Step 2: Delete the file**

Run:
```bash
git rm core/daemon-boot-provisiond/src/main/java/org/deltav/netmgt/provision/boot/NoOpSnmpProfileMapper.java
```

If `git rm` fails (e.g. the file isn't tracked because the working tree drifted), fall back to:
```bash
rm core/daemon-boot-provisiond/src/main/java/org/deltav/netmgt/provision/boot/NoOpSnmpProfileMapper.java
git add -u core/daemon-boot-provisiond/src/main/java/org/deltav/netmgt/provision/boot/
```

- [ ] **Step 3: Compile**

Run:
```bash
./mvnw -pl :org.opennms.core.daemon-boot-provisiond -DskipTests clean compile 2>&1 | tail -5
```
Expected output: `BUILD SUCCESS`.

- [ ] **Step 4: Commit**

```bash
git commit -m "$(cat <<'EOF'
chore(v1.2.0): delete NoOpSnmpProfileMapper — dead after wiring real impl

No backward-compat shim, no @Deprecated. Per feedback_karaf_is_dead, dead
horizon-era stubs go away rather than hanging around as test fixtures.
The wiring test in the next task uses Mockito mocks of the interface, not
the deleted concrete class.
EOF
)"
```

---

## Task 5: Add wiring test

**Files:**
- Create: `core/daemon-boot-provisiond/src/test/java/org/deltav/netmgt/provision/boot/SnmpProfileMapperWiringTest.java`

- [ ] **Step 1: Write the test file**

Create `core/daemon-boot-provisiond/src/test/java/org/deltav/netmgt/provision/boot/SnmpProfileMapperWiringTest.java`:

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
package org.deltav.netmgt.provision.boot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetAddress;
import java.util.Collections;

import org.junit.jupiter.api.Test;
import org.opennms.core.snmp.profile.mapper.impl.SnmpProfileMapperImpl;
import org.opennms.netmgt.config.api.SnmpAgentConfigFactory;
import org.opennms.netmgt.filter.api.FilterDao;
import org.opennms.netmgt.snmp.SnmpProfileMapper;
import org.opennms.netmgt.snmp.proxy.LocationAwareSnmpClient;

/**
 * Wiring-only test for ProvisiondBootConfiguration.snmpProfileMapper.
 * Asserts the bean is the real horizon impl (not the deleted NoOp) and that
 * the empty-profile-list smoke path returns Optional.empty().
 * No Spring context, no DataSource, no Postgres.
 */
class SnmpProfileMapperWiringTest {

    @Test
    void snmpProfileMapperBeanIsRealImplWithAllDepsInjected() {
        FilterDao filterDao = mock(FilterDao.class);
        SnmpAgentConfigFactory configFactory = mock(SnmpAgentConfigFactory.class);
        when(configFactory.getProfiles()).thenReturn(Collections.emptyList());
        LocationAwareSnmpClient snmpClient = mock(LocationAwareSnmpClient.class);

        SnmpProfileMapper mapper = new ProvisiondBootConfiguration()
                .snmpProfileMapper(filterDao, configFactory, snmpClient);

        assertThat(mapper).isInstanceOf(SnmpProfileMapperImpl.class);
    }

    @Test
    void getAgentConfigFromProfilesReturnsEmptyWhenNoProfilesConfigured() throws Exception {
        FilterDao filterDao = mock(FilterDao.class);
        SnmpAgentConfigFactory configFactory = mock(SnmpAgentConfigFactory.class);
        when(configFactory.getProfiles()).thenReturn(Collections.emptyList());
        LocationAwareSnmpClient snmpClient = mock(LocationAwareSnmpClient.class);

        SnmpProfileMapper mapper = new ProvisiondBootConfiguration()
                .snmpProfileMapper(filterDao, configFactory, snmpClient);

        var result = mapper
                .getAgentConfigFromProfiles(InetAddress.getLoopbackAddress(), "Default", null)
                .get();

        assertThat(result).isEmpty();
    }
}
```

- [ ] **Step 2: Run the test**

Run:
```bash
./mvnw -pl :org.opennms.core.daemon-boot-provisiond test -Dtest=SnmpProfileMapperWiringTest 2>&1 | tail -20
```
Expected output: `Tests run: 2, Failures: 0, Errors: 0, Skipped: 0` and `BUILD SUCCESS`.

If failure with `NoClassDefFoundError: org/opennms/core/snmp/profile/mapper/impl/SnmpProfileMapperImpl`: Task 1's Maven dep was not actually picked up. Re-run `./mvnw -pl :org.opennms.core.daemon-boot-provisiond clean compile` and recheck.

If failure with `NullPointerException` constructing `SnmpProfileMapperImpl`: one of the mocks is null (shouldn't happen given the test as written). Inspect carefully.

- [ ] **Step 3: Commit**

```bash
git add core/daemon-boot-provisiond/src/test/java/org/deltav/netmgt/provision/boot/SnmpProfileMapperWiringTest.java
git commit -m "$(cat <<'EOF'
test(v1.2.0): add wiring test for ProvisiondBootConfiguration.snmpProfileMapper

Two JUnit cases: instanceof SnmpProfileMapperImpl, and empty-profile-list
smoke path returns Optional.empty(). Pure JUnit + Mockito + AssertJ; no
Spring context, no DataSource, no Postgres. Acts as a regression detector
should the NoOp creep back in.
EOF
)"
```

---

## Task 6: Verify existing provisiond tests still pass (watchpoint #1)

**Files:**
- (no edits — verification step)
- (potential modify: tests under `core/daemon-boot-provisiond/src/test/java/` that boot a Spring context, if any break)

- [ ] **Step 1: Run the full provisiond test suite**

Run:
```bash
./mvnw -pl :org.opennms.core.daemon-boot-provisiond test 2>&1 | tail -30
```
Expected output: `BUILD SUCCESS`. Tests pass count must be >= the count on `develop`. New count = old count + 2 (the wiring tests added in Task 5).

- [ ] **Step 2: If anything fails, classify the failure**

If a test fails with `NoSuchBeanDefinitionException: FilterDao` or `BeanCreationException: filterDaoInitializer`: the test boots `ProvisiondBootConfiguration` without a `DataSource`. Mitigate with `@MockBean(FilterDao.class)` on the failing test class.

If a test fails with `Cannot load JDBC driver class` or similar Postgres-related: the test has a partial DataSource. Use `@MockBean(JdbcFilterDao.class)` to skip the bean construction.

If a test fails with `database-schema.xml not found`: shouldn't happen — the transitive `opennms-config` dep is present in test scope. Re-check Maven dep tree.

If a test fails for an unrelated reason: investigate; not part of this fill.

- [ ] **Step 3: Apply mitigation (if needed)**

For each failing test that needs `@MockBean`, edit the test class and add to the class-level annotations:

```java
@MockBean(FilterDao.class)
```

(import: `org.springframework.boot.test.mock.mockito.MockBean`)

Re-run step 1 to confirm green.

- [ ] **Step 4: Commit (only if mitigations were applied)**

```bash
git add core/daemon-boot-provisiond/src/test/java/
git commit -m "$(cat <<'EOF'
test(v1.2.0): mock FilterDao in existing provisiond ITs

The new filterDaoInitializer bean would otherwise require a DataSource the
ITs don't provide. @MockBean(FilterDao.class) sidesteps JdbcFilterDao
construction in tests that don't exercise the filter path.
EOF
)"
```

If no mitigations were needed (most likely): skip this step. Note the green test run in the PR description as evidence for watchpoint #1.

---

## Task 7: Verify JdbcFilterDao boot path is lazy (watchpoint #3)

**Files:**
- (no edits — read-only verification)

- [ ] **Step 1: Read `JdbcFilterDao.afterPropertiesSet`**

Run:
```bash
find /Users/david/development/src/opennms/delta-v-horizon -path '*/filter/*/JdbcFilterDao.java' -not -path '*/target/*' 2>/dev/null
```
Note the path. Then open that file and locate `afterPropertiesSet()`.

- [ ] **Step 2: Confirm no JDBC query in the boot path**

Read the method body. Acceptable: input validation, field assignment, registration of listeners. **Unacceptable: `getConnection()`, `executeQuery()`, `JdbcTemplate.query*`, or any SQL string execution.**

Also check the constructor and `setDataSource` / `setDatabaseSchemaConfigFactory` setters for the same.

- [ ] **Step 3: Record the finding**

If the assumption holds (no boot-time query): note this in the PR description with the exact file:line citation.

If the assumption is violated:
- Look for a "Postgres-ready" probe bean in `core/daemon-common/.../DaemonProvisioningConfiguration.java` or similar.
- Add `@DependsOn("...")` to `filterDaoInitializer` to defer until DB readiness.
- Or wrap `JdbcFilterDao` in a lazy proxy that defers construction until first use.

This is a verification step, not a code change. No commit needed unless step 3 mitigation is required — in which case open a separate task and amend this plan.

---

## Task 8: Local container build + smoke

**Files:**
- (no edits — verification step)

- [ ] **Step 1: Clean install at module + daemon-boot level**

Run (per `feedback_spring_boot_repackage_needs_clean` and `feedback_rebuild_all_daemons`):
```bash
./mvnw -pl :org.opennms.core.daemon-boot-provisiond -am clean install -DskipTests 2>&1 | tail -5
```
Expected output: `BUILD SUCCESS`.

- [ ] **Step 2: Build the deltav images**

Run:
```bash
./build.sh deltav 2>&1 | tail -10
```
Expected output: `Successfully tagged` lines for the daemon images, including provisiond.

- [ ] **Step 3: Bring up docker compose (Mac dev env per `feedback_smoke_vm_release_only`)**

Run:
```bash
docker compose down -v 2>/dev/null; docker compose up -d 2>&1 | tail -20
```
Wait for stabilization. Then:
```bash
sleep 60 && docker compose ps --format 'table {{.Service}}\t{{.Status}}' | grep -i provisiond
```
Expected output: `provisiond` shows `Up X seconds (healthy)`.

- [ ] **Step 4: Confirm no boot-time errors in provisiond logs**

Run:
```bash
docker compose logs provisiond 2>&1 | grep -iE 'error|exception|caused by' | grep -v 'WARN' | head -20
```
Expected output: empty, OR only pre-existing warnings unrelated to this fill (e.g., recoverable Kafka reconnect messages). Look specifically for `FilterDao`, `database-schema`, `SnmpProfileMapper` strings — none should appear in error context.

- [ ] **Step 5: Verify FilterDao + SnmpProfileMapper beans booted**

Run:
```bash
docker compose logs provisiond 2>&1 | grep -E 'Initializing FilterDaoFactory|SnmpProfileMapper' | head -5
```
Expected output: `INFO ... Initializing FilterDaoFactory with JdbcFilterDao` (from our new `LOG.info` line).

- [ ] **Step 6: Tear down**

```bash
docker compose down -v
```

No commit. This task produces evidence for the PR description.

---

## Task 9: Push branch, open PR

**Files:**
- (no edits — branch operation)

- [ ] **Step 1: Verify branch state**

Run:
```bash
git log --oneline develop..HEAD
```
Expected: 5–6 commits on this feature branch (1 spec, 1 spec correction, 1 watchpoints, plus 1 commit each for Tasks 1–5 and optionally Task 6).

- [ ] **Step 2: Push branch**

Run:
```bash
git push -u origin feat/v1.2.0-provisiond-snmpprofilemapper-fill
```
Expected output: branch pushed; `gh` URL printed.

- [ ] **Step 3: Open PR against `pbrane/delta-v` (NOT OpenNMS — per project CLAUDE.md "CRITICAL: Git Remote Rules")**

Run:
```bash
gh pr create --repo pbrane/delta-v --base develop \
    --title "feat(v1.2.0): wire real SnmpProfileMapper in provisiond" \
    --body "$(cat <<'EOF'
## Summary
- Replaces `NoOpSnmpProfileMapper` in provisiond with the real `SnmpProfileMapperImpl` horizon class.
- Adds `filterDaoInitializer` bean (JdbcFilterDao + database-schema.xml from classpath) per the collectd pattern.
- Deletes `NoOpSnmpProfileMapper.java` — no backward-compat shim.
- Adds wiring-only test asserting the bean type + empty-profile-list smoke path.

## Why
v1.2.0 requirement G (Daemon null-op audit, full sweep) priority 1b. The original audit misclassified this as one of four `SnmpProfileMapper × 4` items; verification revealed the `× 4` was actually `TextEncryptor × 4` (deferred to v1.3 K8s-native config) and the real `SnmpProfileMapper` debt is one location: provisiond. Spec + watchpoints in `docs/superpowers/specs/2026-05-11-provisiond-snmp-profile-mapper-fill-design.md`.

## Watchpoint verification
1. **Test context pollution:** [fill in: 'no existing ITs broke' or 'mitigated N tests with @MockBean(FilterDao.class)']
2. **Transitive deps:** `dependency:tree` audit added no new ServiceMix/OSGi/Karaf entries (or: added these specific exclusions)
3. **JdbcFilterDao lazy boot:** verified by reading `delta-v-horizon/.../JdbcFilterDao.java`: [fill in: 'no JDBC query in afterPropertiesSet or setters']

## Test plan
- [ ] Wiring test passes (`mvn -pl :org.opennms.core.daemon-boot-provisiond test -Dtest=SnmpProfileMapperWiringTest`)
- [ ] All existing provisiond tests pass (`mvn -pl :org.opennms.core.daemon-boot-provisiond test`)
- [ ] Local container build succeeds (`./build.sh deltav`)
- [ ] `docker compose up -d` shows provisiond healthy
- [ ] No `FilterDao` / `database-schema` / `SnmpProfileMapper` errors in provisiond logs at boot
- [ ] Boot log contains `Initializing FilterDaoFactory with JdbcFilterDao`
- [ ] E2E suite remains green (per `feedback_e2e_continuous_validation_grpc_migration`)
EOF
)"
```
Expected output: PR URL printed.

- [ ] **Step 4: Capture the PR URL for the user**

The PR URL is the deliverable of this plan. Surface it back to the user.

---

## Post-merge follow-up (NOT part of this PR; track separately)

After this PR merges, update `project_daemon_nullop_audit_findings.md` to mark item 1b as DONE with the PR URL. Do not do this in the same PR — memory updates are separate from code merges.
