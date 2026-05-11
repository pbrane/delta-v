# Provisiond `SnmpProfileMapper` Fill — Design Spec

**Date:** 2026-05-11
**Context:** v1.2.0 requirement G (Daemon null-op audit, full sweep) → fill priority 1b.
**Scope:** Replace `NoOpSnmpProfileMapper` in provisiond with a real `SnmpProfileMapperImpl`, enabling `<snmp-profile>` filter expressions during requisition import / node scan.
**Out of scope:** TextEncryptor wiring across 4 daemons (deferred to v1.3; see `project_v1_3_kubernetes_native_configuration`); TracerRegistry × 2 (v1.2.0 requirement G priority 2); alarmd lifecycle decisions.

## Background

The 2026-05-09 daemon null-op audit (memory: `project_daemon_nullop_audit_findings`) initially classified two stubs as a single batch fix called "SnmpProfileMapper × 4 daemons." On follow-up verification (2026-05-09 evening, this session), the `× 4` finding was a constructor-arg mis-identification — the `null` third arg of `new SnmpPeerFactory(config, entityScopeProvider, null)` is `TextEncryptor`, not `SnmpProfileMapper`. The actual `SnmpProfileMapper` debt is one location: provisiond's `NoOpSnmpProfileMapper` bean at `core/daemon-boot-provisiond/.../ProvisiondBootConfiguration.java:251-253`.

Horizon's `DefaultProvisionService` calls `SnmpProfileMapper.getAgentConfigFromProfiles(...)` during `NodeInfoScan` and `NodeScan`. Delta-V's NoOp drops profile-based credential resolution silently — falls through to static `snmp-config.xml` lookup. No fixtures, tests, or E2E paths in delta-v exercise `<snmp-profile>` elements today, so the NoOp is harmless in the running deployment but blocks the feature from working when a user adds profile expressions.

## Goal

Wire `SnmpProfileMapperImpl(filterDao, snmpAgentConfigFactory, locationAwareSnmpClient)` into provisiond's Spring context so profile-based credential resolution works end-to-end. Smoke-test the wiring; defer real-profile E2E to the feature-adoption track.

## Verification before scoping (lessons from `feedback_audit_verify_ctor_signatures`)

Constructor signatures verified against current source on 2026-05-09:

| Component | Signature (verified at) | Status in delta-v provisiond |
|---|---|---|
| `SnmpPeerFactory(SnmpConfig, EntityScopeProvider, TextEncryptor)` | `delta-v-horizon/core/snmp/config/.../SnmpPeerFactory.java:144` | Third arg null — **deferred to v1.3** |
| `SnmpProfileMapperImpl(FilterDao, SnmpAgentConfigFactory, LocationAwareSnmpClient)` | `delta-v-horizon/core/snmp/profile-mapper/.../SnmpProfileMapperImpl.java:66-70` | Bean stubbed with `NoOpSnmpProfileMapper` — **filled by this spec** |
| Maven dep `org.opennms.core.snmp.profile-mapper` (1.0.13) | `delta-v-horizon/core/snmp/profile-mapper/pom.xml` | **Not declared in provisiond pom** — added by this spec |

Required runtime deps in provisiond's Spring context:

| Dep | Present? | Source |
|---|---|---|
| `SnmpAgentConfigFactory` | Yes | `ProvisiondBootConfiguration.java:241` (real bean) |
| `LocationAwareSnmpClient` | Yes | `ProvisiondBootConfiguration.java:264` (RPC-backed) |
| `FilterDao` | **No** | Must be added (new `filterDaoInitializer` bean) |
| `DataSource` | Yes | Existing daemon-common bean (HikariCP) |
| `database-schema.xml` on classpath | **Yes (transitive)** | `opennms-config:1.0.13` already in provisiond's dep tree (transitive via existing deps) bundles `/database-schema.xml` as a classpath resource — same path collectd uses. **No overlay file required.** Verified 2026-05-11 via `./mvnw -pl :org.opennms.core.daemon-boot-provisiond dependency:list`. |

## Approach (selected: Path A — full FilterDao + database-schema.xml)

Path A was selected over Path B (stub `FilterDao` throwing on non-empty filter expressions) and Path C (revert to INTENTIONAL NoOp) because filter-based profile selection is the only useful form of the feature; a half-impl that silently drops `filter="..."` expressions would create user confusion worse than the current NoOp.

## File-level changes

```
core/daemon-boot-provisiond/pom.xml
    + <dependency> on org.opennms.core.snmp:org.opennms.core.snmp.profile-mapper
      (version inherits from horizon BOM)

core/daemon-boot-provisiond/src/main/java/org/deltav/netmgt/provision/boot/
    ProvisiondBootConfiguration.java
        + import: FilterDao, JdbcFilterDao, FilterDaoFactory,
                  DatabaseSchemaConfigFactory, SnmpProfileMapperImpl
        - import: NoOpSnmpProfileMapper (deleted)
        + new bean: filterDaoInitializer(DataSource)
            — pattern matches CollectdJpaConfiguration.filterDaoInitializer
            — calls FilterDaoFactory.setInstance(this) for static-singleton side effect
            — bean name "filterDaoInitializer" intentional (for @DependsOn)
        + new helper: loadDatabaseSchemaConfig() reading etc/database-schema.xml via Jackson XmlMapper
        ~ rewired bean: snmpProfileMapper(FilterDao, SnmpAgentConfigFactory, LocationAwareSnmpClient)
            — returns new SnmpProfileMapperImpl(filterDao, factory, client)
            — annotated @DependsOn("filterDaoInitializer") so the FilterDaoFactory
              static singleton is set before any consumer needs it

core/daemon-boot-provisiond/src/main/java/org/deltav/netmgt/provision/boot/
    NoOpSnmpProfileMapper.java
        — DELETED. No backward-compatibility shim; no @Deprecated. Dead per
          feedback_karaf_is_dead.

(no overlay file needed — see "database-schema.xml on classpath" in
 the table above; transitive opennms-config:1.0.13 dep bundles it)

core/daemon-boot-provisiond/src/test/java/org/deltav/netmgt/provision/boot/
    SnmpProfileMapperWiringTest.java
        + NEW. Pure JUnit + Mockito + AssertJ; no Spring context, no DataSource,
          no Postgres. Two tests:
          1. snmpProfileMapperBeanIsRealImplWithAllDepsInjected
          2. getAgentConfigFromProfilesReturnsEmptyWhenNoProfilesConfigured

core/daemon-boot-provisiond/src/test/resources/etc/
    database-schema.xml
        + MAY be added if existing IT Spring contexts (e.g., NodeContextProducerSpringContextIT)
          require filterDaoInitializer to boot successfully. Verified during implementation.
```

## Architecture

```
                    ┌────────────────────────────────────┐
                    │  DefaultProvisionService           │
                    │  (horizon, unchanged)              │
                    └──┬─────────────────────────────────┘
                       │ injected
                       ▼
                    ┌────────────────────────────────────┐
                    │  SnmpProfileMapperImpl             │   horizon class on classpath
                    │  (replaces NoOpSnmpProfileMapper)  │   via new Maven dep
                    └──┬──────────┬─────────────┬────────┘
                       │          │             │
                       ▼          ▼             ▼
                  FilterDao  SnmpAgentConfig  LocationAware
                  (NEW)      Factory          SnmpClient
                  JdbcFilter (existing)       (existing,
                  Dao bean   bean             RPC-backed)
                       │
                       ▼
                  DataSource (existing) + database-schema.xml (NEW overlay file)
```

All bean wiring uses constructor-injection factory methods (per project CLAUDE.md convention and `feedback_*` rules). No `@Autowired` field injection introduced.

## Data Flow

### Boot-time

1. `DataSource` bean resolves (already wired in daemon-common).
2. `filterDaoInitializer(DataSource)` runs — constructs `JdbcFilterDao`, sets DataSource, loads `/database-schema.xml` from the **classpath** (transitive `opennms-config` JAR) via Jackson XmlMapper into `DatabaseSchemaConfigFactory`, calls `afterPropertiesSet()`, then calls `FilterDaoFactory.setInstance(this)` for the horizon static-singleton side effect.
3. `snmpProfileMapper(FilterDao, SnmpAgentConfigFactory, LocationAwareSnmpClient)` runs — constructs `SnmpProfileMapperImpl`, which calls `Objects.requireNonNull` on each dep (fail-fast if wiring is broken).
4. `defaultProvisionService(..., SnmpProfileMapper)` receives the real impl, not the NoOp.

### Runtime — profile resolution during NodeInfoScan / NodeScan

```
SnmpProfileMapper.getAgentConfigFromProfiles(inetAddress, location, oid)
    │
    ▼
agentConfigFactory.getProfiles()                    ← <snmp-profile> elements from snmp-config.xml
    │
    ▼
for each profile:
    filterDao.isValid(ipAddr, profile.filterExpression)   ← JDBC query against PostgreSQL
        — empty filterExpression: always matches
        — invalid filterExpression: FilterParseException caught, WARN logged, treated as no-match
    │ (matched profiles only)
    ▼
locationAwareSnmpClient
    .get(agentConfigFromProfile, sysObjectIdOID)
    .withLocation(location)
    .execute()                                     ← RPC to Minion at named location
    │
    ▼
first profile that returns non-null SNMP value wins
    │
    ▼
CompletableFuture<Optional<SnmpAgentConfig>>
```

### Behavior parity

- **No-profile users** (today's only delta-v deployments): zero behavior change. Empty `<profiles>` list → empty futures list → immediate `Optional.empty()` → caller falls through to static lookup.
- **Future profile users**: can add `<snmp-profile filter="ipaddr LIKE '10.%'" .../>` elements to `provisiond-overlay/etc/snmp-config.xml`. Provisiond resolves credentials dynamically during requisition import.

## Error Handling

### Boot-time

| Failure | Outcome |
|---|---|
| `/database-schema.xml` not on classpath | `filterDaoInitializer` throws at boot → daemon fails fast → container orchestrator surfaces via restart-count. Cannot occur in practice because `opennms-config:1.0.13` is a transitive dep that always carries the resource. **Correct behavior** if it ever does — fail-fast, no silent fallback. |
| `/database-schema.xml` malformed | Same — Jackson XmlMapper throws `JsonProcessingException` at boot. (Schema lives in a versioned horizon JAR, so malformation is a horizon-source bug, not a delta-v config drift.) |
| `DataSource` not yet available | Cannot occur unless someone forces eager init; Spring resolves DataSource bean before `filterDaoInitializer` via DI ordering. |
| `JdbcFilterDao.afterPropertiesSet()` fails (schema config invalid) | Bubbles up; daemon fails fast (matches collectd behavior at `CollectdJpaConfiguration.java:199`). |
| Postgres unreachable at boot | `JdbcFilterDao` is lazy — DataSource not queried until first `isValid()` call. Boot succeeds; Postgres dependency deferred to runtime. |

**No silent NoOp fallback.** The whole point of this fill is to remove the NoOp; resurrecting it on boot failure would undo the audit's intent.

### Runtime — handled by horizon's `SnmpProfileMapperImpl`, not modified by this fill

| Concern | Horizon handling | Delta-V rule | Compliant? |
|---|---|---|---|
| Filter parse error | `isFilterExpressionValid` catches `FilterParseException`, logs WARN, returns false | N/A | Yes |
| Filter eval Postgres timeout | Same path — `FilterParseException` covers JDBC failures | N/A | Yes |
| Minion RPC timeout for `sysObjectID get` | `fitProfile` completes future with `Optional.empty()` on throwable | `feedback_rpc_timeout_no_outages`: RPC failures must NOT cause outages/fault events | Yes — code returns Optional, no event emission |
| All profiles fail to match | `getAgentConfigFromProfiles` completes with `Optional.empty()` | Caller falls back to static lookup | Yes |
| Empty profile list | Immediate `Optional.empty()` | Identical to today's NoOp | Yes |

### Observability

Horizon's impl emits two log lines:

- `LOG.info("Exception while doing SNMP get on OID '{}' with profile '{}'", ...)` at `SnmpProfileMapperImpl.java:127`. Fires per profile per failure. Could be noisy if Minion is flaky and many profiles are in play. Acceptable for v1.2.0; flag for log-level tuning later if it becomes a signal-to-noise problem.
- `LOG.warn("Filter expression '{}' is invalid", ...)` at line 165. Fires per misconfigured profile. Low frequency expected.

**No new Micrometer meters in this fill.** Profile-mapper hit/miss metrics belong with `project_v1_2_app_observability` Phase 2 (per-daemon business signal audit), not here.

## Testing

Wiring-only depth (no Postgres, no Spring context):

```java
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
    /* same mocks */
    var result = mapper
            .getAgentConfigFromProfiles(InetAddress.getLoopbackAddress(), "Default", null)
            .get();
    assertThat(result).isEmpty();
}
```

### Coverage matrix

| Concern | Caught? | How |
|---|---|---|
| Maven dep on `profile-mapper` present | Yes | `SnmpProfileMapperImpl` import must resolve at compile-time |
| Factory method returns real impl, not NoOp | Yes | `instanceof SnmpProfileMapperImpl` assertion |
| All three deps passed non-null | Yes | `SnmpProfileMapperImpl` ctor uses `Objects.requireNonNull` — would NPE if any null |
| Empty profile list → `Optional.empty()` | Yes | Direct assertion |
| `JdbcFilterDao.afterPropertiesSet()` actually works | **No** (out of scope) | Caught at daemon-boot time in CI via container health check |
| Filter expression evaluation against Postgres | **No** (out of scope) | Belongs to feature-adoption track |
| Real Minion RPC | **No** (out of scope) | `LocationAwareSnmpClient` mocked |

### Existing test compatibility

`NodeContextProducerSpringContextIT` and other provisiond ITs that boot a Spring context now will also boot `filterDaoInitializer`. If those tests use Testcontainers Postgres, no impact. If they mock the DataSource, the bean construction may still need `database-schema.xml` on the test classpath. Implementation will verify; if needed, add `src/test/resources/etc/database-schema.xml` (byte copy).

### Pre-merge verification (beyond JUnit)

1. `./mvnw -pl :daemon-boot-provisiond clean install` succeeds.
2. `build.sh deltav` succeeds with new overlay file present (per `feedback_rebuild_all_daemons` — rebuild all 12 daemon boot JARs).
3. Local `docker compose up -d` shows provisiond health-check green on Mac dev env (per `feedback_smoke_vm_release_only` — dev validation on Mac, not the smoke VM).
4. Existing E2E suite still passes (per `feedback_e2e_continuous_validation_grpc_migration`).

## PR shape

- Branch: `feat/v1.2.0-provisiond-snmpprofilemapper-fill` off `develop`.
- Target: `--repo pbrane/delta-v --base develop` (per project CLAUDE.md "CRITICAL: Git Remote Rules").
- Commit style: Conventional Commits, e.g. `feat(v1.2.0): wire real SnmpProfileMapper in provisiond (#XXX)`.
- Single PR; small enough to review as one diff.

## What this fill does NOT include

- **TextEncryptor wiring** in 4 daemons (collectd/enlinkd/pollerd/perspectivepollerd) — deferred to v1.3 alongside K8s-native credential delivery decision. See `project_v1_3_kubernetes_native_configuration`.
- **Profile-based feature documentation for users** — the capability becomes available; user-facing docs ("how to write `<snmp-profile filter=...>` in delta-v") are a separate track once a real consumer surfaces.
- **DRY cleanup of duplicated `database-schema.xml`** across 4 daemon overlays — separate cleanup track; v1.3 K8s-native config delivery (ConfigMap-shared schema) will obsolete the duplication anyway.
- **Real IT for end-to-end profile resolution** — needs a `<snmp-profile>` fixture, Testcontainers Postgres in provisiond's test scope, and a Minion RPC stub. Deferred to whichever cycle adds a real profile consumer.

## Related memory

- `project_daemon_nullop_audit_findings` — parent audit (corrected this session)
- `feedback_audit_verify_ctor_signatures` — new feedback memo from this brainstorm; verify signatures before scoping fill PRs
- `feedback_horizon_parallel_vs_additive` — applies: horizon impl reused, no parallel abstraction needed
- `feedback_karaf_is_dead` — applies: NoOp class deleted, no `@Deprecated` shim
- `feedback_rpc_timeout_no_outages` — applies: RPC failures return Optional, no event emission
- `feedback_rebuild_all_daemons` — applies: rebuild all daemon boot JARs before build.sh deltav
- `project_v1_3_kubernetes_native_configuration` — strategic owner of the deferred TextEncryptor decision

## Open question for implementation

Whether existing Spring-context ITs (`NodeContextProducerSpringContextIT` etc.) need `database-schema.xml` on the test classpath, or whether they already mock around the `filterDaoInitializer` boot path. Determined during implementation; resolved either by adding the test resource or by adjusting test scope.

## Implementation watchpoints (added 2026-05-11 from spec review)

These are verification gates the implementation plan must cover explicitly, not just assume.

### 1. Test context pollution

Adding `filterDaoInitializer` to `ProvisiondBootConfiguration` may break existing Spring-context ITs if they boot the full configuration without a `DataSource`. Specifically check `NodeContextProducerSpringContextIT`, `NodeContextKafkaIT`, and any other `*IT` that imports `ProvisiondBootConfiguration` or `@SpringBootTest`-bootstraps the daemon. The `database-schema.xml` classpath resource is fine in tests (comes from the same transitive `opennms-config` JAR); the only test-side concern is whether tests have a real DataSource.

**Mitigation options** (decided per-test during implementation):
- Use `@MockBean(FilterDao.class)` on tests that don't care about the filter path — Spring Boot will replace the real bean before `snmpProfileMapper` resolves, sidestepping `JdbcFilterDao` construction entirely.
- For tests with no `DataSource` at all, scope the test's Spring config to exclude `filterDaoInitializer` (e.g., `@SpringBootTest(classes = ...)` listing only the beans under test).
- For tests with Testcontainers Postgres already wired, expect them to pick up the new bean transparently — no change required.

**Verification gate:** all existing provisiond tests pass on the feature branch before adding new tests. If any break, fix per the mitigation options above before proceeding.

### 2. Transitive dependency hygiene

The new Maven dep on `org.opennms.core.snmp:org.opennms.core.snmp.profile-mapper` may pull in transitive ServiceMix / OSGi bundles that pollute the Spring Boot 4 classpath. Per `feedback_karaf_is_dead`: Karaf is dead; classpath cleanup matters.

**Verification gate:** Run `./mvnw -pl :daemon-boot-provisiond dependency:tree -Dscope=compile` after adding the dep. Audit the diff for new entries containing `servicemix`, `karaf`, `org.osgi`, `org.apache.felix`, or `aries`. Add `<exclusions>` to the dep declaration for any such transitives (pattern: see existing exclusions in the same pom).

### 3. Postgres-at-boot timing

The design assumes `JdbcFilterDao.afterPropertiesSet()` and `FilterDaoFactory.setInstance(...)` are lazy — they validate inputs and store the singleton reference but don't open a JDBC connection or run a query. If that assumption is wrong, provisiond will crash at boot whenever it races ahead of the `db-init` container in the docker-compose / K8s startup sequence.

**Verification gate:** Read `JdbcFilterDao.afterPropertiesSet()` source in `delta-v-horizon/.../filter/.../JdbcFilterDao.java` before merge. Confirm no `getConnection()` or `executeQuery()` calls in the boot path. If the assumption fails:
- Defer FilterDao construction until first use (lazy init wrapper), OR
- Add a `@DependsOn` on a Postgres-readiness probe bean if one exists in daemon-common, OR
- Add explicit Spring `@PostConstruct` ordering that runs after the db-init container's readiness signal.

If the assumption holds (most likely), no code change needed — just document the verification in the PR description.
