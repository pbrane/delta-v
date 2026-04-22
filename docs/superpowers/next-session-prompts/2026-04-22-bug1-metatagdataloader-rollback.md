# Next session: diagnose Bug #1 — UnexpectedRollbackException in MetaTagDataLoader

**Background memory:** `project_phase0_inner_persister_bugs_followup` (re-read first).

**Branch:** `fix/metatagdataloader-rollback` off develop.

Bug #1 is the last open item from the Phase 0 horizon inner-persister trio. Bugs #2 (NPE) and #4 (ClassCastException) were fixed in horizon 1.0.11 via delta-v #189 (merged 2026-04-21 / 22). The cascade is broken — #1 no longer takes the other two down with it — but the `{step=visitResource}` counter still ticks every Collectd poll cycle on labbox, and it should read `0.0`.

---

## One-paragraph problem statement

Every Collectd poll cycle trips an `UnexpectedRollbackException: Transaction silently rolled back because it has been marked as rollback-only` inside `org.opennms.netmgt.timeseries.samplewrite.MetaTagDataLoader.load`, specifically on the commit of delta-v's `withReadOnlyTransaction` wrapper. On labbox it fires once per resource per cycle (~8 per 90 s). Since horizon 1.0.11 (delta-v #189) this no longer cascades, so time-series samples still persist — but the counter `deltav_collectd_persister_inner_failures_total{step=visitResource}` stays non-zero in steady state.

## Stack frame (from labbox, horizon 1.0.10 — re-capture with DEBUG on 1.0.11)

```
org.springframework.transaction.UnexpectedRollbackException:
  Transaction silently rolled back because it has been marked as rollback-only
    at AbstractPlatformTransactionManager.processCommit(AbstractPlatformTransactionManager.java:803)
      [spring-tx-7.0.5]
    at TransactionTemplate.execute(TransactionTemplate.java:149)
    at CollectdJpaConfiguration$1.withReadOnlyTransaction(CollectdJpaConfiguration.java:158)
    at MetaTagDataLoader.load(MetaTagDataLoader.java:85)
      [features.timeseries-1.0.10]
    at TimeseriesPersister.visitResource(TimeseriesPersister.java:96)
      [features.timeseries-1.0.11]
```

The delta-v entry point (`core/daemon-boot-collectd/src/main/java/org/deltav/netmgt/collectd/boot/CollectdJpaConfiguration.java` around line 146-165) wraps a standard Spring `TransactionTemplate` configured with `setReadOnly(true)` and default propagation (`REQUIRED`):

```java
@Bean
public SessionUtils sessionUtils(PlatformTransactionManager txManager) {
    var readOnlyTxTemplate = new TransactionTemplate(txManager);
    readOnlyTxTemplate.setReadOnly(true);
    return new SessionUtils() {
        @Override
        public <V> V withReadOnlyTransaction(java.util.function.Supplier<V> supplier) {
            return readOnlyTxTemplate.execute(status -> supplier.get());
        }
        // ...
    };
}
```

Horizon's `MetaTagDataLoader.load` (line 85 in 1.0.11 / `delta-v-horizon/features/timeseries/src/main/java/org/opennms/netmgt/timeseries/samplewrite/MetaTagDataLoader.java`) opens the supplier and calls `nodeDao.get`, `entityScopeProvider.getScopeForNode`, `entityScopeProvider.getScopeForInterfaceByIfIndex`, etc.

## Three hypotheses, in best-guess order

**Hypothesis 1 — Spring 4.2 → Spring 7 read-only-commit semantics changed.**
Legacy horizon ran Spring 4.2. Delta-v's Spring Boot 4.0.3 pulls Spring 7. The stack trace confirms Spring 7 (`spring-tx-7.0.5`). Spring 7's `AbstractPlatformTransactionManager.processCommit` may enforce rollback-only state on read-only tx commit more strictly than 4.2 did. If something internal to the supplier (Hibernate autoflush, dirty-check, or a nested caught exception) marks the tx rollback-only, Spring 7 throws at commit where 4.2 silently no-op'd.

- **How to test:** In `CollectdJpaConfiguration.java` swap `readOnlyTxTemplate` for a non-read-only template (drop the `setReadOnly(true)` line). Re-run. If the counter drops to zero, hypothesis 1 is confirmed.
- **Important:** do NOT ship the test change as a fix. The real fix if H1 is right is to keep the read-only tx AND make the supplier truly side-effect-free — otherwise you're papering over something (probably H2) that H1's strictness is legitimately flagging.

**Hypothesis 2 — something inside `MetaTagDataLoader.load` marks the outer tx rollback-only.**
Candidates inside the supplier (horizon code, not ours):
- Line 186 `nodeDao.get(nodeCriteria)` — wrapped in a try/catch that logs and swallows. If `nodeDao.get` throws a RuntimeException that the Spring-managed tx layer sees *before* the catch, the outer tx is marked rollback-only.
- Line 95 `entityScopeProvider.getScopeForNode(node.getId())` — no try/catch; any throw here surfaces but could also mark rollback-only mid-throw.
- Line 100 `entityScopeProvider.getScopeForInterfaceByIfIndex(node.getId(), ifIndex)` — same.

- **How to test:** Either temporarily instrument `MetaTagDataLoader.load` with `TransactionSynchronizationManager.isActualTransactionActive()` + `isRollbackOnly()` logging after each call, OR set the inner-persister logger to `TRACE` and look for silent-catch breadcrumbs. The goal is to find the FIRST call inside the supplier that flips the tx to rollback-only.

**Hypothesis 3 — nested transaction conflict.**
If `withReadOnlyTransaction` is called from a code path that already has an outer non-read-only tx, `PROPAGATION_REQUIRED` joins the outer. The commit semantics of joined txs interact weirdly with the read-only flag. Least likely given the call site (`TimeseriesPersister.visitResource`) doesn't look like it runs under an outer tx, but worth a sanity check.

- **How to test:** log `TransactionSynchronizationManager.getCurrentTransactionName()` and `isCurrentTransactionReadOnly()` at entry to `withReadOnlyTransaction`. If there's an outer tx, it'll have a non-null name.

## First diagnostic steps

1. **Get a Linux-equivalent environment where provisiond imports work.** macOS Docker Desktop on this machine hits `project_docker_compose_nested_bind_mount_bug` — the nested named volume at `/opt/deltav/etc/imports` is silently dropped by Docker Desktop, so seed requisitions don't reach provisiond, no nodes get created, Collectd has nothing to poll, and the bug won't reproduce. Pick one:
   - SSH to labbox (memory `reference_labbox_setup`) and run `cd opennms-container/delta-v && docker compose --profile lite --profile metrics up -d --build`
   - Fix the mount bug first (memory `project_docker_compose_nested_bind_mount_bug` has the layout)
   - Try OrbStack / Colima instead of Docker Desktop

2. **Boot the stack. Wait two Collectd poll cycles (~90 s each).** Confirm:
   ```bash
   docker exec delta-v-collectd wget -q -O- http://127.0.0.1:8080/actuator/prometheus \
     | grep -E '^deltav_collectd_persister_inner_failures_total\{step="visitResource"'
   ```
   Should be non-zero. If zero on 1.0.11 already, we're done — the bug only lived during the cascade. Not likely, but worth checking.

3. **Bump the inner-persister logger to DEBUG** so full stack traces (including causes) reach the logs. The shipped WARN log in `FanoutPersister.runInner` already includes the throwable but via SLF4J — confirm the cause chain is there or bump the `TransactionTemplate` / `AbstractPlatformTransactionManager` logger directly:
   ```bash
   docker compose logs --since 5m collectd \
     | grep -B 2 -A 60 'UnexpectedRollbackException'
   ```

4. **Test Hypothesis 1 first** — swap the read-only flag off in `CollectdJpaConfiguration.java` and re-run. If the counter drops to zero, H1 is right and you switch to H2 to find what the read-only tx was legitimately flagging.

5. **If H1 is wrong, test H2** — add `TransactionSynchronizationManager` logging at each call inside `MetaTagDataLoader.load`, find the first one that trips `isRollbackOnly()`.

## Out of scope

- Bugs #2 and #4 — fixed. The E2E regression assertion in `opennms-container/delta-v/test-timeseries-e2e.sh` guards them.
- Re-investigating the cascade — it's broken in horizon 1.0.11.

## Done = this reads `0.0`

```bash
docker exec delta-v-collectd wget -q -O- http://127.0.0.1:8080/actuator/prometheus \
  | grep -E '^deltav_collectd_persister_inner_failures_total\{step="visitResource"'
```

After two poll cycles.

## Key files

| File | Why |
|---|---|
| `core/daemon-boot-collectd/src/main/java/org/deltav/netmgt/collectd/boot/CollectdJpaConfiguration.java` (lines 146-165) | Delta-v side of the stack frame; the read-only tx wrapper |
| `delta-v-horizon/features/timeseries/src/main/java/org/opennms/netmgt/timeseries/samplewrite/MetaTagDataLoader.java` (line 84 onward) | Horizon side; the supplier body |

## Session size

Half a day if Hypothesis 1 is right (swap the flag, verify, then find and fix the inner side-effect). Full day or more for Hypothesis 2 — every call inside the supplier is a suspect and the real fix may need a `REQUIRES_NEW` propagation boundary or restructuring `MetaTagDataLoader.load` to avoid setting rollback-only.
