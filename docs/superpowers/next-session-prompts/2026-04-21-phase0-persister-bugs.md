# Next session: three horizon TimeseriesPersister bugs firing every Collectd poll

**Background memory:** `project_phase0_inner_persister_bugs_followup` (authoritative — re-read before this prompt). Surfaced 2026-04-18 on labbox; horizon 1.0.10; counters shipped in delta-v #175.

**Branch:** `fix/horizon-timeseries-persister-bugs` off develop. Expected to be one or more PRs — possibly a horizon-side patch in `pbrane/delta-v-horizon` plus a horizon version bump in delta-v. Decide the split after diagnostic step 3.

---

## The bugs in one paragraph

Every Collectd poll cycle silently trips three distinct exceptions inside horizon's `org.opennms.netmgt.timeseries.samplewrite.TimeseriesPersister`. All three are swallowed by the `FanoutPersister` isolation delta-v #170 introduced, so the operator never sees a failure and the Kafka Time-Series pipeline keeps working — but the inner `TimeseriesPersister` path never successfully writes a single time-series sample, and the operator-visible counters from delta-v #175 are non-zero in steady state. The three bugs are each a separate investigation; they may or may not share a root cause.

| Bug | Symptom | Rate on labbox | Counter label |
|---|---|---|---|
| **#1** | `UnexpectedRollbackException` on `MetaTagDataLoader.load` → `withReadOnlyTransaction` commit | 8 / 90 s (one per resource per cycle) | `step=visitResource` |
| **#2** | `NullPointerException` on `TimeseriesPersistOperationBuilder.setAttributeValue` — `currentBuilder` is null | 76 / 90 s (one per numeric attribute per cycle) | `step=visitAttribute` or `persistNumericAttribute` |
| **#4** | `ClassCastException`: `ResourcePath` cannot be cast to `CollectionResource` inside `getUserDefinedMetaTags` guava cache load | 10 / 90 s (one per group per resource per cycle) | `step=visitGroup` |

(There is no Bug #3 — numbering from the original Phase-0 known-issues list.)

## Operator surface

Already wired up. `/actuator/prometheus` on Collectd exposes:

```
deltav_collectd_persister_inner_failures_total{step="visitResource"}  # Bug #1
deltav_collectd_persister_inner_failures_total{step="visitGroup"}     # Bug #4
deltav_collectd_persister_inner_failures_total{step="visitAttribute"} # Bug #2 (or persistNumericAttribute)
```

A `rate(deltav_collectd_persister_inner_failures_total[5m]) > 0` alert catches any of them. Today all three rates are non-zero; when this work lands and the bugs are fixed (or the failing code paths skipped), the rates should go to zero.

## First six diagnostic steps

Do these before writing any fix. The fix direction depends on which bugs are already fixed upstream in horizon and which require new patches.

1. **Boot the stack and reproduce.** `cd opennms-container/delta-v && docker compose --profile lite --profile metrics up -d --build`. Wait ~2 min for Collectd to do at least two poll cycles, then:
   ```bash
   docker exec delta-v-collectd wget -q -O- http://127.0.0.1:8080/actuator/prometheus \
     | grep -E '^deltav_collectd_persister_inner_failures_total'
   ```
   All three counters should be non-zero. If any are zero, investigate why (version skew? labbox vs simulator differences?) before proceeding.

2. **Capture one full stack trace per bug.** The shipped logs at `LOG.warn(...)` level only log the exception message; for root-cause work you need the trace. Either temporarily bump the inner-persister logger to `DEBUG`/`TRACE` or grep Collectd logs for the exception class names:
   ```bash
   docker compose logs --since 5m collectd \
     | grep -A 40 -E 'UnexpectedRollbackException|persistNumericAttribute|ResourcePath.*CollectionResource'
   ```

3. **Check horizon upstream for existing fixes.** The current pin is `<deltav.horizon.version>1.0.10</deltav.horizon.version>` in `pom.xml`. Compare against the newest `pbrane/delta-v-horizon` tag and against upstream OpenNMS `foundation-2025` / `develop` branches for commits touching:
   - `org.opennms.netmgt.timeseries.samplewrite.TimeseriesPersister`
   - `org.opennms.netmgt.timeseries.meta.MetaTagDataLoader`
   - `TimeseriesPersistOperationBuilder`

   If Bug #2 or Bug #4 already has an upstream fix, the cheapest path is **bump the horizon version in delta-v and verify the counter goes to zero**. Only investigate further if upstream has nothing.

4. **Extract the 1.0.10 sources** to read what the code is actually doing in our pinned version:
   ```bash
   find ~/.m2/repository -name '*timeseries*1.0.10-sources.jar' | head
   # For each: mkdir -p /tmp/ts-src && cd /tmp/ts-src && jar xf <path>
   ```
   Focus on `TimeseriesPersister.visitResource`, `visitGroup`, `visitAttribute`, `persistNumericAttribute`, and `getUserDefinedMetaTags`. Trace `currentBuilder`'s lifecycle end-to-end to understand Bug #2.

5. **Diagnose Bug #4 cache key confusion.** Read `getUserDefinedMetaTags` and the guava cache it calls. The cast failure says a `ResourcePath` is being passed where a `CollectionResource` is expected. Find the `.get(key)` call site(s) and trace what `key` is at each. This is almost certainly a horizon bug, not a delta-v wiring bug.

6. **Decide scope split before touching code.** By now you know: which bugs are fixed upstream (just bump the pin), which need new patches, and whether a patch belongs in delta-v or in `pbrane/delta-v-horizon`. Write a short implementation plan (one paragraph per bug, naming the files you'll change) and confirm it before implementing.

## Key files

**Delta-v side (read/reference):**

| File | Why |
|---|---|
| `core/flow-enricher` / `daemon-boot-collectd` — wherever the `FanoutPersister` lives today | Confirms the isolation boundary that swallows these exceptions — useful to know what a "fixed" path looks like end-to-end. |
| `daemon-boot-collectd/src/main/java/.../FanoutPersister*.java` and the inner-failure counters bean | Delta-v #175's observability shim. No change expected here. |

**Horizon side (the bug source):**

```
org/opennms/netmgt/timeseries/samplewrite/TimeseriesPersister.java
org/opennms/netmgt/timeseries/meta/MetaTagDataLoader.java
org/opennms/netmgt/timeseries/samplewrite/TimeseriesPersistOperationBuilder.java
```

Start with the 1.0.10 sources jar in `~/.m2/repository`. Fix patches land in `pbrane/delta-v-horizon` on a branch, get a new 1.0.11 tag, and delta-v bumps the pin.

## How to verify the fix(es)

Boot the full stack, let Collectd do at least two poll cycles, then:

```bash
docker exec delta-v-collectd wget -q -O- http://127.0.0.1:8080/actuator/prometheus \
  | grep -E '^deltav_collectd_persister_inner_failures_total'
```

All three `step=...` counters should read `0.0`. Any non-zero means a bug is still firing.

Also run `./test-prometheus-writer-e2e.sh` and `./test-timeseries-e2e.sh` — both should continue to pass; they exercise the outer Kafka Time-Series path that the `FanoutPersister` isolates from these bugs, so regressions in the inner path shouldn't break them, but it's a cheap regression check.

## Scope for the PR(s)

Target one or more of:

- **`pbrane/delta-v-horizon`**: patches for the `TimeseriesPersister` / `MetaTagDataLoader` / `TimeseriesPersistOperationBuilder` bugs. One commit per bug where feasible so each is independently revertable. Tag a new `1.0.11` when all three are in.
- **`pbrane/delta-v` `fix/horizon-timeseries-persister-bugs`**: bump `<deltav.horizon.version>` to `1.0.11`. Add a regression assertion to an existing Collectd E2E test that all three counters stay zero after two poll cycles (so future regressions fail a known test rather than hiding behind the isolation).

If a bug turns out to have an upstream fix that's already in a newer horizon, the delta-v-side work collapses to a version bump + the regression assertion.

## Stack state at session start

Stack is expected down. `docker compose --profile lite --profile metrics up -d --build` is sufficient. No extra mock/sim wiring is needed — the Collectd labbox poll path reproduces all three bugs within one or two cycles.

## Estimated session size

Uncertain. 30–60 min if two of three bugs already have upstream fixes (version bump + verification). Half a day to a day if new horizon patches are needed — add time for the `pbrane/delta-v-horizon` round-trip (branch → build → tag → consume in delta-v). Stop after diagnostic step 3 and reassess scope before committing to a plan.
