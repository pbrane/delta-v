# Next-Session Prompt: Horizon Spring 7 `Assert.notNull` Sweep

Use this prompt to start a new Claude session focused on closing out the Spring 7 `Assert.notNull` audit that was surfaced — but only partially fixed — during the 2026-05-13 topology entity cache fill (delta-v PR #265, horizon PRs #15 and #16).

---

## Background (paste verbatim into the new session)

During PR #265 (TopologyEntityCache fill, audit item #4), Hibernate's JPQL constructor projection in the new `TopologyEntityDaoJpa` triggered a runtime `NoSuchMethodError` on horizon's `LldpLinkTopologyEntity.<init>`. Root cause: `Assert.notNull(remportdescr)` is the single-arg overload that Spring 7 removed; only the two-arg `Assert.notNull(Object, String)` survives. Horizon PR #15 fixed that one call site.

A wider scan during PR #265's code review found **nine more** single-arg `Assert.notNull(...)` call sites in horizon's enlinkd modules. They're not exercised by delta-v's daemon execution today — they live in Bridge/IS-IS algorithm code that no current daemon code path activates — but they will throw `NoSuchMethodError` the first time those paths run under Spring 7. They block any v1.3 feature that activates Bridge topology discovery, BroadcastDomain calculations, SubNetwork analysis, or BridgePortWithMacs lookups.

This session's job is to convert all nine to the two-arg form, plus a wider sweep to catch any sites outside enlinkd that the original audit missed.

## Known land mines (from `project_horizon_spring7_assert_notnull_audit` memory)

All in `delta-v-horizon` at `/Users/david/development/src/opennms/delta-v-horizon`, `main` branch at 1.0.15 (`9cc877de1b5`):

```
features/enlinkd/adapters/discovers/bridge/src/main/java/org/opennms/netmgt/enlinkd/service/api/DiscoveryBridgeTopology.java:377
features/enlinkd/adapters/discovers/bridge/src/main/java/org/opennms/netmgt/enlinkd/service/api/DiscoveryBridgeTopology.java:480
features/enlinkd/service/api/src/main/java/org/opennms/netmgt/enlinkd/service/api/BroadcastDomain.java:57
features/enlinkd/service/api/src/main/java/org/opennms/netmgt/enlinkd/service/api/BridgePortWithMacs.java:40
features/enlinkd/service/api/src/main/java/org/opennms/netmgt/enlinkd/service/api/BridgePortWithMacs.java:41
features/enlinkd/service/api/src/main/java/org/opennms/netmgt/enlinkd/service/api/SubNetwork.java:42
features/enlinkd/service/api/src/main/java/org/opennms/netmgt/enlinkd/service/api/SubNetwork.java:47
features/enlinkd/service/api/src/main/java/org/opennms/netmgt/enlinkd/service/api/SubNetwork.java:48
features/enlinkd/service/api/src/main/java/org/opennms/netmgt/enlinkd/service/api/SubNetwork.java:49
```

Nine line references across four files.

## Task

For each site, convert:

```java
Assert.notNull(x);
```

to:

```java
Assert.notNull(x, "<canonical-field-name> must not be null");
```

Use the **canonical field or property name**, not the local variable name — that's the convention `LldpLinkTopologyEntity` was fixed with (`Assert.notNull(remportdescr, "lldpRemPortDescr must not be null")`, where `remportdescr` was the misspelled constructor parameter and `lldpRemPortDescr` is the entity's field name). In some sites the local variable name and field name match; in others they don't. Inspect each file individually rather than running a blind `sed` — the wrong message string survives forever in production stack traces.

## Wider sweep — do this first, before fixing

The PR #15 / PR #265 audit was scoped to `features/enlinkd/persistence/api/`. The follow-up scan widened to `features/enlinkd/` but no further. Other horizon modules may have the same land mine:

```bash
cd /Users/david/development/src/opennms/delta-v-horizon
git fetch origin main && git checkout main && git pull --ff-only

# Single-arg Assert.notNull anywhere in the horizon source tree
grep -rnE "Assert\.notNull\([^,)]*\)\s*;" \
    --include="*.java" \
    core/ \
    features/ \
    opennms-* \
    integrations/ \
    2>/dev/null
```

If the wider grep surfaces additional sites outside enlinkd, INCLUDE them in this PR's scope. Document the audit boundary in the PR body so the next person doesn't have to re-derive it. Confirm the final list is **zero** matches in horizon after the fix:

```bash
# After fixes — must produce zero output
grep -rnE "Assert\.notNull\([^,)]*\)\s*;" --include="*.java" . 2>/dev/null
```

## Scope considerations

1. **Version bump.** These are correctness-only fixes; no API change, no behavior change. Pick the next patch version (`1.0.16`). Don't combine with unrelated horizon work in the same PR — keep the diff scoped to `Assert.notNull` lines plus the `versions:set` pom changes.

2. **Workflow.** `publish.yml` is already at `timeout-minutes: 240` with `-T 4` parallelism (PR #16). No further workflow changes needed unless 1.0.16's publish run reveals new issues.

3. **GHCR ghost cleanup.** PR #15's cancelled run left ~612 ghost `1.0.14` entries in GHCR (per `feedback_github_packages_ghost_versions`). They're documented as accepted cruft in PR #16's body. Decide whether to clean them up while we have a publish workflow open — needs a `delete:packages` PAT, which isn't on the runner's `GITHUB_TOKEN`. Acceptable to leave them.

4. **Delta-v consumer bump.** Delta-v's `deltav.horizon.version` is at 1.0.15 (set by PR #265). It does NOT need to bump to 1.0.16 immediately — these are dormant fixes until a v1.3 feature activates the affected paths. Bumping is purely defensive.

## Recommended PR shape

**Horizon side** — one PR, branch `fix/v1.0.16-assert-notnull-spring7-sweep`:

- Apply the 9 (or N, after wider sweep) one-line fixes
- `mvn versions:set -DnewVersion=1.0.16 -DprocessAllModules=true -DgenerateBackupPoms=false`
- Title: `fix(enlinkd): complete Spring 7 Assert.notNull sweep`
- Body: reference `project_horizon_spring7_assert_notnull_audit` + PR #15 as precursor, enumerate every site fixed (file:line:message), note that delta-v can pick up 1.0.16 when convenient, acknowledge ghost-cleanup decision

After merge, tag `1.0.16` and let `publish.yml` fire. Expected wall-clock: ~40-90 min based on PR #16's 38m3s with `-T 4`.

**Delta-v side** — defer unless a v1.3 feature lands that needs the dormant fix activated. When that day comes, bump `deltav.horizon.version` in a single-line commit.

## What success looks like

- Grep for single-arg `Assert.notNull` across horizon returns **zero** matches
- New horizon tag (`1.0.16` or whatever's next) published successfully to GHCR
- Memory file `project_horizon_spring7_assert_notnull_audit` updated to mark this work DONE with the horizon PR # and squash commit SHA
- Optionally: `feedback_audit_verify_ctor_signatures` get an additional note about the value of also auditing static-utility-method overload usage (not just constructor signatures), since this land-mine class shares the same root cause

## Constraints

- **Never PR against `OpenNMS/opennms`** — both horizon and delta-v are forks. Always `--repo pbrane/delta-v-horizon` or `--repo pbrane/delta-v`. (See `feedback_never_pr_opennms`.)
- **Never commit directly to `main` or `develop`** — always feature branches. (See `feedback_feature_branches`.)
- **Pull before branching**, especially for horizon — main may have moved since 1.0.15. (See `feedback_pull_before_branching`.)
- **Use canonical field names in messages**, not local variable names. The earlier 2026-05-13 code review flagged this exact issue on `LldpLinkTopologyEntity`.
- **Never use `sed -i.bak`** in a tree where `*.bak` might be a tracked file. (We learned this the hard way on 2026-05-13 — `pom.xml.bak` is tracked in delta-v.) Use `sed -i ''` on macOS or a temp-file pattern when overwriting in-place.

## Related memory items

- `project_horizon_spring7_assert_notnull_audit` — site enumeration, lives at `/Users/david/.claude/projects/-Users-david-development-src-opennms-delta-v/memory/`
- `feedback_fix_horizon_not_exclusions` — the rule driving "fix at horizon source"
- `feedback_github_packages_ghost_versions` — chronic GHCR partial-deploy problem
- `feedback_audit_verify_ctor_signatures` — the audit rigor lesson from PR #265's planning
- `feedback_never_pr_opennms`, `feedback_feature_branches`, `feedback_pull_before_branching` — the standard workflow guards

## Why this is dormant debt and not urgent

Today (post-PR #265), delta-v doesn't exercise these code paths at runtime. The `TopologyEntityCache` producer side is wired and verified (PR #265), but no consumer of `OnmsTopologyDao` exists in delta-v yet — see `project_v1_3_topology_ui_resurrection`. The 9 land mines are in Bridge/IS-IS algorithm code that runs only when something asks for the topology graph and that graph triggers re-computation. That request path is v1.3 work.

If a v1.3 contributor activates a topology consumer (Vue UI, K8s operator endpoint, REST API) without this sweep landing first, their first end-to-end smoke test crashes with `NoSuchMethodError` deep in Bridge discovery. That's the failure mode this PR pre-empts.

Pick this up when convenient. It's pure hygiene, no deadline.
