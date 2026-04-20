# Next-Session Prompt — l8opensim mock-lab integration (executes a fully-specced + planned PR)

**Pick up an already-brainstormed, already-specced, already-planned PR and execute it via subagent-driven-development.** No design work needed — the prior session settled all open questions, wrote the spec + plan, and committed both to branch `feat/l8opensim-mock-lab`. This session is pure execution.

**Spec:** `docs/superpowers/specs/2026-04-20-l8opensim-mock-lab-design.md`
**Plan:** `docs/superpowers/plans/2026-04-20-l8opensim-mock-lab-plan.md`
**Branch:** `feat/l8opensim-mock-lab` (off develop tip `7ec759eb8e5`)
**PR target:** `pbrane/delta-v` `develop`. **NEVER `OpenNMS/opennms`.** Title prefix `feat(mock-lab):`.

---

## Open this prompt by invoking subagent-driven-development directly

The plan and spec are committed. No need to re-brainstorm or re-plan. Skip straight to:

```
/skill superpowers:subagent-driven-development

Execute the implementation plan at:

  docs/superpowers/plans/2026-04-20-l8opensim-mock-lab-plan.md

Spec for context:

  docs/superpowers/specs/2026-04-20-l8opensim-mock-lab-design.md

We are on branch `feat/l8opensim-mock-lab` (off pbrane/delta-v develop tip
7ec759eb8e5). Spec and plan are committed (ce13b0aaed0 and 2145f727da0).
The plan has 12 tasks; most produce code/config, Tasks 6 and 8 are
docker-running verification gates that don't commit, Task 11 is the full
E2E gate, Task 12 is push + PR.

PR target: pbrane/delta-v develop. NEVER OpenNMS/opennms.

Use one fresh subagent per task. Review between tasks per the skill's
two-stage review pattern.
```

---

## Preflight checks before invoking the skill

Run these from the repo root `/Users/david/development/src/opennms/delta-v`:

```bash
git branch --show-current      # expect: feat/l8opensim-mock-lab
git status --short             # expect: clean
git log --oneline -5           # expect: ce13b0aaed0 (spec), 2145f727da0 (plan), 7ec759eb8e5 (#182)
docker ps --format '{{.Names}}' | grep delta-v | head -5  # expect: empty
./mvnw -version                # Maven 3.9.x + Java 21 (no Java rebuild needed but version-check is cheap)
```

If `git status` shows `M opennms-container/delta-v/provisiond-overlay/etc/imports/*.xml` files, that's the recurring requisition runtime drift from a prior docker compose run. Discard with:

```bash
git checkout -- opennms-container/delta-v/provisiond-overlay/etc/imports/
```

If Docker shows leftover delta-v containers, tear down with:

```bash
cd opennms-container/delta-v && docker compose --profile lite --profile metrics down -v --remove-orphans && cd ../..
```

---

## Memory to load first (before invoking the skill)

These memos are essential context for the executor:

- **`project_grafana_dashboard_bundle`** — the dashboard this PR extends with the `monitoring_location` variable; explains why panels query `ifHCInOctets` etc.
- **`project_e2e_hostname_resolution_timeout`** — RESOLVED via PR #182. Explains why mhuot-labs auto-import is being demoted in this PR.
- **`feedback_never_pr_opennms`** — `--repo pbrane/delta-v`, never OpenNMS/opennms.
- **`feedback_feature_branches`** — never commit directly to develop; use feature branches and PRs.
- **`feedback_provisiond_requisition_drift`** — discard `imports/*.xml` modifications after each docker run; do NOT commit them.
- **`feedback_delta_v_full_reactor_verify`** — full-reactor verification only matters when Java code changes. **THIS PR has no Java changes** — that gate doesn't apply.
- **`feedback_clean_m2_between_branches`** — same, only matters with Java code changes; not relevant here.

---

## What this PR ships (one paragraph summary)

Adds a richer mock SNMP environment to delta-v's docker-compose stack via the `l8opensim` simulator (20 simulated devices across 8 categories at IPs `10.0.0.1-20`), plus a second Minion at `location=l8opensim-lab` honoring the project's Minion-mandatory tenet. Disables the `mhuot-labs` requisition auto-import (file preserved on disk; operators uncomment to re-enable for LLDP topology testing). Dashboard `snmp-overview.json` gains a `monitoring_location` template variable + Location column. E2E test gains Step 10 asserting the new lab data path actually flows.

---

## Key risks the executor should know about

The plan flags these explicitly. Repeated here for the next session's risk-awareness:

### 1. Task 1 (l8opensim REST API spike) has unknown outcome

The brainstorming spike confirmed `POST /api/v1/devices` works with `auto-count` parameters but didn't verify whether the API supports per-device profile selection. Task 1 is structured as an empirical exploration:

- Start l8opensim ad-hoc (`docker run ... -no-namespace`).
- Probe three JSON variants (per-device `device_type` field; top-level `category` field; `devices` array with per-device profiles).
- Confirm via `GET /api/v1/devices` what the simulator actually accepted.
- Author `devices.json` based on findings — may be Scenario A (per-device profile selection) or Scenario B (auto-count round-robin only).

Treat divergence from the Scenario A template as expected work, not a failure. The plan documents both scenarios.

### 2. Task 8 (POLL_GRACE measurement) could surface > 180s first-data latency

Task 8 boots the full stack and measures how long it takes for `opennms_mib2_x_interfaces_ifhcinoctets_total{foreign_source="l8opensim-lab"}` to first appear in VictoriaMetrics. Expected: 90-180s for 21 nodes' import + node-scan + first poll cycle.

**If > 180s**, the plan's instruction is: **stop and surface to the user** rather than blindly bumping `POLL_GRACE_SECONDS`. > 180s suggests a deeper issue (Twin API not delivering, RPC routing broken, etc.) that needs diagnosis before merging.

### 3. Tasks 6, 8, 11 each boot the full stack (~3-5 min boot + verify)

Aggregate ~15 min of stack-time across the plan. Use `docker compose ... down -v --remove-orphans` between tasks; discard requisition drift after each. Tasks 6, 8 are verification gates (no commit); Task 11 is the final E2E gate.

### 4. No Java rebuild needed

This PR is configuration + JSON + new directory of shell scripts. No daemon-boot JARs change, no Maven build required. The prometheus-writer image is already current with PR #180. Don't waste time on `./mvnw clean install` runs.

---

## Established session conventions to honor

From the prior session (which shipped #180, #181, #182 + this branch's spec + plan):

- **Use Edit tool with unique-context strings** for compose YAML edits; don't trust line numbers blindly (the file has shifted from prior PRs).
- **Discard provisiond requisition drift** after each docker stack run (`git checkout -- opennms-container/delta-v/provisiond-overlay/etc/imports/`).
- **Cleanup spike containers** if Task 1 leaves any (`docker rm -f l8-spike` etc.).
- **127.0.0.1 not localhost** for healthchecks inside containers (BusyBox wget IPv6 quirk per `feedback_127.0.0.1_in_healthchecks` lesson — fixed in PR #182).
- **For port-collision verification**, the spec settles on Minion-lab listening on 8181 (Spring Boot `SERVER_PORT=8181`), l8opensim on 8080, both in shared netns. Don't move either port.
- **Squash-merge with `--delete-branch`** is the project convention. CI may show `UNSTABLE` (pending checks) for ~10 min; GitHub considers it mergeable. PRs #180/#181/#182 all merged in that state.

---

## Hot files for the implementer

The executor will touch these specific files:

| File | Change | Task |
|---|---|---|
| `opennms-container/delta-v/l8opensim/devices.json` | NEW | Task 1 |
| `opennms-container/delta-v/l8opensim/post-each.sh` | NEW | Task 1 |
| `opennms-container/delta-v/provisiond-overlay/etc/imports/l8opensim-lab.xml` | NEW (20 nodes) | Task 2 |
| `opennms-container/delta-v/collectd-daemon-overlay/etc/collectd-configuration.xml` | EDIT (add l8opensim-lab package) | Task 3 |
| `opennms-container/delta-v/provisiond-overlay/etc/provisiond-configuration.xml` | EDIT (add l8opensim-lab requisition-def, comment mhuot-labs) | Task 4 |
| `opennms-container/delta-v/docker-compose.yml` | EDIT (add l8opensim + l8opensim-provisioner services) | Task 5 |
| `opennms-container/delta-v/docker-compose.yml` | EDIT (add minion-lab service) | Task 7 |
| `opennms-container/delta-v/grafana/dashboards/snmp-overview.json` | EDIT (add monitoring_location var + Location column + 6 panel selectors) | Task 9 |
| `opennms-container/delta-v/test-prometheus-writer-e2e.sh` | EDIT (add Step 10, set POLL_GRACE) | Task 10 |

---

## Success criteria

- [ ] All 12 plan tasks completed (subagent-driven-development pattern).
- [ ] `bash opennms-container/delta-v/test-prometheus-writer-e2e.sh` exits 0 with all 10 steps green.
- [ ] Manual visual check: `http://localhost:13000/d/snmp-overview` shows Monitoring location dropdown with `Default` AND `l8opensim-lab`. Filter to each, confirm panels populate.
- [ ] Devices-monitored table shows ~21 rows with the new "Monitoring location" column populated correctly.
- [ ] PR opened `--repo pbrane/delta-v --base develop` with title prefix `feat(mock-lab):`.
- [ ] PR description calls out the **mhuot-labs auto-import disabled** as the one upgrade-time behavior change.
- [ ] Post-merge: file `project_l8opensim_mock_lab_done` memory entry with merge SHA. Update `project_grafana_dashboard_bundle` to note the canonical mock environment.

---

## After this PR (queued follow-ups)

Captured in spec for context but **out of scope** for this session:

- **LLDP-MIB upstream contribution to `labmonkeys-space/l8opensim`** — would let Enlinkd topology testing move off the user's real mhuot-labs hardware.
- **Wire l8opensim's flow exporters** (NetFlow / IPFIX / sFlow) — three follow-up PRs.
- **Wire l8opensim's SNMP trap exporter** — Trapd scale testing.
- **Wire l8opensim's UDP syslog exporter** — Syslogd scale testing.
- **Add `device_type` Prometheus label** — provisiond-side metadata adapter to capture l8opensim's device_type.
- **Once l8opensim has interface-errors + host-resources MIB support**, retire the original `mock-snmp-agent` service.
- **Rename mhuot-labs to "external-lldp-lab"** if the maintainer wants to fully de-personalize the demo.

---

## Why this PR matters in the broader Delta-V arc

Today's prior session (2026-04-20) shipped three PRs in sequence that together unlocked the Collectd→Grafana pipeline:

- **#180**: `instance` + `foreign_id` labels + cardinality tracker
- **#181**: Grafana service + starter SNMP dashboard
- **#182**: E2E unblock (extra_hosts fix + SNMP interval fix)

After #182 merged, the user observed the dashboard's "Devices monitored" table showed only `mhuot-labs` rows because:
1. The user's real lab at `172.20.20.x` was reachable via host network.
2. The in-compose `mock-snmp-agent` doesn't serve `ifHCInOctets` (HC counters) — only the 32-bit equivalents.
3. So the panel query never returned anything for `snmp-agent-canary`.

This PR fixes that gap *cleanly* by introducing l8opensim (which DOES serve HC counters) as the canonical in-compose mock environment, while also demonstrating multi-location monitoring (the next-natural Delta-V demo story). It also addresses the user's observation that "mhuot-labs" branding shouldn't be the default fixture for new operators.

---

## State at session pause (2026-04-20)

- Branch `feat/l8opensim-mock-lab` exists on `pbrane/delta-v` and locally.
- Two commits on the branch beyond develop: `ce13b0aaed0` (spec), `2145f727da0` (plan).
- Working tree should be clean.
- Local develop pulled to `7ec759eb8e5` (PR #182 merge tip).
- No Docker stack running (verified at session pause).
