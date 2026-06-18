# Next-session kickoff — Eliminate labbox from E2E (enlinkd → nl6 LLDP)

> Paste the **Prompt** block below as the first message in a fresh Claude Code session
> started from the delta-v clone (`/Users/david/development/src/opennms/delta-v`).
> This is a dedicated, multi-step migration deferred from a long prior session where the
> feasibility was confirmed but the work itself not started.

---

## Prompt

Migrate the **enlinkd E2E test (and any other labbox/mhuot-labs-dependent E2E tests) off the
labbox + Containerlab cEOS hardware onto the local nl6 simulator**, using nl6's new LLDP-topology
support. Goal: enlinkd LLDP topology discovery runs entirely in-stack (no VPN/labbox), so the suite
is self-contained on any workstation.

**Confirmed feasibility (prior session):** nl6 **v0.11.0** (latest; current compose pin is v0.9.1)
supports LLDP topology — link two devices via its REST API and the LLDP neighbor table + a
`to_<peer>_<port>` ifAlias appear on both ends, served under SNMP OID `1.0.8802.1.1.2` (exactly what
enlinkd walks). nl6 also exposes a topology-status endpoint (configured/active link counts). Source:
github.com/labmonkeys-space/nl6.

**v0.11.0 pin** (replace the v0.9.1 pin at `deploy/compose.yml:135`):
`ghcr.io/labmonkeys-space/nl6@sha256:f2812a4abd76cc8b333904d73099e4905793cb8b62e14c58023761ebe4e57e24`

**Plan (each step needs empirical verification — bring up the stack, don't trust assumptions):**
1. **Bump nl6 v0.9.1 → v0.11.0** in compose. Verify v0.11.0 doesn't break the existing nl6 flow/trap/
   syslog pipeline (it's 2 minor versions up — `test-prometheus-writer-e2e.sh` exercises the flow path
   and references `nl6-minion`/nl6-lab series; sanity-check it still works).
2. **De-risk LLDP first (standalone):** run nl6 v0.11.0, use its REST API to add ≥2 devices and **link
   them**, then `snmpwalk` one device for `1.0.8802.1.1.2` and confirm the lldpRem neighbor table is
   populated and references the peer's chassis id. (Find the link endpoint — the topology REST API.)
3. **nl6-provisioner: author the LLDP links** between the seeded nl6 devices so they form a topology
   enlinkd can discover. `nl6-provisioner` currently imports devices via `fleet.sh import` against the
   nl6 REST API (`/api/v1/devices`); extend it to also POST the topology links. The nl6 devices live at
   `location=nl6-lab`, `foreign-source=nl6` (the real requisition is now the profile-gated
   `imports-seed-nl6/nl6-lab.xml`, swapped in by `provisiond-nl6-init` — see #369).
4. **Rewrite `deploy/test-enlinkd-e2e.sh`** to: bring up the nl6 stack (nl6 + nl6-minion +
   provisiond-nl6-init + the nl6-lab requisition), trigger enlinkd discovery for the nl6 nodes, and
   assert `lldpelement` + `lldplink` rows between nl6 devices (replacing all 59 labbox/cEOS/mhuot-labs
   references). enlinkd's SNMP runs via **nl6-minion** at location=nl6-lab (RPC through the gateway).
5. **Audit + migrate the other labbox-Minion-bound tests** (the prior audit found these refs):
   `test-perspective-e2e.sh` (24 mhuot-labs refs — the big one), and the light ones
   `test-grpc-heartbeat-e2e.sh` / `test-minion-rpc-e2e.sh` / `test-timeseries-e2e.sh` (1–2 each — may be
   comments). Eliminate the labbox/mhuot-labs/VPN dependency from each.
6. Verify every migrated test green against a real nl6 stack (bring-ups are slow on Mac — validate
   assertions on a lean stack per [[feedback_e2e_suite_execution_model]]).

**Locked context / guardrails:**
- nl6 (the simulator) ↔ nl6-minion (the location-`nl6-lab` Minion that does SNMP RPC + is the flow
  listener via shared netns). Renamed from `minion-lab` in #369. Two Minions total: `minion`@Default,
  `nl6-minion`@nl6-lab.
- The nl6-lab requisition is **profile-gated** (#369): empty by default; the real 29 nodes swap in only
  under profiles `active/full/metrics/demo` via `provisiond-nl6-init`. The enlinkd test must run with the
  nl6 profile (so nl6-minion + the nodes exist) — otherwise no nl6 nodes, no topology.
- E2E delivery: write requisitions/config **into the running container** (`docker exec tee` /
  `docker cp`), NOT to host `overlays/` (not mounted) — see [[project_provisiond_e2e_slowness_root_cause]]
  and the converted `test-minion-rpc-e2e.sh` / `test-pollerd-restart-outage-e2e.sh` for the pattern.
  (`test-enlinkd-e2e.sh` itself still uses the broken host-overlays delivery — fix that as part of the rewrite.)
- New delta-v code = `org.deltav.*` + own copyright. PRs go to `--repo pbrane/delta-v` only, base develop.

**References:**
- nl6 project (LLDP + REST topology API): github.com/labmonkeys-space/nl6
- Resolved related work: #361/#363/#369/#370 (poller catalog + provisiond storm + nl6 gating + horizon
  1.0.19 null-guard), #368 (gateway fast-NACK on minion-less location), [[feedback_poller_catalog_frozen_engine_null_guards]].
- Current enlinkd test: `deploy/test-enlinkd-e2e.sh` (labbox cEOS / mhuot-labs / 172.20.20.x).
- nl6 wiring: `deploy/compose.yml` (`nl6`, `nl6-minion`, `nl6-provisioner` services), the nl6-provisioner
  `command:` block + `/seed/fleet.sh` + `/seed/devices.json`.

---

## Quick state reference (for the human)

- Feasibility CONFIRMED; work NOT started. This is a focused ~multi-hour migration deferred from a long session.
- Biggest unknowns to nail early: (a) the nl6 REST **link** endpoint (step 2 de-risk), (b) whether v0.11.0
  changed anything in the flow pipeline (step 1).
- Suggested order: de-risk LLDP standalone (step 2) BEFORE touching the test, so you know the topology
  actually populates before investing in the rewrite.
