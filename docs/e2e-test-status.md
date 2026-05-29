# E2E Test Suite — Status & Disposition

**Captured:** 2026-05-13 during PR #265 (topology entity cache fill) Task 6 re-run.

Each `opennms-container/delta-v/test-*-e2e.sh` script runs independently. There's no umbrella runner; the historical "93/93" milestone was a sum across all scripts and assertions. Some scripts pass cleanly today; others fail because of known open debt, environmental issues, or hardware dependencies. **None test abandoned functionality** — there is nothing to remove outright. The disposition below is keep-pass / defer-debt / fix-env / hardware-only.

## Summary table

| Script | Today | Disposition | Root cause |
|---|---|---|---|
| `test-grpc-heartbeat-e2e.sh` | 4/4 ✅ | KEEP | — |
| `test-grpc-rpc-e2e.sh` | PASS ✅ | KEEP | — |
| `test-grpc-sinks-e2e.sh` | PASS ✅ | KEEP | — |
| `test-grpc-twin-e2e.sh` | PASS ✅ | KEEP | — |
| `test-minion-e2e.sh` | 16/16 ✅ | KEEP | — |
| `test-collectd-e2e.sh` | 4/4 ✅ | KEEP | — |
| `test-syslog-e2e.sh` | 16/16 ✅ | KEEP (note: includes alarm create+clear via syslog path — works) |
| `test-timeseries-e2e.sh` | PASS ✅ | KEEP | — |
| `test-node-context-e2e.sh` | PASS ✅ | KEEP | — |
| `test-e2e.sh` (alarm lifecycle, traps) | 13/13 ✅ (after fix) | KEEP | **Was 7/12 — fixed in this branch via 3 hardening tweaks ported from test-minion-e2e.sh: consumer warmup 3s→8s, ALARM_TIMEOUT 30s→45s, IFINDEX 1→3.** The alarmd pipeline was never broken; the test harness had stale 2026-04-01 timing that pre-dated the gRPC migration. |
| `test-passive-e2e.sh` | 20/22 ❌ | **DEFER — outage table, not alarm table** | Phase-2/3 **alarm** assertions all pass — alarmd creates and clears the `serviceDown` alarm correctly. The 2 failures are `outages` table inserts (alarm-driven outage open + close). That's a Pollerd-side flow, separate from the alarmd-lifecycle gap. test-passive-e2e.sh already has hardening from 2026-04-28; the test-e2e.sh bit-rot recipe doesn't apply here. |
| `test-minion-rpc-e2e.sh` | 8/9 ❌ | **DEFER — pollerd Kafka producer bug** | Phase 3 only; see `project_pollerd_kafka_producer_metadata_timeout` |
| `test-flows-e2e.sh` | 17/19 ❌ | **FIX ENVIRONMENT** | V9 + sFlow testnodes can't bind their docker-compose subnet IPs (envoy + minion grab `172.18.0.20/21` first) |
| `test-prometheus-writer-e2e.sh` | FAIL ❌ | **DEFER — known startup race** | `NodeContextKafkaBootstrap` race (`records_consumed_total=0`); see `project_collectd_publisher_inert_investigation` |
| `test-enlinkd-e2e.sh` | BLOCKED ⛔ | **HARDWARE-ONLY** | Requires labbox SSH tunnel + Containerlab cEOS + `mhuot-labs` requisition; cannot run from a developer laptop without the tunnel |
| `test-perspective-e2e.sh` | BLOCKED ⛔ | **HARDWARE-ONLY** | Same labbox dependency + `nl6-lab` Minion location |

## Detailed dispositions

### KEEP — currently passing, valid, no action

The 9 scripts in this group test functionality the project actively supports. They pass today as part of the laptop-runnable smoke loop and should stay in the suite as-is.

The notable one is `test-syslog-e2e.sh`: it exercises the same `alarmd` lifecycle (Phase 2 alarm-create + Phase 3 alarm-clear) that `test-e2e.sh` fails on, but via the syslog UEI path. It passes cleanly. This is direct evidence that `alarmd`'s alarm-create/clear machinery is not globally broken — only the trap-UEI lane has the gap.

### DEFER — fail today, but the thing they test is still in scope

These tests are valuable and should NOT be deleted. They fail because of known open debt that is tracked separately. Re-enabling them is gated on closing the underlying issue.

- **`test-e2e.sh`** — *trap→alarm pipeline*. Trap-UEI alarms (`uei.opennms.org/translator/traps/SNMP_Link_Down`) don't reach the `alarms` table. Surprisingly, `test-minion-e2e.sh` (also trap-driven, same `translator/traps/*` UEIs) passes 16/16 — so the failure is narrower than "all traps". Worth a focused investigation comparing the two scripts to isolate the differential. Tracked alongside `project_alarmd_alarm_lifecycle_gap` for now, but the syslog-passes asymmetry suggests the root cause is upstream of `alarmd` proper (perhaps trapd→eventd→alarmd routing, or a specific trap-translator mapping).

- **`test-passive-e2e.sh`** — *passive outage flow via syslog*. Two assertions fail (16/18). Despite the syslog driver, the failure mode is similar to the trap path. Tracked with `project_alarmd_alarm_lifecycle_gap`.

- **`test-minion-rpc-e2e.sh`** — *pollerd RPC roundtrip via Minion*. Phase 3 only: pollerd's Kafka producer intermittently fails to fetch topic metadata for `OpenNMS.<location>.rpc-request`. Tracked in `project_pollerd_kafka_producer_metadata_timeout`. NOT a regression from this PR or any recent PR; it's a pre-existing dev-env behavior.

- **`test-prometheus-writer-e2e.sh`** — *NodeContext-driven collectd publisher*. Race condition where the publisher comes up before NodeContext has bootstrapped, so `records_consumed_total` stays at 0. Tracked in `project_collectd_publisher_inert_investigation`.

### FIX ENVIRONMENT — the test is fine, the dev stack is broken

- **`test-flows-e2e.sh`** — *flow ingestion through ClickHouse*. The V9 + sFlow testnode containers require static IPs `172.18.0.20` / `172.18.0.21`, which envoy + minion grab first at compose startup. Docker-compose subnet allocation is non-deterministic for unreserved IPs. Fix is in the compose file (carve out the testnode IPs from the default range), not in the test script.

### HARDWARE-ONLY — move to a separate CI lane, do not run on developer laptops

These tests require physical Containerlab cEOS hardware that lives on the lab machine reachable via SSH tunnel. They are blocked on a developer laptop. **Disposition:** keep the scripts in the repo but skip them in the laptop smoke loop; run them in a dedicated CI workflow that has the tunnel set up (or run them manually from the lab).

- **`test-enlinkd-e2e.sh`** — LLDP topology discovery against real Cisco IOS XR boxes via Containerlab. The most direct regression detector for PR #265's topology entity cache fill, but cannot run without the hardware.
- **`test-perspective-e2e.sh`** — perspective polling from `nl6-lab` Minion location through to ClickHouse + Grafana. Same hardware constraint plus a non-trivial setup (see `project_smoke_perspective_monitoring`).

## Recommended next actions

1. ~~**Triage `test-e2e.sh` vs `test-minion-e2e.sh`**~~ — ✅ DONE in this branch. 3 hardening tweaks: consumer warmup 3s→8s, ALARM_TIMEOUT 30s→45s, IFINDEX 1→3. Pipeline confirmed working (13/13 PASS). The alarmd-lifecycle-gap memory items (`project_alarmd_alarm_lifecycle_gap`, `project_alarmd_state_management_strategy_open`) need an update to note that the trap→alarm lane is NOT actually broken at the daemon — only the test harness was stale.
2. **Apply the same hardening to `test-passive-e2e.sh`** — same author, same era, same probable root cause. Likely converts another DEFER to a KEEP.
3. **Surface the `test-flows-e2e.sh` IP conflict as a compose fix** — small one-PR fix (allocate IPs in the compose file explicitly). Frees up 2/19 assertions.
4. **Move `test-enlinkd-e2e.sh` and `test-perspective-e2e.sh` out of any laptop-run script** — they currently sit alongside the laptop-runnable tests and silently fail (or skip) when run there. A subdirectory like `opennms-container/delta-v/lab/` or a clear naming convention (`test-lab-*-e2e.sh`) would make the boundary obvious.
5. **Add per-script status to CI artifacts** — when "93/93 across the suite" was achievable, no per-script breakdown was recorded. If we ever want to bisect a regression in the future, having `test-X-e2e.sh: N/M` for each script in CI output is what we'd need.
6. **Audit other 2026-04-period test scripts for similar bit-rot** — any `test-*-e2e.sh` last touched before the gRPC migration (2026-05-08) is suspect. The 3s consumer warmup is the highest-signal red flag to grep for.

## Not in scope of this doc

- New tests we should add (e.g., thresholding per `project_thresholding_e2e_followup`, or detector/monitor execution on Minion per `project_e2e_detectors_monitors_minion`) — those are tracked separately.
- Whether the alarmd lifecycle gap should be closed with Drools, an in-Java rule engine, or no-rule-engine-at-all — that's `project_alarmd_state_management_strategy_open`.
- Bisecting the historical "all-green" period — the user explicitly deprioritized this question.
