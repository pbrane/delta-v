# Next-session kickoff — Rusty Minion (resume after Phase 0)

> Paste the **Prompt** block below as the first message in a fresh Claude Code session
> started from the delta-v clone (`/Users/david/development/src/opennms/delta-v`).
> The session auto-loads the `[[project_rusty_minion]]` memory; this prompt restates the
> immediate goal, the locked decisions, and the guardrails so nothing gets re-litigated.

---

## Prompt

Resume the **Rusty Minion** work. Read the `[[project_rusty_minion]]` memory first — it has the
full state. Summary: **Phase 0 is DONE and green, executed locally (16/17 tasks), kept local**
(no remotes yet, per my "push later" decision). Two standalone repos exist as siblings of the
delta-v clone:
- `~/development/src/opennms/opennms-ipc-contract` — buf-managed contract, dual Java+Rust codegen,
  golden corpus byte-identical in both languages, CollectionSet round-tripped vs the real Java collector.
- `~/development/src/opennms/rusty-minion` — the Rust Minion; 10 tests green incl. the in-process
  e2e; live smoke passed against the real delta-v gateway. Consumes the contract via a **path dep**
  (`../opennms-ipc-contract/rust`) for now.

**Confirm before doing anything:** `git -C ~/development/src/opennms/rusty-minion log --oneline -1`
and the same for `opennms-ipc-contract` and the delta-v branch `rusty-minion-rust-design` — make
sure the repos are where the memory says and nothing drifted.

**What I want this session — ask me which, don't assume:**

1. **Publish + C1 (the one deferred Phase 0 task).** Create `pbrane/opennms-ipc-contract` and
   `pbrane/rusty-minion` remotes, push both. Tag the contract (e.g. `v0.1.0`) so GH Packages
   publishes `org.deltav:ipc-contract-java:0.1.0`. Switch rusty-minion's Rust dep from the path dep
   to the published git tag and re-verify `cargo test`. Then execute **C1** from the Phase 0 plan:
   repoint `core/minion-grpc-contracts` to depend on the published artifact, delete its local
   `src/main/proto`, verify the dependent delta-v modules (`minion-gateway`,
   `daemon-boot-minion-common`) still compile, and open a PR to **pbrane/delta-v** (NEVER OpenNMS/*).
   C1 is gated on the publish — that's why it was deferred; do the publish first.

2. **Plan Phase 1 (listeners).** Use the `superpowers:writing-plans` skill to write the Phase 1
   implementation plan from the spec §8: Sink listeners in order **Flows → Trap → Syslog**
   (fire-and-forget, one-way). Flows first (already protobuf, lowest contract risk). Milestone:
   `test-syslog-e2e.sh` and the trap path of `test-minion-e2e.sh` pass against a real Core.

**Locked decisions — do NOT re-litigate** (see memory + spec §10/§11): Rust for production; gRPC-to-
gateway ONLY (no direct-Kafka transport in v1); Rusty Minion targets the existing delta-v gateway
contract `org.deltav.minion.grpc.v1` (NOT horizon `OpenNMSIpc`); contract is the standalone
`opennms-ipc-contract` repo; net-snmp via FFI later (Phase 2).

**Guardrails:** New delta-v code is `org.deltav.*` + own copyright. PRs go to `--repo pbrane/delta-v`
only. Pushing branches that touch `.github/workflows/*.yml` needs SSH/`workflow` scope. The contract
repo is local-only until you push it — until then delta-v cannot depend on it (would break CI), which
is exactly why C1 is sequenced after the publish.

**Spec:** `docs/superpowers/specs/2026-06-13-rusty-minion-rust-design.md`
**Phase 0 plan (for C1 task text):** `docs/superpowers/plans/2026-06-13-rusty-minion-phase0-foundation.md`

---

## Quick state reference (for the human)

- **Phase 0:** A1–A8 (contract repo) + B1–B8 (rusty-minion) done, all reviewed, green. C1 deferred.
- **Repos (local, no remote):** `~/development/src/opennms/opennms-ipc-contract` (10 commits),
  `~/development/src/opennms/rusty-minion` (13 commits).
- **delta-v branch:** `rusty-minion-rust-design` — spec, plan, gated A8 collectd capture test.
- **Tools used:** buf 1.70, protoc 35 (brew), rustup/cargo 1.96, tonic 0.12 / prost 0.13.
- **Live smoke entry point:** `rusty-minion/scripts/smoke-register.sh` (asserts the
  `DeltaV.Sink.Heartbeat` Kafka bridge; rusty-minion reaches the gateway via Envoy :8443 h2c).
