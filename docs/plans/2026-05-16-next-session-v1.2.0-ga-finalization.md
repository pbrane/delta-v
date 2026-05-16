# Next-Session Prompt: v1.2.0 GA Finalization Plan

Use this prompt to start a new Claude session whose **deliverable is a plan** —
not code. The job is to inventory everything still outstanding for the Delta-V
v1.2.0 release, decide what is GA-blocking vs. deferrable to v1.3, and produce a
concrete, sequenced finalization plan that ends with cutting `v1.2.0` (GA).

Drive it with the `superpowers:brainstorming` skill (to settle scope and the
GA-vs-v1.3 cut line with the user) and then `superpowers:writing-plans` (to emit
the finalization plan). Do **not** start implementing — this session produces the
plan and gets it approved.

---

## Where the release stands (paste verbatim into the new session)

The master release plan is `docs/plans/2026-04-23-v1.2.0-release-plan.md`. Read it
first — but note it is **stale**: its "Remaining" section still lists rc1/rc2/GA,
while the project has since cut rc1, rc2, rc2.2, rc3, and **rc3.1** (2026-05-16).
The first job of the new session is to reconcile that doc against actual state.

Release theme: *"Delta-V runs well on Kubernetes."* Four tracks:

1. **Self-contained images** — ✅ DONE (alpha1–3).
2. **Minion Kafka IPC → gRPC** — the large remaining track. rc1 (RPC), rc2
   (Twin, sinks, build cleanup), rc3 (poller-timeseries unification) have all
   advanced it. **Reconciling exactly what remains here is the central task of
   the finalization plan.**
3. **Application observability** — ✅ DONE (beta1 Scope A, beta2 Scope B).
4. **Pollerd + PerspectivePollerd response-time publishing** — ✅ DONE (Pollerd
   beta2, PerspectivePollerd rc3). Verified end-to-end on the rc3.1 smoke run
   2026-05-16: `opennms_response_time_response{producer="pollerd"|"perspective_pollerd"}`
   flows to VictoriaMetrics.

Current tag: **`v1.2.0-rc3.1`** (2026-05-16). It shipped the horizon 1.0.18
RPC consumer startup-race fix and was smoke-validated on the UTM VM this session.

## Known-outstanding items (the new session must verify each and decide GA vs v1.3)

This is a **starting inventory, not a final scope** — the brainstorming step
exists to confirm the cut line with the user. Each item has a memory file under
`/Users/david/.claude/projects/-Users-david-development-src-opennms-delta-v/memory/`.

**Track 2 (Minion gRPC) tail — most likely GA-blocking:**

- **Minion Kafka removal** — `project_v1_2_minion_kafka_removal`. The Kafka IPC
  path has been a fallback since PR3; GA intent is to remove it so minion-gateway
  is the sole ingress. Confirm current state and whether removal is GA-blocking.
- **Topic rename `OpenNMS.*` / `opennms-*` → `DeltaV.*`** — `project_sink_topic_rename`.
  The user flagged this twice this cycle. Observed on the rc3.1 smoke: `Sink.*`
  topics ARE renamed (`DeltaV.Sink.Heartbeat`, `DeltaV.Sink.Telemetry-*`, etc.),
  but the **RPC topics** (`OpenNMS.<location>.rpc-request`, `OpenNMS.rpc-response`)
  and **event topics** (`opennms-fault-events`, `opennms-ipc-events`) are still
  legacy-prefixed. Decide: rename for GA, or accept and defer. This is currently
  un-scoped — the plan must scope it.
- **Retire the "Default" Minion location** — `project_retire_default_minion`.
  Horizon-era "here, wherever here is" semantic; incompatible with K8s named
  locations. Bundled with track 2 in the release plan.
- **Minion ActiveMQ/ServiceMix bundle purge + image slimming** —
  `project_minion_servicemix_activemq_cleanup`, `project_minion_image_slimming`.
  17 ActiveMQ + 15 ServiceMix bundles still ship in minion-boot; the gRPC track
  was the planned moment to drop them.

**Surfaced this session (2026-05-16) — likely v1.3, but the plan must rule):**

- **Dashboard actuator-metrics gap** — `project_dashboard_actuator_metrics_gap`.
  The "Polls/sec" and "Phase 3" counter panels on the pollerd/perspective
  dashboards show "No data" — `deltav_*` Micrometer counters never reach
  VictoriaMetrics (the `deltav-timeseries` pipeline carries only measurement
  series). Fix needs an actuator→VM scrape path. User said "fix after
  observability is fully baked" — decide if that means a GA item or v1.3.
- **perspective-app-init cold-start race** — `project_perspective_app_init_cold_start_race`.
  FIXED in PR #277 (merged to develop 2026-05-16); ships in the next cut after
  rc3.1. No action needed beyond confirming the next cut includes it.

**Release mechanics:**

- The next cut after rc3.1 must include **PR #277** (perspective-app-init retry
  loop + Google-Search seeding + dashboard location/service filters).
- GA version bump `1.2.0-rc3.1` → `1.2.0` (use `mvn versions:set`, never bulk-sed;
  remember to bump `.env.example` — see `feedback_image_tag_version_mismatch`).
- Final GA smoke run on the UTM VM per `reference_smoke_test_procedure` (note the
  `--profile full` requirement to exercise perspectivepollerd).
- v1.1.1 test-harness issues #201/#202/#203 — confirm closed or explicitly
  non-blocking.
- Release notes for v1.2.0 (the release plan lists v1.1.1 notes as "TBD" — check
  whether GA notes are expected).

## What the finalization plan must deliver

1. A reconciled, accurate statement of v1.2.0 state (supersede the stale
   "Remaining" section of the master release plan, or update it in place).
2. A decided **GA-blocking vs. v1.3-deferred** line for every item above, agreed
   with the user during brainstorming.
3. A sequenced task list for the GA-blocking items, each scoped to a PR, with
   dependencies and bundle-ability noted (the master plan's style).
4. The release-mechanics checklist (version bump, PR #277 inclusion, GA smoke,
   release notes) as explicit plan steps.
5. A definition of done: `v1.2.0` tagged, 26 images published, GA smoke green.

## Constraints (standard workflow guards)

- **Never PR against `OpenNMS/*`** — delta-v and delta-v-horizon are forks. Always
  `--repo pbrane/delta-v` (or `pbrane/delta-v-horizon`). See `feedback_never_pr_opennms`.
- **Never commit directly to `develop`/`main`** — feature branches + PRs only.
  See `feedback_feature_branches`.
- **Pull `develop` before branching.** See `feedback_pull_before_branching`.
- Tag pushes trigger the image-publish workflow and cannot be easily cancelled —
  see `feedback_delayed_tag_workflow_trigger`. A partial/transient publish failure
  is often just a re-run, not a re-publish — see `feedback_gh_packages_cached_notfound`.
- Smoke VM = release validation only; dev/local validation is docker-compose on
  the Mac. See `feedback_smoke_vm_release_only`.

## Related memory items

Release-scope: `project_v1_2_minion_kafka_removal`, `project_v1_2_minion_grpc_migration`,
`project_sink_topic_rename`, `project_retire_default_minion`,
`project_minion_servicemix_activemq_cleanup`, `project_minion_image_slimming`,
`project_dashboard_actuator_metrics_gap`, `project_perspective_app_init_cold_start_race`.

Workflow/release-mechanics: `feedback_never_pr_opennms`, `feedback_feature_branches`,
`feedback_pull_before_branching`, `feedback_image_tag_version_mismatch`,
`feedback_auxiliary_image_retag_on_version_bump`, `feedback_build_sh_and_gha_must_match`,
`reference_smoke_test_procedure`.

## Why now

rc3.1 closed out the perspective-polling track (RPC startup-race fix +
perspective-timeseries, both validated on the smoke VM 2026-05-16). With tracks
1, 3, and 4 done and track 2 in its tail, the release is close enough to GA that
it is worth one focused planning pass to enumerate the true remaining surface,
draw the GA cut line with the user, and sequence the run to `v1.2.0`.
