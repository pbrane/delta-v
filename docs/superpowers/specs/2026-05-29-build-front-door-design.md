# Design: `make` as the Single Build/Run Front Door

- **Date:** 2026-05-29
- **Status:** Draft for review
- **Author:** David Hustace (with Claude)
- **Scope:** Build & local-deploy tooling for delta-v. No application/runtime code changes.

## 1. Problem

Contributors report that the delta-v build "ecosystem" is confusing, and the
confusion is real. There are multiple, overlapping entry points with no single
documented front door, plus stale artifacts inherited from the OpenNMS Horizon
fork:

| Path | What it is | Disposition |
|------|------------|-------------|
| `./mvnw` | The real Maven build (21-module reactor) | Keep (internal engine) |
| `Makefile` (root) | Thin facade over `./mvnw`: `build/test/daemon/clean` | Keep & extend → **front door** |
| `opennms-container/delta-v/build.sh` | Docker image build (local, native arch) | Keep (internal engine) |
| `opennms-container/delta-v/build` | Stale 8.5 KB earlier copy of `build.sh` | **Delete** |
| `opennms-container/delta-v/deploy.sh` | Run-time stack lifecycle (`docker compose` wrapper) | Keep (internal engine) |
| `opennms-container/delta-v/compute-shared-libs.sh` | Fat-JAR dedup helper (called by `build.sh`) | Keep (rewrite for portability) |
| `opennms-container/Makefile` | Recursive dispatcher into subdir Makefiles — **no such subdirs exist** | **Delete** (dead) |
| `clean.pl` (root) | Leftover Horizon Perl | **Delete** |
| project `CLAUDE.md` | Documents `compile.pl`/`assemble.pl`/`make assemble`/`make module` — **none exist in delta-v** | **Fix** |

Three concrete failure modes — all the same root cause (no single source of
truth, build logic duplicated/divided across `build.sh` and CI YAML) — have
been observed:

1. **bash 3.2 crash.** `compute-shared-libs.sh` uses `declare -A` (bash 4+).
   Stock macOS ships bash 3.2.57. The script runs for maintainers (Homebrew
   bash 5) and in CI (Linux bash 5) but crashes on a clean Mac with
   `declare: -A: invalid option`.

2. **CI ↔ `build.sh` drift.** `.github/workflows/delta-v-build-images.yml`
   re-implements the build inline (`./mvnw install`, `./compute-shared-libs.sh`,
   ~20 per-image `docker buildx build` steps) rather than calling `build.sh`.
   The two must be hand-synced; an rc2 build already shipped with two images
   missing from one side.

3. **Auxiliary-image / version gap.** `build.sh deltav` builds the 12 daemons
   but **none** of the auxiliary images (mock-snmp-agent, flow/sflow-exporter,
   clickhouse, clickhouse-init, provisiond-imports-init, l8opensim-provisioner).
   Their Dockerfiles exist, but the only build recipe lives in CI YAML. Combined
   with `.env` being gitignored (so a fresh clone has no `VERSION`, and
   `docker compose` resolves the empty tag to `:latest`), `deploy.sh up` fails
   with `pull access denied for deltav/mock-snmp-agent` because the image was
   never built locally and is not published to Docker Hub.

## 2. Goals / Non-Goals

**Goals**

- One documented, executable front door: `make`. Same commands locally and in CI.
- `build.sh`, `deploy.sh`, `compute-shared-libs.sh` become **internal engines**;
  only `make` targets are documented/supported.
- Eliminate the CI/`build.sh` drift by having CI call the same `make` targets.
- `build.sh` becomes the single image engine, parameterized for both local
  native builds and multi-arch GHCR-push release builds, and covering **all**
  images (daemons + auxiliaries).
- `make doctor` preflight that catches the common newcomer failures before they
  manifest as confusing errors.
- `compute-shared-libs.sh` runs on stock macOS (bash 3.2) with zero prerequisites.
- Remove stale artifacts and fix stale docs.

**Non-Goals**

- No changes to application/daemon source or runtime behavior.
- No re-architecture of the Spring Boot fat-JAR layering or the compose topology.
- No change to the smoke-on-VM contract (smoke still runs on the UTM VM).
- Not inlining script logic into Make recipes (recipes stay thin).

## 3. Approach (selected: A — Parameterized single engine)

`make` is the orchestration layer; the existing scripts own the "how". The image
engine is unified and parameterized so local dev and CI release are one code path.

Rejected alternatives:
- **B — per-image make targets, CI keeps ~20 steps:** preserves CI step
  granularity but keeps the image list duplicated between Makefile and YAML
  (weaker drift survives).
- **C — consolidate on `build.sh`, make is a cosmetic local alias:** smallest
  change, but the real CI front door becomes `build.sh`, contradicting the goal.

## 4. Component Design

### 4.1 Root `Makefile` — the front door

Extend the existing root `Makefile` with the full lifecycle. Recipes are thin
delegations; no business logic in Make.

Target catalog (`make help` self-documents these):

```
# Build-time (delegate to ./mvnw and build.sh)
build           Compile + install all reactor modules (skip tests)
test            Build + run all tests
test-class      Single test class (MODULE=, TEST=)
daemon          Rebuild one daemon boot JAR (DAEMON=)
images          Build ALL Docker images (daemons + auxiliaries)
daemon-image    Build one daemon image (DAEMON=)
clean           mvn clean

# Run-time (delegate to deploy.sh)
up              Start stack (PROFILE=lite|passive|full)
down            Stop stack (preserve data)
reset           Stop + remove data volumes
status          Show service status
logs            Tail logs (SVC=)
verify          Run deploy.sh test (health checks)

# Composite / meta
dev             images + up   (one-command inner loop)
doctor          Preflight: verify the environment can build & run
help            List targets
```

Parameters are environment/`make`-variable overrides consumed by the engines:
`PROFILE`, `SVC`, `MODULE`, `TEST`, `DAEMON`, and (for image builds) `PUSH`,
`PLATFORMS`, `REGISTRY`, `ORG`, `VERSION`.

Portability: must stay within GNU Make 3.81 features (macOS ships 3.81). No
`.ONESHELL`, no `$(file ...)`, no GNU 4.x-only functions.

### 4.2 `build.sh` — the single image engine

`build.sh` becomes the one place that knows how to build every image, for both
targets:

- **Canonical image list** (single array in `build.sh`) that includes the
  auxiliaries currently only built in CI: mock-snmp-agent, flow-exporter,
  sflow-exporter, clickhouse, clickhouse-init, provisiond-imports-init,
  l8opensim-provisioner, grafana, envoy, db-init, minion-gateway, minion-boot,
  alarms-materializer, alerts-forwarder, prometheus-writer, flow-enricher, the
  per-daemon images, plus the JRE/daemon base images.
- **Parameterization** via env vars:
  - `PUSH=true|false` — `docker buildx ... --push` vs `--load` (default false).
  - `PLATFORMS` — e.g. `linux/amd64,linux/arm64` (default = host arch).
  - `REGISTRY` / `ORG` — image prefix (default `deltav`; CI sets `ghcr.io/pbrane`).
  - `VERSION` — image tag (default = resolved project version).
- **Local:** `make images` → native arch, `--load`, no push.
- **CI:** `make images PUSH=true PLATFORMS=linux/amd64,linux/arm64 ORG=...` →
  identical code path; the ~20 inline workflow steps collapse into the engine loop.

This closes failure mode #3: a single `make images` produces a complete local
image set including auxiliaries, so `make up` no longer hits missing images.

### 4.3 `compute-shared-libs.sh` — bash 3.2-safe rewrite

Remove the `declare -A` associative array. Replace the
`daemon → jar-path` map with a portable construct (a `case`-based lookup
function, or parallel indexed arrays) so the script runs unchanged on bash 3.2.
Add `shellcheck` clean-up while here. No behavior change to its output contract
(`shared-external/`, `shared-internal/`, `<daemon>/libs`, `<daemon>/app`,
`<daemon>/.main_class`).

### 4.4 `deploy.sh` — wrapped, unchanged in behavior

`deploy.sh` stays the run-time engine (`up/down/reset/status/logs/test`).
`make` targets delegate to it. CI does not call it (CI builds+pushes only;
smoke runs on the VM). Compose-profile logic, volume cleanup, and health probes
remain script-shaped.

### 4.5 `make doctor` — preflight gate

A small `scripts/doctor.sh` (bash 3.2-safe) invoked by `make doctor`, checking:

1. **JDK 21** present and `JAVA_HOME` resolves.
2. **Docker** installed and daemon running (`docker info`).
3. **GitHub Packages auth:** `~/.m2/settings.xml` contains a `<server>` with id
   `github-deltav-horizon`; optional live `curl` probe of the Maven registry to
   confirm the PAT works (read:packages).
4. **Image/version consistency (pre-`up`):** `.env` exists and `VERSION` is
   non-empty; every image referenced by `docker-compose.yml` exists locally at
   `${IMAGE_PREFIX}/<name>:${VERSION}`. Reports the first missing image with the
   remedy (`make images`).

Each check prints ✓/✗ with an actionable fix. Non-zero exit on any ✗.

### 4.6 `.env` handling

`.env` is gitignored (intentional — holds local overrides). To stop the
empty-tag→`latest` failure:

- Ship a committed **`.env.example`** with `VERSION=<current project version>`
  and `IMAGE_PREFIX=deltav`.
- `make doctor` (and `make up`) warn loudly if `.env` is missing or `VERSION`
  is empty, pointing at `cp .env.example .env`.
- Keep the existing `.env.example`↔`project.version` sync discipline.

### 4.7 CI cutover

Rewrite `.github/workflows/delta-v-build-images.yml` to call `make` targets:

```yaml
- run: make build
- run: make images PUSH=true PLATFORMS=linux/amd64,linux/arm64 ORG=ghcr.io/pbrane VERSION=${{ steps.version.outputs.version }}
```

The ~20 per-image build steps collapse into `make images` (which loops the
canonical list with per-image progress logging). The settings.xml/GHCR-login
setup steps remain. This is the change that actually eliminates drift: the image
list and build recipes exist once, in `build.sh`, consumed identically by laptop
and CI.

### 4.8 Cleanup (in scope)

- Delete `opennms-container/delta-v/build` (verify no CI/doc refs first).
- Delete `opennms-container/Makefile` (dead recursive dispatcher).
- Delete root `clean.pl`.
- Fix `CLAUDE.md`: replace `compile.pl`/`assemble.pl`/`make assemble`/`make module`
  references with the real `make`/`./mvnw` interface.
- Fix the retag step that produced the corrupted `*atest:latest` ghost tags, and
  document a one-liner to prune existing ghosts (`docker rmi` the `*atest` names).
  *(Cheap and observed; droppable if the user prefers to keep this PR tight.)*

## 5. Interface Contract (engine env vars)

| Var | Default | Consumed by | Meaning |
|-----|---------|-------------|---------|
| `PUSH` | `false` | build.sh | `--push` (true) vs `--load` (false) |
| `PLATFORMS` | host arch | build.sh | buildx target platforms |
| `REGISTRY`/`ORG` | `deltav` | build.sh, compose | image prefix |
| `VERSION` | resolved project version | build.sh, deploy.sh, compose | image tag |
| `PROFILE` | (none) | deploy.sh | compose profile (lite/passive/full) |
| `SVC` | (all) | deploy.sh | service name for `logs` |
| `MODULE`/`TEST`/`DAEMON` | (none) | mvnw | reactor selectors |

## 6. Error Handling

- Engines keep `set -euo pipefail`; fail fast with `ERROR:` + remedy.
- `make doctor` is the gate: a green `doctor` means `make build && make images && make up` should succeed.
- `make up` runs the doctor image/version check first and refuses to start with a missing-image message pointing to `make images`, instead of letting `docker compose` emit a confusing `pull access denied`.

## 7. Testing Strategy

A build system still needs verification:

- **shellcheck** on `build.sh`, `deploy.sh`, `compute-shared-libs.sh`, `doctor.sh`
  (add to CI as a lint step).
- **bash 3.2 compatibility:** run `compute-shared-libs.sh` and `doctor.sh` under
  `/bin/bash` (3.2) in a quick check (locally and/or a macOS CI runner if available).
- **`make images` completeness:** after a build, assert every image referenced by
  `docker-compose.yml` exists locally at `${VERSION}` (this is the doctor check —
  reuse it as a post-build assertion).
- **CI parity dry-run:** the cutover is validated by a green multi-arch pipeline
  run (the proof the drift is gone) before merge.
- **`make help`** lists all targets (sanity).

## 8. Risks & Rollout

- **Blast radius on the release pipeline.** The CI cutover touches multi-arch
  push to GHCR. Mitigation: land Makefile + engine changes first (local-validated),
  then flip CI in the same PR but validate against a throwaway tag/branch before
  the next real release tag.
- **Collapsing ~20 named CI steps into one `make images` step** reduces per-image
  visibility in the Actions UI. Mitigation: per-image progress logging in the
  engine loop; failures name the image.
- **Auxiliary build recipes move from YAML to `build.sh`.** Must transcribe each
  `docker buildx build` faithfully (build-args, context, Dockerfile path).
  Mitigation: do it image-by-image, diffing against the current YAML.

## 9. Out of Scope / Future

- Holistic daemon **config delivery** rework (tracked separately).
- GHCR package **visibility** automation (no API; web-UI only).
- `org.opennms.*` → `org.deltav.*` artifactId renames.
