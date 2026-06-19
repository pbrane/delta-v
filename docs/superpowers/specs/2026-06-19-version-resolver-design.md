# Design: single VERSION resolver to prevent image-tag drift

**Date:** 2026-06-19
**Status:** approved (brainstorm), pending implementation plan
**Branch:** `chore/version-resolver-drift` (independent of the enlinkd→nl6 PR #378)

## Problem

Delta-V images are tagged `${IMAGE_PREFIX}/<name>:${VERSION}`. Today every
runtime entry point resolves `VERSION` from the **gitignored** `deploy/.env`:

- `make up|down|reset|status|logs` → `tools/deploy.sh` → `docker compose`
- the E2E `deploy/test-*.sh` scripts call `docker compose` **directly**
- bare `docker compose` invocations

`deploy/.env` is local-only, so when `project.version` is bumped (e.g. an rc → GA
transition) the local `.env` silently lags. Result: `docker compose` resolves
images at the stale tag and runs **weeks-old content** even though fresh images
exist at the current `project.version`. This recently surfaced as a confusing
"provisiond can't import anything" failure — the local `.env` was pinned to
`1.3.0-rc9` (horizon 1.0.18) while `project.version`/`.env.example` were already
`1.3.0` (horizon 1.0.19). `tools/build.sh` already resolves `VERSION` correctly
from `project.version`, so the build and run sides disagree.

This is a chronic, ~2-month-old foot-gun (see the memory
`feedback_image_tag_version_mismatch`). The goal: make tag drift **structurally
impossible** for from-source flows, at every entry point, while preserving the
ability to run a specific published tag (e.g. on the smoke VM).

## Key constraint — the smoke VM has no source tree

The post-tag smoke VM validates a **no-git-clone** contract: it pulls published
images and has **no `pom.xml`** to resolve `project.version` from. It legitimately
relies on `.env`'s `VERSION` (the published `IMG_TAG`). So the fix must not delete
the `.env` override path — it must reorder precedence so a pom, when present, wins.

## Design

### VERSION precedence (highest → lowest)

1. **Explicit `VERSION` in the shell environment** — ad-hoc / CI / smoke override.
   Respected as-is, never overwritten.
2. **`project.version` from `pom.xml`** — the from-source default. **When a pom is
   present this wins over `.env`**, so a stale `.env` `VERSION` is never consulted →
   drift is impossible for any from-source flow.
3. **`.env`'s `VERSION`** — used only when there is no pom (the smoke VM /
   published-image case), which matches its real purpose. `docker compose` reads
   `.env` natively, so this path needs no special handling beyond *not* exporting
   an override.

### Components

- **`tools/version.sh`** (new) — one sourceable helper exposing
  `deltav_resolve_version()`:
  - If `VERSION` is already set in the environment, return (respect override).
  - Else, if `<repo-root>/pom.xml` exists, resolve `project.version` and
    `export VERSION`. Resolution uses `./mvnw help:evaluate
    -Dexpression=project.version -q -DforceStdout` (authoritative). NOTE: the
    current `tools/build.sh` grep fallback is `grep '<version>0\.'` — it only
    matches `0.x` and would silently miss `1.3.0`; the shared helper must
    **generalize** the fallback to extract the project `<version>` (the one
    outside `<parent>`/plugin blocks, e.g. via `xmllint --xpath` on
    `/project/version`, or a `<parent>`-aware sed) rather than copy the 0.x-only
    pattern. The result is cached in a gitignored
    `target/.deltav-version` keyed by `pom.xml` mtime, so the many `docker compose`
    calls in a single test script don't each pay the ~seconds `mvnw` cost.
  - Else (no pom): do nothing — leave `VERSION` unset so `docker compose` reads
    `.env` natively (smoke VM path).
  - Computes repo root relative to its own location (`tools/` → parent).
- **`tools/build.sh`** — replace its inline `VERSION="$(... mvnw ...)"` line with a
  `source tools/version.sh; deltav_resolve_version`. Single source of truth. Keep
  its existing "also-tag the `.env` version if it differs" behavior (now rarely
  triggers, harmless).
- **`tools/deploy.sh`** — `source tools/version.sh; deltav_resolve_version` before
  any `docker compose` call, so `make up|down|reset|status|logs` use the resolved
  tag.
- **`deploy/test-lib.sh`** — source `version.sh` and call the resolver once (the
  file is sourced by every `test-*.sh`), so each test's direct `docker compose`
  calls inherit the exported `VERSION`.
- **`deploy/.env.example`** — demote the `VERSION=` pin to a commented, documented
  override:
  > `# VERSION is auto-resolved from project.version when building from source.`
  > `# Set it ONLY to pull a published tag / on the smoke VM (no source tree).`
  > `# VERSION=1.3.0`
- **Legacy-`.env` nudge** — when a pom is present *and* `deploy/.env` contains an
  active `VERSION=` line, `deltav_resolve_version()` prints a one-line warning to
  stderr that the `.env` `VERSION` is now ignored in favor of `project.version`
  (so existing devs understand why their pin no longer takes effect, and can
  remove it). Warning only; never fatal.

### Smoke VM

No code change required: with no pom present, the resolver leaves `VERSION` unset
and `docker compose` reads `.env` as before. If `tools/smoke-vm*` sets `VERSION`
via the shell instead of `.env`, that also works (precedence #1). The existing
`.env`-based smoke procedure is unaffected.

## Testing

- **`tools/version.test.sh`** (new, plain shell, runnable standalone) asserting the
  precedence matrix:
  1. shell `VERSION=foo` → resolver keeps `foo`.
  2. pom present, no shell override, stale `.env VERSION=old` → resolver yields
     `project.version` (not `old`).
  3. no pom, `.env VERSION=bar` → resolver leaves `VERSION` unset (compose reads
     `.env` → `bar`).
  4. cache: second call within the same `pom.xml` mtime does not re-invoke `mvnw`.
- **Manual integration check:** with a deliberately stale `deploy/.env`
  (`VERSION=0.0.0-stale`), confirm `make up` and one `deploy/test-*.sh` both
  resolve images at the real `project.version` (inspect `docker compose config`
  / running container image tags).

## Scope

- **In:** `tools/version.sh`, `tools/build.sh`, `tools/deploy.sh`,
  `deploy/test-lib.sh`, `deploy/.env.example`, `tools/version.test.sh`.
- **Out:** the enlinkd→nl6 migration (PR #378); any change to image *content* or
  the build graph; CI workflow changes (CI already calls `make images`, which goes
  through `build.sh` → the shared resolver, so it inherits the fix for free).
