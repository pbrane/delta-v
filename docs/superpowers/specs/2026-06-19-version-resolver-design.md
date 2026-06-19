# Design: single VERSION resolver to prevent image-tag drift

**Date:** 2026-06-19
**Status:** approved (brainstorm) + review round 1 folded in; pending implementation plan
**Branch:** `chore/version-resolver-drift` (independent of the enlinkd→nl6 PR #378)

## Problem

Delta-V images are tagged `${IMAGE_PREFIX}/<name>:${VERSION}`. Every runtime entry
point resolves `VERSION` from the **gitignored** `deploy/.env`:

- `make up|down|reset|status|logs` → `tools/deploy.sh` → `docker compose`
- the E2E `deploy/test-*.sh` scripts call `docker compose` **directly**
- bare `docker compose` invocations run by hand in `deploy/`
- `tools/doctor.sh` reads `.env` and fails if `VERSION` is empty (lines 43–61)

`deploy/.env` is local-only, so when `project.version` is bumped (e.g. rc → GA)
the local `.env` silently lags and `docker compose` runs **weeks-old content** at
the stale tag even though fresh images exist at the current `project.version`.
`tools/build.sh` already resolves `VERSION` from `project.version`, so the build
and run sides disagree. This recently surfaced as a confusing "provisiond can't
import anything" failure (local `.env` pinned `1.3.0-rc9` / horizon 1.0.18 while
`project.version` was `1.3.0` / horizon 1.0.19). Chronic ~2-month foot-gun — see
the memory `feedback_image_tag_version_mismatch`.

Goal: make tag drift **structurally impossible** for from-source flows at *every*
entry point (scripts, bare compose, doctor), while preserving the ability to run
a specific published tag (notably the no-source smoke VM).

## Key constraint — the smoke VM has no source tree

The post-tag smoke VM validates a **no-git-clone** contract: it pulls published
images and has **no `pom.xml`** to resolve `project.version` from. It legitimately
relies on `.env`'s `VERSION` (the published `IMG_TAG`). The fix must not break
that — when no pom is present, `.env` stays authoritative.

## Design — resolve from `project.version`, auto-sync the gitignored `.env`

A read of the review (round 1) showed that merely exporting `VERSION` inside the
three wrapper scripts would still leave **bare `docker compose`** and
**`tools/doctor.sh`** broken (both read `.env` directly). The robust fix is to
keep `.env` itself correct: resolve the authoritative version and **write it back
into the gitignored `deploy/.env`** whenever it differs. Then everything that
reads `.env` — scripts, bare compose, doctor — is automatically correct, and drift
self-heals on the next scripted invocation.

### VERSION precedence (highest → lowest)

1. **Explicit `VERSION` in the shell environment** — ad-hoc / CI / published-tag
   override. Respected as-is, exported, and **never written to `.env`** (it is an
   ephemeral override, not a new default).
2. **`project.version` from `pom.xml`** — the from-source default. When a pom is
   present this wins, and the resolver syncs it into `.env` (see below). A stale
   `.env` `VERSION` is therefore never the effective value on a source tree.
3. **`.env`'s `VERSION`** — authoritative only when there is no pom (the smoke VM /
   published-image case). The resolver does not touch `.env` in this case.

### Components

- **`tools/version.sh`** (new) — one sourceable helper, repo-root-relative to its
  own location (`tools/` → parent). Two single-purpose functions:
  - `deltav_resolve_version()` — **read-only** (w.r.t. `.env`). Sets and exports
    `VERSION` per the precedence above. When an explicit shell `VERSION` is honored
    (precedence #1) it also exports `DELTAV_VERSION_OVERRIDDEN=true` so
    `deltav_sync_env_version()` can cheaply tell an override from an auto-resolved
    value and skip writing `.env`. From-source resolution uses
    `./mvnw help:evaluate -Dexpression=project.version -q -DforceStdout`
    (authoritative). Fallback when `mvnw` is unavailable/fails: a POSIX parser that
    takes the first `<version>` **after `</parent>`** so the Spring Boot parent
    version (`<parent><version>4.0.3</version>`) is ignored and the project version
    (`1.3.0`) is selected, e.g.
    `awk '/<\/parent>/{p=1} p&&/<version>/{gsub(/.*<version>|<\/version>.*/,"");print;exit}' pom.xml`.
    (NOT `xmllint` — not installed on all hosts; NOT the current
    `grep '<version>0\.'` in build.sh — that is `0.x`-only and would miss `1.3.0`.)
    Result cached in a gitignored `target/.deltav-version` keyed by `pom.xml` mtime
    so repeated calls (many per test run) don't each pay the `mvnw` cost.
  - `deltav_sync_env_version()` — **writes `deploy/.env`.** Only acts when: a pom
    is present, no explicit shell `VERSION` override is in effect, `deploy/.env`
    exists, and its `VERSION` differs from (or is missing) the resolved value. It
    rewrites just the `VERSION=` line atomically — write to a temp file **in
    `deploy/`** (e.g. `deploy/.env.tmp`), then `mv` over `.env`; the temp must be on
    the same filesystem as `.env` (NOT `/tmp`, which may be a separate mount) for
    `mv` to be atomic. Preserves all other lines and comments, and logs a one-line
    notice when it writes. Skips when `DELTAV_VERSION_OVERRIDDEN=true`, and no-ops
    on the smoke VM (no pom).
- **`tools/build.sh`** — replace its inline `VERSION="$(... mvnw ...)"` with
  `source tools/version.sh; deltav_resolve_version`. Single source of truth; keep
  its "also-tag the `.env` version if it differs" behavior (now rarely triggers).
- **`tools/deploy.sh`** — `source tools/version.sh; deltav_resolve_version;
  deltav_sync_env_version` before any `docker compose` call. Its current
  empty-`VERSION` hard-error becomes unreachable on a source tree (resolver always
  sets it) and still guards the no-pom-no-.env case.
- **`deploy/test-lib.sh`** — source `version.sh` and call both functions once (the
  file is sourced by every `test-*.sh`), so each test's direct `docker compose`
  calls use the synced `.env` / exported `VERSION`.
- **`tools/doctor.sh`** — source `version.sh`; report the **resolved** version as
  authoritative. Its existing check evolves from "`.env` has a non-empty VERSION"
  to "resolved VERSION is non-empty AND (on a source tree) `.env` matches
  `project.version`", flagging drift (which `deltav_sync_env_version` will have
  already healed on any prior scripted run). The image-completeness check (lines
  53–61) uses the resolved version.
- **`deploy/.env.example`** — keep an active `VERSION=` line (so a cold bare
  `docker compose` on a fresh `cp .env.example .env` works before any script runs),
  but document it: "auto-synced to `project.version` by the build/deploy/test
  tooling on a source tree; to pull a published tag use `VERSION=<tag> make …`
  (ephemeral) or set it here on a no-source host (smoke VM)."

### Behavior change to document

On a source tree, `deploy/.env`'s `VERSION` is now **auto-managed** — pinning it to
a published tag there will be overwritten on the next scripted run. To run
published images while on a source tree, use an ephemeral shell override
(`VERSION=<tag> make up`). The no-source smoke VM is unaffected (manual `.env`).

## Testing

- **`tools/version.test.sh`** (new, plain POSIX shell, standalone) — precedence +
  sync matrix using temp fixtures:
  1. shell `VERSION=foo` → resolver keeps `foo`; `deltav_sync_env_version` does NOT
     write `.env`.
  2. pom present, no shell override, stale `.env VERSION=old` → resolver yields
     `project.version`; sync rewrites `.env` to `project.version`, leaving other
     lines/comments intact.
  3. no pom, `.env VERSION=bar` → resolver leaves `VERSION` unset and `.env`
     untouched (compose reads `bar`).
  4. fallback parser: against a fixture pom with a `<parent><version>` and a
     project `<version>`, returns the project version.
  5. cache: a second `deltav_resolve_version` within the same `pom.xml` mtime does
     not re-invoke `mvnw`.
- **Manual integration check:** with a deliberately stale `deploy/.env`
  (`VERSION=0.0.0-stale`), confirm (a) `make up`, (b) one `deploy/test-*.sh`, and
  (c) a bare `docker compose config` afterwards all resolve images at the real
  `project.version`; and `make doctor` passes.

## Scope

- **In:** `tools/version.sh`, `tools/version.test.sh`, `tools/build.sh`,
  `tools/deploy.sh`, `tools/doctor.sh`, `deploy/test-lib.sh`,
  `deploy/.env.example`.
- **Out:** the enlinkd→nl6 migration (PR #378); any change to image *content* or
  the build graph; CI workflow changes (CI calls `make images` → `build.sh` → the
  shared resolver, inheriting the fix for free).
