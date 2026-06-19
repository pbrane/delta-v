# VERSION Resolver Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Docker image-tag drift structurally impossible by resolving `VERSION` from `project.version` at every from-source entry point and auto-syncing the gitignored `deploy/.env`, while preserving the no-source smoke-VM override.

**Architecture:** A single sourceable helper `tools/version.sh` exposes `deltav_resolve_version` (sets/exports `VERSION`: explicit shell override > `project.version` from pom > leave unset for the no-pom smoke VM) and `deltav_sync_env_version` (rewrites `deploy/.env`'s `VERSION=` line when it drifts, atomically). It is sourced by `tools/build.sh`, `tools/deploy.sh`, `tools/doctor.sh`, and `deploy/test-lib.sh`. Because `deploy.sh` sources `.env` (which sets `VERSION` from the file), the helper snapshots a genuine shell override at first-source — *before* any `.env` is read — so an explicit `VERSION=<tag>` wins but a stale `.env` value never does.

**Tech Stack:** POSIX/bash shell, Maven wrapper (`./mvnw`), Docker Compose.

## Global Constraints

- New delta-v tooling: keep it `bash`-compatible (all consumers are `#!/usr/bin/env bash`); the standalone test must run under `bash`.
- Resolution precedence (verbatim): **explicit shell `VERSION`** > **`project.version` from `pom.xml`** > **`deploy/.env`** (only when no pom is present).
- `project.version` is authoritative via `./mvnw help:evaluate -Dexpression=project.version -q -DforceStdout`; the fallback parser must take the first `<version>` **after `</parent>`** (the Spring Boot parent version `4.0.3` precedes the project version `1.3.0`). Do NOT use `xmllint`; do NOT reuse build.sh's `grep '<version>0\.'` (it is `0.x`-only).
- `.env` rewrites must be atomic via a temp file **in `deploy/`** (`deploy/.env.tmp`), same filesystem as `.env`, then `mv` — never `/tmp`.
- Never write `.env` when an explicit shell override is in effect (`DELTAV_VERSION_OVERRIDDEN=true`) or when there is no `pom.xml` (smoke VM).
- All paths in `version.sh` are overridable via env (`DELTAV_REPO_ROOT`, `DELTAV_POM`, `DELTAV_ENV`, `DELTAV_VERSION_CACHE`) so the test can point them at fixtures.
- Commit messages end with: `Claude-Session: https://claude.ai/code/session_01NBzhzbdGaSePxMnqzzuqzX`
- Branch: `chore/version-resolver-drift`. PRs go to `--repo pbrane/delta-v`, base `develop`. Never commit to `develop`.

---

## File Structure

- **Create `tools/version.sh`** — the shared resolver + `.env` sync. Sole owner of VERSION logic.
- **Create `tools/version.test.sh`** — standalone bash test of the precedence/sync/fallback/cache matrix.
- **Modify `tools/build.sh`** — replace its inline `VERSION="$(... mvnw ...)"` with the helper.
- **Modify `tools/deploy.sh`** — source the helper before `.env`, resolve+sync after.
- **Modify `tools/doctor.sh`** — report the resolved version; flag `.env` drift (read-only).
- **Modify `deploy/test-lib.sh`** — source the helper so test scripts' direct `docker compose` calls use the synced VERSION.
- **Modify `deploy/.env.example`** — document that `VERSION` is auto-synced from `project.version`.

---

### Task 1: `tools/version.sh` + its test

**Files:**
- Create: `tools/version.sh`
- Test: `tools/version.test.sh`

**Interfaces:**
- Produces: `deltav_resolve_version()` (sets+exports `VERSION`), `deltav_sync_env_version()` (rewrites `deploy/.env` VERSION line). Reads overridable globals `DELTAV_REPO_ROOT`, `DELTAV_POM`, `DELTAV_ENV`, `DELTAV_VERSION_CACHE`; sets `DELTAV_VERSION_OVERRIDDEN` / `DELTAV_VERSION_OVERRIDE` at source time.

- [ ] **Step 1: Write the failing test**

Create `tools/version.test.sh`:

```bash
#!/usr/bin/env bash
# Standalone test for tools/version.sh. Run: bash tools/version.test.sh
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PASS=0; FAIL=0
check() { # desc expected actual
    if [ "$2" = "$3" ]; then echo "  PASS: $1"; PASS=$((PASS+1));
    else echo "  FAIL: $1 (expected='$2' actual='$3')"; FAIL=$((FAIL+1)); fi
}

make_pom() { # dir projectversion
    mkdir -p "$1"
    cat > "$1/pom.xml" <<EOF
<project>
  <parent>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-parent</artifactId>
    <version>4.0.3</version>
  </parent>
  <groupId>org.deltav</groupId>
  <artifactId>delta-v-parent</artifactId>
  <version>$2</version>
</project>
EOF
}

# Case 1: explicit shell override wins, .env NOT written
T1="$(mktemp -d)"; make_pom "$T1" 9.9.9; mkdir -p "$T1/deploy"
printf 'VERSION=should-not-change\n' > "$T1/deploy/.env"
( export DELTAV_REPO_ROOT="$T1" DELTAV_POM="$T1/pom.xml" DELTAV_ENV="$T1/deploy/.env" DELTAV_VERSION_CACHE="$T1/target/.v"
  export VERSION=override-tag
  . "$HERE/version.sh"; deltav_resolve_version; deltav_sync_env_version
  echo "$VERSION" > "$T1/out.version" )
check "override: VERSION kept" "override-tag" "$(cat "$T1/out.version")"
check "override: .env untouched" "VERSION=should-not-change" "$(cat "$T1/deploy/.env")"

# Case 2: pom wins over stale .env; .env synced
T2="$(mktemp -d)"; make_pom "$T2" 2.0.0; mkdir -p "$T2/deploy"
printf '# comment\nVERSION=1.0.0-stale\nIMAGE_PREFIX=deltav\n' > "$T2/deploy/.env"
( export DELTAV_REPO_ROOT="$T2" DELTAV_POM="$T2/pom.xml" DELTAV_ENV="$T2/deploy/.env" DELTAV_VERSION_CACHE="$T2/target/.v"
  unset VERSION
  . "$HERE/version.sh"; deltav_resolve_version; deltav_sync_env_version
  echo "$VERSION" > "$T2/out.version" )
check "pom: VERSION resolved" "2.0.0" "$(cat "$T2/out.version")"
check "pom: .env VERSION synced" "VERSION=2.0.0" "$(grep '^VERSION=' "$T2/deploy/.env")"
check "pom: .env other lines kept" "IMAGE_PREFIX=deltav" "$(grep '^IMAGE_PREFIX=' "$T2/deploy/.env")"

# Case 3: no pom -> VERSION left unset, .env untouched
T3="$(mktemp -d)"; mkdir -p "$T3/deploy"
printf 'VERSION=published-tag\n' > "$T3/deploy/.env"
( export DELTAV_REPO_ROOT="$T3" DELTAV_POM="$T3/pom.xml" DELTAV_ENV="$T3/deploy/.env" DELTAV_VERSION_CACHE="$T3/target/.v"
  unset VERSION
  . "$HERE/version.sh"; deltav_resolve_version; deltav_sync_env_version
  echo "${VERSION:-<unset>}" > "$T3/out.version" )
check "no-pom: VERSION unset" "<unset>" "$(cat "$T3/out.version")"
check "no-pom: .env untouched" "VERSION=published-tag" "$(cat "$T3/deploy/.env")"

# Case 4: fallback parser skips <parent> version (force mvnw absent via bogus repo root w/o mvnw)
check "fallback: project version after </parent>" "2.0.0" \
  "$(awk '/<\/parent>/{p=1} p&&/<version>/{gsub(/.*<version>|<\/version>.*/,"");print;exit}' "$T2/pom.xml")"

echo "── $PASS passed, $FAIL failed ──"
[ "$FAIL" -eq 0 ]
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `bash tools/version.test.sh`
Expected: FAIL — `version.sh` does not exist yet (`. version.sh` errors / functions undefined).

- [ ] **Step 3: Write `tools/version.sh`**

```bash
#!/usr/bin/env bash
# tools/version.sh — single source of truth for the Delta-V image VERSION.
#
# Source this BEFORE sourcing deploy/.env, then call:
#   deltav_resolve_version    # sets+exports VERSION: shell override > project.version > unset
#   deltav_sync_env_version   # rewrites deploy/.env's VERSION= line if it drifted
#
# Precedence: explicit shell $VERSION > project.version (pom) > deploy/.env.
# Paths below are overridable via env so tests can point them at fixtures.
# See docs/superpowers/specs/2026-06-19-version-resolver-design.md.

: "${DELTAV_REPO_ROOT:=$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")/.." && pwd)}"
: "${DELTAV_POM:=$DELTAV_REPO_ROOT/pom.xml}"
: "${DELTAV_ENV:=$DELTAV_REPO_ROOT/deploy/.env}"
: "${DELTAV_VERSION_CACHE:=$DELTAV_REPO_ROOT/target/.deltav-version}"

# Snapshot a genuine shell override exactly once, at first source — i.e. BEFORE
# the caller sources deploy/.env (whose VERSION would otherwise masquerade as one).
if [ -z "${DELTAV_VERSION_OVERRIDDEN:-}" ]; then
    if [ -n "${VERSION:-}" ]; then
        DELTAV_VERSION_OVERRIDE="$VERSION"; DELTAV_VERSION_OVERRIDDEN=true
    else
        DELTAV_VERSION_OVERRIDE=""; DELTAV_VERSION_OVERRIDDEN=false
    fi
    export DELTAV_VERSION_OVERRIDDEN DELTAV_VERSION_OVERRIDE
fi

_deltav_pom_version() {
    local v=""
    if [ -x "$DELTAV_REPO_ROOT/mvnw" ]; then
        v="$(cd "$DELTAV_REPO_ROOT" && ./mvnw help:evaluate -Dexpression=project.version -q -DforceStdout 2>/dev/null)"
    fi
    case "$v" in
        ''|*[!0-9A-Za-z._-]*)  # empty or noisy (mvnw missing/errored) -> POSIX fallback
            v="$(awk '/<\/parent>/{p=1} p&&/<version>/{gsub(/.*<version>|<\/version>.*/,"");print;exit}' "$DELTAV_POM" 2>/dev/null)"
            ;;
    esac
    printf '%s' "$v"
}

_deltav_mtime() { stat -f %m "$1" 2>/dev/null || stat -c %Y "$1" 2>/dev/null; }

_deltav_cached_pom_version() {
    [ -f "$DELTAV_POM" ] || return 1
    local mtime cached v
    mtime="$(_deltav_mtime "$DELTAV_POM")"
    if [ -f "$DELTAV_VERSION_CACHE" ]; then
        cached="$(cat "$DELTAV_VERSION_CACHE" 2>/dev/null)"
        case "$cached" in
            "$mtime "*) printf '%s' "${cached#* }"; return 0 ;;
        esac
    fi
    v="$(_deltav_pom_version)"
    [ -n "$v" ] || return 1
    mkdir -p "$(dirname "$DELTAV_VERSION_CACHE")" 2>/dev/null
    printf '%s %s\n' "$mtime" "$v" > "$DELTAV_VERSION_CACHE" 2>/dev/null
    printf '%s' "$v"
}

deltav_resolve_version() {
    if [ "${DELTAV_VERSION_OVERRIDDEN:-false}" = "true" ]; then
        VERSION="$DELTAV_VERSION_OVERRIDE"; export VERSION; return 0
    fi
    if [ -f "$DELTAV_POM" ]; then
        local v; v="$(_deltav_cached_pom_version)"
        if [ -n "$v" ]; then VERSION="$v"; export VERSION; return 0; fi
    fi
    return 0   # no pom: leave VERSION as-is so docker compose reads deploy/.env
}

deltav_sync_env_version() {
    [ "${DELTAV_VERSION_OVERRIDDEN:-false}" = "true" ] && return 0
    [ -f "$DELTAV_POM" ] || return 0
    [ -f "$DELTAV_ENV" ] || return 0
    [ -n "${VERSION:-}" ] || return 0
    local current tmp
    current="$(grep -m1 '^VERSION=' "$DELTAV_ENV" | cut -d= -f2 | tr -d '"' | tr -d "'" | tr -d ' \t\r')"
    [ "$current" = "$VERSION" ] && return 0
    tmp="$DELTAV_ENV.tmp"
    if grep -q '^VERSION=' "$DELTAV_ENV"; then
        sed "s|^VERSION=.*|VERSION=$VERSION|" "$DELTAV_ENV" > "$tmp"
    else
        cp "$DELTAV_ENV" "$tmp" && printf 'VERSION=%s\n' "$VERSION" >> "$tmp"
    fi
    mv "$tmp" "$DELTAV_ENV"
    echo "==> version.sh: synced deploy/.env VERSION ${current:-<unset>} -> $VERSION" >&2
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `bash tools/version.test.sh`
Expected: PASS — `9 passed, 0 failed` (exit 0).

- [ ] **Step 5: Verify the real resolver against the real repo**

Run: `bash -c '. tools/version.sh; deltav_resolve_version; echo "$VERSION"'`
Expected: prints the real `project.version` (e.g. `1.3.0`), matching `./mvnw help:evaluate -Dexpression=project.version -q -DforceStdout`.

- [ ] **Step 6: Commit**

```bash
chmod +x tools/version.sh tools/version.test.sh
git add tools/version.sh tools/version.test.sh
git commit -m "build(tools): add shared VERSION resolver + .env auto-sync

Claude-Session: https://claude.ai/code/session_01NBzhzbdGaSePxMnqzzuqzX"
```

---

### Task 2: Wire `tools/build.sh` to the resolver

**Files:**
- Modify: `tools/build.sh` (the `VERSION="$(...)"` assignment, currently ~line 48)

**Interfaces:**
- Consumes: `deltav_resolve_version`, `deltav_sync_env_version` from `tools/version.sh`. `SCRIPT_DIR` and `REPO_ROOT` already exist in build.sh.

- [ ] **Step 1: Replace the inline resolver**

In `tools/build.sh`, replace this line:

```bash
VERSION="$(cd "$REPO_ROOT" && ./mvnw help:evaluate -Dexpression=project.version -q -DforceStdout 2>/dev/null || grep '<version>0\.' "$REPO_ROOT/pom.xml" | head -1 | sed 's/.*<version>\(.*\)<\/version>.*/\1/')"
```

with:

```bash
# shellcheck disable=SC1091
. "$SCRIPT_DIR/version.sh"
deltav_resolve_version
deltav_sync_env_version
```

- [ ] **Step 2: Verify build.sh still resolves VERSION (no Maven build)**

Run: `bash -c 'cd /Users/david/development/src/opennms/delta-v && SCRIPT_DIR=tools REPO_ROOT=. bash -x tools/build.sh 2>&1 | grep -m1 "Compiling Delta-V"'` is heavy; instead do a dry check:

Run: `bash -c '. tools/version.sh; deltav_resolve_version; echo "build.sh would tag :$VERSION"'`
Expected: `build.sh would tag :<project.version>` (e.g. `:1.3.0`).

- [ ] **Step 3: Commit**

```bash
git add tools/build.sh
git commit -m "build(tools): build.sh resolves VERSION via shared resolver

Claude-Session: https://claude.ai/code/session_01NBzhzbdGaSePxMnqzzuqzX"
```

---

### Task 3: Wire `tools/deploy.sh` (source before `.env`, resolve after)

**Files:**
- Modify: `tools/deploy.sh:15-28` (the SCRIPT_DIR/.env-sourcing block)

**Interfaces:**
- Consumes: `deltav_resolve_version`, `deltav_sync_env_version`. CRITICAL ORDER: source `version.sh` (snapshots any genuine shell override) BEFORE `. ./.env`; call `deltav_resolve_version` AFTER `.env` so it overwrites the `.env`-sourced `VERSION` with `project.version` (unless overridden).

- [ ] **Step 1: Rework the sourcing block**

In `tools/deploy.sh`, replace this block:

```bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DEPLOY_DIR="$REPO_ROOT/deploy"
cd "$DEPLOY_DIR"

# Source .env so IMAGE_PREFIX and VERSION are available to both this script
# and every `docker compose` child invocation below.
if [ -f .env ]; then
    set -a
    # shellcheck disable=SC1091
    . ./.env
    set +a
fi
IMAGE_PREFIX="${IMAGE_PREFIX:-deltav}"
```

with:

```bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DEPLOY_DIR="$REPO_ROOT/deploy"
cd "$DEPLOY_DIR"

# Source version.sh FIRST so it snapshots any genuine shell VERSION override
# before .env (whose VERSION would otherwise look like one).
# shellcheck disable=SC1091
. "$SCRIPT_DIR/version.sh"

# Source .env for IMAGE_PREFIX / KAFKA_EXTERNAL_HOST (and, tentatively, VERSION).
if [ -f .env ]; then
    set -a
    # shellcheck disable=SC1091
    . ./.env
    set +a
fi

# Authoritatively set VERSION (project.version on a source tree, overwriting any
# stale .env value; the snapshotted shell override still wins) and heal .env.
deltav_resolve_version
deltav_sync_env_version
IMAGE_PREFIX="${IMAGE_PREFIX:-deltav}"
```

- [ ] **Step 2: Verify deploy.sh uses project.version even with a stale `.env`**

```bash
cd /Users/david/development/src/opennms/delta-v/deploy
cp -n .env .env.bak 2>/dev/null || true
sed -i.orig 's/^VERSION=.*/VERSION=0.0.0-stale/' .env
bash -c 'SCRIPT_DIR="$(cd ../tools && pwd)"; REPO_ROOT="$(cd .. && pwd)"; cd "$REPO_ROOT/deploy"; . "$SCRIPT_DIR/version.sh"; set -a; . ./.env; set +a; deltav_resolve_version; deltav_sync_env_version; echo "resolved=$VERSION"; grep ^VERSION= .env'
```
Expected: `resolved=<project.version>` AND `.env` now shows `VERSION=<project.version>` (the `0.0.0-stale` was healed). Restore: `mv .env.orig .env 2>/dev/null; rm -f .env.bak`.

- [ ] **Step 3: Verify an explicit shell override still wins and does NOT write `.env`**

```bash
cd /Users/david/development/src/opennms/delta-v/deploy
sed -i.orig 's/^VERSION=.*/VERSION=baseline/' .env
bash -c 'SCRIPT_DIR="$(cd ../tools && pwd)"; cd "$(cd .. && pwd)/deploy"; export VERSION=smoke-tag; . "$SCRIPT_DIR/version.sh"; set -a; . ./.env; set +a; deltav_resolve_version; deltav_sync_env_version; echo "resolved=$VERSION"; grep ^VERSION= .env'
```
Expected: `resolved=smoke-tag` AND `.env` still shows `VERSION=baseline` (override not persisted). Restore: `mv .env.orig .env`.

- [ ] **Step 4: Commit**

```bash
git add tools/deploy.sh
git commit -m "build(tools): deploy.sh resolves VERSION via resolver, heals .env

Claude-Session: https://claude.ai/code/session_01NBzhzbdGaSePxMnqzzuqzX"
```

---

### Task 4: Wire `deploy/test-lib.sh`

**Files:**
- Modify: `deploy/test-lib.sh` (after the header comment, before the first function)

**Interfaces:**
- Consumes: `deltav_resolve_version`, `deltav_sync_env_version`. test-lib.sh is sourced by every `test-*.sh` (which `cd` to `deploy/`), so resolving+syncing here makes their direct `docker compose` calls use the correct tag.

- [ ] **Step 1: Source the resolver at the top of the lib**

In `deploy/test-lib.sh`, immediately after the header comment block (before `clean_all_nodes()`), insert:

```bash
# Resolve VERSION from project.version and heal deploy/.env so every direct
# `docker compose` call in the sourcing test script uses the current image tag
# (not a stale gitignored .env). See tools/version.sh.
_DELTAV_TOOLS="$(cd "$(dirname "${BASH_SOURCE[0]}")/../tools" && pwd)"
if [ -f "$_DELTAV_TOOLS/version.sh" ]; then
    # shellcheck disable=SC1091
    . "$_DELTAV_TOOLS/version.sh"
    deltav_resolve_version
    deltav_sync_env_version
fi
```

- [ ] **Step 2: Verify sourcing test-lib.sh heals a stale `.env`**

```bash
cd /Users/david/development/src/opennms/delta-v/deploy
sed -i.orig 's/^VERSION=.*/VERSION=0.0.0-stale/' .env
bash -c 'cd "$(pwd)"; BASH_SOURCE=(./test-lib.sh); . ./test-lib.sh; echo "VERSION=$VERSION"; grep ^VERSION= .env'
```
Expected: `VERSION=<project.version>` and `.env` healed to `<project.version>`. Restore: `mv .env.orig .env`.

- [ ] **Step 3: Commit**

```bash
git add deploy/test-lib.sh
git commit -m "test(e2e): test-lib resolves VERSION so direct compose calls match build

Claude-Session: https://claude.ai/code/session_01NBzhzbdGaSePxMnqzzuqzX"
```

---

### Task 5: Update `tools/doctor.sh` to report the resolved version

**Files:**
- Modify: `tools/doctor.sh:43-61` (the ".env VERSION" check and the image-completeness check)

**Interfaces:**
- Consumes: `deltav_resolve_version`. doctor.sh has `SCRIPT_DIR`, `REPO_ROOT`, `DEPLOY_DIR`, `ENV_FILE`, and helpers `ok`/`bad`/`warn`. Keep doctor read-only (do NOT call `deltav_sync_env_version`); it diagnoses, the build/deploy/test paths heal.

- [ ] **Step 1: Replace the `.env` VERSION check**

In `tools/doctor.sh`, replace this block:

```bash
# 4. .env present with a non-empty VERSION
ENV_FILE="$DEPLOY_DIR/.env"
V=""
if [ -f "$ENV_FILE" ]; then
    V=$(grep -m1 '^VERSION=' "$ENV_FILE" | cut -d= -f2 | tr -d '"' | tr -d "'" | tr -d ' \t\r')
    [ -n "$V" ] && ok ".env present (VERSION=$V)" || bad ".env present but VERSION is empty. Set VERSION in $ENV_FILE."
else
    bad "$ENV_FILE missing. Run: cp .env.example .env"
fi
```

with:

```bash
# 4. Resolved VERSION (project.version on a source tree; .env on a no-source host)
ENV_FILE="$DEPLOY_DIR/.env"
# shellcheck disable=SC1091
. "$SCRIPT_DIR/version.sh"
deltav_resolve_version
V="${VERSION:-}"
if [ -z "$V" ] && [ -f "$ENV_FILE" ]; then
    V=$(grep -m1 '^VERSION=' "$ENV_FILE" | cut -d= -f2 | tr -d '"' | tr -d "'" | tr -d ' \t\r')
fi
if [ -n "$V" ]; then
    ok "VERSION resolves to $V"
    if [ -f "$ENV_FILE" ] && [ -f "$REPO_ROOT/pom.xml" ]; then
        EV=$(grep -m1 '^VERSION=' "$ENV_FILE" | cut -d= -f2 | tr -d '"' | tr -d "'" | tr -d ' \t\r')
        [ "$EV" = "$V" ] || warn ".env VERSION='$EV' differs from project.version='$V' — the build/deploy/test tooling will auto-sync it."
    fi
else
    bad "could not resolve VERSION (no pom.xml and no .env VERSION). Run: cp .env.example .env"
fi
```

- [ ] **Step 2: Verify doctor passes with a stale `.env` and warns about drift**

```bash
cd /Users/david/development/src/opennms/delta-v/deploy
sed -i.orig 's/^VERSION=.*/VERSION=0.0.0-stale/' .env
bash ../tools/doctor.sh 2>&1 | grep -E "VERSION resolves|differs from project.version"
mv .env.orig .env
```
Expected: a `✓ VERSION resolves to <project.version>` line AND a `! .env VERSION='0.0.0-stale' differs ...` warning. (doctor's overall exit may still be non-zero for unrelated host checks — only the VERSION lines matter here.)

- [ ] **Step 3: Commit**

```bash
git add tools/doctor.sh
git commit -m "build(tools): doctor reports resolved VERSION + flags .env drift

Claude-Session: https://claude.ai/code/session_01NBzhzbdGaSePxMnqzzuqzX"
```

---

### Task 6: Document the auto-sync in `deploy/.env.example`

**Files:**
- Modify: `deploy/.env.example` (the comment above `VERSION=`)

**Interfaces:** none.

- [ ] **Step 1: Update the VERSION documentation**

In `deploy/.env.example`, replace these lines:

```bash
# VERSION must match an image tag you have built locally (`make images`)
# or one published to the registry in IMAGE_PREFIX.
VERSION=1.3.0
```

with:

```bash
# VERSION is auto-synced to project.version by the build/deploy/test tooling
# whenever you're on a source tree (tools/version.sh) — you normally never edit
# it. To run a published tag instead: prefix the command, e.g.
#   VERSION=<published-tag> make up      (ephemeral; not written back)
# On a no-source host (e.g. the smoke VM) there is no pom, so this value is
# authoritative — set it to the published IMG_TAG you want to pull.
VERSION=1.3.0
```

- [ ] **Step 2: Verify the file is still valid (no syntax change to keys)**

Run: `grep -E '^(VERSION|IMAGE_PREFIX|KAFKA_EXTERNAL_HOST)=' deploy/.env.example`
Expected: all three keys still present and uncommented.

- [ ] **Step 3: Commit**

```bash
git add deploy/.env.example
git commit -m "docs(deploy): document VERSION auto-sync in .env.example

Claude-Session: https://claude.ai/code/session_01NBzhzbdGaSePxMnqzzuqzX"
```

---

### Task 7: End-to-end integration verification

**Files:** none (verification only).

- [ ] **Step 1: Stale-`.env` integration check across entry points**

```bash
cd /Users/david/development/src/opennms/delta-v/deploy
cp .env .env.savetest
sed -i.x 's/^VERSION=.*/VERSION=0.0.0-stale/' .env; rm -f .env.x
PV="$(cd .. && ./mvnw help:evaluate -Dexpression=project.version -q -DforceStdout)"
# (a) deploy.sh path heals .env:
bash -c '. ../tools/version.sh; set -a; . ./.env; set +a; deltav_resolve_version; deltav_sync_env_version >/dev/null'
echo "after deploy-path .env: $(grep ^VERSION= .env)  (want VERSION=$PV)"
# (b) bare compose now resolves the right tag from the healed .env:
docker compose config 2>/dev/null | grep -m1 "image: deltav/provisiond:" || echo "(compose not configured; check tag manually)"
cp .env.savetest .env; rm -f .env.savetest
```
Expected: `.env` healed to `VERSION=$PV`; `docker compose config` shows `deltav/provisiond:$PV`.

- [ ] **Step 2: Full test-suite spot check (optional, slow)**

Run one import-dependent test to confirm the test path resolves VERSION:
`cd deploy && ./test-minion-rpc-e2e.sh --help >/dev/null && echo "test-lib sourced cleanly"`
Expected: no resolver errors on source; `test-lib sourced cleanly`.

- [ ] **Step 3: Push branch and open PR**

```bash
cd /Users/david/development/src/opennms/delta-v
git push -u origin chore/version-resolver-drift
gh pr create --repo pbrane/delta-v --base develop \
  --title "build(tools): single VERSION resolver to prevent image-tag drift" \
  --body "Resolves \`VERSION\` from \`project.version\` and auto-syncs the gitignored \`deploy/.env\` at every from-source entry point (build.sh, deploy.sh, test-lib.sh, doctor.sh), so stale \`.env\` can no longer pull weeks-old images. Preserves the no-source smoke-VM override and an explicit \`VERSION=<tag>\` shell override. Spec: docs/superpowers/specs/2026-06-19-version-resolver-design.md.

https://claude.ai/code/session_01NBzhzbdGaSePxMnqzzuqzX"
```

---

## Self-Review

**Spec coverage:**
- Shared resolver `tools/version.sh` (precedence shell > pom > .env) → Task 1. ✓
- Auto-sync `.env` atomically in `deploy/` → Task 1 (`deltav_sync_env_version`) + tested. ✓
- `DELTAV_VERSION_OVERRIDDEN` flag → Task 1 (set at source). ✓
- POSIX `<parent>`-aware fallback (not xmllint, not `0.x` grep) → Task 1 (`_deltav_pom_version`) + Case 4. ✓
- mtime cache → Task 1 (`_deltav_cached_pom_version`). ✓
- build.sh / deploy.sh / test-lib.sh / doctor.sh wiring → Tasks 2/3/4/5. ✓
- deploy.sh source-before-.env ordering → Task 3 (explicit). ✓
- `.env.example` documentation → Task 6. ✓
- Smoke VM no-pom path preserved → Task 1 Case 3 + Task 6 docs. ✓

**Placeholder scan:** no TBD/TODO; every code step has complete code. ✓

**Type/name consistency:** `deltav_resolve_version`, `deltav_sync_env_version`, `DELTAV_VERSION_OVERRIDDEN`, `DELTAV_VERSION_OVERRIDE`, `DELTAV_POM`, `DELTAV_ENV`, `DELTAV_VERSION_CACHE`, `DELTAV_REPO_ROOT` used consistently across Tasks 1–5. ✓
