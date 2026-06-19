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
        sed "s|^VERSION=.*|VERSION=$VERSION|" "$DELTAV_ENV" > "$tmp" || { rm -f "$tmp"; return 1; }
    else
        { cp "$DELTAV_ENV" "$tmp" && printf 'VERSION=%s\n' "$VERSION" >> "$tmp"; } || { rm -f "$tmp"; return 1; }
    fi
    mv "$tmp" "$DELTAV_ENV"
    echo "==> version.sh: synced deploy/.env VERSION ${current:-<unset>} -> $VERSION" >&2
}
