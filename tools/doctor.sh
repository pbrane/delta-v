#!/usr/bin/env bash
# doctor.sh — preflight checks for building & running delta-v.
# Exits non-zero if any required check fails. bash 3.2-safe.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DEPLOY_DIR="$REPO_ROOT/deploy"
fail=0
ok()   { printf '  \033[32m✓\033[0m %s\n' "$1"; }
bad()  { printf '  \033[31m✗\033[0m %s\n' "$1"; fail=1; }
warn() { printf '  \033[33m!\033[0m %s\n' "$1"; }

echo "== delta-v doctor =="

# 1. JDK 21
JH="${JAVA_HOME:-}"
if [ -z "$JH" ] && [ -d "/Library/Java/JavaVirtualMachines/temurin-21.jdk/Contents/Home" ]; then
    JH="/Library/Java/JavaVirtualMachines/temurin-21.jdk/Contents/Home"
fi
if [ -n "$JH" ] && [ -x "$JH/bin/java" ]; then
    jv=$("$JH/bin/java" -version 2>&1 | head -1 | sed 's/.*"\([0-9][0-9]*\).*/\1/')
    [ "$jv" = "21" ] && ok "JDK 21 ($JH)" || bad "JDK is $jv, need 21. Set JAVA_HOME to a JDK 21."
else
    bad "JDK 21 not found. Install Temurin 21 or set JAVA_HOME."
fi

# 2. Docker daemon
if command -v docker >/dev/null 2>&1; then
    if docker info >/dev/null 2>&1; then ok "Docker running"; else bad "Docker installed but daemon not running. Start Docker Desktop."; fi
else
    bad "docker not found. Install Docker Desktop."
fi

# 3. GitHub Packages auth
SETTINGS="$HOME/.m2/settings.xml"
if [ -f "$SETTINGS" ] && grep -q '<id>github-deltav-horizon</id>' "$SETTINGS"; then
    ok "Maven settings.xml has server id 'github-deltav-horizon'"
else
    bad "~/.m2/settings.xml missing <server> id 'github-deltav-horizon' (PAT with read:packages). See BUILD.md."
fi

# 4. .env present with a non-empty VERSION
ENV_FILE="$DEPLOY_DIR/.env"
V=""
if [ -f "$ENV_FILE" ]; then
    V=$(grep -m1 '^VERSION=' "$ENV_FILE" | cut -d= -f2 | tr -d '"' | tr -d "'" | tr -d ' \t\r')
    [ -n "$V" ] && ok ".env present (VERSION=$V)" || bad ".env present but VERSION is empty. Set VERSION in $ENV_FILE."
else
    bad "$ENV_FILE missing. Run: cp .env.example .env"
fi

# 5. Pre-up image completeness (only meaningful if VERSION known)
if [ -n "$V" ]; then
    P=$(grep -m1 '^IMAGE_PREFIX=' "$ENV_FILE" | cut -d= -f2 | tr -d '"' | tr -d "'" | tr -d ' \t\r'); P="${P:-deltav}"
    COMPOSE="$DEPLOY_DIR/docker-compose.yml"
    if [ ! -f "$COMPOSE" ]; then
        warn "docker-compose.yml not found at $COMPOSE; skipping image check"
    else
        missing=0
        for img in $(grep -oE '/[a-z0-9-]+:\$\{VERSION\}' "$COMPOSE" | sed 's#^/##; s#:.*##' | sort -u); do
            docker image inspect "$P/$img:$V" >/dev/null 2>&1 || { warn "image missing: $P/$img:$V"; missing=1; }
        done
        [ "$missing" = 0 ] && ok "all compose images present at :$V" || warn "run 'make images' to build the missing images above"
    fi
fi

echo ""
[ "$fail" = 0 ] && echo "doctor: OK" || { echo "doctor: FAILED — fix the ✗ items above"; exit 1; }
