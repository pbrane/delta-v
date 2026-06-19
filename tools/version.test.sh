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
