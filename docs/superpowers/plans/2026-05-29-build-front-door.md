# `make` Build/Run Front Door — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `make` the single, documented front door for building and running delta-v, with `build.sh`/`deploy.sh`/`compute-shared-libs.sh` as internal engines, CI calling the same `make` targets, and the build covering every image the compose file references.

**Architecture:** Approach A from the spec. `make` orchestrates; scripts own the "how". `build.sh` becomes the one image engine, parameterized (`PUSH`/`PLATFORMS`/`IMAGE_PREFIX`/`VERSION`) so local-native and CI-multi-arch-push are one code path, and grows the 8 auxiliary-image builds currently living only in CI YAML. `compute-shared-libs.sh` is rewritten free of bash-4 associative arrays. A `doctor` preflight catches setup failures before they surface as confusing Docker errors.

**Tech Stack:** GNU Make 3.81 (macOS floor), bash 3.2 (macOS floor), Docker Buildx, Maven wrapper (`./mvnw`), GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-05-29-build-front-door-design.md`

**Branch:** `feat/build-front-door` (already created off `develop`; spec already committed there).

**Conventions for every task below:**
- `DELTAV="opennms-container/delta-v"` — the container build dir (relative to repo root).
- "Run under bash 3.2" means `/bin/bash <script>` on macOS (the frozen 3.2.57). The portability tests assume macOS; on a Linux executor, substitute a `bash3` build or skip with a logged note.
- `shellcheck` is expected on PATH (`brew install shellcheck`). If absent, log it and continue.

---

## Task 1: Rewrite `compute-shared-libs.sh` to be bash 3.2-safe

Removes all four `declare -A` associative arrays. Daemon→path is derived (uniform pattern), fat-JAR paths are resolved by a function, and the shared-library intersection is computed with `sort | uniq -c` instead of a count map. Output contract is unchanged (`staging/shared-external`, `shared-internal`, `priority`, `<daemon>/libs`, `<daemon>/app`, `<daemon>/.main_class`).

**Files:**
- Modify (full rewrite): `opennms-container/delta-v/compute-shared-libs.sh`

- [ ] **Step 1: Capture the current output as a golden baseline (the test)**

If a prior build's staging exists you can diff against it later. Record the current shared/unique counts from a known-good run (maintainer machine, Homebrew bash 5):

```bash
cd opennms-container/delta-v
# Requires the 12 daemon-boot fat JARs already built (./build.sh deltav once).
/opt/homebrew/bin/bash ./compute-shared-libs.sh "$(cd ../.. && pwd)" "$(grep -m1 '^VERSION=' .env | cut -d= -f2)" \
  | tee /tmp/csl-baseline.txt
ls staging/shared-external | sort > /tmp/csl-shared-external.txt
ls staging/shared-internal | sort > /tmp/csl-shared-internal.txt
ls staging/priority        | sort > /tmp/csl-priority.txt
for d in alarmd bsmd collectd discovery enlinkd eventtranslator perspectivepollerd pollerd provisiond syslogd telemetryd trapd; do
  echo "== $d =="; ls "staging/$d/libs" | sort; cat "staging/$d/.main_class"
done > /tmp/csl-perdaemon.txt
```

- [ ] **Step 2: Confirm the bug under bash 3.2 (failing test)**

```bash
/bin/bash --version | head -1          # expect 3.2.x on macOS
/bin/bash ./compute-shared-libs.sh /tmp x
```
Expected: `declare: -A: invalid option` (or the arg-count usage if it exits at line 18 first — if so, call with two args as in Step 1 to reach line 30 and observe the crash).

- [ ] **Step 3: Replace the whole script with the bash 3.2-safe version**

Write `opennms-container/delta-v/compute-shared-libs.sh` with exactly this content:

```bash
#!/usr/bin/env bash
# compute-shared-libs.sh — Extract Spring Boot fat JARs and deduplicate shared dependencies.
#
# Produces a staging/ directory with:
#   shared-external/   — 3rd-party JARs present in ALL 12 daemons
#   shared-internal/   — org.opennms project JARs present in ALL 12 daemons
#   priority/          — model-jakarta + dao-jpa-support (classpath-first)
#   <daemon>/libs/     — JARs unique to this daemon
#   <daemon>/app/      — thin application JAR (classes + resources only)
#   <daemon>/.main_class — fully-qualified Start-Class from MANIFEST.MF
#
# Portability: written for bash 3.2 (the frozen macOS /bin/bash). No
# associative arrays (`declare -A`), no `${var^^}`, no `mapfile`.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

if [ $# -ne 2 ]; then
    echo "Usage: $0 <repo-root> <version>"
    echo "  e.g. $0 /path/to/delta-v 36.0.0-SNAPSHOT"
    exit 1
fi

REPO_ROOT="$1"
VERSION="$2"

# The 12 horizon-derived daemons, sorted, space-separated (indexed iteration only).
DAEMONS="alarmd bsmd collectd discovery enlinkd eventtranslator perspectivepollerd pollerd provisiond syslogd telemetryd trapd"
DAEMON_COUNT=$(printf '%s\n' $DAEMONS | wc -l | tr -d ' ')

# Derive the fat-JAR path base for a daemon (uniform module layout).
jar_base() {
    echo "${REPO_ROOT}/core/daemon-boot-$1/target/org.opennms.core.daemon-boot-$1-${VERSION}"
}

# Echo the resolved fat JAR (prefer plain .jar, fall back to -boot.jar). Empty if none.
resolve_fat_jar() {
    local base; base="$(jar_base "$1")"
    if [ -f "${base}.jar" ]; then
        echo "${base}.jar"
    elif [ -f "${base}-boot.jar" ]; then
        echo "${base}-boot.jar"
    else
        echo ""
    fi
}

echo "=== compute-shared-libs.sh ==="
echo "Repo root : ${REPO_ROOT}"
echo "Version   : ${VERSION}"
echo "Daemons   : ${DAEMON_COUNT}"
echo ""

# Verify all fat JARs exist up front.
missing=0
for daemon in $DAEMONS; do
    if [ -z "$(resolve_fat_jar "$daemon")" ]; then
        echo "ERROR: No fat JAR found for ${daemon}"
        echo "  Tried: $(jar_base "$daemon").jar"
        echo "  Tried: $(jar_base "$daemon")-boot.jar"
        missing=1
    fi
done
if [ "$missing" -ne 0 ]; then
    echo "Aborting — missing fat JARs."
    exit 1
fi

STAGING="${SCRIPT_DIR}/staging"
EXTRACT_DIR="${SCRIPT_DIR}/.extract-tmp"
rm -rf "${STAGING}" "${EXTRACT_DIR}"
mkdir -p "${STAGING}/shared-external" "${STAGING}/shared-internal" "${STAGING}/priority"
mkdir -p "${EXTRACT_DIR}"

# --- Step 1: Extract all fat JARs ---
echo "--- Extracting fat JARs ---"
for daemon in $DAEMONS; do
    jar="$(resolve_fat_jar "$daemon")"
    dest="${EXTRACT_DIR}/${daemon}"
    echo "  ${daemon}: $(basename "${jar}")"
    if ! java -Djarmode=tools -jar "${jar}" extract --destination "${dest}" 2>/dev/null; then
        echo "    jarmode extraction failed, falling back to manual extract..."
        mkdir -p "${dest}/lib"
        tmpdir=$(mktemp -d)
        ( cd "$tmpdir" && jar xf "${jar}" )
        mv "$tmpdir"/BOOT-INF/lib/* "${dest}/lib/" 2>/dev/null || true
        ( cd "$tmpdir/BOOT-INF/classes" && jar cf "${dest}/$(basename "${jar}")" . )
        cp -r "$tmpdir/META-INF" "${dest}/META-INF" 2>/dev/null || true
        rm -rf "$tmpdir"
    fi
done
echo ""

# --- Step 2: Extract Start-Class from each MANIFEST.MF ---
echo "--- Extracting Start-Class ---"
for daemon in $DAEMONS; do
    jar="$(resolve_fat_jar "$daemon")"
    main_class=$(unzip -p "${jar}" META-INF/MANIFEST.MF \
        | tr -d '\r' \
        | sed -e ':a' -e 'N' -e '$!ba' -e 's/\n //g' \
        | grep "^Start-Class:" \
        | sed 's/^Start-Class: *//')
    if [ -z "${main_class}" ]; then
        echo "ERROR: No Start-Class found in ${jar}"
        exit 1
    fi
    mkdir -p "${STAGING}/${daemon}"
    echo "${main_class}" > "${STAGING}/${daemon}/.main_class"
    echo "  ${daemon}: ${main_class}"
done
echo ""

# --- Step 3: Compute strict intersection of lib/ dirs via sort|uniq -c ---
echo "--- Computing shared libraries (strict intersection of all ${DAEMON_COUNT} daemons) ---"
ALL_NAMES="${EXTRACT_DIR}/.all-names.txt"   # one jar basename per daemon-occurrence
SHARED_FILE="${EXTRACT_DIR}/.shared.txt"    # names present in all DAEMON_COUNT daemons
: > "${ALL_NAMES}"
for daemon in $DAEMONS; do
    lib_dir="${EXTRACT_DIR}/${daemon}/lib"
    if [ ! -d "${lib_dir}" ]; then
        echo "ERROR: No lib/ directory found for ${daemon} at ${lib_dir}"
        exit 1
    fi
    # De-dupe within a single daemon (a name counts once per daemon), then append.
    for jar_file in "${lib_dir}"/*.jar; do
        basename "${jar_file}"
    done | sort -u >> "${ALL_NAMES}"
done
total_unique=$(sort -u "${ALL_NAMES}" | wc -l | tr -d ' ')
# A name shared by all daemons appears exactly DAEMON_COUNT times in ALL_NAMES.
sort "${ALL_NAMES}" | uniq -c \
    | awk -v n="${DAEMON_COUNT}" '$1 == n { $1=""; sub(/^ /,""); print }' \
    | sort > "${SHARED_FILE}"
shared_total=$(wc -l < "${SHARED_FILE}" | tr -d ' ')
echo "  Total unique JAR filenames across all daemons: ${total_unique}"
echo "  Shared across all ${DAEMON_COUNT} daemons: ${shared_total}"
echo ""

# Membership helper: is this jar name shared?
is_shared() { grep -Fxq "$1" "${SHARED_FILE}"; }

# --- Step 4: Partition shared libs into external vs internal ---
echo "--- Partitioning shared libs into external / internal ---"
first_daemon=$(printf '%s\n' $DAEMONS | head -1)
first_lib="${EXTRACT_DIR}/${first_daemon}/lib"
shared_external_count=0
shared_internal_count=0
while IFS= read -r name; do
    [ -n "$name" ] || continue
    case "$name" in
        org.opennms.*|opennms-*)
            cp "${first_lib}/${name}" "${STAGING}/shared-internal/"
            shared_internal_count=$(( shared_internal_count + 1 )) ;;
        *)
            cp "${first_lib}/${name}" "${STAGING}/shared-external/"
            shared_external_count=$(( shared_external_count + 1 )) ;;
    esac
done < "${SHARED_FILE}"
echo "  shared-external: ${shared_external_count} JARs"
echo "  shared-internal: ${shared_internal_count} JARs"
echo ""

# --- Step 4b: Move model-jakarta + dao-jpa-support to priority classpath ---
echo "--- Moving model-jakarta to priority classpath ---"
priority_count=0
for jar_file in "${STAGING}/shared-internal"/org.opennms.core.model-jakarta-*.jar \
                "${STAGING}/shared-internal"/org.opennms.core.dao-jpa-support-*.jar; do
    if [ -f "${jar_file}" ]; then
        mv "${jar_file}" "${STAGING}/priority/"
        echo "  moved: $(basename "${jar_file}")"
        priority_count=$(( priority_count + 1 ))
    fi
done
echo "  priority: ${priority_count} JARs"
echo ""

# --- Step 5: Stage per-daemon unique libs and thin app JAR ---
echo "--- Staging per-daemon unique libs and app JARs ---"
for daemon in $DAEMONS; do
    lib_dir="${EXTRACT_DIR}/${daemon}/lib"
    daemon_libs_dir="${STAGING}/${daemon}/libs"
    daemon_app_dir="${STAGING}/${daemon}/app"
    mkdir -p "${daemon_libs_dir}" "${daemon_app_dir}"
    unique_count=0
    for jar_file in "${lib_dir}"/*.jar; do
        name="$(basename "${jar_file}")"
        if ! is_shared "${name}"; then
            cp "${jar_file}" "${daemon_libs_dir}/"
            unique_count=$(( unique_count + 1 ))
        fi
    done
    thin_jar_count=0
    for f in "${EXTRACT_DIR}/${daemon}"/*.jar; do
        cp "${f}" "${daemon_app_dir}/"
        thin_jar_count=$(( thin_jar_count + 1 ))
    done
    echo "  ${daemon}: ${unique_count} unique libs, ${thin_jar_count} app JAR(s)"
done
echo ""

# --- Step 6: Safety check — no JAR in both shared and per-daemon ---
echo "--- Safety check: no overlap between shared and per-daemon ---"
overlap_found=0
for daemon in $DAEMONS; do
    for jar_file in "${STAGING}/${daemon}/libs"/*.jar; do
        [ -e "${jar_file}" ] || continue
        name="$(basename "${jar_file}")"
        if is_shared "${name}"; then
            echo "  OVERLAP: ${name} in both shared and ${daemon}/libs/"
            overlap_found=1
        fi
    done
done
if [ "${overlap_found}" -eq 1 ]; then
    echo "ERROR: Overlap detected — aborting."
    rm -rf "${EXTRACT_DIR}"
    exit 1
fi
echo "  OK — no overlaps."
echo ""

rm -rf "${EXTRACT_DIR}"

echo "==========================================="
echo "  SUMMARY"
echo "==========================================="
echo "  priority        : ${priority_count} JARs (model-jakarta, loaded first)"
echo "  shared-external : ${shared_external_count} JARs"
echo "  shared-internal : $(( shared_internal_count - priority_count )) JARs"
echo "  shared total    : ${shared_total} JARs"
echo ""
printf "  %-22s %s\n" "DAEMON" "UNIQUE LIBS"
printf "  %-22s %s\n" "------" "-----------"
for daemon in $DAEMONS; do
    count=$(ls -1 "${STAGING}/${daemon}/libs/"*.jar 2>/dev/null | wc -l | tr -d ' ')
    main_class=$(cat "${STAGING}/${daemon}/.main_class")
    printf "  %-22s %3s   %s\n" "${daemon}" "${count}" "${main_class}"
done
echo ""
echo "Staging directory: ${STAGING}"
echo "Done."
```

- [ ] **Step 4: Verify it parses and runs under bash 3.2 (test passes)**

```bash
cd opennms-container/delta-v
/bin/bash -n compute-shared-libs.sh                       # syntax OK under 3.2
shellcheck compute-shared-libs.sh || true                 # no errors (warnings OK)
/bin/bash ./compute-shared-libs.sh /tmp x ; echo "exit=$?"  # expect the "No fat JAR" abort, NOT "declare: -A"
```
Expected: no `declare: -A` anywhere; the run aborts cleanly with `ERROR: No fat JAR found for alarmd` (because `/tmp` has no JARs) and a non-`declare` message.

- [ ] **Step 5: Verify output parity against the golden baseline**

With the 12 fat JARs present, run the rewritten script under bash 3.2 and diff its staging against the baseline from Step 1:

```bash
cd opennms-container/delta-v
/bin/bash ./compute-shared-libs.sh "$(cd ../.. && pwd)" "$(grep -m1 '^VERSION=' .env | cut -d= -f2)" | tee /tmp/csl-new.txt
ls staging/shared-external | sort | diff - /tmp/csl-shared-external.txt && echo "shared-external OK"
ls staging/shared-internal | sort | diff - /tmp/csl-shared-internal.txt && echo "shared-internal OK"
ls staging/priority        | sort | diff - /tmp/csl-priority.txt        && echo "priority OK"
```
Expected: all three diffs empty (identical partition to the bash-5 baseline). If the diffs are empty, the rewrite is behavior-preserving.

- [ ] **Step 6: Commit**

```bash
git add opennms-container/delta-v/compute-shared-libs.sh
git commit -m "fix(build): make compute-shared-libs.sh bash 3.2-safe

Remove all four declare -A associative arrays (DAEMON_JAR_BASE, FAT_JAR,
JAR_COUNT, SHARED_SET) so the script runs on the frozen macOS /bin/bash
3.2.57. Daemon->path is derived from the uniform module layout, fat JARs
are resolved by a function, and the shared-library intersection is
computed with sort|uniq -c. Output contract unchanged."
```

---

## Task 2: Parameterize `build.sh` (IMAGE_PREFIX + buildx helper), no new images yet

Introduce a single `build_image()` helper and an `IMAGE_PREFIX` so the engine serves both local-native (`--load`) and CI-multi-arch (`--push`) from one code path. Replace the hardcoded `deltav/` tags. Default behavior (local, `deltav`, native arch, no push) is unchanged.

**Files:**
- Modify: `opennms-container/delta-v/build.sh`

- [ ] **Step 1: Add the engine parameters and helper (after line 34, the `err()` definition)**

Replace the block at lines 23–34 (from `SKIP_TESTS=...` through the `err()` definition) with:

```bash
SKIP_TESTS="${SKIP_TESTS:-true}"
DOCKER_REGISTRY="${DOCKER_REGISTRY:-docker.io}"
DOCKER_ORG="${DOCKER_ORG:-deltav}"

# Image prefix used for every tag. Local default is the bare org ("deltav");
# CI overrides via IMAGE_PREFIX=ghcr.io/pbrane. Explicit IMAGE_PREFIX wins.
IMAGE_PREFIX="${IMAGE_PREFIX:-$DOCKER_ORG}"

# Push vs local-load, and target platforms. Local default: no push, host arch.
# CI sets PUSH=true and PLATFORMS=linux/amd64,linux/arm64.
PUSH="${PUSH:-false}"
PLATFORMS="${PLATFORMS:-}"

# The 12 horizon-derived Spring Boot daemons that share daemon-base.
DAEMON_NAMES="alarmd bsmd collectd discovery enlinkd eventtranslator perspectivepollerd pollerd provisiond syslogd telemetryd trapd"

VERSION="$(cd "$REPO_ROOT" && ./mvnw help:evaluate -Dexpression=project.version -q -DforceStdout 2>/dev/null || grep '<version>0\.' "$REPO_ROOT/pom.xml" | head -1 | sed 's/.*<version>\(.*\)<\/version>.*/\1/')"

log() { echo "==> $*"; }
err() { echo "ERROR: $*" >&2; exit 1; }

# build_image SHORT_NAME -f DOCKERFILE [docker-build-args...] CONTEXT
# Tags ${IMAGE_PREFIX}/SHORT_NAME at :$VERSION and :latest. Uses buildx;
# --load for local single-arch, --platform/--push when PUSH=true.
build_image() {
    local short="$1"; shift
    local img="${IMAGE_PREFIX}/${short}"
    local -a args
    args=(buildx build -t "${img}:${VERSION}" -t "${img}:latest")
    if [ "$PUSH" = "true" ]; then
        [ -n "$PLATFORMS" ] && args+=(--platform "$PLATFORMS")
        args+=(--push)
    else
        args+=(--load)
    fi
    args+=("$@")
    log "  building ${img}:${VERSION} (push=${PUSH}${PLATFORMS:+ platforms=$PLATFORMS})"
    docker "${args[@]}"
    apply_env_version_alias "${img}"
}
```

Note: `build_image` uses an indexed array `args=()` which IS valid in bash 3.2 (only `declare -A` is not). `build.sh` runs under whatever `bash` the shell picks; after Task 1 the only hard 3.2 dependency was the helper script — keep `build.sh` 3.2-safe too by avoiding `declare -A` (this helper does).

- [ ] **Step 2: Route every image build through the helper**

Replace each hardcoded `docker build ... -t "deltav/X:$VERSION" -t "deltav/X:latest" ...` plus its trailing `apply_env_version_alias "deltav/X"` with a single `build_image` call. Apply to these functions (current line ranges in parens):

```bash
# do_db_init_image (159-160):
build_image db-init -f "$REPO_ROOT/core/db-init/Dockerfile" "$REPO_ROOT/core/db-init"

# do_minion_gateway_image (170-175):
build_image minion-gateway \
    -f "$SCRIPT_DIR/minion-gateway/Dockerfile" \
    --build-arg "JRE_BASE=${IMAGE_PREFIX}/jre-deltav:21" \
    "$REPO_ROOT/core/minion-gateway/"

# do_envoy_image (183-187):
build_image envoy -f "$SCRIPT_DIR/envoy/Dockerfile" "$SCRIPT_DIR/envoy"

# do_perspective_app_init_image (196-201):
build_image perspective-app-init -f "$SCRIPT_DIR/Dockerfile.perspective-app-init" "$SCRIPT_DIR"

# do_flow_enricher_image (209-210):
build_image flow-enricher -f "$REPO_ROOT/core/flow-enricher/Dockerfile" "$REPO_ROOT/core/flow-enricher"

# do_prometheus_writer_image (218-219):
build_image prometheus-writer -f "$REPO_ROOT/core/prometheus-writer/Dockerfile" "$REPO_ROOT/core/prometheus-writer"

# do_alerts_forwarder_image (227-228):
build_image alerts-forwarder -f "$REPO_ROOT/core/alerts-forwarder/Dockerfile" "$REPO_ROOT/core/alerts-forwarder"
```

For the daemon-base build (lines 269-274) replace with:

```bash
build_image daemon-base --no-cache \
    -f Dockerfile.daemon-base \
    --build-arg "JRE_IMAGE=${IMAGE_PREFIX}/jre-deltav:21" \
    .
```

For the per-daemon loop (lines 281-289) replace the inner `docker build ... ; apply_env_version_alias` with:

```bash
build_image "$name" \
    -f Dockerfile.daemon-per \
    --build-arg "VERSION=$VERSION" \
    --build-arg "DAEMON_BASE_IMAGE=${IMAGE_PREFIX}/daemon-base" \
    --build-arg "DAEMON_NAME=$name" \
    --build-arg "MAIN_CLASS=$main_class" \
    .
```

For minion-boot (lines 303-309) replace with:

```bash
build_image minion-boot \
    --build-arg "VERSION=$VERSION" \
    --build-arg "JRE_IMAGE=${IMAGE_PREFIX}/jre-deltav:21" \
    -f Dockerfile.minion-boot \
    .
```

For the JRE image `do_jre_image` (234-237) replace with:

```bash
build_image jre-deltav -f Dockerfile.jre .
```
…and change its two info lines that grep `deltav/jre-deltav` to use `${IMAGE_PREFIX}/jre-deltav`. (Note: the JRE tag is `:21`/`:latest`, not `:$VERSION`. Keep a dedicated build for it — see Step 3.)

- [ ] **Step 3: Handle the JRE image's non-version tag**

`build_image` tags `:$VERSION` + `:latest`, but the JRE base is referenced as `:21`. Add a tiny variant rather than overloading the helper. Replace `do_jre_image` body with:

```bash
do_jre_image() {
    log "Building ${IMAGE_PREFIX}/jre-deltav:21..."
    cd "$SCRIPT_DIR"
    local -a args
    args=(buildx build -f Dockerfile.jre -t "${IMAGE_PREFIX}/jre-deltav:21" -t "${IMAGE_PREFIX}/jre-deltav:latest")
    if [ "$PUSH" = "true" ]; then
        [ -n "$PLATFORMS" ] && args+=(--platform "$PLATFORMS")
        args+=(--push)
    else
        args+=(--load)
    fi
    args+=(.)
    docker "${args[@]}"
    log "JRE image built:"
    docker images "${IMAGE_PREFIX}/jre-deltav" --format "  {{.Repository}}:{{.Tag}}\t{{.Size}}" 2>/dev/null || true
}
```

Also update the two `docker image inspect deltav/jre-deltav:21` guards (lines 251, 397, 482, 510) and the `deltav/daemon-base:$VERSION` guard (lines 395-396) to use `${IMAGE_PREFIX}/...`.

- [ ] **Step 4: Verify parse + local build of one cheap image**

```bash
cd opennms-container/delta-v
bash -n build.sh && echo "parse OK"
shellcheck build.sh || true
docker buildx version >/dev/null && echo "buildx OK"
# Prove IMAGE_PREFIX precedence (DOCKER_ORG default vs explicit override):
DOCKER_ORG=deltav      bash -c 'IMAGE_PREFIX="${IMAGE_PREFIX:-$DOCKER_ORG}"; echo "$IMAGE_PREFIX"'   # expect deltav
IMAGE_PREFIX=ghcr.io/pbrane bash -c 'IMAGE_PREFIX="${IMAGE_PREFIX:-$DOCKER_ORG}"; echo "$IMAGE_PREFIX"'  # expect ghcr.io/pbrane
```
Expected: `parse OK`, `buildx OK`, and the prefix resolves to `deltav` by default and to the explicit override when set. (A full `./build.sh deltav` is exercised in Task 3 Step 4.)

- [ ] **Step 5: Commit**

```bash
git add opennms-container/delta-v/build.sh
git commit -m "refactor(build): parameterize build.sh via IMAGE_PREFIX/PUSH/PLATFORMS

Introduce a single build_image() helper using docker buildx, so local
native (--load) and CI multi-arch (--push) are one code path. Replace the
~15 hardcoded deltav/ tags with \${IMAGE_PREFIX}. No default behavior change."
```

---

## Task 3: Add the 8 missing auxiliary-image builds to `build.sh`

Transcribe the auxiliary builds that currently live only in CI YAML (lines 295–405) into `build.sh` functions and wire them into `do_deltav_images`, so a single `make images` produces every image `docker-compose.yml` references. This closes the `pull access denied` failure mode.

**Files:**
- Modify: `opennms-container/delta-v/build.sh`

- [ ] **Step 1: Confirm the gap (failing test)**

```bash
cd opennms-container/delta-v
for n in clickhouse clickhouse-init grafana provisiond-imports-init l8opensim-provisioner mock-snmp-agent flow-exporter sflow-exporter; do
  grep -q "build_image ${n}\b" build.sh && echo "have $n" || echo "MISSING $n"
done
```
Expected: all 8 print `MISSING`.

- [ ] **Step 2: Add the 8 functions (after `do_alerts_forwarder_image`, ~line 229)**

Dockerfile locations confirmed from the repo: `Dockerfile.clickhouse`, `Dockerfile.clickhouse-init`, `Dockerfile.grafana`, `Dockerfile.provisiond-imports-init`, `Dockerfile.l8opensim-provisioner` live in `$SCRIPT_DIR`; `mock-snmp-agent/Dockerfile`, `flow-exporter/Dockerfile`, `sflow-exporter/Dockerfile` live in their subdirs.

```bash
do_clickhouse_image() {
    log "Building clickhouse image..."
    cd "$SCRIPT_DIR"
    build_image clickhouse -f Dockerfile.clickhouse .
}
do_clickhouse_init_image() {
    log "Building clickhouse-init image..."
    cd "$SCRIPT_DIR"
    build_image clickhouse-init -f Dockerfile.clickhouse-init .
}
do_grafana_image() {
    log "Building grafana image..."
    cd "$SCRIPT_DIR"
    build_image grafana -f Dockerfile.grafana .
}
do_provisiond_imports_init_image() {
    log "Building provisiond-imports-init image..."
    cd "$SCRIPT_DIR"
    build_image provisiond-imports-init -f Dockerfile.provisiond-imports-init .
}
do_l8opensim_provisioner_image() {
    log "Building l8opensim-provisioner image..."
    cd "$SCRIPT_DIR"
    build_image l8opensim-provisioner -f Dockerfile.l8opensim-provisioner .
}
do_mock_snmp_agent_image() {
    log "Building mock-snmp-agent image..."
    build_image mock-snmp-agent -f "$SCRIPT_DIR/mock-snmp-agent/Dockerfile" "$SCRIPT_DIR/mock-snmp-agent"
}
do_flow_exporter_image() {
    log "Building flow-exporter image..."
    build_image flow-exporter -f "$SCRIPT_DIR/flow-exporter/Dockerfile" "$SCRIPT_DIR/flow-exporter"
}
do_sflow_exporter_image() {
    log "Building sflow-exporter image..."
    build_image sflow-exporter -f "$SCRIPT_DIR/sflow-exporter/Dockerfile" "$SCRIPT_DIR/sflow-exporter"
}
```

- [ ] **Step 3: Wire them into `do_deltav_images` (after `do_perspective_app_init_image`, before the summary at ~line 361)**

```bash
    # --- Auxiliary images (no Maven; previously only built in CI) ---
    # These complete the set docker-compose.yml references so a single
    # `make images` yields a deployable stack with no missing-image pulls.
    do_clickhouse_image
    do_clickhouse_init_image
    do_grafana_image
    do_provisiond_imports_init_image
    do_l8opensim_provisioner_image
    do_mock_snmp_agent_image
    do_flow_exporter_image
    do_sflow_exporter_image
```

- [ ] **Step 4: Verify a full local build produces every compose-referenced image**

```bash
cd opennms-container/delta-v
bash -n build.sh && echo "parse OK"
./build.sh deltav            # full local build (native arch, --load)
# Assert every compose image exists locally at the .env VERSION:
V=$(grep -m1 '^VERSION=' .env | cut -d= -f2)
P=$(grep -m1 '^IMAGE_PREFIX=' .env | cut -d= -f2); P=${P:-deltav}
miss=0
for img in $(grep -oE "\\$\{IMAGE_PREFIX:-deltav\}/[a-z0-9-]+" docker-compose.yml | sed 's/.*}\///' | sort -u); do
  docker image inspect "$P/$img:$V" >/dev/null 2>&1 || { echo "MISSING $P/$img:$V"; miss=1; }
done
[ "$miss" = 0 ] && echo "ALL compose images present"
```
Expected: `ALL compose images present` (no `MISSING` lines). This is the regression test for the `pull access denied` bug.

- [ ] **Step 5: Commit**

```bash
git add opennms-container/delta-v/build.sh
git commit -m "feat(build): build auxiliary images in build.sh (close compose gap)

Add clickhouse, clickhouse-init, grafana, provisiond-imports-init,
l8opensim-provisioner, mock-snmp-agent, flow-exporter, sflow-exporter —
previously built only in CI YAML. A single build now produces every image
docker-compose.yml references, fixing 'pull access denied' on fresh clones."
```

---

## Task 4: Add `doctor.sh` preflight

**Files:**
- Create: `opennms-container/delta-v/doctor.sh`

- [ ] **Step 1: Write the failing test (run before the script exists)**

```bash
cd opennms-container/delta-v
./doctor.sh; echo "exit=$?"
```
Expected: `No such file or directory`.

- [ ] **Step 2: Create `doctor.sh` (bash 3.2-safe)**

```bash
#!/usr/bin/env bash
# doctor.sh — preflight checks for building & running delta-v.
# Exits non-zero if any required check fails. bash 3.2-safe.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
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
    jv=$("$JH/bin/java" -version 2>&1 | head -1 | sed 's/.*"\([0-9]*\)\..*/\1/')
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
if [ -f "$SETTINGS" ] && grep -q "github-deltav-horizon" "$SETTINGS"; then
    ok "Maven settings.xml has server id 'github-deltav-horizon'"
else
    bad "~/.m2/settings.xml missing <server> id 'github-deltav-horizon' (PAT with read:packages). See BUILD.md."
fi

# 4. .env present with a non-empty VERSION
ENV_FILE="$SCRIPT_DIR/.env"
V=""
if [ -f "$ENV_FILE" ]; then
    V=$(grep -m1 '^VERSION=' "$ENV_FILE" | cut -d= -f2 | tr -d '"' | tr -d "'")
    [ -n "$V" ] && ok ".env present (VERSION=$V)" || bad ".env present but VERSION is empty. Set VERSION in $ENV_FILE."
else
    bad "$ENV_FILE missing. Run: cp .env.example .env"
fi

# 5. Pre-up image completeness (only meaningful if VERSION known)
if [ -n "$V" ]; then
    P=$(grep -m1 '^IMAGE_PREFIX=' "$ENV_FILE" | cut -d= -f2); P="${P:-deltav}"
    missing=0
    for img in $(grep -oE '/[a-z0-9-]+:\$\{VERSION\}' "$SCRIPT_DIR/docker-compose.yml" | sed 's#^/##; s#:.*##' | sort -u); do
        docker image inspect "$P/$img:$V" >/dev/null 2>&1 || { warn "image missing: $P/$img:$V"; missing=1; }
    done
    [ "$missing" = 0 ] && ok "all compose images present at :$V" || warn "run 'make images' to build the missing images above"
fi

echo ""
[ "$fail" = 0 ] && echo "doctor: OK" || { echo "doctor: FAILED — fix the ✗ items above"; exit 1; }
```

- [ ] **Step 3: Make executable and verify**

```bash
cd opennms-container/delta-v
chmod +x doctor.sh
/bin/bash -n doctor.sh && echo "parse OK under 3.2"
shellcheck doctor.sh || true
./doctor.sh; echo "exit=$?"      # exit 0 on a configured maintainer box; non-zero with clear ✗ on a fresh one
```
Expected: parse OK; prints ✓/✗ lines; exit code reflects required-check failures.

- [ ] **Step 4: Commit**

```bash
git add opennms-container/delta-v/doctor.sh
git commit -m "feat(build): add doctor.sh preflight (JDK21, Docker, GH Packages auth, images)"
```

---

## Task 5: Commit `.env.example`; guard empty VERSION in `deploy.sh`

**Files:**
- Create: `opennms-container/delta-v/.env.example`
- Modify: `opennms-container/delta-v/deploy.sh` (`do_up`, ~line 31)
- Maybe modify: `.gitignore` (ensure `.env.example` is tracked)

- [ ] **Step 1: Confirm `.env.example` is not ignored (test)**

```bash
cd opennms-container/delta-v
git check-ignore -v .env.example && echo "IGNORED — must fix .gitignore" || echo "trackable"
```
If `IGNORED`, add a negation to the nearest `.gitignore`: `!opennms-container/delta-v/.env.example` (placed after the `.env` rule).

- [ ] **Step 2: Create `.env.example`**

```bash
# Copy to .env and adjust. .env itself is gitignored (local overrides).
# VERSION must match an image tag you have built locally (`make images`)
# or one published to the registry in IMAGE_PREFIX.
VERSION=1.3.0-rc2
# Local builds: deltav. To pull published images instead: ghcr.io/pbrane
IMAGE_PREFIX=deltav
```
(Keep `VERSION` synced to the current `project.version` on each bump — existing discipline.)

- [ ] **Step 3: Guard empty VERSION in `deploy.sh do_up`**

After the `.env` sourcing block (the `IMAGE_PREFIX="${IMAGE_PREFIX:-deltav}"` line, ~31), add:

```bash
if [ -z "${VERSION:-}" ]; then
    err "VERSION is empty (no .env, or VERSION unset). Run: cp .env.example .env  (docker compose would otherwise default to ':latest' and pull nonexistent images)."
fi
```

- [ ] **Step 4: Verify the guard fires**

```bash
cd opennms-container/delta-v
env -i PATH="$PATH" bash ./deploy.sh up 2>&1 | head -3   # no .env in clean env → expect the VERSION error
git check-ignore .env.example || echo "tracked OK"
```
Expected: clean-env run prints the `VERSION is empty` remedy instead of attempting a `:latest` pull.

- [ ] **Step 5: Commit**

```bash
git add opennms-container/delta-v/.env.example opennms-container/delta-v/deploy.sh .gitignore 2>/dev/null
git commit -m "feat(build): ship .env.example and fail fast on empty VERSION

Stops the empty-tag->':latest' fallback that caused 'pull access denied'
on fresh clones (.env is gitignored, so a clone has no VERSION)."
```

---

## Task 6: Extend the root `Makefile` into the full front door

**Files:**
- Modify: `Makefile` (root)

- [ ] **Step 1: Add the lifecycle targets**

Append after the existing `clean:` target. Keep recipes thin (delegate to engines). `DELTAV` points at the container dir.

```makefile
DELTAV := opennms-container/delta-v

.PHONY: images daemon-image up down reset status logs verify dev doctor

images: ## Build ALL Docker images (daemons + auxiliaries)
	cd $(DELTAV) && ./build.sh deltav

daemon-image: ## Build one daemon image (DAEMON=); reuses cached base
	@test -n "$(DAEMON)" || (echo "ERROR: DAEMON required, e.g. make daemon-image DAEMON=alarmd" && exit 1)
	cd $(DELTAV) && ./build.sh daemon $(DAEMON)

up: ## Start the stack (PROFILE=lite|passive|full)
	cd $(DELTAV) && ./deploy.sh up $(PROFILE)

down: ## Stop the stack (preserve data)
	cd $(DELTAV) && ./deploy.sh down

reset: ## Stop and remove all data volumes
	cd $(DELTAV) && ./deploy.sh reset

status: ## Show service status
	cd $(DELTAV) && ./deploy.sh status

logs: ## Tail logs (SVC=<service>)
	cd $(DELTAV) && ./deploy.sh logs $(SVC)

verify: ## Run deploy health checks
	cd $(DELTAV) && ./deploy.sh test

dev: images up ## Build all images then bring the stack up

doctor: ## Preflight: verify the environment can build & run
	cd $(DELTAV) && ./doctor.sh
```

Add the new variables to the `help` "Variables" footer (after the `MAVEN_FLAGS` line):

```makefile
	@echo "  PROFILE          Compose profile for 'up' (lite|passive|full)    (current: $(PROFILE))"
	@echo "  SVC              Service name for 'logs'                          (current: $(SVC))"
	@echo "  DAEMON           Daemon for 'daemon-image'                        (current: $(DAEMON))"
```

Add `PROFILE ?=` and `SVC ?=` near the other `?=` defaults at the top.

- [ ] **Step 2: Verify targets list and delegate correctly (test)**

```bash
make help | grep -E "images|up|down|status|logs|verify|dev|doctor"   # all listed
make -n images        # expect: cd opennms-container/delta-v && ./build.sh deltav
make -n up PROFILE=full   # expect: ... ./deploy.sh up full
make -n daemon-image DAEMON=alarmd  # expect: ... ./build.sh daemon alarmd
make doctor           # runs the preflight
```
Expected: `make -n` prints the delegated command without executing; `make help` shows every target with its `##` description.

- [ ] **Step 3: Commit**

```bash
git add Makefile
git commit -m "feat(build): make is the single front door (build + run lifecycle)

Add images/daemon-image/up/down/reset/status/logs/verify/dev/doctor
targets delegating to build.sh, deploy.sh, doctor.sh. Recipes stay thin."
```

---

## Task 7: Cut CI over to `make`

Replace the inline image steps in the workflow with `make` targets so CI and local share one definition. Keep checkout/JDK/settings.xml/GHCR-login/version steps.

**Files:**
- Modify: `.github/workflows/delta-v-build-images.yml`

- [ ] **Step 1: Replace the build + all per-image steps**

Delete the steps from "Compile all modules" (line 138) through "Build and push deltav/sflow-exporter image" (line 405) and replace with:

```yaml
      - name: Build all modules
        run: make build

      - name: Build and push all images
        env:
          IMAGE_PREFIX: ${{ env.IMAGE_PREFIX }}
          PLATFORMS: ${{ env.PLATFORMS }}
          VERSION: ${{ steps.version.outputs.version }}
        run: make images PUSH=true
```

Note: `make images` → `build.sh deltav`, which now reads `IMAGE_PREFIX`, `PLATFORMS`, `PUSH`, and resolves `VERSION` itself (the `VERSION` env export is belt-and-suspenders; `build.sh` derives it from the POM identically to `steps.version`). Keep the "Print image summary" step.

- [ ] **Step 2: Validate the workflow**

```bash
# actionlint if available; else a YAML parse:
actionlint .github/workflows/delta-v-build-images.yml || python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/delta-v-build-images.yml')); print('YAML OK')"
```
Expected: no errors / `YAML OK`.

- [ ] **Step 3: Validate end-to-end against a throwaway tag (test)**

Push the branch and trigger the workflow against a disposable tag (never a release tag). Confirm a green run that pushes all images to GHCR. Per repo rules, do NOT open a PR against any `OpenNMS/*` repo; use `--repo pbrane/delta-v` if a PR is opened.

```bash
git push -u origin feat/build-front-door
# Trigger per the workflow's `on:` (e.g. workflow_dispatch or a test tag like v0.0.0-frontdoor-test).
# Verify in the Actions UI: one "Build and push all images" step, green, all images in GHCR.
```
Expected: green pipeline; image count in the summary matches the pre-cutover set.

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/delta-v-build-images.yml
git commit -m "ci(build): call make build/images instead of inline steps

Collapse ~20 hand-synced per-image steps into 'make images PUSH=true'.
Eliminates the build.sh<->workflow drift: image list and recipes live once."
```

---

## Task 8: Remove stale artifacts and fix stale docs

**Files:**
- Delete: `opennms-container/delta-v/build`, `opennms-container/Makefile`, `clean.pl`
- Modify: `CLAUDE.md`

- [ ] **Step 1: Confirm nothing references the doomed files (test)**

```bash
cd "$(git rev-parse --show-toplevel)"
grep -rn --exclude-dir=.git -e "opennms-container/delta-v/build\b" -e "clean\.pl" -e "opennms-container/Makefile" \
  . docs .github 2>/dev/null | grep -v "docs/superpowers/" || echo "no references"
```
Expected: `no references` (or only references inside this plan/spec). Investigate any hit before deleting.

- [ ] **Step 2: Delete the stale files**

```bash
git rm opennms-container/delta-v/build opennms-container/Makefile clean.pl
```

- [ ] **Step 3: Fix `CLAUDE.md` build docs**

In the "Build Commands" section, replace `compile.pl`/`assemble.pl`/`make assemble`/`make module`/`make build` examples with the real interface:

```markdown
# Build all images (daemons + auxiliaries)
make images

# Compile the reactor only
make build           # = ./mvnw -DskipTests install

# Rebuild one daemon image
make daemon-image DAEMON=alarmd

# Bring the stack up / check it
make up PROFILE=full
make status
make doctor          # preflight: JDK 21, Docker, GH Packages auth, images
```
Remove the `compile.pl`/`assemble.pl`-based examples (those scripts do not exist in delta-v).

- [ ] **Step 4: Prune corrupted ghost image tags (local hygiene)**

The `*atest:latest` tags (`clickhouseatest`, `clickhouse-initatest`, `l8opensim-provisioneratest`, `provisiond-imports-initatest`) are local-daemon litter from a past manual retag typo — no current script produces them (confirm: `grep -rn "atest" opennms-container/delta-v/*.sh` returns nothing). With Task 3, these images now build correct `:latest` tags, so the litter won't recur. Prune the bad ones:

```bash
grep -rn "atest" opennms-container/delta-v/*.sh || echo "no script produces *atest — confirmed manual litter"
docker images --format '{{.Repository}}:{{.Tag}}' | grep -E "atest:latest$" | xargs -r docker rmi
```
This is a local-state cleanup; nothing to commit.

- [ ] **Step 5: Verify and commit**

```bash
make help >/dev/null && echo "Makefile still valid"
git add CLAUDE.md
git commit -m "chore(build): remove stale Horizon-era build artifacts; fix CLAUDE.md

Delete superseded build script, dead recursive opennms-container/Makefile,
and leftover clean.pl. Correct CLAUDE.md to document the real make/mvnw
interface (compile.pl/assemble.pl never existed in delta-v)."
```

---

## Done-when

- `make doctor` passes on a configured box and fails with actionable ✗ on a fresh one.
- `make images` from a clean clone produces every image `docker-compose.yml` references; `make up` starts without a `pull access denied`.
- `compute-shared-libs.sh` runs to completion under `/bin/bash` (3.2) with output identical to the bash-5 baseline.
- CI builds & pushes all images via `make images PUSH=true` in one green run.
- `build`, `opennms-container/Makefile`, `clean.pl` are gone; `CLAUDE.md` documents `make`.
