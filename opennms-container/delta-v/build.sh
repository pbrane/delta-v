#!/usr/bin/env bash
#
# build.sh — Build all Docker images for OpenNMS Delta-V
#
# Usage:
#   ./build.sh              Build everything (compile + assemble + images + deltav)
#   ./build.sh images       Build base Docker images only (skip Maven)
#   ./build.sh deltav       Build Delta-V layered images only (requires base images)
#   ./build.sh daemon NAME  Rebuild a single daemon image (reuses cached base)
#   ./build.sh compile      Compile only (skip assembly and images)
#   ./build.sh push         Build and push images to registry
#
# Environment:
#   DOCKER_REGISTRY   Docker registry (default: docker.io)
#   DOCKER_ORG        Docker org/user (default: deltav)
#   SKIP_TESTS        Set to "false" to run tests (default: true)
#   JAVA_HOME         JDK 21 path (auto-detected if unset)
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
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
    if [ "$PUSH" = "true" ]; then
        log "  building ${img}:${VERSION} (push=true${PLATFORMS:+ platforms=$PLATFORMS})"
    else
        log "  building ${img}:${VERSION} (local load)"
    fi
    docker "${args[@]}"
    apply_env_version_alias "${img}"
}

# Tag the just-built image with the .env-declared VERSION too, if it differs
# from the resolved POM $VERSION. Resolves the chronic foot-gun where pom.xml
# bumps but .env doesn't, causing `docker compose up` to fail with
# "pull access denied" against the now-stale .env tag.
# Non-destructive: writes only to the local Docker daemon's tag namespace,
# never modifies .env. See feedback_image_tag_version_mismatch.
apply_env_version_alias() {
    local image_name="$1"   # e.g. "deltav/minion-gateway"
    local env_file="$SCRIPT_DIR/.env"
    [ -f "$env_file" ] || return 0
    local env_version
    env_version=$(grep '^VERSION=' "$env_file" | head -1 | cut -d= -f2 | tr -d '"' | tr -d "'")
    if [ -n "$env_version" ] && [ "$env_version" != "$VERSION" ]; then
        docker tag "${image_name}:${VERSION}" "${image_name}:${env_version}"
        log "  also tagged: ${image_name}:${env_version} (from .env, differs from POM $VERSION)"
    fi
}

# Detect daemon-boot modules whose source is newer than their target JAR and
# rebuild them in-place. Guards against the "stale JAR" failure mode where
# `do_deltav_images` stages a weeks-old JAR into a freshly-built Docker image,
# producing an image with a recent mtime but stale class files inside.
# A single mtime comparison per module is much cheaper than a blind rebuild.
check_daemon_boot_freshness() {
    log "Checking daemon-boot JAR freshness..."
    local stale_modules=()
    local module_dir module_name jar candidate src_dir pom

    for module_dir in "$REPO_ROOT"/core/daemon-boot-*/; do
        module_name=$(basename "${module_dir%/}")
        src_dir="${module_dir%/}/src/main"
        pom="${module_dir%/}/pom.xml"

        # Look for the Spring Boot fat jar, excluding the -sources / -javadoc /
        # .original siblings that live alongside it after `mvn package`.
        jar=""
        for candidate in "${module_dir%/}"/target/org.opennms.core."${module_name}"-*.jar; do
            case "$candidate" in
                *-sources.jar|*-javadoc.jar|*.original) continue ;;
            esac
            if [ -f "$candidate" ]; then
                jar="$candidate"
                break
            fi
        done

        if [ -z "$jar" ]; then
            log "  stale: core/$module_name (no target JAR)"
            stale_modules+=("core/$module_name")
            continue
        fi

        # If any .java under src/main/ or the pom.xml is newer than the JAR,
        # the module needs a rebuild. find -newer is portable across macOS
        # BSD find and GNU find.
        if [ -n "$(find "$src_dir" "$pom" -newer "$jar" -print 2>/dev/null)" ]; then
            log "  stale: core/$module_name (source newer than $(basename "$jar"))"
            stale_modules+=("core/$module_name")
        fi
    done

    if [ ${#stale_modules[@]} -eq 0 ]; then
        log "All daemon-boot JARs are up-to-date"
        return
    fi

    log "Rebuilding ${#stale_modules[@]} stale daemon-boot module(s)..."
    local pl_args
    pl_args=$(IFS=,; echo "${stale_modules[*]}")
    local test_flag=""
    [ "$SKIP_TESTS" = "true" ] && test_flag="-DskipTests"
    ( cd "$REPO_ROOT" && ./mvnw -B $test_flag -pl "$pl_args" -am install ) \
        || err "Failed to rebuild stale daemon-boot modules"
    log "Stale modules rebuilt"
}

check_prereqs() {
    command -v docker >/dev/null 2>&1 || err "docker not found"
    command -v ./mvnw >/dev/null 2>&1 || true  # Maven wrapper

    # Verify Java 21
    if [ -z "${JAVA_HOME:-}" ]; then
        if [ -d "/Library/Java/JavaVirtualMachines/temurin-21.jdk/Contents/Home" ]; then
            export JAVA_HOME="/Library/Java/JavaVirtualMachines/temurin-21.jdk/Contents/Home"
        fi
    fi
    if [ -z "${JAVA_HOME:-}" ]; then
        err "JAVA_HOME not set and temurin-21 not found. Set JAVA_HOME to a JDK 21 installation."
    fi
    java_version=$("${JAVA_HOME}/bin/java" -version 2>&1 | head -1 | sed 's/.*"\([0-9]*\)\..*/\1/')
    [ "$java_version" = "21" ] || err "Java 21 required (JAVA_HOME=$JAVA_HOME reports: $java_version)"
    export PATH="${JAVA_HOME}/bin:${PATH}"

    # Local --load builds need the "default" builder (Docker Desktop's
    # "desktop-linux" is rejected by some sub-Makefiles). In CI, PUSH=true uses
    # the multi-arch docker-container builder set up by setup-buildx-action —
    # the "default"/docker driver cannot do multi-arch --push, so don't switch.
    if [ "$PUSH" != "true" ]; then
        # Capture the full `buildx inspect` output FIRST, then extract the name.
        # Piping `... | head -1` directly closes the pipe early and SIGPIPEs
        # buildx (exit 141), which under `set -o pipefail` aborts the script
        # intermittently (race on the pipe buffer). Full-capture avoids the pipe.
        local buildx_info current_buildx
        buildx_info=$(docker buildx inspect 2>/dev/null) || true
        current_buildx=$(printf '%s\n' "$buildx_info" | sed -n '1s/^Name: *//p')
        if [ "$current_buildx" != "default" ]; then
            log "Switching Docker buildx from '$current_buildx' to 'default'..."
            docker context use default 2>/dev/null || true
            docker buildx use default 2>/dev/null || true
        fi
    fi
}

do_compile() {
    log "Compiling Delta-V (version $VERSION)..."
    local test_flag=""
    [ "$SKIP_TESTS" = "true" ] && test_flag="-DskipTests"
    cd "$REPO_ROOT"
    ./mvnw -B $test_flag install
}

do_assemble() {
    log "Karaf assembly removed — Delta-V uses Spring Boot daemons."
    log "Use 'make images' to build daemon images."
}

do_db_init_image() {
    log "Building db-init image (${IMAGE_PREFIX}/db-init:$VERSION)..."
    cd "$REPO_ROOT"
    ./mvnw -B -f core/db-init/pom.xml -DskipTests package
    build_image db-init -f "$REPO_ROOT/core/db-init/Dockerfile" "$REPO_ROOT/core/db-init"
}

do_minion_gateway_image() {
    log "Building minion-gateway image (${IMAGE_PREFIX}/minion-gateway:$VERSION)..."
    cd "$REPO_ROOT"
    # Use -pl ... -am install (not -f pom.xml package) because minion-gateway
    # depends on org.opennms.core.minion-grpc-contracts; Spring Boot repackage
    # needs the contracts JAR present in ~/.m2 (feedback_spring_boot_repackage_needs_clean).
    ./mvnw -B -pl core/minion-gateway -am -DskipTests install
    build_image minion-gateway \
        -f "$SCRIPT_DIR/minion-gateway/Dockerfile" \
        --build-arg "JRE_BASE=${IMAGE_PREFIX}/jre-deltav:21" \
        "$REPO_ROOT/core/minion-gateway/"
}

do_envoy_image() {
    log "Building envoy image (${IMAGE_PREFIX}/envoy:$VERSION)..."
    # Pure Docker build — Envoy is the upstream image plus envoy.yaml + curl
    # (for the docker-compose healthcheck). No Maven involvement.
    build_image envoy -f "$SCRIPT_DIR/envoy/Dockerfile" "$SCRIPT_DIR/envoy"
}

do_perspective_app_init_image() {
    log "Building perspective-app-init image (${IMAGE_PREFIX}/perspective-app-init:$VERSION)..."
    # Tiny Alpine + psql client + init.sh. Seeds the smoke-baseline
    # perspective application after provisiond imports the perspective-smoke
    # requisition. No Maven involvement.
    build_image perspective-app-init -f "$SCRIPT_DIR/Dockerfile.perspective-app-init" "$SCRIPT_DIR"
}

do_flow_enricher_image() {
    log "Building flow-enricher image (${IMAGE_PREFIX}/flow-enricher:$VERSION)..."
    cd "$REPO_ROOT"
    ./mvnw -B -f core/flow-enricher/pom.xml -DskipTests package
    build_image flow-enricher -f "$REPO_ROOT/core/flow-enricher/Dockerfile" "$REPO_ROOT/core/flow-enricher"
}

do_prometheus_writer_image() {
    log "Building prometheus-writer image (${IMAGE_PREFIX}/prometheus-writer:$VERSION)..."
    cd "$REPO_ROOT"
    ./mvnw -B -f core/prometheus-writer/pom.xml -DskipTests package
    build_image prometheus-writer -f "$REPO_ROOT/core/prometheus-writer/Dockerfile" "$REPO_ROOT/core/prometheus-writer"
}

do_alerts_forwarder_image() {
    log "Building alerts-forwarder image (${IMAGE_PREFIX}/alerts-forwarder:$VERSION)..."
    cd "$REPO_ROOT"
    ./mvnw -B -f core/alerts-forwarder/pom.xml -DskipTests package
    build_image alerts-forwarder -f "$REPO_ROOT/core/alerts-forwarder/Dockerfile" "$REPO_ROOT/core/alerts-forwarder"
}

do_clickhouse_image() {
    log "Building ${IMAGE_PREFIX}/clickhouse:$VERSION..."
    # Repo-root context: Dockerfile.clickhouse COPYs from opennms-container/delta-v/clickhouse/
    # and core/flow-enricher/src/main/proto/ (paths relative to the build context).
    build_image clickhouse -f "$SCRIPT_DIR/Dockerfile.clickhouse" "$REPO_ROOT"
}

do_clickhouse_init_image() {
    log "Building ${IMAGE_PREFIX}/clickhouse-init:$VERSION..."
    # Repo-root context (same reason as clickhouse).
    build_image clickhouse-init -f "$SCRIPT_DIR/Dockerfile.clickhouse-init" "$REPO_ROOT"
}

do_grafana_image() {
    log "Building ${IMAGE_PREFIX}/grafana:$VERSION..."
    cd "$SCRIPT_DIR"
    build_image grafana -f Dockerfile.grafana .
}

do_provisiond_imports_init_image() {
    log "Building ${IMAGE_PREFIX}/provisiond-imports-init:$VERSION..."
    cd "$SCRIPT_DIR"
    build_image provisiond-imports-init -f Dockerfile.provisiond-imports-init .
}

do_nl6_provisioner_image() {
    log "Building ${IMAGE_PREFIX}/nl6-provisioner:$VERSION..."
    cd "$SCRIPT_DIR"
    build_image nl6-provisioner -f Dockerfile.nl6-provisioner .
}

do_mock_snmp_agent_image() {
    log "Building ${IMAGE_PREFIX}/mock-snmp-agent:$VERSION..."
    build_image mock-snmp-agent -f "$SCRIPT_DIR/mock-snmp-agent/Dockerfile" "$SCRIPT_DIR/mock-snmp-agent"
}

do_flow_exporter_image() {
    log "Building ${IMAGE_PREFIX}/flow-exporter:$VERSION..."
    build_image flow-exporter -f "$SCRIPT_DIR/flow-exporter/Dockerfile" "$SCRIPT_DIR/flow-exporter"
}

do_sflow_exporter_image() {
    log "Building ${IMAGE_PREFIX}/sflow-exporter:$VERSION..."
    build_image sflow-exporter -f "$SCRIPT_DIR/sflow-exporter/Dockerfile" "$SCRIPT_DIR/sflow-exporter"
}

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

do_images() {
    log "Karaf-era images removed — Delta-V uses Spring Boot daemons."
    log "Use 'make images' to build daemon images."
}

do_deltav_images() {
    log "Building Delta-V layered images..."

    # Ensure the JRE base image exists. On a fresh runner/clone it won't be in
    # the local daemon (and multi-arch --push never loads locally), so build it.
    # Keeps `make images` self-contained both locally (--load) and in CI (--push).
    if ! docker image inspect "${IMAGE_PREFIX}/jre-deltav:21" >/dev/null 2>&1; then
        do_jre_image
    fi

    # Phase 0: Self-heal stale daemon-boot JARs before staging. If any
    # module's source files are newer than its target JAR, `do_deltav_images`
    # would otherwise stage the stale JAR into the new image, producing a
    # container with fresh mtime but outdated class files. Rebuild the stale
    # modules in-place so the images carry current code.
    check_daemon_boot_freshness

    # Phase 1: Extract and deduplicate
    "$SCRIPT_DIR/compute-shared-libs.sh" "$REPO_ROOT" "$VERSION"

    cd "$SCRIPT_DIR"

    # Phase 2: Build daemon-base image
    log "Building ${IMAGE_PREFIX}/daemon-base:$VERSION..."
    build_image daemon-base --no-cache \
        -f Dockerfile.daemon-base \
        --build-arg "JRE_IMAGE=${IMAGE_PREFIX}/jre-deltav:21" \
        .

    # Phase 3: Build per-daemon images
    for name in $DAEMON_NAMES; do
        local main_class
        main_class=$(cat "staging/$name/.main_class")
        log "Building ${IMAGE_PREFIX}/$name:$VERSION (main: $main_class)..."
        build_image "$name" \
            -f Dockerfile.daemon-per \
            --build-arg "VERSION=$VERSION" \
            --build-arg "DAEMON_BASE_IMAGE=${IMAGE_PREFIX}/daemon-base" \
            --build-arg "DAEMON_NAME=$name" \
            --build-arg "MAIN_CLASS=$main_class" \
            .
    done

    # --- Stage Minion Boot fat JAR ---
    log "Staging Minion Boot fat JAR..."
    mkdir -p "$SCRIPT_DIR/staging/minion-boot"
    # Copy only the fat JAR (exclude -sources.jar, -javadoc.jar, .original)
    find "$REPO_ROOT/core/daemon-boot-minion/target" \
        -maxdepth 1 -name "*.jar" \
        ! -name "*-sources.jar" ! -name "*-javadoc.jar" ! -name "*.original" \
        -exec cp {} "$SCRIPT_DIR/staging/minion-boot/daemon-boot-minion.jar" \;

    # --- Build Minion Boot image ---
    log "Building ${IMAGE_PREFIX}/minion-boot:$VERSION..."
    build_image minion-boot \
        --build-arg "VERSION=$VERSION" \
        --build-arg "JRE_IMAGE=${IMAGE_PREFIX}/jre-deltav:21" \
        -f Dockerfile.minion-boot \
        .

    # Clean up staging
    rm -rf "$SCRIPT_DIR/staging"

    # --- Build db-init (one-shot PostgreSQL schema migration) ---
    # db-init is a small standalone image used once at stack startup to
    # run Liquibase against postgres. Included in `./build.sh deltav`
    # so a single command produces every image docker-compose.yml
    # references. Otherwise a freshly-cloned workspace would fail its
    # first deploy with "pull access denied for deltav/db-init".
    do_db_init_image

    # --- Build flow-enricher (standalone Spring Cloud Stream service) ---
    # flow-enricher does not share daemon-base because its dependency
    # profile is fundamentally different from the 12 horizon-derived
    # daemons (Spring Cloud Stream + Kafka binder + Caffeine + protobuf,
    # vs. legacy Spring 4.2 / Hibernate / Karaf-era libs). Build it as a
    # standalone image, like db-init.
    do_flow_enricher_image

    # --- Build prometheus-writer (standalone Spring Cloud Stream service) ---
    # prometheus-writer consumes the Kafka Time Series topic and publishes
    # samples via Prometheus Remote Write. Like flow-enricher, it has a
    # dependency profile distinct from the horizon-derived daemons, so it is
    # built as a standalone image rather than sharing daemon-base.
    do_prometheus_writer_image

    # --- Build alerts-forwarder (standalone Spring Boot service) ---
    # alerts-forwarder consumes the Kafka alarms topic and forwards alerts to
    # Alertmanager. Like prometheus-writer, it has its own Dockerfile in
    # core/alerts-forwarder/ and does not share the daemon-base layer.
    do_alerts_forwarder_image

    # --- Build minion-gateway (gRPC ingress translator for Minion-facing surface) ---
    # minion-gateway sits between Envoy and the internal Kafka topics, translating
    # gRPC bidi calls from Minions into Kafka publishes. Built standalone (like
    # db-init / flow-enricher / prometheus-writer) because its dependency profile
    # is Spring gRPC + protobuf rather than the horizon-derived daemon stack.
    do_minion_gateway_image

    # --- Build envoy (gRPC ingress in front of minion-gateway) ---
    # envoyproxy/envoy:1.30.4 + envoy.yaml + curl (compose healthcheck needs it).
    # No Maven; pure docker build.
    do_envoy_image

    # --- Build perspective-app-init (smoke-baseline perspective seed) ---
    # Tiny Alpine + psql client. Wires the Devices-API-Perspective-App
    # application + service map + perspective locations after provisiond
    # imports the perspective-smoke requisition.
    do_perspective_app_init_image

    # --- Auxiliary images (no Maven; previously only built in CI) ---
    # These complete the set docker-compose.yml references so a single
    # `make images` yields a deployable stack with no missing-image pulls.
    do_clickhouse_image
    do_clickhouse_init_image
    do_grafana_image
    do_provisiond_imports_init_image
    do_nl6_provisioner_image
    do_mock_snmp_agent_image
    do_flow_exporter_image
    do_sflow_exporter_image

    # Post-build summary. With PUSH=true the images are multi-arch and pushed
    # to the registry — they are NOT loaded into the local daemon, so a local
    # `docker images` summary would be empty (and the grep would exit 1, which
    # under `set -euo pipefail` would fail the whole build after every image
    # already pushed successfully). Only show the local listing for --load builds.
    if [ "$PUSH" = "true" ]; then
        log "Delta-V images pushed to ${IMAGE_PREFIX} (multi-arch; not loaded locally)."
    else
        log "Delta-V images built:"
        docker images --format "  {{.Repository}}:{{.Tag}}\t{{.Size}}" | grep -E "daemon-base|alarmd|alerts-forwarder|bsmd|collectd|discovery|enlinkd|eventtranslator|perspectivepollerd|pollerd|provisiond|syslogd|telemetryd|trapd|minion-boot|flow-enricher|prometheus-writer|minion-gateway|envoy|perspective-app-init|clickhouse|clickhouse-init|grafana|provisiond-imports-init|nl6-provisioner|mock-snmp-agent|flow-exporter|sflow-exporter" | sort | head -60 || true
    fi
}

# Build a single daemon's layered image, reusing the existing cached
# deltav/daemon-base. Dev-loop shortcut: when only one daemon's code changed,
# `./build.sh daemon <name>` rebuilds just that daemon's boot JAR and image
# instead of `./build.sh deltav` (which rebuilds the --no-cache shared base,
# all 12 per-daemon images, and the minion/auxiliary images). The shared base
# is intentionally NOT rebuilt — run `./build.sh deltav` if a dependency that
# lands in the shared base changed.
do_single_daemon_image() {
    local name="${1:-}"
    [ -n "$name" ] || err "the 'daemon' command needs a daemon name, e.g. 'make daemon-image DAEMON=alarmd'"

    # Standalone images that have their own Dockerfile in core/<name>/ rather
    # than going through daemon-base + compute-shared-libs.sh. Route them to
    # their dedicated build functions and return immediately.
    case "$name" in
        alerts-forwarder)
            do_alerts_forwarder_image
            return
            ;;
    esac

    # Validate against the known daemon set (word-boundary match).
    case " $DAEMON_NAMES " in
        *" $name "*) ;;
        *) err "unknown daemon '$name'. Valid daemons: $DAEMON_NAMES (plus standalone: alerts-forwarder)" ;;
    esac

    # The shared base and JRE images must already exist — single-daemon mode
    # reuses them rather than rebuilding. A prior `./build.sh deltav` produces
    # both (and the other 11 daemon-boot JARs that compute-shared-libs needs).
    docker image inspect "${IMAGE_PREFIX}/daemon-base:$VERSION" >/dev/null 2>&1 \
        || err "${IMAGE_PREFIX}/daemon-base:$VERSION not found — run 'make images' once first (single-daemon mode reuses the shared base)"
    docker image inspect "${IMAGE_PREFIX}/jre-deltav:21" >/dev/null 2>&1 \
        || err "${IMAGE_PREFIX}/jre-deltav:21 not found — run 'make images' first (it builds the JRE base)"

    log "Single-daemon build: $name (reusing ${IMAGE_PREFIX}/daemon-base:$VERSION)"
    log "NOTE: the shared base layer is not rebuilt — run 'make images' if a shared dependency changed."

    # Rebuild this daemon's boot JAR. `-am` also rebuilds its delta-v reactor
    # dependencies (e.g. newly added feature modules) so the staged fat JAR is
    # current.
    local test_flag=""
    [ "$SKIP_TESTS" = "true" ] && test_flag="-DskipTests"
    log "Rebuilding core/daemon-boot-$name..."
    ( cd "$REPO_ROOT" && ./mvnw -B $test_flag -pl "core/daemon-boot-$name" -am install ) \
        || err "Failed to build core/daemon-boot-$name"

    cd "$SCRIPT_DIR"

    # compute-shared-libs.sh re-derives the shared/unique library split — it
    # needs every daemon's fat JAR present — and repopulates staging/. This is
    # unzip/copy work, not a compile.
    "$SCRIPT_DIR/compute-shared-libs.sh" "$REPO_ROOT" "$VERSION"

    local main_class
    main_class=$(cat "staging/$name/.main_class")
    log "Building ${IMAGE_PREFIX}/$name:$VERSION (main: $main_class)..."
    build_image "$name" \
        -f Dockerfile.daemon-per \
        --build-arg "VERSION=$VERSION" \
        --build-arg "DAEMON_BASE_IMAGE=${IMAGE_PREFIX}/daemon-base" \
        --build-arg "DAEMON_NAME=$name" \
        --build-arg "MAIN_CLASS=$main_class" \
        .

    rm -rf "$SCRIPT_DIR/staging"
    log "Built ${IMAGE_PREFIX}/$name:$VERSION (+ :latest). Recreate just that service with: docker compose up -d $name"
}


usage() {
    cat <<'USAGE'
Usage: ./build.sh [command]

Commands:
  (none)    Full build: compile + assemble + images + deltav
  compile   Compile only (Maven)
  assemble  Assemble distributions (Daemon + Alarmd + Minion + Sentinel)
  images    Build base Docker images only (requires prior assembly)
  jre       Build JRE base image (deltav/jre-deltav:21, rarely needed)
  deltav    Build Delta-V layered images (stages JARs into derived images)
  daemon    Rebuild a single daemon image, reusing the cached shared base
            (requires a daemon name argument; needs a prior './build.sh deltav')
  push      Build and push images to registry
  clean     Remove named Docker volumes (fresh start)
  help      Show this help

Environment variables:
  DOCKER_REGISTRY   Registry (default: docker.io)
  DOCKER_ORG        Organization (default: deltav)
  IMAGE_PREFIX      Image name prefix (default: $DOCKER_ORG, i.e. "deltav"; CI sets ghcr.io/pbrane)
  PUSH              "true" to push images instead of local --load (default: false)
  PLATFORMS         Comma-separated buildx platforms (e.g. linux/amd64,linux/arm64); used only when PUSH=true
  SKIP_TESTS        Skip tests (default: true)
  JAVA_HOME         JDK 21 path

Examples:
  ./build.sh                                    # Full build
  ./build.sh images                             # Rebuild images only
  ./build.sh daemon alarmd                      # Rebuild just the alarmd image
  PUSH=true PLATFORMS=linux/amd64,linux/arm64 IMAGE_PREFIX=ghcr.io/pbrane ./build.sh deltav   # Build + push multi-arch
  ./build.sh clean && docker compose up -d      # Fresh deployment
USAGE
}

do_clean() {
    log "Removing Delta-V Docker volumes..."
    cd "$SCRIPT_DIR"
    docker compose down -v 2>/dev/null || true
    log "Volumes removed. Run 'make up' for a fresh start."
}

main() {
    check_prereqs

    case "${1:-all}" in
        all)
            do_compile
            do_deltav_images
            log "Build complete! Run: make up"
            ;;
        compile)
            do_compile
            ;;
        assemble)
            do_assemble
            ;;
        images)
            do_images
            ;;
        jre)
            do_jre_image
            ;;
        deltav)
            do_deltav_images
            ;;
        daemon)
            do_single_daemon_image "${2:-}"
            ;;
        push)
            do_compile
            do_assemble
            do_images push
            do_deltav_images
            ;;
        clean)
            do_clean
            ;;
        help|-h|--help)
            usage
            ;;
        *)
            err "Unknown command: $1 (run './build.sh help' for usage)"
            ;;
    esac
}

main "$@"
