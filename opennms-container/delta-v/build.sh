#!/usr/bin/env bash
#
# build.sh — Build all Docker images for OpenNMS Delta-V
#
# Usage:
#   ./build.sh              Build everything (compile + assemble + images + deltav)
#   ./build.sh images       Build base Docker images only (skip Maven)
#   ./build.sh deltav       Build Delta-V layered images only (requires base images)
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

# Detect version from POM (skip parent version, get project version)
VERSION="$(cd "$REPO_ROOT" && ./mvnw help:evaluate -Dexpression=project.version -q -DforceStdout 2>/dev/null || grep '<version>0\.' "$REPO_ROOT/pom.xml" | head -1 | sed 's/.*<version>\(.*\)<\/version>.*/\1/')"

log() { echo "==> $*"; }
err() { echo "ERROR: $*" >&2; exit 1; }

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

    # Ensure Docker buildx uses the "default" builder instance.
    # Docker Desktop sets the active builder to "desktop-linux", which the
    # Makefile in opennms-container/core and sentinel rejects.
    local current_buildx
    current_buildx=$(docker buildx inspect 2>/dev/null | head -1 | sed 's/^Name: *//')
    if [ "$current_buildx" != "default" ]; then
        log "Switching Docker buildx from '$current_buildx' to 'default'..."
        docker context use default 2>/dev/null || true
        docker buildx use default 2>/dev/null || true
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
    log "Use './build.sh deltav' to build daemon images."
}

do_db_init_image() {
    log "Building db-init image (deltav/db-init:$VERSION)..."
    cd "$REPO_ROOT"
    ./mvnw -B -f core/db-init/pom.xml -DskipTests package
    cd "$REPO_ROOT/core/db-init"
    docker build -t "deltav/db-init:$VERSION" -t "deltav/db-init:latest" .
    apply_env_version_alias "deltav/db-init"
}

do_minion_gateway_image() {
    log "Building minion-gateway image (deltav/minion-gateway:$VERSION)..."
    cd "$REPO_ROOT"
    # Use -pl ... -am install (not -f pom.xml package) because minion-gateway
    # depends on org.opennms.core.minion-grpc-contracts; Spring Boot repackage
    # needs the contracts JAR present in ~/.m2 (feedback_spring_boot_repackage_needs_clean).
    ./mvnw -B -pl core/minion-gateway -am -DskipTests install
    docker build \
        -t "deltav/minion-gateway:$VERSION" \
        -t "deltav/minion-gateway:latest" \
        -f "$SCRIPT_DIR/minion-gateway/Dockerfile" \
        "$REPO_ROOT/core/minion-gateway/"
    apply_env_version_alias "deltav/minion-gateway"
}

do_envoy_image() {
    log "Building envoy image (deltav/envoy:$VERSION)..."
    # Pure Docker build — Envoy is the upstream image plus envoy.yaml + curl
    # (for the docker-compose healthcheck). No Maven involvement.
    cd "$SCRIPT_DIR/envoy"
    docker build \
        -t "deltav/envoy:$VERSION" \
        -t "deltav/envoy:latest" \
        .
    apply_env_version_alias "deltav/envoy"
}

do_perspective_app_init_image() {
    log "Building perspective-app-init image (deltav/perspective-app-init:$VERSION)..."
    # Tiny Alpine + psql client + init.sh. Seeds the smoke-baseline
    # perspective application after provisiond imports the perspective-smoke
    # requisition. No Maven involvement.
    cd "$SCRIPT_DIR"
    docker build \
        -f Dockerfile.perspective-app-init \
        -t "deltav/perspective-app-init:$VERSION" \
        -t "deltav/perspective-app-init:latest" \
        .
    apply_env_version_alias "deltav/perspective-app-init"
}

do_flow_enricher_image() {
    log "Building flow-enricher image (deltav/flow-enricher:$VERSION)..."
    cd "$REPO_ROOT"
    ./mvnw -B -f core/flow-enricher/pom.xml -DskipTests package
    cd "$REPO_ROOT/core/flow-enricher"
    docker build -t "deltav/flow-enricher:$VERSION" -t "deltav/flow-enricher:latest" .
    apply_env_version_alias "deltav/flow-enricher"
}

do_prometheus_writer_image() {
    log "Building prometheus-writer image (deltav/prometheus-writer:$VERSION)..."
    cd "$REPO_ROOT"
    ./mvnw -B -f core/prometheus-writer/pom.xml -DskipTests package
    cd "$REPO_ROOT/core/prometheus-writer"
    docker build -t "deltav/prometheus-writer:$VERSION" -t "deltav/prometheus-writer:latest" .
    apply_env_version_alias "deltav/prometheus-writer"
}

do_jre_image() {
    log "Building deltav/jre-deltav:21..."
    cd "$SCRIPT_DIR"
    docker build -f Dockerfile.jre \
        -t "deltav/jre-deltav:21" \
        -t "deltav/jre-deltav:latest" \
        .
    log "JRE image built:"
    docker images deltav/jre-deltav --format "  {{.Repository}}:{{.Tag}}\t{{.Size}}"
}

do_images() {
    log "Karaf-era images removed — Delta-V uses Spring Boot daemons."
    log "Use './build.sh deltav' to build daemon images."
}

do_deltav_images() {
    log "Building Delta-V layered images..."

    # Check that JRE base image exists
    if ! docker image inspect deltav/jre-deltav:21 >/dev/null 2>&1; then
        err "deltav/jre-deltav:21 not found — run './build.sh jre' first"
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
    log "Building deltav/daemon-base:$VERSION..."
    docker build --no-cache \
        -f Dockerfile.daemon-base \
        -t "deltav/daemon-base:$VERSION" \
        -t "deltav/daemon-base:latest" \
        .
    apply_env_version_alias "deltav/daemon-base"

    # Phase 3: Build per-daemon images
    local daemon_names="alarmd bsmd collectd discovery enlinkd eventtranslator perspectivepollerd pollerd provisiond syslogd telemetryd trapd"
    for name in $daemon_names; do
        local main_class
        main_class=$(cat "staging/$name/.main_class")
        log "Building deltav/$name:$VERSION (main: $main_class)..."
        docker build \
            -f Dockerfile.daemon-per \
            --build-arg "VERSION=$VERSION" \
            --build-arg "DAEMON_NAME=$name" \
            --build-arg "MAIN_CLASS=$main_class" \
            -t "deltav/$name:$VERSION" \
            -t "deltav/$name:latest" \
            .
        apply_env_version_alias "deltav/$name"
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
    log "Building deltav/minion-boot:$VERSION..."
    docker build \
        --build-arg "VERSION=$VERSION" \
        -f Dockerfile.minion-boot \
        -t "deltav/minion-boot:$VERSION" \
        -t "deltav/minion-boot:latest" \
        .
    apply_env_version_alias "deltav/minion-boot"

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

    log "Delta-V images built:"
    docker images --format "  {{.Repository}}:{{.Tag}}\t{{.Size}}" | grep -E "daemon-base|alarmd|bsmd|collectd|discovery|enlinkd|eventtranslator|perspectivepollerd|pollerd|provisiond|syslogd|telemetryd|trapd|daemon-deltav|minion-deltav|minion-boot|flow-enricher|prometheus-writer|minion-gateway|envoy|perspective-app-init" | sort | head -30
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
  push      Build and push images to registry
  clean     Remove named Docker volumes (fresh start)
  help      Show this help

Environment variables:
  DOCKER_REGISTRY   Registry (default: docker.io)
  DOCKER_ORG        Organization (default: deltav)
  SKIP_TESTS        Skip tests (default: true)
  JAVA_HOME         JDK 21 path

Examples:
  ./build.sh                                    # Full build
  ./build.sh images                             # Rebuild images only
  DOCKER_ORG=pbranestrategy ./build.sh push     # Push to custom registry
  ./build.sh clean && docker compose up -d      # Fresh deployment
USAGE
}

do_clean() {
    log "Removing Delta-V Docker volumes..."
    cd "$SCRIPT_DIR"
    docker compose down -v 2>/dev/null || true
    log "Volumes removed. Run 'docker compose up -d' for a fresh start."
}

main() {
    check_prereqs

    case "${1:-all}" in
        all)
            do_compile
            if ! docker image inspect deltav/jre-deltav:21 >/dev/null 2>&1; then
                do_jre_image
            fi
            do_deltav_images
            log "Build complete! Run: cd $SCRIPT_DIR && docker compose up -d"
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
        push)
            do_compile
            do_assemble
            do_images push
            if ! docker image inspect deltav/jre-deltav:21 >/dev/null 2>&1; then
                do_jre_image
            fi
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
