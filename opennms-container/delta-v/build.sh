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
#   DOCKER_ORG        Docker org/user (default: opennms)
#   SKIP_TESTS        Set to "false" to run tests (default: true)
#   JAVA_HOME         JDK 21 path (auto-detected if unset)
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SKIP_TESTS="${SKIP_TESTS:-true}"
DOCKER_REGISTRY="${DOCKER_REGISTRY:-docker.io}"
DOCKER_ORG="${DOCKER_ORG:-opennms}"

# Detect version from POM (extract <version> from root pom.xml)
VERSION="$(grep -m1 '<version>' "$REPO_ROOT/pom.xml" | sed 's/.*<version>\(.*\)<\/version>.*/\1/')"

log() { echo "==> $*"; }
err() { echo "ERROR: $*" >&2; exit 1; }

check_prereqs() {
    command -v docker >/dev/null 2>&1 || err "docker not found"
    command -v perl >/dev/null 2>&1   || err "perl not found (needed by compile.pl)"

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
    log "Compiling OpenNMS (version $VERSION)..."
    local test_flag=""
    [ "$SKIP_TESTS" = "true" ] && test_flag="-DskipTests"
    cd "$REPO_ROOT"
    ./compile.pl $test_flag
}

do_assemble() {
    log "Building Karaf container modules (shared + karaf + features)..."
    cd "$REPO_ROOT"
    ./maven/bin/mvn -DskipTests -pl container/shared,container/karaf,container/features clean install

    log "Building Sentinel and Minion features modules..."
    cd "$REPO_ROOT"
    ./maven/bin/mvn -DskipTests -pl features/container/sentinel,features/container/minion,features/minion/core/repository,features/minion/repository clean install

    log "Building Sentinel assembly..."
    cd "$REPO_ROOT/opennms-assemblies/sentinel"
    ../../maven/bin/mvn -DskipTests clean install

    log "Building Minion assembly..."
    cd "$REPO_ROOT/opennms-assemblies/minion"
    ../../maven/bin/mvn -DskipTests clean install

    log "Building Daemon assembly..."
    cd "$REPO_ROOT/opennms-assemblies/daemon"
    ../../maven/bin/mvn -DskipTests clean install

    log "Building Alarmd assembly..."
    cd "$REPO_ROOT/opennms-assemblies/alarmd"
    ../../maven/bin/mvn -DskipTests clean install
}

do_db_init_image() {
    log "Building db-init image (opennms/db-init:$VERSION)..."
    cd "$REPO_ROOT"
    ./maven/bin/mvn -f core/db-init/pom.xml -DskipTests package
    cd "$REPO_ROOT/core/db-init"
    docker build -t "opennms/db-init:$VERSION" -t "opennms/db-init:latest" .
}

do_jre_image() {
    log "Building opennms/jre-deltav:21..."
    cd "$SCRIPT_DIR"
    docker build -f Dockerfile.jre \
        -t "opennms/jre-deltav:21" \
        -t "opennms/jre-deltav:latest" \
        .
    log "JRE image built:"
    docker images opennms/jre-deltav --format "  {{.Repository}}:{{.Tag}}\t{{.Size}}"
}

do_images() {
    local make_args="DOCKER_REGISTRY=$DOCKER_REGISTRY DOCKER_ORG=$DOCKER_ORG"
    [ "${1:-}" = "push" ] && make_args="$make_args DOCKER_FLAGS=--push"

    # The legacy Horizon webapp image (opennms-full-assembly) is not built by Delta-V.
    # Delta-V uses Spring Boot daemons, not the monolithic Karaf webapp.

    # The sentinel Makefile tags as opennms/sentinel, but the Delta-V
    # docker-compose expects opennms/daemon. Build then re-tag.
    log "Building Daemon image (opennms/daemon:$VERSION)..."
    cd "$REPO_ROOT/opennms-container/sentinel"
    make image $make_args
    docker image tag "opennms/sentinel:$VERSION" "opennms/daemon:$VERSION"
    docker image tag "opennms/sentinel:$VERSION" "opennms/daemon:latest"

    # Build Minion base image
    log "Building Minion image (opennms/minion:$VERSION)..."
    cd "$REPO_ROOT/opennms-container/minion"
    make image $make_args

    do_db_init_image

    log "Docker images built:"
    docker images --format "  {{.Repository}}:{{.Tag}}\t{{.Size}}" | grep -E "(horizon|daemon|sentinel|minion|db-init)" | head -20
}

do_deltav_images() {
    log "Building Delta-V layered images..."

    # Check that JRE base image exists
    if ! docker image inspect opennms/jre-deltav:21 >/dev/null 2>&1; then
        err "opennms/jre-deltav:21 not found — run './build.sh jre' first"
    fi

    # Phase 1: Extract and deduplicate
    "$SCRIPT_DIR/compute-shared-libs.sh" "$REPO_ROOT" "$VERSION"

    cd "$SCRIPT_DIR"

    # Phase 2: Build daemon-base image
    log "Building opennms/daemon-base:$VERSION..."
    docker build --no-cache \
        -f Dockerfile.daemon-base \
        -t "opennms/daemon-base:$VERSION" \
        -t "opennms/daemon-base:latest" \
        .

    # Phase 3: Build per-daemon images
    local daemon_names="alarmd bsmd collectd discovery enlinkd eventtranslator perspectivepollerd pollerd provisiond syslogd telemetryd trapd"
    for name in $daemon_names; do
        local main_class
        main_class=$(cat "staging/$name/.main_class")
        log "Building opennms/$name:$VERSION (main: $main_class)..."
        docker build \
            -f Dockerfile.daemon-per \
            --build-arg "VERSION=$VERSION" \
            --build-arg "DAEMON_NAME=$name" \
            --build-arg "MAIN_CLASS=$main_class" \
            -t "opennms/$name:$VERSION" \
            -t "opennms/$name:latest" \
            .
    done

    # Stage Minion overlay JARs (these are separate from the daemon deduplication)
    log "Staging Minion overlay JARs..."
    mkdir -p "$SCRIPT_DIR/staging/daemon"
    local minion_pairs=(
        "features/poller/api/target/org.opennms.features.poller.api-$VERSION.jar:poller-api.jar"
        "features/poller/client-rpc/target/org.opennms.features.poller.client-rpc-$VERSION.jar:poller-client-rpc.jar"
        "features/minion/core/impl/target/core-impl-$VERSION.jar:minion-core-impl.jar"
        "features/poller/monitors/core/target/org.opennms.features.poller.monitors.core-$VERSION.jar:poller-monitors-core.jar"
    )
    for pair in "${minion_pairs[@]}"; do
        local src="${pair%%:*}"
        local dst="${pair##*:}"
        if [ -f "$REPO_ROOT/$src" ]; then
            cp "$REPO_ROOT/$src" "$SCRIPT_DIR/staging/daemon/$dst"
        else
            log "WARNING: $src not found"
        fi
    done

    # Minion image
    log "Building opennms/minion-deltav:$VERSION..."
    docker build \
        --build-arg "VERSION=$VERSION" \
        -f Dockerfile.minion \
        -t "opennms/minion-deltav:$VERSION" \
        -t "opennms/minion-deltav:latest" \
        .

    # --- Stage Minion Boot fat JAR ---
    log "Staging Minion Boot fat JAR..."
    mkdir -p "$SCRIPT_DIR/staging/minion-boot"
    # Copy only the fat JAR (exclude -sources.jar, -javadoc.jar, .original)
    find "$REPO_ROOT/core/daemon-boot-minion/target" \
        -maxdepth 1 -name "*.jar" \
        ! -name "*-sources.jar" ! -name "*-javadoc.jar" ! -name "*.original" \
        -exec cp {} "$SCRIPT_DIR/staging/minion-boot/daemon-boot-minion.jar" \;

    # --- Build Minion Boot image ---
    log "Building opennms/minion-boot:$VERSION..."
    docker build \
        --build-arg "VERSION=$VERSION" \
        -f Dockerfile.minion-boot \
        -t "opennms/minion-boot:$VERSION" \
        -t "opennms/minion-boot:latest" \
        .

    # Clean up staging
    rm -rf "$SCRIPT_DIR/staging"

    log "Delta-V images built:"
    docker images --format "  {{.Repository}}:{{.Tag}}\t{{.Size}}" | grep -E "daemon-base|alarmd|bsmd|collectd|discovery|enlinkd|eventtranslator|perspectivepollerd|pollerd|provisiond|syslogd|telemetryd|trapd|daemon-deltav|minion-deltav|minion-boot" | sort | head -20
}


usage() {
    cat <<'USAGE'
Usage: ./build.sh [command]

Commands:
  (none)    Full build: compile + assemble + images + deltav
  compile   Compile only (Maven)
  assemble  Assemble distributions (Daemon + Alarmd + Minion + Sentinel)
  images    Build base Docker images only (requires prior assembly)
  jre       Build JRE base image (opennms/jre-deltav:21, rarely needed)
  deltav    Build Delta-V layered images (stages JARs into derived images)
  push      Build and push images to registry
  clean     Remove named Docker volumes (fresh start)
  help      Show this help

Environment variables:
  DOCKER_REGISTRY   Registry (default: docker.io)
  DOCKER_ORG        Organization (default: opennms)
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
            do_assemble
            do_images
            if ! docker image inspect opennms/jre-deltav:21 >/dev/null 2>&1; then
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
            if ! docker image inspect opennms/jre-deltav:21 >/dev/null 2>&1; then
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
