#!/usr/bin/env bash
#
# deploy.sh — Deploy and manage OpenNMS Delta-V
#
# Usage:
#   ./deploy.sh up [profile] Start services (profiles: lite, passive, full)
#   ./deploy.sh down        Stop all services (preserve data)
#   ./deploy.sh reset       Stop and remove all data
#   ./deploy.sh status      Show service status
#   ./deploy.sh logs [svc]  Tail logs (optionally for a specific service)
#   ./deploy.sh test        Verify deployment is working
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Source .env so IMAGE_PREFIX and VERSION are available to both this script
# and every `docker compose` child invocation below.
if [ -f .env ]; then
    set -a
    # shellcheck disable=SC1091
    . ./.env
    set +a
fi
IMAGE_PREFIX="${IMAGE_PREFIX:-deltav}"

log() { echo "==> $*"; }
err() { echo "ERROR: $*" >&2; exit 1; }

do_up() {
    log "Starting Delta-V (version $VERSION)..."

    # Check a sample daemon image exists (Delta-V layered images)
    for img in "$IMAGE_PREFIX/trapd:$VERSION" "$IMAGE_PREFIX/minion-boot:$VERSION"; do
        docker image inspect "$img" >/dev/null 2>&1 || err "Image $img not found. Run ./build.sh deltav first, or set IMAGE_PREFIX in .env to a registry prefix you've pulled from (e.g. ghcr.io/pbrane)."
    done

    local profile="${1:-}"
    if [ -n "$profile" ]; then
        log "Using profile: $profile"
        COMPOSE_PROFILES="$profile" docker compose up -d
    else
        log "Starting infrastructure only (postgres + kafka + db-init + minion + snmp-agent)"
        docker compose up -d
    fi

    log "Waiting for services to start..."
    log "Run './deploy.sh status' to check progress."
}

do_down() {
    log "Stopping Delta-V..."
    docker compose down
}

do_reset() {
    log "Stopping Delta-V and removing all data volumes..."
    docker compose down --remove-orphans 2>/dev/null || true
    # docker compose down -v only removes volumes for active profile services.
    # Explicitly remove ALL delta-v volumes to ensure clean data state
    # (Liquibase schema, Kafka offsets, ClickHouse tables, etc.).
    local stale_vols
    stale_vols=$(docker volume ls --format '{{.Name}}' | grep "^delta-v_" || true)
    if [ -n "$stale_vols" ]; then
        echo "$stale_vols" | xargs docker volume rm 2>/dev/null || true
    fi
    log "Clean slate. Run './deploy.sh up' to start fresh."
}

do_status() {
    docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"
}

do_logs() {
    local service="${1:-}"
    if [ -n "$service" ]; then
        docker compose logs "$service" --tail=100 -f
    else
        docker compose logs --tail=20 -f
    fi
}

do_test() {
    log "Testing Delta-V deployment..."
    local pass=0
    local fail=0

    # Test 1: Check services are running
    local running
    running=$(docker compose ps --status running --format "{{.Name}}" | wc -l | tr -d ' ')
    if [ "$running" -ge 3 ]; then
        log "  [PASS] $running services running"
        pass=$((pass + 1))
    else
        log "  [FAIL] Only $running services running"
        fail=$((fail + 1))
    fi

    # Test 2: Database accessible
    if docker compose exec -T -e PGPASSWORD=opennms postgres psql -U opennms -d opennms -c "SELECT 1" >/dev/null 2>&1; then
        log "  [PASS] PostgreSQL accessible"
        pass=$((pass + 1))
    else
        log "  [FAIL] PostgreSQL not accessible"
        fail=$((fail + 1))
    fi

    # Test 3: Kafka topic exists
    if docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null | grep -q "opennms"; then
        log "  [PASS] Kafka topics created"
        pass=$((pass + 1))
    else
        log "  [FAIL] Kafka topics not found"
        fail=$((fail + 1))
    fi

    # Test 4: db-init completed successfully. Use --all so we see the
    # exited init container; without --all, `docker compose ps` only shows
    # running containers and the exited db-init is filtered out, producing
    # an empty-string status that the equality check rejects.
    local db_init_status
    db_init_status=$(docker compose ps --all db-init --format "{{.State}}" 2>/dev/null || echo "unknown")
    if [ "$db_init_status" = "exited" ]; then
        log "  [PASS] db-init completed (exited)"
        pass=$((pass + 1))
    else
        log "  [FAIL] db-init status: $db_init_status (expected: exited)"
        fail=$((fail + 1))
    fi

    log ""
    log "Results: $pass passed, $fail failed"
    [ "$fail" -eq 0 ] && return 0 || return 1
}

usage() {
    cat <<'USAGE'
Usage: ./deploy.sh <command> [args]

Commands:
  up [profile]    Start services (profiles: lite, passive, full)
  down            Stop services (preserve data volumes)
  reset           Stop and destroy all data (clean slate)
  status          Show service status
  logs [service]  Tail logs (all or specific service)
  test            Run deployment verification tests
  test-e2e        Run end-to-end trap-to-alarm integration test
  help            Show this help

Profiles:
  (none)    Infrastructure only: postgres + kafka + db-init + minion + snmp-agent
  lite      + essential daemons (alarmd, pollerd, collectd, discovery, provisiond, bsmd,
              eventtranslator, perspectivepollerd, flow-enricher, prometheus-writer)
  passive   + trap/syslog receivers (alarmd, trapd, syslogd, eventtranslator, provisiond)
  full      All ~20 services (lite + enlinkd + telemetryd + clickhouse + flow testnodes + l8opensim)

Examples:
  ./deploy.sh up                    # Infrastructure only
  ./deploy.sh up full               # Start everything
  ./deploy.sh up passive            # Trap/syslog receivers with alarmd
  ./deploy.sh up lite               # Essential daemons
  ./deploy.sh logs alarmd           # Tail alarmd logs
  ./deploy.sh test                  # Verify deployment
  ./deploy.sh test-e2e              # Full trap-to-alarm integration test
  ./deploy.sh test-e2e --verbose    # With Kafka event trace
  ./deploy.sh reset && ./deploy.sh up  # Fresh start
USAGE
}

main() {
    case "${1:-help}" in
        up)      shift; do_up "$@" ;;
        down)    do_down ;;
        reset)   do_reset ;;
        status)  do_status ;;
        logs)    shift; do_logs "$@" ;;
        test)    do_test ;;
        test-e2e) shift; "$SCRIPT_DIR/test-e2e.sh" "$@" ;;
        help|-h|--help) usage ;;
        *)       err "Unknown command: $1 (run './deploy.sh help')" ;;
    esac
}

main "$@"
