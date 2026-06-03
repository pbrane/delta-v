#!/usr/bin/env bash
#
# deploy.sh — Deploy and manage OpenNMS Delta-V
#
# Usage:
#   ./deploy.sh up [profile] Start services (profiles: active, passive, full, demo)
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
    if [ -z "${VERSION:-}" ]; then
        err "VERSION is empty (no .env, or VERSION unset). Run: cp .env.example .env  (docker compose would otherwise default to ':latest' and pull nonexistent images)."
    fi
    log "Starting Delta-V (version $VERSION)..."

    # Check a sample daemon image exists (Delta-V layered images)
    for img in "$IMAGE_PREFIX/trapd:$VERSION" "$IMAGE_PREFIX/minion-boot:$VERSION"; do
        docker image inspect "$img" >/dev/null 2>&1 || err "Image $img not found. Run 'make images' first, or set IMAGE_PREFIX in .env to a registry prefix you've pulled from (e.g. ghcr.io/pbrane)."
    done

    local profile="${1:-}"

    # The 'demo' profile mirrors the smoke-VM orchestration: full stack +
    # observability, with the lean JVM override (docker-compose.dev.yml) layered
    # on top — sizing heaps/GC/thread-stacks for resource-constrained lab/demo
    # hosts. Other profiles keep the production-shaped JVM defaults.
    local -a compose_files
    compose_files=(-f docker-compose.yml)
    if [ "$profile" = "demo" ]; then
        compose_files+=(-f docker-compose.dev.yml)
        log "Demo: layering lean JVM override (docker-compose.dev.yml)"
    fi

    if [ -n "$profile" ]; then
        log "Using profile: $profile"
        COMPOSE_PROFILES="$profile" docker compose "${compose_files[@]}" up -d
    else
        log "Starting infrastructure only — no daemons (postgres, kafka, minion, minion-gateway, envoy, db-init, snmp-agent)."
        log "  The 12 daemons are profile-gated. For the full stack:  make up PROFILE=full   (active | passive | demo also available)"
        docker compose "${compose_files[@]}" up -d
    fi

    log "Waiting for services to start..."
    log "Run 'make status' to check progress."
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
    log "Clean slate. Run 'make up' to start fresh."
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

# Verify the live ClickHouse flows_raw schema is not BEHIND the source DDL.
# A stale clickhouse-init image (its baked DDL never carried newer columns) or a
# clickhouse-init that never re-ran leaves flows_raw missing columns that the
# flows dashboards/queries reference — a silent failure. This converts it into a
# loud one by diffing the expected columns (parsed from clickhouse/init/02-flows-raw.sql:
# the CREATE TABLE block + every ADD COLUMN IF NOT EXISTS migration) against the
# live system.columns. Exit: 0 current, 1 drift/missing, 2 skipped (no clickhouse).
check_clickhouse_schema() {
    local ddl="clickhouse/init/02-flows-raw.sql"

    # Skip when clickhouse is not part of this profile/run (e.g. passive).
    if ! docker compose ps --status running --format '{{.Name}}' 2>/dev/null | grep -qw clickhouse; then
        return 2
    fi
    if [ ! -f "$ddl" ]; then
        log "  [FAIL] ClickHouse schema check: source DDL $ddl not found"
        return 1
    fi

    local expected live missing
    expected=$(awk '
        /CREATE TABLE IF NOT EXISTS deltav\.flows_raw/ {inblk=1; next}
        inblk && /^\)/ {inblk=0}
        inblk && $1 ~ /^[a-z_][a-z0-9_]*$/ {print $1}
        /ADD COLUMN IF NOT EXISTS/ && !/^[[:space:]]*--/ {for (i=1;i<=NF;i++) if ($i=="EXISTS") print $(i+1)}
    ' "$ddl" | sort -u)
    live=$(docker compose exec -T clickhouse clickhouse-client \
            --user "${CLICKHOUSE_USER:-deltav}" --password "${CLICKHOUSE_PASSWORD:-deltav}" \
            -q "SELECT name FROM system.columns WHERE database='deltav' AND table='flows_raw'" 2>/dev/null | sort -u)

    if [ -z "$live" ]; then
        log "  [FAIL] ClickHouse schema: deltav.flows_raw has no columns (clickhouse-init has not applied DDL yet?)"
        return 1
    fi

    missing=$(comm -23 <(printf '%s\n' "$expected") <(printf '%s\n' "$live"))
    if [ -n "$missing" ]; then
        log "  [FAIL] ClickHouse schema is BEHIND source DDL — deltav.flows_raw is missing columns:"
        printf '             %s\n' $missing
        echo   "         The clickhouse-init image is stale (baked DDL predates these columns)."
        echo   "         Rebuild it from source and recreate the sidecar:"
        echo   "           make images && docker compose up -d --force-recreate clickhouse-init"
        return 1
    fi
    log "  [PASS] ClickHouse schema current (deltav.flows_raw matches source DDL)"
    return 0
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
    if docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null | grep -qiE 'deltav[.-]'; then
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

    # Test 5: ClickHouse schema currency (stale clickhouse-init detection).
    # '|| sc=$?' keeps the non-zero return from aborting under set -e and lets
    # check_clickhouse_schema print its own [PASS]/[FAIL] line to the terminal.
    local sc=0
    check_clickhouse_schema || sc=$?
    case "$sc" in
        0) pass=$((pass + 1)) ;;
        2) log "  [SKIP] ClickHouse schema check (clickhouse not in this profile)" ;;
        *) fail=$((fail + 1)) ;;
    esac

    log ""
    log "Results: $pass passed, $fail failed"
    [ "$fail" -eq 0 ] && return 0 || return 1
}

usage() {
    cat <<'USAGE'
Usage: ./deploy.sh <command> [args]

Commands:
  up [profile]    Start services (profiles: active, passive, full, demo)
  down            Stop services (preserve data volumes)
  reset           Stop and destroy all data (clean slate)
  status          Show service status
  logs [service]  Tail logs (all or specific service)
  test            Run deployment verification tests
  test-e2e        Run end-to-end trap-to-alarm integration test
  help            Show this help

Profiles:
  (none)    Infrastructure only: postgres + kafka + minion + minion-gateway + envoy + db-init + snmp-agent
  active    + core daemons (alarmd, pollerd, collectd, provisiond, bsmd) + flow stack
              (clickhouse, flow-enricher, nl6 — emits netflow9/netflow5/sflow/ipfix)
  passive   + trap/syslog receivers (alarmd, trapd, syslogd, discovery, eventtranslator, provisiond)
  full      All daemons (active + passive + enlinkd + perspectivepollerd + telemetryd)
  demo      full + observability: victoriametrics + vmagent + prometheus-writer + grafana
              + alertmanager + alerts-forwarder (metrics, dashboards, and alerting)

Examples:
  ./deploy.sh up                    # Infrastructure only
  ./deploy.sh up full               # Start everything
  ./deploy.sh up passive            # Trap/syslog receivers with alarmd
  ./deploy.sh up active             # Core daemons + flow stack
  ./deploy.sh up demo               # Everything + metrics/dashboards/alerting
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
