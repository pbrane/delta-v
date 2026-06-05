#!/usr/bin/env bash
#
# test-lib.sh -- Shared helper functions for Delta-V E2E tests
#
# Source this file from test scripts: source "$(dirname "$0")/test-lib.sh"
#

# Delete ALL nodes and dependent data from the database (FK-safe order).
# Usage: clean_all_nodes
clean_all_nodes() {
    local ALL_IDS="SELECT nodeid FROM node"
    local count
    count=$(psql_query "SELECT count(*) FROM node" || echo "0")
    if [ "${count:-0}" -eq 0 ]; then
        log "  No nodes to clean"
        return 0
    fi
    log "  Deleting all ${count} nodes and dependent data..."
    psql_query "DELETE FROM outages WHERE nodeid IN (${ALL_IDS})" || true
    psql_query "DELETE FROM ifservices WHERE ipinterfaceid IN (SELECT id FROM ipinterface WHERE nodeid IN (${ALL_IDS}))" || true
    psql_query "DELETE FROM alarms WHERE nodeid IN (${ALL_IDS})" || true
    psql_query "DELETE FROM events WHERE nodeid IN (${ALL_IDS})" || true
    psql_query "UPDATE ipinterface SET snmpinterfaceid = NULL WHERE nodeid IN (${ALL_IDS})" || true
    psql_query "DELETE FROM snmpinterface WHERE nodeid IN (${ALL_IDS})" || true
    psql_query "DELETE FROM ipinterface WHERE nodeid IN (${ALL_IDS})" || true
    psql_query "DELETE FROM lldplink WHERE nodeid IN (${ALL_IDS})" || true
    psql_query "DELETE FROM lldpelement WHERE nodeid IN (${ALL_IDS})" || true
    psql_query "DELETE FROM cdplink WHERE nodeid IN (${ALL_IDS})" || true
    psql_query "DELETE FROM cdpelement WHERE nodeid IN (${ALL_IDS})" || true
    psql_query "DELETE FROM ospflink WHERE nodeid IN (${ALL_IDS})" || true
    psql_query "DELETE FROM ospfelement WHERE nodeid IN (${ALL_IDS})" || true
    psql_query "DELETE FROM isislink WHERE nodeid IN (${ALL_IDS})" || true
    psql_query "DELETE FROM isiselement WHERE nodeid IN (${ALL_IDS})" || true
    psql_query "DELETE FROM ipnettomedia WHERE sourcenodeid IN (${ALL_IDS})" || true
    psql_query "DELETE FROM bridgemaclink WHERE nodeid IN (${ALL_IDS})" || true
    psql_query "DELETE FROM bridgestplink WHERE nodeid IN (${ALL_IDS})" || true
    psql_query "DELETE FROM bridgebridgelink WHERE nodeid IN (${ALL_IDS}) OR designatednodeid IN (${ALL_IDS})" || true
    psql_query "DELETE FROM bridgeelement WHERE nodeid IN (${ALL_IDS})" || true
    psql_query "DELETE FROM node" || true
    log "  Deleted ${count} nodes"
}

# Delete all alarms from the database.
# Usage: clean_all_alarms
clean_all_alarms() {
    local count
    count=$(psql_query "SELECT count(*) FROM alarms" || echo "0")
    if [ "${count:-0}" -gt 0 ]; then
        psql_query "DELETE FROM alarms" || true
        log "  Deleted ${count} alarms"
    fi
}
