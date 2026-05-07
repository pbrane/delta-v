#!/bin/sh
#
# Seeds the smoke-baseline perspective application:
#   - waits for provisiond to import the `perspective-smoke` requisition
#     (the HTTP-8080 service row landing in `ifservices` is the trigger)
#   - inserts the application + service map + two perspective-location rows
#     (Default + l8opensim-lab) idempotently, via `ON CONFLICT` clauses
#
# PerspectivePollerd's PerspectiveServiceTracker timer-polls applications
# every 5s, so it picks up the new mapping without a daemon restart.
#
# Required env: PGHOST, PGUSER, PGPASSWORD, PGDATABASE.

set -eu

APP_NAME="${PERSPECTIVE_APP_NAME:-Devices-API-Perspective-App}"
FOREIGN_SOURCE="${PERSPECTIVE_FOREIGN_SOURCE:-perspective-smoke}"
SERVICE_NAME="${PERSPECTIVE_SERVICE_NAME:-HTTP-8080}"
LOCATION_A="${PERSPECTIVE_LOCATION_A:-Default}"
LOCATION_B="${PERSPECTIVE_LOCATION_B:-l8opensim-lab}"
WAIT_TIMEOUT="${PERSPECTIVE_WAIT_TIMEOUT:-300}"
POLL_INTERVAL=5

log() { echo "[perspective-app-init] $*"; }

psql_q() {
    psql -h "$PGHOST" -U "$PGUSER" -d "$PGDATABASE" -t -A -c "$1"
}

# 1. Wait for the perspective-smoke service row to land in ifservices.
#    This proves provisiond has finished importing the requisition.
log "Waiting for ${FOREIGN_SOURCE}/${SERVICE_NAME} to appear in ifservices (timeout ${WAIT_TIMEOUT}s)..."
deadline=$(( $(date +%s) + WAIT_TIMEOUT ))
ifservice_id=""
while [ -z "$ifservice_id" ]; do
    if [ "$(date +%s)" -gt "$deadline" ]; then
        log "FAIL: ${SERVICE_NAME} on ${FOREIGN_SOURCE} did not appear in ifservices within ${WAIT_TIMEOUT}s"
        log "      check provisiond logs: docker logs delta-v-provisiond"
        exit 1
    fi
    ifservice_id=$(psql_q "
        SELECT s.id
          FROM ifservices s
          JOIN ipinterface ip ON s.ipinterfaceid = ip.id
          JOIN node n         ON ip.nodeid = n.nodeid
          JOIN service st     ON s.serviceid = st.serviceid
         WHERE n.foreignsource = '${FOREIGN_SOURCE}'
           AND st.servicename  = '${SERVICE_NAME}'
         LIMIT 1
    " 2>/dev/null || echo "")
    if [ -z "$ifservice_id" ]; then
        sleep "$POLL_INTERVAL"
    fi
done
log "Found ifservice id=${ifservice_id} for ${SERVICE_NAME}"

# 2. Verify both perspective monitoring locations exist (Default is always
#    present from db-init; l8opensim-lab appears once minion-lab registers).
loc_count=$(psql_q "
    SELECT count(*) FROM monitoringlocations WHERE id IN ('${LOCATION_A}', '${LOCATION_B}')
")
if [ "${loc_count:-0}" -ne 2 ]; then
    log "FAIL: expected both monitoring locations (${LOCATION_A}, ${LOCATION_B}); found ${loc_count}"
    log "      ensure minion-lab is running and has registered with the gateway"
    exit 1
fi
log "Both monitoring locations present: ${LOCATION_A}, ${LOCATION_B}"

# 3. Idempotently provision application + mappings.
psql_q "
    INSERT INTO applications (id, name)
    VALUES (nextval('opennmsnxtid'), '${APP_NAME}')
    ON CONFLICT (name) DO NOTHING;
" >/dev/null
app_id=$(psql_q "SELECT id FROM applications WHERE name = '${APP_NAME}'")
if [ -z "$app_id" ]; then
    log "FAIL: could not resolve application id for ${APP_NAME}"
    exit 1
fi
log "Application ${APP_NAME} ready (id=${app_id})"

psql_q "
    INSERT INTO application_service_map (appid, ifserviceid)
    VALUES (${app_id}, ${ifservice_id})
    ON CONFLICT DO NOTHING;
" >/dev/null
log "Service ${SERVICE_NAME} mapped to application"

for loc in "$LOCATION_A" "$LOCATION_B"; do
    psql_q "
        INSERT INTO application_perspective_location_map (appid, monitoringlocationid)
        VALUES (${app_id}, '${loc}')
        ON CONFLICT DO NOTHING;
    " >/dev/null
done
log "Perspective locations mapped: ${LOCATION_A}, ${LOCATION_B}"

log "Done — PerspectivePollerd tracker (5s polling timer) will pick up the application within seconds."
