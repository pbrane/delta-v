#!/bin/sh
#
# Seeds the smoke-baseline perspective application:
#   - waits for both perspective monitoring locations to register
#   - waits for provisiond to import the requisition backing each perspective
#     service (the service's row landing in `ifservices` is the trigger)
#   - inserts the application + service maps + perspective-location rows
#     idempotently, via `ON CONFLICT` clauses
#
# PerspectivePollerd's PerspectiveServiceTracker timer-polls applications
# every 5s, so it picks up the new mapping without a daemon restart.
#
# Required env: PGHOST, PGUSER, PGPASSWORD, PGDATABASE.

set -eu

APP_NAME="${PERSPECTIVE_APP_NAME:-Devices-API-Perspective-App}"
LOCATION_A="${PERSPECTIVE_LOCATION_A:-Default}"
LOCATION_B="${PERSPECTIVE_LOCATION_B:-nl6-lab}"
WAIT_TIMEOUT="${PERSPECTIVE_WAIT_TIMEOUT:-300}"
POLL_INTERVAL=5

# Services to perspective-poll, as a space-separated list of
# "<foreign-source>/<service-name>" pairs. Each is mapped into APP_NAME and
# therefore polled from every perspective location listed above.
PERSPECTIVE_SERVICES="${PERSPECTIVE_SERVICES:-perspective-smoke/HTTP-8080 perspective-test/Google-Search}"

# Log to stderr so wait_for_ifservice's stdout carries only the id it echoes.
log() { echo "[perspective-app-init] $*" >&2; }

psql_q() {
    psql -h "$PGHOST" -U "$PGUSER" -d "$PGDATABASE" -t -A -c "$1"
}

# Wait for a service's ifservices row to appear -- this proves provisiond has
# finished importing the requisition that carries it -- then echo its id.
# Args: <foreign-source> <service-name>
wait_for_ifservice() {
    fs="$1"
    svc="$2"
    log "Waiting for ${fs}/${svc} to appear in ifservices (timeout ${WAIT_TIMEOUT}s)..."
    deadline=$(( $(date +%s) + WAIT_TIMEOUT ))
    id=""
    while [ -z "$id" ]; do
        id=$(psql_q "
            SELECT s.id
              FROM ifservices s
              JOIN ipinterface ip ON s.ipinterfaceid = ip.id
              JOIN node n         ON ip.nodeid = n.nodeid
              JOIN service st     ON s.serviceid = st.serviceid
             WHERE n.foreignsource = '${fs}'
               AND st.servicename  = '${svc}'
             LIMIT 1
        " 2>/dev/null || echo "")
        if [ -z "$id" ]; then
            if [ "$(date +%s)" -gt "$deadline" ]; then
                log "FAIL: ${svc} on ${fs} did not appear in ifservices within ${WAIT_TIMEOUT}s"
                log "      check provisiond logs: docker logs delta-v-provisiond"
                exit 1
            fi
            sleep "$POLL_INTERVAL"
        fi
    done
    log "Found ifservice id=${id} for ${fs}/${svc}"
    echo "$id"
}

# 1. Wait for both perspective monitoring locations to exist. Default is
#    present from db-init; nl6-lab only appears once provisiond has
#    imported the nl6-lab requisition, whose nodes are scanned through
#    Minion RPC -- on a cold start that lags well behind provisiond becoming
#    healthy, so this needs a retry loop just like the ifservices waits.
log "Waiting for monitoring locations ${LOCATION_A}, ${LOCATION_B} (timeout ${WAIT_TIMEOUT}s)..."
deadline=$(( $(date +%s) + WAIT_TIMEOUT ))
loc_count=0
while [ "${loc_count:-0}" -ne 2 ]; do
    loc_count=$(psql_q "
        SELECT count(*) FROM monitoringlocations WHERE id IN ('${LOCATION_A}', '${LOCATION_B}')
    " 2>/dev/null || echo 0)
    if [ "${loc_count:-0}" -ne 2 ]; then
        if [ "$(date +%s)" -gt "$deadline" ]; then
            log "FAIL: expected both monitoring locations (${LOCATION_A}, ${LOCATION_B}); found ${loc_count} within ${WAIT_TIMEOUT}s"
            log "      ensure nl6-minion is running and has registered with the gateway"
            exit 1
        fi
        sleep "$POLL_INTERVAL"
    fi
done
log "Both monitoring locations present: ${LOCATION_A}, ${LOCATION_B}"

# 2. Idempotently provision the application.
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

# 3. Wait for each perspective service to import, then map it into the app.
for pair in $PERSPECTIVE_SERVICES; do
    fs=${pair%/*}
    svc=${pair#*/}
    ifservice_id=$(wait_for_ifservice "$fs" "$svc")
    psql_q "
        INSERT INTO application_service_map (appid, ifserviceid)
        VALUES (${app_id}, ${ifservice_id})
        ON CONFLICT DO NOTHING;
    " >/dev/null
    log "Service ${svc} (${fs}) mapped to application"
done

# 4. Map the perspective monitoring locations onto the application. Every
#    mapped service is then polled from each of these perspectives.
for loc in "$LOCATION_A" "$LOCATION_B"; do
    psql_q "
        INSERT INTO application_perspective_location_map (appid, monitoringlocationid)
        VALUES (${app_id}, '${loc}')
        ON CONFLICT DO NOTHING;
    " >/dev/null
done
log "Perspective locations mapped: ${LOCATION_A}, ${LOCATION_B}"

log "Done — PerspectivePollerd tracker (5s polling timer) will pick up the application within seconds."
