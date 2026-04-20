#!/bin/sh
# Provisioner script: read devices.json, POST each entry to l8opensim's
# REST API. Idempotent: re-POSTing existing IPs returns the existing device.
#
# Args:
#   $1 = base URL of l8opensim REST API (e.g. http://127.0.0.1:8080)
#
# Reads JSON from stdin (each line is one JSON object -- caller pre-splits).
set -eu
base_url="$1"
created=0
failed=0
while IFS= read -r entry || [ -n "$entry" ]; do
    [ -z "$entry" ] && continue
    if curl -sf -X POST "$base_url/api/v1/devices" \
            -H "Content-Type: application/json" \
            -d "$entry" > /dev/null; then
        created=$((created + 1))
    else
        failed=$((failed + 1))
        echo "FAILED to POST: $entry" >&2
    fi
done
echo "post-each: created=$created failed=$failed"
[ "$failed" -eq 0 ] || exit 1
