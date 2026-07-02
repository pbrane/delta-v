#!/usr/bin/env bash
# Copyright 2026 Ronny Trommer <ronny@no42.org>
# SPDX-License-Identifier: Apache-2.0
#
# Standalone test for tools/build.sh stage_shared_snmp_config().
# Drift guard: the committed shared snmp-config.xml must fan out byte-identically
# into every SNMP-consuming daemon overlay. Run: bash tools/stage-shared-snmp-config.test.sh
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PASS=0; FAIL=0
check() { # desc expected actual
    if [ "$2" = "$3" ]; then echo "  PASS: $1"; PASS=$((PASS+1));
    else echo "  FAIL: $1 (expected='$2' actual='$3')"; FAIL=$((FAIL+1)); fi
}

# Source build.sh for its functions only (the BASH_SOURCE guard skips version
# resolution and main, so sourcing has no side effects).
# shellcheck disable=SC1091
. "$HERE/build.sh"
# build.sh enables `set -euo pipefail`; this test handles non-zero exits itself
# (it deliberately triggers a failure path), so relax -e/pipefail but keep -u.
set +e +o pipefail

check "SNMP_CONSUMERS list" "collectd pollerd provisiond enlinkd perspectivepollerd" "$SNMP_CONSUMERS"

# Stage the real committed shared source into an isolated temp overlay tree.
SRC="$HERE/../deploy/overlays/shared/etc/snmp-config.xml"
check "shared source exists" "yes" "$([ -f "$SRC" ] && echo yes || echo no)"

TMP="$(mktemp -d)"
mkdir -p "$TMP/overlays/shared/etc"
cp "$SRC" "$TMP/overlays/shared/etc/snmp-config.xml"
DEPLOY_DIR="$TMP" stage_shared_snmp_config >/dev/null
DEPLOY_DIR="$TMP" stage_shared_snmp_config >/dev/null   # idempotent: run twice

for name in $SNMP_CONSUMERS; do
    if cmp -s "$SRC" "$TMP/overlays/$name/etc/snmp-config.xml"; then
        check "staged $name byte-identical" "ok" "ok"
    else
        check "staged $name byte-identical" "ok" "differs-or-missing"
    fi
done

# Fail-fast when the shared source is absent. Run in a subshell because err()
# calls exit, which would otherwise terminate this test process.
EMPTY="$(mktemp -d)"
( DEPLOY_DIR="$EMPTY" stage_shared_snmp_config ) >/dev/null 2>&1
check "missing source fails fast" "fail" "$([ $? -ne 0 ] && echo fail || echo passed)"

rm -rf "$TMP" "$EMPTY"

echo "── $PASS passed, $FAIL failed ──"
[ "$FAIL" -eq 0 ]
