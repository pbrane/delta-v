#!/usr/bin/env bash
# compute-shared-libs.sh — Extract Spring Boot fat JARs and deduplicate shared dependencies.
#
# Produces a staging/ directory with:
#   shared-external/   — 3rd-party JARs present in ALL 12 daemons
#   shared-internal/   — org.opennms project JARs present in ALL 12 daemons
#   priority/          — model-jakarta + dao-jpa-support (classpath-first)
#   <daemon>/libs/     — JARs unique to this daemon
#   <daemon>/app/      — thin application JAR (classes + resources only)
#   <daemon>/.main_class — fully-qualified Start-Class from MANIFEST.MF
#
# Portability: written for bash 3.2 (the frozen macOS /bin/bash). No
# associative arrays (`declare -A`), no `${var^^}`, no `mapfile`.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

if [ $# -ne 2 ]; then
    echo "Usage: $0 <repo-root> <version>"
    echo "  e.g. $0 /path/to/delta-v 36.0.0-SNAPSHOT"
    exit 1
fi

REPO_ROOT="$1"
VERSION="$2"

# The 12 horizon-derived daemons, sorted, space-separated (indexed iteration only).
DAEMONS="alarmd bsmd collectd discovery enlinkd eventtranslator perspectivepollerd pollerd provisiond syslogd telemetryd trapd"
DAEMON_COUNT=$(printf '%s\n' $DAEMONS | wc -l | tr -d ' ')

# Derive the fat-JAR path base for a daemon (uniform module layout).
jar_base() {
    echo "${REPO_ROOT}/core/daemon-boot-$1/target/org.opennms.core.daemon-boot-$1-${VERSION}"
}

# Echo the resolved fat JAR (prefer plain .jar, fall back to -boot.jar). Empty if none.
resolve_fat_jar() {
    local base; base="$(jar_base "$1")"
    if [ -f "${base}.jar" ]; then
        echo "${base}.jar"
    elif [ -f "${base}-boot.jar" ]; then
        echo "${base}-boot.jar"
    else
        echo ""
    fi
}

echo "=== compute-shared-libs.sh ==="
echo "Repo root : ${REPO_ROOT}"
echo "Version   : ${VERSION}"
echo "Daemons   : ${DAEMON_COUNT}"
echo ""

# Verify all fat JARs exist up front.
missing=0
for daemon in $DAEMONS; do
    if [ -z "$(resolve_fat_jar "$daemon")" ]; then
        echo "ERROR: No fat JAR found for ${daemon}"
        echo "  Tried: $(jar_base "$daemon").jar"
        echo "  Tried: $(jar_base "$daemon")-boot.jar"
        missing=1
    fi
done
if [ "$missing" -ne 0 ]; then
    echo "Aborting — missing fat JARs."
    exit 1
fi

STAGING="${SCRIPT_DIR}/staging"
EXTRACT_DIR="${SCRIPT_DIR}/.extract-tmp"
rm -rf "${STAGING}" "${EXTRACT_DIR}"
mkdir -p "${STAGING}/shared-external" "${STAGING}/shared-internal" "${STAGING}/priority"
mkdir -p "${EXTRACT_DIR}"

# --- Step 1: Extract all fat JARs ---
echo "--- Extracting fat JARs ---"
for daemon in $DAEMONS; do
    jar="$(resolve_fat_jar "$daemon")"
    dest="${EXTRACT_DIR}/${daemon}"
    echo "  ${daemon}: $(basename "${jar}")"
    if ! java -Djarmode=tools -jar "${jar}" extract --destination "${dest}" 2>/dev/null; then
        echo "    jarmode extraction failed, falling back to manual extract..."
        mkdir -p "${dest}/lib"
        tmpdir=$(mktemp -d)
        ( cd "$tmpdir" && jar xf "${jar}" )
        mv "$tmpdir"/BOOT-INF/lib/* "${dest}/lib/" 2>/dev/null || true
        ( cd "$tmpdir/BOOT-INF/classes" && jar cf "${dest}/$(basename "${jar}")" . )
        cp -r "$tmpdir/META-INF" "${dest}/META-INF" 2>/dev/null || true
        rm -rf "$tmpdir"
    fi
done
echo ""

# --- Step 2: Extract Start-Class from each MANIFEST.MF ---
echo "--- Extracting Start-Class ---"
for daemon in $DAEMONS; do
    jar="$(resolve_fat_jar "$daemon")"
    main_class=$(unzip -p "${jar}" META-INF/MANIFEST.MF \
        | tr -d '\r' \
        | sed -e ':a' -e 'N' -e '$!ba' -e 's/\n //g' \
        | grep "^Start-Class:" \
        | sed 's/^Start-Class: *//')
    if [ -z "${main_class}" ]; then
        echo "ERROR: No Start-Class found in ${jar}"
        exit 1
    fi
    mkdir -p "${STAGING}/${daemon}"
    echo "${main_class}" > "${STAGING}/${daemon}/.main_class"
    echo "  ${daemon}: ${main_class}"
done
echo ""

# --- Step 3: Compute strict intersection of lib/ dirs via sort|uniq -c ---
echo "--- Computing shared libraries (strict intersection of all ${DAEMON_COUNT} daemons) ---"
ALL_NAMES="${EXTRACT_DIR}/.all-names.txt"   # one jar basename per daemon-occurrence
SHARED_FILE="${EXTRACT_DIR}/.shared.txt"    # names present in all DAEMON_COUNT daemons
: > "${ALL_NAMES}"
for daemon in $DAEMONS; do
    lib_dir="${EXTRACT_DIR}/${daemon}/lib"
    if [ ! -d "${lib_dir}" ]; then
        echo "ERROR: No lib/ directory found for ${daemon} at ${lib_dir}"
        exit 1
    fi
    # De-dupe within a single daemon (a name counts once per daemon), then append.
    for jar_file in "${lib_dir}"/*.jar; do
        basename "${jar_file}"
    done | sort -u >> "${ALL_NAMES}"
done
total_unique=$(sort -u "${ALL_NAMES}" | wc -l | tr -d ' ')
# A name shared by all daemons appears exactly DAEMON_COUNT times in ALL_NAMES.
sort "${ALL_NAMES}" | uniq -c \
    | awk -v n="${DAEMON_COUNT}" '$1 == n { $1=""; sub(/^ /,""); print }' \
    | sort > "${SHARED_FILE}"
shared_total=$(wc -l < "${SHARED_FILE}" | tr -d ' ')
echo "  Total unique JAR filenames across all daemons: ${total_unique}"
echo "  Shared across all ${DAEMON_COUNT} daemons: ${shared_total}"
echo ""

# Membership helper: is this jar name shared?
is_shared() { grep -Fxq "$1" "${SHARED_FILE}"; }

# --- Step 4: Partition shared libs into external vs internal ---
echo "--- Partitioning shared libs into external / internal ---"
first_daemon=$(printf '%s\n' $DAEMONS | head -1)
first_lib="${EXTRACT_DIR}/${first_daemon}/lib"
shared_external_count=0
shared_internal_count=0
while IFS= read -r name; do
    [ -n "$name" ] || continue
    case "$name" in
        org.opennms.*|opennms-*)
            cp "${first_lib}/${name}" "${STAGING}/shared-internal/"
            shared_internal_count=$(( shared_internal_count + 1 )) ;;
        *)
            cp "${first_lib}/${name}" "${STAGING}/shared-external/"
            shared_external_count=$(( shared_external_count + 1 )) ;;
    esac
done < "${SHARED_FILE}"
echo "  shared-external: ${shared_external_count} JARs"
echo "  shared-internal: ${shared_internal_count} JARs"
echo ""

# --- Step 4b: Move model-jakarta + dao-jpa-support to priority classpath ---
echo "--- Moving model-jakarta to priority classpath ---"
priority_count=0
for jar_file in "${STAGING}/shared-internal"/org.opennms.core.model-jakarta-*.jar \
                "${STAGING}/shared-internal"/org.opennms.core.dao-jpa-support-*.jar; do
    if [ -f "${jar_file}" ]; then
        mv "${jar_file}" "${STAGING}/priority/"
        echo "  moved: $(basename "${jar_file}")"
        priority_count=$(( priority_count + 1 ))
    fi
done
echo "  priority: ${priority_count} JARs"
echo ""

# --- Step 5: Stage per-daemon unique libs and thin app JAR ---
echo "--- Staging per-daemon unique libs and app JARs ---"
for daemon in $DAEMONS; do
    lib_dir="${EXTRACT_DIR}/${daemon}/lib"
    daemon_libs_dir="${STAGING}/${daemon}/libs"
    daemon_app_dir="${STAGING}/${daemon}/app"
    mkdir -p "${daemon_libs_dir}" "${daemon_app_dir}"
    unique_count=0
    for jar_file in "${lib_dir}"/*.jar; do
        name="$(basename "${jar_file}")"
        if ! is_shared "${name}"; then
            cp "${jar_file}" "${daemon_libs_dir}/"
            unique_count=$(( unique_count + 1 ))
        fi
    done
    thin_jar_count=0
    for f in "${EXTRACT_DIR}/${daemon}"/*.jar; do
        cp "${f}" "${daemon_app_dir}/"
        thin_jar_count=$(( thin_jar_count + 1 ))
    done
    echo "  ${daemon}: ${unique_count} unique libs, ${thin_jar_count} app JAR(s)"
done
echo ""

# --- Step 6: Safety check — no JAR in both shared and per-daemon ---
echo "--- Safety check: no overlap between shared and per-daemon ---"
overlap_found=0
for daemon in $DAEMONS; do
    for jar_file in "${STAGING}/${daemon}/libs"/*.jar; do
        [ -e "${jar_file}" ] || continue
        name="$(basename "${jar_file}")"
        if is_shared "${name}"; then
            echo "  OVERLAP: ${name} in both shared and ${daemon}/libs/"
            overlap_found=1
        fi
    done
done
if [ "${overlap_found}" -eq 1 ]; then
    echo "ERROR: Overlap detected — aborting."
    rm -rf "${EXTRACT_DIR}"
    exit 1
fi
echo "  OK — no overlaps."
echo ""

rm -rf "${EXTRACT_DIR}"

echo "==========================================="
echo "  SUMMARY"
echo "==========================================="
echo "  priority        : ${priority_count} JARs (model-jakarta, loaded first)"
echo "  shared-external : ${shared_external_count} JARs"
echo "  shared-internal : $(( shared_internal_count - priority_count )) JARs"
echo "  shared total    : ${shared_total} JARs"
echo ""
printf "  %-22s %s\n" "DAEMON" "UNIQUE LIBS"
printf "  %-22s %s\n" "------" "-----------"
for daemon in $DAEMONS; do
    count=$(ls -1 "${STAGING}/${daemon}/libs/"*.jar 2>/dev/null | wc -l | tr -d ' ')
    main_class=$(cat "${STAGING}/${daemon}/.main_class")
    printf "  %-22s %3s   %s\n" "${daemon}" "${count}" "${main_class}"
done
echo ""
echo "Staging directory: ${STAGING}"
echo "Done."
