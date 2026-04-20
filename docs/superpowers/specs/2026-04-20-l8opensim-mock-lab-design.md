# l8opensim Mock-Lab Integration — Design Spec

**Date:** 2026-04-20
**Branch:** `feat/l8opensim-mock-lab` (off develop tip `7ec759eb8e5`)
**Predecessor PRs:** delta-v#180 (label coverage), delta-v#181 (Grafana bundle), delta-v#182 (E2E unblock)
**PR target:** pbrane/delta-v develop. **NEVER OpenNMS/opennms.**

## Goal

Replace the current limited `mock-snmp-agent`+`flow-exporter`+`sflow-exporter` mock environment with a richer simulated network using [l8opensim](https://github.com/labmonkeys-space/l8opensim), giving Delta-V's compose stack an out-of-the-box demo of 21 monitored devices across two distinct OpenNMS monitoring locations. Operators run `docker compose up`, open Grafana, and see a populated SNMP Overview dashboard with realistic interface throughput, multi-device variety, and proper location-based filtering — without needing any external lab connectivity.

## Scope

### In Scope

- **New `l8opensim` service** in `opennms-container/delta-v/docker-compose.yml` running 20 simulated network devices (4 routers + 5 switches + 2 firewalls + 4 servers + 2 GPU servers + 3 storage) on per-device TUN interfaces at `10.0.0.1` through `10.0.0.20`.
- **New `l8opensim-provisioner`** one-shot init container that POSTs a 20-device mix to l8opensim's REST API after the simulator is healthy.
- **New `minion-lab` service** — second Minion at `location=l8opensim-lab`, sharing l8opensim's network namespace (`network_mode: "container:delta-v-l8opensim"`) so it can reach the TUN-bound device IPs directly. Honors the project's Minion-mandatory tenet: SNMP polls go through this Minion via Kafka RPC, not collectd's JVM.
- **New `l8opensim-lab` requisition** at `provisiond-overlay/etc/imports/l8opensim-lab.xml`, foreign-source `l8opensim-lab`, location `l8opensim-lab`, with 20 nodes matching the simulator IPs.
- **New collectd package** in `collectd-daemon-overlay/etc/collectd-configuration.xml` matching `10.0.0.0/24` with the 30s SNMP interval (inherited from PR #182).
- **mhuot-labs auto-import disabled** — comment out the `<requisition-def import-name="mhuot-labs">` block in `provisiond-configuration.xml` with an explanatory comment marking it as opt-in for LLDP topology testing. The requisition file stays on disk; only the auto-import cron is disabled.
- **Dashboard update** — `snmp-overview.json` gains a `monitoring_location` template variable (display label "Monitoring location"), all 6 panels gain `location=~"$monitoring_location"` filtering, and the "Devices monitored" table grows a "Monitoring location" column.
- **E2E extension** — `test-prometheus-writer-e2e.sh` gains a Step 10 asserting `opennms_mib2_x_interfaces_ifhcinoctets_total{foreign_source="l8opensim-lab"}` returns ≥ 1 series. `POLL_GRACE_SECONDS` may bump from 90 to 180 (empirically determined during plan execution).

### Out of Scope

- **Retiring the existing `mock-snmp-agent` + `flow-exporter` + `sflow-exporter` containers.** They keep running. l8opensim is *additive* — needed because the existing mock-snmp-agent has different MIB coverage (32-bit interface counters + host-resources, no HC counters) than l8opensim (HC counters + system MIB, no interface-errors or host-resources). Both targets together produce richer telemetry than either alone. A future PR can consolidate once l8opensim's coverage is extended (see follow-ups).
- **LLDP-MIB support in l8opensim.** l8opensim doesn't serve LLDP-MIB via SNMP today (only via SSH CLI emulation, which Enlinkd doesn't read). Queued as a future upstream contribution to `labmonkeys-space/l8opensim`.
- **Wiring l8opensim's flow / trap / syslog exporters.** l8opensim ships per-device NetFlow v5/v9, IPFIX, sFlow v5, SNMP traps, and UDP syslog exporters. Cool capabilities; out of scope here. Plans to integrate these would be three separate follow-up PRs (one per protocol family).
- **Renaming mhuot-labs to "labbox" or "external-lldp-lab."** Considered during brainstorming, deferred to keep this PR's diff scoped. The auto-import comment-out is the operator-facing fix.
- **Visual regression test for the Grafana dashboard.** Step 10 asserts data flow; visual verification stays manual per PR #181's pattern.
- **Performance regression / scale test** of 21 monitored devices vs the previous 1-device load. Footprint analysis (~150 MiB simulator RAM at 20 devices, ~3.3 SNMP walks/sec from Minion-lab) confirms the cost is trivial. No benchmark needed.
- **Resource adapters / NodeContext metadata for `device_type` label.** l8opensim exposes a `device_type` field via its REST API but currently doesn't propagate it through SNMP. Adding a `device_type` Prometheus label would require separate provisiond-side metadata work. Out of scope.

## Background

PR #181 shipped a Grafana SNMP Overview dashboard with a "Devices monitored" table panel grouping by `instance, foreign_source, snmp_syscontact, snmp_syslocation`. After PR #181 + #182 landed, the operator's first impression of the dashboard is misleading: the table shows entries for the user's real `mhuot-labs` lab devices (because the user's machine has network connectivity to `172.20.20.x`) but doesn't show the in-compose `snmp-agent-canary` because the existing `mock-snmp-agent` doesn't serve `ifHCInOctets` (the panel queries an HC counter; the mock only has 32-bit equivalents).

This produces three concrete problems for any developer running the stack without a real lab:
1. **Empty-feeling demo:** interface-throughput panels show no rows; "Devices monitored" table shows only what their host network can reach (probably nothing).
2. **mhuot-labs prominence:** the requisition is named after the maintainer's initials and references a personal lab subnet (`172.20.20.0/24`). It's confusing as a default fixture.
3. **No demonstration of multi-location semantics:** the Minion-mandatory tenet means OpenNMS shines when there are multiple monitoring locations with multiple Minions. The default compose has one location (`Default`) with one Minion; the multi-location path is untested.

l8opensim solves (1) and (3): it provides a single container that emulates many SNMP devices with realistic dynamic HC counters, system MIB, multi-protocol (SNMP + SSH + REST), and per-device TUN interfaces for proper IP segregation. Pair it with a second Minion at a new location, and the demo telegraphs "this is a multi-location production-shaped setup" out of the box.

## Architectural verification: collectd does NOT poll SNMP locally

Before settling the multi-location design, the brainstorm session empirically verified the existing collectd dispatch behavior using `tcpdump` on collectd's network namespace during a live stack run:

| Container | UDP port 161 packets in 90s |
|---|---|
| `delta-v-collectd` | **0 packets** |
| `delta-v-minion` | **10 packets** (filled buffer immediately) |

Conclusion: collectd's `LocationAwareSnmpClientRpcImpl` (`CollectdRpcConfiguration.java:104`) IS dispatching SNMP walks to Minion via Kafka RPC. The `force-remote=false` setting in `application.yml` controls the Collect *orchestration* RPC (parsing collection config, building CollectionSet) — not the individual SNMP walk RPC. The Minion-mandatory tenet (`project_minion_mandatory`) is honored. Adding a second Minion at a new location is the architecturally-correct way to extend the demo; in-compose collectd-direct polling is not happening today.

## Design

### 1. Service topology

Five compose-level changes:

| Change | Service | Notes |
|---|---|---|
| ADD | `l8opensim` | `ghcr.io/labmonkeys-space/l8opensim:latest`, no auto-create, REST API exposed on host port 19081, simulated-device SNMP on host 19161 (for direct ad-hoc testing) |
| ADD | `l8opensim-provisioner` | One-shot `curlimages/curl:8.10.1` that POSTs the 20-device JSON spec to `localhost:8080/api/v1/devices` |
| ADD | `minion-lab` | Second Minion, `network_mode: "container:delta-v-l8opensim"`, `MINION_LOCATION=l8opensim-lab` |
| EDIT | `mhuot-labs` requisition-def | Comment out the `<requisition-def import-name="mhuot-labs">` XML block in `provisiond-configuration.xml`; add explanatory comment for opt-in LLDP testing |
| ADD | `l8opensim-lab` requisition-def | New `<requisition-def import-name="l8opensim-lab">` block pointing at the new requisition XML, same cron schedule as other auto-imports |

Existing services (`snmp-agent`, the original `minion`, `pollerd`, `collectd`, etc.) unchanged.

### 2. Compose details

**`l8opensim` service** (insert after the existing `snmp-agent` service block):

```yaml
  l8opensim:
    image: ghcr.io/labmonkeys-space/l8opensim:latest
    container_name: delta-v-l8opensim
    profiles: [lite, full, metrics]
    cap_add:
      - NET_ADMIN
      - SYS_ADMIN
    devices:
      - /dev/net/tun
    # Start with no devices; the provisioner POSTs the 20-device mix.
    # `-no-namespace` makes TUN interfaces live in the container root netns,
    # which the shared-netns Minion-lab needs to reach them on `simN`.
    command: ["-no-namespace"]
    healthcheck:
      # 127.0.0.1 not localhost — BusyBox wget IPv6 quirk per project memory.
      test: ["CMD", "wget", "-q", "-O-", "http://127.0.0.1:8080/health"]
      interval: 5s
      timeout: 3s
      retries: 12
      start_period: 5s
    ports:
      - "19161:161/udp"  # ad-hoc SNMP testing from host
      - "19081:8080/tcp" # web UI + REST API for ops
```

**`l8opensim-provisioner` service** (immediately after `l8opensim`):

```yaml
  l8opensim-provisioner:
    image: curlimages/curl:8.10.1
    container_name: delta-v-l8opensim-provisioner
    profiles: [lite, full, metrics]
    depends_on:
      l8opensim:
        condition: service_healthy
    network_mode: "container:delta-v-l8opensim"
    restart: "no"
    entrypoint: ["sh", "-c"]
    command:
      - |
        set -eu
        echo "provisioner: creating 20 devices"
        # devices.json is a JSON array. Iterate and POST each spec.
        cat /seed/devices.json | sh /seed/post-each.sh http://127.0.0.1:8080
        count=$(curl -sf http://127.0.0.1:8080/api/v1/devices | sh -c 'cat | grep -o "\"id\"" | wc -l')
        echo "provisioner: l8opensim now reports $$count devices"
    volumes:
      - ./l8opensim:/seed:ro
```

(The exact provisioner script shape — JSON parsing in pure shell vs. requiring `jq` — is finalized during the plan-task spike. The two files `devices.json` and `post-each.sh` live in a new `opennms-container/delta-v/l8opensim/` directory.)

**`minion-lab` service** (insert after the original `minion` service):

```yaml
  minion-lab:
    image: ${IMAGE_PREFIX:-opennms}/minion-boot:${VERSION}
    container_name: delta-v-minion-lab
    profiles: [lite, full, metrics]
    network_mode: "container:delta-v-l8opensim"
    cap_add:
      - NET_RAW
    depends_on:
      kafka:
        condition: service_healthy
      l8opensim-provisioner:
        condition: service_completed_successfully
    environment:
      MINION_ID: minion-l8opensim-lab-01
      MINION_LOCATION: l8opensim-lab
      KAFKA_IPC_BOOTSTRAP_SERVERS: kafka:9092
      # CRITICAL — port conflict avoidance under shared netns.
      # l8opensim binds 8080 for its web UI/REST API. Minion's actuator
      # defaults to 8080 too (Spring Boot's server.port). When two services
      # share a network namespace, both can't bind the same port. Move
      # Minion's actuator to 8181.
      SERVER_PORT: "8181"
      JAVA_OPTS: >-
        -Xms256m -Xmx512m
        -Djava.security.egd=file:/dev/./urandom
    healthcheck:
      # Healthcheck targets the relocated actuator port (see SERVER_PORT above).
      test: ["CMD-SHELL", "curl -sf http://127.0.0.1:8181/actuator/health | grep -q '\"status\":\"UP\"'"]
      interval: 10s
      timeout: 5s
      retries: 12
      start_period: 15s
```

Notes on shared netns:
- **Port collision avoidance:** l8opensim and Minion both want `:8080` by default. Minion-lab moves to `:8181` via `SERVER_PORT=8181`. Healthcheck and any internal references must use 8181.
- Minion-lab does NOT get its own host port mappings (the netns is owned by `l8opensim`). Its `/actuator/health` at port 8181 is reachable via `docker compose exec minion-lab curl http://127.0.0.1:8181/actuator/health` for diagnostics. If an operator wants to access it from the host, add a port mapping on the **l8opensim** service like `"8302:8181"` (the netns owner is who controls port mappings).
- `MINION_ID` is unique (`minion-l8opensim-lab-01`) so it doesn't collide with the original Minion in the OpenNMS database.
- `cap_add: NET_RAW` matches the original Minion service definition; required for ICMP probes and any raw-socket needs.

### 3. The 20-device mix

The provisioner JSON spec creates devices distributed across all 8 l8opensim categories:

| IP | foreign-id | Category | Node-label |
|---|---|---|---|
| 10.0.0.1 | lab-01 | Core router | lab-01-core-rtr-01 |
| 10.0.0.2 | lab-02 | Core router | lab-02-core-rtr-02 |
| 10.0.0.3 | lab-03 | Edge router | lab-03-edge-rtr-01 |
| 10.0.0.4 | lab-04 | Edge router | lab-04-edge-rtr-02 |
| 10.0.0.5 | lab-05 | DC switch | lab-05-dc-sw-01 |
| 10.0.0.6 | lab-06 | DC switch | lab-06-dc-sw-02 |
| 10.0.0.7 | lab-07 | DC switch | lab-07-dc-sw-03 |
| 10.0.0.8 | lab-08 | Campus switch | lab-08-campus-sw-01 |
| 10.0.0.9 | lab-09 | Campus switch | lab-09-campus-sw-02 |
| 10.0.0.10 | lab-10 | Firewall | lab-10-fw-01 |
| 10.0.0.11 | lab-11 | Firewall | lab-11-fw-02 |
| 10.0.0.12 | lab-12 | Generic server | lab-12-server-01 |
| 10.0.0.13 | lab-13 | Generic server | lab-13-server-02 |
| 10.0.0.14 | lab-14 | Generic server | lab-14-server-03 |
| 10.0.0.15 | lab-15 | Generic server | lab-15-server-04 |
| 10.0.0.16 | lab-16 | GPU server (NVIDIA DGX) | lab-16-gpu-dgx-01 |
| 10.0.0.17 | lab-17 | GPU server (NVIDIA HGX) | lab-17-gpu-hgx-01 |
| 10.0.0.18 | lab-18 | Storage | lab-18-storage-01 |
| 10.0.0.19 | lab-19 | Storage | lab-19-storage-02 |
| 10.0.0.20 | lab-20 | Storage | lab-20-storage-03 |

The exact l8opensim API JSON keys per device profile (e.g., `device_type`, `category`, `profile_name`) are confirmed during the first plan task by querying l8opensim's API after a manual `auto-create` to learn the canonical field names.

### 4. New requisition + collectd package

**`opennms-container/delta-v/provisiond-overlay/etc/imports/l8opensim-lab.xml`** (new file):

```xml
<?xml version="1.0" encoding="UTF-8"?>
<model-import xmlns="http://xmlns.opennms.org/xsd/config/model-import"
              date-stamp="2026-04-20T00:00:00.000Z"
              foreign-source="l8opensim-lab">
   <node location="l8opensim-lab" foreign-id="lab-01" node-label="lab-01-core-rtr-01">
      <interface ip-addr="10.0.0.1" status="1" snmp-primary="P">
         <monitored-service service-name="ICMP"/>
         <monitored-service service-name="SNMP"/>
      </interface>
   </node>
   <!-- ...19 more nodes following the table in §3... -->
</model-import>
```

**`opennms-container/delta-v/provisiond-overlay/etc/provisiond-configuration.xml`** (edits):

```xml
<!-- Add this requisition-def block alongside the others -->
<requisition-def import-name="l8opensim-lab"
                 import-url-resource="file:///opt/deltav/etc/imports/l8opensim-lab.xml">
    <cron-schedule>0/30 * * * * ?</cron-schedule>
</requisition-def>

<!-- Comment out the existing mhuot-labs block. Operators wanting LLDP topology
     testing against an external lab should uncomment this block. The mhuot-labs
     requisition file remains on disk under etc/imports/. -->
<!--
<requisition-def import-name="mhuot-labs"
                 import-url-resource="file:///opt/deltav/etc/imports/mhuot-labs.xml">
    <cron-schedule>0/30 * * * * ?</cron-schedule>
</requisition-def>
-->
```

**`opennms-container/delta-v/collectd-daemon-overlay/etc/collectd-configuration.xml`** (add new package):

```xml
<package name="l8opensim-lab" remote="false">
    <filter>IPADDR != '0.0.0.0'</filter>
    <include-range begin="10.0.0.1" end="10.0.0.255"/>
    <service name="SNMP" interval="30000" user-defined="false" status="on">
        <parameter key="collection" value="default"/>
        <parameter key="thresholding-enabled" value="false"/>
    </service>
</package>
```

The package matches by IP range (10.0.0.0/24). 30s interval matches the `example1` package interval set in PR #182. No thresholding (we're not testing thresholds in this PR's scope).

### 5. Dashboard changes

**`opennms-container/delta-v/grafana/dashboards/snmp-overview.json`** edits:

Add a new template variable to `templating.list`:

```json
{
    "name": "monitoring_location",
    "label": "Monitoring location",
    "type": "query",
    "datasource": { "type": "prometheus", "uid": "victoriametrics" },
    "query": {
        "query": "label_values(opennms_mib2_x_interfaces_ifhcinoctets_total, location)",
        "refId": "PrometheusVariableQueryEditor-VariableQuery"
    },
    "refresh": 1,
    "regex": "",
    "sort": 1,
    "multi": true,
    "includeAll": true,
    "allValue": ".*",
    "current": {
        "selected": true,
        "text": ["All"],
        "value": ["$__all"]
    }
}
```

All 6 panels' PromQL gets a `location=~"$monitoring_location"` selector appended. Example for panel 1:

```
topk(10, rate(opennms_mib2_x_interfaces_ifhcinoctets_total{
    instance=~"$instance",
    foreign_source=~"$foreign_source",
    location=~"$monitoring_location"
}[5m]) * 8)
```

Devices-monitored table query becomes:

```
count by (instance, foreign_source, location, snmp_syscontact, snmp_syslocation)
  (rate(opennms_mib2_x_interfaces_ifhcinoctets_total[5m]))
```

And the `transformations[].options.renameByName` map adds `"location": "Monitoring location"`.

### 6. E2E test changes

**`opennms-container/delta-v/test-prometheus-writer-e2e.sh`** edits:

1. Bump `POLL_GRACE_SECONDS` if the plan task's empirical measurement shows 90s is insufficient. Plan task targets keeping it at 90; bumps to 150 or 180 if needed.

2. Add Step 10 immediately after Step 9 (Dashboard OK) and before the final `echo "==> ALL ASSERTIONS PASSED"; exit 0`:

```bash
# ── Step 10: Verify l8opensim-lab location is actually producing metrics ──────
echo "==> Step 10: Verify l8opensim-lab location produces interface HC counters"
deadline=$((SECONDS + VM_QUERY_TIMEOUT))
lab_landed=false
while (( SECONDS < deadline )); do
    resp=$(curl -sGf "http://localhost:18428/api/v1/query" \
            --data-urlencode 'query=opennms_mib2_x_interfaces_ifhcinoctets_total{foreign_source="l8opensim-lab"}' \
            2>/dev/null || echo '{"data":{"result":[]}}')
    count=$(echo "$resp" | python3 -c \
            'import json,sys; d=json.load(sys.stdin); print(len(d.get("data",{}).get("result",[])))' \
            2>/dev/null || echo "0")
    if (( count > 0 )); then
        echo "==> VM returned ${count} series for l8opensim-lab (Minion-lab is working)"
        echo "$resp" | grep -q '"location":"l8opensim-lab"' || \
            { echo "FAIL: series missing location label"; exit 1; }
        lab_landed=true
        break
    fi
    sleep 2
done
if [[ "$lab_landed" != "true" ]]; then
    echo "FAIL: l8opensim-lab produced no interface HC metrics within ${VM_QUERY_TIMEOUT}s"
    echo "Last VM response: $resp"
    docker compose logs minion-lab | tail -30
    exit 1
fi
```

What Step 10 catches that no other step does:
- Minion-lab not registering with Kafka RPC consumer group.
- l8opensim-provisioner silently failing (devices not created → no SnmpInterface rows → no polls).
- Shared-netns wiring bug (Minion-lab can't reach 10.0.0.0/24 inside l8opensim).
- Collectd's collect-RPC dispatch missing the l8opensim-lab Minion (Twin/RPC routing bug for the new location).

## Edge cases & error handling

| Scenario | Handling |
|---|---|
| l8opensim-provisioner fails (network blip during POST) | `restart: "no"` so failure is loud; operator runs `docker compose restart l8opensim-provisioner`. Idempotent: re-POSTing existing device IDs returns the existing entries. |
| l8opensim API rejects an unsupported device profile name | Provisioner exits non-zero on the failing entry. Plan task validates all 8 profile names against the `latest` image tag. |
| Minion-lab can't reach 10.0.0.0/24 inside l8opensim (shared-netns bug) | Step 10 of E2E catches this. Manual fix: confirm `network_mode: "container:delta-v-l8opensim"` is set; check `docker exec delta-v-minion-lab ip addr show` reveals `simN` interfaces. |
| Twenty SNMP polls overload Minion-lab | Trivial load: 20 nodes × ~5 interface walks each / 30s = ~3.3 walks/sec. Single Minion JVM thread pool handles this. |
| Operator runs `docker compose --profile lite up` without `--profile metrics` | `l8opensim` is in `[lite, full, metrics]` so it comes up. `minion-lab` requires `kafka` (always-on) and `l8opensim-provisioner` (in same profile set). The lab path works in `lite` even without `metrics`/Grafana. Demo metric flow goes to prometheus-writer (always in `lite,full,metrics` per PR #181) but no Grafana visualization. |
| Operator wants mhuot-labs back | Uncomment the `<requisition-def import-name="mhuot-labs">` block in `provisiond-configuration.xml`. The requisition file at `imports/mhuot-labs.xml` is unchanged. |

## Performance

| Resource | Cost |
|---|---|
| l8opensim container memory (20 devices) | ~150 MiB RAM (extrapolated from spike: 11 MiB at 3 devices) |
| l8opensim startup time | ~5s base + provisioner ~5-10s for 20 devices |
| Minion-lab container memory | ~400-500 MiB RAM (typical Minion JVM) |
| Total stack startup time | ~2-3 minutes from `docker compose up` to data flowing |
| Minion-lab SNMP poll rate | ~3.3 SNMP walks/sec at 30s interval × 20 devices |
| Container count (lite + metrics profile) | +3 services (l8opensim, l8opensim-provisioner, minion-lab) |

The simulator scaling from 3 → 20 devices on the spike was sub-second, so the cost is dominated by node-scan + first-poll-cycle latency, not the simulator itself.

## Backward compatibility

| Change | Impact |
|---|---|
| New l8opensim service | Additive. Operators not opting into `lite`/`full`/`metrics` profiles see no change. |
| New minion-lab service | Additive. Operators using only the original Minion at `Default` see no change. |
| New l8opensim-lab requisition + collectd package | Additive. Existing rpc-canary, default, etc. requisitions unaffected. |
| mhuot-labs auto-import commented out | **Behavior change at upgrade.** Existing operators relying on auto-imported mhuot-labs nodes for LLDP testing must uncomment the block. The requisition file remains so the data is recoverable. PR description must call this out. |
| Dashboard `monitoring_location` variable + Location column | Additive. Existing dashboard queries continue to work; new variable defaults to "All" so the operator's filter behavior is preserved unless they actively filter. |
| E2E Step 10 added | Additive. Steps 1-9 still pass even if Step 10 fails (only the final `exit 0` doesn't fire). Effectively a stricter test. |

No PromQL query in any external dashboard breaks. No existing label changes.

## Testing strategy

### Automated (CI)

`bash opennms-container/delta-v/test-prometheus-writer-e2e.sh` runs steps 1-10. All must pass for green. Specifically:
- Steps 1-6 unchanged (rpc-canary path proves data flow at `Default` location).
- Steps 7-9 unchanged (Grafana provisioning checks).
- Step 10 NEW (l8opensim-lab path proves data flow at `l8opensim-lab` location).

### Manual (before merge)

1. `docker compose --profile lite --profile metrics up -d --build`
2. Wait ~2-3 minutes (significantly longer than today's E2E because of the 20 nodes' node-scan + first poll cycle).
3. Open `http://localhost:13000/d/snmp-overview`.
4. Confirm `Monitoring location` dropdown lists `Default` AND `l8opensim-lab`.
5. Filter to `l8opensim-lab`; confirm interface throughput / error / storage panels populate.
6. Filter to `Default`; confirm rpc-canary `snmp-agent-canary` data still renders (regression check).
7. Confirm "Devices monitored" table shows ~21 rows with the new "Monitoring location" column populated correctly.
8. Optional: hit `http://localhost:19081/ui` — l8opensim's own web UI — and verify the 20 devices listed with their assigned IPs.

### Verification gates

- E2E exit 0 with all 10 steps green.
- Manual visual check passes.
- `docker compose ps` shows all services healthy within 3 minutes of `up`.

## Success criteria (settled in brainstorming)

- [x] l8opensim service in compose with 20 simulated devices across all 8 categories.
- [x] Second Minion at `location=l8opensim-lab` honoring the Minion-mandatory tenet.
- [x] mhuot-labs auto-import disabled, requisition file preserved for opt-in LLDP testing.
- [x] Dashboard gains `monitoring_location` template variable and Location column.
- [x] E2E gains Step 10 asserting l8opensim-lab data flow.
- [x] PR opened `--repo pbrane/delta-v --base develop`. Title prefix `feat(mock-lab):`.
- [x] Memory updates queued: file `project_l8opensim_mock_lab_done`, update `project_grafana_dashboard_bundle` to note the canonical mock environment, update `project_e2e_hostname_resolution_timeout` resolution memo.

## After this PR (queued follow-ups)

- **LLDP-MIB upstream contribution to l8opensim.** Today only mhuot-labs (real devices) exercises Enlinkd's topology discovery. A small Go contribution adding LLDP-MIB serving to l8opensim's SNMP module would let topology testing run entirely from compose.
- **Wire l8opensim's flow exporters.** Three follow-up PRs: NetFlow v5/v9, IPFIX, sFlow v5. Each potentially retires one of the existing per-protocol exporter containers.
- **Wire l8opensim's SNMP trap exporter.** Exercise Trapd at scale.
- **Wire l8opensim's UDP syslog exporter.** Exercise Syslogd at scale.
- **Add `device_type` Prometheus label.** Requires provisiond-side metadata adapter to capture l8opensim's device_type from the REST API (or sysObjectID parsing) and add to NodeContext metadata. Then add to the `from-metadata` allowlist (PR #180).
- **Rename mhuot-labs to "external-lldp-lab"** if the maintainer wants to fully de-personalize the demo. Separate cleanup PR.
- **Once l8opensim has interface-errors + host-resources MIB support (or contribution lands)**, retire the original `mock-snmp-agent` service. Consolidates the demo to a single SNMP simulator.
