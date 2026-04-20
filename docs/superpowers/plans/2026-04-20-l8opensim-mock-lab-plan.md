# l8opensim Mock-Lab Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace Delta-V's current limited single-device mock SNMP environment with `l8opensim` running 20 simulated devices across 8 categories, plus a second Minion at `location=l8opensim-lab` to honor the Minion-mandatory tenet, plus dashboard + E2E updates so the result is provably correct end-to-end.

**Architecture:** Three new compose services (`l8opensim`, `l8opensim-provisioner` one-shot, `minion-lab` shared netns at port 8181). New requisition + collectd package for the lab. `mhuot-labs` requisition auto-import disabled (file kept). Dashboard gains `monitoring_location` template variable. E2E gains Step 10 asserting the new lab actually polls.

**Tech Stack:** Docker Compose, l8opensim (Go SNMP/SSH/REST simulator from `ghcr.io/labmonkeys-space/l8opensim:latest`), Grafana 11.4.0, VictoriaMetrics, bash + curl + python3 for the E2E script.

**Branch:** `feat/l8opensim-mock-lab` (already created off `develop` at `7ec759eb8e5`, with the spec committed as `ce13b0aaed0`).

**Spec reference:** `docs/superpowers/specs/2026-04-20-l8opensim-mock-lab-design.md`.

**PR target:** `pbrane/delta-v` `develop`. Title prefix `feat(mock-lab):`. **NEVER `OpenNMS/opennms`.**

**No Java rebuild needed** — this PR touches docker-compose, XML config, dashboard JSON, the E2E script, and a new directory of shell/JSON files. The prometheus-writer image is already current with PR #180. Other daemon images need no changes.

---

## Pre-flight (one-time per session)

- [ ] **Verify branch and clean working tree**

Run: `git branch --show-current && git status --short`
Expected:
```
feat/l8opensim-mock-lab
```
(no dirty files; if `provisiond-overlay/etc/imports/*.xml` files show modified, run `git checkout -- opennms-container/delta-v/provisiond-overlay/etc/imports/` to discard the requisition runtime drift per `feedback_provisiond_requisition_drift`.)

- [ ] **Verify spec is committed**

Run: `git log --oneline -3`
Expected: most recent commit is `ce13b0aaed0 docs(spec): l8opensim mock-lab integration` (or your spec commit SHA).

- [ ] **Verify Docker Desktop is running and stack is torn down**

Run:
```
docker ps --format '{{.Names}}' | grep delta-v | head -5
```
Expected: empty output. If anything's running, run `cd opennms-container/delta-v && docker compose --profile lite --profile metrics down -v --remove-orphans` first.

If any pre-flight check fails, stop and resolve before starting Task 1.

---

### Task 1: Spike l8opensim REST API + author `devices.json` and `post-each.sh`

**Files:**
- Create: `opennms-container/delta-v/l8opensim/devices.json` (the 20-device spec)
- Create: `opennms-container/delta-v/l8opensim/post-each.sh` (provisioner script)

**Empirical question:** the brainstorming spike confirmed `POST /api/v1/devices` works with `{"start_ip":"X", "device_count":N, "netmask":"24"}` and creates devices of the "Default" type only. We need to verify whether a per-device profile field (`device_type`, `category`, `profile`) is accepted, OR if the API only supports auto-count with round-robin profiles.

This task does the spike, records findings, and writes the two files based on what's actually supported.

- [ ] **Step 1: Pull the image**

```
docker pull ghcr.io/labmonkeys-space/l8opensim:latest
```
Expected: `Status: Downloaded` or `Image is up to date`.

- [ ] **Step 2: Run a temporary l8opensim instance (no-namespace mode)**

```
docker run -d --rm --name l8-spike \
  --cap-add=NET_ADMIN --cap-add=SYS_ADMIN \
  --device /dev/net/tun \
  -p 19161:161/udp -p 19081:8080/tcp \
  ghcr.io/labmonkeys-space/l8opensim:latest -no-namespace
```

Then wait briefly for healthcheck:
```
sleep 5 && docker logs l8-spike 2>&1 | grep -E 'Web UI|server starting' | head -2
```
Expected: lines containing `Web UI` and `server starting`.

- [ ] **Step 3: Probe the REST API for per-device profile selection**

Try variant A (per-device device_type field):
```
curl -sf -X POST http://localhost:19081/api/v1/devices \
  -H "Content-Type: application/json" \
  -d '{"start_ip":"10.0.0.1","device_count":1,"netmask":"24","device_type":"core_router"}' \
  | python3 -m json.tool | head -10
```

Then list devices to see what `device_type` was actually assigned:
```
curl -sf http://localhost:19081/api/v1/devices | python3 -m json.tool | head -30
```

Try variant B (top-level `category` field):
```
curl -sf -X POST http://localhost:19081/api/v1/devices \
  -H "Content-Type: application/json" \
  -d '{"start_ip":"10.0.0.2","device_count":1,"netmask":"24","category":"router"}' \
  | python3 -m json.tool | head -10
```

Try variant C (an alternative schema — `devices` array with per-device profiles):
```
curl -sf -X POST http://localhost:19081/api/v1/devices \
  -H "Content-Type: application/json" \
  -d '{"devices":[{"ip":"10.0.0.3","device_type":"firewall"}]}' \
  | python3 -m json.tool | head -10
```

**Document findings:** capture which variant the API actually accepts. List devices via `GET /api/v1/devices` and observe the `device_type` field on each entry to confirm whether your hint was honored or ignored.

- [ ] **Step 4: Tear down the spike**

```
docker stop l8-spike
```

- [ ] **Step 5: Author `devices.json` based on Step 3 findings**

Create `opennms-container/delta-v/l8opensim/` directory and the `devices.json` file. Two scenarios:

**Scenario A — API accepts per-device profile selection.** Use one POST per device with the discovered field name:

```json
[
  {"start_ip":"10.0.0.1","device_count":1,"netmask":"24","device_type":"<core_router_name>","node_label":"lab-01-core-rtr-01"},
  {"start_ip":"10.0.0.2","device_count":1,"netmask":"24","device_type":"<core_router_name>","node_label":"lab-02-core-rtr-02"},
  {"start_ip":"10.0.0.3","device_count":1,"netmask":"24","device_type":"<edge_router_name>","node_label":"lab-03-edge-rtr-01"},
  {"start_ip":"10.0.0.4","device_count":1,"netmask":"24","device_type":"<edge_router_name>","node_label":"lab-04-edge-rtr-02"},
  {"start_ip":"10.0.0.5","device_count":1,"netmask":"24","device_type":"<dc_switch_name>","node_label":"lab-05-dc-sw-01"},
  {"start_ip":"10.0.0.6","device_count":1,"netmask":"24","device_type":"<dc_switch_name>","node_label":"lab-06-dc-sw-02"},
  {"start_ip":"10.0.0.7","device_count":1,"netmask":"24","device_type":"<dc_switch_name>","node_label":"lab-07-dc-sw-03"},
  {"start_ip":"10.0.0.8","device_count":1,"netmask":"24","device_type":"<campus_switch_name>","node_label":"lab-08-campus-sw-01"},
  {"start_ip":"10.0.0.9","device_count":1,"netmask":"24","device_type":"<campus_switch_name>","node_label":"lab-09-campus-sw-02"},
  {"start_ip":"10.0.0.10","device_count":1,"netmask":"24","device_type":"<firewall_name>","node_label":"lab-10-fw-01"},
  {"start_ip":"10.0.0.11","device_count":1,"netmask":"24","device_type":"<firewall_name>","node_label":"lab-11-fw-02"},
  {"start_ip":"10.0.0.12","device_count":1,"netmask":"24","device_type":"<server_name>","node_label":"lab-12-server-01"},
  {"start_ip":"10.0.0.13","device_count":1,"netmask":"24","device_type":"<server_name>","node_label":"lab-13-server-02"},
  {"start_ip":"10.0.0.14","device_count":1,"netmask":"24","device_type":"<server_name>","node_label":"lab-14-server-03"},
  {"start_ip":"10.0.0.15","device_count":1,"netmask":"24","device_type":"<server_name>","node_label":"lab-15-server-04"},
  {"start_ip":"10.0.0.16","device_count":1,"netmask":"24","device_type":"<gpu_dgx_name>","node_label":"lab-16-gpu-dgx-01"},
  {"start_ip":"10.0.0.17","device_count":1,"netmask":"24","device_type":"<gpu_hgx_name>","node_label":"lab-17-gpu-hgx-01"},
  {"start_ip":"10.0.0.18","device_count":1,"netmask":"24","device_type":"<storage_name>","node_label":"lab-18-storage-01"},
  {"start_ip":"10.0.0.19","device_count":1,"netmask":"24","device_type":"<storage_name>","node_label":"lab-19-storage-02"},
  {"start_ip":"10.0.0.20","device_count":1,"netmask":"24","device_type":"<storage_name>","node_label":"lab-20-storage-03"}
]
```

Replace `<*_name>` placeholders with the actual profile names accepted by the API. Remove `node_label` if the API doesn't accept it.

**Scenario B — API only supports auto-count round-robin.** Use a single POST creating all 20:

```json
[
  {"start_ip":"10.0.0.1","device_count":20,"netmask":"24"}
]
```

Plus a comment in `devices.json` (since JSON has no comments, use a sentinel `_comment` key inside an array entry) noting that device-type heterogeneity is sacrificed because the API doesn't support per-device profiles. Document this as a Section-§3 caveat to fold into the PR description.

- [ ] **Step 6: Author `post-each.sh`**

Create `opennms-container/delta-v/l8opensim/post-each.sh` with:

```bash
#!/bin/sh
# Provisioner script: read devices.json, POST each entry to l8opensim's
# REST API. Idempotent: re-POSTing existing IPs returns the existing device.
#
# Args:
#   $1 = base URL of l8opensim REST API (e.g. http://127.0.0.1:8080)
#
# Reads JSON from stdin (each line is one JSON object — caller pre-splits).
set -eu
base_url="$1"
created=0
failed=0
while IFS= read -r entry; do
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
```

Make it executable:
```
chmod +x opennms-container/delta-v/l8opensim/post-each.sh
```

- [ ] **Step 7: Validate JSON syntax**

```
python3 -c 'import json; d=json.load(open("opennms-container/delta-v/l8opensim/devices.json")); print(f"OK entries={len(d)}")'
```
Expected: `OK entries=20` (Scenario A) or `OK entries=1` (Scenario B).

- [ ] **Step 8: Commit**

```
git add opennms-container/delta-v/l8opensim/
git commit -m "feat(mock-lab): add l8opensim devices.json + provisioner script

devices.json holds the 20-device mix specification (4 routers + 5 switches
+ 2 firewalls + 4 servers + 2 GPU + 3 storage). post-each.sh iterates the
spec and POSTs each entry to l8opensim's /api/v1/devices REST API.

API shape verified empirically: <Scenario A or B finding here>."
```

---

### Task 2: Create `l8opensim-lab.xml` requisition

**Files:**
- Create: `opennms-container/delta-v/provisiond-overlay/etc/imports/l8opensim-lab.xml`

20 nodes, one per IP in 10.0.0.1-20, all at `location="l8opensim-lab"` and `foreign-source="l8opensim-lab"`.

- [ ] **Step 1: Create the requisition file**

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
   <node location="l8opensim-lab" foreign-id="lab-02" node-label="lab-02-core-rtr-02">
      <interface ip-addr="10.0.0.2" status="1" snmp-primary="P">
         <monitored-service service-name="ICMP"/>
         <monitored-service service-name="SNMP"/>
      </interface>
   </node>
   <node location="l8opensim-lab" foreign-id="lab-03" node-label="lab-03-edge-rtr-01">
      <interface ip-addr="10.0.0.3" status="1" snmp-primary="P">
         <monitored-service service-name="ICMP"/>
         <monitored-service service-name="SNMP"/>
      </interface>
   </node>
   <node location="l8opensim-lab" foreign-id="lab-04" node-label="lab-04-edge-rtr-02">
      <interface ip-addr="10.0.0.4" status="1" snmp-primary="P">
         <monitored-service service-name="ICMP"/>
         <monitored-service service-name="SNMP"/>
      </interface>
   </node>
   <node location="l8opensim-lab" foreign-id="lab-05" node-label="lab-05-dc-sw-01">
      <interface ip-addr="10.0.0.5" status="1" snmp-primary="P">
         <monitored-service service-name="ICMP"/>
         <monitored-service service-name="SNMP"/>
      </interface>
   </node>
   <node location="l8opensim-lab" foreign-id="lab-06" node-label="lab-06-dc-sw-02">
      <interface ip-addr="10.0.0.6" status="1" snmp-primary="P">
         <monitored-service service-name="ICMP"/>
         <monitored-service service-name="SNMP"/>
      </interface>
   </node>
   <node location="l8opensim-lab" foreign-id="lab-07" node-label="lab-07-dc-sw-03">
      <interface ip-addr="10.0.0.7" status="1" snmp-primary="P">
         <monitored-service service-name="ICMP"/>
         <monitored-service service-name="SNMP"/>
      </interface>
   </node>
   <node location="l8opensim-lab" foreign-id="lab-08" node-label="lab-08-campus-sw-01">
      <interface ip-addr="10.0.0.8" status="1" snmp-primary="P">
         <monitored-service service-name="ICMP"/>
         <monitored-service service-name="SNMP"/>
      </interface>
   </node>
   <node location="l8opensim-lab" foreign-id="lab-09" node-label="lab-09-campus-sw-02">
      <interface ip-addr="10.0.0.9" status="1" snmp-primary="P">
         <monitored-service service-name="ICMP"/>
         <monitored-service service-name="SNMP"/>
      </interface>
   </node>
   <node location="l8opensim-lab" foreign-id="lab-10" node-label="lab-10-fw-01">
      <interface ip-addr="10.0.0.10" status="1" snmp-primary="P">
         <monitored-service service-name="ICMP"/>
         <monitored-service service-name="SNMP"/>
      </interface>
   </node>
   <node location="l8opensim-lab" foreign-id="lab-11" node-label="lab-11-fw-02">
      <interface ip-addr="10.0.0.11" status="1" snmp-primary="P">
         <monitored-service service-name="ICMP"/>
         <monitored-service service-name="SNMP"/>
      </interface>
   </node>
   <node location="l8opensim-lab" foreign-id="lab-12" node-label="lab-12-server-01">
      <interface ip-addr="10.0.0.12" status="1" snmp-primary="P">
         <monitored-service service-name="ICMP"/>
         <monitored-service service-name="SNMP"/>
      </interface>
   </node>
   <node location="l8opensim-lab" foreign-id="lab-13" node-label="lab-13-server-02">
      <interface ip-addr="10.0.0.13" status="1" snmp-primary="P">
         <monitored-service service-name="ICMP"/>
         <monitored-service service-name="SNMP"/>
      </interface>
   </node>
   <node location="l8opensim-lab" foreign-id="lab-14" node-label="lab-14-server-03">
      <interface ip-addr="10.0.0.14" status="1" snmp-primary="P">
         <monitored-service service-name="ICMP"/>
         <monitored-service service-name="SNMP"/>
      </interface>
   </node>
   <node location="l8opensim-lab" foreign-id="lab-15" node-label="lab-15-server-04">
      <interface ip-addr="10.0.0.15" status="1" snmp-primary="P">
         <monitored-service service-name="ICMP"/>
         <monitored-service service-name="SNMP"/>
      </interface>
   </node>
   <node location="l8opensim-lab" foreign-id="lab-16" node-label="lab-16-gpu-dgx-01">
      <interface ip-addr="10.0.0.16" status="1" snmp-primary="P">
         <monitored-service service-name="ICMP"/>
         <monitored-service service-name="SNMP"/>
      </interface>
   </node>
   <node location="l8opensim-lab" foreign-id="lab-17" node-label="lab-17-gpu-hgx-01">
      <interface ip-addr="10.0.0.17" status="1" snmp-primary="P">
         <monitored-service service-name="ICMP"/>
         <monitored-service service-name="SNMP"/>
      </interface>
   </node>
   <node location="l8opensim-lab" foreign-id="lab-18" node-label="lab-18-storage-01">
      <interface ip-addr="10.0.0.18" status="1" snmp-primary="P">
         <monitored-service service-name="ICMP"/>
         <monitored-service service-name="SNMP"/>
      </interface>
   </node>
   <node location="l8opensim-lab" foreign-id="lab-19" node-label="lab-19-storage-02">
      <interface ip-addr="10.0.0.19" status="1" snmp-primary="P">
         <monitored-service service-name="ICMP"/>
         <monitored-service service-name="SNMP"/>
      </interface>
   </node>
   <node location="l8opensim-lab" foreign-id="lab-20" node-label="lab-20-storage-03">
      <interface ip-addr="10.0.0.20" status="1" snmp-primary="P">
         <monitored-service service-name="ICMP"/>
         <monitored-service service-name="SNMP"/>
      </interface>
   </node>
</model-import>
```

- [ ] **Step 2: Validate XML syntax**

```
python3 -c 'import xml.etree.ElementTree as ET; t=ET.parse("opennms-container/delta-v/provisiond-overlay/etc/imports/l8opensim-lab.xml"); ns={"o":"http://xmlns.opennms.org/xsd/config/model-import"}; print(f"OK nodes={len(t.findall(chr(46)+chr(47)+chr(47)+chr(111)+chr(58)+chr(110)+chr(111)+chr(100)+chr(101), ns))}")'
```

Simpler equivalent:
```
python3 << 'EOF'
import xml.etree.ElementTree as ET
t = ET.parse("opennms-container/delta-v/provisiond-overlay/etc/imports/l8opensim-lab.xml")
ns = {"o": "http://xmlns.opennms.org/xsd/config/model-import"}
nodes = t.findall(".//o:node", ns)
ips = [n.find("o:interface", ns).get("ip-addr") for n in nodes]
print(f"OK nodes={len(nodes)} ip-range={ips[0]}..{ips[-1]}")
EOF
```

Expected: `OK nodes=20 ip-range=10.0.0.1..10.0.0.20`.

- [ ] **Step 3: Commit**

```
git add opennms-container/delta-v/provisiond-overlay/etc/imports/l8opensim-lab.xml
git commit -m "feat(mock-lab): add l8opensim-lab requisition (20 nodes at 10.0.0.1-20)"
```

---

### Task 3: Update `collectd-configuration.xml` — add `l8opensim-lab` package

**Files:**
- Modify: `opennms-container/delta-v/collectd-daemon-overlay/etc/collectd-configuration.xml`

A new package matching IPs `10.0.0.0/24` with the 30s SNMP interval (matching the `example1` package interval set in PR #182).

- [ ] **Step 1: Locate the existing `example1` package and insert the new one above it**

The new `l8opensim-lab` package matches the more specific 10.0.0.0/24 range; placing it BEFORE the broad `example1` (which catches 1.1.1.1-254.254.254.254) ensures the more-specific package wins for our lab IPs.

Find the line `   <package name="example1" remote="false">` in `opennms-container/delta-v/collectd-daemon-overlay/etc/collectd-configuration.xml`.

Insert these lines immediately BEFORE that line (preserve the leading 3 spaces of indent):

```xml
   <package name="l8opensim-lab" remote="false">
      <filter>IPADDR != '0.0.0.0'</filter>
      <include-range begin="10.0.0.1" end="10.0.0.255"/>
      <!-- 30s interval matches example1 (set by delta-v#182). The l8opensim
           devices' HC counter sine waves cycle on a 100-point timeline, so
           30s is enough resolution to see realistic rate changes in Grafana. -->
      <service name="SNMP" interval="30000" user-defined="false" status="on">
         <parameter key="collection" value="default"/>
         <parameter key="thresholding-enabled" value="false"/>
      </service>
   </package>
```

- [ ] **Step 2: Validate XML syntax**

```
python3 -c 'import xml.etree.ElementTree as ET; ET.parse("opennms-container/delta-v/collectd-daemon-overlay/etc/collectd-configuration.xml"); print("OK")'
```
Expected: `OK`.

- [ ] **Step 3: Verify the new package is in the file**

```
grep -A1 'name="l8opensim-lab"' opennms-container/delta-v/collectd-daemon-overlay/etc/collectd-configuration.xml | head -3
```
Expected: shows the package opening line + filter.

- [ ] **Step 4: Commit**

```
git add opennms-container/delta-v/collectd-daemon-overlay/etc/collectd-configuration.xml
git commit -m "feat(mock-lab): add l8opensim-lab collectd package matching 10.0.0.0/24

Inserted before the broader example1 package so the more-specific 10.0.0.0/24
range wins for the new lab IPs. 30s SNMP interval matches example1 (set by
delta-v#182)."
```

---

### Task 4: Update `provisiond-configuration.xml` — add l8opensim-lab requisition-def, comment out mhuot-labs

**Files:**
- Modify: `opennms-container/delta-v/provisiond-overlay/etc/provisiond-configuration.xml`

Two changes.

- [ ] **Step 1: Comment out the `mhuot-labs` requisition-def block**

In `opennms-container/delta-v/provisiond-overlay/etc/provisiond-configuration.xml`, locate this block (currently lines 31-34):

```xml
  <requisition-def import-name="mhuot-labs"
                   import-url-resource="file:///opt/deltav/etc/imports/mhuot-labs.xml">
    <cron-schedule>0/30 * * * * ?</cron-schedule>
  </requisition-def>
```

Replace with:

```xml
  <!--
    mhuot-labs auto-import disabled (delta-v#<this PR>): the requisition
    references real lab devices at 172.20.20.x reachable only when the
    operator's host has VPN/LAN connectivity to that subnet. Useful for
    Enlinkd LLDP topology testing against real hardware. Uncomment to
    re-enable. The imports/mhuot-labs.xml file is preserved on disk.
  <requisition-def import-name="mhuot-labs"
                   import-url-resource="file:///opt/deltav/etc/imports/mhuot-labs.xml">
    <cron-schedule>0/30 * * * * ?</cron-schedule>
  </requisition-def>
  -->
```

- [ ] **Step 2: Add the new `l8opensim-lab` requisition-def block**

In the same file, immediately AFTER the (now-commented) mhuot-labs block and BEFORE the `perspective-test` requisition-def, insert:

```xml
  <requisition-def import-name="l8opensim-lab"
                   import-url-resource="file:///opt/deltav/etc/imports/l8opensim-lab.xml">
    <cron-schedule>0/30 * * * * ?</cron-schedule>
  </requisition-def>
```

- [ ] **Step 3: Validate XML syntax**

```
python3 -c 'import xml.etree.ElementTree as ET; t=ET.parse("opennms-container/delta-v/provisiond-overlay/etc/provisiond-configuration.xml"); print(f"OK reqs={len(t.findall(chr(46)+chr(47)+chr(47){chr(123)}str(chr(34))+chr(104)+chr(116)+chr(116)+chr(112)+chr(58)+chr(47)+chr(47)+chr(120)+chr(109)+chr(108)+chr(110)+chr(115)+chr(46)+chr(111)+chr(112)+chr(101)+chr(110)+chr(110)+chr(109)+chr(115)+chr(46)+chr(111)+chr(114)+chr(103)+chr(47)+chr(120)+chr(115)+chr(100)+chr(47)+chr(99)+chr(111)+chr(110)+chr(102)+chr(105)+chr(103)+chr(47)+chr(112)+chr(114)+chr(111)+chr(118)+chr(105)+chr(115)+chr(105)+chr(111)+chr(110)+chr(100)+chr(45)+chr(99)+chr(111)+chr(110)+chr(102)+chr(105)+chr(103)+chr(117)+chr(114)+chr(97)+chr(116)+chr(105)+chr(111)+chr(110)+chr(34)+chr(125):requisition-def"))}")'
```

That's awkward. Use a clearer Python check:
```
python3 << 'EOF'
import xml.etree.ElementTree as ET
t = ET.parse("opennms-container/delta-v/provisiond-overlay/etc/provisiond-configuration.xml")
ns = {"o": "http://xmlns.opennms.org/xsd/config/provisiond-configuration"}
reqs = t.findall(".//o:requisition-def", ns)
names = [r.get("import-name") for r in reqs]
print(f"OK active requisitions: {names}")
print(f"  l8opensim-lab present: {'l8opensim-lab' in names}")
print(f"  mhuot-labs absent: {'mhuot-labs' not in names}")
EOF
```

Expected output includes `l8opensim-lab present: True` and `mhuot-labs absent: True` (mhuot-labs is now commented out, so it's not in the parsed tree).

- [ ] **Step 4: Commit**

```
git add opennms-container/delta-v/provisiond-overlay/etc/provisiond-configuration.xml
git commit -m "feat(mock-lab): activate l8opensim-lab auto-import; disable mhuot-labs default

mhuot-labs requisition-def commented out (auto-import disabled). The
imports/mhuot-labs.xml file is preserved on disk; operators wanting LLDP
topology testing against real hardware uncomment the block."
```

---

### Task 5: Add `l8opensim` and `l8opensim-provisioner` services to docker-compose.yml

**Files:**
- Modify: `opennms-container/delta-v/docker-compose.yml`

Two new service blocks. Inserted after the existing `snmp-agent` service (the existing simulated SNMP target).

- [ ] **Step 1: Locate insertion point**

In `opennms-container/delta-v/docker-compose.yml`, find the end of the `snmp-agent` block. Currently (around line 97):

```yaml
  snmp-agent:
    build: ./mock-snmp-agent
    container_name: delta-v-snmp-agent
    hostname: snmp-agent
    healthcheck:
      test: ["CMD-SHELL", "true"]
      interval: 10s
      retries: 3
```

The next service after `snmp-agent` is `minion`. Insert the two new services BETWEEN `snmp-agent` and `minion`.

- [ ] **Step 2: Insert the `l8opensim` service block**

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
    # Start with no devices; the provisioner POSTs the 20-device mix from
    # ./l8opensim/devices.json after this service is healthy.
    # `-no-namespace` mode keeps the per-device TUN interfaces in this
    # container's root netns so minion-lab (which shares the netns) can
    # reach the device IPs at 10.0.0.1-20 directly.
    command: ["-no-namespace"]
    healthcheck:
      # 127.0.0.1 not localhost — BusyBox wget IPv6 quirk per delta-v#182.
      test: ["CMD", "wget", "-q", "-O-", "http://127.0.0.1:8080/health"]
      interval: 5s
      timeout: 3s
      retries: 12
      start_period: 5s
    ports:
      - "19161:161/udp"  # ad-hoc SNMP testing from the host
      - "19081:8080/tcp" # web UI + REST API
```

- [ ] **Step 3: Insert the `l8opensim-provisioner` service block immediately after**

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
        echo "provisioner: posting 20-device spec to l8opensim REST API"
        # devices.json is a JSON array; pipe each entry as one line through
        # post-each.sh which iterates and POSTs.
        cat /seed/devices.json \
            | sed -e 's/^\[//' -e 's/\]$//' -e 's/},/}\n/g' \
            | sh /seed/post-each.sh http://127.0.0.1:8080
        # Verify total count.
        count=$$(wget -q -O- http://127.0.0.1:8080/api/v1/devices \
                 | grep -o '"id"' | wc -l)
        echo "provisioner: l8opensim now reports $$count devices"
        [ "$$count" -ge 20 ] || { echo "FAIL expected >= 20 devices got $$count"; exit 1; }
    volumes:
      - ./l8opensim:/seed:ro
```

Note: the `sed` pipeline on `cat /seed/devices.json` is a JSON-array-to-line-delimited-objects converter that works in plain `sh` without requiring `jq`. It assumes each entry in the array sits on one logical line in the file — which is what the Task 1 `devices.json` produces.

- [ ] **Step 4: Validate the YAML**

```
docker compose -f opennms-container/delta-v/docker-compose.yml --profile lite --profile metrics config > /dev/null && echo OK
```

Expected: `OK`.

(If `docker compose config` complains about `--profile` flag position, run from `opennms-container/delta-v/` directory: `cd opennms-container/delta-v && docker compose --profile lite --profile metrics config > /dev/null && echo OK && cd ../..`)

- [ ] **Step 5: Commit**

```
git add opennms-container/delta-v/docker-compose.yml
git commit -m "feat(mock-lab): add l8opensim + l8opensim-provisioner compose services

l8opensim: ghcr.io/labmonkeys-space/l8opensim:latest in -no-namespace mode,
healthchecked via /health, host ports 19161 (SNMP) + 19081 (web UI + REST).
l8opensim-provisioner: one-shot curl container that POSTs ./l8opensim/devices.json
after l8opensim is healthy. Shared netns so it hits 127.0.0.1:8080.
Profile membership: lite, full, metrics."
```

---

### Task 6: Verify l8opensim integration (boot stack, confirm 20 devices reachable via SNMP)

**No file changes** — verification gate before adding minion-lab.

- [ ] **Step 1: Start the stack with `lite + metrics` profiles**

```
cd opennms-container/delta-v
docker compose --profile lite --profile metrics up -d --build 2>&1 | tail -10
cd ../..
```

Expected: services start; `l8opensim` becomes healthy within ~30s, `l8opensim-provisioner` exits 0 within another ~10s.

- [ ] **Step 2: Wait for healthy + confirm provisioner created 20 devices**

```
sleep 30
docker ps --format '{{.Names}} {{.Status}}' | grep -E 'delta-v-l8opensim'
docker compose -f opennms-container/delta-v/docker-compose.yml logs l8opensim-provisioner | tail -5
```

Expected: `delta-v-l8opensim Up X seconds (healthy)`. Provisioner log shows `l8opensim now reports 20 devices` (or similar).

- [ ] **Step 3: Confirm via REST API**

```
curl -sf http://localhost:19081/api/v1/devices | python3 -c 'import json,sys; d=json.load(sys.stdin); print(f"devices={len(d.get(chr(34)+chr(100)+chr(97)+chr(116)+chr(97)+chr(34), []))}")'
```

Cleaner equivalent:
```
curl -sf http://localhost:19081/api/v1/devices > /tmp/devs.json
python3 -c 'import json; d=json.load(open("/tmp/devs.json")); print(f"devices={len(d.get(\"data\",[]))}")'
```

Expected: `devices=20`.

- [ ] **Step 4: Confirm at least one device responds to direct SNMP from inside l8opensim's netns**

```
docker run --rm --network container:delta-v-l8opensim --entrypoint snmpwalk polinux/snmpd:latest \
    -v2c -c public 10.0.0.1 1.3.6.1.2.1.31.1.1.1.6 2>&1 | head -3
```

Expected: shows lines like `IF-MIB::ifHCInOctets.1 = Counter64: <large_number>`. If this fails, the simulator's TUN interfaces aren't bound correctly OR the device isn't running — check `docker compose logs l8opensim` for errors.

- [ ] **Step 5: Tear down**

```
cd opennms-container/delta-v && docker compose --profile lite --profile metrics down -v --remove-orphans 2>&1 | tail -3 && cd ../..
git checkout -- opennms-container/delta-v/provisiond-overlay/etc/imports/ 2>&1 || true
```

(No commit. This task is verification only.)

If any step fails, debug from logs (`docker compose logs l8opensim` and `docker compose logs l8opensim-provisioner`). Common causes: provisioner JSON-splitting `sed` pipeline mis-handles multi-line JSON entries (fix: ensure devices.json is one entry per line). Don't proceed to Task 7 until this works.

---

### Task 7: Add `minion-lab` service to docker-compose.yml

**Files:**
- Modify: `opennms-container/delta-v/docker-compose.yml`

Insert after the existing `minion` service. Shares network namespace with `delta-v-l8opensim` so it can reach `10.0.0.1-20` on the per-device TUN interfaces.

- [ ] **Step 1: Locate insertion point**

In `opennms-container/delta-v/docker-compose.yml`, find the end of the existing `minion` service block (the one with `MINION_LOCATION: Default`). The next service after `minion` is `pollerd`. Insert the new `minion-lab` block BETWEEN `minion` and `pollerd`.

- [ ] **Step 2: Insert `minion-lab` service block**

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
      # Port-collision avoidance under shared netns: l8opensim binds 8080 for
      # its REST API, Minion's actuator defaults to 8080 too. Move Minion's
      # actuator to 8181.
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

- [ ] **Step 3: Validate compose YAML**

```
cd opennms-container/delta-v && docker compose --profile lite --profile metrics config > /dev/null && echo OK && cd ../..
```
Expected: `OK`.

- [ ] **Step 4: Commit**

```
git add opennms-container/delta-v/docker-compose.yml
git commit -m "feat(mock-lab): add minion-lab service at location=l8opensim-lab

Shared network namespace with l8opensim (network_mode: container:delta-v-l8opensim)
so SNMP polls reach the simulator's per-device TUN interfaces directly.
SERVER_PORT=8181 to avoid port collision with l8opensim's :8080 in shared netns.
Honors the project's Minion-mandatory tenet: SNMP polls dispatched via Kafka
RPC to this Minion at the new location, not collectd's JVM."
```

---

### Task 8: Verify minion-lab integration + measure data-flow latency

**No file changes** — verification gate, also EMPIRICALLY MEASURES timing for the Step 10 POLL_GRACE adjustment in Task 10.

- [ ] **Step 1: Start the stack**

```
cd opennms-container/delta-v
docker compose --profile lite --profile metrics up -d --build 2>&1 | tail -10
cd ../..
```

- [ ] **Step 2: Record start time, wait for healthy, then poll VM until l8opensim-lab data appears**

Run this measurement loop:

```
START=$(date +%s)
echo "stack started at: $(date)"
deadline=$((START + 360))   # 6-minute upper bound
while (( $(date +%s) < deadline )); do
    elapsed=$(( $(date +%s) - START ))
    count=$(curl -sGf "http://localhost:18428/api/v1/query" \
        --data-urlencode 'query=opennms_mib2_x_interfaces_ifhcinoctets_total{foreign_source="l8opensim-lab"}' \
        2>/dev/null | python3 -c \
        'import json,sys; d=json.load(sys.stdin); print(len(d.get("data",{}).get("result",[])))' \
        2>/dev/null || echo "0")
    echo "[t+${elapsed}s] l8opensim-lab series in VM: $count"
    if (( count > 0 )); then
        echo "FIRST DATA at t+${elapsed}s"
        break
    fi
    sleep 15
done
```

**Record the FIRST DATA timestamp.** This is the empirical measurement that determines `POLL_GRACE_SECONDS` in Task 10.

- [ ] **Step 3: Confirm minion-lab is healthy and processing RPC**

```
docker exec delta-v-minion-lab curl -sf http://127.0.0.1:8181/actuator/health 2>&1 | head -3
docker compose -f opennms-container/delta-v/docker-compose.yml logs minion-lab 2>&1 | grep -iE "registered|ready|listening" | tail -10
```

Expected: health returns `"status":"UP"`. Logs show Minion registration with the Kafka brokers.

- [ ] **Step 4: Confirm a few sample series have all the expected labels**

```
curl -sGf "http://localhost:18428/api/v1/series" \
    --data-urlencode 'match[]=opennms_mib2_x_interfaces_ifhcinoctets_total{foreign_source="l8opensim-lab"}' \
    | python3 -m json.tool | head -25
```

Expected: at least one series with labels including `instance` (a node-label like `lab-01-core-rtr-01`), `foreign_source: "l8opensim-lab"`, `foreign_id` (`lab-01`), `location: "l8opensim-lab"`, `node_id` (an integer), `producer: "collectd"`.

- [ ] **Step 5: Tear down**

```
cd opennms-container/delta-v && docker compose --profile lite --profile metrics down -v --remove-orphans 2>&1 | tail -3 && cd ../..
git checkout -- opennms-container/delta-v/provisiond-overlay/etc/imports/ 2>&1 || true
```

(No commit. Record the FIRST DATA timestamp from Step 2 — needed for Task 10 to set `POLL_GRACE_SECONDS`.)

If FIRST DATA never appears within 360s: collectd's RPC dispatch may not be reaching minion-lab. Common causes:
- minion-lab not registered with Kafka RPC for `location=l8opensim-lab` (check minion-lab logs for "registered" lines).
- Twin API not delivering collection-config to minion-lab.
- l8opensim's TUN interfaces not bound when minion-lab tries to reach them (verify `docker exec delta-v-minion-lab ip addr show` reveals `simN` interfaces).

---

### Task 9: Update `snmp-overview.json` dashboard — add `monitoring_location` template variable + Location column

**Files:**
- Modify: `opennms-container/delta-v/grafana/dashboards/snmp-overview.json`

Three groups of edits:
1. Add a third template variable `monitoring_location` (after `foreign_source`).
2. Add `, location=~"$monitoring_location"` to all 6 panels' PromQL `expr` lines.
3. Update Devices-monitored table: query gains `location` in `count by (...)`; transformations gain `"location": "Monitoring location"` rename mapping.

- [ ] **Step 1: Add `monitoring_location` template variable**

In `opennms-container/delta-v/grafana/dashboards/snmp-overview.json`, locate the `templating.list` array (lines 13-63). The current second variable ends at line 62 (`}`) followed by line 63 (`]`). Insert a comma after the second variable's closing brace and add the new variable before the `]`.

The `templating.list` array becomes:

```json
  "templating": {
    "list": [
      {
        "name": "instance",
        "label": "Instance",
        "type": "query",
        "datasource": { "type": "prometheus", "uid": "victoriametrics" },
        "query": {
          "query": "label_values(opennms_mib2_x_interfaces_ifhcinoctets_total, instance)",
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
      },
      {
        "name": "foreign_source",
        "label": "Foreign source",
        "type": "query",
        "datasource": { "type": "prometheus", "uid": "victoriametrics" },
        "query": {
          "query": "label_values(opennms_mib2_x_interfaces_ifhcinoctets_total{instance=~\"$instance\"}, foreign_source)",
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
      },
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
    ]
  },
```

(Note the inner-datasource shorthand `{ "type": ..., "uid": ... }` — preserve exactly to match the existing file's style.)

- [ ] **Step 2: Update all 6 panel `expr` lines to include the location selector**

For each panel's `expr`, append `, location=~"$monitoring_location"` to the existing label selector. The 6 panels' `expr` lines:

**Panel 1 (line ~78) — interface throughput in:**

OLD:
```
"expr": "topk(10, rate(opennms_mib2_x_interfaces_ifhcinoctets_total{instance=~\"$instance\", foreign_source=~\"$foreign_source\"}[5m]) * 8)",
```
NEW:
```
"expr": "topk(10, rate(opennms_mib2_x_interfaces_ifhcinoctets_total{instance=~\"$instance\", foreign_source=~\"$foreign_source\", location=~\"$monitoring_location\"}[5m]) * 8)",
```

**Panel 2 (line ~110) — interface throughput out:**

OLD:
```
"expr": "topk(10, rate(opennms_mib2_x_interfaces_ifhcoutoctets_total{instance=~\"$instance\", foreign_source=~\"$foreign_source\"}[5m]) * 8)",
```
NEW:
```
"expr": "topk(10, rate(opennms_mib2_x_interfaces_ifhcoutoctets_total{instance=~\"$instance\", foreign_source=~\"$foreign_source\", location=~\"$monitoring_location\"}[5m]) * 8)",
```

**Panel 3 (line ~142) — interface error rate (4 rate() additions):**

OLD:
```
"expr": "rate(opennms_mib2_interface_errors_ifinerrors_total{instance=~\"$instance\", foreign_source=~\"$foreign_source\"}[5m]) + rate(opennms_mib2_interface_errors_ifouterrors_total{instance=~\"$instance\", foreign_source=~\"$foreign_source\"}[5m]) + rate(opennms_mib2_interface_errors_ifindiscards_total{instance=~\"$instance\", foreign_source=~\"$foreign_source\"}[5m]) + rate(opennms_mib2_interface_errors_ifoutdiscards_total{instance=~\"$instance\", foreign_source=~\"$foreign_source\"}[5m])",
```
NEW (each of the 4 selector groups gets the location addition):
```
"expr": "rate(opennms_mib2_interface_errors_ifinerrors_total{instance=~\"$instance\", foreign_source=~\"$foreign_source\", location=~\"$monitoring_location\"}[5m]) + rate(opennms_mib2_interface_errors_ifouterrors_total{instance=~\"$instance\", foreign_source=~\"$foreign_source\", location=~\"$monitoring_location\"}[5m]) + rate(opennms_mib2_interface_errors_ifindiscards_total{instance=~\"$instance\", foreign_source=~\"$foreign_source\", location=~\"$monitoring_location\"}[5m]) + rate(opennms_mib2_interface_errors_ifoutdiscards_total{instance=~\"$instance\", foreign_source=~\"$foreign_source\", location=~\"$monitoring_location\"}[5m])",
```

**Panel 4 (line ~182) — CPU load per processor:**

OLD:
```
"expr": "opennms_mib2_host_resources_processor_hrprocessorload{instance=~\"$instance\", foreign_source=~\"$foreign_source\"}",
```
NEW:
```
"expr": "opennms_mib2_host_resources_processor_hrprocessorload{instance=~\"$instance\", foreign_source=~\"$foreign_source\", location=~\"$monitoring_location\"}",
```

**Panel 5 (line ~216) — storage utilization (two metric references):**

OLD:
```
"expr": "100 * opennms_mib2_host_resources_storage_hrstorageused{instance=~\"$instance\", foreign_source=~\"$foreign_source\"} / opennms_mib2_host_resources_storage_hrstoragesize{instance=~\"$instance\", foreign_source=~\"$foreign_source\"}",
```
NEW:
```
"expr": "100 * opennms_mib2_host_resources_storage_hrstorageused{instance=~\"$instance\", foreign_source=~\"$foreign_source\", location=~\"$monitoring_location\"} / opennms_mib2_host_resources_storage_hrstoragesize{instance=~\"$instance\", foreign_source=~\"$foreign_source\", location=~\"$monitoring_location\"}",
```

**Panel 6 (line ~255) — devices monitored:** This panel does NOT take a label-filter input (it uses `count by (...)` to aggregate). Leave the inner expression unfiltered, but add `location` to the `count by (...)` GROUPING clause:

OLD:
```
"expr": "count by (instance, foreign_source, snmp_syscontact, snmp_syslocation) (rate(opennms_mib2_x_interfaces_ifhcinoctets_total[5m]))",
```
NEW:
```
"expr": "count by (instance, foreign_source, location, snmp_syscontact, snmp_syslocation) (rate(opennms_mib2_x_interfaces_ifhcinoctets_total[5m]))",
```

- [ ] **Step 3: Update the Devices-monitored panel's transformations to add the Location column rename**

Find the `transformations[].options.renameByName` block (around line 280-285). Add `"location": "Monitoring location"` to the rename map. The full transformations block becomes:

```json
      "transformations": [
        {
          "id": "organize",
          "options": {
            "renameByName": {
              "instance": "Instance",
              "foreign_source": "Foreign source",
              "location": "Monitoring location",
              "snmp_syscontact": "SNMP sysContact",
              "snmp_syslocation": "SNMP sysLocation"
            }
          }
        }
      ]
```

- [ ] **Step 4: Validate JSON syntax**

```
python3 << 'EOF'
import json
d = json.load(open("opennms-container/delta-v/grafana/dashboards/snmp-overview.json"))
assert d["uid"] == "snmp-overview"
assert len(d["panels"]) == 6, f"expected 6 panels, got {len(d['panels'])}"
assert len(d["templating"]["list"]) == 3, f"expected 3 templates, got {len(d['templating']['list'])}"
template_names = [v["name"] for v in d["templating"]["list"]]
assert "monitoring_location" in template_names, f"missing monitoring_location var: {template_names}"
# Confirm all 6 panels mention monitoring_location in their expr
for i, p in enumerate(d["panels"]):
    expr = p["targets"][0]["expr"]
    if i == 5:  # devices-monitored uses count-by-location instead of label-filter
        assert ", location," in expr or "(location," in expr or " location," in expr, f"panel 6 expr missing location grouping: {expr}"
    else:
        assert "$monitoring_location" in expr, f"panel {i+1} expr missing monitoring_location filter: {expr}"
print(f"OK uid=snmp-overview panels=6 templates=3 monitoring_location wired in all 6 panels")
EOF
```

Expected: `OK uid=snmp-overview panels=6 templates=3 monitoring_location wired in all 6 panels`.

- [ ] **Step 5: Commit**

```
git add opennms-container/delta-v/grafana/dashboards/snmp-overview.json
git commit -m "feat(mock-lab): add monitoring_location template variable + Location column

Dashboard SNMP Overview gains a third template variable (Monitoring location)
matching the OpenNMS UI long-form terminology. All 6 panels' PromQL gain a
location=~'\$monitoring_location' selector. Devices-monitored table groups by
location too and renames the column to 'Monitoring location'. Operators see
both Default (rpc-canary + snmp-agent) and l8opensim-lab side by side, with
one-click filtering."
```

---

### Task 10: Update `test-prometheus-writer-e2e.sh` — add Step 10, possibly bump POLL_GRACE_SECONDS

**Files:**
- Modify: `opennms-container/delta-v/test-prometheus-writer-e2e.sh`

Two changes:
1. Bump `POLL_GRACE_SECONDS` IF Task 8's empirical measurement showed `> 90s` to first l8opensim-lab data. Bump to the smallest value that comfortably covers the measurement (round up to nearest 30: 120, 150, 180).
2. Add Step 10 between Step 9 (Dashboard OK) and the final `exit 0`.

- [ ] **Step 1 (conditional): Bump `POLL_GRACE_SECONDS` if Task 8 measured > 90s**

Find line `POLL_GRACE_SECONDS=90` (around line 35). Replace 90 with the value derived from Task 8:

- If Task 8 first-data timestamp was ≤ 90s: keep `POLL_GRACE_SECONDS=90`.
- If 91-120s: set `POLL_GRACE_SECONDS=120`.
- If 121-150s: set `POLL_GRACE_SECONDS=150`.
- If 151-180s: set `POLL_GRACE_SECONDS=180`.
- If > 180s: stop and investigate why; do NOT bump beyond 180. The compose stack should not need more than 3 minutes for first poll cycle of 21 nodes; longer suggests a deeper issue (Twin API not delivering, RPC routing broken, etc.).

- [ ] **Step 2: Add Step 10 after Step 9 (Dashboard OK)**

In `opennms-container/delta-v/test-prometheus-writer-e2e.sh`, find the lines:

```bash
echo "==> ALL ASSERTIONS PASSED"
exit 0
```

Insert this block IMMEDIATELY BEFORE those two lines:

```bash

# ── Step 10: Verify l8opensim-lab location is producing metrics ───────────────
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

- [ ] **Step 3: Verify the script is syntactically valid**

```
bash -n opennms-container/delta-v/test-prometheus-writer-e2e.sh && echo OK
```
Expected: `OK`.

- [ ] **Step 4: Commit**

```
git add opennms-container/delta-v/test-prometheus-writer-e2e.sh
git commit -m "test(mock-lab): add Step 10 asserting l8opensim-lab data flow

Asserts opennms_mib2_x_interfaces_ifhcinoctets_total{foreign_source=l8opensim-lab}
returns at least one series. Catches: minion-lab Kafka registration
failure, l8opensim-provisioner not creating devices, shared-netns wiring
break, collectd-RPC dispatch missing the new location's Minion. POLL_GRACE_SECONDS
adjusted to <Task 8 measurement> based on empirical first-data timing."
```

---

### Task 11: Full E2E verification gate

**No file changes** — runs the extended `test-prometheus-writer-e2e.sh` end-to-end.

- [ ] **Step 1: Run the full E2E**

```
bash opennms-container/delta-v/test-prometheus-writer-e2e.sh 2>&1 | tee /tmp/l8opensim-e2e.log | grep -E "==> Step|FAIL|ALL ASSERTIONS|deltav_prometheus_writer_(records|samples|batches)" | tail -30
```

Expected: all 10 steps pass, final lines `==> Dashboard OK`, `==> VM returned ... series for l8opensim-lab`, `==> ALL ASSERTIONS PASSED`. The full E2E with l8opensim brought-up will take 4-6 minutes (longer than today's E2E because of the 21 nodes' node-scan + first-poll-cycle latency).

- [ ] **Step 2: If anything fails, debug from logs**

Common failure modes:
- **Step 4 `records_consumed = 0`**: Provisiond didn't import. Check `docker compose logs provisiond | grep -i 'l8opensim-lab\|import'`.
- **Step 6 timeout on `ifindiscards_total`**: rpc-canary path broke (regression in something we changed). Check `docker compose logs collectd minion`.
- **Step 10 timeout on `l8opensim-lab`**: minion-lab not polling. Check `docker compose logs minion-lab` for registration / RPC errors. Re-run Task 8's diagnostic measurement loop.

- [ ] **Step 3: Discard requisition drift after the run**

```
git checkout -- opennms-container/delta-v/provisiond-overlay/etc/imports/ 2>&1 || true
git status --short
```

Expected: empty output (no leftover changes).

(No commit. This task is a gate.)

---

### Task 12: Push branch + open PR

- [ ] **Step 1: Push branch with upstream tracking**

```
git push -u origin feat/l8opensim-mock-lab 2>&1 | tail -5
```
Expected: branch created on `pbrane/delta-v` (origin remote per project convention).

- [ ] **Step 2: Open the PR — `--repo pbrane/delta-v` is non-negotiable**

```
gh pr create --repo pbrane/delta-v --base develop --head feat/l8opensim-mock-lab \
  --title "feat(mock-lab): add l8opensim simulator + minion-lab + monitoring_location dashboard variable" \
  --body "$(cat <<'EOF'
## Summary

Adds a richer mock SNMP environment to Delta-V's docker-compose stack and demonstrates multi-location monitoring without requiring any external lab connectivity.

### What ships

- **`l8opensim` service** (`ghcr.io/labmonkeys-space/l8opensim:latest`) — simulates 20 network devices across all 8 categories (4 routers, 5 switches, 2 firewalls, 4 servers, 2 GPU servers, 3 storage) on per-device TUN interfaces at `10.0.0.1-20`. Modern HC interface counters with realistic dynamic values.
- **`l8opensim-provisioner`** one-shot init container — POSTs `opennms-container/delta-v/l8opensim/devices.json` to l8opensim's REST API after the simulator is healthy.
- **`minion-lab` service** — second Minion at `location=l8opensim-lab` sharing l8opensim's network namespace. Honors the project's Minion-mandatory tenet: SNMP polls go through this Minion via Kafka RPC, not collectd's JVM. (Empirically verified during brainstorming via tcpdump on collectd's vs Minion's netns.)
- **New `l8opensim-lab` requisition + collectd package** — 20 nodes auto-imported every 30s, polled at 30s SNMP interval.
- **`mhuot-labs` auto-import disabled** — requisition file at `imports/mhuot-labs.xml` preserved on disk; the `<requisition-def>` block in `provisiond-configuration.xml` is commented out with an explanatory note. Operators wanting LLDP topology testing against real hardware uncomment to re-enable.
- **Dashboard `monitoring_location` template variable** — third dropdown alongside Instance and Foreign source, matching OpenNMS's "Monitoring Location" UI long-form. All 6 panels filter by it. Devices-monitored table grows a "Monitoring location" column.
- **E2E Step 10** — asserts `opennms_mib2_x_interfaces_ifhcinoctets_total{foreign_source="l8opensim-lab"}` returns ≥ 1 series. Catches regressions in the l8opensim-lab data path that today's Step 6 (rpc-canary) wouldn't.

### Backward compatibility

| Change | Impact |
|---|---|
| New l8opensim, l8opensim-provisioner, minion-lab services | Additive (lite/full/metrics profiles). Operators not opting into those profiles see no change. |
| New requisition + collectd package | Additive. Existing requisitions unaffected. |
| **mhuot-labs auto-import commented out** | **Behavior change at upgrade.** Operators relying on auto-imported mhuot-labs nodes for LLDP testing must uncomment the `<requisition-def>` block in `provisiond-configuration.xml`. The requisition file at `imports/mhuot-labs.xml` is preserved. |
| Dashboard monitoring_location variable + Location column | Additive. Existing dashboard queries still work; new variable defaults to "All". |
| E2E Step 10 added | Additive. Steps 1-9 still pass even if Step 10 fails (only the final `exit 0` doesn't fire). Effectively a stricter test. |

### Architectural verification (from brainstorming)

Empirically verified via `tcpdump` on collectd's vs Minion's network namespaces during a live stack run:

| Container | UDP port 161 packets in 90s |
|---|---|
| `delta-v-collectd` | **0 packets** |
| `delta-v-minion` | **10 packets** (filled buffer immediately) |

Conclusion: `LocationAwareSnmpClientRpcImpl` IS dispatching SNMP walks to Minion via Kafka RPC. The `force-remote=false` setting in collectd's boot config controls only the Collect orchestration RPC, not individual SNMP walks. Adding minion-lab at the new location is the architecturally-correct way to extend the demo.

Spec: `docs/superpowers/specs/2026-04-20-l8opensim-mock-lab-design.md`. Plan: `docs/superpowers/plans/2026-04-20-l8opensim-mock-lab-plan.md`.

## Test plan

- [x] `bash opennms-container/delta-v/test-prometheus-writer-e2e.sh` — green (steps 1-10).
- [x] Manual visual verification of Grafana dashboard:
  - Monitoring location dropdown lists `Default` AND `l8opensim-lab`.
  - Filter to `l8opensim-lab`: interface throughput / errors / storage panels populate.
  - Filter to `Default`: rpc-canary `snmp-agent-canary` data renders (regression check).
  - Devices-monitored table shows ~21 rows with the new "Monitoring location" column populated.

## Out of scope (queued follow-ups)

- LLDP-MIB upstream contribution to `labmonkeys-space/l8opensim` so even Enlinkd topology testing moves off mhuot-labs.
- Wire l8opensim's flow exporters (NetFlow / IPFIX / sFlow) — three follow-up PRs.
- Wire l8opensim's SNMP trap exporter (Trapd scale).
- Wire l8opensim's UDP syslog exporter (Syslogd scale).
- Add `device_type` Prometheus label (provisiond-side metadata adapter).
- Once l8opensim has interface-errors + host-resources MIB support, retire the original `mock-snmp-agent` service.

EOF
)" 2>&1 | tail -3
```

Expected: GitHub CLI returns the PR URL.

- [ ] **Step 3: Print the PR URL**

The `gh pr create` command prints the URL on success. Echo it back so the user can open it.

---

## Self-Review Checklist (run after writing the plan; not a separate task)

The following list pins what I checked when writing this plan. Re-verify if the plan is changed.

**1. Spec coverage**

| Spec section | Implementing task |
|---|---|
| New `l8opensim` service | Task 5 |
| New `l8opensim-provisioner` service | Task 5 |
| New `minion-lab` service (port 8181, shared netns) | Task 7 |
| `provisiond-overlay/etc/imports/l8opensim-lab.xml` | Task 2 |
| `provisiond-overlay/etc/provisiond-configuration.xml` (add l8opensim-lab requisition-def, comment mhuot-labs) | Task 4 |
| `collectd-daemon-overlay/etc/collectd-configuration.xml` (add l8opensim-lab package) | Task 3 |
| New `l8opensim/` directory with `devices.json` + `post-each.sh` | Task 1 |
| Dashboard JSON edits | Task 9 |
| E2E script edits | Task 10 |
| Empirical POLL_GRACE_SECONDS measurement | Task 8 (measured) → Task 10 (applied) |
| Full E2E green | Task 11 |
| PR opened `--repo pbrane/delta-v` | Task 12 |

All scope items have an task. Spec out-of-scope items (LLDP contribution, flow/trap/syslog wiring, device_type label, mock-snmp-agent retirement) are absent from the plan.

**2. Placeholder scan:** the only `<placeholder>`-style tokens in the plan are intentional: in Task 1's Scenario A devices.json, `<core_router_name>` etc. are MEANT to be filled in based on the empirical API spike outcome. Everything else is concrete.

**3. Type/name consistency**:
- Service names: `l8opensim`, `l8opensim-provisioner`, `minion-lab` consistent across Tasks 5, 7, 12.
- Container names: `delta-v-l8opensim`, `delta-v-l8opensim-provisioner`, `delta-v-minion-lab` consistent.
- Foreign source: `l8opensim-lab` consistent across Tasks 2, 3, 4, 9, 10.
- Location: `l8opensim-lab` consistent across Tasks 2, 7, 9, 10.
- Port: `8181` for minion-lab actuator (Task 7) matched in healthcheck (Task 7) and shared-netns notes.
- Profile membership: `[lite, full, metrics]` consistent across Tasks 5, 7.
- Provisiond cron: `0/30 * * * * ?` matches existing requisitions in the file (Task 4).
- Collectd interval: 30000ms matches example1 package set by PR #182 (Task 3).
- Dashboard variable: `monitoring_location` consistent in Task 9 across template definition + 6 panel updates + transformations rename.

All names match across tasks.
