#!/usr/bin/env python3
# Copyright 2026 Ronny Trommer <ronny@no42.org>
# SPDX-License-Identifier: Apache-2.0
#
# gen-fabric.py — delta-v fork of nl6's examples/large-clos/gen-clos.py.
#
# Generates the delta-v 5-stage Clos (folded 3-tier / k-ary Al-Fares fat-tree)
# and emits the three artifacts the nl6-provisioner consumes:
#
#   topology     {"links":[...]} graph  -> POST /api/v1/topology
#   devices      batch-manifest array   -> fleet.sh import -> POST /api/v1/devices
#   requisition  OpenNMS model-import   -> imports-seed-nl6/nl6-lab.xml
#   summary      one-line device/link count (default)
#   write <dir>  write clos.json + devices.json + nl6-lab.xml into <dir>
#
# Differences from upstream gen-clos.py:
#   - CLOS_K defaults to 4 (36 devices / 48 links), not 20.
#   - IP plan rebased onto delta-v's 10.0.0.0/24 (was 10.42.0.0/16), so the
#     dns-lab PTR synthesis and the flow E2E (which key on 10.0.0.x exporters)
#     keep matching.
#   - Devices carry delta-v's per-device flow/trap/syslog collectors (10.254.0.1)
#     so per-device flow attribution survives the device-set swap.
#   - Emits the OpenNMS requisition directly, pinning every node to ONE city
#     (single datacenter on the geomap); gNMI is intentionally omitted.
#
#   core  = (k/2)^2          aggregation = k * (k/2)
#   edge  = k * (k/2)        hosts       = k^3 / 4
#
# Every switch uses ports 1..k, so k must not exceed the smallest switch
# interface table — the Arista 7280R3 (agg tier) with 32 ports. k <= 32 keeps
# every link resolvable to a real ifDescr.

import ipaddress
import json
import os
import sys
from xml.sax.saxutils import quoteattr

MAX_K = 32  # Arista 7280R3 port count — the tightest switch interface table.

_raw_k = os.environ.get("CLOS_K", "4")
try:
    K = int(_raw_k)
except ValueError:
    sys.exit("CLOS_K must be a positive even integer (got %r)" % _raw_k)
if K < 2 or K % 2 != 0:
    sys.exit("CLOS_K must be a positive even integer (got %d)" % K)
if K > MAX_K:
    sys.exit("CLOS_K must be <= %d (Arista 7280R3 has %d ports; got %d)"
             % (MAX_K, MAX_K, K))
HALF = K // 2

N_CORE = HALF * HALF       # (k/2)^2
N_AGG = K * HALF           # k pods * k/2 aggregation
N_EDGE = K * HALF          # k pods * k/2 edge
N_HOST = K * HALF * HALF   # k^3 / 4

# Per-tier base IP within delta-v's 10.0.0.0/24. Bases are spaced so the tiers
# never collide at k<=8 (k=4 default: core .1-.4, agg .21-.28, edge .41-.48,
# host .61-.76 — all inside the /24).
BASE = {"core": "10.0.0.1", "agg": "10.0.0.21", "edge": "10.0.0.41", "host": "10.0.0.61"}
RES = {
    "core": "cisco_crs_x.json",
    "agg": "arista_7280r3.json",
    "edge": "cisco_catalyst_9500.json",
    # NOT linux_server.json: that nl6 resource file serves a degenerate system
    # group (sysObjectID = 0.0/ccitt, no sysDescr — "OID not supported"), so
    # provisiond stores NULL sysObjectID for every host. dell_poweredge_r750 is
    # a real compute-leaf model with a valid sysObjectID (enterprises.674...).
    "host": "dell_poweredge_r750.json",
}

# delta-v per-device export collectors — the veth host end where nl6-minion
# listens (shared netns). Mirrors the blocks the legacy devices.json carried.
NETMASK = "24"
FLOW = {"collector": "10.254.0.1:4729", "protocol": "netflow9",
        "tick_interval": "5s", "active_timeout": "30s", "inactive_timeout": "15s"}
TRAPS = {"collector": "10.254.0.1:1162", "mode": "trap", "community": "public",
         "interval": "30s", "inform_timeout": "5s", "inform_retries": 2}
SYSLOG = {"collector": "10.254.0.1:1514", "format": "5424", "interval": "10s"}

# Single datacenter — every node pinned to one site on the OpenNMS geomap.
DC_CITY = "Montepertuso, 84017 SA, Italy"
DC_LAT = "40.631895"
DC_LON = "14.490686"

FOREIGN_SOURCE = "nl6"
LOCATION = "nl6-lab"


def ip(tier, idx):
    return str(ipaddress.IPv4Address(int(ipaddress.IPv4Address(BASE[tier])) + idx))


def core_id(j, i):     # core group j (0..HALF-1), member i (0..HALF-1)
    return j * HALF + i


def agg_id(p, a):      # pod p, aggregation switch a
    return p * HALF + a


def edge_id(p, e):     # pod p, edge switch e
    return p * HALF + e


def host_id(p, e, h):  # pod p, edge e, host h
    return (p * HALF + e) * HALF + h


def links():
    out = []
    # Edge <-> aggregation: full mesh inside each pod. Edge uplink ports 1..k/2,
    # aggregation downlink ports 1..k/2.
    for p in range(K):
        for e in range(HALF):
            for a in range(HALF):
                out.append({
                    "a": {"ip": ip("edge", edge_id(p, e)), "ifindex": 1 + a},
                    "b": {"ip": ip("agg", agg_id(p, a)), "ifindex": 1 + e},
                })
    # Aggregation <-> core: aggregation switch a connects to core group a; each
    # core uses port (pod+1), one per pod. Aggregation uplink ports k/2+1..k.
    for p in range(K):
        for a in range(HALF):
            for i in range(HALF):
                out.append({
                    "a": {"ip": ip("agg", agg_id(p, a)), "ifindex": HALF + 1 + i},
                    "b": {"ip": ip("core", core_id(a, i)), "ifindex": 1 + p},
                })
    # Edge <-> host: each edge switch fans out to k/2 hosts on downlink ports
    # k/2+1..k; the host uses eth0 (ifIndex 2).
    for p in range(K):
        for e in range(HALF):
            for h in range(HALF):
                out.append({
                    "a": {"ip": ip("edge", edge_id(p, e)), "ifindex": HALF + 1 + h},
                    "b": {"ip": ip("host", host_id(p, e, h)), "ifindex": 2},
                })
    return out


def tiers():
    return [
        ("core", BASE["core"], N_CORE, RES["core"]),
        ("agg", BASE["agg"], N_AGG, RES["agg"]),
        ("edge", BASE["edge"], N_EDGE, RES["edge"]),
        ("host", BASE["host"], N_HOST, RES["host"]),
    ]


def devices_manifest():
    # One batch-manifest entry per tier; fleet.sh import POSTs each to
    # /api/v1/devices, which fans out device_count sequential IPs from start_ip.
    return [
        {
            "start_ip": base,
            "device_count": count,
            "netmask": NETMASK,
            "resource_file": res,
            "flow": FLOW,
            "traps": TRAPS,
            "syslog": SYSLOG,
        }
        for tier, base, count, res in tiers()
    ]


def node_ips():
    # Deterministic (tier, ip) list matching what nl6 creates from the manifest.
    out = []
    for tier, base, count, _res in tiers():
        for idx in range(count):
            out.append((tier, ip(tier, idx)))
    return out


def requisition():
    lines = ['<?xml version="1.0" encoding="UTF-8"?>']
    lines.append('<model-import xmlns="http://xmlns.opennms.org/xsd/config/model-import"')
    lines.append('              foreign-source=%s>' % quoteattr(FOREIGN_SOURCE))
    for tier, addr in node_ips():
        node_id = "%s-%s" % (tier, addr)
        lines.append('   <node location=%s foreign-id=%s node-label=%s>'
                     % (quoteattr(LOCATION), quoteattr(node_id), quoteattr(node_id)))
        lines.append('      <interface ip-addr=%s status="1" snmp-primary="P">' % quoteattr(addr))
        lines.append('         <monitored-service service-name="ICMP"/>')
        lines.append('         <monitored-service service-name="SNMP"/>')
        lines.append('      </interface>')
        # Single-DC geolocation — identical on every node (geomap shows one site).
        lines.append('      <asset name="latitude" value=%s/>' % quoteattr(DC_LAT))
        lines.append('      <asset name="longitude" value=%s/>' % quoteattr(DC_LON))
        lines.append('      <asset name="city" value=%s/>' % quoteattr(DC_CITY))
        lines.append('   </node>')
    lines.append('</model-import>')
    return "\n".join(lines) + "\n"


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "summary"
    if cmd == "topology":
        json.dump({"links": links()}, sys.stdout, indent=2)
        sys.stdout.write("\n")
    elif cmd == "devices":
        json.dump(devices_manifest(), sys.stdout, indent=2)
        sys.stdout.write("\n")
    elif cmd == "requisition":
        sys.stdout.write(requisition())
    elif cmd == "summary":
        total = N_CORE + N_AGG + N_EDGE + N_HOST
        print("k=%d fat-tree: %d core + %d agg + %d edge + %d hosts = %d devices, %d links"
              % (K, N_CORE, N_AGG, N_EDGE, N_HOST, total, len(links())))
    elif cmd == "write":
        if len(sys.argv) < 3:
            sys.exit("usage: gen-fabric.py write <component-dir> [<requisition-path>]")
        comp_dir = sys.argv[2]
        with open(os.path.join(comp_dir, "clos.json"), "w") as f:
            json.dump({"links": links()}, f, indent=2)
            f.write("\n")
        with open(os.path.join(comp_dir, "devices.json"), "w") as f:
            json.dump(devices_manifest(), f, indent=2)
            f.write("\n")
        req_path = sys.argv[3] if len(sys.argv) > 3 else os.path.join(comp_dir, "nl6-lab.xml")
        with open(req_path, "w") as f:
            f.write(requisition())
        total = N_CORE + N_AGG + N_EDGE + N_HOST
        print("wrote clos.json, devices.json (%d devices), %s (%d nodes), %d links"
              % (total, req_path, total, len(links())))
    else:
        sys.exit("usage: gen-fabric.py [topology|devices|requisition|summary|write <dir>]")


if __name__ == "__main__":
    main()
