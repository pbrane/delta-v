# nl6 per-device flow exporter attribution

## What this guarantees

Each simulated nl6 device exports flow (and trap/syslog) telemetry from **its own
`10.0.0.x` source IP**, so OpenNMS records a distinct flow exporter per device and
maps it to a distinct node — instead of collapsing every device onto the nl6
container bridge IP (`172.18.0.30`).

Verify in ClickHouse:

```sql
SELECT host, exporter_node_id, count()
FROM deltav.flows_raw
WHERE host LIKE '10.0.0.%'
GROUP BY host, exporter_node_id
ORDER BY host;
```

Expect ~29 distinct `10.0.0.x` `host` values mapped to distinct `exporter_node_id`s.

## Why the configuration is the way it is (load-bearing constraints)

OpenNMS derives the exporter identity from the **L3 source IP of the UDP datagram**
at the Minion's flow listener — it does not read an exporter address from the
flow payload. So per-device attribution depends entirely on the on-wire source IP.

1. **nl6 MUST run in namespace mode** (do NOT pass `-no-namespace`).
   In namespace mode nl6 builds an `opensim` netns holding the per-device TUN
   interfaces (`10.0.0.x` on `simN`), joined to the container root netns by a
   veth pair: `veth-sim-ns (10.254.0.2)` in opensim ⟷ `veth-sim-host (10.254.0.1)`
   in root. `-flow-source-per-device` (default true) then binds a per-device UDP
   socket inside opensim, so each packet carries the device's own `10.0.0.x` IP.
   Under `-no-namespace` all devices share the root netns and the source falls
   back to the egress interface (`172.18.0.30`) for every device — the original
   bug.

   **The nl6 service MUST run `privileged: true`.** Creating the `opensim` netns
   does `mount --make-shared /var/run/netns`, which fails with "Permission
   denied" under plain `cap_add: [NET_ADMIN, SYS_ADMIN]`. When that mount fails
   nl6 silently falls back to the root netns (the `-no-namespace` failure mode
   above) and emits **zero** attributable flows, while its `/health` endpoint
   still reports healthy — so the regression is invisible unless you check
   ClickHouse. A narrower grant may suffice later, but `privileged` is the
   proven baseline.

2. **Collector MUST be the veth host end `10.254.0.1`** (where `minion-lab`,
   sharing nl6's root netns via `network_mode: container:delta-v-nl6`, listens on
   `:4729`/`:1162`/`:1514`). There is no NAT on the veth, so the `10.0.0.x`
   source survives to the collector. Do **not** target `minion:4729` (the Default
   minion across the Docker bridge): `10.0.0.x`-sourced packets cannot traverse
   the bridge to it.

3. **`./nl6/devices.json` is the source of truth for the collector.** Its
   per-device `collector` fields override nl6's CLI `-flow-collector` flag, so
   both must point at `10.254.0.1`. (Proof: when the CLI said `127.0.0.2` but
   devices.json said `minion:4729`, all flows arrived at the Default minion.)

4. **SNMP polling still works** across the veth: `minion-lab` (root netns) polls
   each device at `10.0.0.x:161` in opensim; the symmetric root route
   `10.0.0.0/24 via 10.254.0.2 dev veth-sim-host` keeps this reachable.

## Operations: recreate nl6 and minion-lab together

`minion-lab` joins nl6's netns via `network_mode: container:delta-v-nl6`, pinned
**by container ID**. Any nl6 recreate (image bump, command/config change)
invalidates that reference: `minion-lab` exits and `docker restart` cannot
recover it (`joining network namespace of container: No such container`). Since
all nl6 flows route through `minion-lab`, an unnoticed nl6 recreate silently
stops the entire flow pipeline. Always recreate the pair together:

```sh
docker compose up -d --force-recreate nl6 minion-lab
```

## Known caveats

- Flows currently land with `location=Default` rather than `nl6-lab`; the minion
  location label does not survive the minion-gateway Sink path in this build. Node
  attribution is unaffected (lookup is by exporter IP).
- Devices `10.0.0.21`–`10.0.0.29` may show `exporter_node_id=0` (unmapped) until a
  requisition covering those IPs is imported. The running stack's `nl6-lab`
  requisition historically covered only `10.0.0.1`–`10.0.0.20`; the committed
  `provisiond-overlay/etc/imports-seed/nl6-lab.xml` (foreign-source `nl6`) defines
  all 29. These two requisitions have diverged — reconcile before relying on clean
  1-node-per-device mapping for the full fleet.
