# Pollerd Flat Service Catalog — Operations Guide

How the main poller (`pollerd`) is configured in delta-v, and how to diagnose it at 3am.

Pollerd no longer reads `poller-configuration.xml`. It is driven by a **flat service-definition
catalog** — `etc/poller-services.yaml` — baked into the image at build time. Service *selection*
is inventory-driven: pollerd polls every monitored service that exists in inventory; there are no
packages, filters, include-ranges, or downtime models in the config.

## 1. The catalog format

`deploy/overlays/pollerd/etc/poller-services.yaml`:

```yaml
services:
  # Simplest possible definition — exact service name, monitor, interval (ms).
  - name: ICMP
    monitor: org.opennms.netmgt.poller.monitors.IcmpMonitor
    interval: 300000

  # With monitor parameters. Values are opaque strings; metadata DSL chains
  # (${requisition:...|detector:...|default}) round-trip verbatim — never translated.
  - name: HTTP
    monitor: org.opennms.netmgt.poller.monitors.HttpMonitor
    interval: 300000
    parameters:
      retry: "${requisition:poller-retry|requisition:retry|detector:retry|1}"
      timeout: "${requisition:poller-timeout|requisition:timeout|detector:timeout|3000}"
      port: "${requisition:port|detector:port|80}"
      url: "${requisition:url|detector:url|/}"

  # A dynamic service family: `pattern` (regex) matches many inventory service
  # names with one definition. Capture groups feed the engine's pattern variables.
  - name: PTP
    pattern: "^PTP-.*$"
    monitor: org.opennms.netmgt.poller.monitors.PtpMonitor
    interval: 300000

  # Nested-XML parameters (e.g. page-sequence) are first-class block scalars.
  - name: Deltav-Health
    monitor: org.opennms.netmgt.poller.monitors.PageSequenceMonitor
    interval: 30000
    parameters:
      page-sequence: |
        <page-sequence>
          <page host="${nodelabel}" path="/actuator/health" port="8080"
                response-range="200-299"/>
        </page-sequence>

  # Type-level kill switch: omit a type from scheduling without deleting it.
  - name: NRPE
    monitor: org.opennms.netmgt.poller.monitors.NrpeMonitor
    interval: 300000
    enabled: false
```

Fields per entry: `name` (required; the exact inventory service name and the synthetic package
name), `pattern` (optional regex matching a family), `monitor` (required FQCN), `interval`
(required, milliseconds; warn below 5000), `enabled` (defaults `true`), `parameters` (verbatim
monitor parameters).

**Resolution precedence:** an exact `name` match beats any `pattern`; among matching patterns,
the first in file order wins. Duplicate names and identical patterns are rejected by the linter.

**Dropped vs. the legacy XML** (no behavior change): downtime models, `<rrd>` blocks,
`rrd-repository`/`rrd-base-name`/`ds-name`, `thresholding-enabled`, `user-defined`, and all
filters/include-ranges. Polling is fixed-interval regardless of state; recovery latency is one
poll interval.

## 2. Diagnosing "service X is not being polled"

Work the three layers in order — inventory, then catalog resolution, then the gap signal.

**Step 1 — Is the service actually in inventory and active?**
A service is polled iff its `ifservices.status = 'A'` (Managed). Check it:

```sql
SELECT n.nodelabel, svc.servicename, s.status
  FROM ifservices s
  JOIN service svc ON s.serviceid = svc.serviceid
  JOIN ipinterface ip ON s.ipinterfaceid = ip.id
  JOIN node n ON ip.nodeid = n.nodeid
 WHERE svc.servicename = 'X';
```

`status` other than `A` (e.g. `F` forced-unmanaged, `U` unmanaged, `D` deleted) means the service
is excluded **by inventory state**, not by the catalog. Fix it through provisioning/REST, not the
catalog.

**Step 2 — Does the catalog resolve the service type?**
The type must match a definition in `poller-services.yaml` (exact name, or a `pattern`), the
definition must be `enabled`, and its `monitor` class must be one pollerd supports. A definition
whose monitor class is **not** in pollerd's `ServiceMonitorRegistry` cannot be polled — pollerd
throws *"Monitor not found"* on every attempt, producing a false DOWN.

**Step 3 — Consult the config-gap signal (FR9).**
On startup (and every 5 minutes) pollerd logs exactly one summary line:

```
catalog-summary scheduled=N types=M unresolved=K unresolved-types=[A, B, ...]
```

and publishes a labeled gauge for each unschedulable type:

```
deltav_pollerd_services_unscheduled{service="X"} 1
```

If `X` appears there, it has **no catalog definition** or **a missing monitor class**. Add or fix
the definition (or register the monitor). A type intentionally turned off with `enabled: false` is
*excluded* from the gauge (disabled by choice, not a gap). The bundled Grafana alert
(*"Pollerd service type unscheduled"*) and the *"Unscheduled service types"* panel on the **Pollerd
Monitoring** dashboard surface the same signal; "No data" there is healthy.

## 3. Changing the catalog (config-change cycle)

The catalog is **immutable at runtime** — it is baked into the image and validated by a build-time
lint stage that fails the image build on a malformed catalog. There is no runtime reload.

```bash
# 1. Edit the overlay catalog.
$EDITOR deploy/overlays/pollerd/etc/poller-services.yaml

# 2. Lint locally (same check the image build runs; exit 0 = clean).
make lint-catalog CATALOG=deploy/overlays/pollerd/etc/poller-services.yaml

# 3. Rebuild the pollerd image (the lint stage runs again here, unbypassable).
make daemon-image DAEMON=pollerd

# 4. Roll the stack onto the new image.
make up PROFILE=full
```

## 4. Behavior change: runtime reload is unsupported

In legacy OpenNMS, editing `poller-configuration.xml` and triggering `reloadDaemonConfig` re-read
the file live. **That is gone.** The catalog is file-based and baked into the image; `update()` and
`save()` are deliberately neutralized so the running engine never re-reads or overwrites the YAML.
Any catalog change requires the rebuild-and-roll cycle in §3. Plan changes as deployments, not live
edits.

## Related

- Parity audit (legacy XML → YAML): `docs/audits/2026-06-14-pollerd-catalog-parity-audit.md`
- Restart-during-outage E2E: `deploy/test-pollerd-restart-outage-e2e.sh`
