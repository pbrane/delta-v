# PerspectivePollerd Catalog Content-Parity Audit — 2026-06-17

Epic 3 / PR 3. Verifies that switching PerspectivePollerd from
`deploy/overlays/perspectivepollerd/etc/poller-configuration.xml` to the flat service catalog
`deploy/overlays/shared/poller-services.yaml` changed the **format only**, not the monitored
behavior (NFR7).

PerspectivePollerd is the second (and last) consumer of `poller-configuration.xml`; with this
switch the legacy XML dialect is fully retired from the poller daemons.

## Shared catalog (single source of truth)

PerspectivePollerd and Pollerd now run on the **same** catalog file. The committed source is
`deploy/overlays/shared/poller-services.yaml`; `tools/build.sh` (`stage_shared_poller_catalog`)
copies it into each consumer's overlay (`overlays/{pollerd,perspectivepollerd}/etc/`) at
image-build time, where the per-daemon Dockerfile bakes and lints it. The staged copies are
git-ignored generated artifacts.

This is parity-correct, not a narrowing: PerspectivePollerd consumes the catalog **only** as the
per-service monitor-class + parameter registry, looked up by service name. The set of polled
`(service, perspective-location)` tuples is driven by application membership
(`ApplicationDao.getServicePerspectives()`), never by package filters. The catalog therefore must
remain a **superset** of every service that could become an application/perspective member — which
is exactly the full pollerd service set. The legacy perspective XML carried the same default
service catalogue as pollerd, so the shared catalog is the faithful union.

## Policy

**NFR7 (scope guard):** Format translation only — no service renames, no interval changes beyond
the documented field drops. Same monitor classes, same intervals, same parameter values (modulo
dropped fields), for every retained service.

## Documented field drops (FR5 / FR6)

Identical to the pollerd audit — removed from every definition with **no runtime effect**:

| Dropped | Reason |
|---------|--------|
| `<downtime>` blocks | FR5 — fixed-interval polling; per-package downtime is synthesized by the translator (`begin=0, interval=<service interval>`) |
| `<rrd>` blocks (`step`, `rra`) | FR6 — RRD never adopted; no-op PersisterFactory |
| `rrd-repository`, `rrd-base-name`, `ds-name` params | FR6 — RRD no-op |
| `thresholding-enabled` param | FR6 — no-op ThresholdingService |
| `user-defined` attribute | not part of the flat shape; inventory state, not config |
| `status="on"` attribute | default; flat `enabled` defaults true (no service was `status="off"`) |
| `<filter>`, `<include-range>` | FR2 — selection is inventory-/membership-driven, no filters/ranges |

## Engine root attributes & node-outage (format-only)

The legacy XML root carried `threads="30" asyncPollingEngineEnabled="false"
maxConcurrentAsyncPolls="200" serviceUnresponsiveEnabled="false" pathOutageEnabled="false"` plus a
`<node-outage status="off" pollAllIfNoCriticalServiceDefined="true"><critical-service name="ICMP"/>`
block. These are reproduced exactly with **no behavior change**:

- `threads` / `asyncPollingEngineEnabled` / `maxConcurrentAsyncPolls` → bound from
  `application.yml` (`poller.engine.*`, FR1b three-way split), same defaults.
- `serviceUnresponsiveEnabled=false`, `pathOutageEnabled=false`, and the disabled `node-outage`
  (status `off`, `pollAllIfNoCriticalServiceDefined=true`, critical service `ICMP`) are emitted by
  `CatalogTranslator` as invisible adapter internals. The disabled node-outage element is still
  present so the frozen engine never NPEs dereferencing `getNodeOutage()`.

## FilterDao (FR7)

The legacy `filterDaoInitializer` bean (`JdbcFilterDao` + `FilterDaoFactory` static singleton +
classpath `database-schema.xml`) is removed. `PollerConfigFactory` is now built with a
constructor-injected `InventoryFilterDao` (catch-all over non-deleted inventory IPs), exactly as
pollerd. The `InventoryFilterDao` **is** on the perspective scheduling path: the frozen
`PerspectivePollerd.onServicePerspectiveAdded` selects each service's package via
`isInterfaceInPackage(ip, pkg)` — matched against the IP map built from
`InventoryFilterDao.getActiveIPAddressList` — and `isServiceInPackageAndEnabled(serviceName, pkg)`.
The catch-all supplier returns the same active-IP set the legacy `JdbcFilterDao` did
(`isManaged != 'D'`), so the selection result is unchanged — this is behavior parity, not an inert
swap. The `deploy/overlays/perspectivepollerd/etc/database-schema.xml` overlay file is deleted.

## Explained deltas (service-set changes)

The legacy perspective XML defined **39** services across three packages (`cassandra-via-jmx`,
`example1`, `strafer`). The shared catalog has **32**. The 7 differences:

| Service | Action | Justification |
|---------|--------|---------------|
| `JMX-Cassandra` | **Removed (deprecated)** | JMX/Cassandra monitoring deprecated, out of scope; `Jsr160Monitor` absent from delta-v's registry (could only false-DOWN). |
| `JMX-Cassandra-Newts` | **Removed (deprecated)** | Same as JMX-Cassandra. |
| `NRPE` | **Removed (deprecated)** | NRPE deprecated/unmaintained; `NrpeMonitor` unsupported by the registry. |
| `NRPE-NoSSL` | **Removed (deprecated)** | Same as NRPE. |
| `ActiveMQ` | **Removed (out of scope)** | delta-v is Kafka-first; `ActiveMQMonitor` unsupported by the registry. |
| `JMX-Kafka` | **Dropped (orphan)** | Defined as a `<service>` but had **no `<monitor>` class-name binding** anywhere in the file → never pollable (registry lookup finds nothing). The flat format requires `monitor`; dropping the orphan preserves runtime behavior exactly. |
| `OpenNMS-JVM` | **Dropped (orphan)** | Same as JMX-Kafka — a `<service>` with no `<monitor>` binding, never pollable. (This is the one service the perspective XML carried that pollerd's did not; both were never-pollable orphans.) |

The first five removals match the pollerd audit verbatim (same deprecated/out-of-scope monitors,
all unsupported by the registry). `JMX-Kafka` and `OpenNMS-JVM` are never-pollable orphans, so
dropping them is pure parity. Other still-unsupported types (SMTP, FTP, IMAP, POP3, PTP,
Windows-Task-Scheduler, VMware\*) are retained, identical to pollerd.

## Service-set & interval parity

The 32 retained services, their monitors and intervals are byte-identical to the shared catalog
already validated by the pollerd parity audit (2026-06-14) — same names, monitor classes,
intervals, and the one `pattern` (PTP). The perspective XML's retained services carried the same
monitor/interval/parameter values as pollerd's (both derive from the same upstream default
`poller-configuration.xml`); spot-confirmed for the perspective-relevant shapes below. See the
pollerd audit for the full per-service table.

## Parameter parity

Every retained parameter key/value carried over **verbatim** (no key translation, no value
normalization). Spot-confirmed for the high-risk shapes:

- **Metadata DSL chains** (e.g. `${requisition:poller-timeout|requisition:timeout|detector:timeout|3000}`)
  round-trip byte-identical as double-quoted strings.
- **`page-sequence` nested XML** (Deltav-Health, Google-Search) survives as a block scalar — this
  is the parameter the removed `patchNestedXmlParameters` XML workaround existed to rescue; in the
  flat format it is first-class, so the workaround is deleted.
- **Embedded-quote / regex values** (`response-text: ~.*status.:.green.*` on Elasticsearch)
  preserved via double-quoting.
- **Empty-default DSL values** (`userid`/`password` FTP defaults `|}`) preserved.

The only parameters removed from any definition are the documented FR5/FR6 drops.

## FR9 config-gap observability (perspective-membership-aware)

`PerspectiveCatalogStartupCheck` registers `deltav_perspectivepollerd_services_unscheduled{service=...}`
(per-daemon prefix, mirroring pollerd's `deltav_pollerd_services_unscheduled`) plus a one-line
`perspective-catalog-summary` INFO log at startup, re-evaluated every 5 minutes. Unlike pollerd's
inventory-wide check, the gap universe is the **application-membership** set
(`ApplicationDao.getServicePerspectives()`) — the services PerspectivePollerd actually attempts to
poll. A perspective service type with no matching (enabled) definition, or whose monitor class is
absent from the `ServiceMonitorRegistry`, is reported on the gauge (D4: `enabled:false` types are
excluded — a kill switch must not page). This closes the silent-drop path in the frozen
`onServicePerspectiveAdded` (a mapped-but-uncatalogued service returns without log/metric). A Grafana
alert (`components/grafana/provisioning/alerting/perspectivepollerd-catalog.yaml`) fires on the gauge.

## Validation

`make lint-catalog CATALOG=deploy/overlays/shared/poller-services.yaml` → **exit 0** (clean).
Each daemon image re-lints its staged copy at build time (Dockerfile lint stage), and
`PerspectivePollerdDaemonConfiguration` re-parses+validates at startup (refusing to start on any
ERROR) — defense-in-depth, same as pollerd.

## Behavioral gate

`deploy/test-perspective-e2e.sh` is the end-to-end gate: it provisions a `Google-Search` service
(now sourced from the flat catalog), maps it into an application with two perspective locations,
and asserts perspective polling, outage creation on real service failure, RPC-timeout isolation,
and recovery — exercising the migrated daemon on the catalog path unchanged.

## Conclusion

**Zero unexplained deltas.** Seven service types differ from the legacy perspective XML, each
explained above: two never-pollable orphans (`JMX-Kafka`, `OpenNMS-JVM` — pure parity) and five
intentional removals (`JMX-Cassandra`, `JMX-Cassandra-Newts`, `ActiveMQ`, `NRPE`, `NRPE-NoSSL`)
whose monitor classes delta-v does not support and which could only ever false-DOWN. All remaining
differences are the documented FR5/FR6 field drops. With this switch, `poller-configuration.xml`
is retired from the poller daemons — the flat catalog is the only poller config dialect.
