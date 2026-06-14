# Pollerd Catalog Content-Parity Audit — 2026-06-14

Story 2.1 (Epic 2 / PR 2). Verifies that translating
`deploy/overlays/pollerd/etc/poller-configuration.xml` →
`deploy/overlays/pollerd/etc/poller-services.yaml` changed the **format only**, not the
monitored behavior (NFR7).

## Policy

**NFR7 (scope guard):** Format translation only — no service renames, no retirements, no
interval changes beyond the documented field drops. The flat catalog must poll the same
service set, at the same intervals, with the same parameter values (modulo dropped fields).

## Documented field drops (FR6 / FR5)

These are removed from every definition with **no runtime effect** (no-op subsystems / model
removed from user-facing config):

| Dropped | Reason |
|---------|--------|
| `<downtime>` blocks | FR5 — fixed-interval polling; per-package downtime is synthesized by the translator (`begin=0, interval=<service interval>`) |
| `<rrd>` blocks (`step`, `rra`) | FR6 — RRD never adopted; no-op PersisterFactory |
| `rrd-repository`, `rrd-base-name`, `ds-name` params | FR6 — RRD no-op |
| `thresholding-enabled` param | FR6 — no-op ThresholdingService |
| `user-defined` attribute | not part of the flat shape; inventory state, not config |
| `status="on"` attribute | default; flat `enabled` defaults true (no service was `status="off"`) |
| `<filter>`, `<include-range>` | FR2 — selection is inventory-driven, no filters/ranges |

`rrd-status` is **kept verbatim** as a parameter — it is not in the documented drop list, so
preserving it is the zero-delta choice.

## Explained deltas (service-set changes)

| Service | Action | Justification |
|---------|--------|---------------|
| `JMX-Kafka` | **Dropped (orphan)** | Defined as a `<service>` in the legacy XML but had **no `<monitor>` class-name binding** anywhere in the file. The legacy engine never polls a service with no monitor mapping (registry lookup finds nothing), so it was never actually pollable. The flat format requires `monitor`; dropping the orphan preserves runtime behavior exactly. Confirmed orphan: `JMX-Kafka` appears only at `poller-configuration.xml:222`. |
| `JMX-Cassandra` | **Removed (deprecated)** | JMX/Cassandra monitoring is deprecated and out of scope for delta-v. (Also: `Jsr160Monitor` is not in delta-v's supported `LocalServiceMonitorRegistry`, so it could only false-DOWN.) |
| `JMX-Cassandra-Newts` | **Removed (deprecated)** | Same as JMX-Cassandra — deprecated, doesn't matter for delta-v. |
| `ActiveMQ` | **Removed (out of scope)** | delta-v is built entirely on Kafka; ActiveMQ is explicitly not a focus. (Also: `ActiveMQMonitor` is unsupported by the registry.) |
| `NRPE` | **Removed (deprecated)** | NRPE is deprecated and not actively maintained. (Also: `NrpeMonitor` is unsupported by the registry.) |
| `NRPE-NoSSL` | **Removed (deprecated)** | Same as NRPE — deprecated/unmaintained. |

`JMX-Kafka` was never pollable, so dropping it is pure parity. The other five are a **deliberate
deviation from format-only parity (NFR7)**, removed on product grounds (deprecated / out of
delta-v's Kafka-first scope) and reinforced by the fact that their monitor classes aren't in the
supported registry (they could only ever false-DOWN). Other still-unsupported types (SMTP, FTP,
IMAP, POP3, PTP, Windows-Task-Scheduler, VMware*) are retained for now and surfaced by the FR9
`deltav_pollerd_services_unscheduled` gauge — see `deferred-work.md`.

## Service-set & interval parity

38 services in the legacy XML → 32 in the catalog (JMX-Kafka orphan + 5 intentional removals
for unsupported monitors — see above). All retained intervals unchanged. `pattern` carried over
for the one dynamic family (PTP).

| Service | Monitor | Interval (ms) | Pattern | Notes |
|---------|---------|---------------|---------|-------|
| ~~JMX-Cassandra~~ | Jsr160Monitor | 300000 | — | **removed — deprecated / out of scope** |
| ~~JMX-Cassandra-Newts~~ | Jsr160Monitor | 300000 | — | **removed — deprecated / out of scope** |
| ICMP | IcmpMonitor | 300000 | — | |
| DNS | DnsMonitor | 300000 | — | |
| Elasticsearch | HttpMonitor | 300000 | — | |
| SMTP | SmtpMonitor | 300000 | — | |
| FTP | FtpMonitor | 300000 | — | |
| SNMP | SnmpMonitor | 300000 | — | |
| HTTP | HttpMonitor | 300000 | — | |
| HTTP-8080 | HttpMonitor | 300000 | — | |
| HTTP-8000 | HttpMonitor | 300000 | — | |
| HTTPS | HttpsMonitor | 300000 | — | |
| MySQL | TcpMonitor | 300000 | — | |
| SQLServer | TcpMonitor | 300000 | — | |
| Oracle | TcpMonitor | 300000 | — | |
| Postgres | TcpMonitor | 300000 | — | |
| SSH | SshMonitor | 300000 | — | |
| IMAP | ImapMonitor | 300000 | — | |
| POP3 | Pop3Monitor | 300000 | — | |
| PTP | PtpMonitor | 300000 | `^PTP-.*$` | dynamic family preserved as `pattern:` |
| ~~NRPE~~ | NrpeMonitor | 300000 | — | **removed — deprecated / out of scope** |
| ~~NRPE-NoSSL~~ | NrpeMonitor | 300000 | — | **removed — deprecated / out of scope** |
| Windows-Task-Scheduler | Win32ServiceMonitor | 300000 | — | |
| ~~JMX-Kafka~~ | _(none)_ | 300000 | — | **dropped — orphan, no monitor binding** |
| VMwareCim-HostSystem | VmwareCimMonitor | 300000 | — | |
| VMware-ManagedEntity | VmwareMonitor | 300000 | — | |
| MS-RDP | TcpMonitor | 300000 | — | |
| ~~ActiveMQ~~ | ActiveMQMonitor | 300000 | — | **removed — deprecated / out of scope** |
| MinaSSH | MinaSshMonitor | 300000 | — | |
| Deltav-Health | PageSequenceMonitor | 30000 | — | `page-sequence` block scalar |
| Minion-Health | TcpMonitor | 30000 | — | |
| PostgreSQL | TcpMonitor | 30000 | — | |
| Kafka | TcpMonitor | 30000 | — | |
| Google-Search | PageSequenceMonitor | 30000 | — | `page-sequence` block scalar |
| GoogleCloud | PassiveServiceMonitor | 30000 | — | no parameters |
| Azure | PassiveServiceMonitor | 30000 | — | no parameters |
| AWS | PassiveServiceMonitor | 30000 | — | no parameters |
| StrafePing | StrafePingMonitor | 300000 | — | |

## Parameter parity

Every retained parameter key/value was carried over **verbatim** (no key translation, no value
normalization — anti-pattern guard). Spot-confirmed for the high-risk shapes:

- **Metadata DSL chains** (e.g. `${requisition:poller-timeout|requisition:timeout|detector:timeout|3000}`)
  round-trip byte-identical as double-quoted strings.
- **`page-sequence` nested XML** (Deltav-Health, Google-Search) survives as a block scalar; XML
  structure preserved (PSM parses it whitespace-insensitively).
- **Embedded-quote / regex values** (`response-text: ~.*status.:.green.*` on Elasticsearch)
  preserved via double-quoting.
- **Empty-default DSL values** (`userid`/`password` FTP defaults `|}`) preserved.

The only parameters removed from any definition are the documented drops above
(`rrd-repository`, `rrd-base-name`, `ds-name`, `thresholding-enabled`).

## Validation

`make lint-catalog CATALOG=deploy/overlays/pollerd/etc/poller-services.yaml` → **exit 0** (clean).

## Conclusion

**Zero unexplained deltas.** Six service types differ from the legacy XML, each explained above:
`JMX-Kafka` (never-pollable orphan — pure parity) and five intentional removals
(`JMX-Cassandra`, `JMX-Cassandra-Newts`, `ActiveMQ`, `NRPE`, `NRPE-NoSSL`) whose monitor classes
delta-v's pollerd does not support and which could therefore only false-DOWN. All remaining
differences are the documented FR5/FR6 field drops. The five removals are a deliberate,
documented deviation from strict NFR7 format-only parity (justified: unschedulable definitions).
