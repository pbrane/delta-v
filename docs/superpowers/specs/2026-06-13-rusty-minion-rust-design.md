# Rusty Minion — Rust Reimplementation of the Delta-V Minion

**Status:** Design (approved for planning)
**Date:** 2026-06-13
**Author:** David Hustace
**Branch target:** new feature branch off `develop`

## 1. Summary

Rusty Minion is a ground-up reimplementation of the OpenNMS Delta-V Minion in **Rust**,
targeting **new Delta-V deployments only**. It is a production-grade, API-compatible,
reduced-functionality Minion: it speaks the same logical IPC contract to Core/daemon
containers but is built on a protobuf-native wire format, a native async runtime, and
native protocol libraries instead of the JVM + Karaf/OSGi + Spring stack.

Because there are no legacy Delta-V deployments and no Minions in the field, this design
takes a **big-bang protobuf cutover**: the IPC contract is redefined as protobuf end to
end, with **no JAXB/XML compatibility layer on either side**. The existing Java Minion and
Rusty Minion both become conforming clients of one shared, versioned `.proto` contract.

## 2. Goals and non-goals

### Goals
- A native Rust Minion that registers with Core and is operationally interchangeable with
  the Java Minion for the supported capability subset.
- A **protobuf-native IPC contract** in a standalone shared repository, consumed (via
  codegen) by both the Rust Minion and the Java Core. No hand-written serialization on
  either side.
- Support **both transports**: Kafka IPC and the gRPC tunnel.
- Memory safety for all network-facing parsing of untrusted input, with the single
  `unsafe` FFI boundary (net-snmp) contained and fuzzed.
- A conformance harness that proves wire interoperability against a real Java Core.

### Supported capabilities (v1)
- **ServiceMonitors:** Icmp, Snmp, Http, Https, DnsResolution
- **ServiceCollectors:** Snmp
- **Sink listeners:** SNMP Trap Listener, Syslog Receiver, single-port Flows (telemetry) Listener
- **Supporting IPC:** Twin subscriber (SNMPv3 user/agent config distribution),
  Heartbeat / registration

### Non-goals (explicitly out of scope for v1, but architecturally accommodated)
- **Rusty Minion Lite** — a future reduced build for edge/WASM (Cloudflare Worker) deployments,
  limited to **PerspectiveMonitoring with HTTPS only**. The Worker sandbox forbids raw
  sockets and UDP binds, so only HTTPS-reachability polling survives there. The core is
  structured so Lite is later a reduced `cargo` feature set plus a `wasm32` target, **not a
  fork**. Nothing in v1 should preclude it; nothing in v1 builds it.
- Any monitor/collector/listener outside the lists above.
- Replacing or supporting JAXB/XML payloads. The contract is protobuf-only.
- Changes to Core-side persistence, alarms, provisioning, or RRD handling. Those remain
  Java and are unaffected beyond regenerating the new protobuf payload types.

## 3. Background

The current Minion is the right boundary to fork at: it is nearly stateless. It executes
RPC requests (poll/collect) dispatched from Core, and forwards inbound packets (traps,
syslog, flows) to Core via the Sink API. All stateful, DB-bound complexity (alarms,
provisioning, RRD persistence, collection-config interpretation) lives on the Core/daemon
side and stays in Java.

Two findings from the codebase investigation drive the design:

1. **IPC envelopes are already language-neutral protobuf**, but the **payloads inside them
   are JAXB-serialized XML** for RPC and most Sink modules (Twin and Telemetry payloads are
   already protobuf). The XML payloads are the single largest porting risk; the big-bang
   protobuf cutover eliminates them.

2. **`className` in an RPC request is a dispatch key, not a class to instantiate.** A poll
   request carries `className="org.opennms.netmgt.poller.monitors.IcmpMonitor"`. The Java
   Minion reflectively instantiates that class; Rusty Minion instead pattern-matches the string
   and routes to its own native implementation, returning a contract-compatible response.
   Core never observes the difference.

Relevant Java reference points:
- RPC envelope: `core/ipc/rpc/kafka/src/main/proto/kafka-rpc.proto`
- Sink envelope: `core/ipc/sink/common/src/main/proto/sink-message.proto`
- Twin envelope: `core/ipc/twin/common/src/main/proto/twin-message.proto`
- gRPC tunnel: `core/ipc/grpc/common/src/main/proto/ipc.proto` (`OpenNMSIpc` service,
  bidirectional `RpcStreaming`/`SinkStreaming`), `core/ipc/twin/grpc/common/src/main/proto/twin-grpc.proto`
- ServiceMonitor SPI: `features/poller/api/src/main/java/org/opennms/netmgt/poller/ServiceMonitor.java`
- ServiceCollector SPI: `features/collection/api/src/main/java/org/opennms/netmgt/collection/api/ServiceCollector.java`
- Telemetry listener framework: `features/telemetry/listeners/src/main/java/org/opennms/netmgt/telemetry/listeners/UdpListener.java`
- Telemetry wire format (already protobuf): `features/telemetry/common/src/main/resources/telemetry.proto`,
  `features/telemetry/protocols/netflow/transport/src/main/proto/netflow.proto`

## 4. Architecture

Rusty Minion is organized as six layers, each a clean seam. Capability code is unaware of both
transport and wire framing.

```
┌─────────────────────────────────────────────────────────┐
│ runtime/lifecycle   tokio, config load, identity,         │
│                     heartbeat producer, graceful shutdown │
├─────────────────────────────────────────────────────────┤
│ capability layer    Monitor   → icmp/snmp/http/https/dns  │
│                     Collector → snmp                      │
│                     Listener  → trap/syslog/flows         │
├─────────────────────────────────────────────────────────┤
│ protocol clients    snmp (net-snmp FFI, unsafe-isolated), │
│                     icmp (socket2), http (reqwest),       │
│                     dns (hickory)                         │
├─────────────────────────────────────────────────────────┤
│ dispatch layer      module_id + className → handler       │
├─────────────────────────────────────────────────────────┤
│ contract layer      shared .proto → prost/tonic codegen   │
│                     envelopes + all payload messages      │
│                     SINGLE SOURCE OF TRUTH (Java regens)  │
├─────────────────────────────────────────────────────────┤
│ transport layer     Transport trait → KafkaTransport      │
│                     (rdkafka) | GrpcTransport (tonic)     │
└─────────────────────────────────────────────────────────┘
```

### 4.1 Transport layer

A `Transport` abstraction hides Kafka vs gRPC from everything above it. It exposes three
roles:

- **RpcServer** — subscribe to inbound RPC requests, return responses. (Kafka:
  request/response topics with chunking/reassembly; gRPC: the bidirectional `RpcStreaming`
  stream.)
- **SinkProducer** — send fire-and-forget Sink messages (traps/syslog/flows/heartbeat).
- **TwinSubscriber** — subscribe to config updates keyed by `consumer_key`.

Implementations:
- `KafkaTransport` — `rdkafka` (librdkafka bindings). Reproduces topic naming
  (`{instanceId}.{location}.rpc-request`, `{instanceId}.rpc-response`, `Sink.*` topics,
  `{instanceId}.twin.request|response[.{location}]`), the protobuf envelope chunking for
  messages over the configured threshold, and consumer-group semantics (RPC group =
  location, Twin group = minion id).
- `GrpcTransport` — `tonic` client of `OpenNMSIpc`/`OpenNMSTwinIpc`. Handles stream
  establishment, TLS, reconnection/backoff, and multiplexing RPC + Sink over the
  bidirectional streams.

Transport selection is configuration-driven; capability code never branches on it.

### 4.2 Contract layer (standalone shared repo)

A standalone, versioned repository (working name `opennms-ipc-contract`) owns every
`.proto` and is consumed by both implementations via codegen — Rust through
`prost`/`tonic-build`, Java through the protobuf Maven plugin. **`buf`** drives lint and
**breaking-change detection in CI**; the contract is treated as a release artifact with
semantic versioning.

Contents:
- **Envelopes** (largely reused/relocated from existing protos): RPC, Sink, Twin
  request/response.
- **New payload messages replacing JAXB DTOs:** `PollerRequest`, `PollerResponse`
  (carrying `PollStatus`: status enum, reason, response-time, numeric properties map),
  `CollectorRequest`, `CollectorResponse`, `TrapLog`/`TrapDto`, `SyslogMessageLog`/
  `SyslogMessage`, `MinionIdentity`, and `SnmpAgentConfig` (Twin payload).

**Highest-risk modeling task:** `CollectionSet` is a visitor-pattern tree
(resources → attributes → values) in the Java API. It must be redesigned as a flat,
explicit protobuf message. This is prototyped first and validated by a round-trip against
the Java collector before anything is built on top of it.

### 4.3 Dispatch layer

Maps `(module_id, className)` from an RPC request to a registered native handler. Two
modules in v1: `Poller` (className → one of the five monitors) and `Collect` (className →
SNMP collector). Unknown classNames return a structured "unsupported" response rather than
failing the stream, so a Core that schedules an out-of-subset monitor degrades gracefully.

### 4.4 Capability layer

Three traits, each independently testable:

- `Monitor::poll(target, params) -> PollStatus` — impls: `IcmpMonitor`, `SnmpMonitor`,
  `HttpMonitor`, `HttpsMonitor` (HTTPS = HTTP over TLS, sharing the HTTP impl),
  `DnsResolutionMonitor`.
- `Collector::collect(agent, params) -> CollectionSet` — impl: `SnmpCollector`
  (Minion-side only: walk configured OIDs, package values; RRD persistence and
  datacollection-config interpretation stay on Core).
- `Listener::run()` — binds a socket, parses inbound packets, dispatches to `SinkProducer`.
  Impls: `TrapListener`, `SyslogReceiver`, `FlowsListener` (single-port UDP).

### 4.5 Protocol clients

Shared protocol stacks, split from capabilities so the SNMP monitor, SNMP collector, and
trap listener share one SNMP implementation:

- `snmp` — safe wrapper over `snmp-sys` (net-snmp FFI). See §5.
- `icmp` — raw ICMP/ICMPv6 sockets via `socket2`. (Native raw sockets replace the Java
  side's ~3,000-LOC JNA/`jicmp` bridge entirely.)
- `http` — `reqwest`/`hyper` with `rustls` for HTTPS.
- `dns` — `hickory-dns` (pure Rust).
- UDP listeners (syslog, flows) — `tokio` UDP. Flows already use protobuf end to end.

### 4.6 Runtime / lifecycle

`tokio` multi-threaded runtime; configuration loading; minion identity (id + location);
heartbeat producer (periodic `MinionIdentity` over the Sink `Heartbeat` topic, doubling as
registration + keepalive); Twin subscriber bootstrap; graceful shutdown that drains
in-flight RPCs and closes listeners cleanly.

## 5. The net-snmp FFI safety boundary

SNMP is the heaviest protocol in scope and the one place native Rust libraries are not yet
production-grade for SNMPv3 auth/priv plus walk and trap decode. The deliberate trade-off
is to use **net-snmp via FFI**:

- `snmp-sys` — raw FFI bindings; **all `unsafe` lives here and nowhere else**.
- `snmp` — safe wrapper exposing get/getnext/getbulk/walk, v3 auth/priv, and trap decode.
  Nothing above this crate ever sees a raw pointer.
- net-snmp's session API is blocking/thread-affine, so it runs on a dedicated
  `spawn_blocking` pool — it is **not** placed on the tokio async reactor. (Mishandling
  this is a classic FFI-into-async deadlock.)
- Every hostile-input parser gets a `cargo-fuzz` target: trap PDU decode, netflow/IPFIX
  records, syslog framing. The net-snmp seam is fuzzed hardest and audited most, because it
  is the one place the memory-safety guarantee is suspended — which is exactly why it is
  contained to a single crate.

## 6. Configuration

Karaf/OSGi/Spring previously provided feature wiring and config loading for free; Rusty Minion
rebuilds this. Two config surfaces:

- **Bootstrap config** — transport selection and connection (Kafka brokers or gRPC
  endpoint + TLS), instance id, minion id, location. Native format (TOML), not XML.
- **Listener config** — the trapd/syslogd/telemetryd equivalents (ports, parsers, SNMPv3
  users). Modeled natively; SNMPv3 user material is delivered at runtime via the Twin
  subscriber rather than static files where possible.

## 7. Testing and conformance

Two tiers:

- **Tier 1 — golden-message corpus:** captured protobuf bytes for each payload message;
  both the Rust and Java codegen must round-trip them. Runs in unit CI; catches contract
  drift in seconds. Paired with `buf` breaking-change detection on the contract repo.
- **Tier 2 — end-to-end against a real Java Core:** Rusty Minion must pass the **existing**
  acceptance scripts the Java Minion passes — `test-minion-e2e.sh` (trap → Kafka → Trapd →
  EventTranslator → Alarmd → Postgres), `test-syslog-e2e.sh`, and the relevant phases of
  `test-e2e.sh`. These already encode pass/fail phases for the real wire path, so "Rusty Minion
  is done for capability X" means "it passes the same script."

Per-capability unit tests use protocol fixtures (canned SNMP agents, HTTP test servers, DNS
fixtures) and assert `PollStatus`/`CollectionSet` shape.

The conformance harness is also the entry ticket for any future implementation (Rusty Minion
Lite, or a C++ experiment): pass Tier 1 plus the applicable subset of the e2e scripts.

## 8. Phasing

Phases deliver vertical, protobuf-native slices. Registration/transport is the true
prerequisite — a Minion that cannot register can do nothing — so it precedes the first
capability.

- **Phase 0 — Foundation & registration.** Contract repo stood up; Java-side protobuf
  cutover for the payloads in scope; transport skeleton (Kafka + gRPC); identity +
  heartbeat; Twin subscriber. **Milestone:** Rusty Minion registers, is visible to Core, and
  receives Twin config. No capabilities yet.
- **Phase 1 — Listeners (Sink, one-way).** Simplest data path (fire-and-forget). Order:
  **Flows first** (already protobuf, lowest contract risk), then Trap, then Syslog.
  **Milestone:** `test-syslog-e2e.sh` and the trap path of `test-minion-e2e.sh` pass.
- **Phase 2 — Poller (RPC request/response).** The five monitors; exercises the
  bidirectional RPC path, dispatch, and Twin-delivered SNMPv3 credentials for the SNMP
  monitor. **Milestone:** poll requests for all five services return correct `PollStatus`
  via Core.
- **Phase 3 — Collector (RPC).** SNMP collector, reusing the Phase 2 SNMP client. Includes
  the `CollectionSet` protobuf modeling validated in Phase 0. **Milestone:** Core persists
  collected SNMP metrics gathered by Rusty Minion.

## 9. Risks and mitigations

| Risk | Mitigation |
| --- | --- |
| `CollectionSet` protobuf modeling is the hardest contract item | Prototype and round-trip against the Java collector in Phase 0, before dependents exist |
| net-snmp FFI is the only `unsafe` and parses hostile input | Contain to `snmp-sys`; fuzz the trap decoder; run net-snmp on a `spawn_blocking` pool |
| Two transports double the integration surface | `Transport` trait + a shared transport conformance test run against both impls |
| Rust SNMP ecosystem immaturity | Accepted; net-snmp FFI is the chosen strategy (signed off) |
| Lost Karaf/OSGi config machinery | Native TOML bootstrap + Twin-delivered runtime config; smaller surface than the XML stack it replaces |
| Contract drift between Java and Rust | Single shared repo + `buf` breaking-change CI + Tier 1 golden-message corpus |

## 10. Decisions (resolved)

- **Target:** new Delta-V deployments only; no legacy compatibility. (C)
- **Language:** Rust for production. C++26 optionally as a non-production experiment behind
  the same conformance suite.
- **Wire format:** big-bang protobuf cutover; no JAXB/XML.
- **Contract:** standalone shared `.proto` repository; both Java and Rust generate from it.
- **Transports:** Kafka IPC and gRPC tunnel, behind one `Transport` trait.
- **SNMP:** net-snmp via FFI, `unsafe` isolated to `snmp-sys`.
- **Future tier:** Rusty Minion Lite (WASM/Cloudflare Worker, PerspectiveMonitoring, HTTPS-only)
  accommodated as a reduced feature set, not built in v1.

## 11. Planning reconciliation (2026-06-13)

During Phase 0 planning, codebase investigation found that delta-v **already shipped a
gRPC-native Minion contract and a `minion-gateway` bridge** (the rc2/boot4 migration) that the
§3/§4.1 reference points (horizon `kafka-rpc.proto`, `OpenNMSIpc`, `twin-message.proto`) predate.
That existing contract (`core/minion-grpc-contracts`, package `org.deltav.minion.grpc.v1`:
`HeartbeatService`, `RpcChannelService`, `TwinChannelService`, `TrapService`, `SyslogService`,
`TelemetryService`) is **envelope-only** — it protobuf-frames the transport but still carries
horizon's opaque JAXB-XML/JSON payload bytes inside, exactly the porting risk §3 finding #1 names.
Three decisions resolved (confirmed with the author):

1. **gRPC target:** Rusty Minion implements the existing **delta-v gateway contract**
   (`org.deltav.minion.grpc.v1`), not horizon's `OpenNMSIpc`. It reuses the envelope services and
   replaces their opaque `bytes payload` with typed protobuf payloads.
2. **Transport scope:** **gRPC-to-gateway only** for Phase 0/v1. New remote Minions reach Core
   through the gateway; the horizon direct-from-Minion Kafka path is not built. The `Transport`
   trait seam is preserved so Kafka could be added later. (Supersedes "support both transports" in
   §2/§4.1 for v1.)
3. **Contract repo:** `opennms-ipc-contract` is seeded by **relocating** the existing
   `core/minion-grpc-contracts` protos (plus the frozen `deltav-timeseries.proto` as the
   `CollectionSet` flat model) and adding typed payloads; delta-v Java modules switch to depending
   on the published contract artifact.

Bonus de-risk: the §4.2 "highest-risk modeling task" (`CollectionSet` → flat protobuf) is already
solved in-tree by `deltav-timeseries.proto` (frozen at Phase 2 GA); Phase 0 validates against it
rather than inventing a schema.

Phase 0 plan: `docs/superpowers/plans/2026-06-13-rusty-minion-phase0-foundation.md`.
