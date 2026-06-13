# Rusty Minion Phase 0 — Foundation & Registration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up the standalone `opennms-ipc-contract` repo and a new Rust `rusty-minion` repo such that a Rust Minion connects to the existing delta-v `minion-gateway` over gRPC, registers (visible in Core's Minion list via heartbeat), and receives Twin config — with zero capabilities.

**Architecture:** Rusty Minion implements the **existing** delta-v gateway gRPC contract (`org.deltav.minion.grpc.v1`: `HeartbeatService`, `TwinChannelService`, plus skeletons for `RpcChannelService`/`TrapService`/`SyslogService`/`TelemetryService` consumed in later phases). The contract `.proto` files are **relocated** out of `core/minion-grpc-contracts` into a standalone, `buf`-managed repo that is the single source of truth; both Java (protobuf-maven-plugin) and Rust (`tonic-build`/`prost`) generate from it. Transport is **gRPC-only** for Phase 0 — new remote Minions reach Core through the gateway, so the horizon direct-Kafka path is not built (the `Transport` trait seam is kept so Kafka could be added later). Identity rides in gRPC metadata headers `x-minion-id`/`x-minion-location`; the gateway rejects streams lacking them with `UNAUTHENTICATED`.

**Tech Stack:** Rust (tokio 1, tonic 0.12, prost 0.13, tonic-build, serde/toml, json-patch, tracing); `buf` (lint + breaking-change CI); Java protobuf-maven-plugin 0.6.1 / protoc 3.25.5 / protobuf-java 4.33.4 / grpc-java 1.75.0 (unchanged from delta-v); GitHub Actions.

---

## Source-of-Truth Facts (verified against the delta-v tree)

These are the load-bearing facts every task below depends on. Cited so the engineer can re-verify.

| Fact | Value | Source |
| --- | --- | --- |
| Existing native contract module | `core/minion-grpc-contracts` (artifactId `org.opennms.core.minion-grpc-contracts`), protos in `src/main/proto/` | `core/minion-grpc-contracts/pom.xml` |
| Protos to relocate | `heartbeat.proto`, `rpc.proto`, `twin.proto`, `trap.proto`, `syslog.proto`, `telemetry.proto` | `core/minion-grpc-contracts/src/main/proto/` |
| Proto package / java_package | `org.deltav.minion.grpc.v1` (both) | every proto, lines 3–6 |
| Flat CollectionSet model (already frozen) | `core/deltav-kafka-contracts/src/main/proto/deltav-timeseries.proto` — `TimeseriesBatch → Resource → AttributeGroup → Attribute` | proto frozen at Phase 2 GA (delta-v#174) |
| Gateway gRPC listen port | `9090` plaintext h2c; TLS terminated at Envoy frontend `8443` | `core/minion-gateway/src/main/resources/application.yml:6` |
| Identity metadata header keys | `x-minion-id`, `x-minion-location`; missing → `Status.UNAUTHENTICATED` | `core/minion-gateway/.../grpc/MinionIdentityServerInterceptor.java:42,44,59` |
| Heartbeat stream | `HeartbeatService.Publish(stream Heartbeat) returns (stream HeartbeatAck)`, 30 s interval | `heartbeat.proto:32`, `HeartbeatConfiguration.java:49` |
| Heartbeat → Kafka bridge | gateway publishes to `DeltaV.Sink.Heartbeat`, key `location + "@" + minionId`, XML `MinionIdentityDTO` body | `HeartbeatTranslator.java:36,45` |
| Core registration consumer | `HeartbeatConsumer.handleMessage` → `minionDao.saveOrUpdate(minion)` | `features/minion/heartbeat/consumer/.../HeartbeatConsumer.java:168` |
| Twin stream | `TwinChannelService.Channel(stream TwinSubscription) returns (stream TwinUpdate)`; first reply after SUBSCRIBE is a full snapshot (`is_patch=false`) | `twin.proto:23-27`, `TwinChannelGrpcService.java:72-75` |
| Twin update semantics | `twin_object` = JSON bytes (full state or RFC6902 patch); `version` monotonic per `(consumer_key, location)`; reconnect on version gap | `twin.proto:45-62`, `MinionTwinStreamClient.java:74-86` |
| Minion config keys / defaults | `opennms.minion.gateway.host`=`envoy`, `.gateway.port`=`8443`, `opennms.minion.id`, `opennms.minion.location`=`Default`, `opennms.minion.transport`=`grpc` | `MinionProperties.java:24-25,66-67` |
| Client channel settings | `usePlaintext()` to gateway:9090, keepalive 30 s, `keepAliveWithoutCalls(true)` | `GrpcHeartbeatDispatcherConfiguration.java:45-52` |
| Java toolchain (unchanged) | protobuf-maven-plugin 0.6.1, protoc 3.25.5, protobuf-java 4.33.4, grpc-java 1.75.0, os-maven-plugin 1.7.1 | `core/minion-grpc-contracts/pom.xml`, root `pom.xml` |
| Existing Rust in delta-v | none | `find . -name Cargo.toml` (empty) |
| Existing buf usage | none | repo-wide grep for `buf.yaml` (empty) |

**Phase 0 contract scope decision (explicit):** relocate the 6 envelope protos + relocate the `deltav-timeseries.proto` CollectionSet model for the round-trip prototype + add one new typed payload (`SnmpAgentConfig`, used later by the SNMP monitor's Twin path). The `PollerRequest/Response` and `CollectorRequest/Response` typed payloads are **defined in their own phases (1–3)**, not Phase 0 — they have no consumer yet and adding them now would be dead schema. Phase 0 wires only Heartbeat + Twin.

**Repo layout decision (from planning):** two standalone repos. `opennms-ipc-contract` is polyglot (proto + `buf` + a Java Maven module + a Rust crate). `rusty-minion` is a Rust cargo workspace that depends on the contract repo's Rust crate via a pinned git dependency. A small follow-up in the delta-v tree repoints `core/minion-grpc-contracts` at the published contract.

---

## File Structure

### Repo A — `opennms-ipc-contract` (new standalone git repo)

```
opennms-ipc-contract/
├── LICENSE                              # AGPL-3.0 (matches delta-v)
├── README.md                            # what this is, codegen instructions, versioning policy
├── buf.yaml                             # buf module root + lint + breaking config (v2)
├── buf.gen.yaml                         # codegen config (Java + docs); Rust uses tonic-build directly
├── proto/
│   └── org/deltav/minion/grpc/v1/
│       ├── heartbeat.proto              # relocated verbatim
│       ├── rpc.proto                    # relocated verbatim
│       ├── twin.proto                   # relocated verbatim
│       ├── trap.proto                   # relocated verbatim
│       ├── syslog.proto                 # relocated verbatim
│       ├── telemetry.proto              # relocated verbatim
│       ├── snmp_agent_config.proto      # NEW typed Twin payload (Phase 0 adds, Phase 2 uses)
│       └── collectionset.proto          # relocated from deltav-timeseries.proto (CollectionSet flat model)
├── corpus/                              # Tier-1 golden-message byte fixtures (.bin)
│   ├── heartbeat.bin
│   ├── twin_update.bin
│   └── collectionset.bin
├── java/                                # Java Maven module → publishes the contract artifact
│   ├── pom.xml
│   └── src/test/java/org/deltav/ipc/contract/GoldenCorpusRoundTripTest.java
├── rust/                                # Rust crate → consumed by rusty-minion
│   ├── Cargo.toml
│   ├── build.rs                         # tonic-build over ../proto
│   ├── src/lib.rs                       # re-exports generated modules
│   └── tests/golden_corpus.rs           # Tier-1 round-trip in Rust
└── .github/workflows/
    ├── buf.yml                          # buf lint + breaking-change vs main
    ├── java.yml                         # mvn verify (runs GoldenCorpusRoundTripTest), publish on tag
    └── rust.yml                         # cargo test (runs golden_corpus.rs)
```

### Repo B — `rusty-minion` (new standalone Rust cargo workspace)

```
rusty-minion/
├── LICENSE                              # AGPL-3.0
├── README.md
├── Cargo.toml                           # workspace manifest
├── rust-toolchain.toml                  # pin stable channel
├── deny.toml                            # cargo-deny license/advisory policy (optional gate)
├── config/
│   └── minion.example.toml              # documented bootstrap config
├── crates/
│   ├── config/                          # bootstrap TOML loading + identity
│   │   ├── Cargo.toml
│   │   └── src/lib.rs
│   ├── transport/                       # Transport trait + GrpcTransport (tonic)
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs                    # Transport trait + roles
│   │       ├── identity.rs               # x-minion-id / x-minion-location interceptor
│   │       └── grpc.rs                   # GrpcTransport: channel build, heartbeat, twin streams
│   ├── twin/                            # Twin subscriber state machine (versioning, patch, reconnect)
│   │   ├── Cargo.toml
│   │   └── src/lib.rs
│   └── minion/                          # binary: runtime, lifecycle, heartbeat producer, wiring
│       ├── Cargo.toml
│       └── src/
│           ├── main.rs
│           ├── runtime.rs
│           └── heartbeat.rs
└── .github/workflows/
    └── ci.yml                           # cargo fmt --check, clippy -D warnings, test
```

### Repo C — delta-v tree (follow-up repoint, one task)

```
core/minion-grpc-contracts/pom.xml       # depend on published org.deltav:ipc-contract-java; drop local protos
core/minion-grpc-contracts/src/main/proto/  # deleted (now sourced from the contract repo)
```

---

## Sequencing

Contract first (everything generates from it), then the Rust Minion (depends on the contract's Rust crate), then the delta-v Java repoint (depends on a published contract artifact). Within the Rust Minion: config → identity interceptor → gRPC channel → heartbeat → twin → runtime → integration.

- **A1–A8:** `opennms-ipc-contract` repo (relocate, codegen both languages, golden corpus, CollectionSet round-trip, CI).
- **B1–B8:** `rusty-minion` repo (workspace, config, identity, transport, heartbeat, twin, runtime, integration test).
- **C1:** delta-v repoint of `core/minion-grpc-contracts`.

---

# Repo A — `opennms-ipc-contract`

> All Repo A tasks run inside the new `opennms-ipc-contract` git repo. Create it first (Task A1). Paths in A-tasks are relative to that repo root.

### Task A1: Repo skeleton, license, README, git init

**Files:**
- Create: `opennms-ipc-contract/LICENSE`
- Create: `opennms-ipc-contract/README.md`
- Create: `opennms-ipc-contract/.gitignore`

- [ ] **Step 1: Create the repo and license**

```bash
mkdir -p opennms-ipc-contract && cd opennms-ipc-contract
git init -b main
curl -fsSL https://www.gnu.org/licenses/agpl-3.0.txt -o LICENSE
```

- [ ] **Step 2: Write `.gitignore`**

```gitignore
# Rust
/rust/target/
# Java
/java/target/
# buf
/gen/
```

- [ ] **Step 3: Write `README.md`**

```markdown
# opennms-ipc-contract

Single source of truth for the Delta-V Minion ↔ minion-gateway IPC contract.
Package: `org.deltav.minion.grpc.v1`.

Both implementations generate from `proto/`:
- **Java** — `java/` module via protobuf-maven-plugin → Maven artifact `org.deltav:ipc-contract-java`.
- **Rust** — `rust/` crate via `tonic-build` → consumed by `rusty-minion` as a git dependency.

Lint and breaking-change detection use [`buf`](https://buf.build). The contract is a
release artifact with semantic versioning; a tag `vX.Y.Z` publishes both the Java artifact
and pins the Rust crate.

## Layout
- `proto/` — the `.proto` source (buf module root)
- `corpus/` — Tier-1 golden-message byte fixtures, round-tripped by both languages in CI
- `java/`, `rust/` — per-language codegen + golden-corpus tests

## Versioning
`buf breaking` runs against `main` on every PR. Wire-incompatible changes require a major bump.
```

- [ ] **Step 4: Commit**

```bash
git add LICENSE README.md .gitignore
git commit -m "chore: initialize opennms-ipc-contract repo"
```

---

### Task A2: Relocate the six envelope protos + add `buf.yaml`

**Files:**
- Create: `proto/org/deltav/minion/grpc/v1/heartbeat.proto` (and rpc/twin/trap/syslog/telemetry)
- Create: `buf.yaml`

- [ ] **Step 1: Copy the six protos verbatim from the delta-v tree**

```bash
# Run from opennms-ipc-contract/, with $DELTAV pointing at the delta-v checkout.
mkdir -p proto/org/deltav/minion/grpc/v1
for f in heartbeat rpc twin trap syslog telemetry; do
  cp "$DELTAV/core/minion-grpc-contracts/src/main/proto/$f.proto" \
     "proto/org/deltav/minion/grpc/v1/$f.proto"
done
```

The files are copied unchanged. For reference, `heartbeat.proto` content is:

```proto
// proto/org/deltav/minion/grpc/v1/heartbeat.proto
syntax = "proto3";

package org.deltav.minion.grpc.v1;

option java_package = "org.deltav.minion.grpc.v1";
option java_multiple_files = true;
option java_outer_classname = "HeartbeatProto";

import "google/protobuf/timestamp.proto";

message Heartbeat {
  string minion_id = 1;
  string location = 2;
  google.protobuf.Timestamp sent_at = 3;
  string version = 4;
}

message HeartbeatAck {
  string minion_id = 1;
  google.protobuf.Timestamp received_at = 2;
}

service HeartbeatService {
  rpc Publish(stream Heartbeat) returns (stream HeartbeatAck);
}
```

- [ ] **Step 2: Write `buf.yaml` (v2)**

```yaml
version: v2
modules:
  - path: proto
lint:
  use:
    - STANDARD
  except:
    # The contract preserves horizon-era names; do not fail on package-version dir layout.
    - PACKAGE_DIRECTORY_MATCH
breaking:
  use:
    - WIRE_JSON
```

- [ ] **Step 3: Verify buf can build the module**

Run: `buf build`
Expected: exits 0, no output (module compiles).

- [ ] **Step 4: Run the linter**

Run: `buf lint`
Expected: exits 0. If any non-`PACKAGE_DIRECTORY_MATCH` rule fires (e.g. `RPC_REQUEST_RESPONSE_UNIQUE`), add the specific rule to `lint.except` with a one-line comment — do not relax `STANDARD` wholesale.

- [ ] **Step 5: Commit**

```bash
git add proto buf.yaml
git commit -m "feat: relocate org.deltav.minion.grpc.v1 envelope protos under buf"
```

---

### Task A3: Add `snmp_agent_config.proto` and relocate the CollectionSet model

**Files:**
- Create: `proto/org/deltav/minion/grpc/v1/snmp_agent_config.proto`
- Create: `proto/org/deltav/minion/grpc/v1/collectionset.proto`

- [ ] **Step 1: Write `snmp_agent_config.proto`** (the typed Twin payload the SNMP monitor consumes in Phase 2; defined now to fix the contract early)

```proto
// proto/org/deltav/minion/grpc/v1/snmp_agent_config.proto
syntax = "proto3";

package org.deltav.minion.grpc.v1;

option java_package = "org.deltav.minion.grpc.v1";
option java_multiple_files = true;
option java_outer_classname = "SnmpAgentConfigProto";

// Typed SNMPv1/v2c/v3 agent configuration distributed to the Minion via the
// Twin channel (consumer_key "snmpv3-user" / per-agent keys). Replaces the
// JSON blob that horizon's Twin currently carries for SNMP credentials.
message SnmpAgentConfig {
  string address = 1;            // agent IP/host
  int32 port = 2;                // default 161
  SnmpVersion version = 3;
  int32 timeout_ms = 4;
  int32 retries = 5;

  // v1/v2c
  string read_community = 6;

  // v3
  SecurityLevel security_level = 7;
  string security_name = 8;      // username
  AuthProtocol auth_protocol = 9;
  string auth_passphrase = 10;
  PrivProtocol priv_protocol = 11;
  string priv_passphrase = 12;
  string context_name = 13;      // optional
  string engine_id = 14;         // optional, hex

  enum SnmpVersion {
    SNMP_VERSION_UNSPECIFIED = 0;
    V1 = 1;
    V2C = 2;
    V3 = 3;
  }
  enum SecurityLevel {
    SECURITY_LEVEL_UNSPECIFIED = 0;
    NO_AUTH_NO_PRIV = 1;
    AUTH_NO_PRIV = 2;
    AUTH_PRIV = 3;
  }
  enum AuthProtocol {
    AUTH_PROTOCOL_UNSPECIFIED = 0;
    MD5 = 1;
    SHA = 2;
    SHA224 = 3;
    SHA256 = 4;
    SHA384 = 5;
    SHA512 = 6;
  }
  enum PrivProtocol {
    PRIV_PROTOCOL_UNSPECIFIED = 0;
    DES = 1;
    TRIPLE_DES = 2;
    AES128 = 3;
    AES192 = 4;
    AES256 = 5;
  }
}
```

- [ ] **Step 2: Relocate the frozen CollectionSet flat model**

Copy `deltav-timeseries.proto` and re-home its package into the contract namespace. The message shape is reused unchanged (it is frozen at Phase 2 GA); only `package`/`java_package` are aligned to the contract.

```proto
// proto/org/deltav/minion/grpc/v1/collectionset.proto
syntax = "proto3";

package org.deltav.minion.grpc.v1;

option java_package = "org.deltav.minion.grpc.v1";
option java_multiple_files = true;
option java_outer_classname = "CollectionSetProto";

// Flat, explicit protobuf model of the Java CollectionSet visitor tree
// (resources -> attribute groups -> attributes). Reuses the structure frozen
// in delta-v deltav-timeseries.proto. Validated by a round-trip against the
// Java collector in Task A8.
enum AttributeType {
  ATTRIBUTE_TYPE_UNSPECIFIED = 0;
  ATTRIBUTE_TYPE_COUNTER = 1;
  ATTRIBUTE_TYPE_GAUGE = 2;
  ATTRIBUTE_TYPE_STRING = 3;
}

message CollectionSet {
  int64 timestamp_ms = 1;
  repeated Resource resources = 2;
}

message Resource {
  string resource_id = 1;        // e.g. "node[N].if[eth0]"
  string type = 2;               // "node", "if", custom
  string instance = 3;           // "" for node, "eth0"/OID-suffix for indexed
  repeated AttributeGroup groups = 4;
}

message AttributeGroup {
  string name = 1;               // e.g. "mib2-tcp"
  repeated Attribute attributes = 2;
}

message Attribute {
  string name = 1;               // e.g. "tcpActiveOpens"
  oneof value {
    double numeric = 2;          // for COUNTER/GAUGE
    string text = 3;             // for STRING
  }
  AttributeType type = 4;
}
```

- [ ] **Step 3: Verify and lint**

Run: `buf build && buf lint`
Expected: exits 0 (apply the same targeted-`except` rule as Task A2 if a STANDARD rule fires on the new files; document each).

- [ ] **Step 4: Commit**

```bash
git add proto/org/deltav/minion/grpc/v1/snmp_agent_config.proto \
        proto/org/deltav/minion/grpc/v1/collectionset.proto
git commit -m "feat: add typed SnmpAgentConfig and flat CollectionSet payloads"
```

---

### Task A4: `buf` lint + breaking-change CI

**Files:**
- Create: `.github/workflows/buf.yml`

- [ ] **Step 1: Write the workflow**

```yaml
# .github/workflows/buf.yml
name: buf
on:
  pull_request:
  push:
    branches: [main]
jobs:
  buf:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: bufbuild/buf-setup-action@v1
        with:
          version: 1.45.0
      - run: buf build
      - run: buf lint
      - name: Breaking-change check vs main
        if: github.event_name == 'pull_request'
        run: buf breaking --against "https://github.com/${{ github.repository }}.git#branch=main"
```

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/buf.yml
git commit -m "ci: buf lint + breaking-change detection"
```

Note: this is verified live when the repo is pushed and a PR is opened (Step has no local run). The local equivalent the engineer must confirm passes before pushing: `buf lint && buf build`.

---

### Task A5: Java codegen module that publishes the contract artifact

**Files:**
- Create: `java/pom.xml`

- [ ] **Step 1: Write `java/pom.xml`** (mirrors the delta-v `minion-grpc-contracts` plugin stack so generated classes are byte-for-byte compatible; reads protos from `../proto`)

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>

  <groupId>org.deltav</groupId>
  <artifactId>ipc-contract-java</artifactId>
  <version>0.1.0</version>
  <packaging>jar</packaging>
  <name>OpenNMS IPC Contract :: Java</name>

  <properties>
    <maven.compiler.release>21</maven.compiler.release>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    <protoc.version>3.25.5</protoc.version>
    <protobuf.version>4.33.4</protobuf.version>
    <grpc.version>1.75.0</grpc.version>
  </properties>

  <dependencies>
    <dependency>
      <groupId>com.google.protobuf</groupId>
      <artifactId>protobuf-java</artifactId>
      <version>${protobuf.version}</version>
    </dependency>
    <dependency>
      <groupId>io.grpc</groupId>
      <artifactId>grpc-protobuf</artifactId>
      <version>${grpc.version}</version>
    </dependency>
    <dependency>
      <groupId>io.grpc</groupId>
      <artifactId>grpc-stub</artifactId>
      <version>${grpc.version}</version>
    </dependency>
    <dependency>
      <groupId>javax.annotation</groupId>
      <artifactId>javax.annotation-api</artifactId>
      <version>1.3.2</version>
    </dependency>
    <dependency>
      <groupId>junit</groupId>
      <artifactId>junit</artifactId>
      <version>4.13.2</version>
      <scope>test</scope>
    </dependency>
  </dependencies>

  <build>
    <extensions>
      <extension>
        <groupId>kr.motd.maven</groupId>
        <artifactId>os-maven-plugin</artifactId>
        <version>1.7.1</version>
      </extension>
    </extensions>
    <plugins>
      <plugin>
        <groupId>org.xolstice.maven.plugins</groupId>
        <artifactId>protobuf-maven-plugin</artifactId>
        <version>0.6.1</version>
        <configuration>
          <protoSourceRoot>${project.basedir}/../proto</protoSourceRoot>
          <protocArtifact>com.google.protobuf:protoc:${protoc.version}:exe:${os.detected.classifier}</protocArtifact>
          <pluginId>grpc-java</pluginId>
          <pluginArtifact>io.grpc:protoc-gen-grpc-java:${grpc.version}:exe:${os.detected.classifier}</pluginArtifact>
        </configuration>
        <executions>
          <execution>
            <goals>
              <goal>compile</goal>
              <goal>compile-custom</goal>
            </goals>
          </execution>
        </executions>
      </plugin>
    </plugins>
  </build>
</project>
```

- [ ] **Step 2: Verify Java generation compiles**

Run: `cd java && mvn -q -DskipTests compile`
Expected: BUILD SUCCESS; `target/generated-sources/protobuf/java/org/deltav/minion/grpc/v1/Heartbeat.java` exists.

- [ ] **Step 3: Add publish-on-tag CI**

Create `.github/workflows/java.yml`:

```yaml
# .github/workflows/java.yml
name: java
on:
  pull_request:
  push:
    branches: [main]
    tags: ['v*']
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-java@v4
        with: { distribution: temurin, java-version: '21' }
      - run: cd java && mvn -B verify
      - name: Publish on tag
        if: startsWith(github.ref, 'refs/tags/v')
        run: cd java && mvn -B -DskipTests deploy
        env:
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
```

- [ ] **Step 4: Commit**

```bash
git add java/pom.xml .github/workflows/java.yml
git commit -m "build: Java codegen module for the IPC contract"
```

---

### Task A6: Rust codegen crate

**Files:**
- Create: `rust/Cargo.toml`
- Create: `rust/build.rs`
- Create: `rust/src/lib.rs`

- [ ] **Step 1: Write `rust/Cargo.toml`**

```toml
[package]
name = "opennms-ipc-contract"
version = "0.1.0"
edition = "2021"
license = "AGPL-3.0"

[dependencies]
prost = "0.13"
prost-types = "0.13"
tonic = "0.12"

[build-dependencies]
tonic-build = "0.12"
```

- [ ] **Step 2: Write `rust/build.rs`** (generates prost messages + tonic clients/servers from the shared `../proto` tree)

```rust
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_root = "../proto";
    let protos = [
        "org/deltav/minion/grpc/v1/heartbeat.proto",
        "org/deltav/minion/grpc/v1/rpc.proto",
        "org/deltav/minion/grpc/v1/twin.proto",
        "org/deltav/minion/grpc/v1/trap.proto",
        "org/deltav/minion/grpc/v1/syslog.proto",
        "org/deltav/minion/grpc/v1/telemetry.proto",
        "org/deltav/minion/grpc/v1/snmp_agent_config.proto",
        "org/deltav/minion/grpc/v1/collectionset.proto",
    ];
    let paths: Vec<String> = protos.iter().map(|p| format!("{proto_root}/{p}")).collect();
    tonic_build::configure()
        .build_client(true)
        .build_server(true) // server stubs used by the mock gateway in B8 integration tests
        .compile_protos(&paths, &[proto_root])?;
    Ok(())
}
```

- [ ] **Step 3: Write `rust/src/lib.rs`** (re-export the generated module tree)

```rust
//! Generated Delta-V Minion IPC contract (package org.deltav.minion.grpc.v1).
pub mod v1 {
    tonic::include_proto!("org.deltav.minion.grpc.v1");
}
```

- [ ] **Step 4: Verify Rust generation compiles**

Run: `cd rust && cargo build`
Expected: compiles; `cargo doc --no-deps` would show `opennms_ipc_contract::v1::Heartbeat`, `heartbeat_service_client::HeartbeatServiceClient`, `twin_channel_service_client::TwinChannelServiceClient`.

- [ ] **Step 5: Add Rust CI**

Create `.github/workflows/rust.yml`:

```yaml
# .github/workflows/rust.yml
name: rust
on:
  pull_request:
  push: { branches: [main] }
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - run: sudo apt-get update && sudo apt-get install -y protobuf-compiler
      - run: cd rust && cargo test
```

- [ ] **Step 6: Commit**

```bash
git add rust/Cargo.toml rust/build.rs rust/src/lib.rs .github/workflows/rust.yml
git commit -m "build: Rust codegen crate for the IPC contract"
```

---

### Task A7: Tier-1 golden-message corpus (round-trip in both languages)

The corpus is **captured bytes** for representative messages. Both Java and Rust must decode them, re-encode, and get identical bytes — proving the two codegen paths agree on the wire.

**Files:**
- Create: `corpus/heartbeat.bin`, `corpus/twin_update.bin`, `corpus/collectionset.bin`
- Create: `rust/tests/golden_corpus.rs`
- Create: `java/src/test/java/org/deltav/ipc/contract/GoldenCorpusRoundTripTest.java`

- [ ] **Step 1: Write the Rust round-trip test FIRST (it will fail — no corpus yet)**

```rust
// rust/tests/golden_corpus.rs
use opennms_ipc_contract::v1::{Heartbeat, TwinUpdate, CollectionSet};
use prost::Message;

fn load(name: &str) -> Vec<u8> {
    std::fs::read(format!("../corpus/{name}")).expect("corpus file present")
}

#[test]
fn heartbeat_round_trips() {
    let bytes = load("heartbeat.bin");
    let msg = Heartbeat::decode(&*bytes).expect("decode");
    assert_eq!(msg.minion_id, "00000000-0000-0000-0000-000000000001");
    assert_eq!(msg.location, "Default");
    let mut re = Vec::new();
    msg.encode(&mut re).unwrap();
    assert_eq!(re, bytes, "re-encode must be byte-identical");
}

#[test]
fn twin_update_round_trips() {
    let bytes = load("twin_update.bin");
    let msg = TwinUpdate::decode(&*bytes).expect("decode");
    assert_eq!(msg.consumer_key, "snmpv3-user");
    assert!(!msg.is_patch);
    let mut re = Vec::new();
    msg.encode(&mut re).unwrap();
    assert_eq!(re, bytes);
}

#[test]
fn collectionset_round_trips() {
    let bytes = load("collectionset.bin");
    let msg = CollectionSet::decode(&*bytes).expect("decode");
    assert_eq!(msg.resources.len(), 1);
    let mut re = Vec::new();
    msg.encode(&mut re).unwrap();
    assert_eq!(re, bytes);
}
```

- [ ] **Step 2: Run it — confirm it fails for the right reason**

Run: `cd rust && cargo test --test golden_corpus`
Expected: FAIL — `corpus file present: No such file or directory`.

- [ ] **Step 3: Generate the corpus with a one-shot Rust helper, then freeze the bytes**

Create `rust/examples/gen_corpus.rs`:

```rust
// rust/examples/gen_corpus.rs
// Run once: `cargo run --example gen_corpus`. Writes deterministic fixtures.
use opennms_ipc_contract::v1::{
    Heartbeat, TwinUpdate, CollectionSet, Resource, AttributeGroup, Attribute, AttributeType,
    attribute::Value,
};
use prost::Message;
use prost_types::Timestamp;

fn write(name: &str, bytes: Vec<u8>) {
    std::fs::write(format!("../corpus/{name}"), bytes).unwrap();
}

fn main() {
    let hb = Heartbeat {
        minion_id: "00000000-0000-0000-0000-000000000001".into(),
        location: "Default".into(),
        sent_at: Some(Timestamp { seconds: 1_700_000_000, nanos: 0 }),
        version: "1.3.0".into(),
    };
    write("heartbeat.bin", hb.encode_to_vec());

    let tu = TwinUpdate {
        consumer_key: "snmpv3-user".into(),
        twin_object: br#"{"users":[]}"#.to_vec(),
        is_patch: false,
        version: 1,
        session_id: "sess-1".into(),
        location: "Default".into(),
        dispatched_at: Some(Timestamp { seconds: 1_700_000_000, nanos: 0 }),
    };
    write("twin_update.bin", tu.encode_to_vec());

    let cs = CollectionSet {
        timestamp_ms: 1_700_000_000_000,
        resources: vec![Resource {
            resource_id: "node[1].node".into(),
            r#type: "node".into(),
            instance: "".into(),
            groups: vec![AttributeGroup {
                name: "mib2-tcp".into(),
                attributes: vec![Attribute {
                    name: "tcpActiveOpens".into(),
                    r#type: AttributeType::Counter as i32,
                    value: Some(Value::Numeric(42.0)),
                }],
            }],
        }],
    };
    write("collectionset.bin", cs.encode_to_vec());
}
```

Run: `cd rust && cargo run --example gen_corpus`
Expected: writes `corpus/heartbeat.bin`, `corpus/twin_update.bin`, `corpus/collectionset.bin`.

- [ ] **Step 4: Run the Rust round-trip test — confirm it passes**

Run: `cd rust && cargo test --test golden_corpus`
Expected: 3 passed.

- [ ] **Step 5: Write the Java round-trip test against the SAME corpus**

```java
// java/src/test/java/org/deltav/ipc/contract/GoldenCorpusRoundTripTest.java
package org.deltav.ipc.contract;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.nio.file.Files;
import java.nio.file.Path;

import org.deltav.minion.grpc.v1.Heartbeat;
import org.deltav.minion.grpc.v1.TwinUpdate;
import org.deltav.minion.grpc.v1.CollectionSet;
import org.junit.Test;

public class GoldenCorpusRoundTripTest {

    private byte[] load(String name) throws Exception {
        return Files.readAllBytes(Path.of("..", "corpus", name));
    }

    @Test
    public void heartbeatRoundTrips() throws Exception {
        byte[] bytes = load("heartbeat.bin");
        Heartbeat msg = Heartbeat.parseFrom(bytes);
        assertEquals("00000000-0000-0000-0000-000000000001", msg.getMinionId());
        assertEquals("Default", msg.getLocation());
        assertArrayEquals(bytes, msg.toByteArray());
    }

    @Test
    public void twinUpdateRoundTrips() throws Exception {
        byte[] bytes = load("twin_update.bin");
        TwinUpdate msg = TwinUpdate.parseFrom(bytes);
        assertEquals("snmpv3-user", msg.getConsumerKey());
        assertArrayEquals(bytes, msg.toByteArray());
    }

    @Test
    public void collectionSetRoundTrips() throws Exception {
        byte[] bytes = load("collectionset.bin");
        CollectionSet msg = CollectionSet.parseFrom(bytes);
        assertEquals(1, msg.getResourcesCount());
        assertArrayEquals(bytes, msg.toByteArray());
    }
}
```

- [ ] **Step 6: Run the Java round-trip test — confirm it passes**

Run: `cd java && mvn -q test -Dtest=GoldenCorpusRoundTripTest`
Expected: BUILD SUCCESS, 3 tests pass. This proves Java and Rust agree on the captured bytes.

- [ ] **Step 7: Commit**

```bash
git add corpus rust/examples/gen_corpus.rs rust/tests/golden_corpus.rs \
        java/src/test/java/org/deltav/ipc/contract/GoldenCorpusRoundTripTest.java
git commit -m "test: Tier-1 golden-message corpus round-tripped in Java and Rust"
```

---

### Task A8: CollectionSet round-trip against the real Java collector output

De-risks the riskiest contract item: prove the flat `CollectionSet` proto faithfully represents a Java `CollectionSet`. This reuses the **existing** delta-v translator (`CollectionSetToProtobufTranslator`) which already maps the visitor tree to the frozen flat shape — so the round-trip is: Java visitor tree → proto bytes → Rust decode → assert structure matches.

**Files:**
- Create: `corpus/collectionset_from_java.bin` (captured from a Java collector fixture)
- Create: `rust/tests/collectionset_shape.rs`

- [ ] **Step 1: Capture a CollectionSet from a Java collector fixture**

In the delta-v tree, a unit test already builds a `CollectionSet` for the SNMP collector. Add a one-shot capture that walks a representative `CollectionSet` through the existing translator and writes the proto bytes. Add to the delta-v `core/daemon-boot-collectd` test sources:

```java
// (delta-v tree) core/daemon-boot-collectd/src/test/java/org/deltav/collectd/timeseries/CollectionSetCorpusCaptureTest.java
package org.deltav.collectd.timeseries;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Test;

public class CollectionSetCorpusCaptureTest {
    @Test
    public void captureRepresentativeCollectionSet() throws Exception {
        // Build a CollectionSet with one node resource, one mib2-tcp group,
        // one COUNTER attribute (mirror the fixtures already used by
        // CollectionSetToProtobufTranslatorTest), run it through the
        // existing translator, and dump the TimeseriesBatch bytes.
        byte[] bytes = TranslatorTestSupport.translateRepresentativeSetToBytes();
        Path out = Path.of(System.getProperty("corpus.out",
            "/tmp/collectionset_from_java.bin"));
        Files.write(out, bytes);
    }
}
```

Run: `cd $DELTAV && ./mvnw -q -pl :org.opennms.core.daemon-boot-collectd test -Dtest=CollectionSetCorpusCaptureTest -Dcorpus.out=$PWD/../opennms-ipc-contract/corpus/collectionset_from_java.bin`
Expected: writes `corpus/collectionset_from_java.bin`. (If `TranslatorTestSupport` does not exist, inline the fixture-building from the existing `CollectionSetToProtobufTranslatorTest`; the translator class is `core/daemon-boot-collectd/.../CollectionSetToProtobufTranslator.java`.)

Note on field mapping: the delta-v `TimeseriesBatch` wraps `Resource` with extra envelope fields (`node_id`, `location`, `collection_package`, `producer`). The contract's `CollectionSet` keeps only the `Resource` subtree plus `timestamp_ms`. The capture must serialize a `CollectionSet` (timestamp + resources), not the full `TimeseriesBatch`. Build the `CollectionSet` from the translated `Resource` list so the bytes match the contract schema.

- [ ] **Step 2: Write the Rust shape assertion test FIRST (fails — no fixture yet if Step 1 not run)**

```rust
// rust/tests/collectionset_shape.rs
use opennms_ipc_contract::v1::{CollectionSet, AttributeType, attribute::Value};
use prost::Message;

#[test]
fn java_collectionset_decodes_with_expected_shape() {
    let bytes = std::fs::read("../corpus/collectionset_from_java.bin")
        .expect("capture fixture present (run CollectionSetCorpusCaptureTest)");
    let cs = CollectionSet::decode(&*bytes).expect("decode java-produced bytes");

    assert!(!cs.resources.is_empty(), "at least one resource");
    let node = cs.resources.iter().find(|r| r.r#type == "node")
        .expect("a node resource");
    let group = node.groups.iter().find(|g| g.name == "mib2-tcp")
        .expect("mib2-tcp group");
    let attr = group.attributes.iter().find(|a| a.name == "tcpActiveOpens")
        .expect("tcpActiveOpens attribute");
    assert_eq!(attr.r#type, AttributeType::Counter as i32);
    match attr.value {
        Some(Value::Numeric(_)) => {}
        _ => panic!("counter attribute must carry a numeric value"),
    }
}
```

- [ ] **Step 3: Run it — confirm pass against the Java-produced bytes**

Run: `cd rust && cargo test --test collectionset_shape`
Expected: pass. This proves Rust decodes a CollectionSet emitted by the real Java collector path with the expected resource/group/attribute structure.

- [ ] **Step 4: Commit**

```bash
git add corpus/collectionset_from_java.bin rust/tests/collectionset_shape.rs
git commit -m "test: round-trip Java collector CollectionSet bytes through the Rust contract"
```

---

# Repo B — `rusty-minion`

> All Repo B tasks run inside the new `rusty-minion` git repo (Task B1 creates it). The contract repo's Rust crate is consumed as a pinned git dependency. Paths in B-tasks are relative to the `rusty-minion` root.

### Task B1: Cargo workspace skeleton + CI

**Files:**
- Create: `Cargo.toml`, `rust-toolchain.toml`, `.gitignore`, `LICENSE`, `README.md`
- Create: `crates/{config,transport,twin,minion}/Cargo.toml` (+ empty `src/lib.rs`/`src/main.rs`)
- Create: `.github/workflows/ci.yml`

- [ ] **Step 1: Init repo + workspace manifest**

```bash
mkdir -p rusty-minion && cd rusty-minion
git init -b main
curl -fsSL https://www.gnu.org/licenses/agpl-3.0.txt -o LICENSE
```

`Cargo.toml`:

```toml
[workspace]
resolver = "2"
members = ["crates/config", "crates/transport", "crates/twin", "crates/minion"]

[workspace.package]
version = "0.1.0"
edition = "2021"
license = "AGPL-3.0"

[workspace.dependencies]
tokio = { version = "1", features = ["full"] }
tonic = "0.12"
prost = "0.13"
prost-types = "0.13"
tower = "0.5"
http = "1"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
serde = { version = "1", features = ["derive"] }
toml = "0.8"
anyhow = "1"
thiserror = "1"
async-trait = "0.1"
futures = "0.3"
json-patch = "2"
serde_json = "1"
# Pin the contract crate to a tagged commit; bump on contract releases.
opennms-ipc-contract = { git = "https://github.com/pbrane/opennms-ipc-contract.git", tag = "v0.1.0" }
```

- [ ] **Step 2: `rust-toolchain.toml` and `.gitignore`**

```toml
# rust-toolchain.toml
[toolchain]
channel = "stable"
components = ["rustfmt", "clippy"]
```

```gitignore
/target
```

- [ ] **Step 3: Create the four member crates as compiling stubs**

```toml
# crates/config/Cargo.toml
[package]
name = "minion-config"
version.workspace = true
edition.workspace = true
license.workspace = true

[dependencies]
serde.workspace = true
toml.workspace = true
thiserror.workspace = true
```

`crates/config/src/lib.rs`: `// filled in Task B2`

```toml
# crates/transport/Cargo.toml
[package]
name = "minion-transport"
version.workspace = true
edition.workspace = true
license.workspace = true

[dependencies]
opennms-ipc-contract.workspace = true
tonic.workspace = true
prost.workspace = true
prost-types.workspace = true
tower.workspace = true
http.workspace = true
tokio.workspace = true
async-trait.workspace = true
anyhow.workspace = true
tracing.workspace = true
futures.workspace = true
```

`crates/transport/src/lib.rs`: `// filled in Task B3`

```toml
# crates/twin/Cargo.toml
[package]
name = "minion-twin"
version.workspace = true
edition.workspace = true
license.workspace = true

[dependencies]
json-patch.workspace = true
serde_json.workspace = true
thiserror.workspace = true
tracing.workspace = true
```

`crates/twin/src/lib.rs`: `// filled in Task B5`

```toml
# crates/minion/Cargo.toml
[package]
name = "minion"
version.workspace = true
edition.workspace = true
license.workspace = true

[[bin]]
name = "rusty-minion"
path = "src/main.rs"

[dependencies]
minion-config = { path = "../config" }
minion-transport = { path = "../transport" }
minion-twin = { path = "../twin" }
opennms-ipc-contract.workspace = true
tokio.workspace = true
tonic.workspace = true
prost-types.workspace = true
tracing.workspace = true
tracing-subscriber.workspace = true
anyhow.workspace = true
futures.workspace = true
```

`crates/minion/src/main.rs`:

```rust
fn main() {
    println!("rusty-minion placeholder");
}
```

- [ ] **Step 4: Verify the workspace builds**

Run: `cargo build`
Expected: compiles all four crates (contract pulled from git). If the contract tag `v0.1.0` is not yet pushed, temporarily use `path = "../opennms-ipc-contract/rust"` and switch to the git tag before Task B8.

- [ ] **Step 5: CI workflow**

```yaml
# .github/workflows/ci.yml
name: ci
on: [pull_request, push]
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with: { components: rustfmt, clippy }
      - run: sudo apt-get update && sudo apt-get install -y protobuf-compiler
      - run: cargo fmt --all --check
      - run: cargo clippy --all-targets -- -D warnings
      - run: cargo test
```

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "chore: rusty-minion cargo workspace skeleton + CI"
```

---

### Task B2: Bootstrap config loader (TOML) + identity

**Files:**
- Create: `crates/config/src/lib.rs`
- Create: `config/minion.example.toml`
- Test: `crates/config/src/lib.rs` (inline `#[cfg(test)]`)

- [ ] **Step 1: Write the failing test**

```rust
// crates/config/src/lib.rs  (append below the impl once written; shown here first to drive it)
#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"
        [minion]
        id = "00000000-0000-0000-0000-000000000001"
        location = "Default"

        [gateway]
        host = "envoy"
        port = 8443
        tls = true
    "#;

    #[test]
    fn parses_full_config() {
        let cfg: MinionConfig = toml::from_str(SAMPLE).unwrap();
        assert_eq!(cfg.minion.id, "00000000-0000-0000-0000-000000000001");
        assert_eq!(cfg.minion.location, "Default");
        assert_eq!(cfg.gateway.host, "envoy");
        assert_eq!(cfg.gateway.port, 8443);
        assert!(cfg.gateway.tls);
    }

    #[test]
    fn applies_defaults_for_optional_fields() {
        let cfg: MinionConfig = toml::from_str(
            "[minion]\nid=\"m1\"\n[gateway]\nhost=\"h\"\n").unwrap();
        assert_eq!(cfg.minion.location, "Default"); // default
        assert_eq!(cfg.gateway.port, 8443);          // default
        assert!(cfg.gateway.tls);                    // default true
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p minion-config`
Expected: FAIL — `cannot find type MinionConfig`.

- [ ] **Step 3: Write the implementation**

```rust
// crates/config/src/lib.rs (top of file)
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct MinionConfig {
    pub minion: MinionIdentitySection,
    pub gateway: GatewaySection,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MinionIdentitySection {
    pub id: String,
    #[serde(default = "default_location")]
    pub location: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GatewaySection {
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "default_tls")]
    pub tls: bool,
}

fn default_location() -> String { "Default".to_string() }
fn default_port() -> u16 { 8443 }
fn default_tls() -> bool { true }

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("reading {path}: {source}")]
    Io { path: String, source: std::io::Error },
    #[error("parsing {path}: {source}")]
    Parse { path: String, source: toml::de::Error },
}

impl MinionConfig {
    /// Load from a TOML file. Environment overrides (MINION_ID, MINION_LOCATION,
    /// MINION_GATEWAY_HOST, MINION_GATEWAY_PORT) are applied by the binary in B6,
    /// keeping this crate pure/testable.
    pub fn from_path(path: &str) -> Result<Self, ConfigError> {
        let text = std::fs::read_to_string(path)
            .map_err(|source| ConfigError::Io { path: path.into(), source })?;
        toml::from_str(&text)
            .map_err(|source| ConfigError::Parse { path: path.into(), source })
    }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p minion-config`
Expected: 2 passed.

- [ ] **Step 5: Write `config/minion.example.toml`**

```toml
# rusty-minion bootstrap configuration.
# Env overrides (applied by the binary): MINION_ID, MINION_LOCATION,
# MINION_GATEWAY_HOST, MINION_GATEWAY_PORT.

[minion]
id = "00000000-0000-0000-0000-000000000001"
location = "Default"

[gateway]
host = "envoy"   # gateway frontend; 9090 plaintext direct, 8443 TLS via Envoy
port = 8443
tls = true
```

- [ ] **Step 6: Commit**

```bash
git add crates/config/src/lib.rs config/minion.example.toml
git commit -m "feat(config): TOML bootstrap config loader with defaults"
```

---

### Task B3: Transport trait + identity interceptor + gRPC channel

The gateway rejects any stream missing `x-minion-id`/`x-minion-location` with `UNAUTHENTICATED`. The interceptor that attaches them is the first thing with teeth.

**Files:**
- Create: `crates/transport/src/lib.rs`
- Create: `crates/transport/src/identity.rs`
- Create: `crates/transport/src/grpc.rs`
- Test: `crates/transport/src/identity.rs` (inline)

- [ ] **Step 1: Write the failing test for the identity interceptor**

```rust
// crates/transport/src/identity.rs
use tonic::service::Interceptor;
use tonic::{Request, Status};

/// Attaches the gateway-required identity metadata to every outbound call.
/// Header keys MUST match MinionIdentityServerInterceptor: x-minion-id / x-minion-location.
#[derive(Clone)]
pub struct MinionIdentityInterceptor {
    pub minion_id: String,
    pub location: String,
}

impl Interceptor for MinionIdentityInterceptor {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        let md = req.metadata_mut();
        md.insert(
            "x-minion-id",
            self.minion_id.parse().map_err(|_| Status::invalid_argument("minion id"))?,
        );
        md.insert(
            "x-minion-location",
            self.location.parse().map_err(|_| Status::invalid_argument("location"))?,
        );
        Ok(req)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::Request;

    #[test]
    fn injects_both_identity_headers() {
        let mut interceptor = MinionIdentityInterceptor {
            minion_id: "m-1".into(),
            location: "Default".into(),
        };
        let req = interceptor.call(Request::new(())).unwrap();
        let md = req.metadata();
        assert_eq!(md.get("x-minion-id").unwrap(), "m-1");
        assert_eq!(md.get("x-minion-location").unwrap(), "Default");
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p minion-transport`
Expected: FAIL — `module identity not declared` / unresolved (the lib root does not yet declare `mod identity`).

- [ ] **Step 3: Write the Transport trait (`lib.rs`) and the gRPC channel builder (`grpc.rs`)**

```rust
// crates/transport/src/lib.rs
pub mod identity;
pub mod grpc;

use async_trait::async_trait;

/// Role: fire-and-forget Sink producer (heartbeat in Phase 0; traps/syslog/flows later).
#[async_trait]
pub trait SinkProducer: Send + Sync {
    /// Stream heartbeats until `shutdown` resolves. Returns on graceful stop or fatal error.
    async fn run_heartbeat(&self, shutdown: tokio::sync::watch::Receiver<bool>) -> anyhow::Result<()>;
}

/// Role: Twin subscriber — opens the Twin channel, forwards updates to a sink.
#[async_trait]
pub trait TwinSubscriber: Send + Sync {
    async fn run(
        &self,
        consumer_keys: Vec<String>,
        updates: tokio::sync::mpsc::Sender<opennms_ipc_contract::v1::TwinUpdate>,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> anyhow::Result<()>;
}
```

```rust
// crates/transport/src/grpc.rs
use std::time::Duration;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

/// Connection parameters mirrored from the Java Minion client
/// (GrpcHeartbeatDispatcherConfiguration: usePlaintext to :9090, keepalive 30s).
#[derive(Clone)]
pub struct GatewayEndpoint {
    pub host: String,
    pub port: u16,
    pub tls: bool,
}

impl GatewayEndpoint {
    pub fn channel_endpoint(&self) -> anyhow::Result<Endpoint> {
        let scheme = if self.tls { "https" } else { "http" };
        let uri = format!("{scheme}://{}:{}", self.host, self.port);
        let mut ep = Channel::from_shared(uri)?
            .keep_alive_while_idle(true)
            .http2_keep_alive_interval(Duration::from_secs(30))
            .keep_alive_timeout(Duration::from_secs(20));
        if self.tls {
            ep = ep.tls_config(ClientTlsConfig::new().with_native_roots())?;
        }
        Ok(ep)
    }

    /// Lazily connect; tonic reconnects on transport failure with default backoff.
    pub fn lazy_channel(&self) -> anyhow::Result<Channel> {
        Ok(self.channel_endpoint()?.connect_lazy())
    }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p minion-transport`
Expected: `injects_both_identity_headers` passes; crate compiles.

- [ ] **Step 5: Commit**

```bash
git add crates/transport/src/lib.rs crates/transport/src/identity.rs crates/transport/src/grpc.rs
git commit -m "feat(transport): Transport trait, identity interceptor, gRPC channel builder"
```

---

### Task B4: Heartbeat producer (GrpcTransport `SinkProducer`)

**Files:**
- Create: `crates/transport/src/heartbeat.rs`
- Modify: `crates/transport/src/lib.rs` (add `pub mod heartbeat;`)
- Test: `crates/transport/src/heartbeat.rs` (inline — message construction)

- [ ] **Step 1: Write the failing test for heartbeat message construction**

```rust
// crates/transport/src/heartbeat.rs
use opennms_ipc_contract::v1::Heartbeat;

/// Build a Heartbeat payload. `epoch_secs` is injected (no wall-clock in pure fn) so it is testable.
pub fn build_heartbeat(minion_id: &str, location: &str, version: &str, epoch_secs: i64) -> Heartbeat {
    Heartbeat {
        minion_id: minion_id.to_string(),
        location: location.to_string(),
        sent_at: Some(prost_types::Timestamp { seconds: epoch_secs, nanos: 0 }),
        version: version.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_identity_carrying_heartbeat() {
        let hb = build_heartbeat("m-1", "Default", "1.3.0", 1_700_000_000);
        assert_eq!(hb.minion_id, "m-1");
        assert_eq!(hb.location, "Default");
        assert_eq!(hb.version, "1.3.0");
        assert_eq!(hb.sent_at.unwrap().seconds, 1_700_000_000);
    }
}
```

- [ ] **Step 2: Add `pub mod heartbeat;` to `lib.rs`, run the test to verify it fails then passes**

Add to `crates/transport/src/lib.rs`: `pub mod heartbeat;`

Run: `cargo test -p minion-transport heartbeat`
Expected: PASS (`builds_identity_carrying_heartbeat`).

- [ ] **Step 3: Implement the `GrpcHeartbeatProducer` streaming loop**

Append to `crates/transport/src/heartbeat.rs`:

```rust
use crate::grpc::GatewayEndpoint;
use crate::identity::MinionIdentityInterceptor;
use crate::SinkProducer;
use async_trait::async_trait;
use opennms_ipc_contract::v1::heartbeat_service_client::HeartbeatServiceClient;
use std::time::Duration;
use tokio::sync::watch;
use tokio_stream::wrappers::IntervalStream;
use tokio_stream::StreamExt;

pub struct GrpcHeartbeatProducer {
    pub endpoint: GatewayEndpoint,
    pub interceptor: MinionIdentityInterceptor,
    pub version: String,
    pub period: Duration, // 30s in production; short in tests
}

#[async_trait]
impl SinkProducer for GrpcHeartbeatProducer {
    async fn run_heartbeat(&self, mut shutdown: watch::Receiver<bool>) -> anyhow::Result<()> {
        let channel = self.endpoint.lazy_channel()?;
        let mut client =
            HeartbeatServiceClient::with_interceptor(channel, self.interceptor.clone());

        let minion_id = self.interceptor.minion_id.clone();
        let location = self.interceptor.location.clone();
        let version = self.version.clone();
        let period = self.period;

        // Outbound stream: emit a Heartbeat every `period` until shutdown flips true.
        let mut stop = shutdown.clone();
        let outbound = async_stream::stream! {
            let mut ticks = IntervalStream::new(tokio::time::interval(period));
            loop {
                tokio::select! {
                    _ = ticks.next() => {
                        let secs = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_secs() as i64).unwrap_or(0);
                        yield build_heartbeat(&minion_id, &location, &version, secs);
                    }
                    _ = stop.changed() => { if *stop.borrow() { break; } }
                }
            }
        };

        let mut acks = client.publish(outbound).await?.into_inner();
        loop {
            tokio::select! {
                msg = acks.next() => match msg {
                    Some(Ok(ack)) => tracing::debug!(minion_id = %ack.minion_id, "heartbeat ack"),
                    Some(Err(status)) => {
                        tracing::warn!(%status, "heartbeat stream error; returning for reconnect");
                        return Ok(());
                    }
                    None => return Ok(()),
                },
                _ = shutdown.changed() => if *shutdown.borrow() {
                    tracing::info!("heartbeat shutdown requested");
                    return Ok(());
                },
            }
        }
    }
}
```

Add to `crates/transport/Cargo.toml` dependencies: `async-stream = "0.3"`, `tokio-stream = "0.1"`.

- [ ] **Step 4: Verify the crate still compiles and tests pass**

Run: `cargo test -p minion-transport`
Expected: compiles; `builds_identity_carrying_heartbeat` passes. (The streaming loop is exercised end-to-end against the mock gateway in Task B8.)

- [ ] **Step 5: Commit**

```bash
git add crates/transport/src/heartbeat.rs crates/transport/src/lib.rs crates/transport/Cargo.toml
git commit -m "feat(transport): gRPC heartbeat producer streaming HeartbeatService.Publish"
```

---

### Task B5: Twin subscriber state machine (version tracking + patch + reconnect signal)

The Twin protocol: SUBSCRIBE → full snapshot (`is_patch=false`) → JSON-patch updates with monotonic `version` per `(consumer_key, location)`; on a version gap or `session_id` change the subscriber must drop and reconnect. This task is the pure state machine; the gRPC stream wiring is in B8/B6.

**Files:**
- Create: `crates/twin/src/lib.rs`
- Test: `crates/twin/src/lib.rs` (inline)

- [ ] **Step 1: Write the failing tests**

```rust
// crates/twin/src/lib.rs
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn snapshot(key: &str, version: i32, session: &str, body: serde_json::Value) -> TwinUpdateIn {
        TwinUpdateIn {
            consumer_key: key.into(),
            location: "Default".into(),
            session_id: session.into(),
            version,
            is_patch: false,
            twin_object: serde_json::to_vec(&body).unwrap(),
        }
    }

    #[test]
    fn accepts_initial_snapshot() {
        let mut store = TwinStore::new();
        let out = store.apply(snapshot("snmpv3-user", 1, "s1", json!({"users": []}))).unwrap();
        assert_eq!(out, Outcome::Updated);
        assert_eq!(store.state("snmpv3-user").unwrap(), &json!({"users": []}));
    }

    #[test]
    fn applies_contiguous_patch() {
        let mut store = TwinStore::new();
        store.apply(snapshot("snmpv3-user", 1, "s1", json!({"users": []}))).unwrap();
        let patch = json!([{"op": "add", "path": "/users/-", "value": "alice"}]);
        let upd = TwinUpdateIn {
            consumer_key: "snmpv3-user".into(), location: "Default".into(),
            session_id: "s1".into(), version: 2, is_patch: true,
            twin_object: serde_json::to_vec(&patch).unwrap(),
        };
        assert_eq!(store.apply(upd).unwrap(), Outcome::Updated);
        assert_eq!(store.state("snmpv3-user").unwrap(), &json!({"users": ["alice"]}));
    }

    #[test]
    fn version_gap_requests_reconnect() {
        let mut store = TwinStore::new();
        store.apply(snapshot("snmpv3-user", 1, "s1", json!({}))).unwrap();
        let upd = TwinUpdateIn {
            consumer_key: "snmpv3-user".into(), location: "Default".into(),
            session_id: "s1".into(), version: 5, is_patch: true, // gap: expected 2
            twin_object: serde_json::to_vec(&json!([])).unwrap(),
        };
        assert_eq!(store.apply(upd).unwrap(), Outcome::Reconnect);
    }

    #[test]
    fn session_change_resyncs_on_snapshot() {
        let mut store = TwinStore::new();
        store.apply(snapshot("snmpv3-user", 7, "s1", json!({"a": 1}))).unwrap();
        // New publisher session: snapshot at any version is accepted as a resync.
        let out = store.apply(snapshot("snmpv3-user", 1, "s2", json!({"b": 2}))).unwrap();
        assert_eq!(out, Outcome::Updated);
        assert_eq!(store.state("snmpv3-user").unwrap(), &json!({"b": 2}));
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p minion-twin`
Expected: FAIL — `TwinStore`, `TwinUpdateIn`, `Outcome` not found.

- [ ] **Step 3: Write the implementation**

```rust
// crates/twin/src/lib.rs (top of file)
use std::collections::HashMap;

/// Transport-agnostic view of a Twin update (decoupled from the prost type so this
/// crate has no gRPC dependency and stays trivially testable).
#[derive(Debug, Clone)]
pub struct TwinUpdateIn {
    pub consumer_key: String,
    pub location: String,
    pub session_id: String,
    pub version: i32,
    pub is_patch: bool,
    pub twin_object: Vec<u8>, // JSON: full state or RFC6902 patch
}

#[derive(Debug, PartialEq, Eq)]
pub enum Outcome {
    Updated,
    Reconnect, // caller must drop the stream and re-SUBSCRIBE
}

#[derive(Debug, thiserror::Error)]
pub enum TwinError {
    #[error("invalid json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("patch apply failed: {0}")]
    Patch(#[from] json_patch::PatchError),
}

struct Entry {
    session_id: String,
    version: i32,
    state: serde_json::Value,
}

pub struct TwinStore {
    entries: HashMap<String, Entry>, // keyed by consumer_key
}

impl TwinStore {
    pub fn new() -> Self { Self { entries: HashMap::new() } }

    pub fn state(&self, consumer_key: &str) -> Option<&serde_json::Value> {
        self.entries.get(consumer_key).map(|e| &e.state)
    }

    pub fn apply(&mut self, upd: TwinUpdateIn) -> Result<Outcome, TwinError> {
        if !upd.is_patch {
            // Full snapshot: always accepted; (re)establishes session + version.
            let state: serde_json::Value = serde_json::from_slice(&upd.twin_object)?;
            self.entries.insert(upd.consumer_key, Entry {
                session_id: upd.session_id, version: upd.version, state,
            });
            return Ok(Outcome::Updated);
        }
        // Patch: requires a contiguous version within the same session.
        match self.entries.get_mut(&upd.consumer_key) {
            Some(entry) if entry.session_id == upd.session_id
                        && upd.version == entry.version + 1 => {
                let patch: json_patch::Patch = serde_json::from_slice(&upd.twin_object)?;
                json_patch::patch(&mut entry.state, &patch)?;
                entry.version = upd.version;
                Ok(Outcome::Updated)
            }
            // No prior state, session changed, or version gap → resync.
            _ => Ok(Outcome::Reconnect),
        }
    }
}

impl Default for TwinStore {
    fn default() -> Self { Self::new() }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p minion-twin`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add crates/twin/src/lib.rs
git commit -m "feat(twin): versioned Twin store with snapshot/patch/reconnect semantics"
```

---

### Task B6: Twin subscriber gRPC wiring + runtime/lifecycle

Wire `TwinStore` to the `TwinChannelService.Channel` bidi stream, and assemble the tokio runtime: load config, apply env overrides, build identity, start heartbeat + twin tasks, graceful shutdown on SIGINT/SIGTERM.

**Files:**
- Create: `crates/transport/src/twin.rs`
- Modify: `crates/transport/src/lib.rs` (`pub mod twin;`)
- Create: `crates/minion/src/runtime.rs`
- Modify: `crates/minion/src/main.rs`

- [ ] **Step 1: Implement the Twin gRPC subscriber (`GrpcTwinSubscriber`)**

```rust
// crates/transport/src/twin.rs
use crate::grpc::GatewayEndpoint;
use crate::identity::MinionIdentityInterceptor;
use crate::TwinSubscriber;
use async_trait::async_trait;
use opennms_ipc_contract::v1::twin_channel_service_client::TwinChannelServiceClient;
use opennms_ipc_contract::v1::{SubscriptionMode, TwinSubscription, TwinUpdate};
use tokio::sync::{mpsc, watch};
use tokio_stream::StreamExt;

pub struct GrpcTwinSubscriber {
    pub endpoint: GatewayEndpoint,
    pub interceptor: MinionIdentityInterceptor,
}

#[async_trait]
impl TwinSubscriber for GrpcTwinSubscriber {
    async fn run(
        &self,
        consumer_keys: Vec<String>,
        updates: mpsc::Sender<TwinUpdate>,
        mut shutdown: watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        let channel = self.endpoint.lazy_channel()?;
        let mut client =
            TwinChannelServiceClient::with_interceptor(channel, self.interceptor.clone());

        // Outbound: one SUBSCRIBE per consumer_key, then idle until shutdown.
        let mut stop = shutdown.clone();
        let outbound = async_stream::stream! {
            for key in consumer_keys {
                yield TwinSubscription { consumer_key: key, mode: SubscriptionMode::Subscribe as i32 };
            }
            let _ = stop.changed().await; // hold the stream open
        };

        let mut inbound = client.channel(outbound).await?.into_inner();
        loop {
            tokio::select! {
                msg = inbound.next() => match msg {
                    Some(Ok(update)) => {
                        if updates.send(update).await.is_err() { return Ok(()); }
                    }
                    Some(Err(status)) => {
                        tracing::warn!(%status, "twin stream error; returning for reconnect");
                        return Ok(());
                    }
                    None => return Ok(()),
                },
                _ = shutdown.changed() => if *shutdown.borrow() { return Ok(()); },
            }
        }
    }
}
```

Add `pub mod twin;` to `crates/transport/src/lib.rs`.

- [ ] **Step 2: Implement the runtime assembly**

```rust
// crates/minion/src/runtime.rs
use minion_config::MinionConfig;
use minion_transport::grpc::GatewayEndpoint;
use minion_transport::heartbeat::GrpcHeartbeatProducer;
use minion_transport::identity::MinionIdentityInterceptor;
use minion_transport::twin::GrpcTwinSubscriber;
use minion_transport::{SinkProducer, TwinSubscriber};
use minion_twin::{TwinStore, TwinUpdateIn};
use std::time::Duration;
use tokio::sync::{mpsc, watch};

pub struct RuntimeParams {
    pub config: MinionConfig,
    pub version: String,
    pub heartbeat_period: Duration,
    pub twin_keys: Vec<String>,
}

/// Apply environment overrides onto a loaded config (mirrors the Java env keys).
pub fn apply_env_overrides(mut cfg: MinionConfig) -> MinionConfig {
    if let Ok(v) = std::env::var("MINION_ID") { cfg.minion.id = v; }
    if let Ok(v) = std::env::var("MINION_LOCATION") { cfg.minion.location = v; }
    if let Ok(v) = std::env::var("MINION_GATEWAY_HOST") { cfg.gateway.host = v; }
    if let Ok(v) = std::env::var("MINION_GATEWAY_PORT") {
        if let Ok(p) = v.parse() { cfg.gateway.port = p; }
    }
    cfg
}

pub async fn run(params: RuntimeParams, shutdown: watch::Receiver<bool>) -> anyhow::Result<()> {
    let cfg = params.config;
    let endpoint = GatewayEndpoint {
        host: cfg.gateway.host.clone(),
        port: cfg.gateway.port,
        tls: cfg.gateway.tls,
    };
    let interceptor = MinionIdentityInterceptor {
        minion_id: cfg.minion.id.clone(),
        location: cfg.minion.location.clone(),
    };

    let heartbeat = GrpcHeartbeatProducer {
        endpoint: endpoint.clone(),
        interceptor: interceptor.clone(),
        version: params.version,
        period: params.heartbeat_period,
    };
    let twin = GrpcTwinSubscriber { endpoint, interceptor };

    let (tx, mut rx) = mpsc::channel(64);
    let twin_keys = params.twin_keys;

    let hb_shutdown = shutdown.clone();
    let twin_shutdown = shutdown.clone();

    // Twin update consumer: drive the pure TwinStore; reconnect on Outcome::Reconnect.
    let consumer = tokio::spawn(async move {
        let mut store = TwinStore::new();
        while let Some(u) = rx.recv().await {
            let incoming = TwinUpdateIn {
                consumer_key: u.consumer_key.clone(),
                location: u.location,
                session_id: u.session_id,
                version: u.version,
                is_patch: u.is_patch,
                twin_object: u.twin_object,
            };
            match store.apply(incoming) {
                Ok(minion_twin::Outcome::Updated) =>
                    tracing::info!(key = %u.consumer_key, version = u.version, "twin updated"),
                Ok(minion_twin::Outcome::Reconnect) =>
                    tracing::warn!(key = %u.consumer_key, "twin version/session break; reconnect needed"),
                Err(e) => tracing::error!(error = %e, "twin apply failed"),
            }
        }
    });

    let hb = tokio::spawn(async move { heartbeat.run_heartbeat(hb_shutdown).await });
    let tw = tokio::spawn(async move { twin.run(twin_keys, tx, twin_shutdown).await });

    let _ = tokio::try_join!(
        async { hb.await? },
        async { tw.await? },
    );
    consumer.abort();
    Ok(())
}
```

- [ ] **Step 3: Implement `main.rs` (config load, signal handling, graceful shutdown)**

```rust
// crates/minion/src/main.rs
mod runtime;

use std::time::Duration;
use tokio::sync::watch;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let path = std::env::var("MINION_CONFIG").unwrap_or_else(|_| "config/minion.toml".into());
    let cfg = runtime::apply_env_overrides(minion_config::MinionConfig::from_path(&path)?);
    tracing::info!(id = %cfg.minion.id, location = %cfg.minion.location,
                   host = %cfg.gateway.host, port = cfg.gateway.port, "rusty-minion starting");

    let (tx, rx) = watch::channel(false);
    let params = runtime::RuntimeParams {
        config: cfg,
        version: env!("CARGO_PKG_VERSION").to_string(),
        heartbeat_period: Duration::from_secs(30),
        twin_keys: vec!["snmpv3-user".to_string()], // Phase 0 subscribes; SNMP monitor consumes in Phase 2
    };

    let runner = tokio::spawn(runtime::run(params, rx));

    shutdown_signal().await;
    tracing::info!("shutdown signal received; draining");
    let _ = tx.send(true);
    let _ = runner.await;
    Ok(())
}

async fn shutdown_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    let mut term = signal(SignalKind::terminate()).expect("SIGTERM");
    let mut int = signal(SignalKind::interrupt()).expect("SIGINT");
    tokio::select! {
        _ = term.recv() => {},
        _ = int.recv() => {},
    }
}
```

- [ ] **Step 4: Verify the whole workspace compiles**

Run: `cargo build`
Expected: builds the `rusty-minion` binary.

- [ ] **Step 5: Confirm unit tests still pass and clippy is clean**

Run: `cargo test && cargo clippy --all-targets -- -D warnings`
Expected: all unit tests pass; no clippy warnings.

- [ ] **Step 6: Commit**

```bash
git add crates/transport/src/twin.rs crates/transport/src/lib.rs \
        crates/minion/src/runtime.rs crates/minion/src/main.rs
git commit -m "feat(minion): twin gRPC subscriber + tokio runtime + graceful shutdown"
```

---

### Task B7: In-process integration test against a mock gateway

Prove the registration + twin vertical slice end to end without the full delta-v stack: stand up an in-process tonic server implementing `HeartbeatService` + `TwinChannelService` that (a) asserts the identity metadata is present, (b) acks heartbeats, (c) sends a snapshot then a patch on the Twin stream. Drive the Rust client against it.

**Files:**
- Create: `crates/minion/tests/registration_e2e.rs`

- [ ] **Step 1: Write the failing integration test**

```rust
// crates/minion/tests/registration_e2e.rs
use opennms_ipc_contract::v1::heartbeat_service_server::{HeartbeatService, HeartbeatServiceServer};
use opennms_ipc_contract::v1::twin_channel_service_server::{TwinChannelService, TwinChannelServiceServer};
use opennms_ipc_contract::v1::{Heartbeat, HeartbeatAck, TwinSubscription, TwinUpdate};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status, Streaming};

#[derive(Default, Clone)]
struct Seen {
    minion_ids: Arc<Mutex<Vec<String>>>,
    locations: Arc<Mutex<Vec<String>>>,
    got_heartbeat: Arc<Mutex<bool>>,
}

fn require_identity<T>(req: &Request<T>) -> Result<(String, String), Status> {
    let md = req.metadata();
    let id = md.get("x-minion-id").and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Status::unauthenticated("x-minion-id required"))?;
    let loc = md.get("x-minion-location").and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Status::unauthenticated("x-minion-location required"))?;
    Ok((id.to_string(), loc.to_string()))
}

struct MockHeartbeat { seen: Seen }

#[tonic::async_trait]
impl HeartbeatService for MockHeartbeat {
    type PublishStream = Pin<Box<dyn Stream<Item = Result<HeartbeatAck, Status>> + Send>>;
    async fn publish(&self, req: Request<Streaming<Heartbeat>>)
        -> Result<Response<Self::PublishStream>, Status>
    {
        let (id, loc) = require_identity(&req)?;
        self.seen.minion_ids.lock().unwrap().push(id);
        self.seen.locations.lock().unwrap().push(loc);
        let mut inbound = req.into_inner();
        let seen = self.seen.clone();
        let (tx, rx) = mpsc::channel(8);
        tokio::spawn(async move {
            while let Some(Ok(hb)) = inbound.next().await {
                *seen.got_heartbeat.lock().unwrap() = true;
                let _ = tx.send(Ok(HeartbeatAck {
                    minion_id: hb.minion_id, received_at: None,
                })).await;
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

struct MockTwin;

#[tonic::async_trait]
impl TwinChannelService for MockTwin {
    type ChannelStream = Pin<Box<dyn Stream<Item = Result<TwinUpdate, Status>> + Send>>;
    async fn channel(&self, req: Request<Streaming<TwinSubscription>>)
        -> Result<Response<Self::ChannelStream>, Status>
    {
        require_identity(&req)?;
        let mut subs = req.into_inner();
        let (tx, rx) = mpsc::channel(8);
        tokio::spawn(async move {
            // On first SUBSCRIBE: snapshot v1, then patch v2.
            if let Some(Ok(sub)) = subs.next().await {
                let _ = tx.send(Ok(TwinUpdate {
                    consumer_key: sub.consumer_key.clone(),
                    twin_object: br#"{"users":[]}"#.to_vec(),
                    is_patch: false, version: 1, session_id: "s1".into(),
                    location: "Default".into(), dispatched_at: None,
                })).await;
                let _ = tx.send(Ok(TwinUpdate {
                    consumer_key: sub.consumer_key,
                    twin_object: br#"[{"op":"add","path":"/users/-","value":"alice"}]"#.to_vec(),
                    is_patch: true, version: 2, session_id: "s1".into(),
                    location: "Default".into(), dispatched_at: None,
                })).await;
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

#[tokio::test]
async fn minion_registers_and_receives_twin() {
    use minion_transport::grpc::GatewayEndpoint;
    use minion_transport::heartbeat::GrpcHeartbeatProducer;
    use minion_transport::identity::MinionIdentityInterceptor;
    use minion_transport::twin::GrpcTwinSubscriber;
    use minion_transport::{SinkProducer, TwinSubscriber};
    use std::time::Duration;

    let seen = Seen::default();
    let addr = "127.0.0.1:0".parse().unwrap();
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    let local = listener.local_addr().unwrap();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    let server = tonic::transport::Server::builder()
        .add_service(HeartbeatServiceServer::new(MockHeartbeat { seen: seen.clone() }))
        .add_service(TwinChannelServiceServer::new(MockTwin))
        .serve_with_incoming(incoming);
    let server = tokio::spawn(server);

    let endpoint = GatewayEndpoint { host: local.ip().to_string(), port: local.port(), tls: false };
    let interceptor = MinionIdentityInterceptor { minion_id: "m-1".into(), location: "Default".into() };

    let (sd_tx, sd_rx) = tokio::sync::watch::channel(false);

    let hb = GrpcHeartbeatProducer {
        endpoint: endpoint.clone(), interceptor: interceptor.clone(),
        version: "test".into(), period: Duration::from_millis(50),
    };
    let hb_rx = sd_rx.clone();
    let hb_task = tokio::spawn(async move { hb.run_heartbeat(hb_rx).await });

    let tw = GrpcTwinSubscriber { endpoint, interceptor };
    let (utx, mut urx) = mpsc::channel(8);
    let tw_rx = sd_rx.clone();
    let tw_task = tokio::spawn(async move { tw.run(vec!["snmpv3-user".into()], utx, tw_rx).await });

    // Assert: a heartbeat was received with identity, and two twin updates arrived.
    let snap = tokio::time::timeout(Duration::from_secs(5), urx.recv()).await.unwrap().unwrap();
    assert!(!snap.is_patch);
    assert_eq!(snap.version, 1);
    let patch = tokio::time::timeout(Duration::from_secs(5), urx.recv()).await.unwrap().unwrap();
    assert!(patch.is_patch);
    assert_eq!(patch.version, 2);

    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(*seen.got_heartbeat.lock().unwrap(), "gateway saw a heartbeat");
    assert_eq!(seen.minion_ids.lock().unwrap().as_slice(), &["m-1"]);
    assert_eq!(seen.locations.lock().unwrap().as_slice(), &["Default"]);

    let _ = sd_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(2), hb_task).await;
    let _ = tokio::time::timeout(Duration::from_secs(2), tw_task).await;
    server.abort();
}
```

- [ ] **Step 2: Add the test-only deps**

Add to `crates/minion/Cargo.toml`:

```toml
[dev-dependencies]
tokio-stream = { version = "0.1", features = ["net"] }
opennms-ipc-contract = { workspace = true }
tonic = { workspace = true }
```

- [ ] **Step 3: Run the integration test**

Run: `cargo test -p minion --test registration_e2e`
Expected: PASS — heartbeat received with identity headers, snapshot (v1) + patch (v2) received. This is the Phase 0 milestone proven in-process.

- [ ] **Step 4: Commit**

```bash
git add crates/minion/tests/registration_e2e.rs crates/minion/Cargo.toml
git commit -m "test: in-process e2e — minion registers + receives twin against mock gateway"
```

---

### Task B8: Live smoke against the real delta-v gateway

Prove registration against the actual `minion-gateway` + Core, so "Rusty Minion is visible to Core" is real, not mocked. Uses the delta-v docker compose stack.

**Files:**
- Create: `scripts/smoke-register.sh`

- [ ] **Step 1: Write the smoke script**

```bash
#!/usr/bin/env bash
# scripts/smoke-register.sh — bring up the delta-v stack, run rusty-minion
# against the real gateway, and assert Core registers the minion.
set -euo pipefail
DELTAV="${DELTAV:?set DELTAV to the delta-v checkout}"

# 1. Bring up a lean stack with the gateway + Core registration path.
( cd "$DELTAV" && make up PROFILE=active )

# 2. Build and run rusty-minion pointed at the gateway (direct h2c on 9090).
cargo build --release
MINION_ID="mpp-smoke-01" \
MINION_LOCATION="Default" \
MINION_GATEWAY_HOST="localhost" \
MINION_GATEWAY_PORT="9090" \
RUST_LOG=info \
./target/release/rusty-minion &
MPP_PID=$!
trap 'kill $MPP_PID 2>/dev/null || true' EXIT

# 3. Poll Core's REST minion list for our id (heartbeat persists OnmsMinion).
for i in $(seq 1 24); do
  if curl -fsS -u admin:admin "http://localhost:8980/opennms/rest/minions" \
       | grep -q "mpp-smoke-01"; then
    echo "PASS: rusty-minion registered with Core"
    exit 0
  fi
  sleep 5
done
echo "FAIL: rusty-minion did not register within timeout" >&2
exit 1
```

- [ ] **Step 2: Note the bootstrap config for the example**

The example `minion.toml` for a plaintext direct-to-gateway run (set `tls = false`, `port = 9090`); the script overrides via env to avoid editing the file. The default `minion.example.toml` keeps `tls = true`/`8443` for the Envoy frontend path.

- [ ] **Step 3: Run the smoke (manually / in CI with docker)**

Run: `DELTAV=/Users/david/development/src/opennms/delta-v bash scripts/smoke-register.sh`
Expected: `PASS: rusty-minion registered with Core`.

Note (from memory): Mac dev boots are slow (provisiond ~10 min); the 24×5s = 120 s poll targets heartbeat registration only (no provisiond dependency), which is fast. If the gateway is reached but Core does not list the minion, check `make logs SVC=minion-gateway` for the `DeltaV.Sink.Heartbeat` publish and the Core `HeartbeatConsumer` saveOrUpdate.

- [ ] **Step 4: Commit**

```bash
chmod +x scripts/smoke-register.sh
git add scripts/smoke-register.sh
git commit -m "test: live smoke — rusty-minion registers against real delta-v gateway"
```

---

# Repo C — delta-v tree repoint

### Task C1: Point `core/minion-grpc-contracts` at the published contract artifact

Make the standalone contract the single source of truth: the delta-v module consumes the published Java artifact instead of carrying local protos.

**Files:**
- Modify: `core/minion-grpc-contracts/pom.xml`
- Delete: `core/minion-grpc-contracts/src/main/proto/*` (now sourced from the contract repo)

- [ ] **Step 1: Branch from develop (per project rules)**

```bash
cd "$DELTAV"
git fetch origin && git checkout develop && git pull
git checkout -b feat/ipc-contract-repoint
```

- [ ] **Step 2: Replace local proto generation with a dependency on the published artifact**

Edit `core/minion-grpc-contracts/pom.xml`: remove the `protobuf-maven-plugin` build section and the local `src/main/proto`, and add a dependency on the published contract:

```xml
<dependency>
  <groupId>org.deltav</groupId>
  <artifactId>ipc-contract-java</artifactId>
  <version>0.1.0</version>
</dependency>
```

Re-export it transitively so existing consumers (`minion-gateway`, `daemon-boot-minion-common`) keep resolving `org.deltav.minion.grpc.v1.*` unchanged (same package, same generated classes).

- [ ] **Step 3: Delete the now-relocated protos**

```bash
git rm core/minion-grpc-contracts/src/main/proto/*.proto
```

- [ ] **Step 4: Verify the dependent delta-v modules still compile**

Run: `./mvnw -q -pl :org.opennms.core.minion-grpc-contracts,:org.opennms.core.minion-gateway,:org.opennms.core.daemon-boot-minion-common -am -DskipTests install`
Expected: BUILD SUCCESS. The generated classes come from the published artifact; package names are identical, so `minion-gateway` and `daemon-boot-minion-common` need no source changes.

Note (from memory): clean stale `.m2` SNAPSHOTs when switching branches; the published contract is a release version (`0.1.0`), not a SNAPSHOT, so it caches cleanly.

- [ ] **Step 5: Commit and open a PR against the fork**

```bash
git add core/minion-grpc-contracts/pom.xml
git commit -m "refactor: source minion gRPC contract from standalone opennms-ipc-contract"
gh pr create --repo pbrane/delta-v --base develop \
  --title "refactor: source minion gRPC contract from standalone repo" \
  --body "Repoints core/minion-grpc-contracts at the published org.deltav:ipc-contract-java; protos now live in the standalone opennms-ipc-contract repo (single source of truth). No package/classname changes; consumers unaffected."
```

---

## Self-Review

**Spec coverage (against `2026-06-13-rusty-minion-rust-design.md` §8 Phase 0):**
- "Contract repo stood up" → A1–A6 (relocated protos, buf, dual codegen). ✓
- "Java-side protobuf cutover for the payloads in scope" → registration path is already typed protobuf via the gateway's `Heartbeat` message; the only Java change Phase 0 needs is sourcing the contract from the standalone repo (C1). Typed `PollerRequest`/`CollectorResponse` cutover is explicitly deferred to Phases 2–3 where they gain consumers. ✓ (scoping noted)
- "transport skeleton (Kafka + gRPC)" → gRPC built (B3–B6); Kafka explicitly out of Phase 0 per the planning decision (Transport trait seam preserved). ✓ (deviation from spec recorded in plan header)
- "identity + heartbeat" → B3 (identity interceptor), B4/B6 (heartbeat). ✓
- "Twin subscriber" → B5 (state machine), B6 (gRPC wiring). ✓
- "Milestone: registers, visible to Core, receives Twin config" → B7 (mock) + B8 (live against Core). ✓
- Risk: "CollectionSet protobuf modeling … prototype and round-trip against the Java collector in Phase 0" → A3 (model) + A8 (round-trip vs real Java collector output). ✓
- Risk: "Contract drift … single shared repo + buf breaking-change CI + Tier-1 golden corpus" → A4 (buf CI) + A7 (golden corpus, both languages). ✓

**Deferred to later phases (intentional, not gaps):** net-snmp FFI (Phase 2), monitors/collectors/listeners (Phases 1–3), `cargo-fuzz` targets for hostile-input parsers (land with the parsers in Phases 1–2), typed `PollerRequest/Response` & `CollectorRequest/Response` payloads (Phases 2–3), `wasm32`/Lite feature gating (post-v1).

**Placeholder scan:** no TBD/TODO; every code step carries complete code; every run step has an exact command + expected result.

**Type consistency:** `MinionConfig`/`GatewaySection`/`MinionIdentitySection` (B2) are the same types used in B3/B6; `TwinStore`/`TwinUpdateIn`/`Outcome` (B5) match their use in B6; `GatewayEndpoint`/`MinionIdentityInterceptor`/`GrpcHeartbeatProducer`/`GrpcTwinSubscriber` are defined once (B3/B4/B6) and reused in B7; generated types (`Heartbeat`, `TwinUpdate`, `TwinSubscription`, `CollectionSet`, `Attribute`/`attribute::Value`, the `*_client`/`*_server` modules) come from the contract crate built in A6 and are referenced consistently.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-06-13-rusty-minion-phase0-foundation.md`. Two execution options:

1. **Subagent-Driven (recommended)** — fresh subagent per task, two-stage review between tasks, fast iteration. Best fit here because the two repos are new and each task is independently verifiable.
2. **Inline Execution** — execute tasks in this session via executing-plans, batch execution with checkpoints.

Note: the contract repo (Repo A) and the Rust repo (Repo B) are separate git repos created outside the delta-v tree; only Task C1 touches the delta-v checkout and follows the fork-PR rules. Confirm where the two new repos should be created (e.g. `~/development/src/opennms/opennms-ipc-contract` and `.../rusty-minion`, remotes under `pbrane/`) before starting Repo A.
