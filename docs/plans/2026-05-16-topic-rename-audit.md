# Delta-V v1.2.0 GA — Topic-Rename Audit

**Date:** 2026-05-16
**Branch:** `docs/topic-rename-audit`
**Status:** Complete — all four findings below are definitive.
**Purpose:** Input to Tasks 2 and 3 of `docs/plans/2026-05-16-v1.2.0-ga-finalization-implementation.md`.

---

## Summary of findings

| Question | Answer |
|---|---|
| RPC/Twin prefix verdict | **(A) Config-overridable** — delta-v-only PR B; no horizon companion needed |
| Event `.cfg` files | **Vestigial** — no Spring Boot code path reads them; PR A deletes them |
| Sink topics on the wire | **`DeltaV.Sink.*` scheme** — `DeltaV.Sink.Heartbeat`, `DeltaV.Sink.Trap`, `DeltaV.Sink.Syslog`, `DeltaV.Sink.Telemetry-*`; already de-legacied, out of scope for this rename |

---

## Step 2: Event-topic reference inventory

### Classification

**a) Live production config — MUST rename in PR A**

These are the code paths that produce actual Kafka topic strings at runtime:

| File | Property / default |
|---|---|
| `core/daemon-common/src/main/java/org/deltav/core/daemon/common/KafkaEventTransportConfiguration.java:55` | `@Value("${opennms.kafka.event-topic:opennms-fault-events}")` |
| `core/daemon-common/src/main/java/org/deltav/core/daemon/common/KafkaEventTransportConfiguration.java:58` | `@Value("${opennms.kafka.ipc-topic:opennms-ipc-events}")` |
| `core/daemon-boot-pollerd/src/main/resources/application.yml:54` | `event-topic: ${KAFKA_EVENT_TOPIC:opennms-fault-events}` |
| `core/daemon-boot-pollerd/src/main/resources/application.yml:55` | `ipc-topic: ${KAFKA_IPC_TOPIC:opennms-ipc-events}` |
| `core/daemon-boot-perspectivepollerd/src/main/resources/application.yml:56` | `event-topic: ${KAFKA_EVENT_TOPIC:opennms-fault-events}` |
| `core/daemon-boot-perspectivepollerd/src/main/resources/application.yml:57` | `ipc-topic: ${KAFKA_IPC_TOPIC:opennms-ipc-events}` |
| `core/daemon-boot-trapd/src/main/resources/application.yml:31` | `event-topic: ${KAFKA_EVENT_TOPIC:opennms-fault-events}` |
| `core/daemon-boot-syslogd/src/main/resources/application.yml:31` | `event-topic: ${KAFKA_EVENT_TOPIC:opennms-fault-events}` |
| `core/daemon-boot-discovery/src/main/resources/application.yml:31` | `event-topic: ${KAFKA_EVENT_TOPIC:opennms-fault-events}` |
| `core/daemon-boot-provisiond/src/main/resources/application.yml:71` | `event-topic: ${KAFKA_EVENT_TOPIC:opennms-fault-events}` |
| `core/daemon-boot-collectd/src/main/resources/application.yml:59` | `event-topic: ${KAFKA_EVENT_TOPIC:opennms-fault-events}` |
| `core/daemon-boot-collectd/src/main/resources/application.yml:60` | `ipc-topic: ${KAFKA_IPC_TOPIC:opennms-ipc-events}` |
| `core/daemon-boot-enlinkd/src/main/resources/application.yml:25` | `event-topic: ${KAFKA_EVENT_TOPIC:opennms-fault-events}` |
| `core/daemon-boot-enlinkd/src/main/resources/application.yml:26` | `ipc-topic: ${KAFKA_IPC_TOPIC:opennms-ipc-events}` |
| `core/daemon-boot-telemetryd/src/main/resources/application.yml:26` | `event-topic: ${KAFKA_EVENT_TOPIC:opennms-fault-events}` |
| `core/daemon-boot-telemetryd/src/main/resources/application.yml:27` | `ipc-topic: ${KAFKA_IPC_TOPIC:opennms-ipc-events}` |
| `core/daemon-boot-eventtranslator/src/main/resources/application.yml:35` | `event-topic: ${KAFKA_EVENT_TOPIC:opennms-fault-events}` |
| `core/daemon-boot-alarmd/src/main/resources/application.yml:41` | `event-topic: ${KAFKA_EVENT_TOPIC:opennms-fault-events}` |
| `core/daemon-boot-bsmd/src/main/resources/application.yml:41` | `event-topic: ${KAFKA_EVENT_TOPIC:opennms-fault-events}` |

No daemon sets `KAFKA_EVENT_TOPIC` or `KAFKA_IPC_TOPIC` in `docker-compose.yml` — the defaults in `application.yml` are what runs in production. PR A changes the defaults and the Docker Compose env var names to `deltav-fault-events` / `deltav-ipc-events`.

**b) Vestigial Karaf `.cfg` files — DELETE in PR A**

These files live under `opennms-container/delta-v/*-overlay/etc/org.opennms.core.event.forwarder.kafka.cfg` and are present for eight daemon overlays: `bsmd-overlay`, `collectd-overlay`, `discovery-overlay`, `enlinkd-overlay`, `pollerd-daemon-overlay`, `provisiond-overlay`, `syslogd-overlay`, `trapd-overlay`. They contain `topic.name=opennms-fault-events` and `ipc.topic.name=opennms-ipc-events`.

Their fate is addressed in Step 3 below: **vestigial — delete in PR A**.

Note: `Dockerfile.daemon-per` (line 33) does `COPY --chown=opennms:opennms ${DAEMON_NAME}-overlay/etc /opt/deltav/etc`, so these files are currently baked into the per-daemon images. However, they are not read by any Spring Boot code path (Step 3 confirms this). Deleting them removes dead files from the image.

**c) Unit-test constants — rename in PR A**

| File | Constant |
|---|---|
| `core/event-forwarder-kafka/src/test/java/org/deltav/core/event/forwarder/kafka/KafkaEventSubscriptionServiceTest.java:54` | `TOPIC = "opennms-fault-events"` |
| `core/event-forwarder-kafka/src/test/java/org/deltav/core/event/forwarder/kafka/KafkaEventForwarderTest.java:47` | `FAULT_TOPIC = "opennms-fault-events"` |
| `core/event-forwarder-kafka/src/test/java/org/deltav/core/event/forwarder/kafka/KafkaEventForwarderTest.java:48` | `IPC_TOPIC = "opennms-ipc-events"` |

**d) E2E test scripts — rename in PR A**

| File | Line |
|---|---|
| `opennms-container/delta-v/test-passive-e2e.sh` | 361, 367 (`--topic opennms-fault-events`, `--topic opennms-ipc-events`) |
| `opennms-container/delta-v/test-e2e.sh` | 166, 172 |
| `opennms-container/delta-v/test-minion-e2e.sh` | 187, 193 |
| `opennms-container/delta-v/test-minion-rpc-e2e.sh` | 138 |
| `opennms-container/delta-v/test-perspective-e2e.sh` | 186 |
| `opennms-container/delta-v/test-syslog-e2e.sh` | 264, 270 |

**e) Docs — update in PR A**

- `README.md` lines 114, 122, 159, 160
- `GEMINI.md` line 71
- `opennms-container/delta-v/README.md` lines 10, 204

Historical plan docs under `docs/plans/` and `docs/superpowers/` contain `opennms-fault-events` as fixed historical text (dozens of hits). These do NOT need updating — they record what was true at the time they were written.

---

## Step 3: `.cfg` files are vestigial

**Search executed:**
```bash
rg -rn 'forwarder.kafka.cfg|\.cfg' core/daemon-common/src/main core/daemon-boot-*/src/main
```
**Result:** no output — zero hits.

**Conclusion: VESTIGIAL.** No Spring Boot daemon reads `org.opennms.core.event.forwarder.kafka.cfg` or any `.cfg` PID file. The `entrypoint.sh` launches daemons as:
```sh
exec java $JAVA_OPTS \
    -cp "/opt/libs/priority/*:/opt/libs/external/*:/opt/libs/internal/*:/opt/libs/daemon/*:/opt/app/*" \
    "$MAIN_CLASS"
```
There is no Karaf OSGi Config Admin, no felix-fileinstall, no PID loader. Spring Boot reads `application.yml` and environment variables only.

The `.cfg` files were overlay artifacts from the Karaf era (removed in PRs #66/#89/#90 and following). **PR A deletes all eight `org.opennms.core.event.forwarder.kafka.cfg` files** from the daemon overlays.

---

## Step 4: RPC/Twin daemon-side topic-prefix mechanism

### Verdict: **(A) Config-overridable — delta-v-only PR B**

**Evidence chain:**

**1. Horizon `KafkaTopicProvider` constructs the topic string**
Source: `~/.m2/repository/org/opennms/core/ipc/common/org.opennms.core.ipc.common.kafka/1.0.9/...-sources.jar`
`org/opennms/core/ipc/common/kafka/KafkaTopicProvider.java`:
```java
public String getRequestTopicAtLocation(String location, String module) {
    if (singleTopic) {
        return String.format(TOPIC_NAME_AT_LOCATION,
            SystemInfoUtils.getInstanceId(), location, RPC_REQUEST_TOPIC_NAME);
        // → "<instanceId>.<location>.rpc-request"
    }
    ...
}
```
`RPC_REQUEST_TOPIC_NAME = "rpc-request"` from `KafkaRpcConstants`.

**2. `SystemInfoUtils.getInstanceId()` reads a JVM system property**
Decompiled from `~/.m2/repository/org/opennms/core/org.opennms.core.lib/1.0.17/org.opennms.core.lib-1.0.17.jar`:
```
static {
    // instruction 8–15:
    ldc "org.opennms.instance.id"
    ldc "OpenNMS"                // default
    invokestatic System.getProperty(String, String)
    putstatic s_instanceId
}
```
The property name is `org.opennms.instance.id` with default `"OpenNMS"`.

**3. Delta-v bridges `OPENNMS_INSTANCE_ID` → `org.opennms.instance.id` before static init**
`core/daemon-common/src/main/java/org/deltav/core/daemon/common/SystemPropertyBridgePostProcessor.java`:
```java
public class SystemPropertyBridgePostProcessor implements EnvironmentPostProcessor {
    @Override
    public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
        bridge(environment, "opennms.instance.id", "org.opennms.instance.id", "OpenNMS");
        ...
    }
}
```
This runs before any `@Configuration` bean is created, so `SystemInfoUtils`'s static initializer sees the correct value.

**4. `opennms.instance.id` binds to `OPENNMS_INSTANCE_ID` env var**
`core/daemon-boot-pollerd/src/main/resources/application.yml:49`:
```yaml
opennms:
  instance:
    id: ${OPENNMS_INSTANCE_ID:OpenNMS}
```
Same pattern present in every daemon's `application.yml` that does RPC.

**5. docker-compose currently sets `OPENNMS_INSTANCE_ID: OpenNMS` in 10 daemon service blocks**

**6. Minion-gateway side: hardcoded strings, but in delta-v source**
`core/minion-gateway/src/main/java/org/deltav/gateway/rpc/RpcChannelDispatcher.java`:
```java
// line 74
private static final Pattern RPC_REQUEST_TOPIC = Pattern.compile("OpenNMS\\.(.*)\\.rpc-request");

// line 77
@KafkaListener(topicPattern = "OpenNMS\\..*\\.rpc-request", ...)
```
`core/minion-gateway/src/main/java/org/deltav/gateway/rpc/RpcResponsePublisher.java`:
```java
// constructor, line 43
@Value("${minion-gateway.rpc.response-topic:OpenNMS.rpc-response}") String responseTopic
```
`core/minion-gateway/src/main/java/org/deltav/gateway/twin/TwinChannelDispatcher.java`:
```java
// line 71
@KafkaListener(topicPattern = "OpenNMS\\.twin\\.response\\..*", ...)
```
These are delta-v source files (org.deltav.gateway package, BeaconStrategists copyright). No horizon code is involved. A single delta-v PR changes them.

### What PR B must change (delta-v only)

| Location | Change |
|---|---|
| `docker-compose.yml` — all 10 `OPENNMS_INSTANCE_ID: OpenNMS` entries | → `OPENNMS_INSTANCE_ID: DeltaV` |
| `RpcChannelDispatcher.java` line 74 — Pattern | `"OpenNMS\\.(.*)\\.rpc-request"` → `"DeltaV\\.(.*)\\.rpc-request"` |
| `RpcChannelDispatcher.java` line 77 — `@KafkaListener` topicPattern | `"OpenNMS\\..*\\.rpc-request"` → `"DeltaV\\..*\\.rpc-request"` |
| `RpcResponsePublisher.java` line 43 — `@Value` default | `OpenNMS.rpc-response` → `DeltaV.rpc-response` |
| `TwinChannelDispatcher.java` line 71 — `@KafkaListener` topicPattern | `"OpenNMS\\.twin\\.response\\..*"` → `"DeltaV\\.twin\\.response\\..*"` |
| `application.yml` defaults for the twin request topic | Confirm via `KafkaTwinConstants`; twin request topic is `<instanceId>.twin.request` — covered by the `OPENNMS_INSTANCE_ID` change above |
| E2E test scripts referencing `OpenNMS.*` topics | Update regex/topic assertions to `DeltaV.*` |
| `README.md`, `GEMINI.md`, container README | Update RPC/Twin topic examples |

No changes to horizon JARs or `pbrane/delta-v-horizon` are needed.

### Twin topic full picture

From `org.opennms.core.ipc.twin.kafka.common.Topic` (same sources JAR):
```java
public static String request() {
    return String.format("%s.%s.%s", SystemInfoUtils.getInstanceId(), "twin", "request");
    // → "OpenNMS.twin.request"
}
public static String responseForLocation(String location) {
    return String.format("%s.%s.%s.%s", SystemInfoUtils.getInstanceId(), "twin", "response", location);
    // → "OpenNMS.twin.response.<location>"
}
```
`KafkaTwinPublisher` (daemon side, horizon JAR) uses `Topic.request()` and `Topic.responseForLocation()`. Both are driven by `SystemInfoUtils.getInstanceId()`, which PR B fixes by setting `OPENNMS_INSTANCE_ID=DeltaV`.

---

## Step 5: Sink-topic truth

**Search executed:**
```bash
rg -n 'DeltaV\.Sink' core/minion-gateway/src/main core/daemon-sink-kafka/src/main
```

> **Plan-command pitfall — corrected here.** The implementation plan's Step 5
> command is `rg -rn 'DeltaV\.Sink|"n\.' ...`. Ripgrep parses `-rn` as
> `--replace=n`, so it **rewrites** every `DeltaV.Sink` match to `n` in the
> printed output. Running the plan's command verbatim makes the wire scheme
> look like `n.*` when it is not. Use `-n` alone (line numbers) and read the
> string literals directly from source.

**Result: the Sink scheme on the wire is `DeltaV.Sink.*`.**

| Topic | Source constant | Location |
|---|---|---|
| `DeltaV.Sink.Heartbeat` | `HeartbeatTranslator.java:36` `TOPIC = "DeltaV.Sink.Heartbeat"` | `core/minion-gateway/src/main/java/org/deltav/gateway/kafka/HeartbeatTranslator.java` |
| `DeltaV.Sink.Trap` | `TrapGrpcService.java` `TOPIC = "DeltaV.Sink.Trap"` | `core/minion-gateway/src/main/java/org/deltav/gateway/sink/TrapGrpcService.java` |
| `DeltaV.Sink.Syslog` | `SyslogGrpcService.java` `TOPIC = "DeltaV.Sink.Syslog"` | `core/minion-gateway/src/main/java/org/deltav/gateway/sink/SyslogGrpcService.java` |
| `DeltaV.Sink.Telemetry-IPFIX` | `TelemetryGrpcService.java` `IPFIX_TOPIC` | `core/minion-gateway/src/main/java/org/deltav/gateway/sink/TelemetryGrpcService.java` |
| `DeltaV.Sink.Telemetry-Netflow-5` | `TelemetryGrpcService.java` `NETFLOW5_TOPIC` | same |
| `DeltaV.Sink.Telemetry-Netflow-9` | `TelemetryGrpcService.java` `NETFLOW9_TOPIC` | same |
| `DeltaV.Sink.Telemetry-SFlow` | `TelemetryGrpcService.java` `SFLOW_TOPIC` | same |
| `DeltaV.Sink.<moduleId>` (generic) | `KafkaSinkBridge.java` `final String topic = "DeltaV.Sink." + module.getId()` | `core/daemon-sink-kafka/src/main/java/org/deltav/core/daemon/sink/kafka/KafkaSinkBridge.java` |

The rc3.1 smoke observation of `DeltaV.Sink.*` topics was **correct**. The gRPC
migration (PR3) already produces Sink topics under the `DeltaV.Sink.*` prefix —
there is no legacy `OpenNMS.Sink.*` and no terse `n.*` scheme on the wire.
**Sink topics are already de-legacied and are out of scope for the v1.2.0 GA
rename** — listed here only to confirm the rename does not need to touch them.

---

## Downstream action summary for implementers

**PR A (event-topic rename — pure delta-v config, no build required):**
1. Rename `@Value` defaults in `KafkaEventTransportConfiguration.java`: `opennms-fault-events` → `deltav-fault-events`, `opennms-ipc-events` → `deltav-ipc-events`.
2. Rename defaults in all 12 `core/daemon-boot-*/src/main/resources/application.yml` files.
3. Delete all 8 `opennms-container/delta-v/*-overlay/etc/org.opennms.core.event.forwarder.kafka.cfg` files.
4. Rename test constants in `core/event-forwarder-kafka/src/test/...`.
5. Update 6 E2E test scripts (`test-passive-e2e.sh`, `test-e2e.sh`, `test-minion-e2e.sh`, `test-minion-rpc-e2e.sh`, `test-perspective-e2e.sh`, `test-syslog-e2e.sh`).
6. Update `README.md`, `GEMINI.md`, `opennms-container/delta-v/README.md`.

**PR B (RPC + Twin rename — delta-v only, no horizon PR needed):**
1. Change `OPENNMS_INSTANCE_ID: OpenNMS` → `OPENNMS_INSTANCE_ID: DeltaV` in all 10 daemon service blocks in `docker-compose.yml`. This automatically renames the daemon-side RPC request topic, RPC response topic, and all Twin topics because they all flow through `SystemInfoUtils.getInstanceId()` → `SystemPropertyBridgePostProcessor` → `opennms.instance.id` Spring property → `OPENNMS_INSTANCE_ID` env var.
2. Update `RpcChannelDispatcher.java`: Pattern and `@KafkaListener` topicPattern `OpenNMS\..*` → `DeltaV\..*`.
3. Update `RpcResponsePublisher.java` `@Value` default: `OpenNMS.rpc-response` → `DeltaV.rpc-response`.
4. Update `TwinChannelDispatcher.java` `@KafkaListener` topicPattern `OpenNMS\.twin\.response\..*` → `DeltaV\.twin\.response\..*`.
5. Update E2E tests that assert RPC/Twin topic names.
6. Update docs (README, design docs).
