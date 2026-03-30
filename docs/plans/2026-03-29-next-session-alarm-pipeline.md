# Next Session: Fix Alarm Pipeline (Minion Trap Listener → Trapd → Alarmd)

> Copy everything below the line into the next Claude Code conversation.

---

## Context

Branch `feature/minion-boot4-clean` now has all 12 missing entities ported to `model-jakarta` and `opennms-model` classpath-isolated from all 12 daemon-boot modules. The `nodeScanCompleted` E2E test PASSES — provisiond can scan nodes with Hibernate 7.

## Root Cause: Minion Has No Trap Listener

The Spring Boot Minion (`daemon-boot-minion`) does NOT have a Trapd Sink module. It has:
- KafkaEventSubscriptionService (event polling)
- Kafka RPC server (for SNMP proxy, ICMP proxy, DNS proxy)
- Kafka Sink client (for sending data to Horizon)

But it's missing:
- **TrapSinkModule** — UDP 1162 listener that receives SNMP traps and dispatches them to Kafka Sink topic `OpenNMS.Sink.Trap`

Without this, SNMP traps sent to Minion port 1162 are silently dropped. The coldStart trap in the E2E test works because it goes through a different path (Minion health check → newSuspect event via Kafka events, not via Trap sink).

## What Needs to Happen

### Option A: Add Trap Listener to Spring Boot Minion
Port the Karaf-based TrapSinkModule to the Spring Boot Minion:
1. Add dependency on `features/events/traps` (TrapSinkModule, TrapLogDTO)
2. Configure SnmpTrapAddress/SnmpTrapPort (default 1162)
3. Wire SnmpTrapListener → TrapSinkModule → KafkaSinkClient → `OpenNMS.Sink.Trap` topic
4. Trapd daemon (Spring Boot) picks up from `OpenNMS.Sink.Trap`, converts to events, forwards to `opennms-fault-events`

### Option B: Keep Using Legacy Minion for Traps
Use the legacy Karaf-based Minion (`minion-deltav` image) instead of `minion-boot` for E2E testing. The legacy Minion already has the Trapd Sink module working via OSGi.

## E2E Results (2026-03-29, post entity porting)

| Test | Result | Notes |
|------|--------|-------|
| Docker image build | PASS | All 15+ images built |
| All services healthy | PASS | 7 passive daemons + minion healthy |
| coldStart trap to Minion | PASS | Via health check path, not Trap Sink |
| nodeScanCompleted | **PASS** | Fixed by entity porting |
| linkDown trap sent | PASS | snmptrap command succeeds |
| Translated SNMP_Link_Down | FAIL | Minion doesn't forward traps to Kafka |
| linkDown alarm in DB | FAIL | No event = no alarm |
| linkUp trap sent | PASS | snmptrap command succeeds |
| Translated SNMP_Link_Up | FAIL | Same root cause |
| linkDown alarm cleared | FAIL | No alarm to clear |

## Key Evidence

1. `OpenNMS.Sink.Trap` Kafka topic has 0 messages after sending traps
2. Minion logs show NO activity when traps are sent to port 1162
3. Minion code loads 0 detector factories (`Loaded 0 detector factories via ServiceLoader`)
4. coldStart works because it goes through a different path (Minion health-check → newSuspect event)

## Key Files

- `core/daemon-boot-minion/` — Spring Boot Minion module
- `core/daemon-boot-minion-common/` — Shared Minion infrastructure (Kafka IPC)
- `features/events/traps/` — TrapSinkModule, TrapListener, TrapLogDTO
- `opennms-container/delta-v/test-e2e.sh` — E2E test script

## Also Track: Pre-clean Timing Issue

When running `test-e2e.sh --pre-clean`, the DB cleanup triggers reimport of all 3 requisitions (12 nodes). SNMP timeouts on non-SNMP containers (eventtranslator, syslogd, etc.) cause the scan to take >180s, exceeding the nodeScanCompleted timeout. Fix: either increase timeout or reduce number of nodes in requisitions.
