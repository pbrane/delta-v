# Next Session: Fix Alarm Pipeline (Trapd → EventTranslator → Alarmd)

> Copy everything below the line into the next Claude Code conversation.

---

## Context

Branch `feature/minion-boot4-clean` now has all 12 missing entities ported to `model-jakarta` and `opennms-model` excluded (but re-added as runtime scope) from all 12 daemon-boot modules. The `nodeScanCompleted` E2E test PASSES — provisiond can scan nodes with Hibernate 7.

## Current E2E Results (2026-03-29)

| Test | Result | Notes |
|------|--------|-------|
| Docker image build | PASS | All 15+ images built |
| All services healthy | PASS | 7 passive daemons + minion healthy <5s |
| coldStart trap to Minion | PASS | |
| nodeScanCompleted | **PASS** | Previously FAIL — fixed by entity porting |
| linkDown trap sent | PASS | |
| Translated SNMP_Link_Down event | FAIL | Not seen in Kafka within 30s |
| linkDown alarm in PostgreSQL | FAIL | No alarm created |
| linkUp trap sent | PASS | |
| Translated SNMP_Link_Up event | FAIL | Not seen in Kafka within 30s |
| linkDown alarm cleared | FAIL | No alarm to clear |

## The Problem

The trap → event translation → alarm pipeline is not working. Traps are received by Minion and forwarded, but translated events (SNMP_Link_Down / SNMP_Link_Up) don't appear in Kafka, and no alarms are created.

## Debugging Steps

1. **Check EventTranslator logs:** `docker logs delta-v-eventtranslator`
   - Is it receiving raw linkDown/linkUp events?
   - Is EventTranslatorConfig loaded?

2. **Check Alarmd logs:** `docker logs delta-v-alarmd`
   - Is it consuming events from Kafka?
   - Is alarm persistence working?

3. **Check Kafka topics:**
   ```bash
   docker exec delta-v-kafka-1 kafka-topics --bootstrap-server localhost:9092 --list
   docker exec delta-v-kafka-1 kafka-console-consumer --bootstrap-server localhost:9092 --topic events --from-beginning --timeout-ms 5000
   ```

4. **Check if EventTranslator configuration is loaded:**
   - EventTranslator needs `translator-configuration.xml`
   - Check if it exists in the daemon's config

5. **Check if trap events are making it to the events topic:**
   - Raw traps should appear in the Kafka events topic
   - Translated events should also appear

## Key Files

- `core/daemon-boot-eventtranslator/` — EventTranslator Spring Boot module
- `features/event-translator/` — Translation logic
- `opennms-container/delta-v/test-e2e.sh` — E2E test script
- `opennms-container/delta-v/etc/translator-configuration.xml` — EventTranslator config

## E2E Baseline History

- Pre-model-api: 4/5 pass (nodeScanCompleted timeout due to UnknownEntityException)
- Post-entity-porting: 7/13 pass (nodeScanCompleted fixed, alarm pipeline timing issues)
