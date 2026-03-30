# Next Session: Fix Minion Trap Listener Port Binding

> Copy everything below the line into the next Claude Code conversation.

---

## Context

Branch `feature/minion-boot4-clean`. Entity porting done (nodeScanCompleted PASSES). Minion @ComponentScan fixed. TrapListener lifecycle fixed to bypass Twin. But the trap port (UDP 1162) still isn't opening.

## What's Been Fixed

1. **@ComponentScan override** — MinionApplication had `@ComponentScan(basePackages="org.opennms.core.daemon.common")` which overrode `scanBasePackages` from `@SpringBootApplication`, silently dropping `org.opennms.minion.boot` and `org.opennms.minion.common`. Fixed by including all 3 packages.

2. **Twin dependency** — TrapListenerConfiguration called `trapListener.bind(twinSubscriber)` which waited for TrapListenerConfig from core via Twin. Since daemon-boot-trapd doesn't publish Twin, the port never opened. Fixed to call `unbind(null)` then `start()` for the 5-second fallback timer.

## Current Problem

After both fixes, the Minion starts all beans correctly (KafkaRemoteMessageDispatcherFactory initializes, RPC server starts, heartbeats send) BUT no "Listening on" message appears for the trap port. The TrapListener's `start()` method schedules a 5-second Timer that should call `open(new TrapListenerConfig())`, which calls `SnmpUtils.registerForTraps(this, address, port, snmpV3Users)`.

### Debugging Hypotheses

1. **SNMP strategy not initialized** — `SnmpUtils.registerForTraps()` needs the Snmp4j strategy. Check if `SnmpUtils.getStrategy()` returns null.

2. **TrapListener.start() exception swallowed** — The Timer task might throw an exception that's silently caught. The `open()` method has a try-catch around `registerForTraps()`.

3. **MDC logging suppression** — TrapListener uses `Logging.withPrefixCloseable(Trapd.LOG4J_CATEGORY)` which sets MDC prefix. Spring Boot logback may not output these. Fix: configure logback-spring.xml or add explicit logger for `org.opennms.netmgt.trapd`.

4. **Spring @Autowired re-injection** — Even though lifecycle calls `unbind(null)`, Spring might re-inject `m_twinSubscriber` after the lifecycle runs. This would make `start()` take the Twin subscriber path instead of the Timer fallback. Fix: don't use `@Autowired` field injection — use constructor injection.

### Quick Test

Add debug logging to `TrapListenerConfiguration`:
```java
@Override
public void start() {
    LOG.info("TrapListenerLifecycle: starting trap listener");
    trapListener.unbind(null);
    trapListener.start();
    LOG.info("TrapListenerLifecycle: start() returned");
    running = true;
}
```

### Key Files

- `core/daemon-boot-minion/src/main/java/org/opennms/minion/boot/TrapListenerConfiguration.java`
- `core/daemon-boot-minion/src/main/java/org/opennms/minion/boot/MinionApplication.java`
- `features/events/traps/src/main/java/org/opennms/netmgt/trapd/TrapListener.java` (lines 122-175)
- `core/snmp/api/src/main/java/org/opennms/netmgt/snmp/SnmpUtils.java` (`registerForTraps`)

## E2E Status

| Test | Result |
|------|--------|
| nodeScanCompleted | **PASS** |
| coldStart trap | PASS |
| linkDown/linkUp traps | FAIL (Minion doesn't forward to Kafka) |
| Alarms | FAIL (no events = no alarms) |
