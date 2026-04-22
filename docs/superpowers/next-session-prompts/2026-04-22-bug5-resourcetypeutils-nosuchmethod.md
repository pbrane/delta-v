# Next session: fix Bug #5 — NoSuchMethodError on ResourceTypeUtils.getResourcePathWithRepository

**Background memory:** `project_phase0_inner_persister_bugs_followup` (now closed for Bug #1; this is the new downstream bug uncovered while verifying the 1.0.11 cascade-break).

**Branch:** `fix/resourcetypeutils-jakarta-parity` off develop.

---

## One-paragraph problem statement

With horizon 1.0.11 (delta-v #189) merged, Bug #2's NPE in `TimeseriesPersister.persistNumericAttribute` stopped firing — but execution now reaches line 182, which calls `ResourceTypeUtils.getResourcePathWithRepository(RrdRepository, ResourcePath)`. That method exists in horizon's `org.opennms.netmgt.model.ResourceTypeUtils` but is **absent** from delta-v's jakarta fork at the same FQCN (`core/opennms-model-jakarta/src/main/java/org/opennms/netmgt/model/ResourceTypeUtils.java`), so every SNMP attribute persist and every resource commit throws `NoSuchMethodError`. Bug #4's `TimeseriesPersistOperationBuilder.commit` → `getSamplesToInsert:141` hits the same missing method. The failures are caught by `FanoutPersister.runInner`, so the Kafka path is unaffected, but the `deltav.collectd.persister.inner.failures{step=visitAttribute|completeResource}` counters tick every poll cycle and the WARN logs spam.

## Evidence from labbox-equivalent local run (2026-04-22, horizon 1.0.11)

After 2 Collectd poll cycles:
```
deltav_collectd_persister_inner_failures_total{step="visitResource"}          0.0   ← Bug #1 DONE (closed)
deltav_collectd_persister_inner_failures_total{step="visitGroup"}             0.0   ← Bug #4 DONE
deltav_collectd_persister_inner_failures_total{step="persistNumericAttribute"} 0.0  ← Bug #2 DONE
deltav_collectd_persister_inner_failures_total{step="completeResource"}      70.0   ← THIS BUG
deltav_collectd_persister_inner_failures_total{step="visitAttribute"}       245.0   ← THIS BUG
```

Growth rate in steady state: `completeResource` ≈ +0.6/s, `visitAttribute` ≈ +2/s on a 3-node cloud-services + 20-device l8opensim-lab imported set.

## Stack traces

**visitAttribute path** (one per SNMP numeric attribute per cycle):
```
java.lang.NoSuchMethodError: 'org.opennms.netmgt.model.ResourcePath org.opennms.netmgt.model.ResourceTypeUtils.getResourcePathWithRepository(org.opennms.netmgt.rrd.RrdRepository, org.opennms.netmgt.model.ResourcePath)'
  at TimeseriesPersister.persistNumericAttribute(TimeseriesPersister.java:182) [features.timeseries-1.0.11]
  at NumericAttributeType.storeAttribute(NumericAttributeType.java:106)
  at SnmpAttribute.storeAttribute(SnmpAttribute.java:90)
  at AbstractPersister.storeAttribute(AbstractPersister.java:216)
  ... [via FanoutPersister.runInner, TimeseriesKafkaPublisherConfiguration$FanoutPersister]
```

**completeResource path** (one per resource per cycle):
```
java.lang.NoSuchMethodError: same method
  at TimeseriesPersistOperationBuilder.getSamplesToInsert(TimeseriesPersistOperationBuilder.java:141) [features.timeseries-1.0.11]
  at TimeseriesPersistOperationBuilder.commit(TimeseriesPersistOperationBuilder.java:134)
  at TimeseriesPersister.commitBuilder(TimeseriesPersister.java:160)
  at TimeseriesPersister.completeResource(TimeseriesPersister.java:149)
  ... [via FanoutPersister.runInner]
```

## Why this is NOT urgent

Delta-V's inner `TimeSeriesStorage` bean is `InMemoryStorage` by default (see `core/daemon-boot-collectd/src/main/java/org/deltav/netmgt/collectd/boot/CollectdDaemonConfiguration.java:241-246`). Nothing reads from it. The real time-series path is Kafka → `prometheus-writer` → VictoriaMetrics → Grafana, and that path's counter (`deltav.collectd.persister.kafka.failures{step=…}`) is at `0.0`. So:
- **No data is lost** — Kafka publishes every sample.
- **No dashboard is affected** — VictoriaMetrics is the backing store.
- The only symptom is log noise (one WARN per attribute per cycle) and non-zero counter values that complicate steady-state alerting.

Treat this as a *cleanup* / *noise-reduction* bug, not a data-integrity one.

## The fix (single method add)

Copy horizon's implementation verbatim into delta-v's jakarta fork:

```java
// In core/opennms-model-jakarta/src/main/java/org/opennms/netmgt/model/ResourceTypeUtils.java
import org.opennms.netmgt.rrd.RrdRepository;

/**
 * Retrieves the ResourcePath relative to rrd.base.dir.
 *
 * Added to match horizon's opennms-model API surface (horizon 1.0.11's
 * features.timeseries calls this method and gets NoSuchMethodError if
 * it resolves to the jakarta fork — which it does in delta-v daemons).
 */
public static ResourcePath getResourcePathWithRepository(RrdRepository repository, ResourcePath resource) {
    return ResourcePath.get(ResourcePath.get(repository.getRrdBaseDir().getName()), resource);
}
```

Horizon source: `delta-v-horizon/opennms-model/src/main/java/org/opennms/netmgt/model/ResourceTypeUtils.java:181-185`.

Verify `RrdRepository` and `ResourcePath.get(...)` are already in the jakarta classpath (they are — `RrdRepository` lives in `org.opennms.netmgt.rrd` under horizon's `core/opennms-model-api`, which delta-v pulls; `ResourcePath` same package).

## Before fixing — confirm this is the only missing method

Run a quick audit: grep horizon's `features.timeseries` bytecode for every `ResourceTypeUtils.*(` call and cross-check each against delta-v's jakarta `ResourceTypeUtils`:

```bash
# Inside the horizon timeseries jar:
javap -c ~/.m2/repository/org/opennms/features/timeseries/1.0.11/timeseries-1.0.11.jar \
  | grep -E "invoke.*ResourceTypeUtils" | sort -u
# Against the jakarta source:
grep -E "^\s*public static" core/opennms-model-jakarta/src/main/java/org/opennms/netmgt/model/ResourceTypeUtils.java
```

If there are more missing methods beyond `getResourcePathWithRepository`, add them all in one PR.

## Testing

1. Unit test in `core/opennms-model-jakarta`: assert the method returns the expected path composition for a known repository + resource.
2. After rebuilding the collectd image and restarting, the DONE criterion is:
   ```bash
   docker exec delta-v-collectd wget -q -O- http://127.0.0.1:8080/actuator/prometheus \
     | grep -E '^deltav_collectd_persister_inner_failures_total\{(step|application).*(step="(visitAttribute|completeResource)")'
   # both should read 0.0 after two 30-s poll cycles
   ```

## Environment notes (if reproducing locally)

- Docker Desktop on macOS: PR #187's named-volume fix for `provisiond_imports` is **silently dropped** by VirtioFS when nested under a bind-mounted parent (same class of bug as `project_docker_compose_nested_bind_mount_bug`). Workaround for local repro only: change `docker-compose.yml` line 521 `:ro` → `:rw` and pre-seed `opennms-container/delta-v/provisiond-overlay/etc/imports/` from `…/imports-seed/` on the host. **Revert before committing.** Alternative: run on Linux (labbox would need a delta-v clone + image push — not stood up there yet).

## Out of scope

- Bug #1 (UnexpectedRollbackException) — CLOSED 2026-04-22. Horizon 1.0.11's `TimeseriesPersister.visitResource` try/catch absorbs it; `{step=visitResource}` stays at 0.0. The underlying rollback-only marking may still happen on some code paths but is now completely contained.
- PR #187's Docker Desktop gap — separate cleanup item (affects only local mac repro; Linux CI and production are unaffected).
- Rewriting the inner persister — delta-v already chose Kafka → Prometheus RW as the real TSDB. The inner path is dev-only InMemoryStorage and exists only to keep horizon's `CollectableService` wiring happy.

## Key files

| File | Why |
|---|---|
| `core/opennms-model-jakarta/src/main/java/org/opennms/netmgt/model/ResourceTypeUtils.java` | Where the new method goes |
| `delta-v-horizon/opennms-model/src/main/java/org/opennms/netmgt/model/ResourceTypeUtils.java:181-185` | Reference implementation to copy |
| `core/daemon-boot-collectd/src/main/java/org/deltav/collectd/timeseries/TimeseriesKafkaPublisherConfiguration.java:221-339` | Where `FanoutPersister.runInner` catches + counters tick |

## Session size

~1 hr. 10 min to add the method + test. 30 min to rebuild the collectd image and re-measure locally. 15 min for commit + PR.
