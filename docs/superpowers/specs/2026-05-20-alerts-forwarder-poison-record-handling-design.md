# alerts-forwarder — Poison-Record Handling Design

**Status:** Spec — surfaced 2026-05-20 by `test-alerts-forwarder-e2e.sh` (PR #298)
**Scope:** Two related bugs in `core/alerts-forwarder` that compound to wedge the alarm consumer indefinitely under realistic conditions
**Priority:** Should land before v1.3.0 GA (RC unblocked; the E2E test sidesteps both bugs)
**Module:** `core/alerts-forwarder` (Track 2b, merged to `develop` via #297)

---

## 1. Why this exists

While writing the end-to-end test that validates the v1.3.0 alarms-on-Kafka
pipeline through Alertmanager, two real production defects surfaced. The test
sidesteps both with `date -u` for syslog timestamps, but the underlying defects
remain in the daemon. Either one alone is recoverable; together they form a
trap where one malformed `AlarmState` record permanently wedges the consumer.

Neither bug is a regression — both are inherent to the Track 2b implementation
as merged. They were not caught earlier because:
- Unit tests don't exercise an out-of-window `firstEventTimeMs`.
- The Testcontainers integration test uses synthetic records with sensible
  timestamps.
- The Task 12 docker-compose smoke happened to use real alarmd timestamps
  generated within the alarmd container (UTC) — no clock skew was introduced.

The E2E test surfaced both because it triggers alarms from outside the alarmd
container via syslog with host-time RFC3164 stamps; OpenNMS interprets those
as container-local UTC, producing alarms whose `firstEventTimeMs` is hours
ahead of wall-clock UTC.

## 2. Bug 1 — `AlertmanagerAlarmSink` rejects `startsAt > endsAt`

### Symptom

Alertmanager returns `400 Bad Request` with body `start time must be before end
time` when the forwarder POSTs an alert whose `startsAt` (derived from the
alarm's `firstEventTimeMs`) is later than its `endsAt` (computed as `now +
resolveTimeoutMs`, default 5 minutes).

### Root cause

`AlertmanagerAlarmSink.forward(ForwardedAlarm alarm)`
(`core/alerts-forwarder/src/main/java/org/deltav/alerts/forwarder/sink/AlertmanagerAlarmSink.java`)
builds the alert JSON unconditionally:

```java
Instant now = Instant.now();
Instant endsAt = alarm.state() == ForwardedAlarm.State.RESOLVED
        ? now
        : now.plus(Duration.ofMillis(resolveTimeoutMs));

Map<String, Object> alert = Map.of(
        "labels", alarm.labels(),
        "annotations", alarm.annotations(),
        "startsAt", Instant.ofEpochMilli(alarm.startsAtMs()).toString(),
        "endsAt", endsAt.toString());
```

There is no validation that `startsAtMs ≤ endsAt`. When the source alarm's
`firstEventTimeMs` is more than `resolveTimeoutMs` in the future relative to
wall-clock (clock skew, mis-parsed timezone, future-dated synthetic event),
the resulting payload is invalid by Alertmanager's contract and the POST
fails with 400. Alertmanager treats this as a permanent rejection; a retry
of the same payload is doomed.

### Trigger conditions in practice

- Non-UTC syslog stamps interpreted as container-UTC by OpenNMS — discovered
  via `test-alerts-forwarder-e2e.sh`.
- Any monitored system whose clock is hours ahead of the OpenNMS host.
- Clock-drift between alarmd's container and downstream consumers.
- Synthetic / test-injected events with future timestamps.

The forwarder must not assume sane timestamps.

### Fix

Clamp `startsAt` defensively inside the sink — never let `startsAt` exceed
`endsAt - 1s`. The clamp is recorded as a `startsAt_clamped="true"` label
or annotation (for operator visibility) so it's auditable rather than silent.

Pseudo-code:
```java
Instant requestedStartsAt = Instant.ofEpochMilli(alarm.startsAtMs());
Instant clampedStartsAt = requestedStartsAt.isAfter(endsAt.minusSeconds(1))
        ? endsAt.minusSeconds(1)
        : requestedStartsAt;
boolean clamped = !requestedStartsAt.equals(clampedStartsAt);
```

If clamped, log `WARN` once per alarm (rate-limited) and add an annotation
`x-deltav-startsAt-original=<requestedStartsAt>` so an operator inspecting
the alert in Alertmanager can see what the source timestamp was.

This is a one-class change with one new unit test (a `ForwardedAlarm` whose
`startsAtMs` is in the future → sink clamps to `endsAt - 1s`, no exception).

## 3. Bug 2 — `AlarmStateKafkaConsumer` infinite-retries on 4xx

### Symptom

When `AlarmForwardingPipeline.SinkForwardException` is thrown by a sink that
returned `4xx` (a permanent client error), the consumer's `handleWithRetry`
loop retries the same record forever — sleep 5 seconds, retry, get the same
4xx, retry again. The consumer's offset never advances. **No subsequent
record on the partition is ever consumed.**

In production this means: a single poison record on
`deltav-alarms-state-change` wedges the entire alarm forwarding pipeline. The
bootstrap-replay design amplifies the failure across restarts — restarting
the daemon does not recover because the same record is replayed and again
trips the retry loop.

### Root cause

`AlarmStateKafkaConsumer.handleWithRetry(ConsumerRecord)`
(`core/alerts-forwarder/src/main/java/org/deltav/alerts/forwarder/consume/AlarmStateKafkaConsumer.java`):

```java
private void handleWithRetry(ConsumerRecord<byte[], byte[]> record) {
    while (running.get()) {
        try {
            handle(record);
            return;
        } catch (AlarmForwardingPipeline.SinkForwardException e) {
            LOG.warn("Sink failure — retrying record in {}s", RETRY_BACKOFF.toSeconds(), e);
            sleep(RETRY_BACKOFF);
        }
    }
}
```

`SinkForwardException` is thrown by `AlarmForwardingPipeline.forward()` for
every sink failure — 4xx and 5xx and transport errors alike. The retry loop
makes no distinction. The intent at design time was back-pressure: "a slow
or transiently-unreachable sink should not lose alarms, so block the
consumer thread until the sink recovers." That intent is correct for `5xx`
and connection errors — it is wrong for `4xx`, which is a poison record
that will never succeed.

### Fix

Classify the sink failure at the point of throw, then handle each class
appropriately.

- **Transient (`5xx`, connection refused, read timeout, DNS failure):**
  current behavior — sleep, retry. This is back-pressure.
- **Permanent (`4xx`):** treat as a poison record. Publish to DLQ
  (`deltav-alerts-forwarder-dlq`) with reason `sink-4xx-rejected` and the
  failing sink name in a header. Advance past the record. Increment
  `deltav_alerts_forwarder_sink_errors_total{sink, classification="poison"}`.

This requires `SinkForwardException` to carry the underlying HTTP status
(or a classification enum), and `handleWithRetry` to branch on it. The two
existing sinks (`AlertmanagerAlarmSink`, `VictoriaMetricsAlarmSink`) both
use Spring's `RestClient.retrieve().toBodilessEntity()`, which throws
`HttpClientErrorException` (4xx) and `HttpServerErrorException` (5xx) as
distinct subclasses of `RestClientResponseException` — the classification
is already available at the throw site.

This is a two-class change (`AlarmForwardingPipeline` for the exception
enrichment, `AlarmStateKafkaConsumer` for the branching retry) plus two
unit tests (poison-record → DLQ + advance; transient → retry).

## 4. Why Bug 1 + Bug 2 must be fixed together

Either bug alone is bounded:

- **Bug 1 alone:** a few alarms with bad timestamps would be rejected by
  Alertmanager and infinite-retried, but operators would notice quickly,
  manually clamp the timestamps upstream, and the system recovers as the
  bad records age out of the compacted topic.
- **Bug 2 alone:** without Bug 1 to trigger a 4xx, the retry loop only
  fires on transient errors, where back-pressure is correct behavior.

Together they create a trap: **one malformed event in the entire history
of `deltav-alarms-state-change` permanently wedges the consumer for every
subsequent alarm** until manual intervention (delete the topic, or
hand-tombstone the bad key with a specific reduction-key crafted producer).
Restarts do not recover because the bootstrap-replay design re-encounters
the same record.

The fixes are therefore coordinated. Bug 1's clamp removes the most common
cause of 4xx; Bug 2's classification ensures any future cause of 4xx (any
sink, any reason) is handled correctly.

## 5. Acceptance criteria

- A unit test for `AlertmanagerAlarmSink` covering a `ForwardedAlarm` with
  `startsAtMs > now + resolveTimeoutMs` — sink clamps and POSTs successfully
  against a `MockWebServer` returning 200.
- A unit test for the new `AlarmStateKafkaConsumer` retry classification —
  a sink that throws on every call with a simulated 4xx response → consumer
  publishes one DLQ record and advances; a sink that throws with 5xx →
  consumer retries (existing behavior preserved).
- The existing `AlertsForwarderIntegrationIT` and `AlarmStateKafkaConsumerIT`
  continue to pass.
- `test-alerts-forwarder-e2e.sh` continues to pass and can have its
  `date -u` workaround removed (drop the workaround in the same PR as
  acceptance evidence).
- An end-to-end smoke confirms: trigger a real alarm with a deliberately
  future-dated timestamp → Alertmanager receives the clamped alert; trigger
  a synthetic poison record on the topic (e.g. random bytes, or an
  AlarmState that drives a 400 some other way) → DLQ record appears
  within seconds and subsequent alarms continue to forward normally.

## 6. Out of scope

- Reworking the bootstrap-replay design itself. The pattern is correct;
  Bug 2 is a defect within it, not against it.
- Adding a separate retry-with-exponential-backoff layer. The existing
  fixed 5-second sleep for transient retries is fine for v1.3.0.
- Source-side timestamp validation (in alarmd, syslogd, EventTranslator).
  The forwarder must be robust regardless of source quality.

## 7. Related

- Track 2b PR #297 — the module these bugs live in.
- RC test-readiness PR #298 — `test-alerts-forwarder-e2e.sh`, which
  surfaced both bugs and contains the `date -u` workaround.
- Memory: `feedback_silent_failure_*` — the general project rule against
  silent error swallowing. Bug 2's infinite-retry is a form of silent
  failure: the consumer reports it's running, metrics show consumed records,
  but in fact no forward progress is made.
