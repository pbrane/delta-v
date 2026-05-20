/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.alerts.forwarder.pipeline;

import java.util.List;
import java.util.Optional;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.deltav.alarms.proto.AlarmState;
import org.deltav.alerts.forwarder.enrich.AlarmEnricher;
import org.deltav.alerts.forwarder.enrich.EnrichedAlarm;
import org.deltav.alerts.forwarder.filter.AlarmFilter;
import org.deltav.alerts.forwarder.lifecycle.ActiveAlertRegistry;
import org.deltav.alerts.forwarder.metrics.AlertsForwarderMetrics;
import org.deltav.alerts.forwarder.sink.AlarmSink;
import org.deltav.alerts.forwarder.sink.ForwardedAlarm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Turns one consumed alarm record into sink calls. Lifecycle (spec §5.3): a
 * tombstone or a sub-threshold record on an active alarm resolves it; a record
 * above the filter fires it. Resolves reuse the {@link ActiveAlertRegistry}'s
 * stored firing identity so label sets stay stable.
 *
 * <p>Sink failures propagate out of {@link #onRecord} so the Kafka consumer
 * can retry and back-pressure rather than drop the alarm.</p>
 */
@Component
public class AlarmForwardingPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(AlarmForwardingPipeline.class);

    private final AlarmFilter filter;
    private final AlarmEnricher enricher;
    private final ActiveAlertRegistry registry;
    private final List<AlarmSink> sinks;
    private final Counter consumed;
    private final MeterRegistry metrics;

    public AlarmForwardingPipeline(AlarmFilter filter, AlarmEnricher enricher,
                                   ActiveAlertRegistry registry, List<AlarmSink> sinks,
                                   MeterRegistry metrics) {
        this.filter = filter;
        this.enricher = enricher;
        this.registry = registry;
        this.sinks = sinks;
        this.metrics = metrics;
        this.consumed = metrics.counter(AlertsForwarderMetrics.ALARMS_CONSUMED);
    }

    /** Processes one record. A {@code null} alarm is a Kafka tombstone (delete). */
    public void onRecord(String reductionKey, AlarmState alarm) {
        consumed.increment();

        if (alarm == null) {
            resolve(reductionKey);
            return;
        }
        if (!filter.accept(alarm)) {
            // Sub-threshold or CLEARED: resolve if we are currently firing it.
            if (registry.isActive(reductionKey)) {
                resolve(reductionKey);
            } else {
                metrics.counter(AlertsForwarderMetrics.FILTERED, "reason", "severity").increment();
            }
            return;
        }

        EnrichedAlarm e = enricher.enrich(alarm);
        metrics.counter(AlertsForwarderMetrics.ENRICHMENT,
                "result", e.enrichmentComplete() ? "hit" : "partial").increment();

        ForwardedAlarm fa = new ForwardedAlarm(reductionKey, ForwardedAlarm.State.FIRING,
                e.labels(), e.annotations(), alarm.getFirstEventTimeMs());
        registry.markFiring(fa);
        forward(fa);
        metrics.counter(AlertsForwarderMetrics.FORWARDED, "outcome", "firing").increment();
    }

    private void resolve(String reductionKey) {
        Optional<ForwardedAlarm> firing = registry.getFiring(reductionKey);
        if (firing.isEmpty()) {
            return;   // not active — nothing to resolve
        }
        ForwardedAlarm prev = firing.get();
        ForwardedAlarm resolved = new ForwardedAlarm(reductionKey, ForwardedAlarm.State.RESOLVED,
                prev.labels(), prev.annotations(), prev.startsAtMs());
        forward(resolved);   // throws → consumer retries; registry still populated
        registry.remove(reductionKey);
        metrics.counter(AlertsForwarderMetrics.FORWARDED, "outcome", "resolved").increment();
    }

    private void forward(ForwardedAlarm alarm) {
        for (AlarmSink sink : sinks) {
            try {
                sink.forward(alarm);
            } catch (Exception e) {
                metrics.counter(AlertsForwarderMetrics.SINK_ERRORS, "sink", sink.name()).increment();
                // Propagate so the consumer retries and back-pressures.
                throw new SinkForwardException(sink.name(), e);
            }
        }
    }

    /** Thrown when a sink fails; the consumer retries the record. */
    public static class SinkForwardException extends RuntimeException {
        public SinkForwardException(String sink, Throwable cause) {
            super("sink '" + sink + "' failed to forward alarm", cause);
        }
    }
}
