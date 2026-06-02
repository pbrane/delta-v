/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.alerts.forwarder.pipeline;

import java.util.ArrayList;
import java.util.List;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.deltav.alarms.proto.AlarmState;
import org.deltav.alerts.forwarder.enrich.AlarmEnricher;
import org.deltav.alerts.forwarder.filter.AlarmFilter;
import org.deltav.alerts.forwarder.lifecycle.ActiveAlertRegistry;
import org.deltav.nodecontext.NodeContextCache;
import org.deltav.alerts.forwarder.pipeline.AlarmForwardingPipeline.SinkForwardException;
import org.deltav.alerts.forwarder.pipeline.AlarmForwardingPipeline.SinkForwardException.Classification;
import org.deltav.alerts.forwarder.sink.AlarmSink;
import org.deltav.alerts.forwarder.sink.ForwardedAlarm;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AlarmForwardingPipelineTest {

    private final List<ForwardedAlarm> sent = new ArrayList<>();

    private final AlarmSink recordingSink = new AlarmSink() {
        public void forward(ForwardedAlarm a) { sent.add(a); }
        public String name() { return "recording"; }
    };

    private AlarmForwardingPipeline pipeline(String minSeverity) {
        return new AlarmForwardingPipeline(
                new AlarmFilter(minSeverity, List.of()),
                new AlarmEnricher(new NodeContextCache()),
                new ActiveAlertRegistry(),
                List.of(recordingSink),
                new SimpleMeterRegistry());
    }

    private AlarmState alarm(String rk, AlarmState.Severity sev) {
        return AlarmState.newBuilder()
                .setReductionKey(rk).setSeverity(sev).setUei("uei/x").setNodeId(1).build();
    }

    @Test
    void firingAlarmAboveThresholdIsForwarded() {
        pipeline("WARNING").onRecord("rk", alarm("rk", AlarmState.Severity.MAJOR));
        assertThat(sent).hasSize(1);
        assertThat(sent.get(0).state()).isEqualTo(ForwardedAlarm.State.FIRING);
    }

    @Test
    void alarmBelowThresholdIsNotForwarded() {
        pipeline("MAJOR").onRecord("rk", alarm("rk", AlarmState.Severity.WARNING));
        assertThat(sent).isEmpty();
    }

    @Test
    void tombstoneResolvesActiveAlarm() {
        AlarmForwardingPipeline p = pipeline("WARNING");
        p.onRecord("rk", alarm("rk", AlarmState.Severity.MAJOR));
        p.onRecord("rk", null);
        assertThat(sent).hasSize(2);
        assertThat(sent.get(1).state()).isEqualTo(ForwardedAlarm.State.RESOLVED);
    }

    @Test
    void clearedSeverityResolvesActiveAlarm() {
        AlarmForwardingPipeline p = pipeline("WARNING");
        p.onRecord("rk", alarm("rk", AlarmState.Severity.MAJOR));
        p.onRecord("rk", alarm("rk", AlarmState.Severity.CLEARED));
        assertThat(sent).hasSize(2);
        assertThat(sent.get(1).state()).isEqualTo(ForwardedAlarm.State.RESOLVED);
    }

    @Test
    void resolveReusesStoredFiringLabels() {
        AlarmForwardingPipeline p = pipeline("WARNING");
        p.onRecord("rk", alarm("rk", AlarmState.Severity.MAJOR));
        p.onRecord("rk", null);
        assertThat(sent.get(1).labels()).isEqualTo(sent.get(0).labels());
    }

    @Test
    void tombstoneForUnknownKeyForwardsNothing() {
        pipeline("WARNING").onRecord("never-seen", null);
        assertThat(sent).isEmpty();
    }

    // ── SinkForwardException classification tests ────────────────────────────

    @Test
    void classifiesHttpClientErrorExceptionAsPoison() {
        SinkForwardException ex = new SinkForwardException("alertmanager",
                new HttpClientErrorException(HttpStatus.BAD_REQUEST, "Bad Request"));
        assertThat(ex.sinkName()).isEqualTo("alertmanager");
        assertThat(ex.classify()).isEqualTo(Classification.POISON);
    }

    @Test
    void classifiesHttpServerErrorExceptionAsTransient() {
        SinkForwardException ex = new SinkForwardException("alertmanager",
                new HttpServerErrorException(HttpStatus.SERVICE_UNAVAILABLE, "Service Unavailable"));
        assertThat(ex.classify()).isEqualTo(Classification.TRANSIENT);
    }

    @Test
    void classifiesResourceAccessExceptionAsTransient() {
        SinkForwardException ex = new SinkForwardException("alertmanager",
                new ResourceAccessException("Connection refused"));
        assertThat(ex.classify()).isEqualTo(Classification.TRANSIENT);
    }

    @Test
    void classifiesUnknownRuntimeExceptionAsTransient() {
        SinkForwardException ex = new SinkForwardException("alertmanager",
                new RuntimeException("unexpected error"));
        assertThat(ex.classify()).isEqualTo(Classification.TRANSIENT);
    }

    @Test
    void resolveRetainsRegistryEntryWhenSinkFails() {
        // Sink that throws on the resolve forward (second call) only.
        AlarmSink flakySink = new AlarmSink() {
            int calls = 0;
            public void forward(ForwardedAlarm a) throws Exception {
                calls++;
                if (a.state() == ForwardedAlarm.State.RESOLVED) {
                    throw new RuntimeException("simulated sink failure");
                }
            }
            public String name() { return "flaky"; }
        };
        ActiveAlertRegistry registry = new ActiveAlertRegistry();
        AlarmForwardingPipeline p = new AlarmForwardingPipeline(
                new AlarmFilter("WARNING", List.of()),
                new AlarmEnricher(new NodeContextCache()),
                registry,
                List.of(flakySink),
                new SimpleMeterRegistry());

        p.onRecord("rk", AlarmState.newBuilder()
                .setReductionKey("rk").setSeverity(AlarmState.Severity.MAJOR)
                .setUei("uei/x").setNodeId(1).build());
        assertThat(registry.isActive("rk")).isTrue();

        // tombstone → resolve forward throws — registry must still have the entry so the consumer can retry.
        assertThrows(AlarmForwardingPipeline.SinkForwardException.class,
                () -> p.onRecord("rk", null));
        assertThat(registry.isActive("rk")).isTrue();
    }
}
