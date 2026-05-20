/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.alerts.forwarder.pipeline;

import java.util.ArrayList;
import java.util.List;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.deltav.alarms.proto.AlarmState;
import org.deltav.alerts.forwarder.enrich.AlarmEnricher;
import org.deltav.alerts.forwarder.filter.AlarmFilter;
import org.deltav.alerts.forwarder.lifecycle.ActiveAlertRegistry;
import org.deltav.alerts.forwarder.nodecontext.NodeContextCache;
import org.deltav.alerts.forwarder.sink.AlarmSink;
import org.deltav.alerts.forwarder.sink.ForwardedAlarm;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

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
}
