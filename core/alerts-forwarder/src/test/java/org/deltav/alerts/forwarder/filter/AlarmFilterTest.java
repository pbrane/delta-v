/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.alerts.forwarder.filter;

import java.util.List;

import org.deltav.alarms.proto.AlarmState;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class AlarmFilterTest {

    private AlarmState alarm(AlarmState.Severity severity, String uei) {
        return AlarmState.newBuilder()
                .setReductionKey("rk").setSeverity(severity).setUei(uei).build();
    }

    @Test
    void acceptsAtOrAboveMinSeverity() {
        AlarmFilter filter = new AlarmFilter("WARNING", List.of());
        assertThat(filter.accept(alarm(AlarmState.Severity.WARNING, "uei/x"))).isTrue();
        assertThat(filter.accept(alarm(AlarmState.Severity.CRITICAL, "uei/x"))).isTrue();
    }

    @Test
    void rejectsBelowMinSeverity() {
        AlarmFilter filter = new AlarmFilter("MAJOR", List.of());
        assertThat(filter.accept(alarm(AlarmState.Severity.WARNING, "uei/x"))).isFalse();
        assertThat(filter.accept(alarm(AlarmState.Severity.CLEARED, "uei/x"))).isFalse();
    }

    @Test
    void rejectsDenylistedUei() {
        AlarmFilter filter = new AlarmFilter("WARNING", List.of("uei/noisy"));
        assertThat(filter.accept(alarm(AlarmState.Severity.CRITICAL, "uei/noisy"))).isFalse();
        assertThat(filter.accept(alarm(AlarmState.Severity.CRITICAL, "uei/ok"))).isTrue();
    }

    @Test
    void invalidMinSeverityFallsBackToWarning() {
        AlarmFilter filter = new AlarmFilter("NONSENSE", List.of());
        assertThat(filter.accept(alarm(AlarmState.Severity.WARNING, "uei/x"))).isTrue();
        assertThat(filter.accept(alarm(AlarmState.Severity.NORMAL, "uei/x"))).isFalse();
    }
}
