/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.alerts.forwarder.lifecycle;

import java.util.Map;
import java.util.Optional;

import org.deltav.alerts.forwarder.sink.ForwardedAlarm;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ActiveAlertRegistryTest {

    private ForwardedAlarm firing(String rk) {
        return new ForwardedAlarm(rk, ForwardedAlarm.State.FIRING,
                Map.of("node", "Servers:web-01"), Map.of("description", "down"), 1000L);
    }

    @Test
    void markFiringThenIsActive() {
        ActiveAlertRegistry registry = new ActiveAlertRegistry();
        assertThat(registry.isActive("rk")).isFalse();
        registry.markFiring(firing("rk"));
        assertThat(registry.isActive("rk")).isTrue();
        assertThat(registry.size()).isEqualTo(1);
    }

    @Test
    void removeReturnsStoredIdentityAndEvicts() {
        ActiveAlertRegistry registry = new ActiveAlertRegistry();
        ForwardedAlarm stored = firing("rk");
        registry.markFiring(stored);

        Optional<ForwardedAlarm> removed = registry.remove("rk");

        assertThat(removed).contains(stored);
        assertThat(registry.isActive("rk")).isFalse();
    }

    @Test
    void removeUnknownKeyReturnsEmpty() {
        assertThat(new ActiveAlertRegistry().remove("nope")).isEmpty();
    }
}
