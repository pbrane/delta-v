/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.startup;

import org.deltav.prometheus.writer.config.PrometheusWriterProperties;
import org.deltav.prometheus.writer.nodecontext.NodeContextCacheReadyEvent;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.stream.binding.BindingsLifecycleController;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Verifies the startup-gate contract:
 * <ul>
 *     <li>The {@link TimeseriesBindingStartupGate} pauses the listener container at
 *     factory time when {@code prometheus-writer.startup-gate.enabled=true}.</li>
 *     <li>It is a no-op when the gate is disabled.</li>
 *     <li>The {@link TimeseriesBindingResumer} resumes the binding when a
 *     {@link NodeContextCacheReadyEvent} is published, gated by the same flag.</li>
 * </ul>
 */
class TimeseriesBindingStartupGateTest {

    private static final String BINDING = "timeseriesConsumer-in-0";

    private static PrometheusWriterProperties props(boolean enabled) {
        return new PrometheusWriterProperties(
                null, null, null, null, null, null,
                new PrometheusWriterProperties.StartupGate(enabled));
    }

    @Test
    void customize_pauses_container_when_gate_enabled() {
        TimeseriesBindingStartupGate gate = new TimeseriesBindingStartupGate(props(true));
        @SuppressWarnings("unchecked")
        AbstractMessageListenerContainer<Object, Object> container =
                mock(AbstractMessageListenerContainer.class);

        gate.configure(container, "anything", "anything");

        verify(container).pause();
    }

    @Test
    void customize_no_op_when_gate_disabled() {
        TimeseriesBindingStartupGate gate = new TimeseriesBindingStartupGate(props(false));
        @SuppressWarnings("unchecked")
        AbstractMessageListenerContainer<Object, Object> container =
                mock(AbstractMessageListenerContainer.class);

        gate.configure(container, "anything", "anything");

        verify(container, never()).pause();
    }

    @Test
    void resumer_calls_resume_on_ready_event() {
        BindingsLifecycleController lifecycle = mock(BindingsLifecycleController.class);
        TimeseriesBindingResumer resumer = new TimeseriesBindingResumer(lifecycle, props(true));

        resumer.onReady(new NodeContextCacheReadyEvent(this, 1234L, 5));

        verify(lifecycle).resume(BINDING);
    }

    @Test
    void resumer_no_op_when_gate_disabled() {
        BindingsLifecycleController lifecycle = mock(BindingsLifecycleController.class);
        TimeseriesBindingResumer resumer = new TimeseriesBindingResumer(lifecycle, props(false));

        resumer.onReady(new NodeContextCacheReadyEvent(this, 1234L, 5));

        verify(lifecycle, never()).resume(BINDING);
    }
}
