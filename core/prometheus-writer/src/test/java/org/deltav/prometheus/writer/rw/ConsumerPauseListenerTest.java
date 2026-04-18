/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.rw;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.stream.binding.BindingsLifecycleController;

import static org.mockito.Mockito.*;

class ConsumerPauseListenerTest {

    private static final String BINDING = "timeseriesConsumer-in-0";

    private CircuitBreaker cb;
    private BindingsLifecycleController lifecycle;
    private ConsumerPauseListener listener;

    @BeforeEach
    void setUp() {
        cb = CircuitBreakerRegistry.ofDefaults().circuitBreaker("test");
        lifecycle = mock(BindingsLifecycleController.class);
        listener = new ConsumerPauseListener(cb, lifecycle, new SimpleMeterRegistry());
        listener.wire();
    }

    @Test
    void closed_to_open_pauses_binding() {
        cb.transitionToOpenState();   // CLOSED → OPEN
        verify(lifecycle, times(1)).pause(BINDING);
        verify(lifecycle, never()).resume(anyString());
    }

    @Test
    void half_open_to_closed_resumes_binding() {
        cb.transitionToOpenState();        // CLOSED → OPEN (one pause)
        cb.transitionToHalfOpenState();    // OPEN → HALF_OPEN
        clearInvocations(lifecycle);
        cb.transitionToClosedState();      // HALF_OPEN → CLOSED (resume)
        verify(lifecycle, times(1)).resume(BINDING);
        verify(lifecycle, never()).pause(anyString());
    }

    @Test
    void half_open_to_open_pauses_again() {
        cb.transitionToOpenState();        // CLOSED → OPEN (initial pause)
        cb.transitionToHalfOpenState();    // OPEN → HALF_OPEN
        clearInvocations(lifecycle);
        cb.transitionToOpenState();        // HALF_OPEN → OPEN (pause again)
        verify(lifecycle, times(1)).pause(BINDING);
        verify(lifecycle, never()).resume(anyString());
    }

    @Test
    void open_to_half_open_is_noop() {
        cb.transitionToOpenState();        // CLOSED → OPEN (one pause)
        clearInvocations(lifecycle);
        cb.transitionToHalfOpenState();    // OPEN → HALF_OPEN — should be noop
        verifyNoInteractions(lifecycle);
    }
}
