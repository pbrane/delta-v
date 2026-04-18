/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.rw;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import org.deltav.prometheus.writer.metrics.PrometheusWriterMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.binding.BindingsLifecycleController;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

@Component
public class ConsumerPauseListener {
    private static final Logger LOG = LoggerFactory.getLogger(ConsumerPauseListener.class);
    private static final String BINDING = "timeseriesConsumer-in-0";

    private final CircuitBreaker cb;
    private final BindingsLifecycleController lifecycle;
    private final AtomicInteger pausedGauge = new AtomicInteger(0);

    public ConsumerPauseListener(CircuitBreaker cb, BindingsLifecycleController lifecycle, MeterRegistry metrics) {
        this.cb = cb;
        this.lifecycle = lifecycle;
        Gauge.builder(PrometheusWriterMetrics.CONSUMER_PAUSED, pausedGauge::get).register(metrics);
    }

    @PostConstruct
    public void wire() {
        cb.getEventPublisher().onStateTransition(e -> {
            var from = e.getStateTransition().getFromState();
            var to = e.getStateTransition().getToState();
            boolean pauseNow = to == CircuitBreaker.State.OPEN
                    && (from == CircuitBreaker.State.CLOSED || from == CircuitBreaker.State.HALF_OPEN);
            boolean resumeNow = from == CircuitBreaker.State.HALF_OPEN && to == CircuitBreaker.State.CLOSED;
            if (pauseNow) {
                LOG.warn("Circuit opened — pausing binding {}", BINDING);
                lifecycle.pause(BINDING);
                pausedGauge.set(1);
            } else if (resumeNow) {
                LOG.info("Circuit closed — resuming binding {}", BINDING);
                lifecycle.resume(BINDING);
                pausedGauge.set(0);
            }
        });
    }
}
