/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.rw;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.deltav.prometheus.writer.config.PrometheusWriterProperties;
import org.deltav.prometheus.writer.metrics.PrometheusWriterMetrics;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

@Configuration
public class PrometheusWriterCircuitBreakerConfiguration {

    public static final String NAME = "prometheus-writer";

    @Bean
    public CircuitBreakerRegistry circuitBreakerRegistry(PrometheusWriterProperties props) {
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .failureRateThreshold(props.circuitBreaker().failureRateThreshold())
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(props.circuitBreaker().slidingWindowSize())
                .minimumNumberOfCalls(props.circuitBreaker().minimumNumberOfCalls())
                .waitDurationInOpenState(Duration.ofMillis(props.circuitBreaker().waitDurationOpenMs()))
                .permittedNumberOfCallsInHalfOpenState(props.circuitBreaker().halfOpenPermittedCalls())
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .build();
        return CircuitBreakerRegistry.of(config);
    }

    @Bean
    public CircuitBreaker prometheusWriterCircuitBreaker(
            CircuitBreakerRegistry registry, MeterRegistry metrics, PrometheusWriterProperties props) {
        CircuitBreaker cb = registry.circuitBreaker(NAME);
        AtomicInteger stateGauge = new AtomicInteger(0);
        cb.getEventPublisher().onStateTransition(e -> {
            stateGauge.set(switch (e.getStateTransition().getToState()) {
                case CLOSED, METRICS_ONLY, DISABLED, FORCED_OPEN -> 0;
                case HALF_OPEN -> 1;
                case OPEN -> 2;
            });
        });
        Gauge.builder(PrometheusWriterMetrics.CIRCUIT_STATE, stateGauge::get)
                .tag("endpoint", props.remoteWrite().url())
                .register(metrics);
        return cb;
    }
}
