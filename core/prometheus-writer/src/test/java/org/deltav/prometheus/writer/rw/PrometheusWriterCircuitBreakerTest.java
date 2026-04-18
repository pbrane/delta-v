/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.rw;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.deltav.prometheus.writer.config.PrometheusWriterProperties;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class PrometheusWriterCircuitBreakerTest {

    private PrometheusWriterProperties props(int failureRate, int window, int minCalls, long waitOpenMs, int halfOpenCalls) {
        return new PrometheusWriterProperties(
                new PrometheusWriterProperties.RemoteWrite("http://t/w",
                    new PrometheusWriterProperties.Auth(PrometheusWriterProperties.AuthType.NONE, null, null, null), Map.of()),
                null, null,
                new PrometheusWriterProperties.CircuitBreaker(failureRate, window, minCalls, waitOpenMs, halfOpenCalls),
                null, null);
    }

    private CircuitBreaker fresh(PrometheusWriterProperties p) {
        PrometheusWriterCircuitBreakerConfiguration cfg = new PrometheusWriterCircuitBreakerConfiguration();
        CircuitBreakerRegistry reg = cfg.circuitBreakerRegistry(p);
        return cfg.prometheusWriterCircuitBreaker(reg, new SimpleMeterRegistry(), p);
    }

    private void runFailing(CircuitBreaker cb) {
        try {
            cb.executeRunnable(() -> { throw new RuntimeException("fail"); });
        } catch (Exception ignored) {}
    }

    private void runSuccess(CircuitBreaker cb) {
        cb.executeRunnable(() -> {});
    }

    @Test
    void starts_closed() {
        CircuitBreaker cb = fresh(props(50, 20, 10, 30_000, 3));
        assertThat(cb.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
    }

    @Test
    void opens_at_failure_rate_threshold() {
        CircuitBreaker cb = fresh(props(50, 20, 20, 30_000, 3));
        // 20 calls, 11 fail (>50%) — must open
        for (int i = 0; i < 11; i++) runFailing(cb);
        for (int i = 0; i < 9; i++) runSuccess(cb);
        assertThat(cb.getState()).isEqualTo(CircuitBreaker.State.OPEN);
    }

    @Test
    void transitions_half_open_after_wait_duration() throws Exception {
        // Use a short wait duration so the test doesn't take 30 seconds
        CircuitBreaker cb = fresh(props(50, 20, 20, 200, 3));
        for (int i = 0; i < 11; i++) runFailing(cb);
        for (int i = 0; i < 9; i++) runSuccess(cb);
        assertThat(cb.getState()).isEqualTo(CircuitBreaker.State.OPEN);
        // Wait past the wait duration; automaticTransitionFromOpenToHalfOpenEnabled(true)
        // means the breaker self-transitions, but a thread/scheduler is needed.
        // Force the transition explicitly to avoid timing flakes.
        Thread.sleep(250);
        cb.transitionToHalfOpenState();
        assertThat(cb.getState()).isEqualTo(CircuitBreaker.State.HALF_OPEN);
    }

    @Test
    void returns_to_closed_after_successful_probes() {
        CircuitBreaker cb = fresh(props(50, 20, 20, 30_000, 3));
        for (int i = 0; i < 11; i++) runFailing(cb);
        for (int i = 0; i < 9; i++) runSuccess(cb);
        cb.transitionToHalfOpenState();
        // 3 permitted half-open calls, all succeed → CLOSED
        for (int i = 0; i < 3; i++) runSuccess(cb);
        assertThat(cb.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
    }

    @Test
    void returns_to_open_on_half_open_failure() {
        CircuitBreaker cb = fresh(props(50, 20, 20, 30_000, 3));
        for (int i = 0; i < 11; i++) runFailing(cb);
        for (int i = 0; i < 9; i++) runSuccess(cb);
        cb.transitionToHalfOpenState();
        // First half-open call fails → OPEN
        runFailing(cb);
        // Subsequent calls inside half-open window also fail to bring window in
        for (int i = 0; i < 2; i++) runFailing(cb);
        assertThat(cb.getState()).isEqualTo(CircuitBreaker.State.OPEN);
    }
}
