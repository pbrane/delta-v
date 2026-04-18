/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.rw;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RemoteWriteRetryPolicyTest {
    private final RemoteWriteRetryPolicy p = new RemoteWriteRetryPolicy();

    @Test void success_200() { assertThat(p.classifyStatus(200)).isEqualTo(RemoteWriteRetryPolicy.Action.SUCCESS); }
    @Test void success_204() { assertThat(p.classifyStatus(204)).isEqualTo(RemoteWriteRetryPolicy.Action.SUCCESS); }
    @Test void retry_429() { assertThat(p.classifyStatus(429)).isEqualTo(RemoteWriteRetryPolicy.Action.RETRY_WITH_BACKOFF); }
    @Test void retry_500() { assertThat(p.classifyStatus(500)).isEqualTo(RemoteWriteRetryPolicy.Action.RETRY_WITH_BACKOFF); }
    @Test void retry_502() { assertThat(p.classifyStatus(502)).isEqualTo(RemoteWriteRetryPolicy.Action.RETRY_WITH_BACKOFF); }
    @Test void retry_503() { assertThat(p.classifyStatus(503)).isEqualTo(RemoteWriteRetryPolicy.Action.RETRY_WITH_BACKOFF); }
    @Test void retry_504() { assertThat(p.classifyStatus(504)).isEqualTo(RemoteWriteRetryPolicy.Action.RETRY_WITH_BACKOFF); }
    @Test void poison_400() { assertThat(p.classifyStatus(400)).isEqualTo(RemoteWriteRetryPolicy.Action.POISON_DLQ); }
    @Test void poison_413() { assertThat(p.classifyStatus(413)).isEqualTo(RemoteWriteRetryPolicy.Action.POISON_DLQ); }
    @Test void circuit_401() { assertThat(p.classifyStatus(401)).isEqualTo(RemoteWriteRetryPolicy.Action.CIRCUIT_OPEN); }
    @Test void circuit_403() { assertThat(p.classifyStatus(403)).isEqualTo(RemoteWriteRetryPolicy.Action.CIRCUIT_OPEN); }
    @Test void circuit_404() { assertThat(p.classifyStatus(404)).isEqualTo(RemoteWriteRetryPolicy.Action.CIRCUIT_OPEN); }
    @Test void unknown_4xx_defaults_to_poison() { assertThat(p.classifyStatus(418)).isEqualTo(RemoteWriteRetryPolicy.Action.POISON_DLQ); }
    @Test void unknown_5xx_defaults_to_retry() { assertThat(p.classifyStatus(599)).isEqualTo(RemoteWriteRetryPolicy.Action.RETRY_WITH_BACKOFF); }
    @Test void network_exception_retries() {
        assertThat(p.classifyException(new java.net.ConnectException("refused")))
                .isEqualTo(RemoteWriteRetryPolicy.Action.RETRY_WITH_BACKOFF);
    }
    @Test void read_timeout_retries() {
        assertThat(p.classifyException(new java.net.SocketTimeoutException("timeout")))
                .isEqualTo(RemoteWriteRetryPolicy.Action.RETRY_WITH_BACKOFF);
    }
}
