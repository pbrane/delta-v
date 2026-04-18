/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.dlq;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.test.OutputDestination;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.messaging.Message;
import org.springframework.test.context.TestPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = DlqPublisherBinderIT.TestApp.class)
@TestPropertySource(properties = {
        "spring.cloud.stream.bindings.publishDlq-out-0.destination=publishDlq-out-0",
        "spring.cloud.stream.bindings.publishDlq-out-0.producer.use-native-encoding=true",
        "management.endpoint.health.group.readiness.include=readinessState"
})
class DlqPublisherBinderIT {

    @Autowired DlqPublisher publisher;
    @Autowired OutputDestination output;
    @Autowired MeterRegistry metrics;

    @Test
    void publish_sends_payload_byte_for_byte_with_diagnostic_headers() {
        byte[] sourcePayload = new byte[]{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, (byte) 0xFF};
        publisher.publish(sourcePayload, "Default@5", "bad_request", 400,
                "http://target/write", "boom: invalid metric name");

        Message<byte[]> sent = output.receive(2000, "publishDlq-out-0");
        assertThat(sent).isNotNull();
        // CRITICAL: byte-for-byte preservation - validates use-native-encoding: true scar prophylactic
        assertThat(sent.getPayload()).isEqualTo(sourcePayload);
        // 6 diagnostic headers
        assertThat(sent.getHeaders()).containsKeys(
                "x-dlq-reason", "x-dlq-http-status", "x-dlq-endpoint",
                "x-dlq-timestamp-ms", "x-dlq-error-message", "x-dlq-writer-version");
        assertThat(sent.getHeaders().get("x-dlq-reason")).isEqualTo("bad_request");
        assertThat(sent.getHeaders().get("x-dlq-http-status")).isEqualTo("400");
        assertThat(sent.getHeaders().get("x-dlq-endpoint")).isEqualTo("http://target/write");
        // Counter incremented
        assertThat(metrics.find("deltav.prometheus.writer.dlq.records")
                .tag("reason", "bad_request").counter().count()).isEqualTo(1.0);
    }

    @EnableAutoConfiguration
    @Import({DlqPublisher.class, TestChannelBinderConfiguration.class})
    static class TestApp {
        @Bean MeterRegistry metrics() { return new SimpleMeterRegistry(); }
    }
}
