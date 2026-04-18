/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.dlq;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Component
public class DlqPublisher {
    private static final Logger LOG = LoggerFactory.getLogger(DlqPublisher.class);
    private static final String BINDING = "publishDlq-out-0";

    private final StreamBridge streamBridge;
    private final MeterRegistry metrics;

    public DlqPublisher(StreamBridge streamBridge, MeterRegistry metrics) {
        this.streamBridge = streamBridge;
        this.metrics = metrics;
    }

    public void publish(byte[] sourcePayload, String key, String reason, int httpStatus,
                        String endpoint, String errorMessage) {
        Map<String, Object> headers = new HashMap<>();
        headers.put(KafkaHeaders.KEY, key.getBytes());
        headers.put("x-dlq-reason", reason);
        headers.put("x-dlq-http-status", Integer.toString(httpStatus));
        headers.put("x-dlq-endpoint", endpoint);
        headers.put("x-dlq-timestamp-ms", Long.toString(System.currentTimeMillis()));
        headers.put("x-dlq-error-message", truncate(errorMessage, 1024));
        headers.put("x-dlq-writer-version",
                Optional.ofNullable(getClass().getPackage().getImplementationVersion()).orElse("dev"));
        Message<byte[]> msg = MessageBuilder.withPayload(sourcePayload).copyHeaders(headers).build();
        boolean sent = streamBridge.send(BINDING, msg);
        if (sent) {
            Counter.builder("deltav.prometheus.writer.dlq.records")
                    .tag("reason", reason).register(metrics).increment();
        } else {
            LOG.error("StreamBridge.send returned false - DLQ record lost: key={} reason={}", key, reason);
        }
    }

    private static String truncate(String s, int max) {
        if (s == null) return "";
        return s.length() <= max ? s : s.substring(0, max);
    }
}
