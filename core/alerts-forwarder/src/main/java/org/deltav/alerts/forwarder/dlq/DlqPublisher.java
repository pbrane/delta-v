/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.alerts.forwarder.dlq;

import java.nio.charset.StandardCharsets;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.deltav.alerts.forwarder.metrics.AlertsForwarderMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Publishes records that cannot be parsed as {@link org.deltav.alarms.proto.AlarmState}
 * to the dead-letter topic, preserving the original key and bytes plus a reason
 * header. A poison record never wedges the consumer — it is DLQ'd and skipped.
 */
public class DlqPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(DlqPublisher.class);

    private final Producer<byte[], byte[]> producer;
    private final String topic;
    private final MeterRegistry metrics;

    public DlqPublisher(Producer<byte[], byte[]> producer, String topic, MeterRegistry metrics) {
        this.producer = producer;
        this.topic = topic;
        this.metrics = metrics;
    }

    public void publish(byte[] key, byte[] sourcePayload, String reason) {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, key, sourcePayload);
        record.headers().add("x-dlq-reason", reason.getBytes(StandardCharsets.UTF_8));
        record.headers().add("x-dlq-timestamp-ms",
                Long.toString(System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8));
        producer.send(record, (md, ex) -> {
            if (ex != null) {
                LOG.error("Failed to publish DLQ record reason={}", reason, ex);
            }
        });
        metrics.counter(AlertsForwarderMetrics.DLQ_RECORDS).increment();
    }
}
