/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.alerts.forwarder.dlq;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.deltav.alerts.forwarder.metrics.AlertsForwarderMetrics;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DlqPublisherTest {

    @Test
    void publishesPoisonRecordToDlqTopicWithReasonHeader() {
        // Kafka client 4.1.1: MockProducer takes (autoComplete, partitioner, keySer, valueSer).
        MockProducer<byte[], byte[]> producer = new MockProducer<>(
                true, null, new ByteArraySerializer(), new ByteArraySerializer());
        SimpleMeterRegistry metrics = new SimpleMeterRegistry();
        DlqPublisher dlq = new DlqPublisher(producer, "deltav-alerts-forwarder-dlq", metrics);

        dlq.publish("rk".getBytes(), "bad-bytes".getBytes(), "parse-error");

        assertThat(producer.history()).hasSize(1);
        ProducerRecord<byte[], byte[]> rec = producer.history().get(0);
        assertThat(rec.topic()).isEqualTo("deltav-alerts-forwarder-dlq");
        assertThat(new String(rec.headers().lastHeader("x-dlq-reason").value()))
                .isEqualTo("parse-error");
        assertThat(metrics.counter(AlertsForwarderMetrics.DLQ_RECORDS).count()).isEqualTo(1.0);
    }
}
