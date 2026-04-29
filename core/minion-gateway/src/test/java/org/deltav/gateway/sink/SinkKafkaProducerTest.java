/*
 * Copyright (C) 2026 BeaconStrategists, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.deltav.gateway.sink;

import com.codahale.metrics.MetricRegistry;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.opennms.core.ipc.sink.model.SinkMessage;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class SinkKafkaProducerTest {

    @Test
    void send_publishesToCorrectTopicWithKeyAndSinkMessageWrappedPayload() throws Exception {
        MockProducer<String, byte[]> mock = new MockProducer<>(true, null,
            new StringSerializer(), new ByteArraySerializer());
        MetricRegistry metrics = new MetricRegistry();
        SinkKafkaProducer producer = new SinkKafkaProducer(mock, metrics);

        CompletableFuture<Void> result = producer.send(
            "OpenNMS.Sink.Syslog", "Default@minion-A", "payload-bytes".getBytes());

        result.get(2, TimeUnit.SECONDS);

        assertThat(mock.history()).hasSize(1);
        ProducerRecord<String, byte[]> record = mock.history().get(0);
        assertThat(record.topic()).isEqualTo("OpenNMS.Sink.Syslog");
        assertThat(record.key()).isEqualTo("Default@minion-A");

        // Published bytes must be a SinkMessage protobuf containing the payload —
        // horizon's KafkaSinkBridge calls SinkMessage.parseFrom on every record.
        SinkMessage parsed = SinkMessage.parseFrom(record.value());
        assertThat(parsed.getContent().toStringUtf8()).isEqualTo("payload-bytes");
        assertThat(parsed.getMessageId()).isNotBlank();
        assertThat(parsed.getCurrentChunkNumber()).isEqualTo(1);
        assertThat(parsed.getTotalChunks()).isEqualTo(1);

        assertThat(metrics.counter("minion_gateway_sink_publish_total").getCount()).isEqualTo(1);
    }

    @Test
    void send_failureIncrementsFailureCounterAndCompletesExceptionally() {
        MockProducer<String, byte[]> mock = new MockProducer<>(false, null,
            new StringSerializer(), new ByteArraySerializer());
        MetricRegistry metrics = new MetricRegistry();
        SinkKafkaProducer producer = new SinkKafkaProducer(mock, metrics);

        CompletableFuture<Void> result = producer.send(
            "OpenNMS.Sink.Syslog", "k", new byte[0]);

        mock.errorNext(new RuntimeException("broker down"));

        assertThat(result).isCompletedExceptionally();
        assertThat(metrics.counter("minion_gateway_sink_publish_failures").getCount()).isEqualTo(1);
    }
}
