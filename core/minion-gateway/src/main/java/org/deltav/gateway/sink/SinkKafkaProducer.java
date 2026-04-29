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
import com.google.protobuf.ByteString;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.opennms.core.ipc.sink.model.SinkMessage;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Generic per-topic byte[] producer backing the three sink gRPC services
 * ({@link SyslogGrpcService}, {@code TrapGrpcService} (Task 7),
 * {@code TelemetryGrpcService} (Task 8)). Each call wraps the opaque
 * payload in a horizon {@link SinkMessage} protobuf, then publishes the
 * wrapped bytes to the named Kafka topic.
 *
 * <p>The {@code SinkMessage} wrapper is required because horizon's
 * {@code KafkaSinkBridge.pollLoop} (in
 * {@code org.opennms.core.ipc.sink.kafka}) calls
 * {@code SinkMessage.parseFrom(record.value())} on every consumed record.
 * Publishing raw payload bytes would throw
 * {@code InvalidProtocolBufferException} on every consumer poll. The
 * wrapper carries:
 * <ul>
 *   <li>{@code message_id} — fresh UUID per record (de-dup hint for the
 *       consumer; matches the existing Kafka-path Minion behaviour)</li>
 *   <li>{@code content} — the gRPC-delivered opaque payload bytes</li>
 *   <li>{@code current_chunk_number / total_chunks} — both 1; we don't
 *       chunk in PR3 (large messages are deferred to a future PR)</li>
 * </ul>
 *
 * <p>Counts success/failure under the
 * {@code minion_gateway_sink_publish_*} Dropwizard counters (surfaced
 * via the {@code HorizonMetricsBridge} at {@code /actuator/prometheus}
 * per {@code feedback_meter_naming_horizon_vs_deltav}).
 */
public class SinkKafkaProducer {

    private final Producer<String, byte[]> producer;
    private final MetricRegistry metricRegistry;

    public SinkKafkaProducer(Producer<String, byte[]> producer, MetricRegistry metricRegistry) {
        this.producer = producer;
        this.metricRegistry = metricRegistry;
    }

    public CompletableFuture<Void> send(String topic, String key, byte[] payload) {
        byte[] wrapped = SinkMessage.newBuilder()
            .setMessageId(UUID.randomUUID().toString())
            .setContent(ByteString.copyFrom(payload))
            .setCurrentChunkNumber(1)
            .setTotalChunks(1)
            .build()
            .toByteArray();

        CompletableFuture<Void> result = new CompletableFuture<>();
        producer.send(new ProducerRecord<>(topic, key, wrapped), (md, ex) -> {
            if (ex != null) {
                metricRegistry.counter("minion_gateway_sink_publish_failures").inc();
                result.completeExceptionally(ex);
            } else {
                metricRegistry.counter("minion_gateway_sink_publish_total").inc();
                result.complete(null);
            }
        });
        return result;
    }
}
