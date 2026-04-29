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
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.concurrent.CompletableFuture;

/**
 * Generic per-topic byte[] producer backing the three sink gRPC services
 * ({@link SyslogGrpcService}, {@code TrapGrpcService} (Task 7),
 * {@code TelemetryGrpcService} (Task 8)). Each call publishes opaque bytes
 * to the named Kafka topic and counts success/failure under the
 * {@code minion_gateway_sink_publish_*} Dropwizard counters
 * (surfaced via the {@code HorizonMetricsBridge} at
 * {@code /actuator/prometheus} per
 * {@code feedback_meter_naming_horizon_vs_deltav}).
 */
public class SinkKafkaProducer {

    private final Producer<String, byte[]> producer;
    private final MetricRegistry metrics;

    public SinkKafkaProducer(Producer<String, byte[]> producer, MetricRegistry metrics) {
        this.producer = producer;
        this.metrics = metrics;
    }

    public CompletableFuture<Void> send(String topic, String key, byte[] payload) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        producer.send(new ProducerRecord<>(topic, key, payload), (md, ex) -> {
            if (ex != null) {
                metrics.counter("minion_gateway_sink_publish_failures").inc();
                result.completeExceptionally(ex);
            } else {
                metrics.counter("minion_gateway_sink_publish_total").inc();
                result.complete(null);
            }
        });
        return result;
    }
}
