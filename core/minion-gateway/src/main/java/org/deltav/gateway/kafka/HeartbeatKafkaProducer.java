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
package org.deltav.gateway.kafka;

import com.codahale.metrics.MetricRegistry;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;

/**
 * Republishes translated Heartbeat records to {@code DeltaV.Sink.Heartbeat}
 * so the existing horizon consumer keys correctly off identity.
 *
 * <p>Per {@code feedback_meter_naming_horizon_vs_deltav}, the publish counters
 * are registered against the Dropwizard {@link MetricRegistry} so they appear
 * at {@code /actuator/prometheus} under the {@code opennms_} prefix via the
 * {@code HorizonMetricsBridge}.
 */
@Component
public class HeartbeatKafkaProducer {

    private static final Logger LOG = LoggerFactory.getLogger(HeartbeatKafkaProducer.class);

    private final String bootstrapServers;
    private final MetricRegistry metricRegistry;
    private KafkaProducer<String, byte[]> producer;

    public HeartbeatKafkaProducer(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
            MetricRegistry metricRegistry) {
        this.bootstrapServers = bootstrapServers;
        this.metricRegistry = metricRegistry;
    }

    @PostConstruct
    public void start() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "minion-gateway-heartbeat");
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, "");
        producer = new KafkaProducer<>(props);
        LOG.info("HeartbeatKafkaProducer started; bootstrap={}", bootstrapServers);
    }

    @PreDestroy
    public void stop() {
        if (producer != null) {
            producer.close();
            LOG.info("HeartbeatKafkaProducer stopped");
        }
    }

    public CompletableFuture<Void> send(ProducerRecord<String, byte[]> record) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                metricRegistry.counter("minion_gateway_heartbeat_kafka_publish_failures").inc();
                result.completeExceptionally(exception);
            } else {
                metricRegistry.counter("minion_gateway_heartbeat_kafka_publish_total").inc();
                result.complete(null);
            }
        });
        return result;
    }
}
