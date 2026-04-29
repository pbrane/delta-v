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
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

/**
 * Spring wiring for the gateway sink channel. Produces a single
 * shared Kafka producer used by {@link SinkKafkaProducer} for all
 * three sink gRPC services. The producer is configured with
 * {@code acks=1} matching rc1's {@code HeartbeatKafkaProducer}
 * — Decision 6 explicitly classifies sinks as lossy by design, so
 * waiting for in-sync-replica acknowledgement is overhead.
 */
@Configuration
public class SinkChannelConfiguration {

    private KafkaProducer<String, byte[]> kafkaProducer;

    @Bean
    public Producer<String, byte[]> sinkKafkaRawProducer(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "minion-gateway-sinks");
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        // Suppress Kafka's built-in JMX reporter; metrics surface via HorizonMetricsBridge
        // under opennms_kafka_producer_* (see SinkKafkaProducer's MetricRegistry counters).
        props.put(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, "");
        kafkaProducer = new KafkaProducer<>(props);
        return kafkaProducer;
    }

    @Bean
    public SinkKafkaProducer sinkKafkaProducer(Producer<String, byte[]> sinkKafkaRawProducer,
                                                MetricRegistry metricRegistry) {
        return new SinkKafkaProducer(sinkKafkaRawProducer, metricRegistry);
    }

    @PreDestroy
    public void shutdown() {
        if (kafkaProducer != null) {
            kafkaProducer.close();
        }
    }
}
