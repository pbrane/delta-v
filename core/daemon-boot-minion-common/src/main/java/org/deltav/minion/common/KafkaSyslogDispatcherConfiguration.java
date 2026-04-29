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
package org.deltav.minion.common;

import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.opennms.core.ipc.sink.api.MessageDispatcherFactory;
import org.opennms.core.ipc.sink.kafka.client.KafkaRemoteMessageDispatcherFactory;
import org.opennms.core.tracing.api.TracerRegistry;
import org.opennms.distributed.core.api.MinionIdentity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.codahale.metrics.MetricRegistry;

/**
 * Kafka-backed {@link MessageDispatcherFactory} for the Syslog sink.
 * Active when {@code opennms.minion.transport.sink.syslog=kafka} (rollback
 * path); the default is {@code grpc}, served by
 * {@code GrpcSyslogDispatcherConfiguration} (Task 5).
 *
 * <p>Lifecycle phase 200 — same as rc1's monolithic config. SmartLifecycle
 * start order: Sink (200) before RPC (300) before listeners (400).
 */
@Configuration
@ConditionalOnProperty(name = "opennms.minion.transport.sink.syslog", havingValue = "kafka")
public class KafkaSyslogDispatcherConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSyslogDispatcherConfiguration.class);

    @Bean(name = "syslogDispatcherFactory")
    public KafkaRemoteMessageDispatcherFactory syslogDispatcherFactory(
            @Value("${opennms.kafka.bootstrap-servers:localhost:9092}") String bootstrapServers,
            MinionIdentity minionIdentity,
            TracerRegistry tracerRegistry,
            MetricRegistry minionSinkMetricRegistry) {

        Properties kafkaProps = new Properties();
        kafkaProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        KafkaRemoteMessageDispatcherFactory factory = new KafkaRemoteMessageDispatcherFactory();
        factory.setConfigAdmin(new SpringConfigurationAdmin(kafkaProps));
        factory.setBundleContext(null);
        factory.setTracerRegistry(tracerRegistry);
        factory.setIdentity(minionIdentity);
        factory.setMetrics(minionSinkMetricRegistry);
        return factory;
    }

    @Bean
    public SmartLifecycle syslogDispatcherFactoryLifecycle(
            @Qualifier("syslogDispatcherFactory") KafkaRemoteMessageDispatcherFactory syslogDispatcherFactory) {
        return new SmartLifecycle() {
            private volatile boolean running = false;

            @Override
            public void start() {
                LOG.info("Starting Kafka Syslog dispatcher (phase 200)");
                try {
                    syslogDispatcherFactory.init();
                    running = true;
                } catch (Exception e) {
                    LOG.error("Failed to init Kafka Syslog dispatcher", e);
                }
            }

            @Override
            public void stop() {
                LOG.info("Stopping Kafka Syslog dispatcher");
                syslogDispatcherFactory.destroy();
                running = false;
            }

            @Override
            public boolean isRunning() {
                return running;
            }

            @Override
            public int getPhase() {
                return 200;
            }
        };
    }
}
