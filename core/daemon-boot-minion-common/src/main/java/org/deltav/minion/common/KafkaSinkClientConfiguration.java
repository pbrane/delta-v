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
import org.deltav.horizon.metrics.HorizonMetricsBridge;
import org.opennms.core.ipc.sink.api.MessageDispatcherFactory;
import org.opennms.core.ipc.sink.kafka.client.KafkaRemoteMessageDispatcherFactory;
import org.opennms.core.tracing.api.TracerRegistry;
import org.opennms.distributed.core.api.MinionIdentity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import com.codahale.metrics.MetricRegistry;

/**
 * Spring Boot {@link Configuration} that wraps the OSGi-designed
 * {@link KafkaRemoteMessageDispatcherFactory} in a Spring {@link SmartLifecycle}.
 *
 * <p>The factory uses setter injection because it was designed for OSGi blueprint
 * wiring. We set {@code bundleContext} to {@code null} so the factory's
 * {@code onInit()} skips OSGi metrics registration.</p>
 *
 * <p>Lifecycle phase 200 ensures the Sink client starts <b>after</b> the
 * Twin subscriber (phase 100) and <b>before</b> the RPC server (phase 300).
 * Shutdown reverses this order automatically.</p>
 */
@Configuration
@ConditionalOnProperty(name = "opennms.minion.sink.enabled", havingValue = "true", matchIfMissing = true)
public class KafkaSinkClientConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSinkClientConfiguration.class);

    @Bean
    public MetricRegistry minionSinkMetricRegistry() {
        return new MetricRegistry();
    }

    @Bean
    public HorizonMetricsBridge minionSinkMetricsBridge(MetricRegistry minionSinkMetricRegistry) {
        return new HorizonMetricsBridge(minionSinkMetricRegistry, "opennms");
    }

    @Bean
    @Primary
    public KafkaRemoteMessageDispatcherFactory kafkaRemoteMessageDispatcherFactory(
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
    public SmartLifecycle kafkaSinkClientLifecycle(
            KafkaRemoteMessageDispatcherFactory factory) {

        return new SmartLifecycle() {
            private volatile boolean running = false;

            @Override
            public void start() {
                LOG.info("Starting Kafka Sink client (phase 200)");
                try {
                    factory.init();
                    running = true;
                    LOG.info("Kafka Sink client started");
                } catch (Exception e) {
                    LOG.error("Failed to initialize Kafka Sink client", e);
                }
            }

            @Override
            public void stop() {
                LOG.info("Stopping Kafka Sink client (phase 200)");
                factory.destroy();
                running = false;
                LOG.info("Kafka Sink client stopped");
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
